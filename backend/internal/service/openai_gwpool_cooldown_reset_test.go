package service

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

func TestGatewayPoolCooldownResetConfig(t *testing.T) {
	for _, raw := range []any{nil, 0, int64(0), float64(0), json.Number("0"), 1, 24, float64(24), gatewayPoolCooldownResetMaxHours} {
		account := gwpoolTestAccount(1)
		account.Extra[openAIGatewayPoolBaseURLExtraKey] = "https://pool.example.test"
		account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-consumer"
		account.Extra[openAIGatewayPoolCooldownResetHoursKey] = raw
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra), "%v", raw)
		require.True(t, touchesOpenAIGatewayPoolConfig(map[string]any{openAIGatewayPoolCooldownResetHoursKey: raw}))
	}
	for _, raw := range []any{-1, 1.5, "24", true, math.Inf(1), math.NaN(), gatewayPoolCooldownResetMaxHours + 1} {
		account := gwpoolTestAccount(1)
		account.Extra[openAIGatewayPoolBaseURLExtraKey] = "https://pool.example.test"
		account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-consumer"
		account.Extra[openAIGatewayPoolCooldownResetHoursKey] = raw
		require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra), "%v", raw)
		require.Zero(t, account.gatewayPoolCooldownResetHours(), "bad stored data must not enable resets")
	}
	require.Equal(t, 24, gwpoolTestAccount(1).gatewayPoolCooldownResetHours())
	account := gwpoolTestAccount(1)
	for _, raw := range []any{nil, 0, int64(0), float64(0), json.Number("0"), 12, 24} {
		account.Extra[openAIGatewayPoolCooldownResetHoursKey] = raw
		hours := account.gatewayPoolCooldownResetHours()
		switch raw {
		case nil, 24:
			require.Equal(t, 24, hours)
		case 12:
			require.Equal(t, 12, hours)
		default:
			require.Zero(t, hours, "explicit zero must remain disabled")
		}
	}
	account.Extra[openAIGatewayPoolExtraKey] = false
	require.Zero(t, account.gatewayPoolCooldownResetHours(), "non-pool accounts remain disabled")
}

func TestGatewayPoolCooldownResetSchedule(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var state gatewayPoolCooldownResetState
	require.True(t, state.advance(24, now, time.Time{}))
	require.Equal(t, now, state.StartedAt)
	require.True(t, state.LastAt.IsZero(), "enabling must not clear existing learning")
	require.False(t, state.advance(24, now.Add(24*time.Hour-time.Nanosecond), time.Time{}))
	require.True(t, state.advance(24, now.Add(24*time.Hour), time.Time{}))
	first := state.LastAt
	encoded, err := json.Marshal(state)
	require.NoError(t, err)
	var restarted gatewayPoolCooldownResetState
	require.NoError(t, json.Unmarshal(encoded, &restarted))
	require.False(t, restarted.advance(24, first.Add(time.Hour), time.Time{}))
	require.True(t, restarted.advance(24, first.Add(72*time.Hour), time.Time{}))
	require.Equal(t, first.Add(72*time.Hour), restarted.LastAt, "one reset after downtime")
	require.True(t, restarted.advance(0, first.Add(73*time.Hour), time.Time{}))
	unobservedDisable := restarted
	unobservedDisable.IntervalHours = 24
	require.True(t, restarted.advance(24, first.Add(100*time.Hour), time.Time{}))
	require.True(t, unobservedDisable.advance(24, first.Add(100*time.Hour), time.Time{}))
	require.Equal(t, unobservedDisable, restarted, "missing a quick off/on must not change timer semantics")
	require.Equal(t, first.Add(100*time.Hour), restarted.LastAt, "reenabling an overdue schedule catches up once")
	require.False(t, restarted.advance(24, first.Add(101*time.Hour), time.Time{}))
}

func resetTestCooldown(touched time.Time) *gatewayPoolCooldown {
	return &gatewayPoolCooldown{
		SourcesKnown: true, BaseSeconds: 3600, LocalFloorSeconds: 28800,
		WindowSeconds: 28800, FixedSeconds: 28800, Successes: map[int]int{28800: 2},
		CycleAt: touched, UpdatedAt: touched, Until: touched.Add(8 * time.Hour),
		Outcome: openAIGatewayVerdictDegraded,
	}
}

func TestGatewayPoolCooldownResetKeepsActualRest(t *testing.T) {
	now := time.Now().UTC()
	for _, age := range []time.Duration{20 * time.Minute, 2 * time.Hour} {
		c := resetTestCooldown(now.Add(-age))
		c.AttemptAt, c.AttemptSeconds, c.ElapsedSeconds = c.UpdatedAt, 28800, 28800
		require.True(t, c.resetBackoff(now, now.Add(-age), 3600))
		require.Equal(t, 3600, c.WindowSeconds)
		require.Equal(t, now.Add(time.Hour-age), c.Until)
		require.Zero(t, c.FixedSeconds)
		require.Zero(t, c.LocalFloorSeconds)
		require.Empty(t, c.Successes)
		require.True(t, c.AttemptAt.IsZero())
		require.True(t, validGatewayPoolCooldown(c, now, 3600))
		require.False(t, c.resetBackoff(now, now, 3600), "same reset must not extend the deadline")
	}
	// A later in-memory contact must not be lost to the throttled DB touch.
	c := resetTestCooldown(now.Add(-2 * time.Hour))
	c.resetBackoff(now, now.Add(-10*time.Minute), 3600)
	require.Equal(t, now.Add(50*time.Minute), c.Until)
}

func cooldownResetService(t *testing.T, now time.Time) (*OpenAIGatewayService, *gatewayEarlyAccountsRepo, string) {
	t.Helper()
	repo := &gatewayEarlyAccountsRepo{rows: map[int64]*gatewayRuntimeRepo{}}
	for _, id := range []int64{1, 2} {
		account := gwpoolTestAccount(id)
		identity, err := (&openAICodexCookieStore{}).gatewayPoolIdentity(context.Background(), account)
		require.NoError(t, err)
		tag := gatewayPoolLedgerTag(identity)
		account.Extra[openAIGatewayLedgerTagExtraKey] = tag
		account.Extra[openAIGatewayPoolCooldownResetHoursKey] = 24
		account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
			LedgerTag: tag, Current: "recent", CurrentRegion: "test-region",
			PoolLive: 10, PoolFree: 0, UpdatedAt: now.Add(-20 * time.Minute),
			CooldownReset: gatewayPoolCooldownResetState{IntervalHours: 24, StartedAt: now.Add(-25 * time.Hour)},
			Seen: map[string]openAIGatewaySeen{
				"recent": {At: now.Add(-20 * time.Minute), FullAt: now.Add(-time.Hour),
					Verdict: openAIGatewayVerdictDegraded, FullHeldMs: 1234, Cooldown: resetTestCooldown(now.Add(-20 * time.Minute))},
				"rested": {At: now.Add(-2 * time.Hour), Cooldown: resetTestCooldown(now.Add(-2 * time.Hour))},
			},
		}
		account.Extra["unrelated"] = "preserved"
		account.Extra[openAIGatewayPoolMetricsExtraKey] = map[string]any{"unrelated_counter": 71}
		repo.rows[id] = &gatewayRuntimeRepo{account: *account}
	}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.codexCookies.accountByID = repo.GetByID
	svc.codexCookies.historyByTag = svc.gatewayPoolHistoryPeers
	account, err := repo.GetByID(context.Background(), 1)
	require.NoError(t, err)
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
	require.NoError(t, err)
	svc.codexCookies.gatewayPoolHydrateUsed(account, identity)
	return svc, repo, identity
}

func TestGatewayPoolCooldownResetPersistsClonesAndSurvivesRestart(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	account, _ := repo.GetByID(ctx, 1)
	stale, _ := repo.GetByID(ctx, 2)
	before, _ := readOpenAIGatewayHistory(account)
	pair := openAIGatewayPoolPair{gateway: "recent", version: "live", until: now.Add(time.Hour), routeExpiresAt: now.Add(time.Hour)}
	svc.codexCookies.poolPairs.Store(identity, pair)
	mark := gatewayPoolVerifiedMark{version: "live", at: now}
	svc.codexCookies.poolVerified.Store(identity, mark)

	require.NoError(t, svc.maintainGatewayPoolCooldownReset(ctx, account, now))
	for _, id := range []int64{1, 2} {
		fresh, err := repo.GetByID(ctx, id)
		require.NoError(t, err)
		history, _ := readOpenAIGatewayHistory(fresh)
		require.Equal(t, now, history.CooldownReset.LastAt)
		require.Equal(t, 3600, history.Seen["recent"].Cooldown.WindowSeconds)
		require.Equal(t, now.Add(40*time.Minute), history.Seen["recent"].Cooldown.Until)
		require.Equal(t, before.Seen["recent"].At, history.Seen["recent"].At)
		require.Equal(t, before.Seen["recent"].FullAt, history.Seen["recent"].FullAt)
		require.Equal(t, before.Seen["recent"].FullHeldMs, history.Seen["recent"].FullHeldMs)
		require.Equal(t, before.Current, history.Current)
		require.Equal(t, before.PoolLive, history.PoolLive)
		require.Equal(t, "preserved", fresh.Extra["unrelated"])
		require.Equal(t, account.Extra[openAIGatewayPoolMetricsExtraKey], fresh.Extra[openAIGatewayPoolMetricsExtraKey])
	}
	gotPair, _ := svc.codexCookies.poolPairs.Load(identity)
	gotMark, _ := svc.codexCookies.poolVerified.Load(identity)
	require.Equal(t, pair, gotPair)
	require.Equal(t, mark, gotMark)
	svc.codexCookies.gatewayPoolHydrateUsed(stale, identity)
	cooldown, _ := svc.codexCookies.cooldownEntry(identity, "recent")
	require.Equal(t, 3600, cooldown.WindowSeconds, "old clone data must not restore 8h")

	restarted := &openAICodexCookieStore{accountByID: repo.GetByID, historyByTag: svc.gatewayPoolHistoryPeers}
	require.NoError(t, restarted.hydrateGatewayPoolSharedHistory(ctx, stale, identity))
	require.True(t, restarted.gatewayPoolUsedRecently(identity, "recent", time.Hour))
	require.False(t, restarted.gatewayPoolUsedRecently(identity, "rested", time.Hour))
	restored, _ := restarted.cooldownEntry(identity, "recent")
	require.Equal(t, now.Add(40*time.Minute), restored.Until)
}

func TestGatewayPoolCooldownResetFailedWriteDoesNotApply(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	account, _ := repo.GetByID(ctx, 1)
	repo.rows[1].fail = true
	require.Error(t, svc.maintainGatewayPoolCooldownReset(ctx, account, now))
	require.True(t, svc.codexCookies.gatewayPoolCooldownResetAt(identity).IsZero())
	cooldown, _ := svc.codexCookies.cooldownEntry(identity, "recent")
	require.Equal(t, 28800, cooldown.WindowSeconds)
	repo.rows[1].fail = false
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, svc.maintainGatewayPoolCooldownReset(canceled, account, now), context.Canceled)
}

func TestGatewayPoolCooldownResetUsesLatestCloneTouch(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	for id, age := range map[int64]time.Duration{1: 2 * time.Hour, 2: 10 * time.Minute} {
		account, _ := repo.GetByID(ctx, id)
		history, _ := readOpenAIGatewayHistory(account)
		history.Seen["recent"] = openAIGatewaySeen{At: now.Add(-age), Cooldown: resetTestCooldown(now.Add(-age))}
		require.NoError(t, repo.UpdateExtra(ctx, id, map[string]any{openAIGatewayHistoryExtraKey: history}))
	}
	svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(identity, "recent"), now.Add(-2*time.Hour))
	svc.codexCookies.poolCooldown[gatewayPoolLedgerKey(identity, "recent")] = *resetTestCooldown(now.Add(-2 * time.Hour))
	account, _ := repo.GetByID(ctx, 1)
	require.NoError(t, svc.maintainGatewayPoolCooldownReset(ctx, account, now))
	for _, id := range []int64{1, 2} {
		fresh, _ := repo.GetByID(ctx, id)
		history, _ := readOpenAIGatewayHistory(fresh)
		require.Equal(t, now.Add(50*time.Minute), history.Seen["recent"].Cooldown.Until,
			"the latest touch belongs to the shared credential, not just this row")
	}
}

func TestGatewayPoolCooldownResetOldVerdictCannotTrainNewAttempt(t *testing.T) {
	now, identity := time.Now().UTC(), gwpoolTestIdentity
	store := &openAICodexCookieStore{}
	store.poolCooldown = map[string]gatewayPoolCooldown{
		gatewayPoolLedgerKey(identity, "g"): *resetTestCooldown(now.Add(-2 * time.Hour)),
	}
	store.applyGatewayPoolCooldownReset(identity, now, 3600)
	require.True(t, store.beginGatewayPoolAttempt(identity, "g", time.Hour))
	before, _ := store.cooldownEntry(identity, "g")
	require.Nil(t, store.observeGatewayPoolCooldown(identity, "g", openAIGatewayVerdictDegraded, time.Hour, time.Time{}))
	after, _ := store.cooldownEntry(identity, "g")
	require.Equal(t, before, after)
	require.NotNil(t, store.observeGatewayPoolCooldown(identity, "g", openAIGatewayVerdictDegraded, time.Hour, now))
	after, _ = store.cooldownEntry(identity, "g")
	require.Equal(t, 7200, after.WindowSeconds, "a real post-reset failed 1h attempt may learn again")
}

func TestGatewayPoolCooldownResetStaleCloneCannotEraseNewLearning(t *testing.T) {
	now, identity := time.Now().UTC(), gwpoolTestIdentity
	resetAt := now.Add(-15 * time.Minute)
	store := &openAICodexCookieStore{}
	store.applyGatewayPoolCooldownReset(identity, resetAt, 3600)
	current := gatewayPoolCooldown{
		ResetAt: resetAt, SourcesKnown: true, BaseSeconds: 3600, LocalFloorSeconds: 7200,
		WindowSeconds: 7200, CycleAt: now.Add(-5 * time.Minute), UpdatedAt: now.Add(-5 * time.Minute),
		Until: now.Add(115 * time.Minute), Outcome: openAIGatewayVerdictDegraded,
	}
	store.poolCooldown = map[string]gatewayPoolCooldown{gatewayPoolLedgerKey(identity, "g"): current}
	old := resetTestCooldown(now.Add(-time.Minute))
	store.hydrateCooldown(identity, "g", old, 3600)
	actual, _ := store.cooldownEntry(identity, "g")
	require.Equal(t, current, actual, "old-generation clone data cannot overwrite post-reset learning, even with a later timestamp")
}

func TestGatewayPoolCooldownResetHistoryWriterRejectsOldMemory(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	account, _ := repo.GetByID(ctx, 1)
	require.NoError(t, svc.maintainGatewayPoolCooldownReset(ctx, account, now))
	svc.codexCookies.poolCooldown[gatewayPoolLedgerKey(identity, "recent")] = *resetTestCooldown(now)
	svc.noteOpenAIGatewayUse(ctx, account, "recent", "new-region", "", true, 0, 0, 0,
		gatewayPoolLedgerTag(identity))
	fresh, _ := repo.GetByID(ctx, 1)
	history, _ := readOpenAIGatewayHistory(fresh)
	require.Equal(t, 3600, history.Seen["recent"].Cooldown.WindowSeconds)
	require.Equal(t, now, history.Seen["recent"].Cooldown.ResetAt)
}

func TestGatewayPoolCooldownResetWritersKeepNewEpochLearning(t *testing.T) {
	for _, writer := range []string{"refresh", "history"} {
		t.Run(writer, func(t *testing.T) {
			ctx, now := context.Background(), time.Now().UTC()
			svc, repo, identity := cooldownResetService(t, now)
			resetAt := now.Add(-15 * time.Minute)
			fresh, _ := repo.GetByID(ctx, 1)
			history, _ := readOpenAIGatewayHistory(fresh)
			history.CooldownReset.LastAt = resetAt
			history.Seen["recent"] = openAIGatewaySeen{At: now.Add(-time.Minute), Cooldown: resetTestCooldown(now.Add(-time.Minute))}
			require.NoError(t, repo.UpdateExtra(ctx, 1, map[string]any{openAIGatewayHistoryExtraKey: history}))
			svc.codexCookies.applyGatewayPoolCooldownReset(identity, resetAt, 3600)
			svc.codexCookies.poolCooldown[gatewayPoolLedgerKey(identity, "recent")] = gatewayPoolCooldown{
				ResetAt: resetAt, SourcesKnown: true, BaseSeconds: 3600, LocalFloorSeconds: 7200,
				WindowSeconds: 7200, CycleAt: now.Add(-5 * time.Minute), UpdatedAt: now.Add(-5 * time.Minute),
				Until: now.Add(115 * time.Minute), Outcome: openAIGatewayVerdictDegraded,
			}
			if writer == "refresh" {
				svc.persistGatewayPoolCooldownRefresh(ctx, fresh, identity)
			} else {
				svc.noteOpenAIGatewayUse(ctx, fresh, "recent", "new-region", "", true, 0, 0, 0,
					gatewayPoolLedgerTag(identity))
			}
			fresh, _ = repo.GetByID(ctx, 1)
			history, _ = readOpenAIGatewayHistory(fresh)
			require.Equal(t, 7200, history.Seen["recent"].Cooldown.WindowSeconds)
			svc.codexCookies.gatewayPoolHydrateUsed(fresh, identity)
			restored, _ := svc.codexCookies.cooldownEntry(identity, "recent")
			require.Equal(t, 7200, restored.WindowSeconds)
		})
	}
}

func TestGatewayPoolCooldownResetIgnoresOldRecommendations(t *testing.T) {
	now, identity := time.Now().UTC(), gwpoolTestIdentity
	store := &openAICodexCookieStore{}
	rec := &gwpool.CooldownRecommendation{Seconds: 28800, Samples: 2, Source: "account"}
	store.noteGatewayPoolRecommendationAt(identity, "g", rec, now.Add(-time.Minute))
	store.applyGatewayPoolCooldownReset(identity, now, 3600)
	base, recommendation := store.gatewayPoolInitialCooldown(identity, "g", time.Hour, true)
	require.Equal(t, 3600, base)
	require.Nil(t, recommendation)
	store.noteGatewayPoolRecommendationAt(identity, "g", rec, now.Add(-time.Second))
	require.True(t, store.beginGatewayPoolAttempt(identity, "g", time.Hour, true))
	cooldown, _ := store.cooldownEntry(identity, "g")
	require.Equal(t, 3600, cooldown.WindowSeconds)
	store.noteGatewayPoolRecommendationAt(identity, "g", rec, now.Add(time.Nanosecond))
	base, _ = store.gatewayPoolInitialCooldown(identity, "g", time.Hour, true)
	require.Equal(t, 28800, base, "new explicitly enabled recommendations remain a separate policy")
}

func TestGatewayPoolCooldownResetRunsWithoutOutboxOrModelRequests(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	for id := range repo.rows {
		require.NoError(t, repo.UpdateExtra(ctx, id, map[string]any{openAIGatewayPoolBaseURLExtraKey: server.URL}))
	}
	svc.flushGatewayPoolReports(ctx)
	require.False(t, svc.codexCookies.gatewayPoolCooldownResetAt(identity).IsZero())
	require.Zero(t, calls.Load())
}

func TestGatewayPoolCooldownResetConcurrentPassesAreIdempotent(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	account, _ := repo.GetByID(ctx, 1)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- svc.maintainGatewayPoolCooldownReset(ctx, account, now)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, now, svc.codexCookies.gatewayPoolCooldownResetAt(identity))
	fresh, _ := repo.GetByID(ctx, 1)
	history, _ := readOpenAIGatewayHistory(fresh)
	require.Equal(t, now.Add(40*time.Minute), history.Seen["recent"].Cooldown.Until)
}

func TestGatewayPoolCooldownResetRejectsFutureState(t *testing.T) {
	now := time.Now().UTC()
	require.False(t, (gatewayPoolCooldownResetState{LastAt: now.Add(time.Hour)}).valid(now))
	require.False(t, (gatewayPoolCooldownResetState{IntervalHours: -1}).valid(now))
	require.True(t, (gatewayPoolCooldownResetState{}).valid(now))
}
