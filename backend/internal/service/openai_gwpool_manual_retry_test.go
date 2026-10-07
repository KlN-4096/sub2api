package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolManualClearPersistsAndKeepsLiveTicket(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	account, _ := repo.GetByID(ctx, 1)
	stale, _ := repo.GetByID(ctx, 2)
	before, _ := readOpenAIGatewayHistory(account)
	pair := openAIGatewayPoolPair{gateway: "recent", version: "live", until: now.Add(time.Hour), routeExpiresAt: now.Add(time.Hour)}
	mark := gatewayPoolVerifiedMark{version: "live", at: now}
	svc.codexCookies.poolPairs.Store(identity, pair)
	svc.codexCookies.poolVerified.Store(identity, mark)
	require.NoError(t, svc.clearGatewayPoolCooldown(ctx, account, identity))
	for _, id := range []int64{1, 2} {
		fresh, err := repo.GetByID(ctx, id)
		require.NoError(t, err)
		history, _ := readOpenAIGatewayHistory(fresh)
		require.False(t, history.CooldownReset.ClearedAt.IsZero())
		require.Equal(t, before.Seen["recent"].At, history.Seen["recent"].At)
		require.Equal(t, before.Seen["recent"].FullHeldMs, history.Seen["recent"].FullHeldMs)
		require.True(t, history.Seen["recent"].Cooldown.Cleared)
		require.Zero(t, history.Seen["recent"].Cooldown.FixedSeconds)
		require.Empty(t, history.Seen["recent"].Cooldown.Successes)
	}
	gotPair, _ := svc.codexCookies.poolPairs.Load(identity)
	gotMark, _ := svc.codexCookies.poolVerified.Load(identity)
	require.Equal(t, pair, gotPair)
	require.Equal(t, mark, gotMark)
	svc.codexCookies.gatewayPoolHydrateUsed(stale, identity)
	require.False(t, svc.codexCookies.gatewayPoolUsedRecently(identity, "recent", time.Hour))
	// A restart may see the stale clone first, then the durable owner barrier.
	restarted := &openAICodexCookieStore{accountByID: repo.GetByID, historyByTag: svc.gatewayPoolHistoryPeers}
	require.NoError(t, restarted.hydrateGatewayPoolSharedHistory(ctx, stale, identity))
	require.False(t, restarted.gatewayPoolUsedRecently(identity, "recent", time.Hour))
	require.True(t, restarted.beginGatewayPoolAttempt(identity, "recent", time.Hour))
	require.True(t, restarted.gatewayPoolUsedRecently(identity, "recent", time.Hour))
	entry, _ := restarted.cooldownEntry(identity, "recent")
	require.Zero(t, entry.AttemptSeconds, "manual clear is not an observed one-hour recovery")
}

func TestGatewayPoolManualClearRejectsFetchStartedBeforeClear(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	svc, repo, identity := cooldownResetService(t, time.Now().UTC())
	account, _ := repo.GetByID(ctx, 1)
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-new"), 150)
	entered, release := make(chan struct{}), make(chan struct{})
	fake.onCookie = func() { close(entered); <-release }
	fake.configure(account)
	pool, err := svc.codexCookies.poolClient(account)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := svc.codexCookies.gatewayPoolTakeBatch(ctx, pool, account, identity, false, nil, 1, 0)
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("fetch did not start")
	}
	clearErr := svc.clearGatewayPoolCooldown(ctx, account, identity)
	close(release)
	require.NoError(t, clearErr)
	require.ErrorIs(t, <-done, errGatewayPoolGenerationChanged)
	require.False(t, svc.codexCookies.gatewayPoolUsedRecently(identity, "unified-new", time.Hour))
}

func TestGatewayPoolManualClearDropsOnlyCandidateBackoff(t *testing.T) {
	for _, code := range []string{gwpool.CodeAllCooling, gwpool.CodeConsumerRejected, gwpool.CodeUpstreamRejected, gwpool.CodeNoExit} {
		ctx := context.Background()
		svc, repo, identity := cooldownResetService(t, time.Now().UTC())
		account, _ := repo.GetByID(ctx, 1)
		svc.codexCookies.rememberGatewayPoolBackoff(identity, time.Time{}, time.Minute, code)
		require.NoError(t, svc.clearGatewayPoolCooldown(ctx, account, identity))
		left, _ := svc.codexCookies.gatewayPoolBackoffFor(identity)
		require.Equal(t, code != gwpool.CodeAllCooling, left > 0)
		svc.codexCookies.rememberGatewayPoolBackoff(identity, time.Time{}, time.Minute, code)
		left, _ = svc.codexCookies.gatewayPoolBackoffFor(identity)
		require.Equal(t, code != gwpool.CodeAllCooling, left > 0, "late candidate failure cannot restore old backoff")
	}
}

func TestGatewayPoolManualClearKeepsInFlightAuthenticationRefusal(t *testing.T) {
	for _, code := range []string{gwpool.CodeConsumerRejected, gwpool.CodeUpstreamRejected, gwpool.CodeNoExit} {
		t.Run(code, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			svc, repo, identity := cooldownResetService(t, time.Now().UTC())
			account, _ := repo.GetByID(ctx, 1)
			fake := newGwpoolFakePool(t, "", 150)
			fake.refuseStatus, fake.refuseCode, fake.refuseRetryAfter = 503, code, 60
			entered, release := make(chan struct{}), make(chan struct{})
			fake.beforeCookie = func() { close(entered); <-release }
			fake.configure(account)
			pool, err := svc.codexCookies.poolClient(account)
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() {
				_, err := svc.codexCookies.gatewayPoolTakeBatch(ctx, pool, account, identity, false, nil, 1, 0)
				done <- err
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("fetch did not start")
			}
			clearErr := svc.clearGatewayPoolCooldown(ctx, account, identity)
			close(release)
			require.NoError(t, clearErr)
			err = <-done
			var refused *gwpool.PoolError
			require.ErrorAs(t, err, &refused)
			require.Equal(t, code, refused.Code)
			require.False(t, gatewayPoolRetryablePreparationError(err))
			left, retainedCode := svc.codexCookies.gatewayPoolBackoffFor(identity)
			require.Positive(t, left)
			require.Equal(t, code, retainedCode)
			require.EqualValues(t, 1, fake.hits.Load())
		})
	}
}

type gatewayManualRestRepo struct {
	*gatewayEarlyAccountsRepo
	failID int64
}

func (r *gatewayManualRestRepo) ClearGatewayPoolRest(ctx context.Context, id int64, patch map[string]any) error {
	if id == r.failID {
		return errors.New("injected partial clear failure")
	}
	if err := r.UpdateExtra(ctx, id, patch); err != nil {
		return err
	}
	row := r.rows[id]
	row.mu.Lock()
	defer row.mu.Unlock()
	if strings.HasPrefix(row.account.TempUnschedulableReason, "网关候选低于") {
		row.account.TempUnschedulableUntil = nil
		row.account.TempUnschedulableReason = ""
	}
	return nil
}

func TestGatewayPoolManualClearRestRetriesPartiallyCommittedClones(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, base, identity := cooldownResetService(t, now)
	repo := &gatewayManualRestRepo{gatewayEarlyAccountsRepo: base, failID: 2}
	svc.accountRepo = repo
	for _, id := range []int64{1, 2} {
		account, _ := repo.GetByID(ctx, id)
		state := gatewayPoolRestState{Tag: gatewayPoolLedgerTag(identity), Active: true, ChangedAt: now, NextCheck: now.Add(time.Minute)}
		require.NoError(t, repo.UpdateExtra(ctx, id, map[string]any{gatewayPoolRestStateKey: state}))
		repo.rows[id].account.TempUnschedulableReason = gatewayPoolRestReason(account)
		repo.rows[id].account.TempUnschedulableUntil = &state.NextCheck
	}
	account, _ := repo.GetByID(ctx, 1)
	require.Error(t, svc.clearGatewayPoolManualRest(ctx, account, identity))
	first, _ := repo.GetByID(ctx, 1)
	require.False(t, readGatewayPoolRest(first, gatewayPoolLedgerTag(identity)).Active)
	repo.failID = 0
	require.NoError(t, svc.clearGatewayPoolManualRest(ctx, account, identity))
	second, _ := repo.GetByID(ctx, 2)
	require.Nil(t, second.TempUnschedulableUntil)
	require.False(t, readGatewayPoolRest(second, gatewayPoolLedgerTag(identity)).Active)
}

func TestGatewayPoolManualClearRejectsLateTouchAndHistory(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	account, _ := repo.GetByID(ctx, 1)
	require.NoError(t, svc.clearGatewayPoolCooldown(ctx, account, identity))
	before, _ := repo.GetByID(ctx, 1)
	svc.codexCookies.gatewayPoolMarkUsed(identity, "recent", time.Time{})
	require.Nil(t, svc.codexCookies.observeGatewayPoolCooldown(identity, "recent", openAIGatewayVerdictDegraded, time.Hour, time.Time{}))
	svc.noteOpenAIGatewayUse(context.WithValue(ctx, gatewayPoolObservationEpochKey{}, time.Time{}),
		account, "recent", "test-region", openAIGatewayVerdictDegraded, true, 0, 0, 0, gatewayPoolLedgerTag(identity))
	after, _ := repo.GetByID(ctx, 1)
	require.Equal(t, before.Extra, after.Extra)
	require.False(t, svc.codexCookies.gatewayPoolUsedRecently(identity, "recent", time.Hour))
}

func TestGatewayPoolManualClearFailedWriteDoesNotPublish(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, repo, identity := cooldownResetService(t, now)
	account, _ := repo.GetByID(ctx, 1)
	repo.rows[1].fail = true
	require.Error(t, svc.clearGatewayPoolCooldown(ctx, account, identity))
	require.True(t, svc.codexCookies.gatewayPoolCooldownClearAt(identity).IsZero())
	require.True(t, svc.codexCookies.gatewayPoolUsedRecently(identity, "recent", time.Hour))
}
