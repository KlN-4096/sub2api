package service

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolLocalResumeIgnoresPoolSupplyAndOldRecheck(t *testing.T) {
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 2
	fake := newGwpoolFakePool(t, "unused", 150)
	fake.configure(account)
	fake.listStatus = http.StatusServiceUnavailable
	lists := 0
	fake.onList = func() { lists++ }
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
	svc := withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: repo})
	now := time.Now().UTC()
	for _, gateway := range []string{"one", "two"} {
		svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, gateway), now.Add(-2*time.Hour))
	}
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, now, now.Add(10*time.Minute)))
	legacy := gatewayPoolRestState{Tag: gatewayPoolLedgerTag(gwpoolTestIdentity), Active: true, ChangedAt: now}
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{gatewayPoolRestStateKey: map[string]any{
		"tag": legacy.Tag, "active": true, "changed_at": now, "next_check": now.Add(10 * time.Minute)}}))
	svc.codexCookies.poolRestState.Store(legacy.Tag, legacy)
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.NoError(t, err)
	require.True(t, allowed, "local expiry must not wait for legacy recheck or pool supply")
	require.Zero(t, lists, "resuming never queries the pool")
	require.Zero(t, fake.hits.Load())
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	require.True(t, readGatewayPoolRest(fresh, legacy.Tag).StartedAt.IsZero(),
		"migration must not invent an original rest entry time")
}

func TestGatewayPoolLocalRestDeadlineIsUnclampedAndCapsKnownCount(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 40
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	require.Zero(t, store.gatewayPoolRestDuration(gwpoolTestIdentity, account, now),
		"no local history must not invent a resting deadline")
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "one"), now.Add(-30*time.Minute))
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "two"), now.Add(-20*time.Minute))
	require.Equal(t, 40*time.Minute, store.gatewayPoolRestDuration(gwpoolTestIdentity, account, now),
		"fewer than the configured threshold means wait for all known gateways, without a 10-minute clamp")
}

func TestGatewayPoolLocalResumeDeadlineFollowsCooldownFormula(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	window := account.gatewayPoolGatewayWindow()
	base := time.Duration(gatewayPoolCooldownBase(window)) * time.Second
	now := time.Now().UTC()
	touched := now.Add(-10 * time.Minute)
	for _, tc := range []struct {
		name     string
		cooldown *gatewayPoolCooldown
		want     time.Time
	}{
		{"no learned cooldown", nil, touched.Add(base)},
		{"local floor", &gatewayPoolCooldown{SourcesKnown: true, CycleAt: touched.Add(-time.Hour), UpdatedAt: touched,
			LocalFloorSeconds: 4 * 3600, WindowSeconds: 4 * 3600}, touched.Add(4 * time.Hour)},
		{"fixed tier", &gatewayPoolCooldown{SourcesKnown: true, CycleAt: touched, UpdatedAt: touched,
			FixedSeconds: 2 * 3600, WindowSeconds: 2 * 3600}, touched.Add(2 * time.Hour)},
		{"manual clear", &gatewayPoolCooldown{Cleared: true, Until: now.Add(-5 * time.Minute), WindowSeconds: 3600}, now.Add(-5 * time.Minute)},
		{"unseen entry", &gatewayPoolCooldown{}, touched},
	} {
		store := &openAICodexCookieStore{}
		key := gatewayPoolLedgerKey(gwpoolTestIdentity, "g")
		store.poolUsed.Store(key, touched)
		if tc.cooldown != nil {
			store.poolCooldown = map[string]gatewayPoolCooldown{key: *tc.cooldown}
		}
		got := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, time.Now())
		require.True(t, got.Equal(tc.want), "%s: got %v, want %v", tc.name, got, tc.want)
		at, cooling := store.gatewayPoolUsedAt(gwpoolTestIdentity, "g", window)
		require.True(t, at.Equal(touched), tc.name)
		require.Equal(t, time.Now().Before(tc.want), cooling, tc.name)
	}
}

// cooldownCohort touches each named gateway now with a learned cooldown of the given hours.
func cooldownCohort(store *openAICodexCookieStore, now time.Time, hours int, names ...string) {
	if store.poolCooldown == nil {
		store.poolCooldown = map[string]gatewayPoolCooldown{}
	}
	for _, name := range names {
		key := gatewayPoolLedgerKey(gwpoolTestIdentity, name)
		store.poolUsed.Store(key, now)
		store.poolCooldown[key] = gatewayPoolCooldown{SourcesKnown: true, CycleAt: now, UpdatedAt: now,
			LocalFloorSeconds: hours * 3600, WindowSeconds: hours * 3600}
	}
}

func cooldownNames(prefix string, n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprint(prefix, i)
	}
	return names
}

// 2026-10-12 pro1: 23 of the 24 quality gateways cooled within 2h, but one at the
// 6h step disabled the cohort rule and the rest waited for the 30th known gateway.
func TestGatewayPoolLocalResumeSkipsALongWaitInTheQualityCohort(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 30
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	quality := append(cooldownNames("q", 23), "slow")
	cooldownCohort(store, now, 2, quality[:23]...)
	cooldownCohort(store, now, 6, "slow")
	cooldownCohort(store, now, 4, cooldownNames("mid", 5)...)
	cooldownCohort(store, now, 6, cooldownNames("long", 60)...)
	got := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now, quality...)
	require.True(t, got.Equal(now.Add(2*time.Hour)), "got %v: over 1.5x the cohort's median wait is an outlier", got.Sub(now))
}

func TestGatewayPoolLocalResumeSkipsLongWaitsInTheKnownCount(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 50
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	cooldownCohort(store, now, 1, cooldownNames("short", 35)...)
	cooldownCohort(store, now, 6, cooldownNames("long", 5)...)
	got := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now)
	require.True(t, got.Equal(now.Add(time.Hour)), "got %v: fewer than N waits for all but the outliers", got.Sub(now))
	cooldownCohort(store, now, 2, cooldownNames("near", 3)...)
	got = store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now)
	require.True(t, got.Equal(now.Add(time.Hour)), "got %v: 2h is over 1.5x the 1h median", got.Sub(now))
	cleared := &openAICodexCookieStore{}
	cooldownCohort(cleared, now.Add(-30*time.Minute), 1, cooldownNames("cleared", 40)...)
	cleared.applyGatewayPoolCooldownClear(gwpoolTestIdentity, now.Add(-time.Minute), 3600)
	cooldownCohort(cleared, now, 6, "after-clear")
	key := gatewayPoolLedgerKey(gwpoolTestIdentity, "after-clear")
	learned := cleared.poolCooldown[key]
	learned.ResetAt = now.Add(-time.Minute) // learned after the clear
	cleared.poolCooldown[key] = learned
	got = cleared.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now)
	require.True(t, got.Equal(now.Add(6*time.Hour)), "got %v: cleared gateways have cooled and never set the median", got.Sub(now))
}

// All four sit at the 6h step, but three were touched 5h ago: only the one
// touched just now is a long wait.
func TestGatewayPoolLocalResumeJudgesOutliersByRemainingWait(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 50
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	cooldownCohort(store, now.Add(-5*time.Hour), 6, "a", "b", "c")
	cooldownCohort(store, now, 6, "fresh")
	got := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now, "a", "b", "c", "fresh")
	require.True(t, got.Equal(now.Add(time.Hour)), "got %v", got.Sub(now))
}

// A long-step gateway that has already cooled is no outlier and still counts
// toward N.
func TestGatewayPoolLocalResumeStillCountsCooledLongSteps(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 2
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	cooldownCohort(store, now.Add(-7*time.Hour), 6, "cooled-a", "cooled-b")
	cooldownCohort(store, now.Add(-30*time.Minute), 1, "s1")
	cooldownCohort(store, now.Add(-10*time.Minute), 1, "s2")
	cooldownCohort(store, now.Add(-5*time.Minute), 1, "s3")
	got := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now)
	require.True(t, got.Equal(now.Add(-time.Hour)), "got %v: two gateways have already cooled", got.Sub(now))
}

// The ladder's 4h->6h step sits exactly on the 1.5x cut and a verdict lands a
// little after its touch; that lag must not turn the 6h gateway into an outlier.
func TestGatewayPoolLocalResumeOutlierCutToleratesVerdictLag(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 50
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	quality := []string{"q0", "q1", "q2", "slow"}
	cooldownCohort(store, now, 4, quality[:3]...)
	cooldownCohort(store, now, 6, "slow")
	key := gatewayPoolLedgerKey(gwpoolTestIdentity, "slow")
	lagged := store.poolCooldown[key]
	lagged.CycleAt = now.Add(time.Microsecond)
	store.poolCooldown[key] = lagged
	got := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now, quality...)
	require.True(t, got.Equal(now.Add(6*time.Hour+time.Microsecond)), "got %v: exactly 1.5x the median is no outlier", got.Sub(now))
}

// Half a cohort still cooling is no outlier: an even count takes the upper
// middle wait (2h here), not the mean (65m) that would skip both 2h gateways.
func TestGatewayPoolLocalResumeNeverSkipsHalfTheList(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 50
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	cooldownCohort(store, now.Add(-50*time.Minute), 1, "a", "b")
	cooldownCohort(store, now, 2, "c", "d")
	got := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now, "a", "b", "c", "d")
	require.True(t, got.Equal(now.Add(2*time.Hour)), "got %v", got.Sub(now))
}

// An hour into a rest the 1h gateways have cooled. Measured from the rest's
// start the 4h and 6h ones stay skipped; measured from now they would become
// the median and hold the rest for five more hours. The dialog's estimate
// measures from the same start.
func TestGatewayPoolRestMeasuresOutlierWaitsFromItsStart(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 50
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	started := now.Add(-time.Hour)
	history := openAIGatewayHistory{LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity), Seen: map[string]openAIGatewaySeen{}}
	for name, hours := range map[string]int{"s0": 1, "s1": 1, "s2": 1, "slow4": 4, "slow6": 6} {
		cooldownCohort(store, started, hours, name)
		history.Seen[name] = openAIGatewaySeen{At: started, Cooldown: &gatewayPoolCooldown{SourcesKnown: true, BaseSeconds: 3600,
			CycleAt: started, UpdatedAt: started, LocalFloorSeconds: hours * 3600, WindowSeconds: hours * 3600,
			Until: started.Add(time.Duration(hours) * time.Hour)}}
	}
	rest := gatewayPoolRestState{Tag: gatewayPoolLedgerTag(gwpoolTestIdentity), Active: true, ChangedAt: started, StartedAt: started}
	admission := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, rest.since(now))
	require.True(t, admission.Equal(now), "admission %v", admission.Sub(now))
	display := gatewayPoolCooldownEstimate(account, store.gatewayPoolDisplayCooldownDeadlines(gwpoolTestIdentity, account, history, now), rest, now)
	require.True(t, display.EligibleAt.Equal(admission), "display %v", display.EligibleAt.Sub(now))
	fromNow := store.gatewayPoolLocalResumeAt(gwpoolTestIdentity, account, now)
	require.True(t, fromNow.Equal(now.Add(5*time.Hour)), "from now %v", fromNow.Sub(now))
}

func TestGatewayPoolRestAdmissionMeasuresWaitsFromItsStart(t *testing.T) {
	account := rotationAccount(1, 7)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account)
	now := time.Now().UTC()
	started := now.Add(-time.Hour)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 50
	account.Extra[gatewayPoolRestStateKey] = map[string]any{
		"tag": gatewayPoolLedgerTag(gwpoolTestIdentity), "active": true,
		"changed_at": started, "started_at": started, "resume_at": now,
	}
	svc := rotationService(account)
	cooldownCohort(&svc.codexCookies, started, 1, "s0", "s1", "s2")
	cooldownCohort(&svc.codexCookies, started, 4, "slow4")
	cooldownCohort(&svc.codexCookies, started, 6, "slow6")
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.NoError(t, err)
	require.True(t, allowed, "the 4h/6h gateways stay skipped after the 1h ones cooled")
}

func TestGatewayPoolLocalResumeKeepsOtherAccountBlocks(t *testing.T) {
	account := rotationAccount(1, 7)
	until := time.Now().Add(time.Hour)
	account.TempUnschedulableUntil, account.TempUnschedulableReason = &until, "auth failure"
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
	svc := withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: repo})
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
	require.NoError(t, err)
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, identity,
		time.Now().Add(-time.Hour), time.Now().Add(-time.Minute)))
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.NoError(t, err)
	require.False(t, allowed)
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, until, *fresh.TempUnschedulableUntil)
	require.False(t, readGatewayPoolRest(fresh, gatewayPoolLedgerTag(identity)).Active,
		"local rest may end independently, but it must not erase auth/health protection")
}

type gatewayLocalRestRepo struct{ *gatewayRuntimeRepo }

func (r *gatewayLocalRestRepo) SetGatewayPoolRest(ctx context.Context, id int64, until time.Time, reason string, patch map[string]any) error {
	if err := r.UpdateExtra(ctx, id, patch); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.account.TempUnschedulableUntil, r.account.TempUnschedulableReason = &until, reason
	return nil
}

func (r *gatewayLocalRestRepo) ClearGatewayPoolRest(ctx context.Context, id int64, patch map[string]any) error {
	if err := r.UpdateExtra(ctx, id, patch); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if gatewayPoolOwnsTempBlock(&r.account) {
		r.account.TempUnschedulableUntil, r.account.TempUnschedulableReason = nil, ""
	}
	return nil
}

func TestGatewayPoolLocalTimerRestoresDeadlineWithoutBusinessOrPoolRequests(t *testing.T) {
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolCooldownResetHoursKey] = 0
	fake := newGwpoolFakePool(t, "unused", 150)
	fake.configure(account)
	fake.listStatus = http.StatusServiceUnavailable
	var listCalls atomic.Int32
	fake.onList = func() { listCalls.Add(1) }
	svc, base := gatewayRuntimeService(account)
	repo := &gatewayLocalRestRepo{base}
	svc.accountRepo = repo
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
	require.NoError(t, err)
	now := time.Now().UTC()
	deadline := now.Add(400 * time.Millisecond)
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{
		openAIGatewayHistoryExtraKey: openAIGatewayHistory{
			LedgerTag: gatewayPoolLedgerTag(identity),
			Seen:      map[string]openAIGatewaySeen{"known": {At: deadline.Add(-time.Hour)}},
		},
	}))
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, identity, now, now.Add(time.Hour)))
	// No process-local rest/touches survive. Startup must load and replace the
	// legacy/wrong deadline using durable history, not wait for business.
	restarted := withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: repo})
	restarted.codexCookies.accountByID = restarted.gatewayPoolLedgerRow
	restarted.StartGatewayPoolReporter()
	defer restarted.StopGatewayPoolReporter()
	require.Eventually(t, func() bool {
		fresh, err := repo.stored(account.ID)
		return err == nil && !readGatewayPoolRest(fresh, gatewayPoolLedgerTag(identity)).Active &&
			fresh.TempUnschedulableUntil == nil
	}, 3*time.Second, 20*time.Millisecond)
	require.Zero(t, fake.hits.Load())
	require.Zero(t, listCalls.Load())
}

func TestGatewayPoolLocalMaintenanceDailyResetReleasesRestImmediately(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, base, identity := cooldownResetService(t, now)
	repo := &gatewayManualRestRepo{gatewayPoolAccountsRepo: base}
	svc.accountRepo = repo
	for _, id := range []int64{1, 2} {
		account, err := repo.stored(id)
		require.NoError(t, err)
		history, _ := readOpenAIGatewayHistory(account)
		delete(history.Seen, "recent") // the remaining 8h cooldown has already rested 2h
		state := gatewayPoolRestState{Tag: gatewayPoolLedgerTag(identity), Active: true,
			ChangedAt: now.Add(-time.Hour), StartedAt: now.Add(-time.Hour), ResumeAt: now.Add(6 * time.Hour)}
		require.NoError(t, repo.UpdateExtra(ctx, id, map[string]any{
			openAIGatewayHistoryExtraKey: history, gatewayPoolRestStateKey: state,
		}))
		base.rows[id].mu.Lock()
		base.rows[id].account.TempUnschedulableUntil = &state.ResumeAt
		base.rows[id].account.TempUnschedulableReason = gatewayPoolRestReason(account)
		base.rows[id].mu.Unlock()
	}
	// Simulate restart, so the removed synthetic fixture touch is absent too.
	svc.codexCookies.poolUsed.Delete(gatewayPoolLedgerKey(identity, "recent"))
	svc.maintainGatewayPoolRests(ctx)
	for _, id := range []int64{1, 2} {
		account, err := repo.stored(id)
		require.NoError(t, err)
		require.False(t, readGatewayPoolRest(account, gatewayPoolLedgerTag(identity)).Active)
		require.Nil(t, account.TempUnschedulableUntil)
	}
}

func TestGatewayPoolLocalDeadlineKeepsClearedKnownGatewaysAfterNewTouchAndRestart(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, base, identity := cooldownResetService(t, now)
	account, err := base.stored(1)
	require.NoError(t, err)
	require.NoError(t, svc.clearGatewayPoolCooldown(ctx, account, identity))
	// 新触碰须严格晚于清除墓碑：Windows 墙钟约 0.5ms 一跳，同一跳内的触碰按清除前的旧触碰算。
	for clearedAt := svc.codexCookies.gatewayPoolCooldownClearAt(identity); !time.Now().After(clearedAt); {
		time.Sleep(time.Millisecond)
	}
	require.True(t, svc.codexCookies.beginGatewayPoolAttempt(identity, "recent", time.Hour))
	deadline := svc.codexCookies.gatewayPoolLocalResumeAt(identity, account, time.Now())
	// Two known, N defaults to 50: both must finish (one has just been touched).
	require.True(t, deadline.After(now))
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	require.False(t, svc.codexCookies.gatewayPoolLocalResumeAt(identity, account, time.Now()).After(time.Now()),
		"the other cleared known gateway still counts")
	fresh, err := base.stored(1)
	require.NoError(t, err)
	fresh.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	restarted := &OpenAIGatewayService{}
	restarted.codexCookies.gatewayPoolHydrateUsed(fresh, identity)
	require.True(t, restarted.codexCookies.beginGatewayPoolAttempt(identity, "recent", time.Hour))
	require.False(t, restarted.codexCookies.gatewayPoolLocalResumeAt(identity, fresh, time.Now()).After(time.Now()))
	view, _ := restarted.gatewayPoolDisplaySnapshot(fresh, identity, nil, nil)
	checkAt := time.Now()
	display := gatewayPoolTestCooldownEstimate(&restarted.codexCookies, identity, fresh, view, checkAt)
	require.Equal(t, restarted.codexCookies.gatewayPoolLocalResumeAt(identity, fresh, checkAt), display.EligibleAt)
}

func TestGatewayPoolLocalResumePreservesAuthBlockWrittenDuringRest(t *testing.T) {
	account := rotationAccount(1, 7)
	svc, base := gatewayRuntimeService(account)
	repo := &gatewayLocalRestRepo{base}
	svc.accountRepo = repo
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, identity, now, now.Add(6*time.Hour)))
	// Repository SetTempUnschedulable now permits the shorter ordinary block
	// to replace a pool-owned field. Its SQL predicate is covered separately.
	authUntil := now.Add(10 * time.Minute)
	base.mu.Lock()
	base.account.TempUnschedulableUntil, base.account.TempUnschedulableReason = &authUntil, "oauth 401"
	base.mu.Unlock()
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.NoError(t, err)
	require.False(t, allowed)
	fresh, err := repo.stored(account.ID)
	require.NoError(t, err)
	require.Equal(t, authUntil, *fresh.TempUnschedulableUntil)
	require.Equal(t, "oauth 401", fresh.TempUnschedulableReason)
	require.False(t, readGatewayPoolRest(fresh, gatewayPoolLedgerTag(identity)).Active)
}
