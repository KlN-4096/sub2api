package service

import (
	"context"
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
	svc := &OpenAIGatewayService{accountRepo: repo}
	now := time.Now().UTC()
	for _, gateway := range []string{"one", "two"} {
		svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, gateway), now.Add(-2*time.Hour))
	}
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, now, now.Add(10*time.Minute)))
	legacy := gatewayPoolRestState{Tag: gatewayPoolLedgerTag(gwpoolTestIdentity), Active: true,
		ChangedAt: now, NextCheck: now.Add(10 * time.Minute)}
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{gatewayPoolRestStateKey: legacy}))
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

func TestGatewayPoolLocalResumeKeepsOtherAccountBlocks(t *testing.T) {
	account := rotationAccount(1, 7)
	until := time.Now().Add(time.Hour)
	account.TempUnschedulableUntil, account.TempUnschedulableReason = &until, "auth failure"
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
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
	restarted := &OpenAIGatewayService{accountRepo: repo}
	restarted.codexCookies.accountByID = repo.GetByID
	restarted.StartGatewayPoolReporter()
	defer restarted.StopGatewayPoolReporter()
	require.Eventually(t, func() bool {
		fresh, err := repo.GetByID(context.Background(), account.ID)
		return err == nil && !readGatewayPoolRest(fresh, gatewayPoolLedgerTag(identity)).Active &&
			fresh.TempUnschedulableUntil == nil
	}, 3*time.Second, 20*time.Millisecond)
	require.Zero(t, fake.hits.Load())
	require.Zero(t, listCalls.Load())
}

func TestGatewayPoolLocalMaintenanceDailyResetReleasesRestImmediately(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, base, identity := cooldownResetService(t, now)
	repo := &gatewayManualRestRepo{gatewayEarlyAccountsRepo: base}
	svc.accountRepo = repo
	for _, id := range []int64{1, 2} {
		account, err := repo.GetByID(ctx, id)
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
		account, err := repo.GetByID(ctx, id)
		require.NoError(t, err)
		require.False(t, readGatewayPoolRest(account, gatewayPoolLedgerTag(identity)).Active)
		require.Nil(t, account.TempUnschedulableUntil)
	}
}

func TestGatewayPoolLocalDeadlineKeepsClearedKnownGatewaysAfterNewTouchAndRestart(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	svc, base, identity := cooldownResetService(t, now)
	account, err := base.GetByID(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, svc.clearGatewayPoolCooldown(ctx, account, identity))
	require.True(t, svc.codexCookies.beginGatewayPoolAttempt(identity, "recent", time.Hour))
	deadline := svc.codexCookies.gatewayPoolLocalResumeAt(identity, account)
	// Two known, N defaults to 50: both must finish (one has just been touched).
	require.True(t, deadline.After(now))
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	require.False(t, svc.codexCookies.gatewayPoolLocalResumeAt(identity, account).After(time.Now()),
		"the other cleared known gateway still counts")
	fresh, err := base.GetByID(ctx, 1)
	require.NoError(t, err)
	fresh.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	restarted := &OpenAIGatewayService{}
	restarted.codexCookies.gatewayPoolHydrateUsed(fresh, identity)
	require.True(t, restarted.codexCookies.beginGatewayPoolAttempt(identity, "recent", time.Hour))
	require.False(t, restarted.codexCookies.gatewayPoolLocalResumeAt(identity, fresh).After(time.Now()))
	view, _ := restarted.gatewayPoolDisplaySnapshot(fresh, identity, nil)
	display := restarted.codexCookies.gatewayPoolCooldownEstimate(identity, fresh, view, time.Now())
	require.Equal(t, restarted.codexCookies.gatewayPoolLocalResumeAt(identity, fresh), display.EligibleAt)
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
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, authUntil, *fresh.TempUnschedulableUntil)
	require.Equal(t, "oauth 401", fresh.TempUnschedulableReason)
	require.False(t, readGatewayPoolRest(fresh, gatewayPoolLedgerTag(identity)).Active)
}
