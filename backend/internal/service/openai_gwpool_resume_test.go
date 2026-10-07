package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func gatewayPoolReadyList(count int) []gwpoolFakeGateway {
	var gateways []gwpoolFakeGateway
	for n := 1; n <= count; n++ {
		gateways = append(gateways, gwpoolFakeGateway{Name: fmt.Sprintf("unified-%d", n), PairReady: true})
	}
	return gateways
}

func gatewayPoolRestDue(t *testing.T, svc *OpenAIGatewayService, repo gatewayRotationRepo, id int64, identity string) {
	t.Helper()
	fresh, err := repo.GetByID(context.Background(), id)
	require.NoError(t, err)
	state := readGatewayPoolRest(fresh, gatewayPoolLedgerTag(identity))
	state.NextCheck = time.Now().Add(-time.Second)
	state.advance(time.Now().UTC())
	require.NoError(t, repo.UpdateExtra(context.Background(), id, map[string]any{gatewayPoolRestStateKey: state}))
	svc.codexCookies.poolRestState.Store(state.Tag, state)
	past := time.Now().Add(-time.Second)
	fresh.TempUnschedulableUntil = &past
}

func TestGatewayPoolResumeThresholdIsIndependentAndDefaultsTo50(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 10
	require.Equal(t, 50, account.gatewayPoolResumeGateways())
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 60
	require.Equal(t, 60, account.gatewayPoolResumeGateways())
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 5
	require.Equal(t, 10, account.gatewayPoolResumeGateways(), "legacy malformed values cannot resume below stop")
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 80
	delete(account.Extra, openAIGatewayPoolResumeGatewaysExtraKey)
	require.Equal(t, 80, account.gatewayPoolResumeGateways(), "default is floored by the stop threshold")
	require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, map[string]any{
		openAIGatewayPoolRotationMinGatewaysExtraKey: 10, openAIGatewayPoolResumeGatewaysExtraKey: 5,
	}))
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, map[string]any{
		openAIGatewayPoolRotationMinGatewaysExtraKey: 10, openAIGatewayPoolResumeGatewaysExtraKey: 50,
	}))
}

func TestGatewayPoolResumeWaitsFor50AfterStoppingBelow10(t *testing.T) {
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 10
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.configure(account)
	fake.listGateways = gatewayPoolReadyList(9)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	require.True(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account))
	svc.restGatewayPoolAccount(context.Background(), account, gwpoolTestIdentity, 7)
	for _, count := range []int{10, 49, 50} {
		gatewayPoolRestDue(t, svc, repo, 1, gwpoolTestIdentity)
		fake.listGateways = gatewayPoolReadyList(count)
		// Restart before each recheck: passing may not depend on in-memory rest.
		svc = &OpenAIGatewayService{accountRepo: repo}
		allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
		require.NoError(t, err)
		require.Equal(t, count == 50, allowed, "count=%d", count)
		fresh, err := repo.GetByID(context.Background(), 1)
		require.NoError(t, err)
		require.Equal(t, count != 50, readGatewayPoolRest(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity)).Active)
	}
	require.Zero(t, fake.hits.Load(), "recovery only lists existing candidates, never fetches/probes")
}

func TestGatewayPoolResumeCloneTombstoneAndLateRestDoNotResurrect(t *testing.T) {
	account, clone := rotationAccount(1, 7), rotationAccount(2, 7)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account, clone)
	fake.listGateways = gatewayPoolReadyList(50)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account, *clone}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	at := time.Now().UTC().Add(-time.Minute)
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, at, at.Add(time.Second)))
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), clone, true)
	require.NoError(t, err)
	require.True(t, allowed)
	restarted := &OpenAIGatewayService{accountRepo: repo}
	allowed, err = restarted.gatewayPoolResumeAllowed(context.Background(), account, false)
	require.NoError(t, err)
	require.True(t, allowed, "inactive peer is newer than the old owner's active snapshot")
	require.NoError(t, restarted.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, at, time.Now().Add(time.Hour)))
	allowed, err = restarted.gatewayPoolResumeAllowed(context.Background(), account, false)
	require.NoError(t, err)
	require.True(t, allowed, "delayed earlier shortage cannot overwrite the inactive tombstone")
}

func TestGatewayPoolResumeUnreadableOrChangingSupplyStaysResting(t *testing.T) {
	for _, mode := range []string{"list-error", "generation", "cooling", "duplicate", "not-ready", "pending"} {
		t.Run(mode, func(t *testing.T) {
			account := rotationAccount(1, 7)
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.configure(account)
			fake.listGateways = gatewayPoolReadyList(50)
			repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
			svc := &OpenAIGatewayService{accountRepo: repo}
			at := time.Now().Add(-time.Minute)
			require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, at, at))
			switch mode {
			case "list-error":
				fake.listStatus = 503
			case "generation":
				fake.onList = func() { done := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity); done() }
			case "cooling":
				svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-1")
			case "duplicate":
				fake.listGateways[49] = fake.listGateways[0]
			case "not-ready":
				fake.listGateways[49].PairReady = false
			case "pending":
				finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
				defer finish()
			}
			allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
			require.NoError(t, err)
			require.False(t, allowed)
		})
	}
}

func TestGatewayPoolResumeDoesNotCountCachedOrSpareOutsideFreshReadyList(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {
			account := rotationAccount(1, 7)
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.configure(account)
			fake.listGateways = gatewayPoolReadyList(49)
			repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
			svc := &OpenAIGatewayService{accountRepo: repo}
			pair := openAIGatewayPoolPair{
				gateway: "unified-142", cookie: gwpoolTestPairCookie(t, "unified-142"),
				until: time.Now().Add(time.Minute),
			}
			if cached {
				svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
				svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, pair.gateway)
			} else {
				svc.codexCookies.poolSpare.Store(gwpoolTestIdentity, &gatewayPoolTicketBatch{pairs: []openAIGatewayPoolPair{pair}})
			}
			at := time.Now().Add(-time.Minute)
			require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, at, at))
			allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
			require.NoError(t, err)
			require.False(t, allowed, "only this fresh Ready list contributes to the resume threshold")
		})
	}
}

func TestGatewayPoolResumeDoesNotClearAuthBlockAndEarlyCannotBypass(t *testing.T) {
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolEarlyEnabledKey] = true
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account)
	fake.listGateways = gatewayPoolReadyList(50)
	until := time.Now().Add(time.Hour)
	account.TempUnschedulableUntil, account.TempUnschedulableReason = &until, "auth"
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	at := time.Now().Add(-time.Minute)
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, at, at))
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.NoError(t, err)
	require.False(t, allowed)
	require.Error(t, svc.claimGatewayPoolEarly(context.Background(), account, gwpoolTestIdentity, time.Now()))
	fresh, err := repo.GetByID(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, until, *fresh.TempUnschedulableUntil)
	require.Equal(t, "auth", fresh.TempUnschedulableReason)
	require.Nil(t, fresh.Extra[openAIGatewayPoolEarlyStateKey], "blocked early attempt spends no reservation")
}

func TestGatewayPoolResumeLastMileBlocksEvenWithVerifiedPair(t *testing.T) {
	account := rotationAccount(1, 7)
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.configure(account)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}
	gwpoolEchoSeedVerified(t, svc, account)
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, time.Now(), time.Now().Add(time.Minute)))
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	response, sent, err := svc.gatewayPoolObservedRoundTrip(req, "", account, true)
	require.Error(t, err)
	require.Nil(t, response)
	require.True(t, sent.IsZero())
	require.Empty(t, upstream.sentBodies)
}

type gatewayRestWriteFails struct {
	gatewayRotationRepo
	fail            bool
	atomicFail      bool
	beforeWriteRead func()
}

func (r *gatewayRestWriteFails) GetByID(ctx context.Context, id int64) (*Account, error) {
	if r.beforeWriteRead != nil {
		callback := r.beforeWriteRead
		r.beforeWriteRead = nil
		callback()
	}
	return r.gatewayRotationRepo.GetByID(ctx, id)
}

func (r *gatewayRestWriteFails) SetGatewayPoolRest(ctx context.Context, id int64, until time.Time, reason string, patch map[string]any) error {
	if r.atomicFail {
		return errors.New("offline atomic write failed")
	}
	if err := r.gatewayRotationRepo.UpdateExtra(ctx, id, patch); err != nil {
		return err
	}
	return r.SetTempUnschedulable(ctx, id, until, reason)
}

func (r *gatewayRestWriteFails) UpdateExtra(ctx context.Context, id int64, patch map[string]any) error {
	if r.fail {
		return errors.New("offline write failed")
	}
	return r.gatewayRotationRepo.UpdateExtra(ctx, id, patch)
}

func TestGatewayPoolResumeFirstRestPersistsTogetherWithTemporaryBlock(t *testing.T) {
	account := rotationAccount(1, 7)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account)
	fake.listGateways = gatewayPoolReadyList(49)
	repo := &gatewayRestWriteFails{
		gatewayRotationRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}},
		fail:                true, // the old independent UpdateExtra path fails
	}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.restGatewayPoolAccount(context.Background(), account, gwpoolTestIdentity, 7)
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	require.NotNil(t, fresh.TempUnschedulableUntil)
	require.True(t, readGatewayPoolRest(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity)).Active)
	gatewayPoolRestDue(t, svc, repo.gatewayRotationRepo, account.ID, gwpoolTestIdentity)
	restarted := &OpenAIGatewayService{accountRepo: repo}
	allowed, err := restarted.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestGatewayPoolResumeFailedAtomicRestDoesNotLeavePartialTemporaryBlock(t *testing.T) {
	account := rotationAccount(1, 7)
	repo := &gatewayRestWriteFails{
		gatewayRotationRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}},
		fail:                true, atomicFail: true,
	}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.restGatewayPoolAccount(context.Background(), account, gwpoolTestIdentity, 7)
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	require.Nil(t, fresh.TempUnschedulableUntil, "a block must not be acknowledged without the durable resume gate")
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, false)
	require.NoError(t, err)
	require.False(t, allowed, "the failed write still blocks this process")
}

func TestGatewayPoolResumeGenerationChangesAtPersistenceCannotPublishRecovery(t *testing.T) {
	account := rotationAccount(1, 7)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account)
	fake.listGateways = gatewayPoolReadyList(50)
	repo := &gatewayRestWriteFails{gatewayRotationRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	at := time.Now().Add(-time.Minute)
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, at, at))
	fake.onList = func() {
		repo.beforeWriteRead = func() {
			finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
			finish()
		}
	}
	allowed, _ := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.False(t, allowed)
	restarted := &OpenAIGatewayService{accountRepo: repo}
	allowed, err := restarted.gatewayPoolResumeAllowed(context.Background(), account, false)
	require.NoError(t, err)
	require.False(t, allowed, "a stale listing cannot durably publish recovery")
}

func TestGatewayPoolResumeWriteFailureCannotPublishRecovery(t *testing.T) {
	account := rotationAccount(1, 7)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account)
	fake.listGateways = gatewayPoolReadyList(50)
	repo := &gatewayRestWriteFails{gatewayRotationRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	at := time.Now().Add(-time.Minute)
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, at, at))
	repo.fail = true
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.Error(t, err)
	require.False(t, allowed)
	allowed, err = svc.gatewayPoolResumeAllowed(context.Background(), account, false)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestGatewayPoolResumeStrongBindingCannotBypassRestOrSwitchAccount(t *testing.T) {
	group := int64(7)
	account := rotationAccount(1, group)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{},
		rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true")}
	calls, releases := 0, 0
	svc.openaiScheduler = gatewayRoundSelectionRace{selectFn: func(context.Context, OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
		calls++
		return &AccountSelectionResult{Account: account, ReleaseFunc: func() { releases++ }}, OpenAIAccountScheduleDecision{}, nil
	}}
	require.NoError(t, svc.enterGatewayPoolRest(context.Background(), account, gwpoolTestIdentity, time.Now(), time.Now().Add(time.Minute)))
	selection, _, err := svc.SelectAccountWithSchedulerForCapability(context.Background(), &group, "bound-response", "", "gpt-6-astra", nil,
		OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, false)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, selection)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, releases)
}
