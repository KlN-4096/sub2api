package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

type gatewayRotationRepo struct{ schedulerTestOpenAIAccountRepo }

func (r gatewayRotationRepo) ListByPlatform(context.Context, string) ([]Account, error) {
	return r.accounts, nil
}

func rotationAccount(id, group int64) *Account {
	account := gwpoolTestAccount(id)
	account.GroupIDs = []int64{group}
	account.Status, account.Schedulable, account.Concurrency = StatusActive, true, 1
	account.Extra[openAIGatewayPoolRotationExtraKey] = true
	return account
}

func rotationService(account *Account) *OpenAIGatewayService {
	return &OpenAIGatewayService{accountRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}}
}

func TestGatewayPoolRotationOnlyAfterFreshCompleteExhaustion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		list   []gwpoolFakeGateway
		status int
		rotate bool
		localCooling bool
	}{
		{"all candidates cooling", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true, UsedByYou: true}}, 0, true, false},
		{"candidate still available", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true, UsedByYou: true}, {Name: "unified-143", PairReady: true}}, 0, false, false},
		{"empty supply is not proven exhaustion", nil, 0, false, false},
		{"list failure is not exhaustion", nil, http.StatusBadGateway, false, false},
		{"local cooling independent of pool", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}, 0, true, true},
		{"nonready listing is not exhaustion", []gwpoolFakeGateway{{Name: "unified-142", PairReady: false, UsedByYou: true}}, 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, "offline-cookie", 150)
			fake.listGateways, fake.listStatus = tc.list, tc.status
			account := rotationAccount(1, 7)
			fake.configure(account)
			svc := rotationService(account)
			if tc.localCooling {
				svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-142")
			}
			failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
			group := int64(7)
			ctx := svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure)
			require.Equal(t, tc.rotate, failure.ShouldRetryNextAccount())
			require.Equal(t, tc.rotate, GatewayPoolAccountRotationActive(ctx))
			if tc.rotate {
				remaining, _ := svc.codexCookies.gatewayPoolBackoffFor(gwpoolTestIdentity)
				require.Positive(t, remaining)
				require.LessOrEqual(t, remaining, openAIGatewayPoolDefaultBackoff)
			}
			require.True(t, account.Schedulable, "rest must not disable the account")
			require.Zero(t, fake.hits.Load(), "exhaustion confirmation must not mint or probe")
		})
	}
}

func TestGatewayPoolRotationFreshlyEnabledSourceStopsUnrelatedFailure(t *testing.T) {
	group := int64(7)
	fresh := rotationAccount(1, group)
	stale := *fresh
	stale.Extra = map[string]any{} // scheduler snapshot predates both switches
	svc := rotationService(fresh)
	failure := &UpstreamFailoverError{StatusCode: http.StatusTooManyRequests}
	svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, &stale, failure)
	require.False(t, failure.ShouldRetryNextAccount(), "current opt-in must apply even when the old snapshot lacked both keys")
}

func TestGatewayPoolRotationNeverSwitchesOnAnyOtherError(t *testing.T) {
	for _, err := range []error{
		errOpenAIGatewayPoolRouteDegraded, errOpenAIGatewayPoolWarmExhausted,
		errOpenAIGatewayPoolWarmUnverified, errOpenAIGatewayPoolWarmNoModel,
		context.DeadlineExceeded, errors.New("authentication failed"), gwpool.ErrNoSlot,
		&gwpool.PoolError{Code: gwpool.CodeNoLivePair}, &gwpool.PoolError{Code: gwpool.CodeNoGateway},
		&gwpool.PoolError{Code: gwpool.CodeRateLimited}, &gwpool.PoolError{Code: gwpool.CodeConsumerRejected},
	} {
		require.False(t, gatewayPoolRotationFailure(err), err.Error())
	}
	require.True(t, gatewayPoolRotationFailure(errGatewayPoolWarmAttemptsFinished))
	require.True(t, gatewayPoolRotationFailure(&gwpool.PoolError{Code: gwpool.CodeAllCooling}))
	account := rotationAccount(1, 7)
	svc := rotationService(account)
	group := int64(7)
	for _, status := range []int{400, 401, 403, 429, 500, 502, 503, 504} {
		for _, alreadyRotating := range []bool{false, true} {
			ctx := context.Background()
			if alreadyRotating {
				ctx = context.WithValue(ctx, gatewayPoolRotationKey{}, &gatewayPoolRotation{groupID: group})
			}
			failure := &UpstreamFailoverError{StatusCode: status, RetryableOnSameAccount: true}
			svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, failure)
			require.False(t, failure.ShouldRetryNextAccount(), "status %d", status)
			require.False(t, failure.RetryableOnSameAccount)
		}
	}
}

func TestGatewayPoolRotationFreshOptInsAndGroupIsolation(t *testing.T) {
	group := int64(7)
	source := rotationAccount(1, group)
	eligible := rotationAccount(2, group)
	off := rotationAccount(3, group)
	off.Extra[openAIGatewayPoolRotationExtraKey] = false
	noPool := rotationAccount(4, group)
	noPool.Extra[openAIGatewayPoolExtraKey] = false
	other := rotationAccount(5, group+1)
	disabled := rotationAccount(6, group)
	disabled.Schedulable = false
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*source, *eligible, *off, *noPool, *other, *disabled}}}
	ctx := context.WithValue(context.Background(), gatewayPoolRotationKey{}, &gatewayPoolRotation{
		groupID: group, attempted: map[int64]struct{}{1: {}},
	})
	omit, allowed, err := gatewayPoolRotationExclusions(ctx, repo, &group, nil)
	require.NoError(t, err)
	require.Equal(t, map[int64]struct{}{2: {}}, allowed)
	for _, id := range []int64{1, 3, 4, 5, 6} {
		require.Contains(t, omit, id)
	}
	for _, bad := range []*int64{nil, &other.GroupIDs[0]} {
		_, _, err = gatewayPoolRotationExclusions(ctx, repo, bad, nil)
		require.Error(t, err)
	}
	// Simulate changing an opt-in after the listing but before final selection.
	repo.accounts[1].Extra[openAIGatewayPoolRotationExtraKey] = false
	require.False(t, gatewayPoolRotationRecheck(ctx, repo, allowed, &AccountSelectionResult{Account: eligible}))
	// A stale enabled source cannot initiate rotation after its DB setting was disabled.
	failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
	stale := *source
	stale.Extra = map[string]any{openAIGatewayPoolExtraKey: true, openAIGatewayPoolRotationExtraKey: true}
	repo.accounts[0].Extra[openAIGatewayPoolRotationExtraKey] = false
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, &stale, failure)
	require.False(t, failure.ShouldRetryNextAccount())
}

func TestGatewayPoolRotationUsesExistingSchedulerAndDoesNotRevisitAccounts(t *testing.T) {
	group := int64(7)
	source := rotationAccount(1, group)
	target := rotationAccount(2, group)
	off := rotationAccount(3, group)
	off.Extra[openAIGatewayPoolRotationExtraKey] = false
	source.Priority, target.Priority, off.Priority = 0, 2, 1
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*source, *target, *off}}}
	svc := &OpenAIGatewayService{accountRepo: repo, cache: &schedulerTestGatewayCache{}, cfg: &config.Config{},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	ctx := context.WithValue(context.Background(), gatewayPoolRotationKey{}, &gatewayPoolRotation{
		groupID: group, attempted: map[int64]struct{}{1: {}},
	})
	selection, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &group, "", "", "gpt-6-astra", nil,
		OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
	require.NoError(t, err)
	require.Equal(t, int64(2), selection.Account.ID)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
	ctx = context.WithValue(ctx, gatewayPoolRotationKey{}, &gatewayPoolRotation{groupID: group, attempted: map[int64]struct{}{1: {}, 2: {}}})
	_, _, err = svc.SelectAccountWithSchedulerForCapability(ctx, &group, "", "", "gpt-6-astra", nil,
		OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
}

func TestGatewayPoolRotationCanceledOrNoGroupDoesNotReadPool(t *testing.T) {
	account := rotationAccount(1, 7)
	svc := rotationService(account)
	failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
	svc.PrepareGatewayPoolAccountRotation(context.Background(), nil, account, failure)
	require.False(t, failure.ShouldRetryNextAccount())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	require.False(t, svc.gatewayPoolNoRemainingRoutes(ctx, account))
}
