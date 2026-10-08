package service

import (
	"context"
	"errors"
	"fmt"
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

func (r gatewayRotationRepo) FindByExtraField(_ context.Context, key string, value any) ([]Account, error) {
	var found []Account
	for _, account := range r.accounts {
		if account.Extra[key] == value {
			found = append(found, account)
		}
	}
	return found, nil
}

func (r gatewayRotationRepo) UpdateExtra(_ context.Context, id int64, patch map[string]any) error {
	for i := range r.accounts {
		if r.accounts[i].ID == id {
			for key, value := range patch {
				r.accounts[i].Extra[key] = value
			}
		}
	}
	return nil
}

func (r gatewayRotationRepo) SetTempUnschedulable(_ context.Context, id int64, until time.Time, reason string) error {
	for i := range r.accounts {
		if r.accounts[i].ID == id && (r.accounts[i].TempUnschedulableUntil == nil || r.accounts[i].TempUnschedulableUntil.Before(until)) {
			r.accounts[i].TempUnschedulableUntil = &until
			r.accounts[i].TempUnschedulableReason = reason
		}
	}
	return nil
}

func (r gatewayRotationRepo) ClearTempUnschedulable(_ context.Context, id int64) error {
	for i := range r.accounts {
		if r.accounts[i].ID == id {
			r.accounts[i].TempUnschedulableUntil = nil
			r.accounts[i].TempUnschedulableReason = ""
		}
	}
	return nil
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
		name         string
		list         []gwpoolFakeGateway
		status       int
		rotate       bool
		localCooling bool
	}{
		{"historical pool use does not block candidates", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true, UsedByYou: true}}, 0, false, false},
		{"candidate still available", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true, UsedByYou: true}, {Name: "unified-143", PairReady: true}}, 0, false, false},
		{"successful empty supply is zero", nil, 0, true, false},
		{"list failure is not exhaustion", nil, http.StatusBadGateway, false, false},
		{"local cooling independent of pool", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}, 0, true, true},
		{"nonready listing has zero candidates", []gwpoolFakeGateway{{Name: "unified-142", PairReady: false, UsedByYou: true}}, 0, true, false},
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
		errOpenAIGatewayPoolWarmUnverified,
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
			require.Equal(t, gatewayPoolBusinessRetryStatus(status), failure.RetryableOnSameAccount)
		}
	}
}

func TestGatewayPoolRotationPreservesConfirmedTransient429SameAccountRetry(t *testing.T) {
	group := int64(7)
	account, other := rotationAccount(1, group), rotationAccount(2, group)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account, *other}}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	body := []byte(`{"detail":"Rate limit exceeded"}`)
	failure := svc.newOpenAIAccountFailoverError(account, http.StatusTooManyRequests, nil, body, "Rate limit exceeded", false, false)
	require.True(t, failure.RetryableOnSameAccount)
	ctx := svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure)
	require.True(t, failure.RetryableOnSameAccount, "pool rotation may forbid switching, not erase a confirmed transient retry")
	require.False(t, failure.ShouldRetryNextAccount(), "429 never authorizes a gateway-pool account switch")
	omit, allowed, err := gatewayPoolRotationExclusions(ctx, repo, &group, nil)
	require.NoError(t, err)
	require.Contains(t, omit, other.ID)
	require.Equal(t, map[int64]struct{}{account.ID: {}}, allowed)
	require.False(t, gatewayPoolRotationRecheck(ctx, repo, allowed, &AccountSelectionResult{Account: other}))
	require.True(t, failure.SameAccountRetryOnly)
	require.False(t, GatewayPoolAccountRotationActive(ctx), "waiting must not start a shortage rotation")
	ctx = svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, failure)
	require.True(t, failure.SameAccountRetryOnly, "preparing an already-classified retry is idempotent")
	firstDeadline := failure.SameAccountRetryDeadline
	// A different request can succeed and reset the account-wide retry clock;
	// this request still retains its original bounded deadline.
	svc.openaiOAuth429RetryStartedAt.Delete(account.ID)
	next := svc.newOpenAIAccountFailoverError(account, http.StatusTooManyRequests, nil, body, "Rate limit exceeded", false, false)
	ctx = svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, next)
	require.Equal(t, firstDeadline, next.SameAccountRetryDeadline)
	require.True(t, next.SameAccountRetryOnly)
	expired := context.WithValue(ctx, gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{
		accountID: account.ID, groupID: group, deadline: time.Now().Add(-time.Second),
	})
	next = svc.newOpenAIAccountFailoverError(account, http.StatusTooManyRequests, nil, body, "Rate limit exceeded", false, false)
	svc.PrepareGatewayPoolAccountRotation(expired, &group, account, next)
	require.False(t, next.RetryableOnSameAccount)
	require.False(t, next.ShouldRetryNextAccount())
}

func TestGatewayPoolRotationDoesNotRetryQuotaOrUnclassified429(t *testing.T) {
	group := int64(7)
	account := rotationAccount(1, group)
	svc := rotationService(account)
	for _, failure := range []*UpstreamFailoverError{
		svc.newOpenAIAccountFailoverError(account, http.StatusTooManyRequests, nil,
			[]byte(`{"error":{"type":"usage_limit_reached","resets_in_seconds":3600}}`), "limit", false, false),
		{StatusCode: http.StatusTooManyRequests, RetryableOnSameAccount: true},
		{StatusCode: http.StatusTooManyRequests, RetryableOnSameAccount: true, SameAccountRetryDeadline: time.Now().Add(-time.Second)},
		{StatusCode: http.StatusTooManyRequests, RetryableOnSameAccount: true, SameAccountRetryDeadline: time.Now().Add(time.Minute),
			NextAccountAction: NextAccountStop},
	} {
		svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure)
		require.False(t, failure.RetryableOnSameAccount)
		require.False(t, failure.SameAccountRetryOnly)
		require.False(t, failure.ShouldRetryNextAccount())
	}
}

func TestGatewayPoolRetryDeadlinePreventsLateBusinessDispatch(t *testing.T) {
	for _, expiresDuringFetch := range []bool{false, true} {
		t.Run(fmt.Sprintf("during_fetch=%t", expiresDuringFetch), func(t *testing.T) {
			account := gwpoolTestAccount(1)
			account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = false
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			fake.configure(account)
			upstream := &gwpoolEchoUpstream{}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			deadline := time.Now().Add(-time.Second)
			if expiresDuringFetch {
				deadline = time.Now().Add(30 * time.Millisecond)
				fake.onCookie = func() {
					delay := time.Until(deadline)
					if delay > 0 {
						time.Sleep(delay + time.Millisecond)
					}
				}
			}
			ctx := context.WithValue(context.Background(), gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{
				accountID: account.ID, deadline: deadline,
				failure: &UpstreamFailoverError{StatusCode: 429, ResponseBody: []byte(`{"detail":"Rate limit exceeded"}`)},
			})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, nil)
			require.NoError(t, err)
			response, err := svc.doOpenAIUpstream(req, "", account)
			require.Nil(t, response)
			var failure *UpstreamFailoverError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, 429, failure.StatusCode)
			require.False(t, failure.RetryableOnSameAccount)
			require.Empty(t, upstream.sentBodies)
			require.NoError(t, ctx.Err(), "dispatch admission must not impose a response-stream deadline")
		})
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
	require.Equal(t, map[int64]struct{}{2: {}, 3: {}}, allowed, "rotation is mandatory even with legacy false")
	for _, id := range []int64{1, 4, 5, 6} {
		require.Contains(t, omit, id)
	}
	for _, bad := range []*int64{nil, &other.GroupIDs[0]} {
		_, _, err = gatewayPoolRotationExclusions(ctx, repo, bad, nil)
		require.Error(t, err)
	}
	// Simulate changing an opt-in after the listing but before final selection.
	repo.accounts[1].Extra[openAIGatewayPoolExtraKey] = false
	require.False(t, gatewayPoolRotationRecheck(ctx, repo, allowed, &AccountSelectionResult{Account: eligible}))
	// A stale enabled source cannot initiate rotation after its DB setting was disabled.
	failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
	stale := *source
	stale.Extra = map[string]any{openAIGatewayPoolExtraKey: true, openAIGatewayPoolRotationExtraKey: true}
	repo.accounts[0].Extra[openAIGatewayPoolExtraKey] = false
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, &stale, failure)
	require.False(t, failure.ShouldRetryNextAccount())
}

func TestGatewayPoolRotationUsesExistingSchedulerAndDoesNotRevisitAccounts(t *testing.T) {
	group := int64(7)
	source := rotationAccount(1, group)
	target := rotationAccount(2, group)
	target.Credentials = map[string]any{"chatgpt_account_id": "rotation-target", "chatgpt_user_id": "user"}
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

func TestGatewayPoolRotationDoesNotConfuseDeliveredWithTried(t *testing.T) {
	for _, state := range []string{"in-flight", "live-unverified"} {
		t.Run(state, func(t *testing.T) {
			fake := newGwpoolFakePool(t, "offline-cookie", 150)
			fake.listGateways = []gwpoolFakeGateway{{Name: "unified-71", PairReady: true, UsedByYou: true}}
			account := rotationAccount(40, 2)
			fake.configure(account)
			svc := rotationService(account)
			pair := openAIGatewayPoolPair{cookie: "offline-cookie", gateway: "unified-71", version: "unused",
				until: time.Now().Add(150 * time.Second), since: time.Now()}
			if state == "in-flight" {
				finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
				defer finish()
			} else {
				svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
				// Acquisition reserves the local attempt before the probe starts.
				svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, pair.gateway)
			}
			failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
			group := int64(2)
			svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure)
			require.False(t, failure.ShouldRetryNextAccount(), "delivered/local-reserved does not prove actually attempted")
		})
	}
}

func TestGatewayPoolRotationRechecksInventoryChangedDuringListing(t *testing.T) {
	fake := newGwpoolFakePool(t, "offline-cookie", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-71", PairReady: true, UsedByYou: true}}
	account := rotationAccount(40, 2)
	fake.configure(account)
	svc := rotationService(account)
	fake.onList = func() {
		// An entire fetch/probe may complete while the remote listing is in
		// flight; the listing predates that work even if active is back to zero.
		finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
		finish()
	}
	require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account))
}

func TestGatewayPoolRotationDoesNotSkipConcurrentWarmProbe(t *testing.T) {
	fake := newGwpoolFakePool(t, "offline-cookie", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-71", PairReady: true, UsedByYou: true}}
	account := rotationAccount(40, 2)
	fake.configure(account)
	svc := rotationService(account)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _, _ = svc.codexCookies.gatewayPoolWarmVerdict(context.Background(), account, gwpoolTestIdentity,
			OpenAIGatewayPoolApplied{Gateway: "unified-71", Version: "inflight"}, "offline-cookie", 1,
			func(context.Context, string, string) (int, string, error) {
				select {
				case <-started:
				default:
					close(started)
				}
				<-release
				return http.StatusOK, "", nil
			})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("probe did not start")
	}
	blocked := !svc.gatewayPoolNoRemainingRoutes(context.Background(), account)
	close(release)
	<-done
	require.True(t, blocked)
	require.Zero(t, fake.listHits.Load(), "local pending inventory should stop before remote listing")
}
