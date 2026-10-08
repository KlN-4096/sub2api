package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func ticketWaitFixture(t *testing.T) (*OpenAIGatewayService, *gatewayRuntimeRepo, *gwpoolFakePool, *Account, *http.Request, *gatewayPoolWaitState) {
	t.Helper()
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := rotationAccount(1, 7)
	fake.configure(account)
	account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = true
	account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = 120
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = false
	account.Credentials["access_token"] = "offline-token"
	svc, repo := gatewayRuntimeService(account)
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	request.Header.Set(openAICodexTurnStateHeader, "business-state")
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
	ctx = svc.gatewayPoolWaitContext(ctx, account)
	state := gatewayPoolWaitFrom(ctx)
	require.NotNil(t, state)
	return svc, repo, fake, account, request.WithContext(ctx), state
}

func TestGatewayPoolWaitBeforeBusinessRetriesOnlyTicketAcquisition(t *testing.T) {
	svc, _, fake, account, request, state := ticketWaitFixture(t)
	fake.refuseStatus, fake.refuseCode, fake.refuseRetryAfter = 503, gwpool.CodeNoLivePair, 1
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}
	sleeps := 0
	state.sleep = func(_ context.Context, delay time.Duration) error {
		sleeps++
		fake.refuseStatus = 0
		return nil
	}
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "probe-state"}, {status: http.StatusOK}, {status: http.StatusOK},
	}}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.NoError(t, response.Body.Close())
	require.Equal(t, 1, sleeps)
	require.EqualValues(t, 3, fake.hits.Load(), "rejected targeted fetch and bare fallback, then the successful retry")
	require.Len(t, upstream.sentBodies, 3, "A/B then one business request")
	require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[2], "the business request must not be retried")
}

func TestGatewayPoolWaitStillVerifiesActualTicketBeforeBusiness(t *testing.T) {
	svc, repo, fake, account, request, state := ticketWaitFixture(t)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	account.Credentials["access_token"] = "offline-token"
	repo.account = *account
	fake.refuseStatus, fake.refuseCode = 503, gwpool.CodeNoLivePair
	state.sleep = func(context.Context, time.Duration) error {
		fake.refuseStatus = 0
		return nil
	}
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "probe-state"}, {status: http.StatusOK}, {status: http.StatusOK},
	}}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Len(t, upstream.sentBodies, 3, "two state-echo probes then exactly one unchanged business request")
}

func TestGatewayPoolWaitRecoversProbeWithoutReplayingBusiness(t *testing.T) {
	svc, repo, _, account, request, state := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	account.Credentials["access_token"] = "offline-token"
	account.Extra[openAIGatewayPoolRecoveryExtraKey] = 1
	repo.account = *account
	sleeps := 0
	state.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK}, // A has no state: recoverable, not degraded.
		{status: http.StatusOK, minted: "probe-state"}, {status: http.StatusOK},
		{status: http.StatusOK}, // business, sent only once
	}}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, 1, sleeps)
	require.Len(t, upstream.sentBodies, 4)
	progress := svc.GatewayPoolProgress([]int64{account.ID})[account.ID]
	require.Equal(t, 1, progress.Attempt, "retrying the same ticket does not invent a second ticket; work budget is separate")
	require.Zero(t, progress.Rejected, "unknown is not a quality verdict")
}

func TestGatewayPoolWaitProbeFailureUsesSharedRecoveryLimitAndStopsAuthentication(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			svc, repo, _, account, request, state := ticketWaitFixture(t)
			account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
			account.Extra[openAIGatewayPoolRecoveryExtraKey] = 1
			repo.account = *account
			sleeps, shots := 0, 0
			state.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
			err := svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel,
				func(context.Context, string, string) (int, string, error) {
					shots++
					return status, "", nil
				})
			require.ErrorIs(t, err, errOpenAIGatewayPoolWarmUnverified)
			require.Equal(t, 1, shots)
			require.Zero(t, sleeps)
		})
	}
}

func TestGatewayPoolWaitRejectsNonShortageErrors(t *testing.T) {
	for _, code := range []string{gwpool.CodeConsumerRejected, gwpool.CodeUpstreamRejected, gwpool.CodeBadRequest,
		gwpool.CodeRateLimited, gwpool.CodeNoExit, gwpool.CodeMintFailed, "unknown"} {
		t.Run(code, func(t *testing.T) {
			svc, _, fake, account, request, state := ticketWaitFixture(t)
			fake.refuseStatus, fake.refuseCode = 503, code
			sleeps := 0
			state.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
			_, err := svc.attachGatewayPoolRouteWithWait(request.Context(), account, gwpoolTestURL, http.Header{})
			require.Error(t, err)
			require.Zero(t, sleeps, "ErrNoSlot is not itself a retry permission")
			require.EqualValues(t, 1, fake.hits.Load())
		})
	}
	for _, err := range []error{context.DeadlineExceeded, errOpenAIGatewayPoolRouteDegraded, errors.New("transport")} {
		_, retry := gatewayPoolRetryableShortage(err)
		require.False(t, retry)
	}
}

func TestGatewayPoolWaitHonorsCallerDeadlineCancellationAndFreshSettings(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel", "pool-disabled", "account-disabled", "identity-changed"} {
		t.Run(mode, func(t *testing.T) {
			svc, repo, fake, account, request, state := ticketWaitFixture(t)
			fake.refuseStatus, fake.refuseCode, fake.refuseRetryAfter = 503, gwpool.CodeNoLivePair, 30
			ctx, cancel := context.WithTimeout(request.Context(), 30*time.Millisecond)
			defer cancel()
			sleeps := 0
			state.sleep = func(ctx context.Context, delay time.Duration) error {
				sleeps++
				if mode == "deadline" {
					return gatewayPoolSleep(ctx, delay)
				}
				repo.mu.Lock()
				defer repo.mu.Unlock()
				switch mode {
				case "cancel":
					return context.Canceled
				case "pool-disabled":
					repo.account.Extra[openAIGatewayPoolExtraKey] = false
				case "account-disabled":
					repo.account.Schedulable = false
				case "identity-changed":
					repo.account.Credentials = map[string]any{"chatgpt_account_id": "different-account", "chatgpt_user_id": "user"}
				}
				return nil
			}
			_, err := svc.attachGatewayPoolRouteWithWait(ctx, account, gwpoolTestURL, http.Header{})
			require.Error(t, err)
			if mode == "deadline" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				_, err = svc.attachGatewayPoolRouteWithWait(ctx, account, gwpoolTestURL, http.Header{})
				require.ErrorIs(t, err, context.DeadlineExceeded, "reentry cannot refresh caller deadline")
				require.Equal(t, 1, sleeps)
				require.EqualValues(t, 1, fake.hits.Load())
			} else {
				require.Equal(t, 1, sleeps)
				require.EqualValues(t, 1, fake.hits.Load(), "no retry after cancellation/config/identity change")
			}
		})
	}
}

func TestGatewayPoolWaitReadsFreshSettingsAndIgnoresRetiredBudget(t *testing.T) {
	account := rotationAccount(1, 7)
	for _, invalid := range []any{0, -1, 3601, 1.5, "60", true, nil} {
		account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = true
		account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = invalid
		require.True(t, account.GatewayPoolLongWaitEnabled())
	}
	delete(account.Extra, openAIGatewayPoolWaitSecondsExtraKey)
	svc, _ := gatewayRuntimeService(account)
	stale := *account
	stale.Extra = map[string]any{openAIGatewayPoolExtraKey: true}
	require.NotNil(t, gatewayPoolWaitFrom(svc.gatewayPoolWaitContext(context.Background(), &stale)))
}

func TestGatewayPoolWaitConfigIgnoresRetiredSavedValues(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	account := fake.account(1)
	for _, raw := range []any{0, -1, 3601, 1.5, "60", true} {
		account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = raw
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
	}
	for _, seconds := range []int{1, 120, 3600} {
		account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = seconds
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
	}
	account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = "true"
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
}

func TestGatewayPoolWaitIdentitySurvivesForwardReentry(t *testing.T) {
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = true
	svc, _ := gatewayRuntimeService(account)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	firstCtx, _ := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
	first := gatewayPoolWaitFrom(svc.gatewayPoolWaitContext(firstCtx, account))
	require.NotNil(t, first)
	secondCtx, _ := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
	second := gatewayPoolWaitFrom(svc.gatewayPoolWaitContext(secondCtx, account))
	require.Same(t, first, second, "same-account reentry retains the captured credential domain")
}

func TestGatewayPoolWaitStopsAcquisitionAfterCallerCancellation(t *testing.T) {
	svc, _, fake, account, request, state := ticketWaitFixture(t)
	fake.refuseStatus, fake.refuseCode = 503, gwpool.CodeNoLivePair
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	state.sleep = func(context.Context, time.Duration) error {
		if fake.hits.Load() == 2 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	require.Positive(t, account.gatewayPoolFetchTimeout())
	_, err := svc.attachGatewayPoolRouteWithWait(ctx, account, gwpoolTestURL, http.Header{})
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 2, fake.hits.Load(), "no third acquisition after cancellation")
}

func TestGatewayPoolWaitRechecksSuccessfulFetchAfterConcurrentDisable(t *testing.T) {
	svc, repo, fake, account, request, state := ticketWaitFixture(t)
	fake.refuseStatus, fake.refuseCode = 503, gwpool.CodeNoLivePair
	state.sleep = func(context.Context, time.Duration) error {
		fake.refuseStatus = 0
		return nil
	}
	fake.onCookie = func() {
		repo.mu.Lock()
		repo.account.Schedulable = false
		repo.mu.Unlock()
	}
	upstream := &gwpoolEchoUpstream{}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.Error(t, err)
	require.Nil(t, response)
	require.Empty(t, upstream.sentBodies, "successful ticket arrival must not revive an account disabled during acquisition")
}

func TestGatewayPoolWaitFollowerCancellationDoesNotCancelSharedFetch(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	store := &openAICodexCookieStore{}
	started, release, leaderDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	fake.onList = func() { close(started); <-release }
	go func() {
		_, _, err := store.gatewayPoolPair(context.Background(), account, gwpoolTestIdentity)
		leaderDone <- err
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	followerDone := make(chan error, 1)
	go func() {
		_, _, err := store.gatewayPoolPair(ctx, account, gwpoolTestIdentity)
		followerDone <- err
	}()
	var followerErr error
	timedOut := false
	select {
	case followerErr = <-followerDone:
	case <-time.After(200 * time.Millisecond):
		timedOut = true
	}
	close(release)
	require.NoError(t, <-leaderDone, "canceling one caller must not cancel the shared fetch")
	require.False(t, timedOut, "follower must obey its own deadline without waiting for the leader")
	require.ErrorIs(t, followerErr, context.DeadlineExceeded)
}

func TestGatewayPoolWaitAbandonedFetchRemainsPending(t *testing.T) {
	for _, budget := range []string{"caller", "operation"} {
		t.Run(budget, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			store := &openAICodexCookieStore{}
			started, release := make(chan struct{}), make(chan struct{})
			fake.onList = func() { close(started); <-release }
			account.Extra[openAIGatewayPoolFetchTimeoutExtraKey] = 1
			timeout := 2 * time.Second
			if budget == "caller" {
				timeout = 20 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, _, err := store.gatewayPoolPair(ctx, account, gwpoolTestIdentity)
				done <- err
			}()
			<-started
			var waitErr error
			select {
			case waitErr = <-done:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("caller must stop at its own budget")
			}
			_, pending := store.gatewayPoolInventorySnapshot(gwpoolTestIdentity)
			close(release)
			require.ErrorIs(t, waitErr, context.DeadlineExceeded)
			// A responsive HTTP transport may already have stopped at this
			// point. A still-unwinding transport is covered independently.
			_ = pending
			require.Eventually(t, func() bool {
				inventory := store.gatewayPoolInventory(gwpoolTestIdentity)
				inventory.mu.Lock()
				defer inventory.mu.Unlock()
				return inventory.active == 0
			}, time.Second, time.Millisecond)
			_, cached := store.cachedPoolPair(gwpoolTestIdentity)
			require.NotEqual(t, openAIGatewayPoolPairLive, cached, "the last waiter cancels acquisition rather than filling an unused cache")
		})
	}
}

func TestGatewayPoolWaitRetriesChangedVerifiedFastPathWithoutReplayingBusiness(t *testing.T) {
	svc, _, _, account, request, state := ticketWaitFixture(t)
	account.Credentials["access_token"] = "offline"
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	// This fixture reads fresh settings during retries.
	repo, ok := svc.accountRepo.(*gatewayRuntimeRepo)
	require.True(t, ok)
	repo.mu.Lock()
	repo.account = *account
	repo.mu.Unlock()
	identity := gwpoolTestIdentity
	pair := openAIGatewayPoolPair{cookie: "offline-cookie", gateway: "old", version: "old", until: time.Now().Add(time.Minute)}
	svc.codexCookies.poolPairs.Store(identity, pair)
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, pair.version, "gpt-6-luna")
	resolved := 0
	svc.codexCookies.identity = func(context.Context, *Account) (string, error) {
		resolved++
		if resolved == 2 {
			next := pair
			next.gateway, next.version = "new", "new"
			svc.codexCookies.poolPairs.Store(identity, next)
		}
		return identity, nil
	}
	state.sleep = func(context.Context, time.Duration) error { return nil }
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: 200, minted: "state"}, {status: 200}, {status: 200}}}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	_ = response.Body.Close()
	require.Len(t, upstream.sentBodies, 3, "one A/B and exactly one business send")
}
