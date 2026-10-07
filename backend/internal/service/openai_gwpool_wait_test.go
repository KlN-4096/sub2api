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
	svc, repo := gatewayRuntimeService(account)
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
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
	clock := time.Now()
	state.now = func() time.Time { return clock }
	sleeps := 0
	state.sleep = func(_ context.Context, delay time.Duration) error {
		sleeps++
		clock = clock.Add(delay)
		fake.refuseStatus = 0
		return nil
	}
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: http.StatusOK}}}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.NoError(t, response.Body.Close())
	require.Equal(t, 1, sleeps)
	require.EqualValues(t, 2, fake.hits.Load())
	require.Len(t, upstream.sentBodies, 1, "the business request must not be retried")
}

func TestGatewayPoolWaitStillVerifiesActualTicketBeforeBusiness(t *testing.T) {
	svc, repo, fake, account, request, state := ticketWaitFixture(t)
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

func TestGatewayPoolWaitProbeFailurePreservesAttemptLimitAndStopsAuthentication(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			svc, repo, _, account, request, state := ticketWaitFixture(t)
			account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
			account.Extra[openAIGatewayPoolWarmTicketsExtraKey] = 2
			repo.account = *account
			sleeps, shots := 0, 0
			state.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
			err := svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel,
				func(context.Context, string, string) (int, string, error) {
					shots++
					return status, "", nil
				})
			require.ErrorIs(t, err, errOpenAIGatewayPoolWarmUnverified)
			if status == http.StatusOK {
				require.Equal(t, 2, shots)
				require.Equal(t, 1, sleeps)
			} else {
				require.Equal(t, 1, shots)
				require.Zero(t, sleeps)
			}
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

func TestGatewayPoolWaitIsCumulativeCancelableAndFresh(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel", "disable", "account-disabled", "identity-changed"} {
		t.Run(mode, func(t *testing.T) {
			svc, repo, fake, account, request, state := ticketWaitFixture(t)
			fake.refuseStatus, fake.refuseCode, fake.refuseRetryAfter = 503, gwpool.CodeNoLivePair, 30
			clock := time.Now()
			state.now = func() time.Time { return clock }
			sleeps := 0
			state.sleep = func(_ context.Context, delay time.Duration) error {
				sleeps++
				clock = clock.Add(delay)
				repo.mu.Lock()
				defer repo.mu.Unlock()
				switch mode {
				case "cancel":
					return context.Canceled
				case "disable":
					repo.account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = false
				case "account-disabled":
					repo.account.Schedulable = false
				case "identity-changed":
					repo.account.Credentials = map[string]any{"chatgpt_account_id": "different-account", "chatgpt_user_id": "user"}
				}
				return nil
			}
			_, err := svc.attachGatewayPoolRouteWithWait(request.Context(), account, gwpoolTestURL, http.Header{})
			require.Error(t, err)
			if mode == "deadline" {
				require.Equal(t, 4, sleeps)
				require.EqualValues(t, 4, fake.hits.Load(), "no new acquisition at or beyond deadline")
				_, err = svc.attachGatewayPoolRouteWithWait(request.Context(), account, gwpoolTestURL, http.Header{})
				require.Error(t, err)
				require.Equal(t, 4, sleeps, "second acquisition must not reset the request's wait budget")
			} else {
				require.Equal(t, 1, sleeps)
				require.EqualValues(t, 1, fake.hits.Load(), "no retry after cancellation/config/identity change")
			}
		})
	}
}

func TestGatewayPoolWaitDefaultsOffValidatesSecondsAndReadsFreshOptIn(t *testing.T) {
	account := rotationAccount(1, 7)
	require.Zero(t, account.gatewayPoolMaxWait())
	for _, invalid := range []any{0, -1, 3601, 1.5, "60", true} {
		account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = true
		account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = invalid
		require.Zero(t, account.gatewayPoolMaxWait())
	}
	delete(account.Extra, openAIGatewayPoolWaitSecondsExtraKey)
	require.Equal(t, 120*time.Second, account.gatewayPoolMaxWait())
	svc, _ := gatewayRuntimeService(account)
	stale := *account
	stale.Extra = map[string]any{openAIGatewayPoolExtraKey: true}
	require.NotNil(t, gatewayPoolWaitFrom(svc.gatewayPoolWaitContext(context.Background(), &stale)))
}

func TestGatewayPoolWaitConfigRejectsInvalidSavedValues(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	account := fake.account(1)
	for _, raw := range []any{0, -1, 3601, 1.5, "60", true} {
		account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = raw
		require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
	}
	for _, seconds := range []int{1, 120, 3600} {
		account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = seconds
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
	}
	account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = "true"
	require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
}

func TestGatewayPoolWaitBudgetSurvivesForwardReentry(t *testing.T) {
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = true
	svc, _ := gatewayRuntimeService(account)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	firstCtx, _ := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
	first := gatewayPoolWaitFrom(svc.gatewayPoolWaitContext(firstCtx, account))
	require.NotNil(t, first)
	first.waited = time.Minute
	first.deadline = time.Now().Add(time.Minute)
	secondCtx, _ := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
	second := gatewayPoolWaitFrom(svc.gatewayPoolWaitContext(secondCtx, account))
	require.Same(t, first, second, "handler failover must not create a fresh wait budget")
}

func TestGatewayPoolWaitCapsFetchWorkAndStopsExpiredWorkBudget(t *testing.T) {
	svc, _, fake, account, request, state := ticketWaitFixture(t)
	fake.refuseStatus, fake.refuseCode = 503, gwpool.CodeNoLivePair
	ctx := context.WithValue(request.Context(), gatewayPoolWaitWorkKey{}, func() time.Duration {
		return time.Duration(2-fake.hits.Load()) * time.Second
	})
	state.sleep = func(context.Context, time.Duration) error { return nil }
	require.Equal(t, 2*time.Second, gatewayPoolFetchTimeoutForContext(ctx, account))
	_, err := svc.attachGatewayPoolRouteWithWait(ctx, account, gwpoolTestURL, http.Header{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 2, fake.hits.Load(), "work budget must be checked inside the acquisition retry loop")
	state.deadline = state.now().Add(time.Millisecond)
	require.LessOrEqual(t, gatewayPoolFetchTimeoutForContext(request.Context(), account), time.Millisecond)
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
	for _, budget := range []string{"work", "wait"} {
		t.Run(budget, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			store := &openAICodexCookieStore{}
			started, release := make(chan struct{}), make(chan struct{})
			fake.onList = func() { close(started); <-release }
			state := &gatewayPoolWaitState{max: time.Minute, now: time.Now}
			ctx := context.WithValue(context.Background(), gatewayPoolWaitKey{}, state)
			if budget == "work" {
				ctx = context.WithValue(ctx, gatewayPoolWaitWorkKey{}, func() time.Duration {
					return 20 * time.Millisecond
				})
			} else {
				state.deadline = time.Now().Add(20 * time.Millisecond)
			}
			done := make(chan error, 1)
			go func() {
				_, _, err := store.gatewayPoolPair(ctx, account, gwpoolTestIdentity)
				done <- err
			}()
			<-started
			var waitErr error
			select {
			case waitErr = <-done:
			case <-time.After(200 * time.Millisecond):
				close(release)
				t.Fatal("caller must stop at its own budget")
			}
			_, pending := store.gatewayPoolInventorySnapshot(gwpoolTestIdentity, account)
			close(release)
			require.ErrorIs(t, waitErr, context.DeadlineExceeded)
			require.True(t, pending, "abandoned shared fetch must still prevent premature rotation")
			require.Eventually(t, func() bool {
				inventory := store.gatewayPoolInventory(gwpoolTestIdentity)
				inventory.mu.Lock()
				defer inventory.mu.Unlock()
				return inventory.active == 0
			}, time.Second, time.Millisecond)
			_, cached := store.cachedPoolPair(gwpoolTestIdentity)
			require.Equal(t, openAIGatewayPoolPairLive, cached, "independent fetch still completes for the next caller")
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
