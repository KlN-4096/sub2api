package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGatewayPoolBusinessRetryKeepsRequestAndVerifiesReplacement(t *testing.T) {
	svc, repo, fake, account, request, wait := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	account.Credentials["access_token"] = "offline-token"
	repo.account = *account
	firstCookie, nextCookie := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-143")
	fake.cookieForHit = func(hit int64) string {
		if hit > 1 {
			return nextCookie
		}
		return firstCookie
	}
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}, {Name: "unified-143", PairReady: true}}
	var waits []time.Duration
	wait.sleep = func(_ context.Context, gap time.Duration) error {
		waits = append(waits, gap)
		return nil
	}
	gwpoolEchoSeedVerified(t, svc, account)
	for range 2 {
		run := svc.startGatewayPoolProgress(request.Context(), account, gwpoolTestIdentity)
		svc.codexCookies.poolProgress.update(run, "ready", 1, "", false, true)
	}
	var replacementProgress GatewayPoolProgress
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: 200, minted: "new-1"}, {status: 200, minted: "new-2"},
		{status: 200, minted: "new-3"}, {status: 200, minted: "new-4"},
		{status: 200, minted: "replacement-proof"}, {status: 200, minted: "replacement-proof"},
		{status: 200},
	}}
	upstream.beforeReply = func(_ *http.Request, call int) error {
		if call == 5 {
			replacementProgress = svc.GatewayPoolProgress([]int64{account.ID})[account.ID]
		}
		return nil
	}
	svc.httpUpstream = upstream
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.NoError(t, response.Body.Close())
	require.Len(t, upstream.sentBodies, 7, "old business + 3 confirmations + new ticket A/B + replay")
	require.Equal(t, upstream.sentBodies[0], upstream.sentBodies[6], "original business bytes must survive replay")
	require.Equal(t, "gpt-6-astra", gjson.Get(upstream.sentBodies[6], "model").String())
	require.Equal(t, "hi", gjson.Get(upstream.sentBodies[4], "input.0.content.0.text").String())
	require.Equal(t, "business-state", upstream.sentState[6], "do not inject confirmation state into business")
	require.NotEqual(t, upstream.sentCookies[0], upstream.sentCookies[6])
	require.True(t, upstream.bodies[0].closed)
	require.Zero(t, upstream.bodies[0].reads)
	require.Empty(t, waits, "confirmed degradation must start replacement preparation without retry backoff")
	require.EqualValues(t, 3, replacementProgress.Sequence, "publish the next run before its A/B verification")
	require.Equal(t, "verifying", replacementProgress.Phase)
}

func TestGatewayPoolBusinessRetryLeavesNonPoolRequestsAlone(t *testing.T) {
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: 503}, {status: 200}}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := gwpoolTestAccount(1)
	delete(account.Extra, openAIGatewayPoolExtraKey)
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.Equal(t, 503, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Len(t, upstream.sentBodies, 1)
}

func TestGatewayPoolBusinessRetryConfirmation500KeepsBackoffAndReplays(t *testing.T) {
	svc, repo, fake, account, request, wait := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	account.Credentials["access_token"] = "offline-token"
	repo.account = *account
	first, replacement := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-143")
	fake.cookieForHit = func(hit int64) string {
		if hit > 1 {
			return replacement
		}
		return first
	}
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}, {Name: "unified-143", PairReady: true}}
	var waits []time.Duration
	wait.sleep = func(_ context.Context, gap time.Duration) error {
		waits = append(waits, gap)
		return nil
	}
	gwpoolEchoSeedVerified(t, svc, account)
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "fresh-state"},
		{status: http.StatusInternalServerError},
		{status: http.StatusOK, minted: "replacement-proof"},
		{status: http.StatusOK, minted: "replacement-proof"},
		{status: http.StatusOK},
	}}
	svc.httpUpstream = upstream
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Len(t, upstream.sentBodies, 5, "business, failed confirmation, replacement A/B, replay")
	require.Equal(t, upstream.sentBodies[0], upstream.sentBodies[4])
	require.Equal(t, upstream.sentState[0], upstream.sentState[4])
	require.NotEqual(t, upstream.sentCookies[0], upstream.sentCookies[4])
	require.Equal(t, []time.Duration{gatewayPoolVerificationRetryGap}, waits)
	require.True(t, upstream.bodies[0].closed)
	require.Empty(t, openAIGatewayPoolSinkFrom(request.Context()).discarded, "a confirmation HTTP error is not degraded evidence")
}

func TestGatewayPoolBusinessRetryTransientHTTPKeepsBytesButNotAuthFailures(t *testing.T) {
	for _, status := range []int{401, 403, 429, 500, 502, 503, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			svc, _, _, account, request, wait := ticketWaitFixture(t)
			gwpoolEchoSeedVerified(t, svc, account)
			var waits []time.Duration
			wait.sleep = func(_ context.Context, gap time.Duration) error {
				waits = append(waits, gap)
				inventory := svc.codexCookies.gatewayPoolInventory(gwpoolTestIdentity)
				inventory.mu.Lock()
				active, requests := inventory.active, inventory.requests
				inventory.mu.Unlock()
				require.Zero(t, active, "a finished attempt cannot hide exhaustion while waiting")
				require.Equal(t, 1, requests, "the logical request still protects idle accounting")
				return nil
			}
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: status}, {status: 200}}}
			svc.httpUpstream = upstream
			response, err := svc.doOpenAIUpstream(request, "", account)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			if gatewayPoolBusinessRetryStatus(status) {
				require.Equal(t, 200, response.StatusCode)
				require.Len(t, upstream.sentBodies, 2)
				require.Equal(t, upstream.sentBodies[0], upstream.sentBodies[1])
				require.True(t, upstream.bodies[0].closed)
				require.Equal(t, []time.Duration{gatewayPoolVerificationRetryGap}, waits,
					"transient HTTP failures must retain their backoff")
			} else {
				require.Equal(t, status, response.StatusCode)
				require.Len(t, upstream.sentBodies, 1)
				require.Empty(t, waits, "authentication and quota failures do not enter this retry loop")
			}
		})
	}
}

func TestGatewayPoolBusinessRetryStopsAtCallerDeadlineOrDisconnect(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "budget", true: "disconnect"}[canceled], func(t *testing.T) {
			svc, _, _, account, request, wait := ticketWaitFixture(t)
			gwpoolEchoSeedVerified(t, svc, account)
			ctx, cancel := context.WithTimeout(request.Context(), 30*time.Millisecond)
			defer cancel()
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: 503}}}
			svc.httpUpstream = upstream
			wait.sleep = func(ctx context.Context, _ time.Duration) error {
				if canceled {
					cancel()
					return context.Canceled
				}
				return gatewayPoolSleep(ctx, time.Second)
			}
			response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
			require.Nil(t, response)
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			require.Len(t, upstream.sentBodies, 1)
			require.True(t, upstream.bodies[0].closed)
		})
	}
}

func TestGatewayPoolBusinessRetryDoesNotTreatEveryUnverifiedAsRetryable(t *testing.T) {
	require.False(t, errors.As(errOpenAIGatewayPoolWarmUnverified, new(*gatewayPoolAttemptRetryError)))
	for _, status := range []int{401, 429} {
		full, conclusive, err := gatewayPoolConfirmState(context.Background(), "cookie", "state", 1,
			func(context.Context, string, string) (int, string, error) { return status, "", nil })
		require.False(t, full)
		require.False(t, conclusive)
		require.False(t, gatewayPoolRetryableProbeError(err))
	}
}

func TestGatewayPoolBusinessRetryConcurrentRejectionDoesNotRetireReplacement(t *testing.T) {
	var retry *gatewayPoolAttemptRetryError
	err := gatewayPoolDegradedAttemptError(context.Background(), false)
	require.ErrorAs(t, err, &retry)
	require.False(t, retry.retire)
	require.ErrorIs(t, gatewayPoolDegradedAttemptError(context.Background(), true), errOpenAIGatewayPoolRouteDegraded)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, gatewayPoolDegradedAttemptError(ctx, false), context.Canceled)
}

func TestGatewayPoolBusinessRetryHandlerKeepsSameAccountAndCallerCancellation(t *testing.T) {
	svc, _, _, account, request, budget := ticketWaitFixture(t)
	group := int64(7)
	first := &UpstreamFailoverError{StatusCode: 502}
	ctx := svc.PrepareGatewayPoolAccountRotation(request.Context(), &group, account, first)
	require.True(t, first.GatewayPoolRetry)
	require.True(t, first.SameAccountRetryOnly)
	require.False(t, first.ShouldRetryNextAccount())
	require.Zero(t, first.SameAccountRetryDeadline, "ordinary recovery has no synthetic aggregate budget")
	require.Same(t, budget, gatewayPoolWaitFrom(ctx))
	ctx = svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, first)
	require.True(t, first.GatewayPoolRetry, "preparing a classified retry is idempotent")
	next := &UpstreamFailoverError{StatusCode: 504}
	ctx = svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, next)
	require.Equal(t, first.SameAccountRetryDeadline, next.SameAccountRetryDeadline)
	require.Equal(t, account.ID, gatewayPoolRetryOnlyFrom(ctx).accountID)
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	last := &UpstreamFailoverError{StatusCode: 502}
	svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, last)
	require.False(t, last.GatewayPoolRetry)
	require.False(t, last.ShouldRetryNextAccount())
}

func TestGatewayPoolBusinessRetryPreservesReal429DeadlineAcrossHTTPRecovery(t *testing.T) {
	svc, _, _, account, request, _ := ticketWaitFixture(t)
	group := int64(7)
	deadline := time.Now().Add(time.Minute)
	first := &UpstreamFailoverError{StatusCode: 429, RetryableOnSameAccount: true,
		SameAccountRetryDeadline: deadline}
	ctx := svc.PrepareGatewayPoolAccountRotation(request.Context(), &group, account, first)
	next := &UpstreamFailoverError{StatusCode: 502}
	ctx = svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, next)
	require.True(t, next.GatewayPoolRetry)
	require.Equal(t, deadline, next.SameAccountRetryDeadline, "HTTP recovery cannot erase the original rate-limit cutoff")
	require.Equal(t, deadline, gatewayPoolRetryOnlyFrom(ctx).deadline)
	retry := gatewayPoolRetryOnlyFrom(ctx)
	retry.deadline = time.Now().Add(-time.Second)
	ctx = context.WithValue(ctx, gatewayPoolRetryOnlyKey{}, retry)
	last := &UpstreamFailoverError{StatusCode: 503}
	svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, last)
	require.False(t, last.GatewayPoolRetry)
	require.False(t, last.ShouldRetryNextAccount())
}

func TestGatewayPoolBusinessRetryRenewsOnlyAttemptTimeout(t *testing.T) {
	svc, _, fake, account, request, wait := ticketWaitFixture(t)
	first, replacement := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-143")
	fake.cookieForHit = func(hit int64) string {
		if hit > 1 {
			return replacement
		}
		return first
	}
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}, {Name: "unified-143", PairReady: true}}
	wait.sleep = func(context.Context, time.Duration) error { return nil }
	gwpoolEchoSeedVerified(t, svc, account)
	budget := &gatewayPoolFirstOutputBudget{timeout: 20 * time.Millisecond}
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, gatewayPoolFirstOutputBudgetKey{}, budget)
	callerDeadline, _ := ctx.Deadline()
	started := time.Now()
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: 200}, {status: 200, minted: "replacement-proof"}, {status: 200}, {status: 200},
	}, beforeReply: func(req *http.Request, n int) error {
		if n == 1 {
			<-req.Context().Done()
			return req.Context().Err()
		}
		return nil
	}}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Len(t, upstream.sentBodies, 4, "failed business, replacement A/B, original business")
	require.NotEqual(t, upstream.sentCookies[0], upstream.sentCookies[3])
	require.Equal(t, upstream.sentBodies[0], upstream.sentBodies[3])
	require.GreaterOrEqual(t, budget.started.Sub(started), 20*time.Millisecond)
	after, _ := ctx.Deadline()
	require.Equal(t, callerDeadline, after, "caller deadline must not move")
}

func TestGatewayPoolBusinessRetryNewWSTurnDoesNotInheritPrevious429Deadline(t *testing.T) {
	svc, _, _, account, request, _ := ticketWaitFixture(t)
	group := int64(7)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = request
	firstHolder := &gatewayPoolWaitHolder{}
	c.Set(gatewayPoolWaitGinKey, firstHolder)
	first := &UpstreamFailoverError{StatusCode: 429, RetryableOnSameAccount: true,
		SameAccountRetryDeadline: time.Now().Add(time.Minute)}
	ctx := svc.PrepareGatewayPoolAccountRotation(request.Context(), &group, account, first, c)
	require.Same(t, firstHolder, gatewayPoolRetryOnlyFrom(ctx).holder)
	c.Set(gatewayPoolWaitGinKey, &gatewayPoolWaitHolder{}) // next response.create
	nextDeadline := time.Now().Add(2 * time.Minute)
	next := &UpstreamFailoverError{StatusCode: 429, RetryableOnSameAccount: true,
		SameAccountRetryDeadline: nextDeadline}
	svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, next, c)
	require.Equal(t, nextDeadline, next.SameAccountRetryDeadline)
	require.True(t, next.SameAccountRetryOnly)
}
