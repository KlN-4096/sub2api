package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolProgressShowsActiveAttemptAndRetiresCompletedRun(t *testing.T) {
	var tracker gatewayPoolProgressTracker
	first := tracker.start(1, 5)
	tracker.update(first, "verifying", 2, "unified-142", true, false)
	second := tracker.start(1, 3)
	tracker.update(second, "unknown", 1, "", false, true)
	snapshot := tracker.snapshot([]int64{1, 2}, time.Now())
	require.Equal(t, 2, snapshot[1].Attempt, "a completed concurrent request cannot hide an active request")
	require.Equal(t, 5, snapshot[1].Limit)
	require.Equal(t, 1, snapshot[1].ActiveRequests)
	require.NotContains(t, snapshot, int64(2))
	tracker.update(first, "ready", 2, "", false, true)
	require.Zero(t, tracker.snapshot([]int64{1}, time.Now())[1].ActiveRequests)
	require.Equal(t, "ready", tracker.snapshot([]int64{1}, time.Now().Add(2*time.Hour))[1].Phase)
}

func TestGatewayPoolProgressWiredToActualWarmLoop(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	account.Extra[openAIGatewayPoolWarmTicketsExtraKey] = 4
	svc := &OpenAIGatewayService{}
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	bounded, cancel := context.WithTimeout(req.Context(), time.Second)
	defer cancel()
	ctx, _ := withOpenAIGatewayPoolSink(bounded, nil)
	calls := 0
	err = svc.gatewayPoolWarmUpWith(req.WithContext(ctx), account, gwpoolTestIdentity, gwpoolWarmModel,
		func(_ context.Context, _, state string) (int, string, error) {
			calls++
			progress := svc.GatewayPoolProgress([]int64{1})[1]
			assert.Equal(t, "verifying", progress.Phase)
			assert.Equal(t, min(calls-1, 1), progress.Attempt, "count only after a verified send")
			assert.Zero(t, progress.Limit, "the shared queue has no fixed ticket limit")
			assert.Equal(t, "unified-142", progress.Gateway)
			return 200, "same-state", nil
		})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, "ready", svc.GatewayPoolProgress([]int64{1})[1].Phase)
	require.Equal(t, 1, svc.GatewayPoolProgress([]int64{1})[1].Attempt)
}

func TestGatewayPoolProgressCountsTwoTicketsNotFourShots(t *testing.T) {
	var tracker gatewayPoolProgressTracker
	run := tracker.start(1, 8)
	for _, ticket := range []string{"v1", "v1", "v2", "v2"} {
		tracker.tried(run, ticket)
	}
	tracker.update(run, "ready", 0, "", false, true)
	require.Equal(t, 2, tracker.snapshot([]int64{1}, time.Now())[1].Attempt)
}

func TestGatewayPoolProgressResumesAfterConcurrentRunReplacedIt(t *testing.T) {
	var tracker gatewayPoolProgressTracker
	first := tracker.start(1, 8)
	tracker.update(first, "ready", 0, "", false, true)
	second := tracker.start(1, 8)
	tracker.resume(first)
	tracker.tried(first, "next")
	snapshot := tracker.snapshot([]int64{1}, time.Now())[1]
	require.Equal(t, first.progress.RunID, snapshot.RunID)
	require.Equal(t, 2, snapshot.ActiveRequests)
	tracker.update(second, "ready", 0, "", false, true)
	require.Equal(t, 1, tracker.snapshot([]int64{1}, time.Now())[1].ActiveRequests)
}

func TestGatewayPoolProgressDoesNotCountMissingOrUnconfirmedTicket(t *testing.T) {
	for _, missing := range []bool{false, true} {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
		if missing {
			fake.refuseStatus = http.StatusServiceUnavailable
		}
		svc := &OpenAIGatewayService{}
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
		require.NoError(t, err)
		ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)
		err = svc.gatewayPoolWarmUpWith(req.WithContext(ctx), fake.account(1), gwpoolTestIdentity, gwpoolWarmModel,
			func(ctx context.Context, _, _ string) (int, string, error) {
				trace, _ := ctx.Value(gatewayPoolProbeTraceKey{}).(*gatewayPoolProbeTrace)
				trace.mayHaveSent = true
				return 0, "", errors.New("ambiguous send")
			})
		require.Error(t, err)
		require.Zero(t, svc.GatewayPoolProgress([]int64{1})[1].Attempt)
	}
}

func TestGatewayPoolProgressEndsWithUsageCycle(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	svc, _ := gatewayRuntimeService(account)
	ctx, finish := svc.beginGatewayPoolUsageRequest(context.Background(), account)
	defer finish()
	ctx, _ = withOpenAIGatewayPoolSink(ctx, nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	err = svc.gatewayPoolWarmUpWith(req, account, gwpoolTestIdentity, gwpoolWarmModel,
		func(context.Context, string, string) (int, string, error) { return 200, "same-state", nil })
	require.NoError(t, err)
	before, err := svc.GatewayPoolRuntimeProgress(ctx, []int64{1})
	require.NoError(t, err)
	require.Equal(t, "ready", before[1].Phase)
	svc.changeGatewayPoolUsage(ctx, account, gwpoolTestIdentity, func(state *gatewayPoolUsageLedger) bool {
		return state.end(time.Now().UTC(), "temporarily_unschedulable")
	})
	after, err := svc.GatewayPoolRuntimeProgress(ctx, []int64{1})
	require.NoError(t, err)
	require.Equal(t, "idle", after[1].Phase)
	require.Empty(t, after[1].RunID)
	require.Zero(t, after[1].Attempt)
	require.Zero(t, after[1].ElapsedMS)
	require.NotNil(t, after[1].Runtime, "ending a cycle must not erase inventory/history")
}

func TestGatewayPoolProgressShowsWaitingDuringShortageSleep(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc := &OpenAIGatewayService{}
	progress := svc.codexCookies.poolProgress.start(account.ID, 0)
	ctx := context.WithValue(context.Background(), gatewayPoolPreparationKey{}, true)
	ctx = context.WithValue(ctx, gatewayPoolProgressRunKey{}, progress)
	ctx = context.WithValue(ctx, gatewayPoolPreparationSleepKey{}, func(context.Context, time.Duration) error {
		require.Equal(t, "waiting", svc.GatewayPoolProgress([]int64{account.ID})[account.ID].Phase)
		return nil
	})
	retry, err := svc.waitGatewayPoolRetry(ctx, account, time.Second)
	require.NoError(t, err)
	require.True(t, retry)
	require.Equal(t, "fetching", svc.GatewayPoolProgress([]int64{account.ID})[account.ID].Phase)
}
