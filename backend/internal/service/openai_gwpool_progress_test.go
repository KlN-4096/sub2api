package service

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

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
	require.Empty(t, tracker.snapshot([]int64{1}, time.Now().Add(2*gatewayPoolProgressRetention)))
}

func TestGatewayPoolProgressWiredToActualWarmLoop(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	account.Extra[openAIGatewayPoolWarmTicketsExtraKey] = 4
	svc := &OpenAIGatewayService{}
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)
	calls := 0
	err = svc.gatewayPoolWarmUpWith(req.WithContext(ctx), account, gwpoolTestIdentity, gwpoolWarmModel,
		func(_ context.Context, _, state string) (int, string, error) {
			calls++
			progress := svc.GatewayPoolProgress([]int64{1})[1]
			require.Equal(t, "verifying", progress.Phase)
			require.Equal(t, 1, progress.Attempt)
			require.Equal(t, 4, progress.Limit)
			require.Equal(t, "unified-142", progress.Gateway)
			return 200, "same-state", nil
		})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, "ready", svc.GatewayPoolProgress([]int64{1})[1].Phase)
}
