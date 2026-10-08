package service

import (
	"context"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolProbeTimeoutSettings(t *testing.T) {
	account := newGwpoolFakePool(t, "", 150).account(1)
	require.Equal(t, 10*time.Second, account.gatewayPoolProbeTimeout())
	require.Equal(t, 10*time.Second, account.gatewayPoolFetchTimeout())
	for _, value := range []any{0, -1, 121, 1.5, "10", true} {
		account.Extra[openAIGatewayPoolProbeTimeoutExtraKey] = value
		require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
		require.Equal(t, 10*time.Second, account.gatewayPoolProbeTimeout())
	}
	for _, value := range []int{1, 10, 35, 120} {
		account.Extra[openAIGatewayPoolProbeTimeoutExtraKey] = value
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
		require.Equal(t, time.Duration(value)*time.Second, account.gatewayPoolProbeTimeout())
		require.Equal(t, 2*account.gatewayPoolProbeTimeout(), gatewayPoolProbeBudget(context.Background(), account))
	}
	account.Extra[openAIGatewayPoolProbeTimeoutExtraKey] = nil
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
	require.Equal(t, 10*time.Second, account.gatewayPoolProbeTimeout())
}

func TestGatewayPoolConfirmationUsesConfiguredProbeTimeoutAndLuna(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	account.Extra[openAIGatewayPoolProbeTimeoutExtraKey] = 2
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "new-state"}, {status: http.StatusOK, minted: "new-state"},
	}}
	upstream.beforeReply = func(request *http.Request, shot int) error {
		if shot == 2 {
			deadline, ok := request.Context().Deadline()
			require.True(t, ok)
			require.InDelta(t, 2, time.Until(deadline).Seconds(), 0.5)
		}
		return nil
	}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	_, response, err := gwpoolEchoRun(t, svc, account, "old-state")
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Len(t, upstream.sentBodies, 2)
	require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[0], "business body is never changed")
	require.Contains(t, upstream.sentBodies[1], `"model":"gpt-6-luna"`)
}

func TestGatewayPoolProbeTimeoutUsesFreshSettingsOnBothShots(t *testing.T) {
	svc, repo, _, account, request, _ := ticketWaitFixture(t)
	account.Credentials["access_token"] = "offline-token"
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	repo.account = *account
	// The business request's account remains stale; only GetByID sees 10.
	account.Extra = maps.Clone(account.Extra)
	repo.account.Extra[openAIGatewayPoolProbeTimeoutExtraKey] = 10
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "probe-state"}, {status: http.StatusOK}, {status: http.StatusOK},
	}}
	upstream.beforeReply = func(req *http.Request, shot int) error {
		if shot <= 2 {
			deadline, ok := req.Context().Deadline()
			require.True(t, ok)
			require.InDelta(t, 10, time.Until(deadline).Seconds(), 1)
		}
		return nil
	}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Len(t, upstream.sentBodies, 3)
}

func TestGatewayPoolProbeConfiguredTimeoutActuallyCancelsTransport(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Credentials["access_token"] = "offline-token"
	account.Extra[openAIGatewayPoolProbeTimeoutExtraKey] = 1
	upstream := &gwpoolEchoUpstream{beforeReply: func(req *http.Request, _ int) error {
		<-req.Context().Done()
		return req.Context().Err()
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	started := time.Now()
	_, _, err := svc.gatewayPoolWarmShot(context.Background(), account, "",
		gwpoolTestPairCookie(t, "unified-142"), gwpoolWarmModel, "")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.InDelta(t, 1, time.Since(started).Seconds(), 0.75)
}
