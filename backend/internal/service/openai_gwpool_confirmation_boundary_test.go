package service

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func confirmationBoundaryFixture(t *testing.T, ctx context.Context, firstSent time.Time, replies []gwpoolEchoReply) (*OpenAIGatewayService, *gwpoolEchoUpstream, *Account, *http.Request, *http.Response) {
	t.Helper()
	account := gwpoolTestAccount(1)
	upstream := &gwpoolEchoUpstream{replies: replies}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	pair := openAIGatewayPoolPair{
		cookie: gwpoolTestPairCookie(t, "unified-142"), version: "v", gateway: "unified-142", firstSent: firstSent,
	}
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
	ctx, sink := withOpenAIGatewayPoolSink(ctx, nil)
	sink.mark(OpenAIGatewayPoolApplied{AccountID: account.ID, Cookie: pair.cookie, Gateway: pair.gateway, Version: pair.version})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	request.Header.Set("Cookie", pair.cookie)
	request.Header.Set(openAICodexTurnStateHeader, "old")
	response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}
	response.Header.Set(openAICodexTurnStateHeader, "s1")
	return svc, upstream, account, request, response
}

func TestGatewayPoolConfirmationFreezesAgeAtBusinessSend(t *testing.T) {
	first := time.Now().Add(-91 * time.Second)
	sent := first.Add(89 * time.Second) // the response crosses the 90-second boundary
	svc, upstream, account, request, response := confirmationBoundaryFixture(t, context.Background(), first,
		[]gwpoolEchoReply{{status: 200, minted: "s2"}, {status: 200, minted: "s2"}})
	degraded, err := svc.gatewayPoolRouteDegraded(request, response, account, "", gwpoolTestIdentity, sent)
	require.NoError(t, err)
	require.False(t, degraded, "response latency must not reduce the frozen young-ticket confirmation count")
	require.Equal(t, []string{"s1", "s2"}, upstream.sentState)
}

func TestGatewayPoolConfirmationAfterSuccessfulSendIgnoresOld429AdmissionDeadline(t *testing.T) {
	now := time.Now()
	ctx := context.WithValue(context.Background(), gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{
		accountID: 1, deadline: now.Add(-time.Second),
		failure: &UpstreamFailoverError{StatusCode: http.StatusTooManyRequests},
	})
	svc, upstream, account, request, response := confirmationBoundaryFixture(t, ctx, now.Add(-time.Minute),
		[]gwpoolEchoReply{{status: 200, minted: "s1"}})
	require.NotNil(t, GatewayPoolRetryFailure(ctx), "the original admission deadline is still expired")
	degraded, err := svc.gatewayPoolRouteDegraded(request, response, account, "", gwpoolTestIdentity, now.Add(-2*time.Second))
	require.NoError(t, err)
	require.False(t, degraded)
	require.Equal(t, []string{"s1"}, upstream.sentState)
	require.NotNil(t, GatewayPoolRetryFailure(ctx), "confirmation must not extend or clear the business retry deadline")
}
