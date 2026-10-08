package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func gatewayPoolPostResponseRun(t *testing.T, svc *OpenAIGatewayService, account *Account, state string) (*gin.Context, *http.Response, error) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer offline-state-credential")
	request.Header.Set(openAICodexTurnStateHeader, state)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), c)
	gwpoolEchoSeedVerified(t, svc, account)
	// This suite isolates one quality decision; logical-request replay has
	// its own multi-ticket tests in openai_gwpool_business_retry_test.go.
	response, err := svc.doOpenAIUpstreamAttempt(request.WithContext(ctx), "", account)
	return c, response, err
}

func TestGatewayPoolPostResponseStateWithoutSent(t *testing.T) {
	for _, tc := range []struct {
		name      string
		first     gwpoolEchoReply
		second    gwpoolEchoReply
		wantCalls int
		wantErr   error
	}{
		{"neither-state-passes", gwpoolEchoReply{status: 200}, gwpoolEchoReply{}, 1, nil},
		{"fresh-then-absent-passes", gwpoolEchoReply{status: 200, minted: "s1"}, gwpoolEchoReply{status: 200}, 2, nil},
		{"fresh-then-same-passes", gwpoolEchoReply{status: 200, minted: "s1"}, gwpoolEchoReply{status: 200, minted: "s1"}, 2, nil},
		{"fresh-then-different-drops", gwpoolEchoReply{status: 200, minted: "s1"}, gwpoolEchoReply{status: 200, minted: "s2"}, 2, errOpenAIGatewayPoolRouteDegraded},
		{"business-429-passes-to-error-handler", gwpoolEchoReply{status: 429, minted: "s1"}, gwpoolEchoReply{}, 1, nil},
		{"confirmation-429-is-unknown", gwpoolEchoReply{status: 200, minted: "s1"}, gwpoolEchoReply{status: 429, minted: "s2"}, 2, errOpenAIGatewayPoolWarmUnverified},
		{"confirmation-502-is-unknown", gwpoolEchoReply{status: 200, minted: "s1"}, gwpoolEchoReply{status: 502, minted: "s2"}, 2, errOpenAIGatewayPoolWarmUnverified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{tc.first, tc.second}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			gwpoolEchoSeedVerified(t, svc, fake.account(1))
			pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
			pair.firstSent = time.Now().Add(-3 * time.Minute)
			svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
			c, response, err := gatewayPoolPostResponseRun(t, svc, fake.account(1), "")
			if tc.wantErr == nil {
				require.NoError(t, err)
				require.NotNil(t, response)
				require.Equal(t, tc.first.status, response.StatusCode)
				require.Same(t, upstream.bodies[0], response.Body)
				require.Equal(t, tc.first.minted, response.Header.Get(openAICodexTurnStateHeader))
				require.NoError(t, response.Body.Close())
			} else {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, response)
				require.True(t, upstream.bodies[0].closed)
			}
			require.Len(t, upstream.sentBodies, tc.wantCalls)
			require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[0], "business is first, with no seed request")
			require.Empty(t, upstream.sentState[0], "never inject cached/probe state into business")
			require.Zero(t, upstream.bodies[0].reads, "classification never consumes the business body")
			if tc.wantCalls == 2 {
				require.Equal(t, "s1", upstream.sentState[1])
				require.Equal(t, upstream.sentCookies[0], upstream.sentCookies[1])
				require.Equal(t, "gpt-6-astra", gjson.Get(upstream.sentBodies[0], "model").String())
				require.Equal(t, gatewayPoolProbeModelLuna, gjson.Get(upstream.sentBodies[1], "model").String())
				require.Equal(t, "hi", gjson.Get(upstream.sentBodies[1], "input.0.content.0.text").String())
				require.True(t, upstream.bodies[1].closed)
			}
			discarded := takeDiscardedOpenAIGatewayPoolAttempts(c)
			if errors.Is(tc.wantErr, errOpenAIGatewayPoolRouteDegraded) {
				require.Len(t, discarded, 1)
			} else {
				require.Empty(t, discarded)
			}
			require.Empty(t, OpenAITurnStateUsageSent(c), "confirmation state is not business outbound state")
			require.Empty(t, OpenAITurnStateUsageSource(c))
		})
	}
}

func TestGatewayPoolPostResponseStateUsesSameAgeBudgetWithoutSent(t *testing.T) {
	for _, tc := range []struct {
		age           time.Duration
		confirmations int
	}{{time.Second, 3}, {90 * time.Second, 1}, {150 * time.Second, 1}} {
		t.Run(tc.age.String(), func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
				{status: 200, minted: "s1"}, {status: 200, minted: "s2"},
				{status: 200, minted: "s3"}, {status: 200, minted: "s4"},
			}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			account := fake.account(1)
			gwpoolEchoSeedVerified(t, svc, account)
			pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
			pair.firstSent = time.Now().Add(-tc.age)
			svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
			_, response, err := gatewayPoolPostResponseRun(t, svc, account, "")
			require.ErrorIs(t, err, errOpenAIGatewayPoolRouteDegraded)
			require.Nil(t, response)
			require.Equal(t, []string{"", "s1", "s2", "s3"}[:tc.confirmations+1], upstream.sentState)
		})
	}
}

func TestGatewayPoolPostResponseStateNeverCachesOrInjects(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: 200}, {status: 200, minted: "response-state"}, {status: 200}, {status: 200},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := fake.account(1)
	for _, state := range []string{"client-state", "", ""} {
		_, response, err := gatewayPoolPostResponseRun(t, svc, account, state)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
	}
	require.Equal(t, []string{"client-state", "", "response-state", ""}, upstream.sentState)
	require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[0])
	require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[1])
	require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[3])
}

func TestGatewayPoolPostResponseStateCompressedBuilders(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "passthrough"}[passthrough], func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			account.Extra[codexFingerprintModeExtraKey] = "device"
			account.Extra[codexFingerprintConvergenceExtraKey] = true
			body := wireProfileTestBody(t)
			c := newConvTestContext(t, body)
			svc, _ := wireProfileTestService()
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: 200, minted: "s1"}, {status: 200}}}
			svc.httpUpstream = upstream
			ctx, _ := withOpenAIGatewayPoolSink(context.Background(), c)
			var request *http.Request
			var err error
			if passthrough {
				request, err = svc.buildUpstreamRequestOpenAIPassthrough(ctx, c, account, body, "offline-token")
			} else {
				request, err = svc.buildUpstreamRequest(ctx, c, account, body, "offline-token", true, convTestSession, true)
			}
			require.NoError(t, err)
			request.Header.Del(openAICodexTurnStateHeader)
			expected, err := request.GetBody()
			require.NoError(t, err)
			wire, err := io.ReadAll(expected)
			require.NoError(t, err)
			require.NoError(t, expected.Close())
			gwpoolEchoSeedVerified(t, svc, account)
			response, err := svc.doOpenAIUpstream(request, "", account)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Len(t, upstream.sentBodies, 2)
			require.Equal(t, string(wire), upstream.sentBodies[0], "original compressed business bytes are unchanged")
			require.Equal(t, []string{"", "s1"}, upstream.sentState)
			decoder, err := zstd.NewReader(nil)
			require.NoError(t, err)
			defer decoder.Close()
			confirmation, err := decoder.DecodeAll([]byte(upstream.sentBodies[1]), nil)
			require.NoError(t, err)
			business, err := decoder.DecodeAll(wire, nil)
			require.NoError(t, err)
			require.Equal(t, "hi", gjson.GetBytes(confirmation, "input.0.content.0.text").String())
			require.Equal(t, gjson.GetBytes(body, "model").String(), gjson.GetBytes(business, "model").String())
			require.Equal(t, gatewayPoolProbeModelLuna, gjson.GetBytes(confirmation, "model").String())
			for _, field := range []string{"prompt_cache_key", "client_metadata.session_id", "client_metadata.thread_id"} {
				require.Equal(t, gjson.GetBytes(business, field).Raw, gjson.GetBytes(confirmation, field).Raw)
			}
			require.NotContains(t, OpenAITurnStateUsageSource(c), "gwpool_")
		})
	}
}
