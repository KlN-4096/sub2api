package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type gatewayPoolWSRetryUpstream struct{ gwpoolEchoUpstream }

func (u *gatewayPoolWSRetryUpstream) Do(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
	resp, err := u.gwpoolEchoUpstream.Do(req, proxy, id, concurrency)
	if err == nil && (len(u.sentBodies) == 1 || len(u.sentBodies) == 8) {
		resp.Body = io.NopCloser(strings.NewReader(
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_good\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"))
	}
	return resp, err
}

func (u *gatewayPoolWSRetryUpstream) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, id, concurrency)
}

func TestGatewayPoolWSRetryKeepsConnectionAndSecondTurnBody(t *testing.T) {
	account := wireProfileTestAccount(false)
	account.Credentials["chatgpt_user_id"] = "offline-member"
	account.Extra[openAIGatewayPoolExtraKey] = true
	account.Credentials["access_token"] = "offline-token"
	svc := codexWSWireProfileService(codexWSWireProfileConfig())
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.configure(account)
	first, replacement := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-143")
	fake.cookieForHit = func(hit int64) string {
		if hit > 1 {
			return replacement
		}
		return first
	}
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}, {Name: "unified-143", PairReady: true}}
	svc.accountRepo = &gatewayRuntimeRepo{account: *account}
	upstream := &gatewayPoolWSRetryUpstream{gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: 200}, {status: 200, minted: "s1"}, {status: 200, minted: "s2"},
		{status: 200, minted: "s3"}, {status: 200, minted: "s4"},
		{status: 200, minted: "proof"}, {status: 200, minted: "proof"}, {status: 200},
	}}}
	svc.httpUpstream = upstream
	gwpoolEchoSeedVerified(t, svc, account)
	done := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		pump := StartGatewayPoolWSWaitClient(r.Context(), conn)
		defer pump.Close()
		_, initial, err := pump.Read(pump.Context(), time.Second)
		if err != nil {
			done <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r.WithContext(pump.Context())
		// Freeze short, deterministic sleeps for each independent client turn.
		hooks := &OpenAIWSIngressHooks{ClientReadMessage: pump.Read, BeforeTurn: func(int) error {
			c.Set(gatewayPoolWaitGinKey, &gatewayPoolWaitHolder{state: &gatewayPoolWaitState{
				accountID: account.ID, identity: gwpoolTestIdentity,
				sleep: func(context.Context, time.Duration) error { return nil },
			}})
			return nil
		}}
		done <- svc.ProxyResponsesWebSocketFromClient(pump.Context(), c, conn, account, "offline-token", initial, hooks)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer func() { _ = client.CloseNow() }()
	for _, frame := range []string{
		`{"type":"response.create","model":"gpt-6-astra","input":[{"role":"user","content":"first request"}]}`,
		`{"type":"response.create","model":"gpt-6-astra","previous_response_id":"resp_good","input":[{"role":"user","content":"second request"}]}`,
	} {
		require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(frame)))
		_, response, err := client.Read(ctx)
		require.NoError(t, err, "the same downstream connection must survive internal replacement")
		require.Equal(t, "response.completed", gjson.GetBytes(response, "type").String())
	}
	require.NoError(t, client.CloseNow())
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("client disconnect left ingress alive")
	}
	require.Len(t, upstream.sentBodies, 8)
	require.Equal(t, upstream.sentBodies[1], upstream.sentBodies[7])
	require.Contains(t, upstream.sentBodies[7], "first request", "keep previous-turn context")
	require.Contains(t, upstream.sentBodies[7], "second request")
	require.NotEqual(t, upstream.sentCookies[1], upstream.sentCookies[7])
}

func TestGatewayPoolWSRetryLaterTurnPreambleFailureDoesNotCommit(t *testing.T) {
	account := gwpoolTestAccount(1)
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.configure(account)
	svc := &OpenAIGatewayService{}
	gwpoolEchoSeedVerified(t, svc, account)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: 200, Header: http.Header{},
		Body: io.NopCloser(strings.NewReader(
			"data: {\"type\":\"response.created\",\"response\":{\"id\":\"discarded\"}}\n\n" +
				"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"temporary\"}}}\n\n")),
	}}
	svc.httpUpstream = upstream
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), c)
	payload := []byte(`{"type":"response.create","model":"gpt-6-astra","input":"followup"}`)
	writes := 0
	_, err := svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "offline-token", payload, len(payload),
		"gpt-6-astra", "", "", "", "", 2, func([]byte) error { writes++; return nil })
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.Zero(t, writes, "second-turn metadata must stay private until semantic output")
}
