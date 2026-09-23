package handler

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 原样中继账号换号耗尽：客户端拿到的是上游原始响应，不经错误映射。
func TestOpenAIRawRelayFailoverExhausted_WritesUpstreamVerbatim(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))
	body := []byte(`{"error":{"code":"concurrency_queue_full","message":"busy"}}`)

	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode:       http.StatusTooManyRequests,
		ResponseBody:     body,
		ResponseHeaders:  http.Header{"Content-Type": []string{"application/json"}, "X-Gateway-Request-Id": []string{"gw-9"}},
		RawRelayResponse: true,
	}, false)

	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, string(body), rec.Body.String())
	require.Equal(t, "gw-9", rec.Header().Get("X-Gateway-Request-Id"))
}

// WS 下换号耗尽：先把上游那条错误帧原样发给客户端，再按状态码关连接。
func TestOpenAIRawRelayWSFailoverExhausted_SendsUpstreamErrorFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)
	frame := []byte(`{"type":"error","status":503,"error":{"code":"no_available_provider","message":"no provider"}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		closeOpenAIWSFailoverExhausted(c, conn, &service.UpstreamFailoverError{
			StatusCode:       http.StatusServiceUnavailable,
			ResponseBody:     frame,
			RawRelayResponse: true,
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer func() { _ = client.CloseNow() }()
	_, got, err := client.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, string(frame), string(got))
	_, _, err = client.Read(ctx)
	require.Equal(t, coderws.StatusTryAgainLater, coderws.CloseStatus(err), "%v", err)
}

// 端到端：handler 选中原样中继账号（apikey 开关 / cpr），两轮帧逐字节到上游，每轮各记一条用量。
func TestOpenAIResponsesWebSocket_RawRelayRelaysTurnsVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name string
		cpr  bool
	}{{"apikey_switch", false}, {"cpr", true}} {
		t.Run(tc.name, func(t *testing.T) {
			first := `{"type":"response.create","model":"gpt-5.4",  "stream":false,"zeta":1,"input":[{"type":"message","role":"user","content":"a <b> & c"}]}`
			second := `{"type":"response.create","model":"gpt-5.4","previous_response_id":"resp_usage_e2e_1","input":[]}`
			got := runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
				firstPayload:  first,
				secondPayload: second,
				userAgent:     testStringPtr("codex_cli_rs/0.130.0"),
				rawRelay:      !tc.cpr,
				cpr:           tc.cpr,
				clientHeaders: map[string]string{"X-Klno-Probe": "raw"},
			})
			// 客户端自定义头只有原样中继会带到上游，借它确认走的是原样中继而不是旧透传。
			require.Equal(t, "raw", got.upstreamHeader.Get("X-Klno-Probe"))
			require.Equal(t, []string{first, second}, []string{string(got.upstreamPayloads[0]), string(got.upstreamPayloads[1])})
			require.Len(t, got.logs, 2)
			for _, usageLog := range got.logs {
				require.Equal(t, 2, usageLog.InputTokens)
				require.Equal(t, 1, usageLog.OutputTokens)
				require.True(t, usageLog.OpenAIWSMode)
			}
		})
	}
}

// 原样中继的用户侧拒绝、握手拒绝透传不算账号失败，不拉低调度统计；普通代理失败照旧上报。
func TestShouldReportOpenAIWSProxyAccountFailure_RawRelayNotAccountFault(t *testing.T) {
	closeErr := service.NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "upstream websocket handshake rejected", nil)
	require.False(t, shouldReportOpenAIWSProxyAccountFailure(fmt.Errorf("%w: %w", service.ErrOpenAIRawRelayNotAccountFault, closeErr)))
	require.True(t, shouldReportOpenAIWSProxyAccountFailure(closeErr))
}
