//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// rawRelayWSUpstream 是本地假 CPR：真 coder/websocket 服务端，记下握手头、收到的帧和连接怎么结束的。
type rawRelayWSUpstream struct {
	server  *httptest.Server
	headers chan http.Header
	conns   chan *coderws.Conn
	frames  chan []byte
	ended   chan error
}

func newRawRelayWSUpstream(t *testing.T, reject http.HandlerFunc) *rawRelayWSUpstream {
	t.Helper()
	u := &rawRelayWSUpstream{
		headers: make(chan http.Header, 4),
		conns:   make(chan *coderws.Conn, 4),
		frames:  make(chan []byte, 16),
		ended:   make(chan error, 4),
	}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.headers <- r.Header.Clone()
		if reject != nil {
			reject(w, r)
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		u.conns <- conn
		for {
			_, payload, err := conn.Read(context.Background())
			if err != nil {
				u.ended <- err
				return
			}
			u.frames <- payload
		}
	}))
	t.Cleanup(u.server.Close)
	return u
}

func rawRelayRecv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}

// rawRelayWSHookLog 按发生顺序记下 handler 钩子被调的情况。
type rawRelayWSHookLog struct {
	mu     sync.Mutex
	events []string
}

func (l *rawRelayWSHookLog) add(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *rawRelayWSHookLog) Events() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func (l *rawRelayWSHookLog) hooks(c *gin.Context) *OpenAIWSIngressHooks {
	return &OpenAIWSIngressHooks{
		InitialRequestModel: "gpt-5.5",
		BeforeRequest: func(turn int, _ []byte, model string) error {
			l.add(fmt.Sprintf("request:%d:%s", turn, model))
			return nil
		},
		BeforeTurn: func(turn int) error {
			l.add(fmt.Sprintf("turn:%d", turn))
			return nil
		},
		AfterTurn: func(turn int, result *OpenAIForwardResult, err error) {
			l.add(fmt.Sprintf("after:%d:in=%d:out=%d:minted=%s:sent=%s:err=%v", turn,
				result.Usage.InputTokens, result.Usage.OutputTokens,
				result.UpstreamHeaders.Get(openAICodexTurnStateHeader), OpenAITurnStateUsageSent(c), err))
		},
	}
}

type rawRelayWSHarness struct {
	upstream  *rawRelayWSUpstream
	client    *coderws.Conn
	serverErr <-chan error
	log       *rawRelayWSHookLog
}

func startRawRelayWS(t *testing.T, reject http.HandlerFunc, header http.Header, first string, hooks func(*rawRelayWSHookLog, *gin.Context) *OpenAIWSIngressHooks) *rawRelayWSHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	upstream := newRawRelayWSUpstream(t, reject)
	cfg := cprTestConfig()
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	svc := &OpenAIGatewayService{cfg: cfg}
	account := newCPRTestAccount()
	account.Credentials["base_url"] = upstream.server.URL
	log := &rawRelayWSHookLog{}
	if hooks == nil {
		hooks = (*rawRelayWSHookLog).hooks
	}
	ingress, serverErr := startPassthroughLifecycleServerWithHooks(t, context.Background(), svc, account, func(c *gin.Context) *OpenAIWSIngressHooks {
		return hooks(log, c)
	})
	t.Cleanup(ingress.Close)

	dialCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(ingress.URL, "http"), &coderws.DialOptions{HTTPHeader: header})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.CloseNow() })
	require.NoError(t, client.Write(dialCtx, coderws.MessageText, []byte(first)))
	return &rawRelayWSHarness{upstream: upstream, client: client, serverErr: serverErr, log: log}
}

func (h *rawRelayWSHarness) send(t *testing.T, conn *coderws.Conn, frame string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(frame)))
}

func (h *rawRelayWSHarness) clientReads(t *testing.T) []byte {
	t.Helper()
	payload, err := readPassthroughLifecycleFrame(t, h.client, 3*time.Second)
	require.NoError(t, err)
	return payload
}

const rawRelayWSFirstFrame = `{"type":"response.create","model":"gpt-5.5",  "input":[{"type":"message","role":"user","content":"a <b> & c"}],"client_metadata":{"x-codex-turn-state":"ts-frame"},"zeta":1,"alpha":2}`

func TestOpenAIRawRelayWSRelaysFramesVerbatimAcrossTurns(t *testing.T) {
	header := http.Header{}
	header.Set("Authorization", "Bearer sk-user-key")
	header.Set("User-Agent", "codex_cli_rs/0.130.0")
	header.Set("originator", "codex_cli_rs")
	header.Set("session_id", "sess-1")
	header.Set("OpenAI-Beta", "responses_websockets=2026-02-06")
	header.Set("X-Custom-Probe", "keep")
	header.Set("Cookie", "session=admin")
	header.Set("X-Forwarded-For", "203.0.113.9")
	header.Set("X-Api-Key", "sk-user-key")
	header.Set("CDN-Loop", "cloudflare")
	header.Set("CF-IPCountry", "CN")
	h := startRawRelayWS(t, nil, header, rawRelayWSFirstFrame, nil)

	got := rawRelayRecv(t, h.upstream.headers)
	require.Equal(t, []string{"Bearer sk-test"}, got.Values("Authorization"), "只换鉴权")
	for _, key := range []string{"User-Agent", "Originator", "Session_id", "Openai-Beta", "X-Custom-Probe"} {
		require.Equal(t, header.Get(key), got.Get(key), key)
	}
	for _, key := range []string{"Cookie", "X-Forwarded-For", "X-Api-Key", "Cdn-Loop", "Cf-Ipcountry"} {
		require.Empty(t, got.Values(key), "本站凭据与转发头不出站：%s", key)
	}
	require.Len(t, got.Values("Sec-Websocket-Key"), 1, "出站握手头由拨号库自己生成，不叠客户端的")
	require.Equal(t, rawRelayWSFirstFrame, string(rawRelayRecv(t, h.upstream.frames)), "首帧逐字节原样")
	up := rawRelayRecv(t, h.upstream.conns)

	for _, event := range []string{
		`{"type":"response.created","response":{"id":"resp_1","model":"gpt-5.5"}}`,
		`{"type":"response.metadata","headers":{"X-Codex-Turn-State":"minted-1","openai-model":"gpt-5.5"}}`,
		`{"type":"response.output_text.delta","delta":"a <b> & c"}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.5","usage":{"input_tokens":11,"output_tokens":7}}}`,
	} {
		h.send(t, up, event)
		require.Equal(t, event, string(h.clientReads(t)), "下行帧逐字节原样")
	}

	second := `{"type":"response.create","model":"gpt-5.5","previous_response_id":"resp_1","input":[]}`
	h.send(t, h.client, second)
	require.Equal(t, second, string(rawRelayRecv(t, h.upstream.frames)))
	h.send(t, up, `{"type":"response.completed","response":{"id":"resp_2","usage":{"input_tokens":3,"output_tokens":2}}}`)
	h.clientReads(t)

	require.NoError(t, h.client.Close(coderws.StatusNormalClosure, "bye"))
	ended := rawRelayRecv(t, h.upstream.ended)
	require.Equal(t, coderws.StatusNormalClosure, coderws.CloseStatus(ended), "客户端的关闭码照抄给上游：%v", ended)
	var closeErr coderws.CloseError
	require.True(t, errors.As(ended, &closeErr))
	require.Equal(t, "bye", closeErr.Reason)
	require.NoError(t, rawRelayRecv(t, h.serverErr))
	require.Equal(t, []string{
		"after:1:in=11:out=7:minted=minted-1:sent=ts-frame:err=<nil>",
		"request:2:gpt-5.5",
		"turn:2",
		"after:2:in=3:out=2:minted=:sent=:err=<nil>",
	}, h.log.Events())
}

func TestOpenAIRawRelayWSMirrorsUpstreamClose(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   coderws.StatusCode
		reason string
	}{
		{"graceful", coderws.StatusGoingAway, "connection lifetime reached"},
		{"custom", 4001, "upstream says bye"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, nil)
			rawRelayRecv(t, h.upstream.frames)
			up := rawRelayRecv(t, h.upstream.conns)
			h.send(t, up, `{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":1,"output_tokens":1}}}`)
			h.clientReads(t)

			require.NoError(t, up.Close(tc.code, tc.reason))
			_, err := readPassthroughLifecycleFrame(t, h.client, 3*time.Second)
			var closeErr coderws.CloseError
			require.True(t, errors.As(err, &closeErr), "%v", err)
			require.Equal(t, tc.code, closeErr.Code)
			require.Equal(t, tc.reason, closeErr.Reason)
			require.NoError(t, rawRelayRecv(t, h.serverErr))
		})
	}
}

func TestOpenAIRawRelayWSClientLeavingMidTurnClosesUpstreamAndRecordsZeroRow(t *testing.T) {
	t.Run("close_frame", func(t *testing.T) {
		h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, nil)
		rawRelayRecv(t, h.upstream.frames)
		up := rawRelayRecv(t, h.upstream.conns)
		h.send(t, up, `{"type":"response.created","response":{"id":"resp_1"}}`)
		h.clientReads(t)

		start := time.Now()
		require.NoError(t, h.client.Close(4002, "user cancelled"))
		ended := rawRelayRecv(t, h.upstream.ended)
		require.Equal(t, coderws.StatusCode(4002), coderws.CloseStatus(ended), "%v", ended)
		require.Less(t, time.Since(start), time.Second, "不为记账多等上游")
		require.NoError(t, rawRelayRecv(t, h.serverErr))
		require.Equal(t, []string{"after:1:in=0:out=0:minted=:sent=ts-frame:err=<nil>"}, h.log.Events(), "没等到终态也记一条 0 token")
	})
	t.Run("dropped", func(t *testing.T) {
		h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, nil)
		rawRelayRecv(t, h.upstream.frames)
		rawRelayRecv(t, h.upstream.conns)

		require.NoError(t, h.client.CloseNow())
		ended := rawRelayRecv(t, h.upstream.ended)
		require.Equal(t, coderws.StatusCode(-1), coderws.CloseStatus(ended), "客户端断线，上游也直接断：%v", ended)
		require.NoError(t, rawRelayRecv(t, h.serverErr))
		require.Len(t, h.log.Events(), 1)
	})
}

func TestOpenAIRawRelayWSErrorEventEndsTurnAndKeepsConnection(t *testing.T) {
	h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, nil)
	rawRelayRecv(t, h.upstream.frames)
	up := rawRelayRecv(t, h.upstream.conns)

	errEvent := `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limit"}}`
	h.send(t, up, errEvent)
	require.Equal(t, errEvent, string(h.clientReads(t)))

	second := `{"type":"response.create","model":"gpt-5.5","input":[]}`
	h.send(t, h.client, second)
	require.Equal(t, second, string(rawRelayRecv(t, h.upstream.frames)), "连接不断，下一轮照常发")
	h.send(t, up, `{"type":"response.completed","response":{"id":"resp_2","usage":{"input_tokens":5,"output_tokens":4}}}`)
	h.clientReads(t)
	require.NoError(t, h.client.Close(coderws.StatusNormalClosure, ""))
	require.NoError(t, rawRelayRecv(t, h.serverErr))
	require.Equal(t, []string{
		"after:1:in=0:out=0:minted=:sent=ts-frame:err=<nil>",
		"request:2:gpt-5.5",
		"turn:2",
		"after:2:in=5:out=4:minted=:sent=:err=<nil>",
	}, h.log.Events(), "上一轮先结算，下一轮才开始")
}

func TestOpenAIRawRelayWSRejectsOverlappingTurn(t *testing.T) {
	// 二进制帧也一样：apikey 原样中继的下一跳是 sub2api，它会执行二进制的 response.create。
	for _, msgType := range []coderws.MessageType{coderws.MessageText, coderws.MessageBinary} {
		t.Run(msgType.String(), func(t *testing.T) {
			h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, nil)
			rawRelayRecv(t, h.upstream.frames)
			rawRelayRecv(t, h.upstream.conns)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			require.NoError(t, h.client.Write(ctx, msgType, []byte(`{"type":"response.create","model":"gpt-5.5","input":[]}`)))
			_, err := readPassthroughLifecycleFrame(t, h.client, 3*time.Second)
			require.Equal(t, coderws.StatusPolicyViolation, coderws.CloseStatus(err), "%v", err)
			var closeErr *OpenAIWSClientCloseError
			require.ErrorAs(t, rawRelayRecv(t, h.serverErr), &closeErr)
			require.NotContains(t, strings.Join(h.log.Events(), ","), "turn:2", "重叠的一轮不占并发槽")
		})
	}
}

// 二进制帧同样过准入、分组策略与计费。
func TestOpenAIRawRelayWSBinaryFrameIsATurn(t *testing.T) {
	h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, func(l *rawRelayWSHookLog, c *gin.Context) *OpenAIWSIngressHooks {
		hooks := l.hooks(c)
		hooks.MaxReasoningEffort = "low"
		hooks.MaxReasoningEffortOverLimit = "downgrade"
		return hooks
	})
	rawRelayRecv(t, h.upstream.frames)
	up := rawRelayRecv(t, h.upstream.conns)
	h.send(t, up, `{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	h.clientReads(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, h.client.Write(ctx, coderws.MessageBinary, []byte(`{"type":"response.create","model":"gpt-5.5","reasoning":{"effort":"xhigh"},"input":[]}`)))
	require.Equal(t, `{"type":"response.create","model":"gpt-5.5","reasoning":{"effort":"low"},"input":[]}`, string(rawRelayRecv(t, h.upstream.frames)))
	h.send(t, up, `{"type":"response.completed","response":{"id":"resp_2","usage":{"input_tokens":7,"output_tokens":7}}}`)
	h.clientReads(t)
	require.NoError(t, h.client.Close(coderws.StatusNormalClosure, ""))
	require.NoError(t, rawRelayRecv(t, h.serverErr))
	require.Equal(t, []string{
		"after:1:in=1:out=1:minted=:sent=ts-frame:err=<nil>",
		"request:2:gpt-5.5",
		"turn:2",
		"after:2:in=7:out=7:minted=:sent=:err=<nil>",
	}, h.log.Events())
}

func TestOpenAIRawRelayWSPatchesOnlyPolicyFields(t *testing.T) {
	first := `{"type":"response.create","model":"gpt-5.5","reasoning":{"effort":"xhigh","summary":"auto"},"input":"hi","z":[1,2]}`
	h := startRawRelayWS(t, nil, nil, first, func(l *rawRelayWSHookLog, c *gin.Context) *OpenAIWSIngressHooks {
		hooks := l.hooks(c)
		hooks.MaxReasoningEffort = "low"
		hooks.MaxReasoningEffortOverLimit = "downgrade"
		hooks.MapRequestModel = func(int, string) (string, error) { return "gpt-5.5-mapped", nil }
		return hooks
	})
	want := `{"type":"response.create","model":"gpt-5.5-mapped","reasoning":{"effort":"low","summary":"auto"},"input":"hi","z":[1,2]}`
	require.Equal(t, want, string(rawRelayRecv(t, h.upstream.frames)))
}

func TestOpenAIRawRelayWSHandshakeUnavailableFailsOver(t *testing.T) {
	h := startRawRelayWS(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"code":"no_available_provider","message":"no provider"}}`)
	}, nil, rawRelayWSFirstFrame, nil)

	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, rawRelayRecv(t, h.serverErr), &failoverErr)
	require.True(t, failoverErr.RawRelayResponse)
	require.Equal(t, http.StatusServiceUnavailable, failoverErr.StatusCode)
	event := failoverErr.ResponseBody
	require.Equal(t, "error", gjson.GetBytes(event, "type").String())
	require.EqualValues(t, 503, gjson.GetBytes(event, "status").Int())
	require.JSONEq(t, `{"code":"no_available_provider","message":"no provider"}`, gjson.GetBytes(event, "error").Raw)
	require.Equal(t, "7", gjson.GetBytes(event, "headers.retry-after").String())
	require.Empty(t, h.log.Events())
}

func TestOpenAIRawRelayWSHandshakeRejectionReachesClientAsErrorEvent(t *testing.T) {
	h := startRawRelayWS(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUpgradeRequired)
		_, _ = io.WriteString(w, `{"error":{"message":"Please upgrade Codex","type":"invalid_request_error"}}`)
	}, nil, rawRelayWSFirstFrame, nil)

	event := h.clientReads(t)
	require.EqualValues(t, http.StatusUpgradeRequired, gjson.GetBytes(event, "status").Int(), "%s", event)
	require.Equal(t, "Please upgrade Codex", gjson.GetBytes(event, "error.message").String())
	_, err := readPassthroughLifecycleFrame(t, h.client, 3*time.Second)
	require.Equal(t, coderws.StatusPolicyViolation, coderws.CloseStatus(err), "%v", err)
	var failoverErr *UpstreamFailoverError
	serverErr := rawRelayRecv(t, h.serverErr)
	require.Error(t, serverErr)
	require.False(t, errors.As(serverErr, &failoverErr), "普通拒绝不换号")
}

func TestOpenAIRawRelayWSEventWrapsNonJSONBody(t *testing.T) {
	event := openAIRawRelayWSErrorEvent(http.StatusBadGateway, nil, []byte("<html>bad gateway</html>"))
	require.JSONEq(t, `{"type":"error","status":502,"error":{"message":"<html>bad gateway</html>"}}`, string(event))
	event = openAIRawRelayWSErrorEvent(http.StatusUnauthorized, nil, []byte(`{"code":"INVALID_API_KEY","message":"Invalid API key"}`))
	require.JSONEq(t, `{"type":"error","status":401,"error":{"code":"INVALID_API_KEY","message":"Invalid API key"}}`, string(event))
}

func TestOpenAIRawRelayWSAccountsSelectableForWebSocket(t *testing.T) {
	cfg := cprTestConfig()
	cfg.Gateway.OpenAIWS.Enabled = true
	svc := &OpenAIGatewayService{cfg: cfg}
	account := newCPRTestAccount()
	require.True(t, svc.isOpenAIAccountTransportCompatible(account, OpenAIUpstreamTransportResponsesWebsocketV2Ingress))

	cfg.Gateway.OpenAIWS.Enabled = false
	require.False(t, svc.isOpenAIAccountTransportCompatible(account, OpenAIUpstreamTransportResponsesWebsocketV2Ingress), "全局关 WS")
	cfg.Gateway.OpenAIWS.Enabled = true
	account.Extra["openai_ws_force_http"] = true
	require.False(t, svc.isOpenAIAccountTransportCompatible(account, OpenAIUpstreamTransportResponsesWebsocketV2Ingress))
	delete(account.Extra, "openai_ws_force_http")
	cfg.Gateway.OpenAIWS.ForceHTTP = true
	require.False(t, svc.isOpenAIAccountTransportCompatible(account, OpenAIUpstreamTransportResponsesWebsocketV2Ingress))

	_, _, armed := svc.BeginOpenAIWSIngressSessionPreemption(context.Background(), nil, account, []byte(rawRelayWSFirstFrame))
	require.False(t, armed, "原样中继不替上游互相顶掉会话")
}

// CPR 把每个文本帧都当 response.create 解码，所以任何文本帧都按一轮准入与计费，不看 type。
func TestOpenAIRawRelayWSEveryTextFrameIsATurn(t *testing.T) {
	h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, nil)
	rawRelayRecv(t, h.upstream.frames)
	up := rawRelayRecv(t, h.upstream.conns)
	h.send(t, up, `{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":1,"output_tokens":1}}}`)
	h.clientReads(t)

	other := `{"type":"session.update","model":"gpt-5.5","input":[]}`
	h.send(t, h.client, other)
	require.Equal(t, other, string(rawRelayRecv(t, h.upstream.frames)), "照样原样转发")
	h.send(t, up, `{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"unsupported Responses WebSocket message type"}}`)
	h.clientReads(t)

	h.send(t, h.client, `{"type":"response.create","model":"gpt-5.5","input":[]}`)
	rawRelayRecv(t, h.upstream.frames)
	h.send(t, up, `{"type":"response.completed","response":{"id":"resp_3","usage":{"input_tokens":2,"output_tokens":2}}}`)
	h.clientReads(t)
	require.NoError(t, h.client.Close(coderws.StatusNormalClosure, ""))
	require.NoError(t, rawRelayRecv(t, h.serverErr))
	require.Equal(t, []string{
		"after:1:in=1:out=1:minted=:sent=ts-frame:err=<nil>",
		"request:2:gpt-5.5",
		"turn:2",
		"after:2:in=0:out=0:minted=:sent=:err=<nil>",
		"request:3:gpt-5.5",
		"turn:3",
		"after:3:in=2:out=2:minted=:sent=:err=<nil>",
	}, h.log.Events())
}

// 本站按首个键做准入与策略、CPR 按末个键生效：带重复键的帧不出站，按策略违规关连接。
func TestOpenAIRawRelayWSRejectsDuplicateKeys(t *testing.T) {
	t.Run("first_frame", func(t *testing.T) {
		first := `{"type":"response.create","model":"gpt-5.5","reasoning":{"effort":"low","effort":"xhigh"},"input":[]}`
		h := startRawRelayWS(t, nil, nil, first, nil)
		err := rawRelayRecv(t, h.serverErr)
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, err, &closeErr)
		require.Equal(t, coderws.StatusPolicyViolation, closeErr.StatusCode())
		require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "用户侧拒绝不算账号失败")
		select {
		case <-h.upstream.headers:
			t.Fatal("首帧被拒前不该连上游")
		case <-time.After(100 * time.Millisecond):
		}
	})
	t.Run("later_frame", func(t *testing.T) {
		h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, nil)
		rawRelayRecv(t, h.upstream.frames)
		up := rawRelayRecv(t, h.upstream.conns)
		h.send(t, up, `{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":1,"output_tokens":1}}}`)
		h.clientReads(t)

		h.send(t, h.client, `{"type":"session.update","model":"gpt-5.5","type":"response.create","input":[]}`)
		_, err := readPassthroughLifecycleFrame(t, h.client, 3*time.Second)
		require.Equal(t, coderws.StatusPolicyViolation, coderws.CloseStatus(err), "%v", err)
		require.ErrorIs(t, rawRelayRecv(t, h.serverErr), ErrOpenAIRawRelayNotAccountFault)
		require.NotContains(t, strings.Join(h.log.Events(), ","), "request:2", "被拒的帧不过准入、不占并发槽")
		select {
		case frame := <-h.upstream.frames:
			t.Fatalf("被拒的帧不该出站：%s", frame)
		default:
		}
	})
}

// CPR 上游一上来就报错的一轮没有 response id：时长按放行时刻算，记账键各自独立。
func TestOpenAIRawRelayWSErrorTurnTimingAndRequestID(t *testing.T) {
	type after struct {
		turn      int
		duration  time.Duration
		requestID string
	}
	afters := make(chan after, 4)
	started := make(chan int, 4)
	h := startRawRelayWS(t, nil, nil, rawRelayWSFirstFrame, func(l *rawRelayWSHookLog, c *gin.Context) *OpenAIWSIngressHooks {
		hooks := l.hooks(c)
		hooks.TurnStarted = func(turn int, _ time.Time) { started <- turn }
		hooks.AfterTurn = func(turn int, result *OpenAIForwardResult, _ error) {
			afters <- after{turn, result.Duration, result.RequestID}
		}
		return hooks
	})
	rawRelayRecv(t, h.upstream.frames)
	up := rawRelayRecv(t, h.upstream.conns)
	time.Sleep(60 * time.Millisecond)
	h.send(t, up, `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limit"}}`)
	h.clientReads(t)
	first := rawRelayRecv(t, afters)
	require.Equal(t, 1, rawRelayRecv(t, started))
	require.GreaterOrEqual(t, first.duration, 50*time.Millisecond)
	require.True(t, strings.HasPrefix(first.requestID, "generated:"), first.requestID)

	h.send(t, h.client, `{"type":"response.create","model":"gpt-5.5","input":[]}`)
	rawRelayRecv(t, h.upstream.frames)
	h.send(t, up, `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limit"}}`)
	h.clientReads(t)
	second := rawRelayRecv(t, afters)
	require.NotEqual(t, first.requestID, second.requestID, "同一连接的失败轮不能共用记账键")
	require.NoError(t, h.client.Close(coderws.StatusNormalClosure, ""))
	require.NoError(t, rawRelayRecv(t, h.serverErr))
}
