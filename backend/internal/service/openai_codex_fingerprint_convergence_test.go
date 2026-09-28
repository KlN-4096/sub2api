package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// 对照 openai/codex codex-rs（commit 16ff14c）的出站身份恒等式，开关开启时三条路径必须同时满足：
//
//	头 session-id == body prompt_cache_key == client_metadata.session_id
//	根会话 session-id == thread-id（子代理二者不等，各自独立派生）
//	头 thread-id  == 头 x-client-request-id == client_metadata.thread_id
//	client_metadata.root_turn_id == client_metadata.turn_id
//	头 x-codex-window-id == client_metadata.x-codex-window-id == turn-metadata.window_id == "<派生 thread-id>:<n>"
//	turn-metadata.context_window_id 单独派生、保持 v7
//	发出 session-id 时不再发 session_id / conversation_id 别名；v7 原始值派生后仍为 v7 且时间戳前缀不变；v4 原始值仍为 v4
//
// 开关关闭时行为与上游完全一致。
const (
	convTestSession      = "01a07c73-e312-76e1-9054-e4665b8ee0a7" // v7，codex 0.153 真实形态
	convTestThread       = "01a07c73-e312-76e1-9054-e4665b8ee0a7" // 新线程：thread_id == session_id
	convTestTurn         = "01a07c73-e3a0-7ae1-baf0-ce1c532f019c" // v7
	convTestWindow       = convTestThread + ":1"                  // 真实形态 "<thread>:<window_number>"（session/mod.rs current_window）
	convTestContext      = "01a07c73-e312-76e1-9054-e4722b79a205" // context_window_id，v7
	convTestInstallation = "7f582abd-05d2-4a59-b4e5-ec1b733b4edc" // v4（installation_id.rs new_v4）
	convTestParentThread = "01a07c60-1111-7aaa-8bbb-cccccccccccc" // v7
)

func convTestTurnMetadata() string {
	return `{"agent_name":"x","installation_id":"` + convTestInstallation + `","session_id":"` + convTestSession +
		`","thread_id":"` + convTestThread + `","turn_id":"` + convTestTurn + `","window_id":"` + convTestWindow +
		`","context_window_id":"` + convTestContext + `","root_turn_id":"` + convTestTurn + `","window_number":1}`
}

func convTestBody(t *testing.T) []byte {
	t.Helper()
	meta := map[string]any{
		"session_id":               convTestSession,
		"thread_id":                convTestThread,
		"turn_id":                  convTestTurn,
		"root_turn_id":             convTestTurn,
		"x-codex-installation-id":  convTestInstallation,
		"x-codex-window-id":        convTestWindow,
		"x-codex-parent-thread-id": convTestParentThread,
		"x-codex-turn-metadata":    convTestTurnMetadata(),
	}
	body := map[string]any{
		"model": "gpt-5.5", "stream": true, "store": false,
		"prompt_cache_key": convTestSession,
		"client_metadata":  meta,
		"input":            []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hi"}}}},
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return raw
}

func newConvTestContext(t *testing.T, body []byte) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	c.Set("api_key_id", int64(77))
	c.Set("api_key", &APIKey{ID: 77})
	h := c.Request.Header
	h.Set("User-Agent", "codex_cli_rs/0.153.4")
	h.Set("originator", "codex_cli_rs")
	h.Set("content-type", "application/json")
	h.Set("session-id", convTestSession)
	h.Set("thread-id", convTestThread)
	h.Set("x-client-request-id", convTestThread)
	h.Set("x-codex-installation-id", convTestInstallation)
	h.Set("x-codex-window-id", convTestWindow)
	h.Set("x-codex-parent-thread-id", convTestParentThread)
	h.Set("x-openai-subagent", "explorer")
	h.Set("x-codex-turn-metadata", convTestTurnMetadata())
	return c
}

func convTestAccount(enabled bool) *Account {
	extra := map[string]any{}
	if enabled {
		extra[codexFingerprintConvergenceExtraKey] = true
	}
	return &Account{ID: 11, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{"chatgpt_account_id": "chatgpt-account-11", "chatgpt_user_id": "user-11"},
		Extra:       extra}
}

func requireV7SameTimestamp(t *testing.T, raw, derived, label string) {
	t.Helper()
	parsed, err := uuid.Parse(derived)
	require.NoError(t, err, label)
	require.Equal(t, uuid.Version(7), parsed.Version(), "%s 应保持 v7", label)
	require.NotEqual(t, raw, derived, "%s 必须被命名空间化", label)
	require.Equal(t, raw[:13], derived[:13], "%s 应保留 48 位时间戳", label)
}

type convOutbound struct {
	label   string
	headers http.Header
	body    []byte // 可为空（WS 握手）
}

func requireConvergenceInvariants(t *testing.T, o convOutbound) {
	t.Helper()
	h := o.headers
	sid, tid := h.Get("session-id"), h.Get("thread-id")
	requireV7SameTimestamp(t, convTestSession, sid, o.label+" session-id")
	requireV7SameTimestamp(t, convTestThread, tid, o.label+" thread-id")
	require.Equal(t, tid, h.Get("x-client-request-id"), o.label+" x-client-request-id 必须等于 thread-id")
	// 根会话的 session_id 就是根线程的 ID（core/src/session/session.rs:791
	// session_id = SessionId::from(thread_id)）。夹具是根会话形态，派生后必须仍相等，
	// 否则每个请求在上游看来都是子代理线程，真客户端不存在这种形态。
	require.Equal(t, sid, tid, o.label+" 根会话 session-id 必须等于 thread-id")
	require.Empty(t, h.Get("session_id"), o.label+" 不应发 session_id")
	require.Empty(t, h.Get("conversation_id"), o.label+" 不应发 conversation_id")
	requireV7SameTimestamp(t, convTestParentThread, h.Get("x-codex-parent-thread-id"), o.label+" x-codex-parent-thread-id")
	require.Equal(t, "explorer", h.Get("x-openai-subagent"), o.label+" x-openai-subagent 原样透传")

	windowID := h.Get("x-codex-window-id")
	require.Equal(t, tid+":1", windowID, o.label+" x-codex-window-id 应为 <派生 thread-id>:<window_number>")
	installation := h.Get("x-codex-installation-id")
	if installation == "" {
		installation = gjson.Get(h.Get(openAIWSTurnMetadataHeader), "installation_id").String()
	}
	require.NotEmpty(t, installation, o.label+" 设备身份必须保留在正确载体中")
	if installation != "" {
		parsed, err := uuid.Parse(installation)
		require.NoError(t, err)
		require.Equal(t, uuid.Version(4), parsed.Version(), o.label+" installation 原始 v4 派生仍应 v4")
		require.NotEqual(t, convTestInstallation, installation)
	}
	headerMeta := gjson.Parse(h.Get("x-codex-turn-metadata"))
	require.True(t, headerMeta.IsObject(), o.label+" 头部 turn-metadata 应为 JSON 对象")
	require.Equal(t, sid, headerMeta.Get("session_id").String(), o.label+" 头部 turn-metadata.session_id")
	require.Equal(t, tid, headerMeta.Get("thread_id").String(), o.label+" 头部 turn-metadata.thread_id")
	require.Equal(t, windowID, headerMeta.Get("window_id").String(), o.label+" 头部 turn-metadata.window_id")
	requireV7SameTimestamp(t, convTestContext, headerMeta.Get("context_window_id").String(), o.label+" 头部 turn-metadata.context_window_id")
	require.Equal(t, headerMeta.Get("turn_id").String(), headerMeta.Get("root_turn_id").String(), o.label+" 头部 turn-metadata.root_turn_id 应等于 turn_id")
	requireV7SameTimestamp(t, convTestTurn, headerMeta.Get("turn_id").String(), o.label+" 头部 turn-metadata.turn_id")

	if len(o.body) == 0 {
		return
	}
	b := gjson.ParseBytes(o.body)
	require.Equal(t, sid, b.Get("prompt_cache_key").String(), o.label+" prompt_cache_key")
	cm := b.Get("client_metadata")
	require.Equal(t, sid, cm.Get("session_id").String(), o.label+" client_metadata.session_id")
	require.Equal(t, tid, cm.Get("thread_id").String(), o.label+" client_metadata.thread_id")
	require.Equal(t, cm.Get("turn_id").String(), cm.Get("root_turn_id").String(), o.label+" client_metadata.root_turn_id 应等于 turn_id")
	requireV7SameTimestamp(t, convTestTurn, cm.Get("turn_id").String(), o.label+" client_metadata.turn_id")
	require.Equal(t, windowID, cm.Get("x-codex-window-id").String(), o.label+" client_metadata.x-codex-window-id")
	require.Equal(t, h.Get("x-codex-parent-thread-id"), cm.Get("x-codex-parent-thread-id").String(), o.label+" client_metadata.x-codex-parent-thread-id")
	require.Equal(t, installation, cm.Get("x-codex-installation-id").String(), o.label+" client_metadata.x-codex-installation-id")
	embedded := gjson.Parse(cm.Get("x-codex-turn-metadata").String())
	requireV7SameTimestamp(t, convTestContext, embedded.Get("context_window_id").String(), o.label+" 嵌入 turn-metadata.context_window_id")
	require.Equal(t, embedded.Get("turn_id").String(), embedded.Get("root_turn_id").String(), o.label+" 嵌入 turn-metadata.root_turn_id")
	require.Equal(t, headerMeta.Get("context_window_id").String(), embedded.Get("context_window_id").String(), o.label+" 头部与嵌入 turn-metadata 同值")
}

func TestCodexFingerprintConvergence_UnifiedAcrossCarriers(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	rawBody := convTestBody(t)

	// HTTP 非透传：Forward() 先对解码后的 body 派生，再由 buildUpstreamRequest 出头。
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(rawBody, &decoded))
	require.True(t, applyCodexAccountIdentityClientMetadataMap(decoded, account, 77))
	scopedBody, err := json.Marshal(decoded)
	require.NoError(t, err)
	c := newConvTestContext(t, rawBody)
	httpReq, err := svc.buildUpstreamRequest(context.Background(), c, account, scopedBody, "tok", true, convTestSession, true)
	require.NoError(t, err)
	requireConvergenceInvariants(t, convOutbound{"HTTP", httpReq.Header, scopedBody})

	// 透传：在原始字节上派生，再构造请求。
	scopedRaw, changed, err := applyCodexAccountIdentityClientMetadataRaw(rawBody, account, 77)
	require.NoError(t, err)
	require.True(t, changed)
	c = newConvTestContext(t, rawBody)
	ptReq, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, scopedRaw, "tok")
	require.NoError(t, err)
	requireConvergenceInvariants(t, convOutbound{"透传", ptReq.Header, scopedRaw})

	// WS 握手。
	c = newConvTestContext(t, rawBody)
	wsHeaders, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "tok",
		OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
		true, "", convTestTurnMetadata(), convTestSession, "", "")
	require.NoError(t, err)
	requireConvergenceInvariants(t, convOutbound{"WS", wsHeaders, nil})

	for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "x-codex-window-id", "x-codex-parent-thread-id"} {
		require.Equal(t, httpReq.Header.Get(name), ptReq.Header.Get(name), "HTTP 与透传 %s 应一致", name)
		require.Equal(t, httpReq.Header.Get(name), wsHeaders.Get(name), "HTTP 与 WS %s 应一致", name)
	}
}

// WS 握手头与帧内 client_metadata 是两次独立处理（握手一次，之后每个 response.create
// 帧一次），必须落到同一身份：真实客户端一条连接内两者同源自一份 CodexResponsesMetadata。
func TestCodexFingerprintConvergence_WSFramePayloadMatchesHandshakeHeaders(t *testing.T) {
	svc := &OpenAIGatewayService{}
	// 双开，与 pro1 线上配置一致：device 模式会替换 installation，握手与帧两侧
	// 必须同时被替换成同一个值。
	account := convTestAccount(true)
	account.Extra[codexFingerprintModeExtraKey] = string(codexFingerprintDevice)
	account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
	wantInstall := resolveConvergedInstallationID(account, "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f")
	require.NotEmpty(t, wantInstall)
	rawBody := convTestBody(t)
	c := newConvTestContext(t, rawBody)

	// 生产顺序：帧身份先落（原生 WS 入口 openai_ws_forwarder_ingress.go:323 的
	// applyCodexIdentityToWSPayload 早于 :767 的握手构造；HTTP→WS 桥接则由
	// forwardOpenAI 在 forwardOpenAIWSV2 之前 stage），握手头再读暂存的同一份 IDs。
	// 顺序不能反：stagedCodexFingerprintIDs 在未暂存时返回 nil 且不惰性求值，
	// 先建连会让握手漏掉 device 收敛。
	payload, err := applyCodexIdentityToWSPayload(c, account, rawBody)
	require.NoError(t, err)

	wsHeaders, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "tok",
		OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
		true, "", convTestTurnMetadata(), convTestSession, "", "")
	require.NoError(t, err)
	cm := gjson.ParseBytes(payload).Get("client_metadata")
	require.True(t, cm.IsObject(), "帧内应有 client_metadata")
	// 两边同时为空也会让下面的相等断言通过，先钉住非空。
	require.NotEmpty(t, wsHeaders.Get("session-id"))
	require.Empty(t, wsHeaders.Get("x-codex-installation-id"))
	headerInstallation := gjson.Get(wsHeaders.Get(openAIWSTurnMetadataHeader), "installation_id").String()
	require.NotEmpty(t, headerInstallation)

	require.Equal(t, wsHeaders.Get("session-id"), cm.Get("session_id").String(),
		"握手头 session-id 与帧内 client_metadata.session_id 必须同源")
	require.Equal(t, wsHeaders.Get("thread-id"), cm.Get("thread_id").String(),
		"握手头 thread-id 与帧内 client_metadata.thread_id 必须同源")
	require.Equal(t, wsHeaders.Get("x-codex-window-id"), cm.Get("x-codex-window-id").String(),
		"握手头与帧内 window-id 必须同源")
	require.Equal(t, headerInstallation, cm.Get("x-codex-installation-id").String(),
		"握手 turn-metadata 与帧内 installation 必须同源")
	require.Equal(t, wsHeaders.Get("session-id"), gjson.ParseBytes(payload).Get("prompt_cache_key").String(),
		"帧内 prompt_cache_key 必须等于握手头 session-id")
	require.Equal(t, wantInstall, headerInstallation,
		"双开时握手 turn-metadata 必须落到 device 模式的账号固定 installation")
}

// codex 复核四 #2：x-openai-memgen-request 与 x-responsesapi-include-timing-metrics
// 加白名单时只接到了 HTTP 两张表，WS 用的是独立的入站头拷贝列表，漏掉后走 WS 的
// 请求会让上游看到一个「从不做记忆整合、从不开计时」的客户端。真实 WS 握手条件性
// 携带这两个头：前者出自 build_responses_compatibility_headers（codex-rs
// core/src/client.rs:817，被 build_websocket_headers 于 :1252 extend），
// 后者由 build_websocket_headers 直接插入（同文件 :1262）。
func TestCodexFingerprintConvergence_WSHandshakeForwardsCompatibilityHeaders(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	rawBody := convTestBody(t)
	c := newConvTestContext(t, rawBody)
	c.Request.Header.Set("x-openai-memgen-request", "true")
	c.Request.Header.Set("x-responsesapi-include-timing-metrics", "true")

	wsHeaders, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "tok",
		OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
		true, "", convTestTurnMetadata(), convTestSession, "", "")
	require.NoError(t, err)

	for _, name := range []string{"x-openai-memgen-request", "x-responsesapi-include-timing-metrics"} {
		require.Equal(t, "true", wsHeaders.Get(name), "WS 握手必须转发 %s（HTTP 白名单已放行）", name)
		require.True(t, openaiAllowedHeaders[name], "HTTP 白名单也必须放行 %s，两条路径不得漂移", name)
	}
}

// 开关关闭：出站与上游 v0.2.2 完全一致（HTTP 不带三个头、下划线别名走旧隔离哈希、
// x-client-request-id 独立派生、root_turn_id 原样、派生一律 v4）。
func TestCodexFingerprintConvergence_OffMatchesUpstream(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(false)
	rawBody := convTestBody(t)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(rawBody, &decoded))
	require.True(t, applyCodexAccountIdentityClientMetadataMap(decoded, account, 77))
	scopedBody, err := json.Marshal(decoded)
	require.NoError(t, err)
	cm := gjson.GetBytes(scopedBody, "client_metadata")
	require.Equal(t, convTestTurn, cm.Get("root_turn_id").String(), "关闭时 root_turn_id 原样透传")
	require.NotEqual(t, convTestTurn, cm.Get("turn_id").String())
	parsed, err := uuid.Parse(cm.Get("turn_id").String())
	require.NoError(t, err)
	require.Equal(t, uuid.Version(4), parsed.Version(), "关闭时派生保持 v4")

	c := newConvTestContext(t, rawBody)
	httpReq, err := svc.buildUpstreamRequest(context.Background(), c, account, scopedBody, "tok", true, convTestSession, true)
	require.NoError(t, err)
	require.Empty(t, httpReq.Header.Get("session-id"), "关闭时 HTTP 白名单不放行 session-id")
	require.Empty(t, httpReq.Header.Get("thread-id"))
	require.Empty(t, httpReq.Header.Get("x-client-request-id"))
	require.Equal(t, isolateOpenAIUpstreamSessionID(77, account, convTestSession), httpReq.Header.Get("session_id"), "关闭时别名走旧隔离哈希")

	c = newConvTestContext(t, rawBody)
	wsHeaders, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "tok",
		OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
		true, "", convTestTurnMetadata(), convTestSession, "", "")
	require.NoError(t, err)
	require.Equal(t, scopeCodexAccountIdentityValue(account, 77, "request", convTestThread), wsHeaders.Get("x-client-request-id"), "关闭时 x-client-request-id 独立派生")
	require.NotEqual(t, wsHeaders.Get("thread-id"), wsHeaders.Get("x-client-request-id"))
	require.Equal(t, isolateOpenAIUpstreamSessionID(77, account, convTestSession), wsHeaders.Get("session_id"))
}

func TestCodexFingerprintConvergence_SwitchParsing(t *testing.T) {
	cases := []struct {
		name  string
		extra map[string]any
		typ   string
		want  bool
	}{
		{"bool true", map[string]any{codexFingerprintConvergenceExtraKey: true}, AccountTypeOAuth, true},
		{"string true", map[string]any{codexFingerprintConvergenceExtraKey: "true"}, AccountTypeOAuth, true},
		{"string 1", map[string]any{codexFingerprintConvergenceExtraKey: "1"}, AccountTypeOAuth, true},
		{"bool false", map[string]any{codexFingerprintConvergenceExtraKey: false}, AccountTypeOAuth, false},
		{"missing", map[string]any{}, AccountTypeOAuth, false},
		{"nil extra", nil, AccountTypeOAuth, false},
		{"api key account", map[string]any{codexFingerprintConvergenceExtraKey: true}, AccountTypeAPIKey, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			account := &Account{Platform: PlatformOpenAI, Type: tc.typ, Extra: tc.extra}
			require.Equal(t, tc.want, codexFingerprintConvergenceEnabled(account))
		})
	}
	require.False(t, codexFingerprintConvergenceEnabled(nil))
}

func TestCodexFingerprintConvergence_DeriveKeepsVersionAndTimestamp(t *testing.T) {
	on, off := convTestAccount(true), convTestAccount(false)
	v7 := scopeCodexAccountIdentityValue(on, 77, "thread", convTestThread)
	requireV7SameTimestamp(t, convTestThread, v7, "v7 派生")
	require.Equal(t, v7, scopeCodexAccountIdentityValue(on, 77, "thread", convTestThread), "确定性")
	require.NotEqual(t, v7, scopeCodexAccountIdentityValue(on, 78, "thread", convTestThread), "不同 API key 不同值")
	require.NotEqual(t, v7, scopeCodexAccountIdentityValue(on, 77, "turn", convTestThread), "不同 kind 不同值")

	v4 := scopeCodexAccountIdentityValue(on, 77, "installation", convTestInstallation)
	parsed, err := uuid.Parse(v4)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(4), parsed.Version())
	require.Equal(t, scopeCodexAccountIdentityValue(off, 77, "installation", convTestInstallation), v4, "v4 原始值的派生不随开关变化")

	nonUUID := scopeCodexAccountIdentityValue(on, 77, "thread", "not-a-uuid")
	parsed, err = uuid.Parse(nonUUID)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(4), parsed.Version(), "非 UUID 原始值仍走 v4 哈希")
}

// device 模式与收敛开关同时开启：installation 收敛为账号常量（含头部/嵌入 turn-metadata），
// 其余恒等式与仅开收敛时完全相同。按 Forward/透传的真实分段顺序驱动。
func TestCodexFingerprintConvergence_WithDeviceMode(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	account.Extra[codexFingerprintModeExtraKey] = string(codexFingerprintDevice)
	account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
	wantInstall := resolveConvergedInstallationID(account, "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f")
	require.NotEmpty(t, wantInstall)
	rawBody := convTestBody(t)

	requireDevice := func(label string, h http.Header, body []byte) {
		require.Empty(t, h.Get("x-codex-installation-id"), label+" 不应重复发送独立安装头")
		require.Equal(t, wantInstall, gjson.Parse(h.Get("x-codex-turn-metadata")).Get("installation_id").String(), label+" 头部 turn-metadata.installation_id")
		if len(body) > 0 {
			cm := gjson.ParseBytes(body).Get("client_metadata")
			require.Equal(t, wantInstall, cm.Get("x-codex-installation-id").String(), label+" client_metadata.x-codex-installation-id")
			require.Equal(t, wantInstall, gjson.Parse(cm.Get("x-codex-turn-metadata").String()).Get("installation_id").String(), label+" 嵌入 turn-metadata.installation_id")
		}
	}

	// HTTP 非透传：identity → device 分段 → buildUpstreamRequest。
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(rawBody, &decoded))
	require.True(t, applyCodexAccountIdentityClientMetadataMap(decoded, account, 77))
	c := newConvTestContext(t, rawBody)
	fp := resolveCodexFingerprintIDsFromRequest(c, account, nil)
	require.NotNil(t, fp)
	require.True(t, applyCodexFingerprintClientMetadata(decoded, fp))
	stageCodexFingerprintIDs(c, fp)
	scopedBody, err := json.Marshal(decoded)
	require.NoError(t, err)
	httpReq, err := svc.buildUpstreamRequest(context.Background(), c, account, scopedBody, "tok", true, convTestSession, true)
	require.NoError(t, err)
	requireConvergenceInvariants(t, convOutbound{"HTTP+device", httpReq.Header, scopedBody})
	requireDevice("HTTP+device", httpReq.Header, scopedBody)

	// 透传：raw 字节上同样顺序。
	scopedRaw, changed, err := applyCodexAccountIdentityClientMetadataRaw(rawBody, account, 77)
	require.NoError(t, err)
	require.True(t, changed)
	c = newConvTestContext(t, rawBody)
	fp = resolveCodexFingerprintIDsFromRequest(c, account, nil)
	require.NotNil(t, fp)
	fpRaw, fpChanged, err := applyCodexFingerprintClientMetadataRaw(scopedRaw, fp)
	require.NoError(t, err)
	require.True(t, fpChanged)
	stageCodexFingerprintIDs(c, fp)
	ptReq, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, fpRaw, "tok")
	require.NoError(t, err)
	requireConvergenceInvariants(t, convOutbound{"透传+device", ptReq.Header, fpRaw})
	requireDevice("透传+device", ptReq.Header, fpRaw)

	// WS 握手。
	c = newConvTestContext(t, rawBody)
	stageCodexFingerprintIDs(c, resolveCodexFingerprintIDsFromRequest(c, account, nil))
	wsHeaders, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "tok",
		OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
		true, "", convTestTurnMetadata(), convTestSession, "", "")
	require.NoError(t, err)
	requireConvergenceInvariants(t, convOutbound{"WS+device", wsHeaders, nil})
	requireDevice("WS+device", wsHeaders, nil)

	for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "x-codex-window-id", "x-codex-installation-id"} {
		require.Equal(t, httpReq.Header.Get(name), ptReq.Header.Get(name), "HTTP 与透传 %s 应一致", name)
		require.Equal(t, httpReq.Header.Get(name), wsHeaders.Get(name), "HTTP 与 WS %s 应一致", name)
	}
}

// 回归：入站不带 session-id / thread-id 的客户端（现网 Codex Desktop 形态）开启收敛后，
// 出站必须仍带会话关联头。曾经无条件删除下划线别名，导致这类请求出站零会话身份，
// 上游缓存路由打散（pro1 HTTP 命中率 96% → 22%）。
func TestCodexFingerprintConvergence_KeepsSessionCorrelationWithoutInboundHeaders(t *testing.T) {
	svc := &OpenAIGatewayService{}
	rawBody := convTestBody(t)

	newCtx := func() *gin.Context {
		c := newConvTestContext(t, rawBody)
		for _, name := range []string{"session-id", "thread-id", "x-codex-parent-thread-id"} {
			c.Request.Header.Del(name)
		}
		return c
	}
	hasCorrelation := func(h http.Header) bool {
		for _, name := range []string{"session-id", "session_id", "conversation_id"} {
			if strings.TrimSpace(h.Get(name)) != "" {
				return true
			}
		}
		return false
	}

	for _, enabled := range []bool{false, true} {
		account := convTestAccount(enabled)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(rawBody, &decoded))
		applyCodexAccountIdentityClientMetadataMap(decoded, account, 77)
		scopedBody, err := json.Marshal(decoded)
		require.NoError(t, err)

		httpReq, err := svc.buildUpstreamRequest(context.Background(), newCtx(), account, scopedBody, "tok", true, convTestSession, true)
		require.NoError(t, err)
		require.True(t, hasCorrelation(httpReq.Header),
			"开关=%v：入站无会话头时出站仍须携带会话关联头，实际 session-id=%q session_id=%q conversation_id=%q",
			enabled, httpReq.Header.Get("session-id"), httpReq.Header.Get("session_id"), httpReq.Header.Get("conversation_id"))

		scopedRaw, _, err := applyCodexAccountIdentityClientMetadataRaw(rawBody, account, 77)
		require.NoError(t, err)
		ptReq, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), newCtx(), account, scopedRaw, "tok")
		require.NoError(t, err)
		require.True(t, hasCorrelation(ptReq.Header), "透传路径 开关=%v 同样要求", enabled)
	}
}

// 入站被中继剥掉连字符会话头（现网 31.108 apikey 中继形态：session-id / thread-id /
// x-codex-parent-thread-id 全被剥离，体内 client_metadata 完整保留）时，出站头必须从体内
// 已派生的同一份值重建，结果与客户端直连时逐字节相同。
func TestCodexFingerprintConvergence_ReconstructsSessionHeadersFromBody(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	rawBody := convTestBody(t)
	stripped := []string{"session-id", "thread-id", "x-codex-parent-thread-id", "x-client-request-id"}

	newCtx := func() *gin.Context {
		c := newConvTestContext(t, rawBody)
		for _, name := range stripped {
			c.Request.Header.Del(name)
		}
		return c
	}

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(rawBody, &decoded))
	require.True(t, applyCodexAccountIdentityClientMetadataMap(decoded, account, 77))
	scopedBody, err := json.Marshal(decoded)
	require.NoError(t, err)
	c := newCtx()
	stageCodexConvergenceBodyIdentityMap(c, account, decoded)
	httpReq, err := svc.buildUpstreamRequest(context.Background(), c, account, scopedBody, "tok", true, convTestSession, true)
	require.NoError(t, err)
	requireConvergenceInvariants(t, convOutbound{"HTTP 中继剥头", httpReq.Header, scopedBody})

	scopedRaw, changed, err := applyCodexAccountIdentityClientMetadataRaw(rawBody, account, 77)
	require.NoError(t, err)
	require.True(t, changed)
	c = newCtx()
	stageCodexConvergenceBodyIdentityRaw(c, account, scopedRaw)
	ptReq, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, scopedRaw, "tok")
	require.NoError(t, err)
	requireConvergenceInvariants(t, convOutbound{"透传 中继剥头", ptReq.Header, scopedRaw})

	// 与客户端直连（入站带头）时逐字节一致：重建值和派生值必须同源。
	directReq, err := svc.buildUpstreamRequest(context.Background(), newConvTestContext(t, rawBody), account, scopedBody, "tok", true, convTestSession, true)
	require.NoError(t, err)
	for _, name := range append([]string{}, stripped...) {
		require.Equal(t, directReq.Header.Get(name), httpReq.Header.Get(name), "重建的 %s 应与直连一致", name)
		require.Equal(t, directReq.Header.Get(name), ptReq.Header.Get(name), "透传重建的 %s 应与直连一致", name)
	}
}

// prompt_cache_key 与会话头同源：体内没有 client_metadata、只有 prompt_cache_key 时，
// 上游按 kind="prompt-cache" 派生 body，而收敛头曾按 kind="session" 派生，两者不等——
// 真 Codex 客户端里 session-id == prompt_cache_key 恒成立。
func TestCodexFingerprintConvergence_PromptCacheKeySharesSessionSource(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)

	body, err := json.Marshal(map[string]any{
		"model": "gpt-5.5", "stream": true, "prompt_cache_key": convTestSession,
		"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
	})
	require.NoError(t, err)

	scopedRaw, changed, err := applyCodexAccountIdentityClientMetadataRaw(body, account, 77)
	require.NoError(t, err)
	require.True(t, changed)
	scopedKey := gjson.GetBytes(scopedRaw, "prompt_cache_key").String()
	require.NotEmpty(t, scopedKey)

	c := newConvTestContext(t, body)
	for _, name := range []string{"session-id", "thread-id", "x-client-request-id"} {
		c.Request.Header.Del(name)
	}
	stageCodexConvergenceBodyIdentityRaw(c, account, scopedRaw)
	req, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, scopedRaw, "tok")
	require.NoError(t, err)
	require.Equal(t, scopedKey, req.Header.Get("session-id"), "session-id 必须与 body prompt_cache_key 同值")

	// 子代理形态 "<source>:<parent_thread_id>"（core prompt_cache_key()）：派生后保持同一形态，
	// 不能被压成一个裸 UUID——真客户端不存在裸 UUID 的这一分支。
	composite := "internal_guardian:" + convTestParentThread
	derived := scopeCodexAccountIdentityValue(account, 77, "prompt-cache", composite)
	prefix, rest, ok := strings.Cut(derived, ":")
	require.True(t, ok, "复合 prompt_cache_key 派生后应保持 <source>:<uuid> 形态，实际 %q", derived)
	require.Equal(t, "internal_guardian", prefix)
	requireV7SameTimestamp(t, convTestParentThread, rest, "复合 prompt_cache_key 的 parent_thread_id")
	require.Equal(t, scopeCodexAccountIdentityValue(account, 77, "thread", convTestParentThread), rest,
		"复合 prompt_cache_key 的 thread 部分应与 thread 类同值")
}

// 根会话 session_id 与 thread_id 是同一个 UUID（core/src/session/session.rs:791
// session_id = SessionId::from(thread_id)），子代理才不等（session_id 取根线程 ID）。
// 命名空间化必须保留这层关系，否则出站永远是"子代理线程"形态。
func TestCodexFingerprintConvergence_RootSessionKeepsThreadIdentity(t *testing.T) {
	on, off := convTestAccount(true), convTestAccount(false)
	subThread := "01a07c99-2222-7bbb-8ccc-dddddddddddd"

	require.Equal(t,
		scopeCodexAccountIdentityValue(on, 77, "thread", convTestSession),
		scopeCodexAccountIdentityValue(on, 77, "session", convTestSession),
		"根会话：同一个原始 UUID 按 session / thread 派生必须得到同值")
	require.NotEqual(t,
		scopeCodexAccountIdentityValue(on, 77, "session", convTestSession),
		scopeCodexAccountIdentityValue(on, 77, "thread", subThread),
		"子代理：session_id 与自己的 thread_id 不同，派生后仍须不同")
	// 这层关系与实验开关无关：账号隔离可以换值，但不该把官方"根 session == 根 thread"
	// 的等式打散。开关关闭的账号同样保留。
	require.Equal(t,
		scopeCodexAccountIdentityValue(off, 77, "thread", convTestSession),
		scopeCodexAccountIdentityValue(off, 77, "session", convTestSession),
		"开关关闭时同样保留根会话等式")
	require.NotEqual(t,
		scopeCodexAccountIdentityValue(off, 77, "session", convTestSession),
		scopeCodexAccountIdentityValue(off, 77, "thread", subThread),
		"关系另一侧也要保住：原值不同则派生后仍不同")
}

// 窗口标识的形态是 "<thread>:<window_number>"（core/src/session/mod.rs current_window），
// 序号随 auto-compact 递增、初值 0（core/src/state/auto_compact_window.rs:51）。账号隔离
// 只该替换 thread 部分，不能把整串压成裸 UUID，也不能把序号写死。
func TestCodexIdentityWindowKeepsStructureAndNumber(t *testing.T) {
	for _, on := range []bool{true, false} {
		account := convTestAccount(on)
		for _, number := range []string{"0", "3", "17"} {
			raw := convTestSession + ":" + number
			derived := scopeCodexAccountIdentityValue(account, 77, "window", raw)
			thread, seq, ok := strings.Cut(derived, ":")
			require.True(t, ok, "convergence=%v number=%s 派生结果丢了复合形态：%q", on, number, derived)
			require.Equal(t, number, seq, "窗口序号必须原样保留")
			require.Equal(t, scopeCodexAccountIdentityValue(account, 77, "thread", convTestSession), thread,
				"窗口的 thread 部分必须与 thread 类同值")
			require.NotEqual(t, raw, derived, "仍须落到账号命名空间")
		}
		// 未知形态不猜测解析，退回整体派生。
		require.NotContains(t, scopeCodexAccountIdentityValue(account, 77, "window", convTestSession+":x"), ":")
	}
}

// session / full 模式自建窗口标识时，序号取客户端当前窗口，缺失才回落 0。
func TestResolveCodexFingerprintIDsWindowNumberFollowsClient(t *testing.T) {
	account := convTestAccount(true)
	account.Extra[codexFingerprintModeExtraKey] = "full"
	account.Extra[codexFingerprintSeedExtraKey] = "951af12d-881d-4865-8f1b-3d952e328525"
	for raw, want := range map[string]string{"": "0", "3": "3", "abc": "0", "-1": "0"} {
		ids := resolveCodexFingerprintIDsWithWindow(account, convTestSession, codexFingerprintFull, raw)
		require.NotNil(t, ids)
		require.Equal(t, ids.threadID+":"+want, ids.windowID, "客户端窗口序号 %q", raw)
	}
}

func TestCodexWindowNumberEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, value, want string
	}{
		{"number", "3", "3"},
		{"string", `" 3 "`, "3"},
		{"bad_string", `"bad"`, "4"},
		{"negative", "-1", "4"},
		{"fraction", "1.5", "4"},
		{"boolean", "true", "4"},
		{"null", "null", "4"},
		{"large_integer", "9007199254740993", "9007199254740993"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := fmt.Sprintf(`{"window_number":%s,"window_id":"%s:4"}`, tc.value, convTestSession)
			headers := http.Header{}
			headers.Set(openAIWSTurnMetadataHeader, metadata)
			require.Equal(t, tc.want, extractClientWindowNumber(headers))
			for _, cm := range []any{
				map[string]any{openAIWSTurnMetadataHeader: metadata},
				map[string]string{openAIWSTurnMetadataHeader: metadata},
			} {
				require.Equal(t, tc.want, codexFingerprintWindowNumberEvidence(cm))
			}
		})
	}
}

// astra 复核 #1：入站带连字符会话头、但请求体只有 prompt_cache_key（没有 client_metadata）。
// 此时 session 头取缓存键派生值、thread 头取入站派生值、turn-metadata 又是第三种，身份分裂。
func TestCodexFingerprintConvergence_AstraNoClientMetadataWithInboundHeaders(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	body, err := json.Marshal(map[string]any{
		"model": "gpt-5.5", "stream": true, "prompt_cache_key": convTestSession,
		"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
	})
	require.NoError(t, err)

	scopedRaw, _, err := applyCodexAccountIdentityClientMetadataRaw(body, account, 77)
	require.NoError(t, err)
	c := newConvTestContext(t, body) // 入站保留 session-id / thread-id
	stageCodexConvergenceBodyIdentityRaw(c, account, scopedRaw)
	req, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, scopedRaw, "tok")
	require.NoError(t, err)

	h := req.Header
	meta := gjson.Parse(h.Get("x-codex-turn-metadata"))
	t.Logf("session-id=%s thread-id=%s meta.session_id=%s meta.thread_id=%s pck=%s",
		h.Get("session-id"), h.Get("thread-id"), meta.Get("session_id").String(),
		meta.Get("thread_id").String(), gjson.GetBytes(scopedRaw, "prompt_cache_key").String())
	require.Equal(t, h.Get("session-id"), h.Get("thread-id"), "根会话 session-id 应等于 thread-id")
	require.Equal(t, h.Get("session-id"), meta.Get("session_id").String(), "session-id 应等于 turn-metadata.session_id")
	require.Equal(t, h.Get("session-id"), gjson.GetBytes(scopedRaw, "prompt_cache_key").String(), "session-id 应等于 body prompt_cache_key")
}

// astra 复核 #2：session/full 模式下，中继剥头且体内只有 prompt_cache_key 时，
// 指纹模块把 session 头改成收敛常量，body 缓存键却因为"证明不了它是默认值"没跟着改。
func TestCodexFingerprintConvergence_AstraSessionModeSyncsPromptCacheKey(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	account.Extra[codexFingerprintModeExtraKey] = string(codexFingerprintSession)
	account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"

	body, err := json.Marshal(map[string]any{
		"model": "gpt-5.5", "stream": true, "prompt_cache_key": convTestSession,
		"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
	})
	require.NoError(t, err)

	c := newConvTestContext(t, body)
	for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "x-codex-parent-thread-id"} {
		c.Request.Header.Del(name)
	}
	scopedRaw, _, err := applyCodexAccountIdentityClientMetadataRaw(body, account, 77)
	require.NoError(t, err)
	fp := resolveCodexFingerprintIDsFromRequest(c, account, nil)
	require.NotNil(t, fp)
	fpRaw, _, err := applyCodexFingerprintClientMetadataRaw(scopedRaw, fp)
	require.NoError(t, err)
	stageCodexFingerprintIDs(c, fp)
	stageCodexConvergenceBodyIdentityRaw(c, account, fpRaw)

	req, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, fpRaw, "tok")
	require.NoError(t, err)
	t.Logf("session-id=%s pck=%s", req.Header.Get("session-id"), gjson.GetBytes(fpRaw, "prompt_cache_key").String())
	require.Equal(t, req.Header.Get("session-id"), gjson.GetBytes(fpRaw, "prompt_cache_key").String(),
		"session 模式下出站 session-id 必须与 body prompt_cache_key 同值")
}

// astra 复核 #3：出站头里的 x-codex-turn-metadata 已经被命名空间化过，带着同一份
// session_id / thread_id。连字符头补不出来时应当从它重建——这条不依赖任何调用点接线，
// 所以 WS 之类的路径漏接暂存也仍然能出会话头。
func TestCodexFingerprintConvergence_AstraRebuildsFromTurnMetadataHeader(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	rawBody := convTestBody(t)

	c := newConvTestContext(t, rawBody)
	for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "x-codex-parent-thread-id"} {
		c.Request.Header.Del(name)
	}
	// 不调用 stageCodexConvergenceBodyIdentity*，模拟调用点漏接暂存。
	wsHeaders, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "tok",
		OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
		true, "", convTestTurnMetadata(), convTestSession, "", "")
	require.NoError(t, err)

	meta := gjson.Parse(wsHeaders.Get("x-codex-turn-metadata"))
	require.True(t, meta.IsObject())
	t.Logf("session-id=%q thread-id=%q meta.session_id=%s",
		wsHeaders.Get("session-id"), wsHeaders.Get("thread-id"), meta.Get("session_id").String())
	require.Equal(t, meta.Get("session_id").String(), wsHeaders.Get("session-id"), "session-id 应从 turn-metadata 重建")
	require.Equal(t, meta.Get("thread_id").String(), wsHeaders.Get("thread-id"), "thread-id 应从 turn-metadata 重建")
	require.Equal(t, wsHeaders.Get("thread-id"), wsHeaders.Get("x-client-request-id"))
}

func convSessionModeAccount(t *testing.T) *Account {
	t.Helper()
	account := convTestAccount(true)
	account.Extra[codexFingerprintModeExtraKey] = string(codexFingerprintSession)
	account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
	return account
}

// 按 forwardOpenAIPassthrough 的真实分段顺序驱动透传路径，返回出站头和最终 body。
func convRunPassthrough(t *testing.T, account *Account, body []byte, stripInbound bool) (http.Header, []byte) {
	t.Helper()
	svc := &OpenAIGatewayService{}
	c := newConvTestContext(t, body)
	if stripInbound {
		for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "x-codex-parent-thread-id"} {
			c.Request.Header.Del(name)
		}
	}
	fp := resolveCodexFingerprintIDsWithBody(c, account, nil, gjson.GetBytes(body, "client_metadata"))
	scoped, _, err := applyCodexAccountIdentityClientMetadataRaw(body, account, 77)
	require.NoError(t, err)
	if fp != nil {
		next, _, fpErr := applyCodexFingerprintClientMetadataRaw(scoped, fp)
		require.NoError(t, fpErr)
		scoped = next
		stageCodexFingerprintIDs(c, fp)
	}
	stageCodexConvergenceBodyIdentityRaw(c, account, scoped)
	req, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, scoped, "tok")
	require.NoError(t, err)
	return req.Header, scoped
}

// codex 复核 #1：client_metadata 存在但没有 session_id（空对象、或只有 installation）时，
// 普通路径会退回用 prompt_cache_key 当会话默认值，透传路径却走了另一个分支没退回，
// 最终 session 身份与缓存键不同。两条路径必须共用同一套缺字段判定。
func TestCodexFingerprintConvergence_CodexClientMetadataWithoutSession(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta map[string]any
	}{
		{"空 client_metadata", map[string]any{}},
		{"只有 installation", map[string]any{"x-codex-installation-id": convTestInstallation}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := convSessionModeAccount(t)
			body, err := json.Marshal(map[string]any{
				"model": "gpt-5.5", "stream": true, "prompt_cache_key": convTestSession,
				"client_metadata": tc.meta,
				"input":           []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
			})
			require.NoError(t, err)

			h, scoped := convRunPassthrough(t, account, body, true)
			t.Logf("透传 session-id=%s pck=%s", h.Get("session-id"), gjson.GetBytes(scoped, "prompt_cache_key").String())
			require.Equal(t, h.Get("session-id"), gjson.GetBytes(scoped, "prompt_cache_key").String(),
				"透传路径 session-id 必须与 body prompt_cache_key 同值")

			// 普通路径同样的判定
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(body, &decoded))
			applyCodexAccountIdentityClientMetadataMap(decoded, account, 77)
			c := newConvTestContext(t, body)
			for _, name := range []string{"session-id", "thread-id", "x-client-request-id"} {
				c.Request.Header.Del(name)
			}
			fp := resolveCodexFingerprintIDsFromRequest(c, account, nil)
			require.NotNil(t, fp)
			applyCodexFingerprintClientMetadata(decoded, fp)
			mapKey, _ := decoded["prompt_cache_key"].(string)
			require.Equal(t, mapKey, gjson.GetBytes(scoped, "prompt_cache_key").String(),
				"普通路径与透传路径的 prompt_cache_key 必须一致")
		})
	}
}

// codex 复核 #2：把"体内没有 session_id"直接当成"prompt_cache_key 就是默认会话键"，
// 会连显式覆盖值和复合键一起抹成账号会话常量。复合键的形态是 codex 自己的子代理分支
// （client.rs:512），必须保住；入站头证明了会话身份、而缓存键与之不同时也不该动它。
func TestCodexFingerprintConvergence_CodexKeepsExplicitAndCompositeCacheKey(t *testing.T) {
	t.Run("入站头证明了会话身份时保留显式缓存键", func(t *testing.T) {
		account := convSessionModeAccount(t)
		body, err := json.Marshal(map[string]any{
			"model": "gpt-5.5", "stream": true, "prompt_cache_key": "explicit-cache-key",
			"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
		})
		require.NoError(t, err)
		_, scoped := convRunPassthrough(t, account, body, false) // 入站保留 session-id
		got := gjson.GetBytes(scoped, "prompt_cache_key").String()
		t.Logf("pck=%s", got)
		require.Equal(t, scopeCodexAccountIdentityValue(account, 77, "prompt-cache", "explicit-cache-key"), got,
			"显式缓存键只应被命名空间化，不该被改成账号会话常量")
	})

	t.Run("复合键保形", func(t *testing.T) {
		account := convSessionModeAccount(t)
		composite := "guardian:" + convTestParentThread
		body, err := json.Marshal(map[string]any{
			"model": "gpt-5.5", "stream": true, "prompt_cache_key": composite,
			"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
		})
		require.NoError(t, err)
		_, scoped := convRunPassthrough(t, account, body, true)
		got := gjson.GetBytes(scoped, "prompt_cache_key").String()
		t.Logf("pck=%s", got)
		prefix, rest, ok := strings.Cut(got, ":")
		require.True(t, ok, "复合 prompt_cache_key 应保持 <source>:<uuid> 形态，实际 %q", got)
		require.Equal(t, "guardian", prefix)
		requireV7SameTimestamp(t, convTestParentThread, rest, "复合键的父线程")
	})
}

// codex 复核三 #1：入站带默认形态的 session-id（pck == 该 session），体内没有 client_metadata。
// 判定拿"已派生的缓存键 H(S)"去和"原始入站头 S"比，必然不等，默认键被误判成显式覆盖：
// session/full 下头收敛成账号常量、缓存键却留在另一套身份里。
func TestCodexFingerprintConvergence_R3DefaultCacheKeyWithInboundSessionHeader(t *testing.T) {
	account := convSessionModeAccount(t)
	body, err := json.Marshal(map[string]any{
		"model": "gpt-5.5", "stream": true, "prompt_cache_key": convTestSession,
		"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
	})
	require.NoError(t, err)
	h, scoped := convRunPassthrough(t, account, body, false) // 入站保留 session-id == pck 原值
	t.Logf("session-id=%s pck=%s", h.Get("session-id"), gjson.GetBytes(scoped, "prompt_cache_key").String())
	require.Equal(t, h.Get("session-id"), gjson.GetBytes(scoped, "prompt_cache_key").String(),
		"默认缓存键应被识别出来，出站 session-id 与 body prompt_cache_key 必须同值")
}

// codex 复核三 #2：device 模式下，体内没有 client_metadata.session_id 但 turn-metadata 里有，
// 且 prompt_cache_key 是自定义值。会话头不该拿自定义缓存键去填，那样会和 turn-metadata 打架。
func TestCodexFingerprintConvergence_R3TurnMetadataBeatsCustomCacheKey(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	account.Extra[codexFingerprintModeExtraKey] = string(codexFingerprintDevice)
	account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"

	body, err := json.Marshal(map[string]any{
		"model": "gpt-5.5", "stream": true, "prompt_cache_key": "explicit-cache-key",
		"client_metadata": map[string]any{
			"x-codex-installation-id": convTestInstallation,
			"x-codex-turn-metadata":   convTestTurnMetadata(),
		},
		"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
	})
	require.NoError(t, err)

	c := newConvTestContext(t, body)
	for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "x-codex-parent-thread-id", "x-codex-turn-metadata"} {
		c.Request.Header.Del(name)
	}
	scoped, _, err := applyCodexAccountIdentityClientMetadataRaw(body, account, 77)
	require.NoError(t, err)
	stageCodexConvergenceBodyIdentityRaw(c, account, scoped)
	req, err := svc.buildUpstreamRequestOpenAIPassthrough(context.Background(), c, account, scoped, "tok")
	require.NoError(t, err)

	embedded := gjson.Parse(gjson.GetBytes(scoped, "client_metadata.x-codex-turn-metadata").String())
	wantSession := embedded.Get("session_id").String()
	require.NotEmpty(t, wantSession)
	t.Logf("session-id=%s turn-metadata.session_id=%s pck=%s",
		req.Header.Get("session-id"), wantSession, gjson.GetBytes(scoped, "prompt_cache_key").String())
	require.Equal(t, wantSession, req.Header.Get("session-id"),
		"会话头应取 turn-metadata 里的 session，而不是自定义缓存键")
	require.Equal(t, embedded.Get("thread_id").String(), req.Header.Get("thread-id"),
		"thread 头同样应取 turn-metadata")
}

// codex 复核三 #3：独立 WS 入口没有解析/暂存指纹 IDs，device 模式下 HTTP 用账号固定
// installation、直连 WS 却还在用客户端安装 ID 的派生值。这里驱动真实入口函数，
// 不手工 stageCodexFingerprintIDs，否则正好把漏接掩盖掉。
func TestCodexFingerprintConvergence_R3WSAppliesDeviceMode(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := convTestAccount(true)
	account.Extra[codexFingerprintModeExtraKey] = string(codexFingerprintDevice)
	account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
	wantInstall := resolveConvergedInstallationID(account, "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f")
	require.NotEmpty(t, wantInstall)

	rawBody := convTestBody(t)
	c := newConvTestContext(t, rawBody)
	payload, err := applyCodexIdentityToWSPayload(c, account, rawBody)
	require.NoError(t, err)

	wsHeaders, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "tok",
		OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
		true, "", convTestTurnMetadata(), convTestSession, "", "")
	require.NoError(t, err)
	require.Empty(t, wsHeaders.Get("x-codex-installation-id"))
	require.Equal(t, wantInstall, gjson.Get(wsHeaders.Get(openAIWSTurnMetadataHeader), "installation_id").String())
	require.Equal(t, wantInstall, gjson.GetBytes(payload, "client_metadata.x-codex-installation-id").String(),
		"独立 WS 入口必须在帧内保留账号固定 installation")
}

// 图片接口自建 Responses 请求体，走的是独立入口。回归：该入口曾漏接指纹 ID 暂存，
// 导致同一账号的图片请求带着按客户端原值派生的另一套设备身份出站——而真实 Codex 的
// 图片与推理请求同属一个客户端进程，安装标识必然相同。
func TestCodexFingerprint_ImagesOAuthMatchesResponsesDeviceIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 双开：pro1 线上就是 device 收敛 + 实验指纹收敛同时开启，两个开关会互相经过
	// 同一批改写点，只测单开会漏掉组合下的漂移。
	newAccount := func() *Account {
		a := newTestOAuthAccount(4501, map[string]any{
			codexFingerprintModeExtraKey:        "device",
			codexFingerprintConvergenceExtraKey: true,
		})
		a.Name = "oauth-images"
		a.Status = StatusActive
		a.Schedulable = true
		a.Concurrency = 1
		a.Credentials = map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-acc"}
		return a
	}
	newCtx := func(path string) *gin.Context {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(nil))
		c.Request.Header.Set("User-Agent", "codex-tui/0.153.4")
		c.Request.Header.Set("originator", "codex-tui")
		// 客户端自带的安装标识：不收敛时只会被账号 scope，得到与推理面不同的另一套设备身份。
		c.Request.Header.Set("x-codex-installation-id", "client-install-A")
		return c
	}
	newSvc := func(respBody string) (*OpenAIGatewayService, *httpUpstreamRecorder) {
		up := &httpUpstreamRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(respBody)),
		}}
		return &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up, toolCorrector: NewCodexToolCorrector()}, up
	}

	// 两条路径都只断言出站请求头；响应体是否可解析与本用例无关。
	svcResp, respUp := newSvc(`{"id":"resp_1","object":"response","status":"completed","output":[]}`)
	_, _ = svcResp.Forward(context.Background(), newCtx("/v1/responses"), newAccount(),
		[]byte(`{"model":"gpt-5.4","stream":false,"input":[{"type":"message","role":"user","content":"hi"}]}`))
	require.NotNil(t, respUp.lastReq, "推理入口必须真正发出上游请求")

	svcImg, imgUp := newSvc(`{"id":"resp_2","object":"response","status":"completed","output":[]}`)
	_, _ = svcImg.forwardOpenAIImagesOAuth(context.Background(), newCtx("/v1/images/generations"), newAccount(),
		&OpenAIImagesRequest{Endpoint: "generations", Model: "gpt-image-2", Prompt: "a cat"}, "")
	require.NotNil(t, imgUp.lastReq, "图片入口必须真正发出上游请求")

	install := gjson.GetBytes(imgUp.lastBody, "client_metadata.x-codex-installation-id").String()
	require.NotEmpty(t, install)
	require.NotEqual(t, "client-install-A", install, "device 模式下不得沿用客户端自带的安装标识")
	require.Empty(t, imgUp.lastReq.Header.Get("x-codex-installation-id"))
	require.Empty(t, respUp.lastReq.Header.Get("x-codex-installation-id"))
	require.Equal(t, gjson.GetBytes(respUp.lastBody, "client_metadata.x-codex-installation-id").String(), install,
		"图片入口与推理入口必须收敛到同一台设备")
}

// /alpha/search 的 turn-metadata 走 MCP 投影，不是 Responses 那一套。边界依据：真实客户端
// 在该端点只发 x-codex-turn-metadata 与 originator（codex-rs ext/web-search/src/tool.rs 的
// search_request_headers），不发会话头，故不能顺手补入 Responses 的那一套。
//
// 双开与单开在该端点的形态不同，两条都要钉住：
//
//	双开：走 MCP 投影。真客户端的模板传的是 installation_id="" / window_id=""，且
//	     request_kind 为 None 使 has_request_identity=false，于是 installation_id、
//	     window_id、window_number、context_window_id 被 skip_serializing_if 整个省略
//	     （16ff14c: core/src/turn_metadata.rs mcp_metadata_template + current_meta_value_for_mcp_request
//	     里显式 remove 的 agent_name / parent_turn_id / root_turn_id 同理）。省略比补一个
//	     收敛值更接近真实形态——没有字段就没有可关联的设备标识。
//	单开：维持既有行为，仍按设备收敛改写 installation_id。
func TestCodexFingerprint_AlphaSearchTurnMetadataUsesMCPProjection(t *testing.T) {
	gin.SetMode(gin.TestMode)

	svc := &OpenAIGatewayService{cfg: &config.Config{}, toolCorrector: NewCodexToolCorrector()}
	// 单看「结果 != 客户端原值」不足以证明收敛：账号 scope 本身就会改掉原值，但它是按原值
	// 派生的，每个客户端各不相同。device 收敛的定义是不同客户端落到同一台设备，故这里用
	// 两个不同的客户端安装标识跑两遍比对。
	run := func(account *Account, clientInstall string) *http.Request {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", bytes.NewReader(nil))
		c.Request.Header.Set("originator", "codex-tui")
		c.Request.Header.Set("X-Codex-Turn-Metadata",
			`{"installation_id":"`+clientInstall+`","session_id":"session-`+clientInstall+`","window_id":"w:0","root_turn_id":"r"}`)
		req, err := svc.buildOpenAIAlphaSearchRequest(context.Background(), c, account, []byte(`{"query":"x"}`), "oauth-token")
		require.NoError(t, err)
		return req
	}
	newAccount := func(id int64, convergence bool) *Account {
		account := newTestOAuthAccount(id, map[string]any{
			codexFingerprintModeExtraKey:        "device",
			codexFingerprintConvergenceExtraKey: convergence,
		})
		account.Name = "oauth-search"
		account.Status = StatusActive
		account.Schedulable = true
		account.Concurrency = 1
		account.Credentials = map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-acc"}
		return account
	}

	t.Run("双开走 MCP 投影", func(t *testing.T) {
		// 与 pro1 线上配置一致。
		reqA := run(newAccount(4502, true), "client-install-A")
		metadata := reqA.Header.Get("X-Codex-Turn-Metadata")
		for _, field := range []string{"installation_id", "window_id", "window_number", "context_window_id",
			"agent_name", "parent_turn_id", "root_turn_id"} {
			require.False(t, gjson.Get(metadata, field).Exists(), "MCP 投影不发 %s：%s", field, metadata)
		}
		// 边界：只删 Responses 专属字段，会话身份仍在（且已按账号隔离改写）。
		session := gjson.Get(metadata, "session_id")
		require.True(t, session.Exists(), metadata)
		require.NotEqual(t, "session-client-install-A", session.String(), "不得原样透传客户端会话")
	})

	t.Run("单开维持设备收敛", func(t *testing.T) {
		account := newAccount(4503, false)
		reqA, reqB := run(account, "client-install-A"), run(account, "client-install-B")
		installA := gjson.Get(reqA.Header.Get("X-Codex-Turn-Metadata"), "installation_id").String()
		installB := gjson.Get(reqB.Header.Get("X-Codex-Turn-Metadata"), "installation_id").String()
		require.NotEmpty(t, installA)
		require.NotEqual(t, "client-install-A", installA, "不得原样透传客户端安装标识")
		require.Equal(t, installA, installB, "device 模式下不同客户端必须收敛到同一台设备")
	})

	// 边界：两种配置都不得补入 Responses 的会话头。
	for _, account := range []*Account{newAccount(4502, true), newAccount(4503, false)} {
		req := run(account, "client-install-A")
		require.Empty(t, req.Header.Get("session-id"))
		require.Empty(t, req.Header.Get("thread-id"))
		require.Empty(t, req.Header.Get("x-client-request-id"))
	}
}

// codex 复核四 #1：真实客户端的搜索请求体 id 就是会话 ID，与随请求发出的
// turn-metadata.session_id 同源（codex-rs ext/web-search/src/tool.rs 的 handle_call：
// SearchRequest.id = self.session_id，extra_headers 取自同一 session）。账号隔离与指纹
// 收敛只改写头里的 turn-metadata，body.id 会停在客户端原值上——同一个请求两套会话身份。
func TestCodexFingerprint_AlphaSearchBodyIDFollowsTurnMetadataSession(t *testing.T) {
	gin.SetMode(gin.TestMode)

	account := newTestOAuthAccount(4503, map[string]any{
		codexFingerprintModeExtraKey:        "device",
		codexFingerprintConvergenceExtraKey: true,
	})
	account.Name = "oauth-search-body"
	account.Status = StatusActive
	account.Schedulable = true
	account.Concurrency = 1
	account.Credentials = map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-acc"}

	svc := &OpenAIGatewayService{cfg: &config.Config{}, toolCorrector: NewCodexToolCorrector()}
	run := func(bodyID string) (*http.Request, string) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", bytes.NewReader(nil))
		c.Request.Header.Set("originator", "codex-tui")
		c.Request.Header.Set("X-Codex-Turn-Metadata",
			`{"installation_id":"client-install-A","session_id":"session-client-A"}`)
		req, err := svc.buildOpenAIAlphaSearchRequest(context.Background(), c,
			account, []byte(`{"id":"`+bodyID+`","query":"x"}`), "oauth-token")
		require.NoError(t, err)
		require.NotNil(t, req.Body)
		sent, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		return req, gjson.GetBytes(sent, "id").String()
	}

	// 同源：body.id == 入站 turn-metadata.session_id，必须跟着一起派生。
	req, sentID := run("session-client-A")
	outSession := gjson.Get(req.Header.Get("X-Codex-Turn-Metadata"), "session_id").String()
	require.NotEmpty(t, outSession)
	require.NotEqual(t, "session-client-A", outSession, "出站 turn-metadata 会话必须被派生")
	t.Logf("body.id=%s turn-metadata.session_id=%s", sentID, outSession)
	require.Equal(t, outSession, sentID, "body.id 必须与出站 turn-metadata.session_id 同源")

	// 非同源：SearchRequest.id 允许是任意自定义值，无法证明同源时不得盲改。
	_, customID := run("not-a-session-id")
	require.Equal(t, "not-a-session-id", customID, "无法证明同源的自定义 id 必须原样透传")
}

// codex 复核四 #4：compact 头侧指纹解析在非透传路径已修（openai_gateway_forward.go
// 的 isCompactRequest 分支），透传路径的同一处判断被漏掉，导致透传开启时 compact
// 请求仍带着按客户端原值派生的另一套设备身份出站。
func TestCodexFingerprint_PassthroughCompactMatchesResponsesDeviceIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)

	newAccount := func() *Account {
		a := newTestOAuthAccount(4504, map[string]any{
			codexFingerprintModeExtraKey:        "device",
			codexFingerprintConvergenceExtraKey: true,
		})
		a.Name = "oauth-passthrough-compact"
		a.Status = StatusActive
		a.Schedulable = true
		a.Concurrency = 1
		a.Credentials = map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-acc"}
		return a
	}
	newCtx := func(path string) *gin.Context {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(nil))
		c.Request.Header.Set("User-Agent", "codex-tui/0.153.4")
		c.Request.Header.Set("originator", "codex-tui")
		c.Request.Header.Set("x-codex-installation-id", "client-install-A")
		return c
	}
	newSvc := func() (*OpenAIGatewayService, *httpUpstreamRecorder) {
		up := &httpUpstreamRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"resp_1","object":"response","status":"completed","output":[],"usage":{}}`)),
		}}
		return &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up, toolCorrector: NewCodexToolCorrector()}, up
	}

	body := []byte(`{"model":"gpt-5.4","stream":false,"instructions":"i","input":[{"type":"message","role":"user","content":"hi"}]}`)

	svcResp, respUp := newSvc()
	_, _ = svcResp.forwardOpenAIPassthrough(context.Background(), newCtx("/v1/responses"),
		newAccount(), body, body, "gpt-5.4", false, nil, false, time.Now())
	require.NotNil(t, respUp.lastReq, "透传推理入口必须真正发出上游请求")

	svcCompact, compactUp := newSvc()
	_, _ = svcCompact.forwardOpenAIPassthrough(context.Background(), newCtx("/v1/responses/compact"),
		newAccount(), body, body, "gpt-5.4", false, nil, false, time.Now())
	require.NotNil(t, compactUp.lastReq, "透传 compact 入口必须真正发出上游请求")

	install := compactUp.lastReq.Header.Get("x-codex-installation-id")
	require.NotEmpty(t, install)
	require.NotEqual(t, "client-install-A", install, "device 模式下不得沿用客户端自带的安装标识")
	require.Empty(t, respUp.lastReq.Header.Get("x-codex-installation-id"))
	require.Equal(t, gjson.GetBytes(respUp.lastBody, "client_metadata.x-codex-installation-id").String(), install,
		"透传 compact 与透传推理必须收敛到同一台设备")
}
