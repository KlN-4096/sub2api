package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// 对照 openai/codex codex-rs（commit 16ff14c）的出站身份恒等式，开关开启时三条路径必须同时满足：
//
//	头 session-id == body prompt_cache_key == client_metadata.session_id
//	头 thread-id  == 头 x-client-request-id == client_metadata.thread_id
//	client_metadata.root_turn_id == client_metadata.turn_id
//	头 x-codex-window-id == client_metadata.x-codex-window-id == turn-metadata.window_id == "<派生 thread-id>:<n>"
//	turn-metadata.context_window_id 单独派生、保持 v7
//	不发 session_id / conversation_id；v7 原始值派生后仍为 v7 且时间戳前缀不变；v4 原始值仍为 v4
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
	require.Empty(t, h.Get("session_id"), o.label+" 不应发 session_id")
	require.Empty(t, h.Get("conversation_id"), o.label+" 不应发 conversation_id")
	requireV7SameTimestamp(t, convTestParentThread, h.Get("x-codex-parent-thread-id"), o.label+" x-codex-parent-thread-id")
	require.Equal(t, "explorer", h.Get("x-openai-subagent"), o.label+" x-openai-subagent 原样透传")

	windowID := h.Get("x-codex-window-id")
	require.Equal(t, tid+":1", windowID, o.label+" x-codex-window-id 应为 <派生 thread-id>:<window_number>")
	installation := h.Get("x-codex-installation-id")
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
