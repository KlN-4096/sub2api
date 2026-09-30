package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 全部离线：上游是 httpUpstreamRecorder，按队列回放 BPS 附件接口与 /responses 的响应。

func bpsTestSSE(events ...string) string {
	var b strings.Builder
	for _, event := range events {
		kind := gjson.Get(event, "type").String()
		_, _ = b.WriteString("event: " + kind + "\ndata: " + event + "\n\n")
	}
	_, _ = b.WriteString("data: [DONE]\n\n")
	return b.String()
}

func bpsTestResponse(status int, contentType, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{contentType}, "X-Request-Id": []string{"rid_bps"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// bpsSlowFirstByteBody：第一次 Read 前先睡一段，用来模拟「上游想了很久才吐第一个事件」。
type bpsSlowFirstByteBody struct {
	delay  time.Duration
	reader io.Reader
	slept  bool
}

func (b *bpsSlowFirstByteBody) Read(p []byte) (int, error) {
	if !b.slept {
		b.slept = true
		time.Sleep(b.delay)
	}
	return b.reader.Read(p)
}

func (b *bpsSlowFirstByteBody) Close() error { return nil }

// bpsTestTransportCall：BPS 回的原生 run_officejs 项，code 里包着客户端工具调用。
func bpsTestTransportCall(callID, code string) string {
	arguments, _ := json.Marshal(map[string]any{
		"summary": "Run client tool", "extended_summary": "relay", "code": code, "destructive": false, "references": []any{},
	})
	item, _ := json.Marshal(map[string]any{
		"type": "function_call", "id": "fc_" + callID, "call_id": callID, "name": "run_officejs",
		"arguments": string(arguments), "status": "completed", "summary": "Run client tool",
	})
	return string(item)
}

func bpsTestCompletedSSE(outputItems ...string) string {
	completed := `{"type":"response.completed","response":{"id":"resp_bps","status":"completed","model":"gpt-6-astra","output":[` +
		strings.Join(outputItems, ",") + `],"usage":{"input_tokens":20,"output_tokens":5,"total_tokens":25}}}`
	return bpsTestSSE(
		`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
		completed,
	)
}

const bpsTestMessageItem = `{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"done"}]}`

func newBasisPointsTestContext(t *testing.T) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))
	c.Request.Header.Set("User-Agent", "codex_cli_rs/0.156.1")
	return c, rec
}

// Fast 策略要经 settingService 才会被评估（evaluateOpenAIFastPolicy 在 settingService==nil 时
// 直接 Pass），而策略走的是 filter / force_priority 的请求会继续读网关转发设置 —— 用带 repo 桩的
// 真 SettingService，否则 getGatewayForwardingSettingsCached 会在 nil repo 上 panic。
type bpsTestSettingRepo struct{ *antigravitySettingRepoStub }

// 网关转发设置走 GetMultiple；这里一律当「没配」处理，走各自的默认值。
func (bpsTestSettingRepo) GetMultiple(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

func newBasisPointsTestSettingService() *SettingService {
	return NewSettingService(
		bpsTestSettingRepo{&antigravitySettingRepoStub{values: map[string]string{}}},
		&config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}})
}

func newBasisPointsTestAccount(id int64) *Account {
	return &Account{
		ID: id, Name: "bps", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1,
		Credentials: map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-acc"},
		Extra:       map[string]any{openAIBasisPointsExtraKey: true},
		Status:      StatusActive, Schedulable: true, RateMultiplier: f64p(1),
	}
}

const bpsTestClientBody = `{
	"model":"gpt-6-astra","stream":true,"store":false,"instructions":"base prompt","prompt_cache_key":"sess-1",
	"reasoning":{"effort":"max","summary":"auto"},"include":["reasoning.encrypted_content"],"max_output_tokens":4096,
	"tools":[
		{"type":"function","name":"exec_command","description":"Run a shell command.","parameters":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}},
		{"type":"custom","name":"apply_patch","description":"Apply a patch.","format":{"type":"grammar","syntax":"lark","definition":"start: x"}},
		{"type":"web_search","external_web_access":false}
	],
	"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}],"internal_chat_message_metadata_passthrough":{"turn_id":"t1"}}]
}`

func TestAccountUsesOpenAIBasisPointsOnlyForOAuthWithFlag(t *testing.T) {
	require.True(t, newBasisPointsTestAccount(1).UsesOpenAIBasisPoints())
	for _, typ := range []string{AccountTypeAPIKey, AccountTypeSetupToken, AccountTypeCPR} {
		account := newBasisPointsTestAccount(1)
		account.Type = typ
		require.False(t, account.UsesOpenAIBasisPoints(), typ)
	}
	off := newBasisPointsTestAccount(1)
	off.Extra = map[string]any{openAIBasisPointsExtraKey: "true"}
	require.False(t, off.UsesOpenAIBasisPoints(), "只认布尔 true")
	var nilAccount *Account
	require.False(t, nilAccount.UsesOpenAIBasisPoints())
}

func TestOpenAIBasisPoints_ForwardRewritesRequestAndRelaysToolCall(t *testing.T) {
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
		`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_1","role":"assistant","content":[]}}`,
		`{"type":"response.output_text.delta","output_index":0,"item_id":"msg_1","content_index":0,"delta":"Let me check."}`,
		`{"type":"response.output_item.done","output_index":0,"item":`+bpsTestMessageItem+`}`,
		`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc_call_1","call_id":"call_1","name":"run_officejs","arguments":""}}`,
		`{"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_call_1","delta":"{\"summary\""}`,
		`{"type":"response.function_call_arguments.done","output_index":1,"item_id":"fc_call_1","arguments":"{}"}`,
		`{"type":"response.output_item.done","output_index":1,"item":`+bpsTestTransportCall("call_1", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)+`}`,
		`{"type":"response.completed","response":{"id":"resp_bps","status":"completed","model":"gpt-6-astra","output":[`+bpsTestMessageItem+`,`+bpsTestTransportCall("call_1", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)+`],"usage":{"input_tokens":20,"output_tokens":5,"total_tokens":25}}}`,
	))}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	account := newBasisPointsTestAccount(9101)

	result, err := svc.Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, upstream.requests, 1)

	req := upstream.lastReq
	require.Equal(t, openAIBasisPointsResponsesURL, req.URL.String())
	require.Equal(t, "Bearer oauth-token", req.Header.Get("Authorization"))
	require.Equal(t, "chatgpt-acc", req.Header.Get("chatgpt-account-id"))
	require.Equal(t, "chatgpt-acc", req.Header.Get("x-openai-account-id"))
	require.Equal(t, "chatgpt", req.Header.Get("x-basispoints-auth-mode"))
	require.Equal(t, "basispoints-excel-plugin", req.Header.Get("x-openai-internal-basispoints-client-product"))
	require.Equal(t, openAIBasisPointsUserAgent, req.Header.Get("User-Agent"))
	require.Empty(t, req.Header.Get("originator"), "不带 Codex 身份头")
	require.Empty(t, req.Header.Get("Content-Encoding"), "不压缩")

	body := upstream.lastBody
	require.Equal(t, "gpt-6-astra", gjson.GetBytes(body, "model").String())
	require.Equal(t, "explicit", gjson.GetBytes(body, "model_selection").String())
	require.False(t, gjson.GetBytes(body, "store").Bool())
	require.True(t, gjson.GetBytes(body, "stream").Bool())
	require.Equal(t, "xhigh", gjson.GetBytes(body, "reasoning_effort").String(), "max → xhigh")
	scopedKey := scopeCodexAccountIdentityValue(account, 0, "prompt-cache", "sess-1")
	require.NotEqual(t, "sess-1", scopedKey)
	require.Equal(t, scopedKey, gjson.GetBytes(body, "prompt_cache_key").String(), "prompt_cache_key 按账号命名空间改写，与原路径同口径")
	// BPS 的请求 schema 是封闭的：多一个它不认的键就整条 422 "Invalid request body."。
	// include / max_output_tokens 两个 2026-09-29 直连实测各自单独加都 422，所以也在禁发清单里。
	for _, absent := range []string{
		"tools", "tool_choice", "instructions", "reasoning", "include", "max_output_tokens",
	} {
		require.False(t, gjson.GetBytes(body, absent).Exists(), absent)
	}
	require.Equal(t, "compaction", gjson.GetBytes(body, "context_management.0.type").String())
	require.Equal(t, "developer", gjson.GetBytes(body, "input.0.role").String())
	require.Equal(t, "base prompt", gjson.GetBytes(body, "input.0.content.0.text").String())
	catalog := gjson.GetBytes(body, "input.1.content.0.text").String()
	require.Equal(t, "developer", gjson.GetBytes(body, "input.1.role").String())
	require.Contains(t, catalog, `"exec_command"`)
	require.Contains(t, catalog, `"cmd" (required)`)
	require.Contains(t, catalog, openAIBasisPointsCustomMarker+"apply_patch")
	require.Contains(t, catalog, "web_search", "默认 cached 声明留在 BPS，提示里说明托管搜索不可用")
	require.NotContains(t, string(body), "sub2api")
	require.Equal(t, "user", gjson.GetBytes(body, "input.2.role").String())
	require.False(t, gjson.GetBytes(body, "input.2.internal_chat_message_metadata_passthrough").Exists())
	require.Len(t, gjson.GetBytes(body, "input").Array(), 3)
	metadata := gjson.GetBytes(body, "metadata")
	require.Len(t, metadata.Get("task_id").String(), 36)
	require.Len(t, metadata.Get("turn_id").String(), 36)
	require.Equal(t, "1", metadata.Get("agent_iteration").String())

	out := rec.Body.String()
	require.Contains(t, out, `"delta":"Let me check."`, "文本实时透传")
	require.Contains(t, out, `"name":"exec_command"`)
	require.Contains(t, out, `"call_id":"call_1"`)
	require.Contains(t, out, `\"cmd\":\"pwd\"`)
	require.NotContains(t, out, "run_officejs", "原生传输项不能漏给客户端")
	require.NotContains(t, out, `"summary":"Run client tool"`)
	require.Contains(t, out, "response.function_call_arguments.done")
	require.Contains(t, out, "event: response.completed")
	require.Equal(t, 20, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
	require.Equal(t, openAIBasisPointsUpstreamEndpoint, result.UpstreamEndpoint)
	require.Equal(t, "gpt-6-astra", result.Model)
	require.Equal(t, "gpt-6-astra", result.UpstreamResponseModel)
	require.NotNil(t, result.ReasoningEffort)
	require.Equal(t, "xhigh", *result.ReasoningEffort)
	require.Equal(t, "rid_bps", result.RequestID)
}

func TestOpenAIBasisPoints_SecondTurnReplaysNativeTransportItem(t *testing.T) {
	account := newBasisPointsTestAccount(9102)
	firstCtx, _ := newBasisPointsTestContext(t)
	first := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream",
		bpsTestCompletedSSE(bpsTestTransportCall("call_2", `{"tool":"exec_command","args":{"cmd":"ls"}}`)))}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: first}).Forward(context.Background(), firstCtx, account, []byte(bpsTestClientBody))
	require.NoError(t, err)
	firstTurnID := gjson.GetBytes(first.lastBody, "metadata.turn_id").String()

	secondBody := strings.Replace(bpsTestClientBody,
		`"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}],"internal_chat_message_metadata_passthrough":{"turn_id":"t1"}}]`,
		`"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}],"internal_chat_message_metadata_passthrough":{"turn_id":"t1"}},
			{"type":"function_call","call_id":"call_2","name":"exec_command","arguments":"{\"cmd\":\"ls\"}"},
			{"type":"function_call_output","call_id":"call_2","output":""}]`, 1)
	require.NotEqual(t, bpsTestClientBody, secondBody)
	secondCtx, rec := newBasisPointsTestContext(t)
	second := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
	_, err = (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: second}).Forward(context.Background(), secondCtx, account, []byte(secondBody))
	require.NoError(t, err)

	body := second.lastBody
	items := gjson.GetBytes(body, "input").Array()
	require.Len(t, items, 5, "developer×2 + user + 原生调用 + 结果")
	native := items[3]
	require.Equal(t, "function_call", native.Get("type").String())
	require.Equal(t, "run_officejs", native.Get("name").String(), "回放 BPS 自己的原生项")
	require.Equal(t, "fc_call_2", native.Get("id").String())
	require.Equal(t, "Run client tool", native.Get("summary").String(), "缓存的原生项连模型字段一起回放")
	output := items[4]
	require.Equal(t, "function_call_output", output.Get("type").String())
	require.Equal(t, "fc_call_2", output.Get("id").String())
	require.Equal(t, openAIBasisPointsEmptyOutput, output.Get("output").String())
	require.Equal(t, "2", gjson.GetBytes(body, "metadata.agent_iteration").String())
	require.Equal(t, firstTurnID, gjson.GetBytes(body, "metadata.turn_id").String(), "同一用户回合的工具轮次不换 turn_id")
	require.Contains(t, rec.Body.String(), `"text":"done"`)
}

func TestOpenAIBasisPoints_HistoryWithoutCacheIsRebuiltFromClientCall(t *testing.T) {
	account := newBasisPointsTestAccount(9103)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"custom_tool_call","call_id":"call_cold","name":"apply_patch","input":"*** Begin Patch\n*** End Patch"},
		{"type":"custom_tool_call_output","call_id":"call_cold","output":"ok"},`, 1)
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	items := gjson.GetBytes(upstream.lastBody, "input").Array()
	rebuilt := items[2]
	require.Equal(t, "run_officejs", rebuilt.Get("name").String())
	code := gjson.Get(rebuilt.Get("arguments").String(), "code").String()
	require.Equal(t, "apply_patch", gjson.Get(code, "name").String())
	require.Equal(t, "*** Begin Patch\n*** End Patch", gjson.Get(code, "input").String())
	require.Equal(t, "function_call_output", items[3].Get("type").String(), "custom 结果统一成 function_call_output")
	require.Equal(t, "ok", items[3].Get("output").String())
}

// BPS 解不开别处（Codex 路径）铸的推理密文，按 400 invalid_encrypted_content 拒掉整条请求。
// 客户端每一轮都会原样回放同一批 blob，所以不记 lineage 就是「这个会话从此每轮硬报错」。
// 参考实现 ranxi2001/sub2api 的 openai_excel_bps_encrypted.go 印证了这条错误码在 BPS 上存在。
func TestOpenAIBasisPoints_InvalidEncryptedContentIsRememberedSoTheNextTurnStrips(t *testing.T) {
	account := newBasisPointsTestAccount(9131)
	body := []byte(strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"reasoning","id":"rs_cold","summary":[],"encrypted_content":"blob-minted-on-the-codex-route"},`, 1))
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusBadRequest, "application/json",
			`{"error":{"code":"invalid_encrypted_content","message":"The encrypted content could not be verified"}}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	// 同一个 service 实例：lineage 存在它自己的 WS state store 上，换实例就测不到跨轮。
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}

	first, rec := newBasisPointsTestContext(t)
	_, err := svc.Forward(context.Background(), first, account, body)
	requireOpenAIBasisPointsUnavailable(t, err, rec, upstream, account, "status_400_invalid_encrypted_content")
	require.Contains(t, string(upstream.bodies[0]), "blob-minted-on-the-codex-route", "第一轮照发，才有被拒这回事")

	// 第二轮同一会话（prompt_cache_key 不变 → 会话哈希不变）：出站体里不该再有那个 blob。
	second, _ := newBasisPointsTestContext(t)
	_, err = svc.Forward(context.Background(), second, account, body)
	require.NoError(t, err)
	require.Len(t, upstream.bodies, 2)
	require.NotContains(t, string(upstream.bodies[1]), "blob-minted-on-the-codex-route")

	// **不许污染 Codex 路径。**「BPS 解不开」≠「这个 blob 失效了」—— 它在 Codex 后端仍然有效。
	// 分组里混着开/关开关的账号是用户允许的形态，共享一把键会让下一轮落到没开开关的账号时，
	// Codex 路径把本来能用的推理密文也剥掉：整个会话在 sticky TTL 内每轮从零推理，且没有任何
	// 客户端可见信号 —— 正好是「宁可失败也不要静默降质」的反面。
	third, _ := newBasisPointsTestContext(t)
	require.Empty(t, svc.sessionInvalidEncryptedContentDigests(
		getOpenAIGroupIDFromContext(third), svc.openAIWSLineageSessionHashFromContext(third, body)),
		"Codex 路径读的是裸哈希那把键，不该看到 BPS 记的摘要")
	require.NotEmpty(t, svc.sessionInvalidEncryptedContentDigests(
		getOpenAIGroupIDFromContext(third),
		openAIBasisPointsLineageSessionKey(svc.openAIWSLineageSessionHashFromContext(third, body))),
		"BPS 自己那把键上记着")
}

// requireOpenAIBasisPointsUnavailable：开了开关的账号，落回原因一律做成客户端可见的终态错误
// （用户 2026-09-29 定的口径：满血和降智掺杂比直接失败更糟）。四条性质一起钉：
// ①没有第二次上游请求（没走 Codex）②精确 reason + 正确状态码 ③不换号（不是 UpstreamFailoverError）
// ④不罚账号（包着 ErrOpenAIRawRelayNotAccountFault，账号状态不动）。
//
// 状态码不是恒 502：上游自己回的 4xx/5xx 原样透出（`status_<code>` 形态的 reason），
// 403 永久封和 429 瞬时限流在客户端侧必须可区分。本层判定的才是 502。
func requireOpenAIBasisPointsUnavailable(t *testing.T, err error, rec *httptest.ResponseRecorder,
	upstream *httpUpstreamRecorder, account *Account, reason string) {
	t.Helper()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "不该拖低账号调度分")
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "不能换号：换到没开开关的账号就又走回 Codex")
	require.Contains(t, err.Error(), reason)
	require.Len(t, upstream.requests, 1, "不该有第二次上游请求")
	require.Equal(t, openAIBasisPointsResponsesURL, upstream.requests[0].URL.String())
	require.Equal(t, openAIBasisPointsUnavailableStatus(reason), rec.Code)
	require.Contains(t, rec.Body.String(), "basispoints_"+reason)
	require.Equal(t, StatusActive, account.Status)
}

// 三道「BPS 的失败不改 Codex 账号状态」闸门。第十轮变异实测：三条各自删掉整仓库照旧全绿 ——
// 逻辑没错，但下一个人删掉它不会有任何信号，而这个功能历史上已经六次「注释写的不变式是假的」。
//
// 这三道守的是同一件事：BPS 打的是另一个 host，它的 401/429/403 与 Codex 后端的账号健康无关；
// 拿它去标限流 / 封号会让一个满血账号被误判，而误判的方向正是「把流量推给没开开关的账号」= 掺杂。
func TestOpenAIBasisPointsAccountStateGatesAreWired(t *testing.T) {
	// 结构化 403（凭据被拒）：非 BPS 轮次上它会 SetError + 摘池，是这三道闸门里最容易钉住的一格。
	payload := []byte(`{"type":"response.failed","response":{"error":{"type":"permission_error","code":"invalid_api_key","status_code":403,"message":"credential rejected"}}}`)

	t.Run("流终态副作用：BPS 轮次不改账号状态", func(t *testing.T) {
		repo := &openAIStream403AccountRepo{}
		svc := &OpenAIGatewayService{rateLimitService: &RateLimitService{accountRepo: repo}}
		account := &Account{ID: 9160, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
		c, _ := newBasisPointsTestContext(t)
		markOpenAIBasisPointsResponse(c, true)
		status, disabled := svc.handleOpenAIStreamTerminalAccountSideEffects(c, account, payload, "credential rejected", nil)
		require.Equal(t, http.StatusForbidden, status, "状态码照旧算出来")
		require.False(t, disabled, "但一个字节的账号状态都不许动")
		require.Zero(t, repo.setErrorCalls)
		require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))

		// 反面：同一发在非 BPS 轮次上是要改状态的 —— 否则上面那几条可能只是因为别的原因恒假。
		plainRepo := &openAIStream403AccountRepo{}
		plainSvc := &OpenAIGatewayService{rateLimitService: &RateLimitService{accountRepo: plainRepo}}
		plainAccount := &Account{ID: 9162, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
		plain, _ := newBasisPointsTestContext(t)
		_, plainDisabled := plainSvc.handleOpenAIStreamTerminalAccountSideEffects(plain, plainAccount, payload, "credential rejected", nil)
		require.True(t, plainDisabled, "非 BPS 轮次照原路径处置")
		require.Equal(t, 1, plainRepo.setErrorCalls)
	})

	t.Run("Forward 开头把标记清掉", func(t *testing.T) {
		// 同一个 gin ctx 上前一次 attempt 留下的 BPS 标记不能让下一次（可能落到没开开关的账号）
		// 继续享受这些闸门 —— 那会让真正需要处置的 Codex 失败被静默跳过。
		c, _ := newBasisPointsTestContext(t)
		markOpenAIBasisPointsResponse(c, true)
		require.True(t, isOpenAIBasisPointsResponse(c))
		account := newBasisPointsTestAccount(9161)
		account.Extra = map[string]any{}
		upstream := &httpUpstreamRecorder{err: errors.New("dial tcp 1.2.3.4:443: i/o timeout")}
		_, _ = (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(
			context.Background(), c, account, []byte(bpsTestClientBody))
		require.False(t, isOpenAIBasisPointsResponse(c), "Forward 开头必须清掉上一次 attempt 的标记")
	})
}

// 状态码映射本身单独钉一遍，别让 requireOpenAIBasisPointsUnavailable 拿同一个函数自证。
func TestOpenAIBasisPointsUnavailableStatus(t *testing.T) {
	for reason, want := range map[string]int{
		"status_403_usage_policy": http.StatusForbidden,
		"status_429":              http.StatusTooManyRequests,
		"status_500":              http.StatusInternalServerError,
		"status_502_stream":       http.StatusBadGateway,
		"status_504_stream":       http.StatusGatewayTimeout,
		"tool_choice":             http.StatusBadGateway,
		"service_tier":            http.StatusBadGateway,
		"stream_incomplete":       http.StatusBadGateway,
		"status_999":              http.StatusBadGateway,
		"status_abc":              http.StatusBadGateway,
		// 401 / 407 刻意不透：客户端会把它们当成自己的凭据失效去清 token、要求重登，
		// 而实际是本站这个账号打 BPS 被拒。
		"status_401_invalid_api_key": http.StatusBadGateway,
		"status_407":                 http.StatusBadGateway,
		// 流内 error.code 是**上游可控**的字符串，不许它决定我们回客户端的状态码。
		"stream_status_503":    http.StatusBadGateway,
		"stream_Status_401":    http.StatusBadGateway,
		"stream_status_0499":   http.StatusBadGateway,
		"stream_rate_limit":    http.StatusBadGateway,
		"stream_model_changed": http.StatusBadGateway,
	} {
		require.Equal(t, want, openAIBasisPointsUnavailableStatus(reason), reason)
	}
}

// Fast 策略以前排在 BPS 分派点之后（openai_gateway_passthrough.go:278），于是开了开关的账号
// 完全绕过它：管理员配的 Block 闸门失效，强制 Fast 分组还会静默降级。BPS 收不了 service_tier
// （2026-09-29 直连实测 422），所以带 Fast 意图的请求只能报错。
func TestOpenAIBasisPoints_ServiceTierIsAHardErrorBeforeUpstream(t *testing.T) {
	account := newBasisPointsTestAccount(9121)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	body := strings.Replace(bpsTestClientBody, `"store":false`, `"store":false,"service_tier":"priority"`, 1)
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault)
	require.Contains(t, err.Error(), "service_tier")
	require.Empty(t, upstream.requests, "判定在发请求之前，一次上游请求都不该发")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "basispoints_service_tier")
	require.Equal(t, StatusActive, account.Status)
}

func TestOpenAIBasisPoints_OrphanToolOutputIsAHardError(t *testing.T) {
	account := newBasisPointsTestAccount(9104)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[{"type":"function_call_output","call_id":"call_lost","output":"ok"},`, 1)
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault)
	require.Contains(t, err.Error(), "tool_history")
	require.Empty(t, upstream.requests, "找不回原生项是发请求前就判定的，一次上游请求都不该发")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "basispoints_tool_history")
}

func TestOpenAIBasisPoints_ExplicitWebSearchIsAHardError(t *testing.T) {
	account := newBasisPointsTestAccount(9105)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
	body := strings.Replace(bpsTestClientBody, `"external_web_access":false`, `"external_web_access":true`, 1)
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "web_search")
	require.Empty(t, upstream.requests)
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "basispoints_web_search")
}

func TestOpenAIBasisPoints_ModelAccessChangedIsAHardError(t *testing.T) {
	account := newBasisPointsTestAccount(9106)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusBadRequest, "application/json", `{"error":{"code":"basispoints_model_access_changed","message":"model unavailable"}}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Nil(t, result)
	requireOpenAIBasisPointsUnavailable(t, err, rec, upstream, account, "model_access_changed")
}

// 原生 v2 压缩回合是裸 /responses + input 里一个 compaction_trigger，路径后缀判空挡不住它
// （normalizeOpenAIResponsesCompactRequest 对它 return body, true 不改路径）。进 BPS 的话
// compaction_trigger 会被挪到数组末尾、前面还插三条 Excel prologue、beta 头也丢了，
// 而这是 Codex CLI 自动压缩的常态形态。它必须走原路径，而且不能变成硬报错。
// 原生 v2 压缩回合**也走 BPS**（2026-09-29 直连实测 BPS 支持，见 openai_gateway_forward.go 那段
// 的实测记录）。它原来是唯一还会真的发降智请求的地方：豁免掉意味着每约 20 万 token 有一整轮摘要
// 由降智的 Codex 生成，而那份摘要成为此后每一轮 BPS 请求的历史 —— 用户口径是「打开开关后不允许
// 降智的请求」。上游那套 compaction 事件本层原样透传（compaction 项不是工具项，不扣留不翻译）。
func TestOpenAIBasisPoints_NativeCompactionTurnGoesToBasisPoints(t *testing.T) {
	account := newBasisPointsTestAccount(9130)
	c, rec := newBasisPointsTestContext(t)
	const compactionItem = `{"type":"compaction","id":"cmp_1"}`
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
			`{"type":"response.created","response":{"id":"r","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
			`{"type":"response.output_item.added","output_index":0,"item":`+compactionItem+`}`,
			`{"type":"response.compaction.compacting","output_index":0}`,
			`{"type":"response.output_item.done","output_index":0,"item":`+compactionItem+`}`,
			`{"type":"response.completed","response":{"id":"r","status":"completed","model":"gpt-6-astra","output":[`+compactionItem+`],"usage":{"input_tokens":20,"output_tokens":5,"total_tokens":25}}}`,
		)),
	}}
	// trigger 上挂一个未知键：它是唯一一个逐字透传客户端对象的 item，而上游对未知 item 字段是整单
	// 400，客户端每轮回放同一批历史 ⇒ 一个多出来的键就是「这个会话从此每轮都失败」。裸 trigger 咬不到
	// 那条白名单（第十一轮变异实测：删掉它整套照旧全绿）。
	body := `{"model":"gpt-6-astra","stream":true,"input":[` +
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]},` +
		`{"type":"compaction_trigger","zzz_unknown":"LEAKED"}]}`
	require.True(t, HasCompactionTriggerInInput([]byte(body)), "前提：这就是 v2 压缩回合的形态")
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.Len(t, upstream.requests, 1)
	require.Equal(t, openAIBasisPointsResponsesURL, upstream.requests[0].URL.String(), "压缩回合要打到 BPS")
	require.True(t, HasCompactionTriggerInInput(upstream.bodies[0]), "trigger 要发出去")
	require.NotContains(t, string(upstream.bodies[0]), "zzz_unknown", "trigger 上的未知字段要丢掉")
	out := rec.Body.String()
	require.NotContains(t, out, "basispoints_", "不能被硬报错拦下")
	require.Contains(t, out, "response.compaction.compacting", "上游的 compaction 事件原样透传")
	require.Contains(t, out, `"type":"compaction"`, "compaction 输出项原样透传")
	require.NotContains(t, out, "run_officejs", "compaction 项不是工具项，不该被当信封翻译")
}

// 只按模型名请求生图（不带 image_generation 工具）以前既绕开分组闸门、又把模型名带着
// 账号主人的 bearer token 发到 bps.openai.com。BPS 做不了生图，必须在发请求前判死。
func TestOpenAIBasisPoints_ImageModelIsAHardErrorBeforeUpstream(t *testing.T) {
	account := newBasisPointsTestAccount(9131)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	body := `{"model":"gpt-image-1","stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"draw a cat"}]}]}`
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault)
	require.Empty(t, upstream.requests, "一个字节都不该发出去")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "basispoints_image_generation")
	require.Equal(t, StatusActive, account.Status)
}

// Fast 策略的白名单按**上游 slug**配，而 BPS 分派点排在 markPatchSet("model", upstreamModel)
// 之前。以前直接拿体里客户端请求的名字去查，配了模型映射的账号上闸门就不命中 —— 管理员配的
// Block 规则被一个账号级开关绕过，客户端拿到的是一条 BPS 错误码而不是管理员写的拒绝文案。
func TestOpenAIBasisPointsFastPolicyBlockUsesMappedUpstreamModel(t *testing.T) {
	account := newBasisPointsTestAccount(9132)
	account.Credentials["model_mapping"] = map[string]any{"gpt-6-astra": "gpt-5.6-sol"}
	_, upstreamModel := resolveOpenAIForwardMappedModels(account, "gpt-6-astra", false)
	require.Equal(t, "gpt-5.6-sol", upstreamModel, "前提：映射生效，客户端名与上游 slug 不同")

	// Block 规则只点名上游 slug；按客户端请求的名字查就不命中，会掉到 fallback pass。
	ctx := withOpenAIFastPolicyContext(context.Background(), &OpenAIFastPolicySettings{
		Rules: []OpenAIFastPolicyRule{{
			ServiceTier: OpenAIFastTierPriority, Action: BetaPolicyActionBlock, Scope: "all",
			ModelWhitelist: []string{"gpt-5.6-sol"}, FallbackAction: BetaPolicyActionPass,
			ErrorMessage: "fast is disabled by the administrator",
		}},
	})
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	body, err := sjson.SetBytes([]byte(bpsTestClientBody), "service_tier", OpenAIFastTierPriority)
	require.NoError(t, err)
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream,
		settingService: &SettingService{cfg: &config.Config{}}}
	result, forwardErr := svc.Forward(ctx, c, account, body)

	require.Nil(t, result)
	require.Error(t, forwardErr)
	require.Empty(t, upstream.requests, "被 Block 的请求一个字节都不该发出去")
	require.Equal(t, http.StatusForbidden, rec.Code, "要的是管理员那条 403，不是 BPS 的 502")
	require.Contains(t, rec.Body.String(), "fast is disabled by the administrator")
	require.NotContains(t, rec.Body.String(), "basispoints_service_tier", "闸门必须命中，不能掉到 BPS 自己的错误码")
	// 策略拒绝是客户端的问题：不能拖低账号调度分，也不能在写完的 403 尾部再追一条 SSE。
	require.ErrorIs(t, forwardErr, ErrOpenAIRawRelayNotAccountFault)
	require.True(t, IsResponseCommitted(c))
	require.Equal(t, StatusActive, account.Status)
}

// service_tier 的闸门只能拦真 Fast 档。normalizeOpenAIServiceTier 把 auto/default/flex/scale
// 也当合法值留在体里（flex 反而更慢更便宜），按「字段存在」判会把它们一起打成硬 502，
// 而出站白名单根本不带 service_tier，拦住它们保护不了任何东西。
func TestOpenAIBasisPoints_NonFastServiceTiersStillReachUpstream(t *testing.T) {
	for _, tier := range []string{"auto", "default", "flex", "scale"} {
		account := newBasisPointsTestAccount(9133)
		c, rec := newBasisPointsTestContext(t)
		upstream := &httpUpstreamRecorder{responses: []*http.Response{
			bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
		}}
		body, err := sjson.SetBytes([]byte(bpsTestClientBody), "service_tier", tier)
		require.NoError(t, err)
		_, _ = (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, body)
		require.NotContains(t, rec.Body.String(), "basispoints_service_tier", tier)
		require.Len(t, upstream.requests, 1, tier)
		require.Equal(t, openAIBasisPointsResponsesURL, upstream.requests[0].URL.String(), tier)
		require.False(t, gjson.GetBytes(upstream.bodies[0], "service_tier").Exists(),
			"出站白名单里没有 service_tier："+tier)
	}
}

// 上游接受之后的流内失败走共用处理器，它有 6 处产出 *UpstreamFailoverError。那种 error 漏出去
// 就会换号 —— 换到没开开关的账号就在 Codex 上重跑同一个请求（掺杂），还会罚这个账号的调度分。
//
// 这条以前是**死代码**：判据写的是 `result == nil`，而 handleStreamingResponseWithReasoning 里的
// usage 是 `&OpenAIUsage{}` 恒非 nil、被 resultWithUsage() 原样带出，于是 result 恒非 nil。
// 现在判据是 openAIUsageHasTokens(usage)：零 token 本来就没账可记。
func TestOpenAIBasisPoints_StreamFailoverIsCollapsedIntoHardError(t *testing.T) {
	account := newBasisPointsTestAccount(9134)
	c, rec := newBasisPointsTestContext(t)
	// 上游接受（首事件 response.created 正常），流正常结束但**没有任何终态事件**：处理器判
	// "stream ended before a terminal event" → resultWithUsage() + *UpstreamFailoverError(502)。
	// 注意不能用「created 之后直接 EOF」—— 那个形态会被 peek 接住判 stream_eof，根本进不了处理器，
	// 也就测不到这条收口（我第一版就写错在这里）。
	truncated := "event: response.created\ndata: " +
		`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}` +
		"\n\ndata: [DONE]\n\n"
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", truncated),
		// 队列里留第二条：一旦换号 / 重试，它就会被取走，requests 也会变成 2。
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))

	require.Nil(t, result, "零 token 的失败不该带出 result，否则 Forward 侧的收口失效")
	require.Error(t, err)
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "不能换号：换到没开开关的账号就又走回 Codex")
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "不该拖低账号调度分")
	require.Len(t, upstream.requests, 1, "不该有第二次上游请求")
	require.Equal(t, StatusActive, account.Status)
	require.Contains(t, rec.Body.String(), "basispoints_status_502_stream",
		"客户端要拿到一条明确的 basispoints 错误，而不是静默换号后的满血/降智混答")
}

// 去掉 `result == nil` 之后新出现的组合：usage 已经到手（有 token）+ 流内 failover。
// 这一支必须既收口成硬报错（不换号、不罚分），又把 result 带出来让计费照常记 —— 额度已经消耗了。
func TestOpenAIBasisPoints_StreamFailoverWithUsageStillBillsAndDoesNotFailover(t *testing.T) {
	account := newBasisPointsTestAccount(9135)
	c, rec := newBasisPointsTestContext(t)
	// response.created → 一个带 usage 的非终态快照 → [DONE]，全程没有终态事件。
	sse := bpsTestSSE(
		`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
		`{"type":"response.in_progress","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[],`+
			`"usage":{"input_tokens":22516,"output_tokens":48,"total_tokens":22564,"input_tokens_details":{"cached_tokens":22516}}}}`,
	)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", sse),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))

	require.Error(t, err)
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "不能换号")
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "不该拖低账号调度分")
	require.Len(t, upstream.requests, 1, "不该有第二次上游请求")
	require.Equal(t, StatusActive, account.Status)
	require.Contains(t, rec.Body.String(), "basispoints_")
	// 无条件断言：这个用例存在的唯一理由就是钉「usage 到手时 result 必须带出来」，
	// 包在 if result != nil 里的话，收口一旦回退成丢 result，前面那几条断言全部照旧成立、用例仍然绿。
	require.NotNil(t, result, "拿到 usage 就必须带出 result，否则这一发的额度记不到账上")
	// 端点仍是 BPS（否则 route_gateway 会被记成 Codex 的读数）。
	require.Equal(t, openAIBasisPointsUpstreamEndpoint, result.UpstreamEndpoint)
	require.True(t, openAIUsageHasTokens(&result.Usage), "额度已经消耗，不能丢账")
}

// 两位评审各自独立实测到的 blocker：共用处理器除了 6 处 *UpstreamFailoverError，还有 9 处返回
// **裸 error**（openai_gateway_response_handling.go 的 `upstream response failed: …` 等），
// 收口只认 failover error，裸 error 原样漏出去后 handler 的
// `!errors.Is(err, ErrOpenAIRawRelayNotAccountFault)` 成立 → 罚这个账号的调度分。
//
// 而并发打出来的 429 与 usage-policy 403 恰好是「首输出之后回 response.failed」这个形态，正落在漏口上。
// 后果是自我强化的：BPS 账号被降权 ⇒ 调度器更倾向挑没开开关的账号 ⇒ Codex 降智答案回来了。
func TestOpenAIBasisPoints_BareStreamErrorAfterOutputIsStillNotAccountFault(t *testing.T) {
	account := newBasisPointsTestAccount(9137)
	c, _ := newBasisPointsTestContext(t)
	// created → 一条真实文本增量（此时 wroteDownstream 已为真、peek 已放行）→ response.failed。
	// 处理器对 response.failed 返回裸 `upstream response failed: …`，不是 *UpstreamFailoverError。
	sse := bpsTestSSE(
		`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
		`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"hi"}`,
		`{"type":"response.failed","response":{"id":"resp_bps","status":"failed","model":"gpt-6-astra","output":[],`+
			`"error":{"code":"rate_limit_exceeded","message":"too many"},`+
			`"usage":{"input_tokens":31,"output_tokens":2,"total_tokens":33}}}`,
	)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", sse),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))

	require.Error(t, err)
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "这一族本来就不是 failover error")
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault,
		"BPS 通道被限流跟 Codex 后端的账号健康无关，不能罚调度分")
	// 哨兵必须追加在尾部：handler 的 openAIForwardErrorAlreadyCommunicated 按这个前缀判断响应
	// 是否已写出，前缀被顶掉就会在已写出的 SSE 尾部再追一条 502。
	require.True(t, strings.HasPrefix(err.Error(), "upstream response failed:"),
		"哨兵不能前缀包裹，会让 ensureForwardErrorResponse 在已写出的 SSE 后面再追一条 502，实际是 %q", err.Error())
	require.Len(t, upstream.requests, 1, "不该有第二次上游请求")
	require.Equal(t, StatusActive, account.Status)
	require.NotNil(t, result, "usage 已到手，必须带出来记账")
	require.Equal(t, openAIBasisPointsUpstreamEndpoint, result.UpstreamEndpoint)
	require.True(t, openAIUsageHasTokens(&result.Usage), "额度已经消耗，不能丢账")
}

// Fast 策略 action=filter 的语义是「剥掉 service_tier、按标准档正常服务」。分派点的预检按策略
// 处理后的体判、而内层那道网按原始体判，filter 正好落在两套判据的差集里 —— 这条请求会被打成
// 硬 502，连上游都不打。两位评审各自独立实测到这个回归。
func TestOpenAIBasisPoints_FastPolicyFilterStillServesThroughBasisPoints(t *testing.T) {
	account := newBasisPointsTestAccount(9136)
	ctx := withOpenAIFastPolicyContext(context.Background(), &OpenAIFastPolicySettings{
		Rules: []OpenAIFastPolicyRule{{
			ServiceTier: OpenAIFastTierPriority, Action: BetaPolicyActionFilter, Scope: "all",
		}},
	})
	for _, tier := range []string{OpenAIFastTierPriority, "fast"} {
		c, rec := newBasisPointsTestContext(t)
		upstream := &httpUpstreamRecorder{responses: []*http.Response{
			bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
		}}
		body, err := sjson.SetBytes([]byte(bpsTestClientBody), "service_tier", tier)
		require.NoError(t, err)
		svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream,
			settingService: newBasisPointsTestSettingService()}
		_, forwardErr := svc.Forward(ctx, c, account, body)

		require.NoError(t, forwardErr, tier)
		require.NotContains(t, rec.Body.String(), "basispoints_service_tier", tier)
		require.Len(t, upstream.requests, 1, tier)
		require.Equal(t, openAIBasisPointsResponsesURL, upstream.requests[0].URL.String(), tier)
		require.False(t, gjson.GetBytes(upstream.bodies[0], "service_tier").Exists(), tier)
	}
}

func TestOpenAIBasisPoints_UpstreamRejectionIsAHardErrorWithoutSideEffects(t *testing.T) {
	account := newBasisPointsTestAccount(9107)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Content-Type": []string{"application/json"}, "Retry-After": []string{"7"}},
			Body: io.NopCloser(strings.NewReader(`{"error":{"code":"rate_limit_exceeded","message":"slow down oauth-token chatgpt-acc"}}`))},
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Nil(t, result)
	// BPS 的 429 / 401 / 403 / 5xx 仍然与 Codex 账号状态无关：不换号、不改账号状态，只是不再落回。
	// 状态码和 Retry-After 要透出去：429（并发打出来的，瞬时）和 403（usage policy 封掉这个号的
	// BPS 通道，永久）在客户端侧必须可区分，全塌成 502 之后两者一样。
	requireOpenAIBasisPointsUnavailable(t, err, rec, upstream, account, "status_429_rate_limit_exceeded")
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, "7", rec.Header().Get("Retry-After"))
	require.NotContains(t, rec.Body.String(), "slow down", "上游错误文案不透给客户端")
}

func TestOpenAIBasisPoints_FirstStreamEventErrorIsAHardError(t *testing.T) {
	account := newBasisPointsTestAccount(9113)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(`{"type":"error","code":"rate_limit_exceeded","message":"Rate limit reached"}`)),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Nil(t, result)
	requireOpenAIBasisPointsUnavailable(t, err, rec, upstream, account, "stream_rate_limit_exceeded")
	require.NotContains(t, rec.Body.String(), "Rate limit reached")
}

func TestOpenAIBasisPoints_MissingProxyBindingFailsClosed(t *testing.T) {
	account := newBasisPointsTestAccount(9114)
	proxyID := int64(7)
	account.ProxyID = &proxyID // 绑了代理但快照没带出来：不能用本机 IP 直连 bps.openai.com
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Error(t, err)
	require.Contains(t, err.Error(), "proxy 7")
	require.Empty(t, upstream.requests)
}

func TestOpenAIBasisPoints_ClientModelNameSurvivesMapping(t *testing.T) {
	account := newBasisPointsTestAccount(9115)
	account.Credentials["model_mapping"] = map[string]any{"gpt-5": "gpt-6-astra"}
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
	body := strings.Replace(bpsTestClientBody, `"model":"gpt-6-astra"`, `"model":"gpt-5"`, 1)
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.Equal(t, "gpt-6-astra", gjson.GetBytes(upstream.lastBody, "model").String(), "上游发映射后的名字")
	require.Equal(t, "gpt-5", result.Model, "与原路径同口径：Model 是客户端的名字")
	require.Equal(t, "gpt-6-astra", result.BillingModel)
	require.Equal(t, "gpt-6-astra", result.UpstreamModel)
	require.Contains(t, rec.Body.String(), `"model":"gpt-5"`, "客户端看到自己的模型名")
	require.NotContains(t, rec.Body.String(), `"model":"gpt-6-astra"`)
}

func TestOpenAIBasisPoints_ResponsesSubPathsStayOnCodex(t *testing.T) {
	account := newBasisPointsTestAccount(9116)
	c, _ := newBasisPointsTestContext(t)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/input_tokens", bytes.NewReader(nil))
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
	_, _ = (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.NotEmpty(t, upstream.requests, "计数子路径照走原路径")
	for _, req := range upstream.requests {
		require.NotEqual(t, openAIBasisPointsResponsesURL, req.URL.String())
	}
}

// **legacy compact 回合判死。** 它与已删掉的原生 v2 豁免逐字同一个后果：那一轮摘要由降智的 Codex
// 生成，然后成为此后每一轮 BPS 请求的历史 ⇒ 同一个会话一半满血一半降智，而客户端拿到 HTTP 200 +
// 一份正常摘要，没有任何可见信号（唯一读数是事后 usage_logs.upstream_endpoint）。第十二轮 blocker。
//
// 第二条命中路径是裸 /responses 带 compaction_trigger 但 stream 不是 bool true：handler 的
// body-signal 提升会改写 c.Request.URL.Path 成 /compact（codex.remote_compact.detected_body_signal），
// 所以这里直接按改写后的路径建 context 就等价。
func TestOpenAIBasisPoints_LegacyCompactPathIsAHardError(t *testing.T) {
	account := newBasisPointsTestAccount(9168)
	// 嵌套形态必须一起咬：`/responses/compact/<子路径>` 在本仓是一等公民（路由 `/responses/*subpath`、
	// endpoint.go 把四种嵌套形态都归到 EndpointResponsesCompact、handler 用例名就叫 nested_compact），
	// 而判死原来写的是精确相等 `== "/compact"` —— 漏掉它等于这一格照旧静默走 Codex。
	for _, path := range []string{
		"/v1/responses/compact",
		"/backend-api/codex/responses/compact",
		"/v1/responses/compact/detail",
		"/openai/v1/responses/compact/00000000-0000-4000-8000-000000000000",
	} {
		t.Run(path, func(t *testing.T) {
			c, rec := newBasisPointsTestContext(t)
			c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(nil))
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
			}}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			_, err := svc.Forward(context.Background(), c, account, []byte(bpsTestClientBody))
			require.Error(t, err)
			require.Contains(t, err.Error(), "legacy_compact_path")
			require.Empty(t, upstream.requests, "一个字节都不许发给 Codex 后端")
			require.Equal(t, http.StatusBadGateway, rec.Code, "本层判定的一律 502")
			require.Contains(t, rec.Body.String(), "basispoints_legacy_compact_path")
			require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "不是账号的错")
			require.Equal(t, StatusActive, account.Status)
		})
	}
}

func TestOpenAIBasisPointsHeadersCarryAccountUserIDFromJWT(t *testing.T) {
	claims := base64.RawURLEncoding.EncodeToString([]byte(`{"https://api.openai.com/auth":{"chatgpt_account_user_id":"user_123"}}`))
	headers := openAIBasisPointsHeaders("h."+claims+".s", "acc")
	require.Equal(t, "user_123", headers.Get("X-OpenAI-Account-User-ID"))
	require.Equal(t, "acc", headers.Get("ChatGPT-Account-ID"))
	require.Empty(t, openAIBasisPointsHeaders("opaque-token", "acc").Get("X-OpenAI-Account-User-ID"), "不是 JWT 就不发")
}

func bpsTestWSPayload() []byte {
	return []byte(`{"type":"response.create",` + strings.TrimPrefix(strings.TrimSpace(bpsTestClientBody), "{"))
}

func TestOpenAIBasisPoints_WSHTTPBridgeTurnRoutesToBasisPoints(t *testing.T) {
	account := newBasisPointsTestAccount(9117)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream",
		bpsTestCompletedSSE(bpsTestMessageItem, bpsTestTransportCall("call_ws", `{"name":"exec_command","arguments":{"cmd":"ls"}}`)))}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream}
	payload := bpsTestWSPayload()
	var frames []string
	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "oauth-token", payload, len(payload),
		"gpt-6-astra", "", "", "", "", 1, func(msg []byte) error { frames = append(frames, string(msg)); return nil })
	require.NoError(t, err)
	require.NotNil(t, result)
	// WS 客户端被强制走 HTTP 桥，桥的每一轮先试 BPS：请求真的发到 bps.openai.com，工具目录不发。
	require.Len(t, upstream.requests, 1)
	require.Equal(t, openAIBasisPointsResponsesURL, upstream.lastReq.URL.String())
	require.Equal(t, "Bearer oauth-token", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, "chatgpt", upstream.lastReq.Header.Get("x-basispoints-auth-mode"))
	require.False(t, gjson.GetBytes(upstream.lastBody, "tools").Exists())
	require.Equal(t, "explicit", gjson.GetBytes(upstream.lastBody, "model_selection").String())
	joined := strings.Join(frames, "\n")
	require.Contains(t, joined, `"name":"exec_command"`)
	require.Contains(t, joined, `"call_id":"call_ws"`)
	require.Contains(t, joined, `"type":"response.completed"`)
	require.NotContains(t, joined, "run_officejs")
	require.Equal(t, openAIBasisPointsUpstreamEndpoint, result.UpstreamEndpoint)
	require.Equal(t, 20, result.Usage.InputTokens)
}

// WS 桥上没有 HTTP 状态码可用，所以终态错误是一条 response.failed 帧；同样不换号、不罚账号，
// 也同样不再转发 Codex 轮次 —— 否则同一个会话里前后两轮的回答质量会不一样。
func TestOpenAIBasisPoints_WSHTTPBridgeTurnFailsInsteadOfPassthrough(t *testing.T) {
	account := newBasisPointsTestAccount(9118)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusForbidden, "application/json", `{"error":{"code":"basispoints_model_access_changed","message":"no"}}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream}
	payload := bpsTestWSPayload()
	var frames []string
	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "oauth-token", payload, len(payload),
		"gpt-6-astra", "", "", "", "", 1, func(msg []byte) error { frames = append(frames, string(msg)); return nil })
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault)
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover))
	require.Len(t, upstream.requests, 1, "不该再转发 Codex 轮次")
	require.Equal(t, openAIBasisPointsResponsesURL, upstream.requests[0].URL.String())
	joined := strings.Join(frames, "\n")
	require.Contains(t, joined, `"type":"response.failed"`)
	require.Contains(t, joined, "basispoints_model_access_changed")
	require.Contains(t, joined, `"model":"gpt-6-astra"`)
	require.NotContains(t, joined, `"text":"done"`)
	require.Equal(t, StatusActive, account.Status)
}

// 首输出**之后**才到的 response.failed：这一半不经 begin 的 peek，走的是 WS 桥自己那个出口。
// 桥上的 lineage 调用刻意放在 `eventType == "error"` 判断**之外** —— response.failed 把码放在
// response.error.code，只在 error 分支里调就漏掉 BPS 上更常见的这一半。断言咬两件事：记进 bps:
// 命名空间、裸键（Codex 路径读的那把）保持空。少这条用例，把桥上那三行挪回 error 分支照旧全绿。
func TestOpenAIBasisPoints_WSHTTPBridgeRemembersInvalidEncryptedContent(t *testing.T) {
	account := newBasisPointsTestAccount(9141)
	payload := []byte(strings.Replace(string(bpsTestWSPayload()), `"input":[`, `"input":[
		{"type":"reasoning","id":"rs_cold","summary":[],"encrypted_content":"blob-minted-on-the-codex-route"},`, 1))
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
		`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
		`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"par"}`,
		`{"type":"response.failed","response":{"id":"resp_bps","status":"failed","error":{"code":"invalid_encrypted_content","message":"Request failed."}}}`,
	))}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream}
	// 失败帧已经原样转给客户端了（首输出之后 = 已通报），所以这一轮返回的是 nil error —— 这条用例
	// 咬的不是错误口径，而是「有没有记下来」。
	_, _ = svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "oauth-token", payload, len(payload),
		"gpt-6-astra", "", "", "", "", 3, func([]byte) error { return nil })

	sessionHash := svc.openAIWSLineageSessionHashFromContext(c, payload)
	require.NotEmpty(t, svc.sessionInvalidEncryptedContentDigests(
		getOpenAIGroupIDFromContext(c), openAIBasisPointsLineageSessionKey(sessionHash)),
		"BPS 自己那把键上要记着，下一轮才剥得掉")
	require.Empty(t, svc.sessionInvalidEncryptedContentDigests(getOpenAIGroupIDFromContext(c), sessionHash),
		"不许污染 Codex 路径读的裸键")
}

// 桥上那个 usage policy 停号出口：相邻两行里 lineage 那行有用例、停号那行没有（第十轮变异实测
// 删掉它整仓库照旧全绿）。这一格正是「封通道更常见的形态是 HTTP 200 + 流内失败帧」在 WS 上的落点，
// 而 WS 是长会话、首输出之后才到失败帧是常态。回归了就是被封通道的账号在 WS 上被无限捶 ——
// 按现场结论那正是封号放大路径。
func TestOpenAIBasisPoints_WSHTTPBridgeUsagePolicyBlockDisablesTheAccount(t *testing.T) {
	account := newBasisPointsTestAccount(9156)
	payload := bpsTestWSPayload()
	c, _ := newBasisPointsTestContext(t)
	repo := &bpsAccountRepoStub{}
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
		`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
		`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"par"}`,
		`{"type":"response.failed","response":{"id":"resp_bps","status":"failed","error":{"code":"usage_policy_violation","message":"This request was blocked by our usage policy. token=oauth-token"}}}`,
	))}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: upstream, accountRepo: repo}
	_, _ = svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "oauth-token", payload, len(payload),
		"gpt-6-astra", "", "", "", "", 2, func([]byte) error { return nil })

	require.Equal(t, 1, repo.setErrCalls, "桥上首输出之后的封通道信号也要停号")
	require.Contains(t, repo.lastErrorMsg, "Basis Points usage policy block")
	require.NotContains(t, repo.lastErrorMsg, "oauth-token", "落库的原因不许带 bearer token")
}

// **首输出之后的 response.failed 不许拖低这个满血账号的调度分。** 这条出口是
// `return resultWithUsage(), nil` —— nil error，所以 markOpenAIBasisPointsNotAccountFault
// 那套哨兵结构上挂不上去；而 AfterTurn 在 turnErr == nil 时会把 SucceededForScheduling()
// （对 response.failed 是 false）喂给 ReportOpenAIAccountScheduleResult ⇒ 罚分 ⇒ 调度器更倾向挑
// 没开开关的账号 = 掺杂。并发打出来的 429 与 usage-policy 403 恰好就是这个形态。第十三轮 blocker。
//
// 上面那两条桥用例都用 `_, _ =` 丢掉返回值，所以这一格从没被咬过。
func TestOpenAIBasisPoints_WSBridgeFailedTerminalIsScheduleNeutral(t *testing.T) {
	account := newBasisPointsTestAccount(9169)
	payload := bpsTestWSPayload()
	bridge := func(terminal string) *OpenAIForwardResult {
		c, _ := newBasisPointsTestContext(t)
		upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
			`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
			`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"hi"}`,
			terminal,
		))}}
		svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
			httpUpstream: upstream}
		result, err := svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "oauth-token", payload, len(payload),
			"gpt-6-astra", "", "", "", "", 2, func([]byte) error { return nil })
		require.NoError(t, err, "这条出口返回的是 nil error —— 哨兵挂不上去，才需要 ScheduleNeutral")
		require.NotNil(t, result)
		return result
	}

	failed := bridge(`{"type":"response.failed","response":{"id":"resp_bps","status":"failed","error":{"code":"rate_limit_exceeded","message":"slow down"},"usage":{"input_tokens":22516,"output_tokens":3,"total_tokens":22519}}}`)
	require.Equal(t, "response.failed", failed.UpstreamTerminalEvent)
	require.False(t, failed.SucceededForScheduling(), "前提：这个终态本来会被报成失败")
	require.True(t, failed.ScheduleNeutral, "失败终态必须中立，否则罚的是一个满血账号")

	ok := bridge(`{"type":"response.completed","response":{"id":"resp_bps","status":"completed","model":"gpt-6-astra","output":[],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}`)
	require.True(t, ok.SucceededForScheduling())
	require.False(t, ok.ScheduleNeutral, "成功轮次照常上报：要那个延迟样本，也要清模型级瞬时状态")
}

// 桥上的 TTFT / Duration 同样要从出站请求发出那刻算：turnStart 原来排在 beginOpenAIBasisPoints
// 之后，而 peek 已经把首输出事件读进缓冲 ⇒ firstTokenMs 恒 ~0、Duration 还漏掉 peek + prepare +
// 图片上传的全部耗时。HTTP 侧那条用例咬不到这一格（两条路各有一套计时）。
func TestOpenAIBasisPoints_WSBridgeTimingIncludesThePeekedWait(t *testing.T) {
	const stall = 300 * time.Millisecond
	account := newBasisPointsTestAccount(9171)
	payload := bpsTestWSPayload()
	c, _ := newBasisPointsTestContext(t)
	slow := bpsTestResponse(http.StatusOK, "text/event-stream", "")
	slow.Body = &bpsSlowFirstByteBody{delay: stall, reader: strings.NewReader(bpsTestSSE(
		`{"type":"response.created","response":{"id":"resp_bps","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
		`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"hi"}`,
		`{"type":"response.completed","response":{"id":"resp_bps","status":"completed","model":"gpt-6-astra","output":[],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}`,
	))}
	upstream := &httpUpstreamRecorder{responses: []*http.Response{slow}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: upstream}
	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "oauth-token", payload, len(payload),
		"gpt-6-astra", "", "", "", "", 1, func([]byte) error { return nil })
	require.NoError(t, err)
	require.NotNil(t, result)
	require.GreaterOrEqual(t, result.Duration, stall, "Duration 不能漏掉 peek 那段")
	require.NotNil(t, result.FirstTokenMs)
	require.GreaterOrEqual(t, *result.FirstTokenMs, int(stall.Milliseconds()), "TTFT 不能记成 ~0")
}

// ScheduleNeutral 的执行点。判断刻意收在 ReportOpenAIForwardScheduleResult 里而不是调用方的 if：
// 那样每个调用点都得记得查一次，而漏查的后果是罚一个满血 BPS 账号的分 = 掺杂。两个调用点（HTTP 与
// WS AfterTurn）都走它，所以这一条覆盖到执行点本身。
func TestReportOpenAIForwardScheduleResultSkipsNeutral(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	account := newBasisPointsTestAccount(9172)
	require.False(t, svc.ReportOpenAIForwardScheduleResult(account, "gpt-6-astra",
		&OpenAIForwardResult{OpenAIWSMode: true, UpstreamTerminalEvent: "response.failed", ScheduleNeutral: true}),
		"中立的结果不许上报")
	require.True(t, svc.ReportOpenAIForwardScheduleResult(account, "gpt-6-astra",
		&OpenAIForwardResult{OpenAIWSMode: true, UpstreamTerminalEvent: "response.failed"}),
		"非中立的失败照旧上报（那是 Codex 路径的常规行为）")
	require.True(t, svc.ReportOpenAIForwardScheduleResult(account, "gpt-6-astra", nil),
		"result 为 nil 也要上报：HTTP 侧那条分支原来就是这么走的")
}

// **TTFT 要把 peek 吃掉的那段算进去。** peek 一直读到「第一个会到客户端的事件」为止，所以流处理器
// 起跑时那条事件已经在缓冲里 —— 以 acceptedAt 为原点量出来的 firstTokenMs 恒等于 ~0。它有三个消费者
// （usage_logs.first_token_ms、ops 的 TTFT 读数、scheduler.ReportResult 的延迟分），等于 BPS 账号在
// 延迟维度上永远满分、真慢的号永远不被降权，而面板上 0 ms 看着还很好 —— 完全静默。第十三轮。
func TestOpenAIBasisPoints_FirstTokenMsIncludesThePeekedWait(t *testing.T) {
	const stall = 300 * time.Millisecond
	account := newBasisPointsTestAccount(9170)
	c, _ := newBasisPointsTestContext(t)
	slow := bpsTestResponse(http.StatusOK, "text/event-stream", "")
	slow.Body = &bpsSlowFirstByteBody{delay: stall, reader: strings.NewReader(bpsTestCompletedSSE(bpsTestMessageItem))}
	upstream := &httpUpstreamRecorder{responses: []*http.Response{slow}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(
		context.Background(), c, account, []byte(bpsTestClientBody))
	require.NoError(t, err)
	require.NotNil(t, result.FirstTokenMs)
	require.GreaterOrEqual(t, *result.FirstTokenMs, int(stall.Milliseconds()),
		"客户端真等了 %v，TTFT 不能记成 ~0", stall)
	require.GreaterOrEqual(t, result.Duration, stall, "对照：Duration 本来就是对的")
}

func TestOpenAIBasisPointsFirstTokenMsOffset(t *testing.T) {
	require.Nil(t, offsetOpenAIBasisPointsFirstTokenMs(nil, time.Second), "没量到就是没量到")
	base := 7
	require.Equal(t, 7, *offsetOpenAIBasisPointsFirstTokenMs(&base, 0), "没等就不补")
	require.Equal(t, 307, *offsetOpenAIBasisPointsFirstTokenMs(&base, 300*time.Millisecond))
	require.Equal(t, 7, base, "不许改调用方那个指针指向的值")
}

func TestOpenAIBasisPoints_DataImageIsUploadedOnceAndReferencedByFileID(t *testing.T) {
	openAIBasisPointsImages = newBPSLRU(openAIBasisPointsImageCacheCap, 0) // 进程级缓存，-count=2 也要从空开始
	account := newBasisPointsTestAccount(9108)
	png := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3}
	dataURL := "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
	body := strings.Replace(bpsTestClientBody, `"content":[{"type":"input_text","text":"hi"}]`,
		`"content":[{"type":"input_text","text":"hi"},{"type":"input_image","image_url":"`+dataURL+`","detail":"original"}]`, 1)

	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "application/json", `{"openai_file_id":"file_abc"}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.Len(t, upstream.requests, 2)
	uploadReq := upstream.requests[0]
	require.Equal(t, openAIBasisPointsAttachmentsURL, uploadReq.URL.String())
	require.True(t, strings.HasPrefix(uploadReq.Header.Get("Content-Type"), "multipart/form-data; boundary="))
	require.Equal(t, "application/json", uploadReq.Header.Get("Accept"))
	require.Equal(t, "Bearer oauth-token", uploadReq.Header.Get("Authorization"))
	require.Equal(t, "chatgpt-acc", uploadReq.Header.Get("chatgpt-account-id"))
	upload := string(upstream.bodies[0])
	require.Contains(t, upload, `name="file"; filename="picture-`)
	require.Contains(t, upload, ".png\"")
	require.Contains(t, upload, "Content-Type: image/png")
	require.True(t, bytes.Contains(upstream.bodies[0], png))

	part := gjson.GetBytes(upstream.lastBody, "input.2.content.1")
	require.Equal(t, "input_image", part.Get("type").String())
	require.Equal(t, "file_abc", part.Get("file_id").String())
	require.False(t, part.Get("image_url").Exists())
	// 带 file_id 就只发 {type, file_id}：多一个 detail 上游稳定 422（同图同请求 A/B 对照，
	// JaxsonWang/cpa-plugin-oai-basispoints#17）。客户端给的 "original" 也不许透过来。
	require.False(t, part.Get("detail").Exists(), "file_id 形态多一个 detail 就是 422")
	keys, ok := part.Value().(map[string]any)
	require.True(t, ok)
	require.Len(t, keys, 2, "只许 type 与 file_id 两个键")
	require.NotContains(t, string(upstream.lastBody), "base64")

	// 同一张图第二次不再上传。
	c2, _ := newBasisPointsTestContext(t)
	again := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
	_, err = (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: again}).Forward(context.Background(), c2, account, []byte(body))
	require.NoError(t, err)
	require.Len(t, again.requests, 1)
	require.Equal(t, openAIBasisPointsResponsesURL, again.lastReq.URL.String())
	require.Equal(t, "file_abc", gjson.GetBytes(again.lastBody, "input.2.content.1.file_id").String())
}

func TestOpenAIBasisPoints_ImageUploadFailureIsAHardError(t *testing.T) {
	account := newBasisPointsTestAccount(9109)
	// 真 JPEG 魔数：类型按字节识别，假字节会在 decode 阶段就被判死，测不到上传失败这条路。
	dataURL := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(
		append([]byte{0xff, 0xd8, 0xff, 0xe0}, []byte("unique-9109")...))
	body := strings.Replace(bpsTestClientBody, `"content":[{"type":"input_text","text":"hi"}]`,
		`"content":[{"type":"input_text","text":"hi"},{"type":"input_image","image_url":"`+dataURL+`"}]`, 1)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusForbidden, "application/json", `{"detail":"no"}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.Nil(t, result)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault)
	require.Contains(t, err.Error(), "image_upload")
	require.Len(t, upstream.requests, 1, "只有那次失败的附件上传，不该再发 Codex")
	require.Equal(t, openAIBasisPointsAttachmentsURL, upstream.requests[0].URL.String())
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "basispoints_image_upload")
	require.NotContains(t, rec.Body.String(), dataURL, "图片不回显给客户端")
}

// 附件上传排在主 /responses **之前**：带内联图片的请求上代理死亡只会经过 uploader 那个出口。
// 三条断言逐条咬那三件事 —— reason 是专用的 image_upload_transport（→ 罚分，不是形态类豁免）、
// 摘池、记 ops 事件。少了最后一条，账号被摘池 10 分钟而 ops 里查不到任何上游错误。
func TestOpenAIBasisPoints_ImageUploadTransportErrorIsAnAccountFault(t *testing.T) {
	account := newBasisPointsTestAccount(9139)
	dataURL := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(
		append([]byte{0xff, 0xd8, 0xff, 0xe0}, []byte("unique-9139")...))
	body := strings.Replace(bpsTestClientBody, `"content":[{"type":"input_text","text":"hi"}]`,
		`"content":[{"type":"input_text","text":"hi"},{"type":"input_image","image_url":"`+dataURL+`"}]`, 1)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{err: errors.New("proxyconnect tcp: dial tcp 10.0.0.1:1080: connect: connection refused")}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	result, err := svc.Forward(context.Background(), c, account, []byte(body))
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "image_upload_transport")
	require.NotErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "代理死了要罚这个账号的调度分")
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account), "持久传输错误要把账号临时摘出池子")
	raw, ok := c.Get(OpsUpstreamErrorsKey)
	require.True(t, ok, "摘池了就必须在 ops 里留下上游错误")
	events, _ := raw.([]*OpsUpstreamErrorEvent)
	require.Len(t, events, 1)
	require.Equal(t, account.ID, events[0].AccountID)
	require.Contains(t, events[0].Message, "basispoints:")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.NotContains(t, rec.Body.String(), dataURL, "图片不回显给客户端")
}

// 反面：**客户端自己断开**不算账号侧故障。uploader 跑在 bridge.prepare 里、用的是客户端 ctx
// （早于主请求那次 detachUpstreamContext），所以这条路上 isClientCanceledTransportError 是真守卫。
//
// 它真正拦住的是 **reason**：删掉那道守卫，每一次「用户按 Ctrl-C」都会把 reason 从形态类的
// image_upload 变成账号类的 image_upload_transport（进 ReasonIsAccountFault → 罚这个账号的调度分）。
// 摘池那一半不受影响 —— context.Canceled 不在 classifyUpstreamTransportError 的持久标记里。
func TestOpenAIBasisPoints_ClientCanceledImageUploadIsNotAnAccountFault(t *testing.T) {
	account := newBasisPointsTestAccount(9151)
	dataURL := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(
		append([]byte{0xff, 0xd8, 0xff, 0xe0}, []byte("unique-9151")...))
	body := strings.Replace(bpsTestClientBody, `"content":[{"type":"input_text","text":"hi"}]`,
		`"content":[{"type":"input_text","text":"hi"},{"type":"input_image","image_url":"`+dataURL+`"}]`, 1)
	c, _ := newBasisPointsTestContext(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// 判据要求**两边都成立**：err 包着 context.Canceled，且 ctx 也确实被取消了。
	upstream := &httpUpstreamRecorder{err: fmt.Errorf("Post \"https://bps.openai.com/...\": %w", context.Canceled)}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	_, err := svc.Forward(ctx, c, account, []byte(body))
	require.Error(t, err)
	require.Contains(t, err.Error(), "image_upload", "仍然是硬报错")
	require.NotContains(t, err.Error(), "image_upload_transport", "但客户端断开不算账号侧故障")
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "不许罚这个账号的调度分")
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
	_, recorded := c.Get(OpsUpstreamErrorsKey)
	require.False(t, recorded, "也不该记一条假的上游错误：客户端自己走了不是上游的错")
}

// 第三种：上传**超时**（非持久）。与 upstream_silent / stream_eof 同一条理由 —— 那更可能是 BPS 通道
// 自己超时，罚分会把流量推给没开开关的账号 = 掺杂，正好违背这个功能的口径。同一根因两种处置本来就是
// 约定文档想消掉的那种不一致，这里方向还是反的（主请求那条豁免、上传这条罚）。
func TestOpenAIBasisPoints_ImageUploadTimeoutIsNotAnAccountFault(t *testing.T) {
	account := newBasisPointsTestAccount(9166)
	dataURL := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(
		append([]byte{0xff, 0xd8, 0xff, 0xe0}, []byte("unique-9166")...))
	body := strings.Replace(bpsTestClientBody, `"content":[{"type":"input_text","text":"hi"}]`,
		`"content":[{"type":"input_text","text":"hi"},{"type":"input_image","image_url":"`+dataURL+`"}]`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{err: errors.New("Post \"https://bps.openai.com/...\": context deadline exceeded")}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	_, err := svc.Forward(context.Background(), c, account, []byte(body))
	require.Error(t, err)
	require.Contains(t, err.Error(), "image_upload_timeout", "仍然是硬报错，但换一个不罚分的 reason")
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "超时不许罚这个账号的调度分")
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account), "非持久错误不摘池")
	// 但**要**记 ops：与主请求那两条豁免出口同口径，超时是真的上游读数。
	_, recorded := c.Get(OpsUpstreamErrorsKey)
	require.True(t, recorded, "超时是真的上游错误，要留在 ops 里")
}

// bpsTestImageBytes 给出四种受支持格式的最小合法字节（够 http.DetectContentType 认出来）。
func bpsTestImageBytes(kind string) []byte {
	switch kind {
	case "png":
		return []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3}
	case "jpeg":
		return []byte{0xff, 0xd8, 0xff, 0xe0, 0, 16, 'J', 'F', 'I', 'F'}
	case "gif":
		return []byte("GIF89a\x01\x00\x01\x00")
	case "webp":
		return []byte("RIFF\x1a\x00\x00\x00WEBPVP8 ")
	}
	return nil
}

// **类型按字节定，客户端声明的那个串只用来先筛掉「压根不是图」。**
//
// BPS 只接受 .jpeg/.jpg/.png/.gif/.webp，别的格式或者扩展名对不上字节（.jfif、无后缀）会让整单
// 400 `Expected image type to be a supported format … but got none`，而历史里一旦带上这张图，
// 这个会话每一轮回放都失败（JaxsonWang/cpa-plugin-oai-basispoints#15）。默认成 png 的代价一样，
// 而且客户端拿不到任何定位信息 —— 所以识别不出就判死。
func TestOpenAIBasisPointsDataURLTypeComesFromBytesNotTheDeclaredString(t *testing.T) {
	for _, kind := range []string{"png", "jpeg", "gif", "webp"} {
		raw := bpsTestImageBytes(kind)
		// 声明成别的类型（甚至声明成另一种受支持格式）也按字节纠正。
		for _, declared := range []string{"image/" + kind, "image/bmp", "image/png", "image/x-whatever"} {
			got, data, ok := decodeOpenAIBasisPointsDataURL(
				"data:" + declared + ";base64," + base64.StdEncoding.EncodeToString(raw))
			require.True(t, ok, "%s declared as %s", kind, declared)
			require.Equal(t, "image/"+kind, got, "类型必须按字节定，不能跟着 %s 走", declared)
			require.Equal(t, raw, data)
		}
	}
	// 真的不是受支持格式（这里是 BMP 字节）：判死，不默认 png。
	bmp := append([]byte("BM"), make([]byte, 40)...)
	_, _, ok := decodeOpenAIBasisPointsDataURL("data:image/bmp;base64," + base64.StdEncoding.EncodeToString(bmp))
	require.False(t, ok, "BPS 不收 bmp，猜成 png 只会换来整单 400")
	// 完全不是图片的字节，哪怕声明成 image/png 也拒。
	_, _, ok = decodeOpenAIBasisPointsDataURL("data:image/png;base64,aGVsbG8=")
	require.False(t, ok, "声明 image/png 但字节是 \"hello\"")
	// 非 image/* 的声明在解码之前就筛掉（省一次 base64 分配）。
	for _, declared := range []string{"application/x-msdownload", "text/html", ""} {
		_, _, ok := decodeOpenAIBasisPointsDataURL(
			"data:" + declared + ";base64," + base64.StdEncoding.EncodeToString(bpsTestImageBytes("png")))
		require.False(t, ok, declared)
	}
}

// 工具结果里的图保持内联（Excel 加载项自己就是内嵌发的），但**必须过同一道解码校验**。
// 原来这条用例刻意用非图片字节 "view-image-9110" 把「内联那条路什么都不校验」钉成了规范，
// 于是按字节识别 / image/* 声明 / 20 MB 三道闸门在这条路上一条都不生效。
// Codex 多智能体 v2 历史里的 encrypted_content 部件：**替换成一句明文说明，保留位置**，不再判死。
// 用户 2026-09-29 拍板（原来是永久硬报错，且失败在出站前、lineage 自愈压根不触发）。
// 三条断言：这一发真的到了上游（不是硬报错）、说明在原来那个位置、密文一个字节都不出站。
func TestOpenAIBasisPoints_EncryptedContentPartBecomesAPlainNotice(t *testing.T) {
	account := newBasisPointsTestAccount(9142)
	body := strings.Replace(bpsTestClientBody,
		`"content":[{"type":"input_text","text":"hi"}]`,
		`"content":[{"type":"input_text","text":"before"},{"type":"encrypted_content","encrypted_content":"OPAQUE-BLOB"},{"type":"input_text","text":"after"}]`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.Equal(t, openAIBasisPointsResponsesURL, upstream.lastReq.URL.String())

	// 按 role 取而不是按下标：本层在前面插了 instructions 降级和工具目录两条 developer 消息。
	parts := gjson.GetBytes(upstream.lastBody, `input.#(role=="user").content`).Array()
	require.Len(t, parts, 3, "部件位置保留")
	require.Equal(t, "before", parts[0].Get("text").String())
	require.Equal(t, "input_text", parts[1].Get("type").String(), "user 消息用 input_text")
	require.Equal(t, openAIBasisPointsEncryptedContentNotice, parts[1].Get("text").String())
	require.Equal(t, "after", parts[2].Get("text").String())
	require.NotContains(t, string(upstream.lastBody), "OPAQUE-BLOB", "密文一个字节都不出站")
}

// assistant 消息里的同一个部件要用 output_text：input_text 和 output_text 本层都在原样转发，
// 选错了等于自己造一个上游没见过的形态，而出站体是严格白名单。
func TestOpenAIBasisPoints_EncryptedContentNoticeUsesOutputTextForAssistant(t *testing.T) {
	account := newBasisPointsTestAccount(9143)
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"message","role":"assistant","content":[{"type":"encrypted_content","encrypted_content":"OPAQUE-BLOB"}]},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	part := gjson.GetBytes(upstream.lastBody, `input.#(role=="assistant").content.0`)
	require.Equal(t, "output_text", part.Get("type").String())
	require.Equal(t, openAIBasisPointsEncryptedContentNotice, part.Get("text").String())
}

// 事前闸门只扫 content / output 真会出站的那几类项。reasoning 项是整项重建（content / summary
// 一概丢弃），闸门却照着部件白名单扫它 —— 一个带 content:[{type:"reasoning_text",…}] 的 reasoning
// 项（本仓库 apicompat 的 fixture 就是这个形状）会被判成 input_content 硬 502，而失败在出站之前
// ⇒ lineage 自愈不触发 ⇒ 这个会话每轮都 502、永不自愈。同一类的第三个实例。
//
// 两条断言：这一发真的出站了（不是硬报错），且 reasoning_text 一个字节都没跟着出去。
func TestOpenAIBasisPoints_ReasoningItemContentDoesNotTripTheGate(t *testing.T) {
	account := newBasisPointsTestAccount(9153)
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"s"}],
		 "content":[{"type":"reasoning_text","text":"PLAN-TEXT"}],"encrypted_content":"BLOB"},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.Len(t, upstream.requests, 1)
	item := gjson.GetBytes(upstream.lastBody, `input.#(type=="reasoning")`)
	require.Equal(t, "BLOB", item.Get("encrypted_content").String())
	require.NotContains(t, string(upstream.lastBody), "PLAN-TEXT", "content 本来就不出站")
	require.NotContains(t, string(upstream.lastBody), "reasoning_text")
}

// 压缩回合改走 BPS 之后，上游产出的 compaction 项会被客户端**每一轮回放**。原来它落到
// translateHistory 的 default、判死成 input_content ⇒ 会话从压缩那一刻起每轮 502 且无自愈路径。
// 2026-09-29 直连实测：BPS 收得回去而且密文真被解开 ⇒ 透传。
//
// 两条断言各咬一半：密文原样出站（少了它换成「丢弃」也绿）、字段是白名单（少了它 item 级未知字段
// 会跟着出站，而上游对未知字段是整单 400）。
func TestOpenAIBasisPoints_CompactionHistoryItemIsRelayed(t *testing.T) {
	account := newBasisPointsTestAccount(9148)
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"compaction","id":"cmp_abc","encrypted_content":"COMPACTED-BLOB","zzz_unknown":"LEAKED"},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	item := gjson.GetBytes(upstream.lastBody, `input.#(type=="compaction")`)
	require.True(t, item.Exists(), "compaction 项要发出去，不是判死也不是丢弃")
	require.Equal(t, "COMPACTED-BLOB", item.Get("encrypted_content").String())
	require.Equal(t, "cmp_abc", item.Get("id").String())
	require.ElementsMatch(t, []string{"type", "id", "encrypted_content"}, bpsTestJSONKeys(item),
		"字段按上游自己产出的那三个白名单，其余丢掉")
}

// compaction_summary 是 Codex 侧的同胞类型，BPS 没见过 —— 出站类型归一成它自己产的 compaction
// （出站体的规矩是「不赌」）。里面的密文是 Codex 铸的、BPS 解不开，会被拒 → lineage 记下 →
// 下一轮整项删除，一次失败后自愈。
func TestOpenAIBasisPoints_CompactionSummaryGoesOutAsCompaction(t *testing.T) {
	account := newBasisPointsTestAccount(9149)
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"compaction_summary","id":"cmp_legacy","encrypted_content":"CODEX-MINTED"},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(upstream.lastBody, `input.#(type=="compaction_summary")`).Exists())
	require.Equal(t, "CODEX-MINTED",
		gjson.GetBytes(upstream.lastBody, `input.#(type=="compaction").encrypted_content`).String())
}

// 没有密文的 compaction 项整项丢掉（与 reasoning 同口径：store:false 下上游拒收空壳）。
func TestOpenAIBasisPoints_CompactionWithoutBlobIsDropped(t *testing.T) {
	account := newBasisPointsTestAccount(9150)
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[{"type":"compaction","id":"cmp_empty"},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.NotContains(t, string(upstream.lastBody), "cmp_empty")
}

// item 级字段也是白名单。上游对未知 item 字段是整单 400（实测
// "Unknown parameter: 'input[1].zzz_unknown_field'"），而客户端每轮回放同一批历史 ⇒ 一个多出来的键
// 就是「这个会话从此每轮都失败」。原来只闸门 item **类型**，其余键原样出站 —— 顺带把 message 项上
// 的 item 级 encrypted_content 也带出去了（第九轮审查的探针就是这么抓到的）。
//
// `name` / `namespace` 在 function_call_output 上**必须留着**：实测它们是该 item 的合法字段。
func TestOpenAIBasisPoints_ItemLevelUnknownFieldsAreDropped(t *testing.T) {
	account := newBasisPointsTestAccount(9152)
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"message","role":"user","content":[{"type":"input_text","text":"a"}],
		 "zzz_unknown_item_field":"LEAKED","encrypted_content":"ITEM-LEVEL-BLOB"},
		{"type":"function_call","call_id":"call_z","name":"exec_command","arguments":"{\"cmd\":\"ls\"}"},
		{"type":"function_call_output","call_id":"call_z","output":"ok","name":"exec_command",
		 "namespace":"","zzz_unknown_output_field":"LEAKED2"},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	out := string(upstream.lastBody)
	require.NotContains(t, out, "zzz_unknown_item_field")
	require.NotContains(t, out, "LEAKED")
	require.NotContains(t, out, "ITEM-LEVEL-BLOB", "item 级密文也不许出站")
	require.NotContains(t, out, "zzz_unknown_output_field")
	require.NotContains(t, out, "LEAKED2")
	// 合法字段一个都不许被顺手删掉。
	outputItem := gjson.GetBytes(upstream.lastBody, `input.#(type=="function_call_output")`)
	require.Equal(t, "call_z", outputItem.Get("call_id").String())
	require.Equal(t, "ok", outputItem.Get("output").String())
	require.Equal(t, "exec_command", outputItem.Get("name").String())
	require.True(t, outputItem.Get("namespace").Exists(), "namespace 是合法字段，实测带着也 200")
	require.Equal(t, "a", gjson.GetBytes(upstream.lastBody, `input.#(role=="user").content.0.text`).String())
}

// 部件级字段也是白名单。`output_text` 天生带 annotations（新版还带 logprobs），把
// response.output 的 assistant 消息原样回放进下一轮 input 是 Responses API 官方的多轮写法；
// 顶层 messages 那条 legacy 入站路径还会产出带 prompt_cache_breakpoint 的 input_text。
// 实测依据：`input_image` 上多一个**合法** Responses 字段（detail）就稳定 422 —— 同一个校验器。
// 顺带咬住 `"text"` 归一（它不是 Responses 的 input 部件类型）。
func TestOpenAIBasisPoints_ContentPartUnknownFieldsAreDropped(t *testing.T) {
	account := newBasisPointsTestAccount(9154)
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"message","role":"assistant","content":[
			{"type":"output_text","text":"prior","annotations":[],"logprobs":[]},
			{"type":"refusal","refusal":"nope","zzz_part":"LEAKED"}]},
		{"type":"message","role":"user","content":[
			{"type":"text","text":"legacy","prompt_cache_breakpoint":true}]},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	out := string(upstream.lastBody)
	require.NotContains(t, out, "annotations")
	require.NotContains(t, out, "logprobs")
	require.NotContains(t, out, "prompt_cache_breakpoint")
	require.NotContains(t, out, "zzz_part")
	require.NotContains(t, out, "LEAKED")

	assistant := gjson.GetBytes(upstream.lastBody, `input.#(role=="assistant").content`).Array()
	require.Len(t, assistant, 2)
	require.ElementsMatch(t, []string{"type", "text"}, bpsTestJSONKeys(assistant[0]))
	require.Equal(t, "output_text", assistant[0].Get("type").String())
	require.Equal(t, "prior", assistant[0].Get("text").String())
	require.ElementsMatch(t, []string{"type", "refusal"}, bpsTestJSONKeys(assistant[1]))
	require.Equal(t, "nope", assistant[1].Get("refusal").String(), "refusal 的文本不在 text 字段上")

	legacy := gjson.GetBytes(upstream.lastBody, `input.#(role=="user").content.0`)
	require.Equal(t, "input_text", legacy.Get("type").String(), `"text" 归一成 input_text`)
	require.Equal(t, "legacy", legacy.Get("text").String())
}

// 工具结果里的文本部件出 input_text：那个位置是**给模型的输入**，官方 schema 的联合类型是
// input_text|input_image|input_file。6fa7dfbfc 把 textKind 从「只给 encrypted_content 那句明文
// 说明用」扩到全部文本部件时，顺带把这里原样转发的 input_text 改成了 output_text —— 参数复用的意外
// 副作用，而这个文件的口径是「换掉类型本身就是赌」（多一个合法字段都稳定 422）。
func TestOpenAIBasisPoints_ToolOutputTextStaysAnInputPart(t *testing.T) {
	account := newBasisPointsTestAccount(9164)
	body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"function_call","call_id":"call_txt","name":"exec_command","arguments":"{\"cmd\":\"ls\"}"},
		{"type":"function_call_output","call_id":"call_txt","output":[{"type":"input_text","text":"TOOL-TEXT"}]},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	part := gjson.GetBytes(upstream.lastBody, `input.#(type=="function_call_output").output.0`)
	require.Equal(t, "input_text", part.Get("type").String())
	require.Equal(t, "TOOL-TEXT", part.Get("text").String())
}

// 正文不是字符串时**不许静默变空串**（bpsText 对非字符串返回 ""，模型会看到一个空文本部件、
// 正文一个字节都不出站、客户端零信号）。2026-09-30 定稿成三分行为：
//
//	能取到正文  → 发正文（含 Assistants v2 的嵌套 {"value":…} 与数字/布尔标量）
//	本来没正文  → 空文本（缺字段 / 显式 null，Jackson / Newtonsoft 的默认序列化很常见）
//	取不到      → 占位部件（数组、没有 value 的对象）
//
// 中间那一格尤其要留着：`"text":null` 在回放历史里 ⇒ 判死或占位都是每轮误伤。
func TestOpenAIBasisPoints_TextPartSalvageBeforePlaceholder(t *testing.T) {
	account := newBasisPointsTestAccount(9165)

	// (1) 能救的：正文必须原样出站，**不许**变成占位符。
	//
	// v2 那条是 blocker 级的：`{"type":"text","text":{"value":…}}` 是 Assistants API v2 客户端
	// **全部**文本部件的规范形状，换成占位符 ⇒ 用户整句提问一个字节都不出站，而请求 HTTP 200、
	// 模型盲答、照常计费，客户端完全看不出来。比原来的 502 更糟：报错可见，盲答不可见。
	for _, tc := range []struct {
		shape string
		want  string
		path  string
	}{
		{`{"type":"text","text":{"value":"SALVAGE-NESTED","annotations":[]}}`, "SALVAGE-NESTED", "text"},
		{`{"type":"input_text","text":123}`, "123", "text"},
		{`{"type":"input_text","text":true}`, "true", "text"},
		{`{"type":"refusal","refusal":{"value":"SALVAGE-REFUSAL"}}`, "SALVAGE-REFUSAL", "refusal"},
	} {
		t.Run("救回 "+tc.shape, func(t *testing.T) {
			body := strings.Replace(bpsTestClientBody, `"input":[`,
				`"input":[{"type":"message","role":"user","content":[`+tc.shape+`]},`, 1)
			c, _ := newBasisPointsTestContext(t)
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
			}}
			_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
			require.NoError(t, err)
			require.Len(t, upstream.requests, 1)
			part := gjson.GetBytes(upstream.lastBody, "input.2.content.0")
			require.Equal(t, tc.want, part.Get(tc.path).String(), "正文必须原样出站，不许变占位符")
			require.NotContains(t, part.Get(tc.path).String(), "omitted")
			require.NotContains(t, string(upstream.lastBody), "annotations", "只留 {type, text}")
		})
	}

	// (2) 真救不回来的：占位部件，且原值不出站。
	body := strings.Replace(bpsTestClientBody, `"input":[`,
		`"input":[{"type":"message","role":"user","content":[`+
			`{"type":"input_text","text":["ARRAY-MUST-NOT-LEAK"]},`+
			`{"type":"input_text","text":"REAL QUESTION"}]},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.Contains(t, gjson.GetBytes(upstream.lastBody, "input.2.content.0.text").String(), "text_not_a_string")
	require.Equal(t, "REAL QUESTION", gjson.GetBytes(upstream.lastBody, "input.2.content.1.text").String(),
		"同一条消息里活着的正文不受影响")
	require.NotContains(t, string(upstream.lastBody), "ARRAY-MUST-NOT-LEAK")

	// (3) 缺字段与显式 null：出空文本，不判死也不占位。
	for _, shape := range []string{
		`{"type":"input_text"}`,
		`{"type":"input_text","text":null}`,
		`{"type":"refusal","refusal":null}`,
	} {
		t.Run("放过 "+shape, func(t *testing.T) {
			body := strings.Replace(bpsTestClientBody, `"input":[`,
				`"input":[{"type":"message","role":"user","content":[`+shape+`]},`, 1)
			c, _ := newBasisPointsTestContext(t)
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
			}}
			_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
			require.NoError(t, err)
			// 断言那一个部件本身，别断言整个 body —— 协议序言里本来就有 "declarations were omitted"。
			part := gjson.GetBytes(upstream.lastBody, "input.2.content.0")
			require.Empty(t, part.Get("text").String()+part.Get("refusal").String(),
				"没正文可丢就出空文本，不写占位符")
		})
	}
}

// **占位符不许把整条请求的正文吃光。** 全部文本部件都占位 ⇒ 出站是一份「什么都被省略了」的
// 上下文 ⇒ 上游 200 + 模型盲答 + 照常计费，客户端零信号。那比 502 更糟，所以这一格判死。
// 判据收窄成「丢过正文**且**一个都没活下来」，纯图片请求（0 丢 0 活）不受影响。
func TestOpenAIBasisPoints_AllTextDroppedIsAHardErrorNotABlindAnswer(t *testing.T) {
	account := newBasisPointsTestAccount(9182)
	// 客户端自己的 instructions 会变成一条 developer 消息，但那不是用户的话 —— 兜底只看
	// input 里的文本部件，所以这一发必须判死。
	body := strings.Replace(bpsTestClientBody, `"input":[`,
		`"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":[1,2]}]},`, 1)
	body = strings.Replace(body,
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}],"internal_chat_message_metadata_passthrough":{"turn_id":"t1"}}`,
		`{"type":"message","role":"user","content":[{"type":"input_text","text":{"nope":1}}]}`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.Error(t, err, "正文被吃光必须是可见失败，不能 200 盲答")
	require.Contains(t, err.Error(), "input_content")
	require.Empty(t, upstream.requests, "判死在发请求之前")
}

// bpsTestJSONKeys 列出一个 JSON 对象的键名。
func bpsTestJSONKeys(value gjson.Result) []string {
	var keys []string
	value.ForEach(func(key, _ gjson.Result) bool {
		keys = append(keys, key.String())
		return true
	})
	return keys
}

func TestOpenAIBasisPoints_ToolOutputImageStaysInline(t *testing.T) {
	account := newBasisPointsTestAccount(9110)
	inline := func(url string) string {
		return strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"function_call","call_id":"call_img","name":"exec_command","arguments":"{\"cmd\":\"view\"}"},
		{"type":"function_call_output","call_id":"call_img","output":[{"type":"input_image","image_url":"`+url+`"}]},`, 1)
	}
	dataURL := "data:image/png;base64," + base64.StdEncoding.EncodeToString(bpsTestImageBytes("png"))
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(inline(dataURL)))
	require.NoError(t, err)
	require.Len(t, upstream.requests, 1, "工具结果里的图不上传")
	require.Equal(t, openAIBasisPointsResponsesURL, upstream.lastReq.URL.String())
	part := gjson.GetBytes(upstream.lastBody, "input.3.output.0")
	require.Equal(t, dataURL, part.Get("image_url").String())
	require.Equal(t, "auto", part.Get("detail").String())

	// 客户端给的形态本身递送不出去的两种：非图片字节（声明 image/png 实际是 "hello"）与超上限。
	// 2026-09-30 起换成占位部件而不是判死 —— 它们在回放历史里每轮都在，判死等于会话永久失败。
	// 原字节仍然一个都不许出站（否则 21 MB 会原样发到 bps.openai.com）。
	huge := "data:image/png;base64," + base64.StdEncoding.EncodeToString(
		append(bpsTestImageBytes("png"), make([]byte, openAIBasisPointsMaxImageBytes+1)...))
	for name, url := range map[string]string{
		"非图片字节": "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte("hello")),
		"超上限":   huge,
	} {
		t.Run(name, func(t *testing.T) {
			cx, _ := newBasisPointsTestContext(t)
			rec := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem))}}
			_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: rec}).Forward(context.Background(), cx, account, []byte(inline(url)))
			require.NoError(t, err, "不再判死")
			require.Len(t, rec.requests, 1, "只发 /responses，不上传")
			require.Equal(t, openAIBasisPointsResponsesURL, rec.lastReq.URL.String())
			part := gjson.GetBytes(rec.lastBody, "input.3.output.0")
			require.Equal(t, "input_text", part.Get("type").String())
			require.Equal(t, openAIBasisPointsDroppedImageNotice, part.Get("text").String())
			require.False(t, part.Get("image_url").Exists())
			require.NotContains(t, string(rec.lastBody), url, "原始 data URL 一个字节都不出站")
		})
	}
}

func TestOpenAIBasisPoints_NonStreamingClientGetsTranslatedJSON(t *testing.T) {
	account := newBasisPointsTestAccount(9111)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream",
		bpsTestCompletedSSE(bpsTestTransportCall("call_ns", `{"name":"exec_command","arguments":{"cmd":"id"}}`)))}}
	body := strings.Replace(bpsTestClientBody, `"stream":true`, `"stream":false`, 1)
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	require.True(t, gjson.GetBytes(upstream.lastBody, "stream").Bool(), "上游一律流式")
	require.False(t, result.Stream)
	require.Equal(t, http.StatusOK, rec.Code)
	out := rec.Body.String()
	require.True(t, gjson.Valid(out), out)
	require.Equal(t, "exec_command", gjson.Get(out, "output.0.name").String())
	require.Equal(t, `{"cmd":"id"}`, gjson.Get(out, "output.0.arguments").String())
	require.NotContains(t, out, "run_officejs")
	require.Equal(t, 20, result.Usage.InputTokens)
}

// 同一条信号的另一半形态：BPS 把拒绝放在 HTTP 200 的 response.failed 帧里，错误码在
// **response.error.code**。extractUpstreamErrorCode 只读 error.code 与嵌在 error.message 里的信封，
// 读不到这条；openAIBasisPointsErrorDetail 读得到。两个取码器交叉、谁都不包含谁，所以判据必须取
// 并集 —— 只用其中一个，对应那半边就是零覆盖（每轮硬报错 + 每轮白发 22.5K 提示词，且不打日志）。
//
// 消息刻意写成一句**不含**判据关键词的中性文案：否则消息兜底会替 code 路径兜住，这条用例就测不出
// 取码器的差别了（第一版就是这么写的，把 code 那半改坏照样绿）。消息兜底只在**三个取码器全空**时
// 才生效，见紧挨着的 UnrelatedErrorCodeIgnoresTheMessageKeywords。
func TestOpenAIBasisPoints_InvalidEncryptedContentIsAlsoCaughtInTheResponseFailedFrame(t *testing.T) {
	account := newBasisPointsTestAccount(9134)
	body := []byte(strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"reasoning","id":"rs_cold","summary":[],"encrypted_content":"blob-minted-on-the-codex-route"},`, 1))
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
			`{"type":"response.failed","response":{"id":"r","status":"failed","error":{"code":"invalid_encrypted_content","message":"Request failed."}}}`,
		)),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}

	first, _ := newBasisPointsTestContext(t)
	_, err := svc.Forward(context.Background(), first, account, body)
	require.Error(t, err)
	require.Contains(t, string(upstream.bodies[0]), "blob-minted-on-the-codex-route", "第一轮照发，才有被拒这回事")

	second, _ := newBasisPointsTestContext(t)
	_, err = svc.Forward(context.Background(), second, account, body)
	require.NoError(t, err)
	require.Len(t, upstream.bodies, 2)
	require.NotContains(t, string(upstream.bodies[1]), "blob-minted-on-the-codex-route")
}

// 判据本体的表驱动用例：三个分支各一条。上面那两条跨轮用例只覆盖「①读到码」这一支，把另外两支
// 改坏（删掉 nested 读法 / 把消息兜底改成 return false）整仓库照旧全绿 —— 第九轮变异测试实测。
func TestOpenAIBasisPointsRejectedInvalidEncryptedContentReadings(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{
			// ①：peek 抓到的 response.failed 帧把码放在 response.error.code。
			name: "码在 response.error.code",
			raw:  `{"type":"response.failed","response":{"error":{"code":"invalid_encrypted_content","message":"Request failed."}}}`,
			want: true,
		},
		{
			// ②：网关包一层 —— 顶层码是泛化的，真码嵌在 error.message 的信封里。
			// extractUpstreamErrorCode 在这个形状上早返回（error.code 非空就不解信封），所以只有
			// 单独读一次 error.message 的信封才拿得到。
			name: "真码嵌在 error.message 的信封里，顶层码是泛化的",
			raw:  `{"error":{"code":"invalid_request_error","message":"{\"error\":{\"code\":\"invalid_encrypted_content\",\"message\":\"nope\"}}"}}`,
			want: true,
		},
		{
			// ③：码完全缺失，只有一句话 —— 这时才允许看消息。
			name: "没有任何码，只有消息",
			raw:  `{"error":{"message":"The encrypted content could not be verified"}}`,
			want: true,
		},
		{
			name: "没有任何码，消息也无关",
			raw:  `{"error":{"message":"Something else went wrong"}}`,
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, openAIBasisPointsRejectedInvalidEncryptedContent([]byte(tc.raw)))
		})
	}
}

// 一次 Codex blob 被拒**不许连带**把有效的 BPS 压缩密文拉黑。两种密文来源互斥可判：
// reasoning 的只可能是 Codex 铸的（发不了 include=[reasoning.encrypted_content]），compaction 的
// 只可能是 BPS 自己铸的（实测它解得开自己那颗）。不区分的后果是静默的 —— 下一轮
// sanitizeEncryptedReasoningInputItem 把 compaction 项整项删除，模型丢掉压缩前的全部历史，
// 客户端侧零信号。
func TestOpenAIBasisPoints_ValidCompactionBlobSurvivesACodexBlobRejection(t *testing.T) {
	account := newBasisPointsTestAccount(9155)
	body := []byte(strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"reasoning","id":"rs_cold","summary":[],"encrypted_content":"CODEX-MINTED-BAD"},
		{"type":"compaction","id":"cmp_ok","encrypted_content":"BPS-MINTED-GOOD"},`, 1))
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusBadRequest, "application/json",
			`{"error":{"code":"invalid_encrypted_content","message":"Request failed."}}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}

	first, _ := newBasisPointsTestContext(t)
	_, err := svc.Forward(context.Background(), first, account, body)
	require.Error(t, err)

	second, _ := newBasisPointsTestContext(t)
	_, err = svc.Forward(context.Background(), second, account, body)
	require.NoError(t, err)
	require.Len(t, upstream.bodies, 2)
	require.NotContains(t, string(upstream.bodies[1]), "CODEX-MINTED-BAD", "元凶要剥掉")
	require.Contains(t, string(upstream.bodies[1]), "BPS-MINTED-GOOD", "有效的压缩摘要不许连带丢掉")
}

// 上一条的另一半：这一轮**只有**压缩摘要一颗密文时它就是唯一可能的元凶（BPS 也会拒自己铸的 blob：
// 过期、跨网关、或者这条会话换到了另一个同样开着开关的账号）。跳过它就等于一个字节都不记 lineage，
// 下一轮没东西可剥、blob 原样再发再被拒 —— 会话从压缩那一刻起**每轮硬错、永不自愈**，而客户端每轮
// 都会回放那个项。第十一轮 blocker，是「出站前/上游判死 ⇒ 会话永久失败」这一类的第四个实例。
func TestOpenAIBasisPoints_CompactionOnlyBlobRejectionStillSelfHeals(t *testing.T) {
	account := newBasisPointsTestAccount(9163)
	body := []byte(strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"compaction","id":"cmp_stale","encrypted_content":"BPS-MINTED-STALE"},`, 1))
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusBadRequest, "application/json",
			`{"error":{"code":"invalid_encrypted_content","message":"Request failed."}}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}

	first, _ := newBasisPointsTestContext(t)
	_, err := svc.Forward(context.Background(), first, account, body)
	require.Error(t, err)
	require.Contains(t, string(upstream.bodies[0]), "BPS-MINTED-STALE", "第一轮照发，才有被拒这回事")

	second, _ := newBasisPointsTestContext(t)
	_, err = svc.Forward(context.Background(), second, account, body)
	require.NoError(t, err)
	require.Len(t, upstream.bodies, 2)
	require.NotContains(t, string(upstream.bodies[1]), "BPS-MINTED-STALE", "第二轮必须剥掉，否则永远自愈不了")
}

// 上面那条回退的前置条件：候选池要先减掉**已经被剥掉**的摘要。entryBody 是剥离前的客户端原体，
// 而客户端每轮原样回放全部历史 ⇒ 曾被拒的 Codex blob 每轮都还在里面。不减掉的话 filtered 永远非空
// （里面是那颗幽灵），压缩密文永远进不了 lineage，回退形同虚设 —— 会话每轮硬错、永不自愈。
// 第十二轮 blocker。
func TestOpenAIBasisPoints_StaleStrippedBlobDoesNotBlockCompactionSelfHeal(t *testing.T) {
	account := newBasisPointsTestAccount(9167)
	body := []byte(strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"reasoning","id":"rs_ghost","summary":[],"encrypted_content":"CODEX-GHOST"},
		{"type":"compaction","id":"cmp_stale","encrypted_content":"BPS-MINTED-STALE"},`, 1))
	reject := func() *http.Response {
		return bpsTestResponse(http.StatusBadRequest, "application/json",
			`{"error":{"code":"invalid_encrypted_content","message":"Request failed."}}`)
	}
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		reject(), // 第一轮：拒 Codex blob，它进 lineage
		reject(), // 第二轮：幽灵已剥掉，这次是压缩密文被拒
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}

	for turn := 1; turn <= 2; turn++ {
		c, _ := newBasisPointsTestContext(t)
		_, err := svc.Forward(context.Background(), c, account, body)
		require.Error(t, err, "第 %d 轮", turn)
	}
	require.NotContains(t, string(upstream.bodies[1]), "CODEX-GHOST", "第二轮幽灵已经剥掉了")
	require.Contains(t, string(upstream.bodies[1]), "BPS-MINTED-STALE", "第二轮压缩密文还在，它才是元凶")

	third, _ := newBasisPointsTestContext(t)
	_, err := svc.Forward(context.Background(), third, account, body)
	require.NoError(t, err)
	require.Len(t, upstream.bodies, 3)
	require.NotContains(t, string(upstream.bodies[2]), "BPS-MINTED-STALE",
		"第三轮必须把压缩密文也剥掉，否则每轮都在重复标记那颗幽灵、永不自愈")
}

// 上一条咬的是 `known` 里 `bps:` 那半边。这一条咬**裸哈希**那半边：会话先在没开开关的账号上被
// Codex 路径拒过一颗密文（混着开/关开关的分组是允许的形态），再落到开着开关的账号。少了这一半，
// 那颗幽灵同样会把候选池永远撑住 ⇒ 压缩密文进不了 lineage ⇒ 自愈看起来做了其实没做。
// 变异实测：`known` 只读 `bps:` 键时整仓库照旧全绿，只有这条会红。
func TestOpenAIBasisPoints_CodexSideLineageAlsoCountsAsAlreadyStripped(t *testing.T) {
	account := newBasisPointsTestAccount(9173)
	body := []byte(strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"reasoning","id":"rs_ghost","summary":[],"encrypted_content":"CODEX-GHOST"},
		{"type":"compaction","id":"cmp_stale","encrypted_content":"BPS-MINTED-STALE"},`, 1))
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusBadRequest, "application/json",
			`{"error":{"code":"invalid_encrypted_content","message":"Request failed."}}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}

	// 先让 Codex 路径那把**裸**键记下那颗幽灵（等价于这个会话之前落在没开开关的账号上被拒过）。
	seed, _ := newBasisPointsTestContext(t)
	svc.markOpenAIWSInvalidEncryptedContentLineage(
		getOpenAIGroupIDFromContext(seed),
		svc.openAIWSLineageSessionHashFromContext(seed, body),
		[]string{openAIEncryptedContentDigest("CODEX-GHOST")},
	)

	first, _ := newBasisPointsTestContext(t)
	_, err := svc.Forward(context.Background(), first, account, body)
	require.Error(t, err)
	require.NotContains(t, string(upstream.bodies[0]), "CODEX-GHOST", "幽灵在第一轮就该被剥掉")

	second, _ := newBasisPointsTestContext(t)
	_, err = svc.Forward(context.Background(), second, account, body)
	require.NoError(t, err)
	require.Len(t, upstream.bodies, 2)
	require.NotContains(t, string(upstream.bodies[1]), "BPS-MINTED-STALE",
		"裸哈希那半边也要算进 known，否则压缩密文永远进不了 lineage")
}

// 反面：**有错误码、但不是这一条**时，绝不许再看消息。消息是上游可控的自由文本，让它决定「剥掉这个
// 会话的推理密文」等于把一个静默降质开关交给对端 —— 而被剥的 blob 在 Codex 后端本来是有效的。
// classifyOpenAIWSErrorEventFromRaw 自己不成立这条性质（code 非空且不匹配时它会穿到消息匹配），
// 所以 openAIBasisPointsRejectedInvalidEncryptedContent 在中间加了这道闸。少了这条用例，把那道闸
// 删掉整仓库照旧全绿。
func TestOpenAIBasisPoints_UnrelatedErrorCodeIgnoresTheMessageKeywords(t *testing.T) {
	account := newBasisPointsTestAccount(9140)
	body := []byte(strings.Replace(bpsTestClientBody, `"input":[`, `"input":[
		{"type":"reasoning","id":"rs_cold","summary":[],"encrypted_content":"blob-minted-on-the-codex-route"},`, 1))
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusBadRequest, "application/json",
			`{"error":{"code":"rate_limit_exceeded","message":"invalid_encrypted_content: the encrypted content could not be verified"}}`),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}

	first, _ := newBasisPointsTestContext(t)
	_, err := svc.Forward(context.Background(), first, account, body)
	require.Error(t, err)

	second, _ := newBasisPointsTestContext(t)
	_, err = svc.Forward(context.Background(), second, account, body)
	require.NoError(t, err)
	require.Len(t, upstream.bodies, 2)
	require.Contains(t, string(upstream.bodies[1]), "blob-minted-on-the-codex-route",
		"码是 rate_limit_exceeded：消息里的关键词不许触发剥离")
}

// 非流分支的记账：流式那半有 ProtocolViolation / StreamFailoverWithUsage 咬住，非流这半原来一条
// 都没有（fixture 写死 stream:true），把 handleSSEToJSON 那三处 failed 改回 nil 整仓库照旧全绿。
// 形态取「completed 带 usage，随后一条 error」：extractOpenAISSETerminalEvent 取最后一个终态事件，
// 所以 terminal 是 error，而 usage 已经在体里 —— 额度被吃掉了，这条路不换号也不会在别处重记。
func TestOpenAIBasisPoints_NonStreamingFailureAfterUsageStillBills(t *testing.T) {
	account := newBasisPointsTestAccount(9132)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
			`{"type":"response.created","response":{"id":"r","status":"in_progress","model":"gpt-6-astra","output":[]}}`,
			`{"type":"response.output_item.done","output_index":0,"item":`+bpsTestMessageItem+`}`,
			`{"type":"response.failed","response":{"id":"r","status":"failed","model":"gpt-6-astra","output":[`+bpsTestMessageItem+`],"usage":{"input_tokens":20,"output_tokens":5,"total_tokens":25},"error":{"code":"rate_limit_exceeded","message":"too many"}}}`,
		)),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	body := strings.Replace(bpsTestClientBody, `"stream":true`, `"stream":false`, 1)
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "不该拖低账号调度分")
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "不能换号")
	require.NotNil(t, result, "usage 到手就必须带出来记账")
	require.Equal(t, 20, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
	require.Len(t, upstream.requests, 1, "不该有第二次上游请求")
	require.Equal(t, StatusActive, account.Status)
	require.Contains(t, rec.Body.String(), "basispoints_status_429_stream")
}

// 代理/网络死了不是「BPS 承载不了这个形态」，是这个账号发不出请求：必须罚分 + 摘池，否则硬报错
// 口径下调度器会一直把请求塞给它，命中它的每一发都是客户端可见的失败，且没有自动恢复路径。
func TestOpenAIBasisPoints_TransportErrorStaysAnAccountFault(t *testing.T) {
	account := newBasisPointsTestAccount(9133)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{err: errors.New("proxyconnect tcp: dial tcp 10.0.0.1:1080: connect: connection refused")}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	result, err := svc.Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Nil(t, result)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "代理死了要罚这个账号的调度分")
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "但仍然不换号：换到没开开关的账号就又走回 Codex")
	require.Contains(t, err.Error(), "transport_error")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "basispoints_transport_error")
	// 罚分只是软权重（oauth 走不到 apikey 那道健康熔断），真正兜住"别再往这个号塞请求"的是摘池。
	// 少这条断言的话，把摘池那几行删掉上面四条照旧全绿。
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account), "持久传输错误要把账号临时摘出池子")
}

// usage policy 封掉这个号的 BPS 通道 ⇒ 报错 + 停用账号（用户 2026-09-29 拍板：403 有冷却，接着打
// 没意义）。**刻意不关这个账号的 BPS 开关** —— 关了开关它会继续服务但走 Codex = 降智。
//
// **判据是签名不是 HTTP 403**，下面四条用例是一组：正面两条（HTTP 403 带签名、HTTP 200 流内 error
// 带签名）+ 反面三条（403 无签名 / 403 model_access_changed / 429）。只按状态码判的话反面那两条 403
// 会红 —— 而那正是「客户端请求一次没权限的模型就永久停掉一个健康账号」。
//
// 嵌的是**接口本身**而不是 mockAccountRepoForGemini：那个 mock 在 `//go:build unit` 后面，而这个
// 文件没有 tag（两种模式都编），引它会让不带 tag 的 vet / golangci-lint 直接 typecheck 失败。
// 嵌接口就地满足签名，除 SetError 以外的方法真被调到会 nil panic —— 正好把「这条路只该调它一个」
// 钉住。
type bpsAccountRepoStub struct {
	AccountRepository
	setErrCalls  int
	lastErrorMsg string
	lastID       int64
}

func (r *bpsAccountRepoStub) SetError(_ context.Context, id int64, errorMsg string) error {
	r.setErrCalls++
	r.lastID = id
	r.lastErrorMsg = errorMsg
	return nil
}

func TestOpenAIBasisPoints_403DisablesTheAccount(t *testing.T) {
	account := newBasisPointsTestAccount(9136)
	c, rec := newBasisPointsTestContext(t)
	repo := &bpsAccountRepoStub{}
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		// 消息**刻意不带**判据关键词：这条只咬 code 分支。两条正面用例的码和消息都带签名的话，
		// 两个分支互相兜住，各自单独改坏整套照旧全绿（第十轮变异实测）。
		// 同时消息里回显 bearer token 与 chatgpt_account_id：这条要落进 accounts.error_message
		// 并在管理台上原样显示，而 sanitizeUpstreamErrorMessage 只替 URL 里的敏感 query 参数，
		// **不打码这两个**。少了那两条断言，这条出口就是唯一一个拿同一份上游文本却不打码就落库的路径。
		bpsTestResponse(http.StatusForbidden, "application/json",
			`{"error":{"code":"usage_policy_violation","message":"nope: token=oauth-token acct=chatgpt-acc"}}`),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream, accountRepo: repo}
	_, err := svc.Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Error(t, err)
	require.Contains(t, err.Error(), "status_403")
	require.Equal(t, http.StatusForbidden, rec.Code, "403 原样透给客户端")

	require.Equal(t, 1, repo.setErrCalls, "403 带 usage policy 签名要停用账号")
	require.Equal(t, account.ID, repo.lastID)
	require.Contains(t, repo.lastErrorMsg, "Basis Points usage policy block")
	require.NotContains(t, repo.lastErrorMsg, "oauth-token", "落库的原因不许带 bearer token")
	require.NotContains(t, repo.lastErrorMsg, "chatgpt-acc", "也不许带 chatgpt_account_id")
	require.Contains(t, repo.lastErrorMsg, "[REDACTED]")
	// extra 里的开关不动：关了开关这个号会继续服务但走 Codex = 降智。
	require.Equal(t, true, account.Extra[openAIBasisPointsExtraKey], "不许顺手关掉 BPS 开关")
}

// 正面第二条：同一个封通道更常见的形态是 **HTTP 200 + 流内 error 帧**（见 peek 那段注释）。
// 只在 HTTP 非 2xx 那个出口判，这一半就是零覆盖 —— 而按代码自己的判断它比 403 更常见。
//
// 码**刻意写成泛化的**：这条只咬消息分支（实测原话 "This request was blocked by our usage
// policy."），与上面那条只咬 code 分支的正好互补。
func TestOpenAIBasisPoints_UsagePolicyBlockInStreamErrorAlsoDisables(t *testing.T) {
	account := newBasisPointsTestAccount(9144)
	c, _ := newBasisPointsTestContext(t)
	repo := &bpsAccountRepoStub{}
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
			`{"type":"error","error":{"code":"invalid_request_error","message":"This request was blocked by our usage policy."}}`,
		)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream, accountRepo: repo}
	_, err := svc.Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Error(t, err)
	require.Equal(t, 1, repo.setErrCalls, "流内 error 帧里的同一签名也要停号")
	require.Contains(t, repo.lastErrorMsg, "Basis Points usage policy block")
}

// 反面三条。403 那两条是 blocker 的本体：这条通道上 403 **同时**是「这个号没有这个模型」的回法
// （09-25 模型表：sol/luna/terra、gpt-5.5 全 403），而本层刻意不做模型白名单 —— 只按状态码判的话，
// 任何持本站 key 的客户端请求一次 luna 就永久停掉一个健康账号，循环 N 次停 N 个。
// Cloudflare 的 WAF 403（HTML 体、解不出 message）同理。
func TestOpenAIBasisPoints_RejectWithoutUsagePolicySignatureDoesNotDisable(t *testing.T) {
	cases := []struct {
		name, reason string
		status       int
		contentType  string
		body         string
		clientBody   string
	}{
		{
			name: "403 但是 Cloudflare 的 WAF HTML，解不出 message", status: http.StatusForbidden,
			contentType: "text/html", body: "<html><head><title>403 Forbidden</title></head></html>",
			reason: "status_403",
		},
		{
			name: "403 但是这个号没这个模型（专用标签）", status: http.StatusForbidden,
			contentType: "application/json",
			body:        `{"error":{"code":"basispoints_model_access_changed","message":"Permission denied for this model."}}`,
			reason:      "model_access_changed",
		},
		{
			name: "429 并发打出来的瞬时限流", status: http.StatusTooManyRequests,
			contentType: "application/json",
			body:        `{"error":{"code":"rate_limit_exceeded","message":"slow down"}}`,
			reason:      "status_429",
		},
		{
			// **这条是最现实的误停**：OpenAI 对 reasoning 模型的逐请求内容审核拒绝，官方文案里带
			// "usage policy"。它是请求级拒绝，跟这个号的通道被封是两件事 —— 按消息宽松匹配的话，
			// 一句被拦下的 prompt 就永久停掉一个满血账号。
			name: "400 invalid_prompt，文案里带 usage policy 但是请求级拒绝", status: http.StatusBadRequest,
			contentType: "application/json",
			body:        `{"error":{"code":"invalid_prompt","message":"Invalid prompt: your prompt was flagged as potentially violating our usage policy. Please try again with a different prompt."}}`,
			reason:      "status_400_invalid_prompt",
		},
		{
			// 判据不能完全由上游文本决定：这条连状态码和错误码都跟封号无关。
			name: "404 model_not_found，文案里让你去看 usage policy", status: http.StatusNotFound,
			contentType: "application/json",
			body:        `{"error":{"code":"model_not_found","message":"The model does not exist. See our usage policy for details."}}`,
			reason:      "status_404_model_not_found",
		},
		{
			// **这条是第十一轮两个审查者各自独立抓到的 blocker 本体。** 出站的 model 是客户端原样控制
			// 的字节（本层刻意不做模型白名单），而 OpenAI 的 model_not_found 文案把模型名**回显**进
			// error.message —— 于是客户端把封号原话本身当模型名发出去，一发停一个号，循环 N 次停 N 个。
			// 上一版判据是 Contains(整句)，命中；现在锚到句首 + 先打掉客户端可控字节，两层都挡。
			name: "404 model_not_found，客户端把封号原话塞进 model 让上游回显", status: http.StatusNotFound,
			contentType: "application/json",
			body: `{"error":{"code":"model_not_found","message":"The model ` +
				"`This request was blocked by our usage policy.`" +
				` does not exist or you do not have access to it."}}`,
			reason: "status_404_model_not_found",
			clientBody: strings.Replace(bpsTestClientBody, `"model":"gpt-6-astra"`,
				`"model":"This request was blocked by our usage policy."`, 1),
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			account := newBasisPointsTestAccount(int64(9145 + i))
			c, _ := newBasisPointsTestContext(t)
			repo := &bpsAccountRepoStub{}
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				bpsTestResponse(tc.status, tc.contentType, tc.body),
			}}
			clientBody := tc.clientBody
			if clientBody == "" {
				clientBody = bpsTestClientBody
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream, accountRepo: repo}
			_, err := svc.Forward(context.Background(), c, account, []byte(clientBody))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.reason)
			require.Zero(t, repo.setErrCalls, "没有 usage policy 签名就不许停号")
			require.Equal(t, StatusActive, account.Status)
		})
	}
}

// 判据本体的表：这是全仓唯一一个**不可逆**动作（SetError 没有任何自动恢复覆盖它）的开关，而载体是
// 上游的一句自由文本。两条正面（实测原话、BPS 惯用的状态码前缀形态 —— 09-30 实测它的 422 体就是
// `{"message":"422: Invalid request body."}`）+ 三条反面（请求级审核拒绝、回显、打码兜底）。
func TestOpenAIBasisPointsUsagePolicyBlockPredicate(t *testing.T) {
	const sentence = "This request was blocked by our usage policy."
	cases := []struct {
		name, code, message string
		clientEcho          []string
		want                bool
	}{
		{name: "实测原话", message: sentence, want: true},
		{name: "带状态码前缀", message: "403 " + sentence, want: true},
		{name: "带状态码冒号前缀", message: "403: " + sentence, want: true},
		{name: "码分支", code: "usage_policy_violation", message: "nope", want: true},
		{
			name:    "请求级内容审核拒绝不算封通道",
			message: "Invalid prompt: your prompt was flagged as potentially violating our usage policy.",
		},
		{
			// 句首锚点这一层：回显永远出现在上游自己的句子里，做不成句首。
			name:    "上游回显客户端模型名",
			message: "The model `" + sentence + "` does not exist or you do not have access to it.",
		},
		{
			// 打码那一层：即便哪天上游换成值打头的语法，clientEcho 也先把它换成 [REDACTED]。
			name:       "值打头的语法也被打码兜住",
			message:    sentence + " is not one of ['gpt-6-astra']",
			clientEcho: []string{"", sentence},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, openAIBasisPointsUsagePolicyBlock(tc.code, tc.message, tc.clientEcho...))
		})
	}
}

// 反面：瞬时错误不摘池。少了这条，谁把 classifyUpstreamTransportError().Persistent 判定去掉都不会红，
// 而那会让一次网络抖动把一个健康账号摘掉 10 分钟。
func TestOpenAIBasisPoints_TransientTransportErrorDoesNotUnschedule(t *testing.T) {
	account := newBasisPointsTestAccount(9135)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{err: errors.New("dial tcp 1.2.3.4:443: i/o timeout")}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	_, err := svc.Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "仍然罚分")
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account), "但瞬时错误不摘池")
}

func TestOpenAIBasisPoints_ProtocolViolationFailsThisRequestOnlyAndStillBills(t *testing.T) {
	account := newBasisPointsTestAccount(9112)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{bpsTestResponse(http.StatusOK, "text/event-stream",
		bpsTestCompletedSSE(bpsTestTransportCall("call_bad", `{"name":"not_in_catalog","arguments":{}}`)))}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Error(t, err)
	// 协议违规是这条请求自己的确定性失败：不换号重跑（否则每个账号的 BPS 额度都白烧一次），
	// 合成的 response.failed 带上 completed 里的 usage，已消耗的额度照样记账。
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "不能 failover")
	require.NotNil(t, result)
	require.Equal(t, 20, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
	require.Equal(t, StatusActive, account.Status)
	require.Len(t, upstream.requests, 1)
	out := rec.Body.String()
	require.Contains(t, out, "basispoints_protocol_error")
	require.Contains(t, out, "invalid_request_error")
	require.NotContains(t, out, "not_in_catalog", "不执行、也不把目录外的调用交给客户端")
	require.NotContains(t, out, "run_officejs")
}

func TestOpenAIBasisPoints_FailureAfterCreatedIsAHardError(t *testing.T) {
	account := newBasisPointsTestAccount(9119)
	c, rec := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestSSE(
			`{"type":"response.created","response":{"id":"r","status":"in_progress","output":[]}}`,
			`{"type":"response.in_progress","response":{"id":"r","status":"in_progress","output":[]}}`,
			`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs","summary":[]}}`,
			// 消息刻意含 "permission denied"：openAIStreamFailureStatus 会因此给 403，于是这条用例
			// 同时钉住"专用标签不被状态码覆写"。原来写 "gone" 时状态是 502，那道守卫从没被求值过。
			`{"type":"response.failed","response":{"id":"r","status":"failed","output":[],"error":{"code":"basispoints_model_access_changed","message":"gone: permission denied"}}}`,
		)),
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Nil(t, result)
	// created / 推理占位都不算客户端可见输出，所以这里仍然是「接受之前」，只是不再落回。
	requireOpenAIBasisPointsUnavailable(t, err, rec, upstream, account, "model_access_changed")
	require.NotContains(t, rec.Body.String(), "gone")
}

func TestOpenAIBasisPoints_SilentUpstreamIsAHardError(t *testing.T) {
	// 这条测的是「接受之前就一直沉默」，走的是 peek 的上限，不是 in-stream 的那个。
	previous := openAIBasisPointsPeekSilence
	openAIBasisPointsPeekSilence = 50 * time.Millisecond
	defer func() { openAIBasisPointsPeekSilence = previous }()
	account := newBasisPointsTestAccount(9120)
	c, rec := newBasisPointsTestContext(t)
	silent, _ := io.Pipe()
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: silent},
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	result, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(bpsTestClientBody))
	require.Nil(t, result)
	// 200 之后一个事件都不来：关掉上游、报错，不能吊死并发槽，也不能落回。
	requireOpenAIBasisPointsUnavailable(t, err, rec, upstream, account, "upstream_silent")
}

// 09-30 现网故障的回归用例：会话里用过一次联网搜索之后，历史里从此有一条 `web_search_call`，
// 客户端每轮原样回放 ⇒ 撞上 translateHistory 的 item 级 default ⇒ 这个会话在开着开关的账号上
// 每轮都 502、永不自愈（抓包实证：31 发请求体逐字节相同，内容部件全在白名单内，唯一越界的就是
// 这条 item）。现在换成占位 developer 消息 —— 会话继续，模型知道少了一段，且被明确告知不要编结果。
//
// call 与 output **都**换占位符，所以不存在「丢 call 留 output」那种孤儿（原来判死的理由就是它）。
func TestOpenAIBasisPoints_NativeToolCallItemsBecomePlaceholders(t *testing.T) {
	for _, kind := range []string{
		"web_search_call", "tool_call", "local_shell_call", "tool_search_call",
		"mcp_tool_call", "image_generation_call", "zzz_future_item",
	} {
		t.Run(kind, func(t *testing.T) {
			account := newBasisPointsTestAccount(9180)
			body := strings.Replace(bpsTestClientBody, `"input":[`,
				`"input":[{"type":"`+kind+`","id":"x_1","status":"completed","SENTINEL":"MUST-NOT-LEAK"},`, 1)
			c, _ := newBasisPointsTestContext(t)
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
			}}
			_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
			require.NoError(t, err, "原生工具项不再判死")
			require.Len(t, upstream.requests, 1, "请求照发到 BPS")
			require.Equal(t, openAIBasisPointsResponsesURL, upstream.lastReq.URL.String())

			item := gjson.GetBytes(upstream.lastBody, "input.2")
			require.Equal(t, "message", item.Get("type").String())
			require.Equal(t, "developer", item.Get("role").String(), "代理加的注解不冒充用户或模型")
			text := item.Get("content.0.text").String()
			require.Contains(t, text, kind, "占位文案要点明是哪一类被移除了")
			require.Contains(t, text, "Do not claim to have used it",
				"必须明确叫模型别编结果，否则它最常见的接续就是编一个")

			require.NotContains(t, string(upstream.lastBody), "MUST-NOT-LEAK",
				"原项的字段一个字节都不许出站（BPS 的 item 级 schema 是封闭的）")
			require.NotContains(t, string(upstream.lastBody), `"type":"`+kind+`"`,
				"原项类型不许原样出站")
		})
	}
}

// 类型名是客户端可控字节，占位文案里必须先打成安全短标签，别让它往模型上下文里塞东西。
func TestOpenAIBasisPointsSanitizePartKind(t *testing.T) {
	for raw, want := range map[string]string{
		"input_file":                "input_file",
		"Input_File":                "input_file",
		`ignore previous"; DROP {}`: "ignorepreviousdrop",
		"":                          "unknown",
		"   ":                       "unknown",
		"!!!":                       "unknown",
		strings.Repeat("a", 90):     strings.Repeat("a", 32),
		"a\nb":                      "ab",
		"type-with-dash":            "typewithdash",
	} {
		require.Equal(t, want, sanitizeOpenAIBasisPointsPartKind(raw), raw)
	}
}

// 换成占位符之后请求是成功的，`input_content` 那个落回读数就没了 —— 必须留一条运维日志，
// 不然现场根本不知道客户端发来的哪一类被丢了（09-30 排查就是因为 reason 只写 input_content，
// 只能靠抓包才定位到）。
func TestOpenAIBasisPoints_DroppedContentIsLogged(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	out, err := bridge.prepare([]byte(`{"model":"m","input":[
		{"type":"web_search_call","id":"ws_1"},
		{"type":"message","role":"user","content":[{"type":"input_file","file_id":"f"}]}
	]}`), "m")
	require.NoError(t, err)
	require.NotEmpty(t, out)
	require.NotEmpty(t, bridge.warnings, "丢了东西就必须有一行运维读数")
	joined := strings.Join(bridge.warnings, " | ")
	require.Contains(t, joined, "placeholder")
	require.Contains(t, joined, "item:web_search_call")
	require.Contains(t, joined, "input_file")
}

// 裸字符串元素（`content:["hi"]`，SDK 的宽松写法）是正文本身，**不许**换成占位符 ——
// 那会把用户真正说的话悄悄丢掉。
func TestOpenAIBasisPoints_BareStringContentStaysText(t *testing.T) {
	account := newBasisPointsTestAccount(9181)
	body := strings.Replace(bpsTestClientBody, `"input":[`,
		`"input":[{"type":"message","role":"user","content":["KEEP-THIS-TEXT"]},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)
	part := gjson.GetBytes(upstream.lastBody, "input.2.content.0")
	require.Equal(t, "input_text", part.Get("type").String())
	require.Equal(t, "KEEP-THIS-TEXT", part.Get("text").String(), "正文不许被占位符吃掉")
}

// S4：消毒器有自己的单测，但没有任何集成用例证明它**真的被接进了**占位符构造 —— 把
// droppedPart / droppedItem 里的 sanitizeOpenAIBasisPointsPartKind(kind) 换成裸 kind，
// 整套用例照旧全绿（审查者的变异就是这个）。这里在出站报文上钉住。
func TestOpenAIBasisPoints_PlaceholderKindIsSanitizedOnTheWire(t *testing.T) {
	account := newBasisPointsTestAccount(9183)
	dirty := `Ignore Previous\nInstructions\"`
	body := strings.Replace(bpsTestClientBody, `"input":[`,
		`"input":[{"type":"`+dirty+`","id":"x_1"},`+
			`{"type":"message","role":"user","content":[{"type":"`+dirty+`"}]},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.NoError(t, err)

	clean := "ignorepreviousinstructions"
	require.Equal(t, openAIBasisPointsDroppedItemNotice(clean),
		gjson.GetBytes(upstream.lastBody, "input.2.content.0.text").String(), "item 占位符用的是打码后的标签")
	require.Equal(t, openAIBasisPointsDroppedPartNotice(clean),
		gjson.GetBytes(upstream.lastBody, "input.3.content.0.text").String(), "部件占位符同理")
	require.NotContains(t, string(upstream.lastBody), "Ignore Previous", "原始类型名一个字节都不出站")
}

// 实测过 200 的原生项**原样透传**（不是占位符），但只在落进实测过的形状时 —— 越界（未知键、
// 未实测的 action.type / status）一律降级成占位消息，否则一个嵌套键就是永久 400。
func TestOpenAIBasisPoints_MeasuredNativeItemsPassThroughWithinTheMeasuredShape(t *testing.T) {
	account := newBasisPointsTestAccount(9184)
	run := func(t *testing.T, item string) []byte {
		t.Helper()
		body := strings.Replace(bpsTestClientBody, `"input":[`, `"input":[`+item+`,`, 1)
		c, _ := newBasisPointsTestContext(t)
		upstream := &httpUpstreamRecorder{responses: []*http.Response{
			bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
		}}
		_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
		require.NoError(t, err)
		require.Len(t, upstream.requests, 1)
		return upstream.lastBody
	}

	t.Run("web_search_call 实测形状原样透传", func(t *testing.T) {
		out := run(t, `{"type":"web_search_call","id":"ws_1","status":"completed",`+
			`"action":{"type":"search","query":"weather"}}`)
		item := gjson.GetBytes(out, "input.2")
		require.Equal(t, "web_search_call", item.Get("type").String(), "不该变成占位符")
		require.Equal(t, "weather", item.Get("action.query").String(), "action 原样带过去")
	})

	t.Run("mcp_call 实测形状原样透传", func(t *testing.T) {
		item := gjson.GetBytes(run(t, `{"type":"mcp_call","id":"mcp_1","server_label":"svc",`+
			`"name":"echo","arguments":"{}","output":"ok"}`), "input.2")
		require.Equal(t, "mcp_call", item.Get("type").String())
		require.Equal(t, "echo", item.Get("name").String())
	})

	// 越界的四种都必须降级 —— 这是整套改动的核心性质：永远不产生永久 400。
	for name, item := range map[string]string{
		"未知键":            `{"type":"web_search_call","id":"ws_1","status":"completed","action":{"type":"search"},"zzz_unknown":1}`,
		"action 里的未知子类型": `{"type":"web_search_call","id":"ws_1","status":"completed","action":{"type":"open_page","url":"https://x"}}`,
		"未实测的 status":    `{"type":"web_search_call","id":"ws_1","status":"in_progress","action":{"type":"search"}}`,
		"action 不是对象":    `{"type":"web_search_call","id":"ws_1","status":"completed","action":"search"}`,
	} {
		t.Run("降级："+name, func(t *testing.T) {
			out := run(t, item)
			got := gjson.GetBytes(out, "input.2")
			require.Equal(t, "message", got.Get("type").String(), "越界必须降级成占位消息")
			require.Equal(t, "developer", got.Get("role").String())
			require.Equal(t, openAIBasisPointsDroppedItemNotice("web_search_call"),
				got.Get("content.0.text").String())
			require.NotContains(t, string(out), `"web_search_call"`, "原项不许出站")
		})
	}
}

// S2：两条「在回放历史里每轮都出现 ⇒ 判死就是会话永久失败」的工具历史形态。两者都来自
// Jackson / Newtonsoft 的默认序列化，和文本部件那边专门为 `"text":null` 破的例是同一个来源 ——
// 那边放过、这边永久杀会话，原来是自相矛盾的。
func TestOpenAIBasisPoints_ToolHistoryTolerateNullNamespaceAndEmptyArguments(t *testing.T) {
	account := newBasisPointsTestAccount(9185)
	for name, call := range map[string]string{
		"namespace 显式 null": `{"type":"function_call","call_id":"c_1","name":"exec_command","namespace":null,"arguments":"{\"cmd\":\"ls\"}"}`,
		"arguments 空串":      `{"type":"function_call","call_id":"c_1","name":"exec_command","arguments":""}`,
		"arguments 全空白":     `{"type":"function_call","call_id":"c_1","name":"exec_command","arguments":"   "}`,
	} {
		t.Run(name, func(t *testing.T) {
			body := strings.Replace(bpsTestClientBody, `"input":[`,
				`"input":[`+call+`,{"type":"function_call_output","call_id":"c_1","output":"ok"},`, 1)
			c, _ := newBasisPointsTestContext(t)
			upstream := &httpUpstreamRecorder{responses: []*http.Response{
				bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
			}}
			_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
			require.NoError(t, err, "不许判死：它在回放历史里每轮都在")
			require.Len(t, upstream.requests, 1)
			// 重建出来的 run_officejs 传输项里工具名不带 namespace 前缀（null 当没给）。
			envelope := gjson.GetBytes(upstream.lastBody, `input.#(type=="custom_tool_call")#|0`)
			_ = envelope
			require.Contains(t, string(upstream.lastBody), "exec_command", "调用照样中继出去")
			require.NotContains(t, string(upstream.lastBody), "tool_history")
		})
	}
	// 非空但解不开的 arguments 仍然判死 —— 那是真畸形，不是零参数。
	body := strings.Replace(bpsTestClientBody, `"input":[`,
		`"input":[{"type":"function_call","call_id":"c_2","name":"exec_command","arguments":"{not json"},`+
			`{"type":"function_call_output","call_id":"c_2","output":"ok"},`, 1)
	c, _ := newBasisPointsTestContext(t)
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err := (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}).Forward(context.Background(), c, account, []byte(body))
	require.Error(t, err)
	require.Contains(t, err.Error(), "tool_history")
	// 带空格的 namespace 也仍然判死：拼出来是 `"a b.exec_command"`，上游没见过这种工具名。
	body = strings.Replace(bpsTestClientBody, `"input":[`,
		`"input":[{"type":"function_call","call_id":"c_3","name":"exec_command","namespace":" a b ","arguments":"{}"},`+
			`{"type":"function_call_output","call_id":"c_3","output":"ok"},`, 1)
	c2, _ := newBasisPointsTestContext(t)
	up2 := &httpUpstreamRecorder{responses: []*http.Response{
		bpsTestResponse(http.StatusOK, "text/event-stream", bpsTestCompletedSSE(bpsTestMessageItem)),
	}}
	_, err = (&OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up2}).Forward(context.Background(), c2, account, []byte(body))
	require.Error(t, err)
	require.Contains(t, err.Error(), "tool_history")
}
