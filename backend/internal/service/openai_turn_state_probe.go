package service

// 合成一条 Codex 第一回合请求，沿真实转发管线出站。网关池的验票垫话（openai_gwpool_warm.go）
// 与 state-echo 的确认请求体（openai_gwpool_echo_confirm.go）都从这里取形态。

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
)

// openAITurnStateProbeIdentity 是一个全新 Codex 会话的身份：codex 0.153 真实形态，
// session == thread（新线程）、turn 独立、window = "<thread>:1"，均为 UUIDv7；
// installation 按账号稳定派生（真客户端的 installation_id 是本机固定值）。
type openAITurnStateProbeIdentity struct {
	session, thread, turn, window, contextWindow, installation string
}

func newOpenAITurnStateProbeIdentity(account *Account) openAITurnStateProbeIdentity {
	thread := uuid.Must(uuid.NewV7()).String()
	return openAITurnStateProbeIdentity{
		session:       thread,
		thread:        thread,
		turn:          uuid.Must(uuid.NewV7()).String(),
		window:        thread + ":1",
		contextWindow: uuid.Must(uuid.NewV7()).String(),
		// 种子沿用旧猎手的字面量：改了它，每个账号探测出站的 x-codex-installation-id 都会变。
		installation: deriveStableUUIDv4("turn-state-hunter:" + codexAccountIdentityNamespace(account)),
	}
}

func (ids openAITurnStateProbeIdentity) turnMetadata() string {
	encoded, _ := json.Marshal(map[string]any{
		"installation_id":   ids.installation,
		"session_id":        ids.session,
		"thread_id":         ids.thread,
		"turn_id":           ids.turn,
		"root_turn_id":      ids.turn,
		"window_id":         ids.window,
		"context_window_id": ids.contextWindow,
		"window_number":     1,
	})
	return string(encoded)
}

// newOpenAITurnStateProbeContext 合成一个入站上下文，形态照 Codex CLI 直连的第一回合
// （见 openai_codex_fingerprint_convergence_test.go 的 newConvTestContext）。管线的
// 头/体投影全从它读入站信息，所以这里必须像一个真客户端。
func newOpenAITurnStateProbeContext(ids openAITurnStateProbeIdentity) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	identity := resolveCodexOutboundIdentity("")
	h := c.Request.Header
	h.Set("User-Agent", identity.userAgent)
	h.Set("originator", identity.originator)
	h.Set("content-type", "application/json")
	h.Set("session-id", ids.session)
	h.Set("thread-id", ids.thread)
	h.Set("x-client-request-id", ids.thread)
	h.Set("x-codex-installation-id", ids.installation)
	h.Set("x-codex-window-id", ids.window)
	h.Set(openAIWSTurnMetadataHeader, ids.turnMetadata())
	return c
}

// openAITurnStateProbeBody 是最小的第一回合请求体：字段集照真实 Codex CLI，内容只有一条
// 用户消息。instructions 用该模型的真实 base prompt（与真客户端一致）。
func openAITurnStateProbeBody(model, effort string, ids openAITurnStateProbeIdentity, text string) map[string]any {
	return map[string]any{
		"model":        model,
		"instructions": openai.CodexBaseInstructionsForModel(model),
		"input": []any{map[string]any{
			"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": text}},
		}},
		"tools":               []any{},
		"tool_choice":         "auto",
		"parallel_tool_calls": false,
		"reasoning":           map[string]any{"effort": effort, "summary": "auto"},
		"store":               false,
		"stream":              true,
		"include":             []any{"reasoning.encrypted_content"},
		"prompt_cache_key":    ids.session,
		"client_metadata": map[string]any{
			"session_id":              ids.session,
			"thread_id":               ids.thread,
			"turn_id":                 ids.turn,
			"root_turn_id":            ids.turn,
			"x-codex-installation-id": ids.installation,
			"x-codex-window-id":       ids.window,
			"x-codex-turn-metadata":   ids.turnMetadata(),
		},
	}
}

// buildOpenAITurnStateProbe 走与 Forward 相同的 Codex OAuth 出站序列：身份来源 → OAuth
// 转换 → 身份收口暂存 → buildUpstreamRequest。
//
// 与真实流量的已知差异：没有入站 API Key，身份来源按「无 key」派生；installation_id 是探测
// 自己的稳定派生值；请求体没有 environment_context，头里没有 x-codex-inference-call-id。
// 调用方要经 doOpenAIUpstreamRoundTrip 发出：裸打 httpUpstream 会绕过接管 OAuth 出站的插件，
// 传输层/TLS 指纹就和真实流量不同了。
func (s *OpenAIGatewayService) buildOpenAITurnStateProbe(ctx context.Context, account *Account, model, effort, text string) (*http.Request, error) {
	if s == nil || account == nil {
		return nil, errors.New("turn-state probe: gateway or account is nil")
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, errors.New("turn-state probe: model is empty")
	}
	ids := newOpenAITurnStateProbeIdentity(account)
	c := newOpenAITurnStateProbeContext(ids)
	if _, err := s.prepareCodexAccountIdentitySource(ctx, c, account); err != nil {
		return nil, err
	}
	decoded := openAITurnStateProbeBody(model, effort, ids, text)
	result := applyCodexOAuthTransformWithOptions(decoded, codexOAuthTransformOptions{IsCodexCLI: true})
	if result.Error != nil {
		return nil, result.Error
	}
	stageCodexOAuthIdentity(c, account, decoded, false)
	upstreamModel := model
	if result.NormalizedModel != "" {
		upstreamModel = result.NormalizedModel
	}
	SetOpsUpstreamModel(c, upstreamModel)
	body, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}
	return s.buildUpstreamRequest(ctx, c, account, body, token, true, ids.session, true)
}
