package service

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// codexGuardianCreditsRequestedKey 是 Guardian（自动审批）的计费标记。codex 0.156 起，ChatGPT 登录走
// codex 后端时，每条非 guardian 评审请求的 client_metadata 都带 "true"（rust-v0.156.1 core/src/client.rs:
// 1122-1155 set_guardian_metadata，HTTP 体与 WS 帧含预热帧都经它；0.155 及以前只在手动开 free_guardian
// 时带，0.156 把该开关降为兼容旧配置的空项，注释写明计费改由后端决定）。下游经 API key 连网关，自己
// 永远不发，双开出站由网关按真客户端规则补。评审请求那一侧（x-codex-guardian 头、parent_response_id）
// 网关拿不到可靠的父响应 ID，未对齐，见约定暂缓项。
const codexGuardianCreditsRequestedKey = "guardian_credits_requested"

// codexGuardianCreditsRequested：双开，凭证是 ChatGPT 登录（PAT、Agent Identity 不带——client.rs 只认
// CodexAuth::Chatgpt / ChatgptAuthTokens；凭证种类看凭证源，影子行共用母账号凭证），且不是 guardian
// 评审会话（is_basic_session_source，guardian/review.rs:102：两种 guardian 会话源的 x-openai-subagent
// 都是 "guardian"）。体内与头任一处标了 guardian 就不带。
func codexGuardianCreditsRequested(c *gin.Context, account *Account, payload []byte) bool {
	if !codexDeviceWireProfileEnabled(c, account) {
		return false
	}
	source := codexAccountIdentitySource(c, account)
	if source.IsOpenAIPersonalAccessToken() || source.IsOpenAIAgentIdentity() {
		return false
	}
	subagents := []string{gjson.GetBytes(payload, "client_metadata."+openAISubagentHeader).String()}
	if c != nil && c.Request != nil {
		subagents = append(subagents, c.GetHeader(openAISubagentHeader))
	}
	for _, subagent := range subagents {
		if strings.EqualFold(strings.TrimSpace(subagent), "guardian") {
			return false
		}
	}
	return true
}

// applyCodexGuardianCreditsRequested 在 HTTP 请求体定稿时补这个键。判据与请求体压缩相同：双开且出站是
// /responses（旧式 /responses/compact 的请求体没有 client_metadata，不补）。client_metadata 缺失就新建，
// 存在但不是对象不动。
func applyCodexGuardianCreditsRequested(c *gin.Context, account *Account, targetURL string, body []byte) []byte {
	if !gjson.ParseBytes(body).IsObject() || !codexRequestBodyCompressionEnabled(c, account, targetURL) ||
		!codexGuardianCreditsRequested(c, account, body) {
		return body
	}
	if meta := gjson.GetBytes(body, "client_metadata"); meta.Exists() && !meta.IsObject() {
		return body
	}
	return setCodexWSClientMetadataString(body, codexGuardianCreditsRequestedKey, "true")
}
