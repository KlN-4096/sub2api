package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Basis Points 直通：ChatGPT 登录的 oauth 账号开了 extra.openai_basispoints 后，/v1/responses 改打
// Excel 产品（Basis Points）的后端 bps.openai.com，同一份 access token、同一个 chatgpt-account-id，
// 绕开 Codex 后端的降智改道。模型名原样发；BPS 承载不了的请求（显式联网搜索、生图、结构化
// 输出、强制 tool_choice、previous_response_id、图片上传失败、工具历史找不回、上游判模型不可用）
// 逐请求落回原 Codex 路径，客户端看不出差别。同账号两条通道的加密推理可互相回放。
// WS 客户端强制走 HTTP 桥，桥的每一轮同样先试 BPS；不做 compact、/v1/messages 与 chat-completions 桥。
// 参考 hloolx/codex2api、Kaixxrua/excel-codex-bridge、JaxsonWang/cpa-plugin-oai-basispoints。

const (
	openAIBasisPointsExtraKey         = "openai_basispoints"
	openAIBasisPointsResponsesURL     = "https://bps.openai.com/basispoints/api/responses"
	openAIBasisPointsAttachmentsURL   = "https://bps.openai.com/basispoints/api/attachments"
	openAIBasisPointsUpstreamEndpoint = "/basispoints/api/responses"
	// Excel 桌面版加载项跑在 Edge WebView2 里，UA 与下面 x-stainless-runtime: browser:chrome 自洽。
	openAIBasisPointsUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
)

// openAIBasisPointsClientProfile：Excel 加载项的客户端画像头。
//
// 这 16 个 = excel-codex-bridge 的 _DEFAULT_CLIENT_HEADERS，即它抓不到真实 Excel 会话时用的兜底集合。
// 它另有一份 _ALLOWED_CAPTURED_HEADERS（真实会话里会出现的全集），比这里多 5 个：
// x-openai-internal-basispoints-browser-{name,ua-brands,ua-mobile,ua-platform} 和
// x-stainless-runtime-version。**刻意不补**：没有任何参考实现公布过它们的真实取值，
// 而 ua-brands 这种结构化串猜错了比缺失更显眼。cpa-plugin 只发 13 个、UA 还是
// "oai-basispoints/<ver>"，比这里更容易认。（2026-09-29 核对四家源码）
var openAIBasisPointsClientProfile = map[string]string{
	"X-Basispoints-Auth-Mode":                             "chatgpt",
	"X-OpenAI-Internal-Basispoints-Client-Agent-Profile":  "excel",
	"X-OpenAI-Internal-Basispoints-Client-Editor":         "excel",
	"X-OpenAI-Internal-Basispoints-Client-Host":           "office",
	"X-OpenAI-Internal-Basispoints-Client-Platform":       "excel",
	"X-OpenAI-Internal-Basispoints-Client-Platform-Class": "PC",
	"X-OpenAI-Internal-Basispoints-Client-Product":        "basispoints-excel-plugin",
	"X-OpenAI-Internal-Basispoints-Client-Runtime":        "desktop",
	"X-OpenAI-Internal-Basispoints-Office-Host":           "Excel",
	"X-OpenAI-Internal-Basispoints-Office-Platform":       "PC",
	"X-Stainless-Arch":                                    "unknown",
	"X-Stainless-Lang":                                    "js",
	"X-Stainless-OS":                                      "Unknown",
	"X-Stainless-Package-Version":                         "6.31.0",
	"X-Stainless-Retry-Count":                             "0",
	"X-Stainless-Runtime":                                 "browser:chrome",
}

// openAIBasisPointsNativeError：这条请求该走原 Codex 路径。reason 是固定标签，不含客户端内容。
type openAIBasisPointsNativeError struct{ reason string }

func (e *openAIBasisPointsNativeError) Error() string { return "basispoints native route: " + e.reason }

func bpsNative(reason string) error { return &openAIBasisPointsNativeError{reason: reason} }

// openAIBasisPointsNativeReason 报告 err 是否为「落回原路径」信号及其原因。
func openAIBasisPointsNativeReason(err error) (string, bool) {
	var native *openAIBasisPointsNativeError
	if errors.As(err, &native) {
		return native.reason, true
	}
	return "", false
}

// openAIBasisPointsFallbackReasonKeepsNativeRoute 报告这个落回原因是否仍然放行到原 Codex 路径。
//
// 开了 Basis Points 直通的账号，落回 Codex 就意味着同一个 API key 上满血和降智的回答混在一起，
// 用户明确要求宁可报错也不要掺杂，所以除了下面这一个原因，其余一律做成客户端可见的终态错误。
//
// client_restriction 是本站自己的客户端限制（detectCodexClientRestriction），原路径会写它自己
// 那条规范的拒绝文案；在这里拦下来只会把真实原因换成一条 BPS 的错误码。
func openAIBasisPointsFallbackReasonKeepsNativeRoute(reason string) bool {
	return reason == "client_restriction"
}

// openAIBasisPointsReasonIsAccountFault：这几条**不是**「BPS 承载不了这个请求形态」，而是这个
// 账号/代理本身发不出请求，所以不豁免罚分。口径与紧挨着的几行对齐 —— resolveCredentialAccount 失败、
// GetAccessToken 失败、requireOpenAIProxyBinding 失败返回的都是裸 error（照旧罚分）；把「代理死了」
// 和「这个号连 chatgpt_account_id 都没有」豁免掉，等于让调度器一直把请求塞给一个 100% 发不出去的
// 账号，而硬报错口径下它又不会落回 Codex —— 命中它的每一发都是客户端可见的失败，且没有自动恢复。
func openAIBasisPointsReasonIsAccountFault(reason string) bool {
	switch reason {
	case "transport_error", "image_upload_transport", "missing_token", "missing_account_id":
		return true
	default:
		return false
	}
}

// openAIBasisPointsUnavailableError 把落回信号变成终态错误。
//
// 两条性质是刻意的：①不是 UpstreamFailoverError，handler 不会换号 —— 换到没开开关的账号就又走回
// Codex 路径，掺杂原封不动地回来了；②包着 ErrOpenAIRawRelayNotAccountFault，
// ReportOpenAIAccountScheduleResult 不会因此拖低这个账号的调度分 —— BPS 承载不了某个请求形态
// 不是账号的错。②对 openAIBasisPointsReasonIsAccountFault 那几条不成立。
func openAIBasisPointsUnavailableError(reason string) error {
	if openAIBasisPointsReasonIsAccountFault(reason) {
		return fmt.Errorf("basispoints unavailable (%s)", reason)
	}
	return fmt.Errorf("basispoints unavailable (%s): %w", reason, ErrOpenAIRawRelayNotAccountFault)
}

// markOpenAIBasisPointsNotAccountFault 给 BPS 这一轮的任何失败补上「不是账号的错」这个哨兵。
//
// 上面那个构造器只覆盖本层判定的落回。上游接受之后再失败的那一族走的是共用处理器，它在
// openai_gateway_response_handling.go 里有 9 处返回**裸 error**（`upstream response failed: …`、
// `non-streaming openai protocol error: …`、读流中断等），一条都不是 *UpstreamFailoverError，
// 于是 handler 的 `!errors.Is(err, ErrOpenAIRawRelayNotAccountFault)` 成立、去罚这个账号的调度分。
// 而并发打出来的 429 与 usage-policy 403 恰好是「首输出之后回 response.failed」这个形态，
// 正落在这里：BPS 通道被限流跟 Codex 后端的账号健康没关系，罚分只会让调度器更倾向挑没开
// 开关的账号，把掺杂延迟一步放回来。
//
// 哨兵**追加在尾部**，不是前缀包裹：handler 的 openAIForwardErrorAlreadyCommunicated 按
// "upstream response failed:" / "non-streaming openai protocol error:" 前缀判断响应是否已经写给
// 客户端，前缀被顶掉就会让 ensureForwardErrorResponse 在已写出的 SSE 尾部再追一条 502。
func markOpenAIBasisPointsNotAccountFault(err error) error {
	if err == nil || errors.Is(err, ErrOpenAIRawRelayNotAccountFault) {
		return err
	}
	return fmt.Errorf("%w [%w]", err, ErrOpenAIRawRelayNotAccountFault)
}

// openAIBasisPointsUnavailableStatus 把落回原因映射成给客户端的 HTTP 状态码。
//
// 上游自己回的 4xx / 5xx 原样透出去：403（usage policy 封掉这个号的 BPS 通道，永久）和
// 429（并发打出来的限流，瞬时）在客户端侧必须可区分，全塌成 502 之后两者一样，而且客户端
// 对 5xx 的重试通常比对带 Retry-After 的 429 更激进 —— 正好把封号主诱因又捶一遍。
// 本层自己判定的原因（形态承载不了、Fast 档、配置缺失）仍是 502：确实是网关侧拒绝。
func openAIBasisPointsUnavailableStatus(reason string) int {
	// 只认 HTTP 拒绝那一族的前缀。**不能顺手剥 "stream_"**：流内 reason 是
	// openAIBasisPointsRejectReason("stream", code) 拼的，code 来自上游 error.code，
	// 上游给个 "status_503" 就能决定我们回客户端的状态码 —— 那是上游可控、客户端不可控。
	raw, ok := strings.CutPrefix(reason, "status_")
	if !ok {
		return http.StatusBadGateway
	}
	code, err := strconv.Atoi(strings.SplitN(raw, "_", 2)[0])
	if err != nil || code < 400 || code > 599 {
		return http.StatusBadGateway
	}
	switch code {
	case http.StatusUnauthorized, http.StatusProxyAuthRequired:
		// 客户端自己的 key 没问题，是本站这个账号打 BPS 被拒。透 401 出去会让
		// Codex CLI / OpenAI SDK 判成「凭据失效」去清 token、要求重新登录。
		return http.StatusBadGateway
	}
	return code
}

// writeOpenAIBasisPointsUnavailable 在首输出之前把终态错误写给客户端并标记响应已提交，
// 免得 handler 再盖一层无信息量的 502 upstream_error。
func writeOpenAIBasisPointsUnavailable(c *gin.Context, accountID int64, reason string) error {
	if c != nil && !IsResponseCommitted(c) && !c.Writer.Written() {
		if retryAfter := getOpenAIBasisPointsRetryAfter(c); retryAfter != "" {
			c.Header("Retry-After", retryAfter)
		}
		c.JSON(openAIBasisPointsUnavailableStatus(reason), gin.H{"error": gin.H{
			"type": "basispoints_unavailable",
			"code": "basispoints_" + reason,
			"message": "Basis Points cannot serve this request (" + reason + "). " +
				"This account has Basis Points passthrough enabled, so it does not fall back to the Codex route.",
		}})
		MarkResponseCommitted(c)
	}
	logger.LegacyPrintf("service.openai_gateway",
		"[Basispoints] account=%d route=error reason=%s", accountID, reason)
	return openAIBasisPointsUnavailableError(reason)
}

// openAIBasisPointsFastPolicyReason 按原路径同一套 Fast 策略判这条请求能不能走 BPS。
//
// 返回 ("", nil) 表示没有 Fast 意图、可以走 BPS；返回 ("service_tier", nil) 表示策略最终要带
// **真 Fast 档**出站，而 BPS 收不了（422），调用方报错；返回非 nil error 表示策略自己要拒
// （OpenAIFastBlockedError 已按原路径口径写回客户端），调用方原样返回。
//
// **第二个返回值是策略处理过的 body，调用方必须用它替换原 body。** 早先这里只返回 reason、把
// patched 丢掉，于是 action=filter（策略要求静默剥掉 Fast、照常服务）在 HTTP 上被
// beginOpenAIBasisPoints 里那道 service_tier 网看成还带着 priority → 硬 502；而同一条策略遇到
// 客户端发的别名 "fast" 反而能成功（Forward 侧 patched 已归一），同策略两种结果。
func (s *OpenAIGatewayService) openAIBasisPointsFastPolicyReason(
	ctx context.Context, c *gin.Context, account *Account, body []byte,
) ([]byte, string, error) {
	// 策略白名单按**上游 slug**配。原路径评估时 body 里的 model 已经被 markPatchSet("model",
	// upstreamModel) 换成映射后的值（openai_gateway_passthrough.go:269-277 的注释说的就是这个），
	// 而这个分派点排在那之前，体里还是客户端请求的名字 —— 直接拿它去查会让按上游 slug 配的
	// Block 闸门不命中，等于开关照旧能绕过闸门。所以这里自己先做一次同样的映射。
	requestedModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	_, policyModel := resolveOpenAIForwardMappedModels(account, requestedModel, false)
	if policyModel == "" {
		policyModel = requestedModel
	}
	patched, err := s.applyOpenAIFastPolicyToBody(ctx, account, policyModel, body)
	if err != nil {
		var blocked *OpenAIFastBlockedError
		if errors.As(err, &blocked) {
			writeOpenAIFastPolicyBlockedResponse(c, blocked)
			// 策略拒绝是客户端的问题，不是账号的：不包这层的话 handler 会把它算成账号失败
			// 去拖调度分，还会在已经写完的 403 尾部再追一条 SSE。
			MarkResponseCommitted(c)
			return nil, "", fmt.Errorf("%w: %w", ErrOpenAIRawRelayNotAccountFault, err)
		}
		return nil, "", err
	}
	if isOpenAIBasisPointsFastTier(gjson.GetBytes(patched, "service_tier").String()) {
		return patched, "service_tier", nil
	}
	return patched, "", nil
}

// isOpenAIBasisPointsFastTier：这个 service_tier 值是不是 BPS 收不了的「真 Fast 档」。
//
// 只认 priority / ultrafast。normalizeOpenAIServiceTier 把 auto/default/flex/scale 也当合法值原样
// 留在体里（flex 反而是更慢更便宜的档），按「字段存在」判会把它们一起打成硬 502；而 prepare
// 的出站白名单根本不带 service_tier，拦住它们保护不了任何东西。
// 走 normalizedOpenAIServiceTierValue 而不是裸比字符串：客户端的别名 "fast" 也要算真 Fast 档，
// 否则这道判定对别名不成立、只是靠上游先归一才碰巧没漏。
func isOpenAIBasisPointsFastTier(raw string) bool {
	switch normalizedOpenAIServiceTierValue(raw) {
	case OpenAIFastTierPriority, OpenAIFastTierUltrafast:
		return true
	default:
		return false
	}
}

// buildOpenAIBasisPointsUnavailableWSEvent 给 WS 客户端的同一条终态错误（桥上没有 HTTP 状态码可用）。
func buildOpenAIBasisPointsUnavailableWSEvent(model, reason string) []byte {
	response := map[string]any{
		"object": "response", "status": "failed", "output": []any{},
		"error": map[string]any{
			"type": "basispoints_unavailable",
			"code": "basispoints_" + reason,
			"message": "Basis Points cannot serve this request (" + reason + "). " +
				"This account has Basis Points passthrough enabled, so it does not fall back to the Codex route.",
		},
	}
	if model = strings.TrimSpace(model); model != "" {
		response["model"] = model
	}
	body, err := json.Marshal(map[string]any{
		"type": "response.failed", "sequence_number": 0, "response": response,
	})
	if err != nil {
		return []byte(`{"type":"response.failed","sequence_number":0,"response":{"status":"failed","output":[],` +
			`"error":{"type":"basispoints_unavailable","code":"basispoints_unavailable",` +
			`"message":"Basis Points cannot serve this request."}}}`)
	}
	return body
}

// UsesOpenAIBasisPoints 报告账号的 /v1/responses 是否走 Basis Points。只对 ChatGPT 登录的 oauth 账号
// （Agent Identity 没有 bearer token，BPS 用不了）。
func (a *Account) UsesOpenAIBasisPoints() bool {
	return a != nil && a.Platform == PlatformOpenAI && a.Type == AccountTypeOAuth &&
		!a.IsOpenAIAgentIdentity() && a.getExtraBool(openAIBasisPointsExtraKey)
}

// openAIBasisPointsRouteReason 发请求前就能判定的「BPS 承载不了」原因；"" 表示可以走 BPS。
// 托管搜索只在客户端显式要求联网时算数：Codex CLI 默认 cached 声明（external_web_access=false）
// 没有联网意图，留在 BPS 并在提示里告诉模型怎么开。
func openAIBasisPointsRouteReason(body []byte) string {
	if !gjson.ValidBytes(body) {
		return ""
	}
	if gjson.GetBytes(body, "previous_response_id").String() != "" {
		return "history_reference"
	}
	if conversation := gjson.GetBytes(body, "conversation"); conversation.Exists() && conversation.Type != gjson.Null {
		return "history_reference"
	}
	if prompt := gjson.GetBytes(body, "prompt"); prompt.Exists() && prompt.Type != gjson.Null {
		return "prompt_template"
	}
	if mode := strings.TrimSpace(gjson.GetBytes(body, "reasoning.mode").String()); mode != "" && mode != "standard" {
		return "reasoning_configuration"
	}
	if reason := bpsToolChoiceRoute(gjson.GetBytes(body, "tool_choice")); reason != "" {
		return reason
	}
	if reason := bpsDeclaredToolsRoute(gjson.GetBytes(body, "tools")); reason != "" {
		return reason
	}
	if input := gjson.GetBytes(body, "input"); input.IsArray() {
		for _, item := range input.Array() {
			switch item.Get("type").String() {
			case "additional_tools":
				if reason := bpsDeclaredToolsRoute(item.Get("tools")); reason != "" {
					return reason
				}
			case "item_reference":
				return "history_reference"
			case "configuration_update":
				return "reasoning_configuration"
			}
			// **只扫 content / output 真会出站的那几类项。**
			//
			// translateHistory 对 reasoning / compaction / compaction_summary 是**整项重建**
			// （只留 type + 密文，content / summary 一概丢弃），对 compaction_trigger 与
			// additional_tools 是挪位 / 丢弃 —— 这些项的 content 里有什么根本不会出站。
			// 原来这里对每个 item 无条件扫，于是一个带 `content:[{type:"reasoning_text",…}]` 的
			// reasoning 项（本仓库 apicompat 的 fixture 就是这个形状，`/v1/responses` 客户端回放
			// 历史时的常规形态）会撞上部件白名单、被判成 input_content 硬 502。
			// 失败在**出站之前**，一个字节都没发 ⇒ bps: lineage 自愈压根不触发 ⇒ 客户端每轮回放
			// 同一批历史 ⇒ 这个会话在开着开关的账号上**每轮都 502、永不自愈**。与
			// encrypted_content 部件、compaction 项是同一类的第三个实例。
			// **字段也要按项分**，不是两个字段都扫：item 级白名单只把 message 的 `content` 和
			// 工具结果的 `output` 送出站，反过来扫（工具结果的 content / message 的 output）就是
			// 同一个缺陷降到字段级 —— 那个字段一个字节都不会发，却能把整条请求判死。
			switch item.Get("type").String() {
			case "message", "":
				if reason := bpsContentRoute(item.Get("content")); reason != "" {
					return reason
				}
			case "function_call_output", "custom_tool_call_output":
				if reason := bpsContentRoute(item.Get("output")); reason != "" {
					return reason
				}
			}
		}
	}
	for _, path := range []string{"text.format.type", "response_format.type"} {
		if kind := strings.TrimSpace(gjson.GetBytes(body, path).String()); kind != "" && kind != "text" {
			return "output_format"
		}
	}
	return ""
}

func bpsToolChoiceRoute(choice gjson.Result) string {
	switch choice.Type {
	case gjson.String:
		switch strings.TrimSpace(choice.String()) {
		case "", "auto", "none":
			return ""
		}
		return "tool_choice"
	case gjson.JSON:
		kind := strings.TrimSpace(choice.Get("type").String())
		switch {
		case strings.HasPrefix(kind, "web_search"):
			return "web_search"
		case kind == "image_generation":
			return "image_generation"
		}
		return "tool_choice"
	}
	return ""
}

func bpsDeclaredToolsRoute(tools gjson.Result) string {
	for _, tool := range tools.Array() {
		kind := strings.TrimSpace(tool.Get("type").String())
		switch {
		case kind == "namespace":
			if reason := bpsDeclaredToolsRoute(tool.Get("tools")); reason != "" {
				return reason
			}
		case strings.HasPrefix(kind, "web_search"):
			if tool.Get("external_web_access").Type != gjson.False {
				return "web_search"
			}
		case kind == "image_generation":
			return "image_generation"
		case isOpenAIBasisPointsHostedTool(kind):
			// mcp / file_search / code_interpreter 之类只有原生后端才跑得了，不能静默丢掉。
			return "hosted_tool"
		}
	}
	return ""
}

func bpsContentRoute(content gjson.Result) string {
	if !content.IsArray() {
		return ""
	}
	for _, part := range content.Array() {
		switch part.Get("type").String() {
		case "input_text", "output_text", "text":
			// **正文必须是字符串。** rewriteContent 用 bpsText 重建这三类部件，而 bpsText 对非字符串
			// 返回 ""：模型看到一个空文本部件，正文一个字节都不出站，客户端侧零信号、日志里零读数 ——
			// 整个 switch 里只有这一格会静默吞。最现实的生产者是 Assistants API v2 的消息形状
			// （`{"type":"text","text":{"value":"…","annotations":[]}}`），而 `text` 这个类型本来
			// 就是为「确实有客户端这么发」才放过的。发请求前判死，与这个文件的口径一致；刻意**不**去
			// 读 text.value 兜底 —— 那是替客户端猜格式。
			// 缺 text / 显式 null 都不判死：那种部件本来就没有正文可丢，静默的只有「有正文但不是
			// 字符串」这一格。gjson 对显式 null 返回 Exists()==true + Type==Null，所以 Null 要单列
			// —— 少了它，`"text":null`（Jackson / Newtonsoft 的默认序列化）这种零内容的写法会被判死，
			// 而它在回放历史里 ⇒ 每轮都死，正是这道闸门要避免的那一类。
			if v := part.Get("text"); v.Exists() && v.Type != gjson.Null && v.Type != gjson.String {
				return "input_content"
			}
		case "refusal":
			if v := part.Get("refusal"); v.Exists() && v.Type != gjson.Null && v.Type != gjson.String {
				return "input_content"
			}
		// Codex 多智能体 v2 历史里的密文部件：rewriteContent 会换成一句明文说明（保留位置），
		// 所以这道事前闸门也得放它过 —— 少这一行整条请求在进 bridge 之前就被判死，替换代码是死代码。
		case "encrypted_content":
		case "input_image":
			url := strings.TrimSpace(part.Get("image_url").String())
			switch {
			case url == "" && part.Get("file_id").String() != "":
			case strings.HasPrefix(url, "https://"), strings.HasPrefix(url, "data:"):
			default:
				return "image_input"
			}
		default:
			return "input_content"
		}
	}
	return ""
}

func openAIBasisPointsHeaders(token, accountID string) http.Header {
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+token)
	headers.Set("ChatGPT-Account-ID", accountID)
	headers.Set("X-OpenAI-Account-ID", accountID)
	if userID := openAIBasisPointsAccountUserID(token); userID != "" {
		headers.Set("X-OpenAI-Account-User-ID", userID)
	}
	headers.Set("Content-Type", "application/json")
	headers.Set("Accept", "text/event-stream")
	headers.Set("Accept-Encoding", "identity")
	headers.Set("Origin", "https://bps.openai.com")
	headers.Set("User-Agent", openAIBasisPointsUserAgent)
	for key, value := range openAIBasisPointsClientProfile {
		headers.Set(key, value)
	}
	return headers
}

// openAIBasisPointsAccountUserID 从 ChatGPT access token（JWT）的 auth 声明里取 chatgpt_account_user_id，
// 不校验签名（只是复述 token 自己带的值）。
func openAIBasisPointsAccountUserID(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(gjson.GetBytes(payload, `https://api\.openai\.com/auth.chatgpt_account_user_id`).String())
}

// openAIBasisPointsContextKey：本次响应来自 BPS。流内终态错误不改账号状态——BPS 的限流 / 鉴权与
// Codex 后端的账号状态不是一回事。每次 Forward / 桥的每一轮开头清掉，failover 换号后不残留。
const openAIBasisPointsContextKey = "openai_basispoints"

func isOpenAIBasisPointsResponse(c *gin.Context) bool {
	return c != nil && c.GetBool(openAIBasisPointsContextKey)
}

func markOpenAIBasisPointsResponse(c *gin.Context, on bool) {
	if c != nil {
		c.Set(openAIBasisPointsContextKey, on)
		// Retry-After 跟着一起复位，生命周期与这个键对齐：否则 WS 的多轮会话里
		// 上一轮上游给的 Retry-After 会被下一轮的错误响应带出去。
		c.Set(openAIBasisPointsRetryAfterContextKey, "")
	}
}

// openAIBasisPointsRetryAfterContextKey：上游 429 的 Retry-After，透给客户端用。
const openAIBasisPointsRetryAfterContextKey = "openai_basispoints_retry_after"

func setOpenAIBasisPointsRetryAfter(c *gin.Context, value string) {
	if c == nil {
		return
	}
	// 只透「delay-seconds」那种纯数字形态。上游的头值不做校验就转发的话，客户端 SDK 各自
	// 退化处理一个解不出来的值（有的当 0 立刻重试）；HTTP-date 形态本层也没有必要支持。
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 10 {
		return
	}
	// ParseUint 而不是 Atoi：Atoi 放行 "-100" 与 "+7"，两者都不是 RFC 9110 的 delay-seconds
	// （`1*DIGIT`），正好是这段注释要防的那种「SDK 解不出来就立刻重试」。
	if _, err := strconv.ParseUint(value, 10, 32); err != nil {
		return
	}
	c.Set(openAIBasisPointsRetryAfterContextKey, value)
}

func getOpenAIBasisPointsRetryAfter(c *gin.Context) string {
	if c == nil {
		return ""
	}
	return c.GetString(openAIBasisPointsRetryAfterContextKey)
}

// openAIBasisPointsAttempt：上游已接受（2xx 且首个事件不是错误）的 BPS 请求，Body 已换成翻译后的流。
type openAIBasisPointsAttempt struct {
	resp           *http.Response
	requestedModel string
	billingModel   string
	upstreamModel  string
	// effort 是客户端 / 模型后缀 / 分组策略推出的档位经 BPS 归一后的值；客户端没给就是 nil，与原路径同口径。
	effort *string
	// acceptedAt 是 peek 确认上游接受、即将把第一个事件写给客户端的时刻。流处理器的
	// first-output 预算要从这里算：peek 本身可能已经等了很久（BPS 高 effort 思考几十秒才吐
	// 第一条），从发请求时刻算的话预算早被吃光，定时器会被设成 1ns 直接判超时 —— 那条路还会
	// 调 HandleStreamTimeout 改账号状态，而 isOpenAIBasisPointsResponse 的闸门不在那上面。
	acceptedAt time.Time
	// requestStart 是出站请求发出的时刻。**TTFT 必须从这里算，不能从 acceptedAt 算**：
	// peek 一直读到「第一个会到客户端的事件」为止，所以流处理器起跑时那条事件已经在缓冲里，
	// 以 acceptedAt 为原点量出来的 firstTokenMs 恒等于 ~0 —— 而它有三个消费者
	// （usage_logs.first_token_ms、ops 的 TTFT 读数、scheduler.ReportResult 的延迟分），
	// 等于 BPS 账号在延迟维度上永远满分、真慢的号永远不被降权，而面板上 0 ms 看着还很好。
	// 预算仍然用 acceptedAt（见上），两件事解耦。
	requestStart time.Time
}

// offsetOpenAIBasisPointsFirstTokenMs 把 peek 吃掉的那段时间补回 TTFT。nil 原样返回（没量到就是没量到）。
func offsetOpenAIBasisPointsFirstTokenMs(firstTokenMs *int, peeked time.Duration) *int {
	if firstTokenMs == nil || peeked <= 0 {
		return firstTokenMs
	}
	adjusted := *firstTokenMs + int(peeked.Milliseconds())
	return &adjusted
}

// openAIBasisPointsKeepaliveInterval：保活周期必须显著小于网关自己的 stream_data_interval_timeout。
// 常量 30s 配上 keepalive/2 的 ticker，两次保活之间实际可达 45s；而该配置项的合法下限就是 30s，
// 运维填 30（合法）就会在 BPS 长思考时被网关自己的空闲计时先炸——那条路会改账号状态并给客户端
// 发 stream_timeout。默认 180 没事纯属侥幸，所以这里按配置取小。
func (s *OpenAIGatewayService) openAIBasisPointsKeepaliveInterval() time.Duration {
	if s.cfg == nil || s.cfg.Gateway.StreamDataIntervalTimeout <= 0 {
		return openAIBasisPointsKeepalive
	}
	// 实际间隔上限是 1.5×keepalive（ticker 走 keepalive/2），留到空闲上限的一半以内。
	if budget := time.Duration(s.cfg.Gateway.StreamDataIntervalTimeout) * time.Second / 3; budget < openAIBasisPointsKeepalive {
		return budget
	}
	return openAIBasisPointsKeepalive
}

// openAIBasisPointsUpstreamSilenceBudget：流内的上游空闲上限。
//
// 取 2×stream_data_interval_timeout：我们补的保活把网关自己那道闸门解除了武装（理由见
// openAIBasisPointsUpstreamSilence），这里是唯一还在的空闲上界，所以要顺着运维填的那个值走，
// 不能写死。翻倍是因为 BPS 高 effort 的真实沉默确实比 Codex 长，直接等于配置值会把「慢但会成功」
// 的请求判死 —— 在硬报错口径下那就是一条客户端可见的失败。
// 下限对齐 peek 的等待上限（接受之前都愿意等 3 分钟，接受之后没理由更短）；上限仍是那个兜底值。
func (s *OpenAIGatewayService) openAIBasisPointsUpstreamSilenceBudget() time.Duration {
	if s == nil || s.cfg == nil || s.cfg.Gateway.StreamDataIntervalTimeout <= 0 {
		return openAIBasisPointsUpstreamSilence
	}
	budget := 2 * time.Duration(s.cfg.Gateway.StreamDataIntervalTimeout) * time.Second
	if budget < openAIBasisPointsPeekSilence {
		budget = openAIBasisPointsPeekSilence
	}
	// 上限钳位放在下限之后，所以谁把 openAIBasisPointsUpstreamSilence 调成 0（测试里它是变量）
	// 就会把 budget 也压成 0，stream() 的 `silence <= 0` 兜底再赋 0，于是"已沉默 > 0"恒真、秒断流。
	// 只在上限本身是正数时才钳。
	if openAIBasisPointsUpstreamSilence > 0 && budget > openAIBasisPointsUpstreamSilence {
		budget = openAIBasisPointsUpstreamSilence
	}
	return budget
}

// beginOpenAIBasisPoints 把请求送到 BPS 并确认上游接受；token 为空时自己取。返回哨兵错误表示这条请求
// 该走原路径——判定不过、上游拒绝（任何非 2xx）、传输错误、流的首个事件就是错误都算——此时没有写过
// 客户端、没有改过账号状态，原路径接着用同一个账号服务。代理绑定缺失按原路径同一口径直接失败（fail-closed）。
func (s *OpenAIGatewayService) beginOpenAIBasisPoints(ctx context.Context, c *gin.Context, account *Account, body []byte, token string) (*openAIBasisPointsAttempt, error) {
	markOpenAIBasisPointsResponse(c, false)
	if reason := openAIBasisPointsRouteReason(body); reason != "" {
		return nil, bpsNative(reason)
	}
	// 真 Fast 档在这里判死，因为这是 HTTP 与 WS 两条路唯一的共用入口。
	// Forward 侧那次 openAIBasisPointsFastPolicyReason 仍要保留（它还负责把 Block 写回客户端），
	// 但 WS 入口（openai_ws_forwarder_ingress.go）是自己评估策略再写回 body 的 —— 只靠 Forward
	// 那一处，force-Fast 分组在 WS 上就会因为出站白名单不含 service_tier 而被**静默降级**成标准档，
	// 正是这套改动要消灭的东西。
	//
	// 前提：进到这里的 body 必须是**策略处理过**的（HTTP 走 openAIBasisPointsFastPolicyReason 的
	// 第一个返回值，WS 走 ingress 的 normalized）。拿未处理的裸体进来会把 action=filter 打成硬报错。
	if isOpenAIBasisPointsFastTier(gjson.GetBytes(body, "service_tier").String()) {
		return nil, bpsNative("service_tier")
	}
	// 本站自己的拒绝交给原路径按既有口径写回。
	if restriction := s.detectCodexClientRestriction(c, account, body); restriction.Enabled && !restriction.Matched {
		return nil, bpsNative("client_restriction")
	}
	// 与原路径同一套会话不变量：剥掉本会话已被上游判失效的加密推理（否则每一轮都被 BPS 拒、每一轮都落回）；
	// prompt_cache_key 按账号命名空间改写（failover 之后同一把 key 不能带到另一份 OAuth 凭据上）。
	//
	// 会话键必须走 openAIWSLineageSessionHashFromContext（不是裸 GenerateSessionHash）：WS 入口把
	// 会话哈希写在 ctx 里，读写两侧只要有一侧按体派生就永远对不上，记了也白记。
	// entryBody 是**剥离前**的体，下面记 lineage 时按它收摘要（与原路径的 lineageEntryBody 同口径）。
	//
	// HasAny 那道全局快速探测必须排在算哈希**之前**：GenerateSessionHash 不是纯函数，HTTP 路径上
	// attachOpenAILegacySessionHashToGin 会替换 c.Request 的 context，而这里算哈希用的是策略处理过
	// 的 body（legacy ingress 归一 / Fast filter 剥过），与 handler 早先按客户端原体算的那次可以不同，
	// 无条件调用等于每发都白付一次改写。
	entryBody := body
	if stateStore := s.getOpenAIWSStateStore(); stateStore != nil && stateStore.HasAnySessionInvalidEncryptedContent() {
		groupID := getOpenAIGroupIDFromContext(c)
		sessionHash := s.openAIWSLineageSessionHashFromContext(c, body)
		// 两把键的并集：bps: 是本层自己记的（BPS 解不开，但在 Codex 上可能仍然有效），
		// 裸哈希是 Codex 路径记的（那边拒过就是真失效，对 BPS 一样死）。
		invalid := stateStore.GetSessionInvalidEncryptedContentDigests(groupID, sessionHash)
		for digest := range stateStore.GetSessionInvalidEncryptedContentDigests(groupID, openAIBasisPointsLineageSessionKey(sessionHash)) {
			if invalid == nil {
				invalid = map[string]struct{}{}
			}
			invalid[digest] = struct{}{}
		}
		if len(invalid) > 0 {
			if stripped, count := s.stripSessionInvalidEncryptedContentLogged(body, invalid, "basispoints_invalid_encrypted_lineage_strip", account.ID, 0); count > 0 {
				body = stripped
			}
		}
	}
	apiKeyID := getAPIKeyIDFromContext(c)
	if raw := gjson.GetBytes(body, "prompt_cache_key").String(); strings.TrimSpace(raw) != "" {
		if scoped := scopeCodexAccountIdentityValue(account, apiKeyID, "prompt-cache", raw); scoped != raw {
			var err error
			if body, err = sjson.SetBytes(body, "prompt_cache_key", scoped); err != nil {
				return nil, fmt.Errorf("scope basispoints prompt cache key: %w", err)
			}
		}
	}
	requestedModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	billingModel, upstreamModel := resolveOpenAIForwardMappedModels(account, requestedModel, false)
	effort := extractOpenAIReasoningEffortFromBody(body, upstreamModel, billingModel, requestedModel)
	effort = ApplyThinkingEnabledFallback(effort, body, requestedModel)
	if effort != nil {
		// 模型后缀 / 分组策略推出的档位写回体里，prepare 只认 reasoning.effort。
		var err error
		if body, err = sjson.SetBytes(body, "reasoning.effort", *effort); err != nil {
			return nil, fmt.Errorf("set basispoints reasoning effort: %w", err)
		}
	}
	credAccount, err := resolveCredentialAccount(ctx, s.accountRepo, account)
	if err != nil {
		return nil, err
	}
	accountID := credAccount.GetChatGPTAccountID()
	if accountID == "" {
		return nil, bpsNative("missing_account_id")
	}
	if token == "" {
		if token, _, err = s.GetAccessToken(ctx, credAccount); err != nil {
			return nil, err
		}
	}
	if token == "" {
		// 母号是 Agent Identity 的影子账号：没有 bearer token。
		return nil, bpsNative("missing_token")
	}
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	if err := requireOpenAIProxyBinding(account, proxyURL); err != nil {
		return nil, err
	}
	headers := openAIBasisPointsHeaders(token, accountID)
	scope := fmt.Sprintf("%d|%d", account.ID, apiKeyID)
	bridge := newOpenAIBasisPointsBridge(scope, openAIBasisPointsReplay, s.openAIBasisPointsUploader(ctx, c, account, proxyURL, headers, token, accountID))
	outBody, err := bridge.prepare(body, upstreamModel)
	if err != nil {
		return nil, err
	}
	// 上游请求不随客户端断开而取消：与原路径一样把流读完，usage 才记得上。
	upstreamCtx, releaseUpstreamCtx := detachUpstreamContext(ctx)
	defer releaseUpstreamCtx()
	req, err := http.NewRequestWithContext(WithHTTPUpstreamProfile(upstreamCtx, HTTPUpstreamProfileOpenAI), http.MethodPost, openAIBasisPointsResponsesURL, bytes.NewReader(outBody))
	if err != nil {
		return nil, err
	}
	req.Header = headers.Clone()
	upstreamStart := time.Now()
	// 绕过插件 RoundTrip：那是给 Codex 请求形态用的。
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		s.appendOpenAIBasisPointsUpstreamError(c, account, 0, "", sanitizeUpstreamErrorMessage(err.Error()), token, accountID)
		// 代理/网络是死的：与原路径同一口径把账号临时摘出池子（handleOpenAIUpstreamTransportError
		// :153 的那一步），否则硬报错口径下调度器会一直把请求塞给一个 100% 发不出去的账号，
		// 且没有任何自动恢复路径（罚分对 oauth 只是软权重，apikey 那道健康熔断走不到）。
		// isClientCanceledTransportError 在这条路上**基本是条便宜的保险**：请求用
		// detachUpstreamContext 建，客户端断开不会让 Do 返回 context.Canceled；照原路径写着，
		// 免得哪天换成不 detach 的形态时漏掉。
		if !isClientCanceledTransportError(ctx, err) && classifyUpstreamTransportError(err).Persistent {
			s.tempUnscheduleOpenAITransportError(ctx, account, sanitizeUpstreamErrorMessage(err.Error()))
		}
		return nil, bpsNative("transport_error")
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, s.rejectOpenAIBasisPointsResponse(ctx, c, account, resp, entryBody,
			[]string{upstreamModel, requestedModel}, token, accountID)
	}
	// 读到处理器会立刻写给客户端的第一个事件为止：BPS 把限流 / 模型不可用这类拒绝也会放在 HTTP 200 的
	// error / response.failed 里，在此之前客户端什么都没收到（连响应头都没写），可以整条落回原路径。
	rest, failure, err := peekOpenAIBasisPointsFirstOutput(resp.Body, openAIBasisPointsPeekSilence)
	if err != nil {
		_ = resp.Body.Close()
		reason := "stream_eof"
		if errors.Is(err, errOpenAIBasisPointsPeekTimeout) {
			reason = "upstream_silent"
		}
		s.appendOpenAIBasisPointsUpstreamError(c, account, resp.StatusCode, resp.Header.Get("x-request-id"),
			sanitizeUpstreamErrorMessage(err.Error()), token, accountID)
		return nil, bpsNative(reason)
	}
	if failure != nil {
		_ = rest.Close()
		code, message := openAIBasisPointsErrorDetail(failure)
		s.appendOpenAIBasisPointsUpstreamError(c, account, resp.StatusCode, resp.Header.Get("x-request-id"), message, token, accountID)
		// BPS 把一部分拒绝放在 HTTP 200 的 error 事件里，密文失效与 usage policy 封通道都可能走这里
		// —— 后者其实比 HTTP 403 更常见（见下面那段注释），所以两个出口都要判。
		s.noteOpenAIBasisPointsInvalidEncryptedLineage(c, account, failure, entryBody, 0)
		s.disableOpenAIBasisPointsAccountOnUsagePolicyBlock(ctx, account, code, message,
			[]string{upstreamModel, requestedModel}, token, accountID)
		// 403（usage policy 永久封掉这个号的 BPS 通道）与 429（并发打出来的瞬时限流）在客户端侧
		// 必须可区分 —— 全塌成 502 之后两者一样，而客户端对 5xx 的重试通常比对带 Retry-After 的
		// 429 更激进，正好把封号主诱因又捶一遍。
		//
		// 首输出**之后**同一条上游信号走的就是 `status_<code>_stream`（openai_gateway_forward.go），
		// 而 BPS 更常见的是把限流 / 模型不可用放在 HTTP 200 的 error 事件里、由 peek 抓到，
		// 两侧不同构等于把更常见的那一半的信息丢了。所以这里用同一套。
		//
		// 用 openAIStreamFailureStatus 而不是剥 code 字符串：它的**值域是封闭的**
		// {401,403,429,503,529,502}，不存在「上游给个 status_503 就能决定我们回客户端的状态码」；
		// 401/407 另有 openAIBasisPointsUnavailableStatus 那道 502 闸门兜着。
		reason := openAIBasisPointsRejectReason("stream", code)
		// **专用标签不被状态码覆写**：model_access_changed 的消息常含 "permission" / "access denied"，
		// openAIStreamFailureStatus 会给它 403，无条件覆写就把这个专用码换成泛化的 status_403_stream
		// （openAIBasisPointsRejectReason 对它的特判白判了，而 403 在这里的语义是"通道被封"，
		// 跟"这个号没这个模型了"是两件事）。它自己映射到 502。
		if status := openAIStreamFailureStatus(failure, message); status != http.StatusBadGateway &&
			!openAIBasisPointsReasonIsDedicatedLabel(reason) {
			reason = fmt.Sprintf("status_%d_stream", status)
			setOpenAIBasisPointsRetryAfter(c, resp.Header.Get("Retry-After"))
		}
		return nil, bpsNative(reason)
	}
	resp.Body = rest
	SetOpsLatencyMs(c, OpsUpstreamLatencyMsKey, time.Since(upstreamStart).Milliseconds())
	if len(bridge.warnings) > 0 {
		logger.LegacyPrintf("service.openai_gateway", "[Basispoints] account=%d %s", account.ID, strings.Join(bridge.warnings, "; "))
	}
	resp.Body = bridge.stream(resp.Body, s.openAIBasisPointsKeepaliveInterval(), s.openAIBasisPointsUpstreamSilenceBudget())
	resp.ContentLength = -1
	resp.Header.Del("Content-Length")
	resp.Header.Set("Content-Type", "text/event-stream")
	markOpenAIBasisPointsResponse(c, true)
	SetOpsUpstreamModel(c, upstreamModel)
	SetActualOpenAIUpstreamEndpoint(c, openAIBasisPointsUpstreamEndpoint)
	if effort != nil {
		normalized := bridge.effort
		effort = &normalized
	}
	return &openAIBasisPointsAttempt{resp: resp, requestedModel: requestedModel, billingModel: billingModel,
		upstreamModel: upstreamModel, effort: effort, acceptedAt: time.Now(), requestStart: upstreamStart}, nil
}

// forwardOpenAIBasisPoints：HTTP /v1/responses 的 BPS 路径。上游接受后交给原路径的流 / 非流处理器，
// usage、served model、SSE→JSON 都与原路径同一套。
func (s *OpenAIGatewayService) forwardOpenAIBasisPoints(ctx context.Context, c *gin.Context, account *Account, body []byte) (_ *OpenAIForwardResult, bpsErr error) {
	startTime := time.Now()
	attempt, err := s.beginOpenAIBasisPoints(ctx, c, account, body, "")
	if err != nil {
		return nil, err
	}
	// 哨兵从**这里**往后收，不是从函数入口：begin 的非 native 错误（代理绑定缺失、
	// GetAccessToken 失败、凭据账号查不到）确实是这个账号自己的故障，Codex 路径就是按账号失败
	// 罚分的，BPS 不该把它豁免掉 —— 豁免了调度器会一直把请求塞给一个根本发不出去的账号。
	// begin 的 native 错误另有出路（分派点按 openAIBasisPointsNativeReason 转成
	// writeOpenAIBasisPointsUnavailable，那条自带哨兵）。WS 桥的 defer 注册点与这里对齐。
	//
	// 收在函数边界而不是每个 err 出口各写一遍：下面三条出口（零 token、usage 已到手、非流式）
	// 语义完全一样，而以后再加出口时最容易漏的就是这个哨兵。
	defer func() { bpsErr = markOpenAIBasisPointsNotAccountFault(bpsErr) }()
	resp := attempt.resp
	defer func() { _ = resp.Body.Close() }()
	effortValue := ""
	if attempt.effort != nil {
		effortValue = *attempt.effort
	}
	reqStream := gjson.GetBytes(body, "stream").Bool()
	var usage *OpenAIUsage
	var firstTokenMs *int
	responseID := ""
	result := func() *OpenAIForwardResult {
		if usage == nil {
			usage = &OpenAIUsage{}
		}
		return &OpenAIForwardResult{
			RequestID:                     resp.Header.Get("x-request-id"),
			UpstreamHeaders:               resp.Header,
			ResponseID:                    responseID,
			Usage:                         *usage,
			Model:                         attempt.requestedModel,
			BillingModel:                  attempt.billingModel,
			UpstreamModel:                 attempt.upstreamModel,
			UpstreamResponseModel:         observedUpstreamResponseModel(c),
			UpstreamResponseModelConflict: observedUpstreamResponseModelConflict(c),
			UpstreamResponseServiceTier:   observedUpstreamResponseServiceTier(c),
			UpstreamEndpoint:              openAIBasisPointsUpstreamEndpoint,
			ReasoningEffort:               attempt.effort,
			Stream:                        reqStream,
			Duration:                      time.Since(startTime),
			FirstTokenMs:                  firstTokenMs,
		}
	}
	if reqStream {
		// 用 acceptedAt 而不是 startTime：first-output 预算要从「第一个事件即将写给客户端」那一刻起算，
		// 理由见 openAIBasisPointsAttempt.acceptedAt。Duration（上面 result 里）仍按 startTime 记全程。
		streamResult, err := s.handleStreamingResponseWithReasoning(ctx, resp, c, account, attempt.acceptedAt, attempt.requestedModel, attempt.upstreamModel, effortValue)
		if streamResult != nil {
			usage, firstTokenMs, responseID = streamResult.usage, streamResult.firstTokenMs, strings.TrimSpace(streamResult.responseID)
			// 把 peek 吃掉的那段补回 TTFT：处理器是以 acceptedAt 为原点量的，而首输出事件在
			// acceptedAt 时已经躺在缓冲里，它量出来的恒等于 ~0。理由见 attempt.requestStart。
			firstTokenMs = offsetOpenAIBasisPointsFirstTokenMs(firstTokenMs, attempt.acceptedAt.Sub(attempt.requestStart))
		}
		if err != nil {
			// 判据是「有没有账可记」，不是「指针是否为 nil」：handleStreamingResponseWithReasoning
			// 里的 usage 是 `&OpenAIUsage{}`（openai_gateway_response_handling.go:166），
			// resultWithUsage() 原样带出，所以几乎所有流内失败都给一个非 nil 的空 usage。
			// 按 nil 判会让这里恒返回非 nil result，Forward 侧那道 failover 收口就成了死代码
			// —— 换号照旧发生，掺杂原封不动地回来。
			if !openAIUsageHasTokens(usage) {
				return nil, err
			}
			// 协议违规 / 上游死流这类失败发生在 usage 已经到手之后：额度已经消耗，照样记账。
			return result(), err
		}
	} else {
		nonStreamResult, err := s.handleNonStreamingResponse(ctx, resp, c, account, attempt.requestedModel, attempt.upstreamModel)
		if nonStreamResult != nil {
			usage, responseID = nonStreamResult.usage, strings.TrimSpace(nonStreamResult.responseID)
		}
		if err != nil {
			// 与流式分支同一条判据（openAIUsageHasTokens，不是指针判 nil）：这条路的失败是终态，
			// 不换号、不会在别处重记，所以 usage 到手就必须带出来记账。handleSSEToJSON 的终态
			// 失败出口现在会把解出来的 usage（有 token 时）一起带回来。
			if !openAIUsageHasTokens(usage) {
				return nil, err
			}
			return result(), err
		}
	}
	s.bindHTTPResponseAccount(ctx, c, account, responseID)
	return result(), nil
}

// rejectOpenAIBasisPointsResponse：非 2xx 一律落回原路径。体只进 ops 事件（消息经既有脱敏），不写客户端；
// 落回原因只带状态与上游错误码。entryBody 是出站前的请求体，只用来记密文 lineage。
func (s *OpenAIGatewayService) rejectOpenAIBasisPointsResponse(ctx context.Context, c *gin.Context, account *Account, resp *http.Response, entryBody []byte, clientEcho []string, secrets ...string) error {
	body := s.readUpstreamErrorBody(resp)
	_ = resp.Body.Close()
	setOpenAIBasisPointsRetryAfter(c, resp.Header.Get("Retry-After"))
	code, message := openAIBasisPointsErrorDetail(body)
	s.appendOpenAIBasisPointsUpstreamError(c, account, resp.StatusCode, resp.Header.Get("x-request-id"), message, secrets...)
	s.noteOpenAIBasisPointsInvalidEncryptedLineage(c, account, body, entryBody, 0)
	s.disableOpenAIBasisPointsAccountOnUsagePolicyBlock(ctx, account, code, message, clientEcho, secrets...)
	return bpsNative(openAIBasisPointsRejectReason(fmt.Sprintf("status_%d", resp.StatusCode), code))
}

// openAIBasisPointsUsagePolicyBlock 判定这条上游拒绝是不是「usage policy 封掉这个号的 BPS 通道」。
//
// **判据是签名，不是 HTTP 403。** 原来只看状态码，两头都错：
//   - **过宽，而且客户端可控。** 403 在这条通道上同时是「这个号没有这个模型」的回法（09-25 的模型表：
//     gpt-6-sol/luna/terra、gpt-5.5 全 403，astra 与 gpt-5.6-* 全 200），而本层**刻意不做模型白名单**
//     （那是会漂的上游权限，见约定文档）。于是一个分组同时开放 astra 和 luna（对 Codex 账号完全正常）
//     时，任何持本站 key 的客户端请求一次 luna 就永久停掉一个健康账号，循环 N 次停 N 个。
//     bps.openai.com 在 Cloudflare 后面，机房 IP 上的 WAF 403（HTML 体、解不出 message）同理。
//   - **同时又过窄。** 真正的封通道更常见的形态是 HTTP 200 + 流内 error / response.failed 帧
//     （见 peek 那一段的注释），只看状态码恰好漏掉它。
//
// 所以改成按签名判，并在所有拿得到这份报文的出口都调一次。`basispoints_model_access_changed`
// 天然落不进来（码与消息都不带 usage policy），与 20 行外那道专用标签守卫口径一致。
func openAIBasisPointsUsagePolicyBlock(code, message string, clientEcho ...string) bool {
	if strings.Contains(strings.ToLower(strings.TrimSpace(code)), "usage_policy") {
		return true
	}
	// **消息分支只认实测过的那一整句、而且必须是句首。** 这里判的是「永久停掉一个满血账号」这个
	// 不可逆动作，而载体是上游的一句自由文本 —— 收窄走了两步：
	//
	//   1. 先从 `Contains(msg, "usage policy")` 收到整句。那一步关掉的是**误触**：OpenAI 对
	//      reasoning 模型的逐请求内容审核拒绝（code=invalid_prompt）官方文案是 "Invalid prompt:
	//      your prompt was flagged as potentially violating our usage policy."，一句被拦下的
	//      prompt 就能停掉一个号；404 model_not_found 的 "See our usage policy for details."
	//      同理。两条都是**请求级**拒绝，跟这个号的通道被封是两件事。
	//   2. 但那一步没关掉**故意**：出站的 `model` 是客户端原样控制的字节（本层刻意不做模型白名单），
	//      而 OpenAI 的 model_not_found 文案会把模型名回显进 error.message —— 于是
	//      `{"model":"blocked by our usage policy"}` 一发停一个号，循环 N 次停 N 个
	//      （第十一轮两个审查者各自独立抓到这一条，都在 worktree 里端到端实证过）。
	//
	// 所以判据锚到**句首**，并且先把这一轮客户端可控的出站字节打掉。两层的分工要说准，别高估锚点：
	//   - 锚点挡的是**上游自己的句子里**的回显（`The model '…' does not exist…` 这种，回显做不成句首）。
	//   - 万一哪天上游换成**值打头**的语法（`'x' is not one of […]`），锚点就不挡了，挡的是打码那层
	//     （`[REDACTED]` 以 `[` 开头，不在任何 trim 集合里，客户端造不出「打码产生句首命中」）。
	//     所以刻意**不**把引号加进下面那个 trim 集合 —— 加了等于把值打头的形态放进锚点，白送一层。
	// 代价是上游改措辞就漏判真封号 —— 但漏判是**可见的反复报错**，误停是不可逆的，方向必须朝这边偏。
	msg := strings.ToLower(strings.TrimSpace(maskOpenAIBasisPointsSecrets(message, clientEcho...)))
	// BPS 会把状态码写进消息开头（实测 09-30：`{"message":"422: Invalid request body."}`），
	// 现场那条封号原话记的也是 `403 This request was blocked by our usage policy.` —— 所以句首
	// 允许一段状态码前缀。数字和冒号里塞不进客户端的字节，这一段不削弱上面那层。
	msg = strings.TrimLeft(msg, "0123456789")
	msg = strings.TrimLeft(msg, ": ")
	return strings.HasPrefix(msg, "this request was blocked by our usage policy")
}

// disableOpenAIBasisPointsAccountOnUsagePolicyBlock：usage policy 封掉这个号的 BPS 通道 ⇒ 停用账号。
// **用户 2026-09-29 拍板：报错 + 停用账号**（403 有冷却，接着打没有意义）。
//
// 用 SetError 而不是单独置 status=disabled：它一次做完三件事（status=error、写 error_message、
// schedulable=false），并带上调度器 outbox 与快照同步 —— 这是全仓"自动停用带原因"的既有原语
// （openai_team_linked_error.go / gateway_scheduling.go 都用它）。status 落的是 error 不是
// disabled，对调度是一回事（两者都 != active），但管理台上能看见原因。全仓没有任何按消息匹配的
// 自动恢复会覆盖这条原因，**所以它是永久的**，恢复是管理员的决定 —— 判据必须窄，见上面那个谓词。
//
// **刻意不关这个账号的 BPS 开关**（参考实现 openai_excel_bps.go:74-93 的 disableExcelBPSOn403
// 就是那么做的）：关了开关这个号会继续服务、但从此走 Codex = 降智，正好违背"开着开关不允许降智
// 请求"的口径。
// clientEcho 是这一轮客户端可控、且上游可能回显进错误消息的出站字节（模型名）。它**只**喂给判据，
// 不参与下面落库那句的打码 —— ops 事件与 error_message 上要能看见真实模型名才排得了障。
func (s *OpenAIGatewayService) disableOpenAIBasisPointsAccountOnUsagePolicyBlock(
	ctx context.Context, account *Account, code, message string, clientEcho []string, secrets ...string,
) {
	if s == nil || s.accountRepo == nil || account == nil {
		return
	}
	if !openAIBasisPointsUsagePolicyBlock(code, message, clientEcho...) {
		return
	}
	// 上游报错时请求 ctx 往往已被取消，落库要独立生命周期。
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openAIAccountStateUpdateTimeout)
	defer cancel()
	// 打码走与 ops 事件同一条：sanitizeUpstreamErrorMessage 只替 URL 里的敏感 query 参数，
	// **不打码 bearer token 也不打码 chatgpt_account_id**，而这条消息是要落进 accounts.error_message
	// 并在管理台上原样显示的。
	reason := "Basis Points usage policy block: " +
		sanitizeUpstreamErrorMessage(maskOpenAIBasisPointsSecrets(message, secrets...))
	if err := s.accountRepo.SetError(opCtx, account.ID, reason); err != nil {
		logger.LegacyPrintf("service.openai_gateway",
			"[Basispoints] account=%d disable_on_usage_policy failed: %v", account.ID, err)
		return
	}
	logger.LegacyPrintf("service.openai_gateway",
		"[Basispoints] account=%d route=disabled reason=usage_policy_block", account.ID)
}

// noteOpenAIBasisPointsInvalidEncryptedLineage：BPS 解不开别处铸的推理密文时按
// invalid_encrypted_content 拒掉整条请求。客户端每一轮都会原样回放同一批 blob，硬报错口径下
// 不记 lineage 就等于「这个会话从此每轮都失败」；记下来，下一轮进 begin 时上面那段剥离就会把
// 它们摘掉，一次失败之后自愈。
//
// 判据见 openAIBasisPointsRejectedInvalidEncryptedContent。
//
// 与参考实现（ranxi2001/sub2api 的 openai_excel_bps_encrypted.go:12-25）的真实区别只有一条：
// 它当场同路重试一次、对客户端零可见失败；这里刻意不发那第二次上游请求（BPS 通道并发打多了会被封，
// 见 sub2api-basispoints-route 的现场结论）。
//
// **命名空间是 bps: 前缀，不与 Codex 路径共享。** 「BPS 解不开」不等于「这个 blob 失效了」——
// 它在 Codex 后端仍然有效。分组里混着开/关开关的账号是用户允许的形态，共享一个键会让下一轮落到
// 没开开关的账号时，Codex 路径把**本来能用**的推理密文也剥掉：整个会话在 sticky TTL 内每轮从零
// 推理，而且没有任何客户端可见信号 —— 正好是「宁可失败也不要静默降质」的反面。
// 反方向仍然共享（读侧取两把键的并集）：Codex 拒过的 blob 对 BPS 确实也是死的。
func (s *OpenAIGatewayService) noteOpenAIBasisPointsInvalidEncryptedLineage(
	c *gin.Context, account *Account, raw, entryBody []byte, turn int,
) {
	if !openAIBasisPointsRejectedInvalidEncryptedContent(raw) {
		return
	}
	// 已知失效的摘要要先从候选池里减掉。entryBody 是**剥离前**的客户端原体（会话哈希两侧要对得上），
	// 而客户端每轮原样回放全部历史 ⇒ 曾被拒、已经不出站的那些 blob 每轮都还在里面。不减掉的话
	// filtered 永远非空（里面是那颗幽灵），真正在被拒的压缩密文永远进不了 lineage，下面那条
	// 「过滤清空就回退」形同虚设 —— 会话每轮硬错、永不自愈（第十二轮 blocker，repro：先拒一颗
	// Codex blob 让它进 lineage，之后压缩密文失效，第 3 轮起每轮都在重复标记那颗幽灵）。
	sessionHash := s.openAIWSLineageSessionHashFromContext(c, entryBody)
	groupID := getOpenAIGroupIDFromContext(c)
	known := s.sessionInvalidEncryptedContentDigests(groupID, sessionHash)
	for digest := range s.sessionInvalidEncryptedContentDigests(
		groupID, openAIBasisPointsLineageSessionKey(sessionHash)) {
		if known == nil {
			known = map[string]struct{}{}
		}
		known[digest] = struct{}{}
	}
	digests := openAIBasisPointsSuspectEncryptedDigests(entryBody, known)
	if len(digests) == 0 {
		return
	}
	s.markOpenAIWSInvalidEncryptedContentLineage(
		groupID,
		openAIBasisPointsLineageSessionKey(sessionHash),
		digests,
	)
	logOpenAIWSModeInfo("basispoints_invalid_encrypted_lineage_mark account_id=%d turn=%d digests=%d",
		account.ID, turn, len(digests))
}

// openAIBasisPointsRejectedInvalidEncryptedContent 判定这条上游失败是否「解不开推理密文」。
//
// 取码是**两个读法的并集**，它们交叉、谁都不包含谁，少一个那半边就是零覆盖而不是概率降低：
//   - openAIBasisPointsErrorDetail：error.code / response.error.code /
//     response.status_details.error.code / 裸 code —— peek 抓到的 response.failed 帧把码放在
//     response.error.code，只有它读得到。
//   - error.message 里的 JSON 信封（网关包一层时码在那儿）。这条**不能**用
//     extractUpstreamErrorCode 代替：它在 error.code 非空时就早返回、压根不解信封，而恰好
//     `{"error":{"code":"invalid_request_error","message":"{…invalid_encrypted_content…}"}}`
//     就是这个形状。反过来它比这里多的那点（截到最后一个 `}` 再解一次）是**死重**：实测 gjson
//     本来就穿过尾部垃圾（`{"error":{"code":"X"}} trailing {garbage` 直接取到 X），所以那次调用
//     已经删掉 —— 原来那条「三个读法、少一个就零覆盖」的注释对它是假的。
//
// **有码但都不是这一条 → 立刻判否，不许再看消息。** classifyOpenAIWSErrorEventFromRaw 自己不成立
// 这条性质（:748 的 switch 在 code 非空且不匹配时会穿到 strings.Contains(msg, …) 那两行），而消息是
// 上游可控的自由文本：让它决定「剥掉这个会话的推理密文」等于把一个静默降质开关交给对端，而被剥的
// blob 在 Codex 后端本来有效。消息兜底只在两个读法**全空**时生效（有些形态确实只有一句话）。
//
// 少任何一层，对应那半边的会话就是每轮硬报错 + 每轮白发一次 22.5K 提示词，而且不记就不打日志，
// 现场没有任何读数看得出来 —— 所以三个分支各有用例，见
// TestOpenAIBasisPointsRejectedInvalidEncryptedContentReadings。
func openAIBasisPointsRejectedInvalidEncryptedContent(raw []byte) bool {
	code, message := openAIBasisPointsErrorDetail(raw)
	nested := ""
	if inner := strings.TrimSpace(gjson.GetBytes(raw, "error.message").String()); strings.HasPrefix(inner, "{") {
		nested = strings.TrimSpace(gjson.Get(inner, "error.code").String())
	}
	sawCode := false
	for _, candidate := range []string{code, nested} {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		sawCode = true
		if reason, _ := classifyOpenAIWSErrorEventFromRaw(candidate, "", ""); reason == openAIWSFallbackReasonInvalidEncryptedContent {
			return true
		}
	}
	if sawCode {
		return false
	}
	reason, _ := classifyOpenAIWSErrorEventFromRaw("", "", message)
	return reason == openAIWSFallbackReasonInvalidEncryptedContent
}

// openAIBasisPointsSuspectEncryptedDigests 收这一轮里**可能是元凶**的密文摘要。
//
// 与通用的 collectOpenAIEncryptedContentDigestsRaw 的区别：**跳过 `compaction` 项**。这条路上两种
// 密文的来源是互斥可判的 —— `reasoning.encrypted_content` 只可能是 Codex 铸的（出站体发不了
// `include=[reasoning.encrypted_content]`，BPS 从不回推理密文），而 `compaction.encrypted_content`
// 只可能是 BPS 自己铸的（09-29 实测它解得开自己那颗）。所以同一轮里两种都在时，元凶一定是前者。
//
// 不跳的后果是**静默**的：一次 Codex blob 被拒会连带把有效的压缩摘要拉黑，下一轮
// sanitizeEncryptedReasoningInputItem 把 compaction 项**整项删除** ⇒ 模型丢掉压缩前的全部历史，
// 客户端侧零信号，日志里只有一行 stripped_items=2。
//
// `compaction_summary` 不跳：那是 Codex 侧的同胞类型，里面的密文本来就是 Codex 铸的。
//
// known 是本会话已知失效、下一轮进 begin 时会被剥掉的摘要。**必须先减掉它们**：它们已经不出站，
// 不可能是这一轮的元凶，留着会让下面那条「过滤清空就回退」永远不触发。理由见调用方。
func openAIBasisPointsSuspectEncryptedDigests(entryBody []byte, known map[string]struct{}) []string {
	suspect := collectOpenAIEncryptedContentDigestsRaw(entryBody)
	if len(known) > 0 {
		live := make([]string, 0, len(suspect))
		for _, digest := range suspect {
			if _, stripped := known[digest]; !stripped {
				live = append(live, digest)
			}
		}
		suspect = live
	}
	if len(suspect) == 0 {
		return nil
	}
	trusted := map[string]struct{}{}
	gjson.GetBytes(entryBody, "input").ForEach(func(_, item gjson.Result) bool {
		if strings.TrimSpace(item.Get("type").String()) != "compaction" {
			return true
		}
		if encrypted := item.Get("encrypted_content"); encrypted.Type == gjson.String && encrypted.String() != "" {
			trusted[openAIEncryptedContentDigest(encrypted.String())] = struct{}{}
		}
		return true
	})
	if len(trusted) == 0 {
		return suspect
	}
	filtered := make([]string, 0, len(suspect))
	for _, digest := range suspect {
		if _, ours := trusted[digest]; !ours {
			filtered = append(filtered, digest)
		}
	}
	// **过滤把候选清空时回退到不过滤。** 这一轮唯一的密文就是压缩摘要 ⇒ 它就是唯一可能的元凶
	// （BPS 也会拒自己铸的 blob：过期、跨网关、或者这条会话换了一个同样开着开关的账号，A 号铸的密文
	// 拿 B 号的凭据去解）。跳过它就等于一个字节都不记 lineage，调用方看到空切片直接 return，下一轮没
	// 东西可剥、blob 原样再发再被拒 —— 会话从压缩那一刻起**每轮硬错、永不自愈**。宁可剥掉压缩历史
	// 自愈一次（可见：模型丢上下文），也不要会话永久死。混合场景（Codex blob + 压缩摘要）的行为不变。
	if len(filtered) == 0 {
		return suspect
	}
	return filtered
}

// openAIBasisPointsLineageSessionKey：BPS 自己那把 lineage 键。空哈希原样返回（上层会丢弃）。
func openAIBasisPointsLineageSessionKey(sessionHash string) string {
	if sessionHash == "" {
		return ""
	}
	return "bps:" + sessionHash
}

// maskOpenAIBasisPointsSecrets 把上游可能回显的 bearer token / chatgpt_account_id 换成 [REDACTED]。
// 这一层是 sanitizeUpstreamErrorMessage **不做**的（它只替 URL 里的敏感 query 参数），凡是把上游
// 自由文本往外带的出口（ops 事件、落库的 error_message）都要过它。
func maskOpenAIBasisPointsSecrets(message string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}

// appendOpenAIBasisPointsUpstreamError 记一条 ops failover 事件；上游回显的 token / account id 打码。
func (s *OpenAIGatewayService) appendOpenAIBasisPointsUpstreamError(c *gin.Context, account *Account, status int, requestID, message string, secrets ...string) {
	message = maskOpenAIBasisPointsSecrets(message, secrets...)
	if message == "" {
		message = "basispoints upstream rejected the request"
	}
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		ProxyID:            opsUpstreamProxyID(account),
		ProxyName:          opsUpstreamProxyName(account),
		Platform:           account.Platform,
		AccountID:          account.ID,
		AccountName:        account.Name,
		UpstreamStatusCode: status,
		UpstreamRequestID:  requestID,
		// **false**：BPS 的出站体在两条入口上都是本层按白名单重新拼的，从来不是客户端字节的透传。
		// 这个字段今天**全仓没有读点**（只有 JSON 序列化与几条测试断言），改它纯粹是让落盘的读数
		// 别说谎 —— 别照着「原来恒 true 会落错桶」去找那个桶，没有那个桶。
		Passthrough: false,
		Kind:        "failover",
		Message:     "basispoints: " + message,
	})
}

// openAIBasisPointsErrorDetail 从错误体或流内 error / response.failed 事件里取错误码与脱敏后的消息。
func openAIBasisPointsErrorDetail(payload []byte) (code, message string) {
	for _, path := range []string{"error.code", "response.error.code", "response.status_details.error.code", "code"} {
		if code = strings.TrimSpace(gjson.GetBytes(payload, path).String()); code != "" {
			break
		}
	}
	for _, path := range []string{"error.message", "response.error.message", "message"} {
		if message = strings.TrimSpace(gjson.GetBytes(payload, path).String()); message != "" {
			break
		}
	}
	return code, sanitizeUpstreamErrorMessage(message)
}

// openAIBasisPointsDedicatedLabels：有专用标签的上游错误码。这些标签的信息量比泛化的
// status_<code>_stream 大，不许被状态码覆写（见 peek 分支那处守卫）。再加第二个专用标签时
// 只改这张表，不要在两处各抄一份字面量。
var openAIBasisPointsDedicatedLabels = map[string]string{
	"basispoints_model_access_changed": "model_access_changed",
}

func openAIBasisPointsReasonIsDedicatedLabel(reason string) bool {
	for _, label := range openAIBasisPointsDedicatedLabels {
		if reason == label {
			return true
		}
	}
	return false
}

// openAIBasisPointsRejectReason：落回原因标签 = 位置 + 上游错误码（只留 [a-z0-9_]，最长 48），进日志。
func openAIBasisPointsRejectReason(where, code string) string {
	if label, ok := openAIBasisPointsDedicatedLabels[code]; ok {
		return label
	}
	label := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		}
		return -1
	}, code)
	if len(label) > 48 {
		label = label[:48]
	}
	if label == "" {
		return where
	}
	return where + "_" + label
}

var errOpenAIBasisPointsPeekTimeout = errors.New("basispoints upstream sent no event before the deadline")

// peekOpenAIBasisPointsFirstOutput 读到「处理器会立刻写给客户端的第一个事件」为止，读过的字节原样接回
// 返回的 body。生命周期事件、原生工具事件都会被处理器暂存 / 被转写扣住，客户端看不见；这段时间客户端
// 还在等响应头，Codex 的空闲计时没有启动。failure 非空表示上游在此之前就以 error / response.failed 收场。
// 相邻两个事件之间超过 deadline 就关掉 body 报超时；读过的字节超过 4 MiB 就不再等。
func peekOpenAIBasisPointsFirstOutput(body io.ReadCloser, deadline time.Duration) (io.ReadCloser, []byte, error) {
	const limit = 4 << 20
	var timedOut atomic.Bool
	timer := time.AfterFunc(deadline, func() {
		timedOut.Store(true)
		_ = body.Close()
	})
	defer timer.Stop()
	reader := bufio.NewReaderSize(body, 64<<10)
	var consumed bytes.Buffer
	var data []byte
	rest := func() io.ReadCloser {
		return openAIBasisPointsPeekedBody{Reader: io.MultiReader(bytes.NewReader(consumed.Bytes()), reader), Closer: body}
	}
	for consumed.Len() < limit {
		line, err := reader.ReadString('\n')
		_, _ = consumed.WriteString(line)
		if err != nil {
			if timedOut.Load() {
				return nil, nil, errOpenAIBasisPointsPeekTimeout
			}
			return nil, nil, io.ErrUnexpectedEOF
		}
		trimmed := strings.TrimRight(line, "\r\n")
		switch {
		case trimmed == "":
			if data == nil {
				continue
			}
			timer.Stop()
			timer.Reset(deadline)
			kind := gjson.GetBytes(data, "type").String()
			// cancelled 以前不在这里：它会被 bpsPeekReachesClientOutput 判成「已经开始输出」，
			// 于是一条取消流被原样交给客户端而不落回。
			if kind == "error" || kind == "response.failed" ||
				kind == "response.cancelled" || kind == "response.canceled" {
				return rest(), data, nil
			}
			if bpsPeekReachesClientOutput(kind, data) {
				return rest(), nil, nil
			}
			data = nil
		case strings.HasPrefix(trimmed, "data:"):
			if data != nil {
				data = append(data, '\n')
			}
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(trimmed, "data:"), " ")...)
		}
	}
	return rest(), nil, nil
}

// bpsPeekReachesClientOutput：这个事件经转写后会不会立刻到客户端。原生工具事件被扣到 completed，
// 其余按处理器自己的首输出判定。
func bpsPeekReachesClientOutput(kind string, data []byte) bool {
	switch {
	case kind == "response.completed", kind == "response.incomplete":
		return true
	case strings.HasPrefix(kind, "response.function_call_arguments."), strings.HasPrefix(kind, "response.custom_tool_call_input."):
		return false
	case kind == "response.output_item.added", kind == "response.output_item.done":
		switch gjson.GetBytes(data, "item.type").String() {
		case "function_call", "custom_tool_call":
			return false
		}
	}
	return openAIStreamDataStartsClientOutput(string(data), kind)
}

type openAIBasisPointsPeekedBody struct {
	io.Reader
	io.Closer
}
