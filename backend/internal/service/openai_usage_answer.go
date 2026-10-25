package service

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// 首答耗时与推理 Token 数保留用于诊断。使用记录的平均输出 TPS 只取
// output_tokens ÷ duration_ms（frontend/src/utils/usageTps.ts），不使用这两个字段。

// openAIFirstAnswer 记首个答案 delta 的到达时延。见到安全缓冲标记即作废且不再记：上游扣住输出、
// 审完才放（Codex 提示 "Giving this request a little extra thought"），到达节奏不反映生成速度。
type openAIFirstAnswer struct {
	ms       *int
	buffered bool
}

// observe 的 eventType 是 effectiveOpenAISSEEventType 的结果。
func (a *openAIFirstAnswer) observe(data, eventType string, start time.Time) {
	if a.buffered {
		return
	}
	if openAIStreamDataSafetyBuffered(data, eventType) {
		a.ms, a.buffered = nil, true
		return
	}
	if a.ms == nil && openAIStreamDataStartsAnswer(data, eventType) {
		ms := int(time.Since(start).Milliseconds())
		a.ms = &ms
	}
}

// openAIStreamDataStartsAnswer：推理与推理摘要以外的非空 delta（正文、拒答、工具参数等）。只认 delta：
// 整段到达的 done/item 事件之后流很快结束，按它计时窗口近于 0。
func openAIStreamDataStartsAnswer(data, eventType string) bool {
	return strings.HasSuffix(eventType, ".delta") && !strings.HasPrefix(eventType, "response.reasoning") &&
		gjson.Get(data, "delta").String() != ""
}

// openAIStreamDataSafetyBuffered 认安全缓冲标记（codex-api/src/sse/responses.rs safety_buffering()）：
// 任一事件顶层的 safety_buffering 对象（false 不算），或 metadata.type 为 safety_buffering 的 response.metadata。
func openAIStreamDataSafetyBuffered(data, eventType string) bool {
	return gjson.Get(data, "safety_buffering").IsObject() ||
		eventType == "response.metadata" && gjson.Get(data, "metadata.type").String() == "safety_buffering"
}

// openAIForwardFirstAnswerMs：客户端工具映射把被映射调用的参数 delta 扣到 .done 才一次放出
// （apicompat/responses_client_tools.go），首答会推迟到参数结束，作废。
func openAIForwardFirstAnswerMs(c *gin.Context, ms *int) *int {
	if _, mapped := openAIResponsesClientToolMapping(c); mapped {
		return nil
	}
	return ms
}

// openAIReasoningTokensFromGJSON 读 usage 里的推理 token，取值与 openAIUsageFromGJSON 相同；
// 两个字段都不是数字（缺失或 null）时为 nil，与「报了 0」区分。
func openAIReasoningTokensFromGJSON(value gjson.Result) *int {
	completion := value.Get("completion_tokens_details.reasoning_tokens")
	output := value.Get("output_tokens_details.reasoning_tokens")
	if completion.Type != gjson.Number && output.Type != gjson.Number {
		return nil
	}
	tokens := firstPositiveGJSONInt(completion, output)
	return &tokens
}
