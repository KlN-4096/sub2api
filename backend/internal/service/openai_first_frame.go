package service

import (
	"strings"
	"time"

	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
)

// sub2api 的首字（first_token_ms）统一记第一个响应帧：从上游收到的第一个数据帧（SSE data、WS 消息），
// 判据是 openaiwsv2.IsResponseFrameEvent（docs/conventions/usage-logs.md「首字」）。
// 各路径流内原有的首字变量还管着首输出计时、WS 先缓冲后转发、flush，不改它们；
// 只在组装结果时改报 openAIFirstFrame.ms。
type openAIFirstFrame struct {
	ms *int
}

// observe 用于 SSE 数据行：空数据与 [DONE] 不算帧。
func (f *openAIFirstFrame) observe(data, eventType string, start time.Time) {
	if f.ms != nil {
		return
	}
	if data = strings.TrimSpace(data); data == "" || data == "[DONE]" {
		return
	}
	f.observeEvent(eventType, start)
}

// observeEvent 用于已解析出类型的 WS 消息。
func (f *openAIFirstFrame) observeEvent(eventType string, start time.Time) {
	if f.ms != nil || !openaiwsv2.IsResponseFrameEvent(eventType) {
		return
	}
	ms := int(time.Since(start).Milliseconds())
	f.ms = &ms
}
