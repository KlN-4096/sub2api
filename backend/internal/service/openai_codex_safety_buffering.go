package service

import (
	"net/http"
	"strconv"
	"strings"
)

// Codex 上游在 /responses 响应头里带的 safety-buffering 读数（codex-api/src/safety_buffering.rs）。
// 客户端只看两个头在不在、从不读 enabled 的值（treatment_from_headers）；faster-model 是
// 客户端在"Giving this request a little extra thought"弹窗里 "Retry with a faster model" 会换到
// 的模型——只是 SSE safety_buffering 事件缺 retry_model 时的兜底默认值，不是实际服务模型，
// 也不证明请求被换过模型。上游→客户端单向，上游不回读，改写它没有任何服务端效果。
// 2026-09-25 直连实测：健康账号请求 gpt-6-sol 也带 enabled=true + gpt-6-luna，降智账号请求
// gpt-6-astra 带 gpt-5.6-luna——头是否存在不区分账号，只有 faster-model 与请求模型的对应
// 关系可能有信息。真实客户端直连时看得到它们，所以下游原样放行；同时落 usage_logs。
const (
	openAICodexSafetyBufferingEnabledHeader     = "x-codex-safety-buffering-enabled"
	openAICodexSafetyBufferingFasterModelHeader = "x-codex-safety-buffering-faster-model"
	maxUsageSafetyBufferingModelLen             = 128
)

var openAICodexSafetyBufferingHeaders = [...]string{
	openAICodexSafetyBufferingEnabledHeader,
	openAICodexSafetyBufferingFasterModelHeader,
}

// headerValuesFold 取 h 里与 name 大小写无关匹配的全部值；上游响应头经 net/http 已规范化，
// 手工构造的 http.Header 可能不是，两种都认。
func headerValuesFold(h http.Header, name string) []string {
	var out []string
	for key, values := range h {
		if strings.EqualFold(key, name) {
			out = append(out, values...)
		}
	}
	return out
}

// relayOpenAICodexSafetyBufferingHeaders 把上游的两个头原样写到 dst；上游缺失时清除 dst 上
// 可能残留的上一 failover attempt 的值（与 turn-state 同一套理由）。
func relayOpenAICodexSafetyBufferingHeaders(dst http.Header, upstream http.Header) {
	if dst == nil {
		return
	}
	for _, name := range openAICodexSafetyBufferingHeaders {
		key := http.CanonicalHeaderKey(name)
		dst.Del(key)
		for _, v := range headerValuesFold(upstream, name) {
			dst.Add(key, v)
		}
	}
}

// stageOpenAICodexSafetyBufferingHeaders 与 stageOpenAICodexTurnState 同型：暂存到首输出守卫
// 延迟提交的响应头集合。既有限制同 turn-state：keepalive（默认 10s）先于首个语义输出写出
// 注释帧时响应头已提交，暂存值不再发给客户端；落库不受影响。
func stageOpenAICodexSafetyBufferingHeaders(dst *http.Header, upstream http.Header) {
	if dst == nil {
		return
	}
	if *dst == nil {
		present := false
		for _, name := range openAICodexSafetyBufferingHeaders {
			if len(headerValuesFold(upstream, name)) > 0 {
				present = true
				break
			}
		}
		if !present {
			return
		}
		*dst = http.Header{}
	}
	relayOpenAICodexSafetyBufferingHeaders(*dst, upstream)
}

// usageCodexSafetyBufferingEnabledPtr 从上游响应头取 enabled 读数写进使用记录；取不到或
// 不是布尔字面量返回 nil（列保持 NULL）。
func usageCodexSafetyBufferingEnabledPtr(h http.Header) *bool {
	values := headerValuesFold(h, openAICodexSafetyBufferingEnabledHeader)
	if len(values) == 0 {
		return nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(values[0]))
	if err != nil {
		return nil
	}
	return &v
}

// usageCodexSafetyBufferingFasterModelPtr 从上游响应头取 faster-model 读数；空值返回 nil。
func usageCodexSafetyBufferingFasterModelPtr(h http.Header) *string {
	values := headerValuesFold(h, openAICodexSafetyBufferingFasterModelHeader)
	if len(values) == 0 {
		return nil
	}
	v := strings.TrimSpace(values[0])
	if v == "" {
		return nil
	}
	// 列是 TEXT：截断不切多字节字符，短值里的非法字节也剔掉，否则整行 INSERT 被 PostgreSQL 拒绝。
	v = strings.ToValidUTF8(truncateUTF8(v, maxUsageSafetyBufferingModelLen), "")
	return &v
}
