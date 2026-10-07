package service

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"unicode/utf16"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// rewriteCodexTurnMetadataJSON replaces only selected top-level identity values.
// The header and the opaque client_metadata string must retain the caller's key
// order, whitespace, Unicode escapes and unknown values. Never marshal the object.
func rewriteCodexTurnMetadataJSON(raw string, rebuildInvalid bool, updates func(map[string]any) map[string]any) string {
	original := raw
	var metadata map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if !json.Valid([]byte(raw)) || decoder.Decode(&metadata) != nil || metadata == nil {
		if !rebuildInvalid {
			return original
		}
		raw, metadata = "{}", map[string]any{}
	}
	fields := updates(metadata)
	encoded := make(map[string]string, len(fields))
	for name, value := range fields {
		next, err := marshalCodexTurnMetadataValue(value)
		if err != nil {
			return original
		}
		encoded[name] = next
	}
	out := make([]byte, 0, len(raw))
	offset := 0
	seen := make(map[string]bool, len(fields))
	gjson.Parse(raw).ForEach(func(key, value gjson.Result) bool {
		name := key.String()
		next, ok := encoded[name]
		if !ok {
			return true
		}
		seen[name] = true
		// Preserve an already-correct string's original escape spelling.
		if text, ok := fields[name].(string); ok && value.Type == gjson.String && value.Str == text {
			return true
		}
		// Visit all duplicates, not just the first match returned by Get/Set.
		// Identity derivation still uses encoding/json's last-value-wins rule.
		out = append(out, raw[offset:value.Index]...)
		out = append(out, next...)
		offset = value.Index + len(value.Raw)
		return true
	})
	out = append(out, raw[offset:]...)
	next := string(out)
	// Only absent identity fields are appended; existing fields never move.
	for _, name := range slices.Sorted(maps.Keys(encoded)) {
		if seen[name] {
			continue
		}
		var err error
		next, err = sjson.SetRaw(next, name, encoded[name])
		if err != nil {
			return original
		}
	}
	return codexTurnMetadataASCII(next)
}

// New scalar values follow Codex's ASCII JSON spelling, without HTML escaping.
func marshalCodexTurnMetadataValue(value any) (string, error) {
	raw, err := marshalOpenAIUpstreamJSON(value)
	if err != nil {
		return "", err
	}
	return codexTurnMetadataASCII(string(raw)), nil
}

// codexTurnMetadataASCII escapes non-ASCII as \uXXXX, like the real client's
// to_ascii_json_string (codex-rs utils/string/src/json.rs): turn metadata is also an
// HTTP header. Client text that is already ASCII stays byte-identical, and non-ASCII
// can only appear inside JSON strings, so decoded values do not change.
func codexTurnMetadataASCII(raw string) string {
	for i := 0; i < len(raw); i++ {
		if raw[i] < 0x80 {
			continue
		}
		out := []byte(raw[:i])
		for _, r := range raw[i:] {
			switch {
			case r < 0x80:
				out = append(out, byte(r))
			case r <= 0xffff:
				out = fmt.Appendf(out, `\u%04x`, r)
			default:
				high, low := utf16.EncodeRune(r)
				out = fmt.Appendf(out, `\u%04x\u%04x`, high, low)
			}
		}
		return string(out)
	}
	return raw
}

// codexTurnMetadataExecutionValues：codex 0.156 起 turn-metadata 带本轮 model，与请求体的 model 是同一个
// model_info.slug（rust-v0.156.1 core/src/session/session.rs:667 ExecutionMetadata::apply_to，client.rs
// build_responses_request；core/tests/suite/step_settings.rs:1885-1908 逐请求断言相等）。网关改了体里的
// model（账号 / 渠道模型映射、兜底换模型）后按出站值同步已有的键；入站没有就不补（0.156 之前的客户端、
// 压缩请求本来就没有）。
//
// 同批加入的 reasoning_effort 刻意不跟：它写的是用户选中的档位（step_settings.rs:87
// effective_reasoning_effort），体里是解析后的值（client.rs build_reasoning → resolve_reasoning_effort：
// ultra 解析成 multi_agent_reasoning_effort / max，persistent 发 "disabled"；另有按窗口钉住的 effort，
// session/reasoning_effort.rs），两者在真客户端本来就可以不同，按体改写反而造出假形态。
func codexTurnMetadataExecutionValues(payload []byte) map[string]string {
	return map[string]string{"model": gjson.GetBytes(payload, "model").String()}
}

// alignCodexEmbeddedTurnMetadata 对齐 client_metadata 里内嵌的那份 turn-metadata（HTTP 体与 WS 帧）。
func alignCodexEmbeddedTurnMetadata(payload []byte, values map[string]string) []byte {
	embedded := gjson.GetBytes(payload, "client_metadata."+openAIWSTurnMetadataHeader).Str // 非字符串得到 ""，下面原样返回
	if next := alignCodexTurnMetadataJSON(embedded, values); next != embedded {
		return setCodexWSClientMetadataString(payload, openAIWSTurnMetadataHeader, next)
	}
	return payload
}

// alignCodexTurnMetadataExecutionBody / Header 是 HTTP 两个构造器的两处落点：体在定稿时、头在身份收口之后。
// 判据与请求体压缩相同：双开且出站是 /responses。
func alignCodexTurnMetadataExecutionBody(c *gin.Context, account *Account, targetURL string, body []byte) []byte {
	if !codexRequestBodyCompressionEnabled(c, account, targetURL) {
		return body
	}
	return alignCodexEmbeddedTurnMetadata(body, codexTurnMetadataExecutionValues(body))
}

func alignCodexTurnMetadataExecutionHeader(c *gin.Context, account *Account, targetURL string, headers http.Header, body []byte) {
	if codexRequestBodyCompressionEnabled(c, account, targetURL) {
		alignCodexTurnMetadataFields(headers, codexTurnMetadataExecutionValues(body))
	}
}
