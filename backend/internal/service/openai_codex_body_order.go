package service

import (
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// 真实客户端的请求体是 serde 结构体序列化的，顶层键序固定为字段声明序；网关的非透传
// 路径把请求体解成 map[string]any 再 marshal，Go 按字典序输出，两者完全对不上。
// 顺序取自 16ff14c codex-api/src/common.rs：
//
//	ResponsesApiRequest  common.rs:282
//	CompactionInput      common.rs:48（注意 input 在 instructions 之前，与 Responses 相反）
//
// 只重排顶层键，值的原始字节原样搬运——嵌套结构的键序、转义、数字写法都不受影响。
// 表外键按原相对顺序追加到末尾：compact 白名单仍放行 previous_response_id（第三方客户端
// 可能带，真客户端的 CompactionInput 没有它），带了就落在 access_programs 之后。
// client_metadata 内层不必处理：真客户端那里是 HashMap<String,String>，Rust 的迭代
// 顺序本身就是随机的，字典序落在它的分布内。
var (
	codexResponsesFieldOrder = []string{
		"model", "instructions", "input", "tools", "tool_choice", "parallel_tool_calls",
		"reasoning", "store", "stream", "stream_options", "include", "service_tier",
		"prompt_cache_key", "text", "client_metadata", "access_programs",
	}
	codexCompactFieldOrder = []string{
		"model", "input", "instructions", "tools", "parallel_tool_calls", "reasoning",
		"service_tier", "prompt_cache_key", "text", "access_programs",
	}
)

// applyCodexBodyFieldOrder 在请求体定稿后、构造 http.Request 之前调用。
// 只对双开账号生效，其他配置字节不变。按**出站** URL 决定字段序，而不是入站路径：
// OAuth 图片走的是网关自建的 Responses body、出站到 /responses，入站路径却是
// /v1/images/generations；出站才是上游看到的形态。
func applyCodexBodyFieldOrder(c *gin.Context, account *Account, targetURL string, body []byte) []byte {
	if !codexDeviceWireProfileEnabled(c, account) {
		return body
	}
	parsed, err := url.Parse(targetURL)
	if err != nil {
		return body
	}
	switch path := strings.TrimRight(parsed.Path, "/"); {
	case strings.HasSuffix(path, "/responses/compact"):
		return reorderCodexTopLevelFields(body, codexCompactFieldOrder)
	case strings.HasSuffix(path, "/responses"):
		return reorderCodexTopLevelFields(body, codexResponsesFieldOrder)
	}
	// 搜索、embeddings、API-key 图片等端点是另一套请求结构，不套用 Responses 的字段序。
	return body
}

// reorderCodexTopLevelFields 按 order 重排顶层键；order 之外的键保持原有相对顺序追加在
// 后面（真客户端不发它们，重排也无从对齐，保持原样比强行排序更少制造差异）。
// 非对象、含重复键或空对象一律原样返回：这条路径只做重排，不做修复。
func reorderCodexTopLevelFields(body []byte, order []string) []byte {
	parsed := gjson.ParseBytes(body)
	if !parsed.IsObject() {
		return body
	}
	type field struct{ name, key, raw string }
	fields := make([]field, 0, len(order))
	index := make(map[string]int, len(order))
	duplicate := false
	parsed.ForEach(func(key, value gjson.Result) bool {
		name := key.String()
		if _, ok := index[name]; ok {
			duplicate = true
			return false
		}
		index[name] = len(fields)
		fields = append(fields, field{name: name, key: key.Raw, raw: value.Raw})
		return true
	})
	if duplicate || len(fields) == 0 {
		return body
	}

	out := make([]byte, 0, len(body))
	out = append(out, '{')
	emitted := make([]bool, len(fields))
	emit := func(i int) {
		if len(out) > 1 {
			out = append(out, ',')
		}
		out = append(out, fields[i].key...)
		out = append(out, ':')
		out = append(out, fields[i].raw...)
		emitted[i] = true
	}
	for _, name := range order {
		if i, ok := index[name]; ok {
			emit(i)
		}
	}
	for i := range fields {
		if !emitted[i] {
			emit(i)
		}
	}
	return append(out, '}')
}

// ResponseCreateWsRequest（16ff14c: codex-api/src/common.rs:334-363）；外层 ResponsesWsRequest
// 的 serde tag 让 type 排在最前（common.rs:388-394）。
var codexWSCreateFieldOrder = []string{
	"type", "model", "instructions", "previous_response_id", "input", "tools", "tool_choice",
	"parallel_tool_calls", "reasoning", "store", "stream", "stream_options", "include",
	"service_tier", "prompt_cache_key", "text", "generate", "client_metadata", "access_programs",
}
