package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 期望序直接抄自 codex-rs 16ff14c，不引用生产表：生产表被改动时这里必须失败。
var (
	codexWantResponsesOrder = []string{ // codex-api/src/common.rs:282-307 ResponsesApiRequest
		"model", "instructions", "input", "tools", "tool_choice", "parallel_tool_calls", "reasoning",
		"store", "stream", "stream_options", "include", "service_tier", "prompt_cache_key", "text",
		"client_metadata", "access_programs",
	}
	codexWantCompactOrder = []string{ // codex-api/src/common.rs:48-65 CompactionInput
		"model", "input", "instructions", "tools", "parallel_tool_calls", "reasoning", "service_tier",
		"prompt_cache_key", "text", "access_programs",
	}
	codexWantWSCreateOrder = []string{ // codex-api/src/common.rs:334-363 + :388-394 的 serde tag
		"type", "model", "instructions", "previous_response_id", "input", "tools", "tool_choice",
		"parallel_tool_calls", "reasoning", "store", "stream", "stream_options", "include",
		"service_tier", "prompt_cache_key", "text", "generate", "client_metadata", "access_programs",
	}
)

// requireCodexFieldOrder：keys 必须全部在 want 里，且是 want 的子序列（不允许重复）。
func requireCodexFieldOrder(t *testing.T, keys, want []string) {
	t.Helper()
	at := 0
	for _, key := range keys {
		for at < len(want) && want[at] != key {
			at++
		}
		require.Less(t, at, len(want), "%s 不在字段序表里或顺序错误：%v", key, keys)
		at++
	}
}

func TestCodexFieldOrderTablesMatchCodexRS(t *testing.T) {
	require.Equal(t, codexWantResponsesOrder, codexResponsesFieldOrder)
	require.Equal(t, codexWantCompactOrder, codexCompactFieldOrder)
	require.Equal(t, codexWantWSCreateOrder, codexWSCreateFieldOrder)
	for name, tc := range map[string]struct{ want, table []string }{
		"responses": {codexWantResponsesOrder, codexResponsesFieldOrder},
		"compact":   {codexWantCompactOrder, codexCompactFieldOrder},
		"ws":        {codexWantWSCreateOrder, codexWSCreateFieldOrder},
	} {
		// 全字段按倒序给入，输出必须逐字等于声明序。
		parts := make([]string, 0, len(tc.want))
		for i := len(tc.want) - 1; i >= 0; i-- {
			parts = append(parts, fmt.Sprintf(`"%s":%d`, tc.want[i], i))
		}
		body := []byte("{" + strings.Join(parts, ",") + "}")
		require.Equal(t, tc.want, topLevelKeys(t, reorderCodexTopLevelFields(body, tc.table)), name)
	}
}

// topLevelKeys 只看顶层键序；值不参与比较。
func topLevelKeys(t *testing.T, body []byte) []string {
	t.Helper()
	require.True(t, gjson.ValidBytes(body), string(body))
	keys := make([]string, 0, 16)
	gjson.ParseBytes(body).ForEach(func(key, _ gjson.Result) bool {
		keys = append(keys, key.String())
		return true
	})
	return keys
}

func TestReorderCodexTopLevelFields(t *testing.T) {
	t.Run("按声明序重排，未知键保持相对顺序追加在后", func(t *testing.T) {
		// 字典序输入，即非透传路径 map 重编码后的实际形态。
		in := []byte(`{"client_metadata":{"b":1,"a":2},"include":["x"],"input":[],` +
			`"instructions":"i","model":"m","zzz_unknown":1,"aaa_unknown":2,"store":false,"stream":true}`)
		out := reorderCodexTopLevelFields(in, codexResponsesFieldOrder)
		require.Equal(t, []string{
			"model", "instructions", "input", "store", "stream", "include", "client_metadata",
			"zzz_unknown", "aaa_unknown",
		}, topLevelKeys(t, out))
		// 值的原始字节原样搬运：嵌套对象的键序不得被重排。
		require.Equal(t, `{"b":1,"a":2}`, gjson.GetBytes(out, "client_metadata").Raw)
	})

	t.Run("compact 的 input 在 instructions 之前", func(t *testing.T) {
		in := []byte(`{"instructions":"i","input":[],"model":"m"}`)
		require.Equal(t, []string{"model", "input", "instructions"},
			topLevelKeys(t, reorderCodexTopLevelFields(in, codexCompactFieldOrder)))
		// 同一份输入按 Responses 的序则相反，两张表不能混用。
		require.Equal(t, []string{"model", "instructions", "input"},
			topLevelKeys(t, reorderCodexTopLevelFields(in, codexResponsesFieldOrder)))
	})

	t.Run("已是目标序时是恒等变换", func(t *testing.T) {
		in := []byte(`{"model":"m","instructions":"i","input":[],"stream":true}`)
		require.Equal(t, string(in), string(reorderCodexTopLevelFields(in, codexResponsesFieldOrder)))
	})

	t.Run("异常输入原样返回", func(t *testing.T) {
		for _, in := range []string{
			``, `not json`, `[1,2]`, `null`, `"scalar"`, `{}`, `{broken`,
			`{"model":"a","model":"b"}`, // 重复键：只重排不修复，交给上游按原样判定
		} {
			require.Equal(t, in, string(reorderCodexTopLevelFields([]byte(in), codexResponsesFieldOrder)), in)
		}
	})

	t.Run("转义键名与 unicode 值不被改写", func(t *testing.T) {
		in := []byte(`{"instructions":"项目 <&>","model":"m"}`)
		out := reorderCodexTopLevelFields(in, codexResponsesFieldOrder)
		require.Equal(t, []string{"model", "instructions"}, topLevelKeys(t, out))
		require.Contains(t, string(out), `"model"`, "键的原始拼写保留")
		require.Contains(t, string(out), `"项目 <&>"`, "值的原始转义保留")
	})
}

// 端到端：非透传是现网形态（自动透传关闭），透传也走同一条规则。
func TestCodexBodyFieldOrderHTTP(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, passthrough := range []bool{false, true} {
			for _, compact := range []bool{false, true} {
				t.Run(fmt.Sprintf("enabled=%v/raw=%v/compact=%v", enabled, passthrough, compact), func(t *testing.T) {
					body := wireProfileTestBody(t)
					if compact {
						// CompactionInput 没有 client_metadata；handler 的 compact 白名单
						// 已经把它丢掉了，Forward 是白名单之后的一段，这里对齐现网入参。
						var err error
						body, err = sjson.DeleteBytes(body, "client_metadata")
						require.NoError(t, err)
					}
					c := newConvTestContext(t, body)
					if compact {
						c.Request.URL.Path = "/v1/responses/compact"
					}
					account := wireProfileTestAccount(enabled)
					account.Extra["openai_passthrough"] = passthrough
					svc, up := wireProfileTestService()
					_, _ = svc.Forward(context.Background(), c, account, body)
					require.NotNil(t, up.lastReq)

					keys := topLevelKeys(t, up.lastBody)
					want := codexWantResponsesOrder
					if compact {
						want = codexWantCompactOrder
					}
					if !enabled {
						// 未开投影的账号不受这条规则影响：透传保持入站键序（只允许在尾部追加），
						// map 路径仍是 Go 的字典序。两者都与声明序不同，任何重排都会在这里失败。
						if passthrough {
							// compact 白名单会裁掉键，只比较两侧共有键的相对顺序。
							in := topLevelKeys(t, body)
							require.Equal(t, orderedIntersection(in, keys), orderedIntersection(keys, in), "%v", keys)
						} else {
							require.True(t, sort.StringsAreSorted(keys), "%v", keys)
						}
						require.NotEqual(t, "model", keys[0], "未开投影不得套用声明序")
						return
					}
					requireCodexFieldOrder(t, keys, want)
					require.Equal(t, "model", keys[0])
				})
			}
		}
	}
}

// 按出站 URL 决定：非 Responses 目标不动；入站是图片路径、出站是 /responses 的
// OAuth 图片必须套用 Responses 字段序。
func TestCodexBodyFieldOrderByTargetURL(t *testing.T) {
	body, err := sjson.SetBytes(wireProfileTestBody(t), "size", "1024x1024")
	require.NoError(t, err)
	c := newConvTestContext(t, body)
	c.Request.URL.Path = "/v1/images/generations"
	account := wireProfileTestAccount(true)
	require.Equal(t, string(body), string(applyCodexBodyFieldOrder(c, account, openAIImagesGenerationsURL, body)),
		"API-key 图片端点不是 Responses 结构")
	require.Equal(t, string(body), string(applyCodexBodyFieldOrder(c, account, "://bad url", body)))
	reordered := applyCodexBodyFieldOrder(c, account, chatgptCodexURL, body)
	require.Equal(t, "model", topLevelKeys(t, reordered)[0], "OAuth 图片出站到 /responses，必须套用 Responses 字段序")
	compact := applyCodexBodyFieldOrder(c, account, chatgptCodexURL+"/compact", body)
	keys := topLevelKeys(t, compact)
	require.Less(t, indexOf(keys, "input"), indexOf(keys, "instructions"), "compact 的 input 在 instructions 之前")
}

// orderedIntersection 返回 a 中同时出现在 b 里的键，保持 a 的顺序。
func orderedIntersection(a, b []string) []string {
	out := make([]string, 0, len(a))
	for _, key := range a {
		if indexOf(b, key) >= 0 {
			out = append(out, key)
		}
	}
	return out
}

func indexOf(keys []string, name string) int {
	for i, k := range keys {
		if k == name {
			return i
		}
	}
	return -1
}
