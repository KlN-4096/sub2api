package service

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const bpsTestTools = `[
	{"type":"function","name":"exec_command","parameters":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}},
	{"type":"custom","name":"apply_patch","format":{"type":"grammar","syntax":"lark","definition":"start: x"}},
	{"type":"function","name":"update_plan","parameters":{"type":"object","properties":{"plan":{"type":"array"},"explanation":{"type":"string"}}}},
	{"type":"namespace","name":"collab","tools":[{"type":"function","name":"spawn","parameters":{"type":"object"}}]}
]`

func newBasisPointsTestBridge(t *testing.T, toolsJSON string) *openAIBasisPointsBridge {
	t.Helper()
	bridge := newOpenAIBasisPointsBridge("scope", newOpenAIBasisPointsReplayCache(), nil)
	var tools []any
	require.NoError(t, json.Unmarshal([]byte(toolsJSON), &tools))
	bridge.collectTools(tools, "")
	return bridge
}

// bpsTestProtocolText 取出站体里那条协议 developer 消息的正文（不是 instructions 那条）。
// 按内容认而不是按下标认：下标随出站形态变过一次，钉死下标的断言当时静默变成了空串比较。
func bpsTestProtocolText(t *testing.T, body []byte) string {
	t.Helper()
	for _, item := range gjson.GetBytes(body, "input").Array() {
		text := item.Get("content.0.text").String()
		if strings.Contains(text, "This request comes from an external Responses client") {
			return text
		}
	}
	require.Fail(t, "出站体里找不到协议 developer 消息")
	return ""
}

func bpsTestNativeCall(t *testing.T, callID string, arguments map[string]any) bpsObject {
	t.Helper()
	encoded, err := json.Marshal(arguments)
	require.NoError(t, err)
	return bpsObject{"type": "function_call", "id": "fc_" + callID, "call_id": callID, "name": "run_officejs", "arguments": string(encoded)}
}

func TestOpenAIBasisPointsEffortNormalization(t *testing.T) {
	cases := map[string]string{
		"": "medium", "medium": "medium", "low": "low", "high": "high", "HIGH": "high",
		"xhigh": "xhigh", "x-high": "xhigh", "max": "xhigh", "ultra": "xhigh",
		"none": "low", "minimal": "low", "bogus": "medium",
	}
	for in, want := range cases {
		require.Equal(t, want, normalizeOpenAIBasisPointsEffort(in, "gpt-5.6-sol"), in)
	}
	// 不按模型钳档：2026-09-29 直连实测 astra + reasoning_effort=low 是 200，
	// 原来那条「astra 没有 low，钳到 medium」的钳制在白吃掉客户端的低档。
	for _, model := range []string{"gpt-6-astra", "gpt-5.6-luna", "gpt-5.6-sol", ""} {
		require.Equal(t, "low", normalizeOpenAIBasisPointsEffort("low", model), model)
		require.Equal(t, "low", normalizeOpenAIBasisPointsEffort("minimal", model), model)
		require.Equal(t, "xhigh", normalizeOpenAIBasisPointsEffort("max", model), model)
	}
}

func TestOpenAIBasisPointsRouteReason(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"plain", `{"model":"m","input":"hi"}`, ""},
		{"cached web search stays", `{"tools":[{"type":"web_search","external_web_access":false}],"input":"hi"}`, ""},
		{"live web search goes to the bridge", `{"tools":[{"type":"web_search","external_web_access":true}],"input":"hi"}`, ""},
		{"web search without flag goes to the bridge", `{"tools":[{"type":"web_search_preview"}],"input":"hi"}`, ""},
		{"web search nested in namespace goes to the bridge", `{"tools":[{"type":"namespace","name":"n","tools":[{"type":"web_search"}]}],"input":"hi"}`, ""},
		{"web search in additional_tools goes to the bridge", `{"input":[{"type":"additional_tools","tools":[{"type":"web_search"}]}]}`, ""},
		{"tool_choice auto", `{"tool_choice":"auto","input":"hi"}`, ""},
		{"tool_choice none", `{"tool_choice":"none","input":"hi"}`, ""},
		{"json_schema output", `{"text":{"format":{"type":"json_schema"}},"input":"hi"}`, "output_format"},
		{"text output ok", `{"text":{"format":{"type":"text"}},"input":"hi"}`, ""},
		{"previous_response_id", `{"previous_response_id":"resp_1","input":"hi"}`, "history_reference"},
		{"conversation reference", `{"conversation":"conv_1","input":"hi"}`, "history_reference"},
		{"prompt template", `{"prompt":{"id":"p_1"},"input":"hi"}`, "prompt_template"},
		// 工具声明与 tool_choice 不再由这道闸门判死（参考实现 v0.2.9 的口径：认不出的工具类型
		// 静默跳过，全程没有任何工具类型的请求级拒收）。它们走 collectTools 的 b.unsupported
		// + 提示说明；item_reference / configuration_update 走 translateHistory 的占位项。
		{"tool_choice required goes to the bridge", `{"tool_choice":"required","input":"hi"}`, ""},
		{"tool_choice forced function goes to the bridge", `{"tool_choice":{"type":"function","name":"x"},"input":"hi"}`, ""},
		{"tool_choice web search goes to the bridge", `{"tool_choice":{"type":"web_search_preview"},"input":"hi"}`, ""},
		{"image_generation goes to the bridge", `{"tools":[{"type":"image_generation"}],"input":"hi"}`, ""},
		{"mcp hosted tool goes to the bridge", `{"tools":[{"type":"mcp","server_label":"x"}],"input":"hi"}`, ""},
		{"code interpreter in namespace goes to the bridge", `{"tools":[{"type":"namespace","name":"n","tools":[{"type":"code_interpreter"}]}],"input":"hi"}`, ""},
		{"additional_tools hosted goes to the bridge", `{"input":[{"type":"additional_tools","tools":[{"type":"mcp"}]}]}`, ""},
		{"item_reference goes to the bridge", `{"input":[{"type":"item_reference","id":"msg_1"}]}`, ""},
		{"configuration_update goes to the bridge", `{"input":[{"type":"configuration_update"}]}`, ""},
		{"https image ok", `{"input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"https://x/y.png"}]}]}`, ""},
		{"data image ok (uploaded later)", `{"input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AA=="}]}]}`, ""},
		{"file_id ok", `{"input":[{"type":"message","role":"user","content":[{"type":"input_image","file_id":"file_1"}]}]}`, ""},
		// 内容部件不再由这道事前闸门判死（2026-09-30 起换成占位部件，见 rewriteContent）：
		// http 图、未知类型、非字符串正文、工具输出里的未知部件全部放行到 bridge。
		{"http image goes to the bridge", `{"input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"http://x/y.png"}]}]}`, ""},
		{"unknown content part goes to the bridge", `{"input":[{"type":"message","role":"user","content":[{"type":"input_file","file_id":"f"}]}]}`, ""},
		{"unknown part in tool output goes to the bridge", `{"input":[{"type":"function_call_output","call_id":"c","output":[{"type":"input_audio"}]}]}`, ""},
		{"native tool call item goes to the bridge", `{"input":[{"type":"web_search_call","id":"ws_1","status":"completed"}]}`, ""},
		{"invalid json", `{`, ""},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, openAIBasisPointsRouteReason([]byte(tc.body)), tc.name)
	}
}

func TestOpenAIBasisPointsTransportEnvelopeDecoding(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	nested, _ := json.Marshal(map[string]any{"code": `{"name":"exec_command","arguments":{"cmd":"pwd"}}`})
	cases := []struct {
		name string
		code any
		want string
	}{
		{"tool/args", `{"tool":"exec_command","args":{"cmd":"pwd"}}`, `{"cmd":"pwd"}`},
		{"name/arguments", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`, `{"cmd":"pwd"}`},
		{"object not string", map[string]any{"name": "exec_command", "arguments": map[string]any{"cmd": "pwd"}}, `{"cmd":"pwd"}`},
		{"double encoded", `"{\"name\":\"exec_command\",\"arguments\":{\"cmd\":\"pwd\"}}"`, `{"cmd":"pwd"}`},
		{"markdown fence", "```json\n{\"name\":\"exec_command\",\"arguments\":{\"cmd\":\"pwd\"}}\n```", `{"cmd":"pwd"}`},
		{"prose prefix", `Calling the tool now: {"name":"exec_command","arguments":{"cmd":"pwd"}}`, `{"cmd":"pwd"}`},
		{"nested transport wrapper", `{"name":"run_officejs","arguments":` + string(nested) + `}`, `{"cmd":"pwd"}`},
		{"arguments as json string", `{"name":"exec_command","arguments":"{\"cmd\":\"pwd\"}"}`, `{"cmd":"pwd"}`},
		{"raw newline inside string", "{\"name\":\"exec_command\",\"arguments\":{\"cmd\":\"echo a\nb\"}}", `{"cmd":"echo a\nb"}`},
		{"invalid backslash escape", `{"name":"exec_command","arguments":{"cmd":"dir C:\dev"}}`, `{"cmd":"dir C:\\dev"}`},
	}
	for _, tc := range cases {
		native := bpsTestNativeCall(t, "call_"+strings.ReplaceAll(tc.name, " ", "_"), map[string]any{"summary": "s", "code": tc.code})
		call, err := bridge.translateCall(native)
		require.NoError(t, err, tc.name)
		require.Equal(t, "function_call", bpsText(call["type"]), tc.name)
		require.Equal(t, "exec_command", bpsText(call["name"]), tc.name)
		require.Equal(t, native["call_id"], call["call_id"], tc.name)
		require.JSONEq(t, tc.want, bpsText(call["arguments"]), tc.name)
	}

	rejected := []struct {
		name string
		code any
	}{
		{"conflicting names", `{"name":"exec_command","tool":"apply_patch","arguments":{}}`},
		{"two envelopes", `{"name":"exec_command","arguments":{"cmd":"a"}}{"name":"exec_command","arguments":{"cmd":"b"}}`},
		{"javascript", `Excel.run(async (ctx) => { await ctx.sync(); })`},
		{"outside catalog", `{"name":"rm_rf","arguments":{}}`},
		{"inner transport name", `{"name":"run_officejs","arguments":{"code":"{\"name\":\"run_officejs\",\"arguments\":{\"code\":\"{\\\"name\\\":\\\"run_officejs\\\"}\"}}"}}`},
		{"missing code", nil},
		{"custom tool via arguments", `{"name":"apply_patch","arguments":{"patch":"x"}}`},
	}
	for _, tc := range rejected {
		native := bpsTestNativeCall(t, "bad_"+tc.name, map[string]any{"summary": "s", "code": tc.code})
		_, err := bridge.translateCall(native)
		require.Error(t, err, tc.name)
		require.NotContains(t, err.Error(), "rm_rf", "错误不带客户端内容")
	}
}

func TestOpenAIBasisPointsCustomToolTransports(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	patch := "*** Begin Patch\n*** Update File: a.go\n@@\n-x \"q\" \\ y\n+z\n*** End Patch"

	marked := bpsTestNativeCall(t, "call_marked", map[string]any{"summary": openAIBasisPointsCustomMarker + "apply_patch", "code": patch})
	call, err := bridge.translateCall(marked)
	require.NoError(t, err)
	require.Equal(t, "custom_tool_call", bpsText(call["type"]))
	require.Equal(t, "apply_patch", bpsText(call["name"]))
	require.Equal(t, patch, bpsText(call["input"]), "原文直传，不经 JSON")
	require.True(t, strings.HasPrefix(bpsText(call["id"]), "ctc_"))

	envelope := bpsTestNativeCall(t, "call_env", map[string]any{"summary": "s", "code": `{"name":"apply_patch","input":"*** Begin Patch\n*** End Patch"}`})
	call, err = bridge.translateCall(envelope)
	require.NoError(t, err)
	require.Equal(t, "*** Begin Patch\n*** End Patch", bpsText(call["input"]))

	_, err = bridge.translateCall(bpsTestNativeCall(t, "call_fn_marked", map[string]any{"summary": openAIBasisPointsCustomMarker + "exec_command", "code": "pwd"}))
	require.Error(t, err, "标记只给 custom 工具")
	_, err = bridge.translateCall(bpsTestNativeCall(t, "call_unknown_marked", map[string]any{"summary": openAIBasisPointsCustomMarker + "nope", "code": "x"}))
	require.Error(t, err)
}

// 标记的近失：大小写、前后空白、标记与名字之间多一个空格。逐字比较会把它们当成「没打标记」，
// 于是拿 patch 原文去解 JSON → 必败 → 整轮死（落回已改成硬报错，代价更大）。
// 只放宽到能精确识别的近失：summary 是别的内容时仍然走 FUNCTION 信封那条路。
func TestOpenAIBasisPointsCustomMarkerToleratesNearMisses(t *testing.T) {
	patch := "*** Begin Patch\n*** End Patch"
	for _, summary := range []string{
		openAIBasisPointsCustomMarker + "apply_patch",
		"  " + openAIBasisPointsCustomMarker + "apply_patch  ",
		openAIBasisPointsCustomMarker + " apply_patch",
		strings.ToUpper(openAIBasisPointsCustomMarker) + "apply_patch",
	} {
		bridge := newBasisPointsTestBridge(t, bpsTestTools)
		call, err := bridge.translateCall(bpsTestNativeCall(t, "near_"+summary, map[string]any{"summary": summary, "code": patch}))
		require.NoError(t, err, summary)
		require.Equal(t, "custom_tool_call", bpsText(call["type"]), summary)
		require.Equal(t, "apply_patch", bpsText(call["name"]), summary)
		require.Equal(t, patch, bpsText(call["input"]), summary)
	}
	// summary 完全是别的东西时不猜：仍按 FUNCTION 信封解 code，解不开就报错。
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	_, err := bridge.translateCall(bpsTestNativeCall(t, "plain_summary",
		map[string]any{"summary": "Applying the patch to a.go", "code": patch}))
	require.Error(t, err, "没打标记的原文不猜成某个 custom 工具")
}

// 原来 translateResponse 恒写 parallel_tool_calls=false，而所有工具项都会被补发出去 ——
// 客户端拿到「声明不许并行 + 多个并行调用」的自相矛盾响应。
func TestOpenAIBasisPointsResponseEchoesParallelToolCalls(t *testing.T) {
	for _, tc := range []struct {
		body string
		want bool
	}{
		{bpsTestClientBody, true}, // 客户端没给 → Responses API 的默认 true
		{strings.Replace(bpsTestClientBody, `"store":false`, `"store":false,"parallel_tool_calls":false`, 1), false},
		{strings.Replace(bpsTestClientBody, `"store":false`, `"store":false,"parallel_tool_calls":true`, 1), true},
	} {
		bridge := newOpenAIBasisPointsBridge("scope", newOpenAIBasisPointsReplayCache(), nil)
		_, err := bridge.prepare([]byte(tc.body), "gpt-6-astra")
		require.NoError(t, err)
		response := bpsObject{"output": []any{}}
		require.NoError(t, bridge.translateResponse(response))
		require.Equal(t, tc.want, response["parallel_tool_calls"])
	}
}

func TestOpenAIBasisPointsDirectCatalogCall(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	direct := bpsObject{"type": "function_call", "id": "fc_d", "call_id": "call_d", "name": "functions.exec_command", "arguments": `{"cmd":"ls"}`}
	call, err := bridge.translateCall(direct)
	require.NoError(t, err)
	require.Equal(t, "exec_command", bpsText(call["name"]))
	require.JSONEq(t, `{"cmd":"ls"}`, bpsText(call["arguments"]))
	replayed := bridge.replay.get("scope", "call_d")
	require.Equal(t, "run_officejs", bpsText(replayed["name"]), "下一轮以传输项回放")

	custom := bpsObject{"type": "custom_tool_call", "id": "ctc_x", "call_id": "call_c", "name": "apply_patch", "input": "patch"}
	call, err = bridge.translateCall(custom)
	require.NoError(t, err)
	require.Equal(t, "custom_tool_call", bpsText(call["type"]))
	require.Equal(t, "patch", bpsText(call["input"]))

	_, err = bridge.translateCall(bpsObject{"type": "function_call", "call_id": "call_u", "name": "list_skills", "arguments": "{}"})
	require.Error(t, err)
	_, err = bridge.translateCall(bpsObject{"type": "custom_tool_call", "call_id": "call_k", "name": "exec_command", "input": "x"})
	require.Error(t, err, "类型不匹配不放行")
}

func TestOpenAIBasisPointsNativeUpdatePlan(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	native := bpsObject{"type": "function_call", "id": "fc_p", "call_id": "call_p", "name": "update_plan",
		"arguments": `{"summary":"why","plan":[{"description":"read code","status":"done"},{"title":"write","status":"in-progress"},{"step":"test","status":"todo"}]}`}
	call, err := bridge.translateCall(native)
	require.NoError(t, err)
	require.Equal(t, "update_plan", bpsText(call["name"]))
	args := bpsText(call["arguments"])
	require.Equal(t, "why", gjson.Get(args, "explanation").String())
	require.Equal(t, "completed", gjson.Get(args, "plan.0.status").String())
	require.Equal(t, "read code", gjson.Get(args, "plan.0.step").String())
	require.Equal(t, "in_progress", gjson.Get(args, "plan.1.status").String())
	require.Equal(t, "pending", gjson.Get(args, "plan.2.status").String())
	// run_officejs 中继那条路早就带这个字段，原生 update_plan 这条漏了：Codex 的协作工具靠
	// 「显式空列表 vs 字段缺失」区分明文与密文，缺了它客户端会把这份明文 arguments 当
	// encrypted_content 塞给子 agent。两条路必须同口径。
	require.Equal(t, []any{}, call["encrypted_function_args"], "显式空列表：客户端靠它区分明文与密文")
	// 64 字符上限对**两条分支**都生效。原来带 fc_ 前缀的直接原样返回、跳过上限，于是同一个 call
	// 的调用项 id 被指纹化、输出项 id 却超长原样出站（rebuild 那条是无条件截的）。
	long := "fc_" + strings.Repeat("x", 80)
	require.LessOrEqual(t, len(openAIBasisPointsFunctionItemID(long)), 64, "带 fc_ 前缀也要截")
	require.Equal(t, "fc_short", openAIBasisPointsFunctionItemID("fc_short"), "没超长就原样")
	require.NotNil(t, bridge.replay.get("scope", "call_p"))

	_, err = bridge.translateCall(bpsObject{"type": "function_call", "call_id": "call_bad", "name": "update_plan", "arguments": `{"plan":[{"step":"x","status":"weird"}]}`})
	require.Error(t, err)

	noPlanTool := newBasisPointsTestBridge(t, `[{"type":"function","name":"exec_command","parameters":{"type":"object"}}]`)
	_, err = noPlanTool.translateCall(native)
	require.Error(t, err, "客户端没声明 update_plan 就不能转")
}

func TestOpenAIBasisPointsReplayCacheMatchesOnArguments(t *testing.T) {
	cache := newOpenAIBasisPointsReplayCache()
	native := bpsObject{"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "run_officejs", "arguments": "{}", "summary": "s"}
	client := bpsObject{"type": "function_call", "call_id": "call_1", "name": "exec_command", "arguments": `{"cmd":"pwd"}`}
	cache.put("a", "call_1", native, client)

	same := bpsObject{"type": "function_call", "call_id": "call_1", "name": "exec_command", "arguments": `{ "cmd" : "pwd" }`, "id": "other", "status": "completed"}
	require.Equal(t, "s", bpsText(cache.getForCall("a", "call_1", same)["summary"]), "键序 / 空白 / 线上字段不影响匹配")
	different := bpsObject{"type": "function_call", "call_id": "call_1", "name": "exec_command", "arguments": `{"cmd":"rm"}`}
	require.Nil(t, cache.getForCall("a", "call_1", different), "同 call_id 不同参数不算同一次调用")
	require.Nil(t, cache.getForCall("b", "call_1", same), "作用域隔离")
	require.NotNil(t, cache.get("a", "call_1"))
	require.Nil(t, cache.get("a", "call_2"))

	returned := cache.get("a", "call_1")
	returned["summary"] = "mutated"
	require.Equal(t, "s", bpsText(cache.get("a", "call_1")["summary"]), "返回副本")
}

func TestOpenAIBasisPointsPrepareShape(t *testing.T) {
	bridge := newOpenAIBasisPointsBridge("scope", newOpenAIBasisPointsReplayCache(), nil)
	body, err := bridge.prepare([]byte(`{"model":"gpt-6-astra","input":"hello <b>","reasoning":{"effort":"ultra"},"metadata":{"x":"y"},"context_management":[{"type":"compaction","compact_threshold":5}]}`), "gpt-6-astra")
	require.NoError(t, err)
	require.Equal(t, "xhigh", gjson.GetBytes(body, "reasoning_effort").String())
	require.Equal(t, "user", gjson.GetBytes(body, "input.2.role").String())
	require.Equal(t, "hello <b>", gjson.GetBytes(body, "input.2.content.0.text").String())
	require.Contains(t, string(body), "hello <b>")
	require.Equal(t, "developer", gjson.GetBytes(body, "input.0.role").String())
	require.Equal(t, defaultCodexSynthInstructions("gpt-6-astra"),
		gjson.GetBytes(body, "input.0.content.0.text").String(),
		"客户端没给 instructions 时补原路径的默认值")
	require.Contains(t, gjson.GetBytes(body, "input.1.content.0.text").String(), "Do not call Excel", "没有工具时的简短协议说明")
	require.False(t, gjson.GetBytes(body, "instructions").Exists(), "instructions 不出站，只降级成 developer 消息")
	require.Equal(t, int64(5), gjson.GetBytes(body, "context_management.0.compact_threshold").Int(), "客户端自己的 compaction 设置优先")

	bare, err := bridge.prepare([]byte(`{"model":"gpt-6-astra","input":"hi","context_management":[]}`), "gpt-6-astra")
	require.NoError(t, err)
	require.Equal(t, int64(872000), gjson.GetBytes(bare, "context_management.0.compact_threshold").Int(),
		"客户端没给（或给空数组）时用 BPS 实测上限 918k 的 95%，不是拍的 200k / 也不是按别名 500k 算的 475k")

	clamped, err := bridge.prepare([]byte(`{"model":"gpt-6-astra","input":"hi","context_management":[{"type":"compaction","compact_threshold":9000000}]}`), "gpt-6-astra")
	require.NoError(t, err)
	require.Equal(t, int64(872000), gjson.GetBytes(clamped, "context_management.0.compact_threshold").Int(),
		"客户端给的阈值超过实测上限就钳回默认值")

	for _, raw := range []string{
		`{"model":"gpt-6-astra","input":"hi","context_management":[{"type":"truncation","strategy":"middle_out"}]}`,
		`{"model":"gpt-6-astra","input":"hi","context_management":[{"type":"compaction"},{"type":"compaction"}]}`,
	} {
		_, err := bridge.prepare([]byte(raw), "gpt-6-astra")
		reason, native := openAIBasisPointsNativeReason(err)
		require.True(t, native, raw)
		require.Equal(t, "context_management", reason, "BPS 不认的 context_management 形态不能原样转发去赌 422")
	}
	require.False(t, gjson.GetBytes(body, "metadata.x").Exists(), "只发 BPS 认的 metadata")
	require.NotContains(t, string(body), `\u003c`, "不做 HTML 转义")

	// prepare 只保留它独有的拒绝：请求体形态不对。目录解不开已经不再判死（认不出的条目丢进
	// b.unsupported，见 collectTools），tool_catalog 这个原因随之作废。
	for want, raw := range map[string]string{
		"request_json": `{"model":"m","input":5}`,
	} {
		_, err := bridge.prepare([]byte(raw), "m")
		reason, native := openAIBasisPointsNativeReason(err)
		require.True(t, native, want)
		require.Equal(t, want, reason)
	}
	// 无名 function 声明：丢出目录 + 在提示里点名，**不判死**。
	// 独立 bridge：顶上那个跨多次 prepare 复用，b.unsupported 会累积，而下面钉的是排序后首项。
	noName, err := newBasisPointsTestBridge(t, "[]").prepare([]byte(`{"model":"m","input":"x","tools":[{"type":"function"}]}`), "m")
	require.NoError(t, err, "认不出的工具声明不再判死")
	require.Contains(t, bpsTestProtocolText(t, noName),
		"Hosted tools unavailable through this channel: function",
		"被丢掉的声明必须在提示里点名，否则模型会声称用过")

	// 承载不了的原生工具项换成占位 developer 消息，**不判死**（2026-09-30）。
	placeheld, err := bridge.prepare([]byte(`{"model":"m","input":[{"type":"tool_call","id":"tc_1"}]}`), "m")
	require.NoError(t, err, "原生工具项不再判死")
	items := gjson.GetBytes(placeheld, "input").Array()
	last := items[len(items)-1]
	require.Equal(t, "message", last.Get("type").String())
	require.Equal(t, "developer", last.Get("role").String())
	require.Equal(t, openAIBasisPointsDroppedItemNotice("tool_call"), last.Get("content.0.text").String())
	// **断言结构，不要 NotContains 整个 body**：占位文案里本来就含 `tool_call`，那条只是因为
	// droppedItemNotice 没给类型名加引号才过 —— 换个 fixture 类型或给文案加上引号都会让它变红，
	// 而两次都不是行为回归。
	gjson.GetBytes(placeheld, "input").ForEach(func(_, item gjson.Result) bool {
		require.NotEqual(t, "tool_call", item.Get("type").String(), "原项不许出站")
		return true
	})
}

// 这些判定**只在 openAIBasisPointsRouteReason 里**（beginOpenAIBasisPoints 里先跑）。以前 prepare
// 也各抄一份，两份已经开始漂移 —— 而「同一判定写在两处、其中一处看的是不同的 body」正是 Fast
// 策略闸门连续两轮出 bug 的同一个模式。这里钉单一来源。
func TestOpenAIBasisPointsRouteReasonIsTheSingleSourceOfPreflightRejects(t *testing.T) {
	for want, raw := range map[string]string{
		"history_reference":       `{"model":"m","input":"x","previous_response_id":"r"}`,
		"output_format":           `{"model":"m","input":"x","text":{"format":{"type":"json_object"}}}`,
		"reasoning_configuration": `{"model":"m","input":"x","reasoning":{"effort":"high","mode":"persistent"}}`,
		"prompt_template":         `{"model":"m","input":"x","prompt":{"id":"p"}}`,
	} {
		require.Equal(t, want, openAIBasisPointsRouteReason([]byte(raw)), want)
	}
	// tool_choice 已经完全不判死：none 不写目录，其余取值写进目录说明由模型遵守。
	for _, raw := range []string{
		`{"model":"m","input":"x","tool_choice":"auto"}`,
		`{"model":"m","input":"x","tool_choice":"none"}`,
		// tool_choice 的**类型**不再判死；「强制一个没声明的工具」那条判在 prepare（它要先建目录
		// 才知道声明了什么，routeReason 建不了），用例见 …ForcedUndeclaredToolIsRejected。
		// 所以这里只放不会触发那条的取值。
		`{"model":"m","input":"x","tool_choice":{"type":"web_search"}}`,
		`{"model":"m","input":"x","reasoning":{"effort":"high","mode":"standard"}}`,
		`{"model":"m","input":"x","text":{"format":{"type":"text"}}}`,
	} {
		require.Empty(t, openAIBasisPointsRouteReason([]byte(raw)), raw)
	}
	// 上面只钉了「routeReason 认得这些」，不钉「prepare 不再重复判」—— 谁把那几条抄回 prepare，
	// 用例照样绿，而那正是要防的模式。所以反向再钉一遍：这四个 body 已经被 routeReason 放行，
	// prepare 里不许再有第二份判定把它们拦下来。
	for _, raw := range []string{
		`{"model":"m","input":"x","previous_response_id":"r"}`,
		`{"model":"m","input":"x","text":{"format":{"type":"json_object"}}}`,
		`{"model":"m","input":"x","reasoning":{"effort":"high","mode":"persistent"}}`,
		// tool_choice 的**类型**同样不许在 prepare 里判（原来两处都判）。「强制一个没声明的工具」
		// 是另一回事：那条只能判在 prepare（要先建目录），routeReason 里没有它，不构成两份判定。
		`{"model":"m","input":"x","tool_choice":{"type":"web_search"}}`,
	} {
		_, err := newOpenAIBasisPointsBridge("scope", newOpenAIBasisPointsReplayCache(), nil).
			prepare([]byte(raw), "m")
		require.NoError(t, err, "prepare 不该再重复判这一条（单一来源是 openAIBasisPointsRouteReason）：%s", raw)
	}
	bridge := newOpenAIBasisPointsBridge("scope", newOpenAIBasisPointsReplayCache(), nil)
	body, err := bridge.prepare([]byte(`{"model":"m","input":"x","tool_choice":"none","tools":[`+
		`{"type":"function","name":"f","parameters":{"type":"object"}}]}`), "m")
	require.NoError(t, err)
	require.NotContains(t, gjson.GetBytes(body, "input.1.content.0.text").String(), `"f"`,
		"tool_choice=none 时不把目录写进提示")
}

func TestOpenAIBasisPointsDataURLDecoding(t *testing.T) {
	jpeg := []byte{0xff, 0xd8, 0xff, 0xe0, 0, 16, 'J', 'F', 'I', 'F'}
	mediaType, data, ok := decodeOpenAIBasisPointsDataURL(
		"data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(jpeg))
	require.True(t, ok)
	require.Equal(t, "image/jpeg", mediaType)
	require.Equal(t, jpeg, data)
	// 折行与缺 padding：切成 4 字符一段再插换行，去掉尾部的 =。
	encoded := base64.StdEncoding.EncodeToString(jpeg)
	wrapped := strings.TrimRight(encoded[:4]+"\n"+encoded[4:], "=")
	_, data, ok = decodeOpenAIBasisPointsDataURL("data:image/jpeg;base64," + wrapped)
	require.True(t, ok, "容忍折行与缺 padding")
	require.Equal(t, jpeg, data)
	_, _, ok = decodeOpenAIBasisPointsDataURL("data:image/png,plain")
	require.False(t, ok, "非 base64")
	_, _, ok = decodeOpenAIBasisPointsDataURL("data:image/png;base64,")
	require.False(t, ok, "空图")
}

// 出站 multipart 的 Content-Type / filename 一个字节都不许来自客户端：multipart.CreatePart
// 对头值既不转义也不校验，放任意串进去 = 持有本站 API key 的人能在带着账号主人 bearer token 的
// 上传请求里注入任意 MIME 部件头，还能在文件体前塞字节、覆盖 Content-Disposition。
// 现在类型按字节识别、只可能是四种之一，注入面从根上没了；这条用例钉的是畸形声明照旧被拒。
func TestOpenAIBasisPointsDataURLRejectsUnlistedMediaType(t *testing.T) {
	png := base64.StdEncoding.EncodeToString([]byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3})
	for _, name := range []string{
		// 注入串的声明段**确实**以 image/ 开头；拦住它的是 mime.FormatMediaType(declared,nil) != declared
		// —— CR/LF 与非 token 字符过不了那道形状校验。
		"data:image/png\r\nX-Injected: yes\r\nContent-Disposition: form-data;base64," + png,
		"data:application/x-msdownload;base64," + png,
		"data:text/html;base64," + png,
		"data:;base64," + png,
	} {
		_, _, ok := decodeOpenAIBasisPointsDataURL(name)
		require.False(t, ok, name)
	}
	// 声明合法且字节是真 PNG 才通过，返回的类型来自字节。
	got, _, ok := decodeOpenAIBasisPointsDataURL("data:image/png;base64," + png)
	require.True(t, ok)
	require.Equal(t, "image/png", got)
}

// 单条就超 maxBytes 时字节淘汰条件永远为真，淘干了还继续 c.order.Front() 就是 nil。
func TestBPSLRUOversizedEntryDoesNotPanic(t *testing.T) {
	cache := newBPSLRU(8, 1024)
	require.NotPanics(t, func() { cache.put("big", "v", 4096) })
	_, ok := cache.get("big")
	require.False(t, ok, "超出字节上限的条目不留下")
	cache.put("small", "v", 16)
	got, ok := cache.get("small")
	require.True(t, ok, "淘汰过头不该把缓存弄坏")
	require.Equal(t, "v", got)
}

func TestOpenAIBasisPointsTransportCodeAcceptsJSAssignmentWrapper(t *testing.T) {
	// ghcp_proxy 的测试里模型真会把信封写成 `const request = {...};`：只剥外壳取 JSON，不执行任何代码。
	for _, code := range []string{
		"const request = {\"name\":\"exec_command\",\"arguments\":{\"cmd\":\"pwd\"}};",
		"let req={\"tool\":\"exec_command\",\"args\":{\"cmd\":\"pwd\"}}",
	} {
		envelope, err := decodeOpenAIBasisPointsTransportEnvelope(code)
		require.NoError(t, err, code)
		require.Equal(t, "exec_command", bpsText(envelope["name"])+bpsText(envelope["tool"]), code)
	}
	for _, code := range []string{
		"const request = fetch({\"name\":\"exec_command\"});",
		"request = {\"name\":\"exec_command\"}",
		"const x = {\"name\":\"exec_command\"}; run(x);",
		"const a.b = {\"name\":\"exec_command\"}",
	} {
		_, err := decodeOpenAIBasisPointsTransportEnvelope(code)
		require.Error(t, err, code)
	}
}

func TestOpenAIBasisPointsHistoryRewritesNativePlanOutputAndItemIDs(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	native := bpsObject{"type": "function_call", "id": "fc_plan", "call_id": "call_plan", "name": "update_plan",
		"arguments": `{"plan":[{"step":"a","status":"pending"}]}`, "status": "completed"}
	translated, err := bridge.translateCall(native)
	require.NoError(t, err)
	require.Equal(t, "update_plan", bpsText(translated["name"]))

	history, err := bridge.translateHistory([]any{
		bpsObject{"type": "function_call", "id": "fc_plan", "call_id": "call_plan", "name": "update_plan", "arguments": bpsText(translated["arguments"])},
		bpsObject{"type": "function_call_output", "call_id": "call_plan", "output": "Plan updated"},
		bpsObject{"type": "custom_tool_call", "call_id": "call_patch", "name": "apply_patch", "input": "*** Begin Patch"},
		bpsObject{"type": "custom_tool_call_output", "id": "ctco_patch", "call_id": "call_patch", "output": ""},
	})
	require.NoError(t, err)
	require.Len(t, history, 4)
	planCall, _ := history[0].(bpsObject)
	require.Equal(t, "update_plan", bpsText(planCall["name"]), "原生 update_plan 原样回放")
	planOutput, _ := history[1].(bpsObject)
	require.Equal(t, `{"status":"ok"}`, bpsText(planOutput["output"]), "BPS 自带的 update_plan 只认 status ok")
	require.Equal(t, "fc_call_plan", bpsText(planOutput["id"]))
	patchOutput, _ := history[3].(bpsObject)
	require.Equal(t, "function_call_output", bpsText(patchOutput["type"]))
	require.Equal(t, "fc_call_patch", bpsText(patchOutput["id"]), "ctco_ 前缀 BPS 不认")
	require.Equal(t, openAIBasisPointsEmptyOutput, bpsText(patchOutput["output"]))
}

// 认不出的工具声明一律丢进目录说明、绝不判死（参考实现 cpa-plugin-oai-basispoints v0.2.9 的口径）。
// 判死发生在出站之前 ⇒ 客户端每轮回放同一份 tools ⇒ 开着开关的账号上这个会话每轮都 502、永不自愈。
func TestOpenAIBasisPoints_UnknownToolDeclarationsAreDroppedNotFatal(t *testing.T) {
	cases := []struct {
		name  string
		tools string
		noted string
	}{
		{"托管 mcp", `[{"type":"mcp","server_label":"x"}]`, "mcp"},
		{"生图", `[{"type":"image_generation"}]`, "image_generation"},
		{"Codex 无 name 的 local_shell", `[{"type":"local_shell"}]`, "local_shell"},
		{"未来类型", `[{"type":"zzz_future_tool","name":"f"}]`, "zzz_future_tool"},
		{"无 name 的 function", `[{"type":"function"}]`, "function"},
		{"非对象条目", `["nope"]`, "(malformed)"},
		{"无名 namespace", `[{"type":"namespace","tools":[{"type":"function","name":"f"}]}]`, "namespace"},
		{"namespace 里的托管工具", `[{"type":"namespace","name":"n","tools":[{"type":"code_interpreter"}]}]`, "code_interpreter"},
	}
	for _, tc := range cases {
		bridge := newBasisPointsTestBridge(t, "[]")
		body, err := bridge.prepare([]byte(`{"model":"m","input":"hi","tools":`+tc.tools+`}`), "m")
		require.NoError(t, err, tc.name)
		protocol := bpsTestProtocolText(t, body)
		require.Contains(t, protocol, "Hosted tools unavailable through this channel: "+tc.noted, tc.name)
		require.Contains(t, protocol, "Do not claim to have used them", tc.name)
		require.False(t, gjson.GetBytes(body, "tools").Exists(), tc.name+"：出站不带 tools")
	}
}

// 同名工具声明两次、定义不同：后一条赢（Codex 的 additional_tools 正是用后一份更新同名 schema），
// 且目录文本里只能出现一次 —— 留旧 schema 会让模型照着目录填参数、再被本层的参数校验拒掉。
func TestOpenAIBasisPoints_RedeclaredToolKeepsTheLastDefinition(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	raw := `{"model":"m","input":[{"type":"additional_tools","tools":[` +
		`{"type":"function","name":"f","description":"description-last","parameters":{"type":"object","properties":{"b":{"type":"string"}}}}]},` +
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],` +
		`"tools":[{"type":"function","name":"f","description":"description-first","parameters":{"type":"object","properties":{"a":{"type":"string"}}}}]}`
	body, err := bridge.prepare([]byte(raw), "m")
	require.NoError(t, err, "重复声明不再判死")
	protocol := bpsTestProtocolText(t, body)
	// 不要断言裸 "new"/"old"：固定文案里有 "newline"，那条正例靠 boilerplate 就能过。
	require.Contains(t, protocol, "description-last", "目录要描述最后那份定义")
	require.NotContains(t, protocol, "description-first", "旧定义必须从目录里去掉")
	require.Equal(t, 1, strings.Count(protocol, `Client tool "f"`), "同名工具只能出现一次")
	// definition 是指纹哈希，认不出文本；参数 schema 才是模型填参数时要对上的那份。
	properties, _ := bridge.tools["f"].parameters["properties"].(bpsObject)
	require.Contains(t, properties, "b", "参数校验必须按最后那份 schema 走")
	require.NotContains(t, properties, "a")
}

// tool_choice 不再判死，改成写进目录说明由模型遵守（出站 schema 封闭，这个字段送不上去）。
func TestOpenAIBasisPoints_ToolChoiceIsDescribedInTheCatalog(t *testing.T) {
	cases := []struct {
		name, choice, want string
	}{
		{"required", `"required"`, "requires exactly one client tool call"},
		{"强制点名", `{"type":"function","name":"exec_command"}`, `call the client tool "exec_command" and no other`},
		{"allowed_tools auto", `{"type":"allowed_tools","tools":[{"type":"function","name":"exec_command"}]}`, `may only call`},
		{"allowed_tools required", `{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"exec_command"}]}`, `must call one of`},
	}
	for _, tc := range cases {
		bridge := newBasisPointsTestBridge(t, "[]")
		body, err := bridge.prepare([]byte(`{"model":"m","input":"hi","tool_choice":`+tc.choice+
			`,"tools":[{"type":"function","name":"exec_command","parameters":{"type":"object","properties":{}}}]}`), "m")
		require.NoError(t, err, tc.name)
		require.Contains(t, bpsTestProtocolText(t, body), tc.want, tc.name)
	}
	// auto / 认不出的形态：不提约束，宁可不提也别把猜错的约束塞给模型。
	for _, choice := range []string{`"auto"`, `{"type":"web_search"}`, `{"type":"allowed_tools","tools":[]}`} {
		bridge := newBasisPointsTestBridge(t, "[]")
		body, err := bridge.prepare([]byte(`{"model":"m","input":"hi","tool_choice":`+choice+`}`), "m")
		require.NoError(t, err, choice)
		protocol := bpsTestProtocolText(t, body)
		require.NotContains(t, protocol, "The client requires", choice)
		require.NotContains(t, protocol, "The client restricts", choice)
	}
	// none：连目录都不写。
	bridge := newBasisPointsTestBridge(t, "[]")
	body, err := bridge.prepare([]byte(`{"model":"m","input":"hi","tool_choice":"none",`+
		`"tools":[{"type":"function","name":"exec_command","parameters":{"type":"object","properties":{}}}]}`), "m")
	require.NoError(t, err)
	protocol := bpsTestProtocolText(t, body)
	require.NotContains(t, protocol, "exec_command", "none 时不写目录")
	require.NotContains(t, protocol, "The client requires")
}

// item_reference / configuration_update 展不开，但也不许判死：落到占位项，会话能往下走。
func TestOpenAIBasisPoints_UnexpandableItemsBecomePlaceholders(t *testing.T) {
	// configuration_update **不**在这里：它是客户端在这一轮要求改推理配置，不是历史里躺着的内容，
	// 换占位符等于静默丢掉客户端明确要求的能力还返 200。与顶层 reasoning.mode 对齐硬报错。
	_, err := newBasisPointsTestBridge(t, "[]").prepare([]byte(
		`{"model":"m","input":[{"type":"configuration_update"},`+
			`{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`), "m")
	reason, native := openAIBasisPointsNativeReason(err)
	require.True(t, native, "configuration_update 要硬报错")
	require.Equal(t, "reasoning_configuration", reason, "与顶层 reasoning.mode 同一个原因")

	for _, kind := range []string{"item_reference"} {
		bridge := newBasisPointsTestBridge(t, "[]")
		body, err := bridge.prepare([]byte(`{"model":"m","input":[{"type":"`+kind+`","id":"x"},`+
			`{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`), "m")
		require.NoError(t, err, kind)
		require.Contains(t, string(body), "an earlier "+kind+" item is not supported over this channel", kind)
		require.Contains(t, string(body), "Do not claim to have used it or invent its result", kind)
		// 断言结构，不要 NotContains 整个 body（同文件上面那段注释警告过：给文案加上引号
		// 或换 fixture 类型都会让它假红/假绿）。
		for _, item := range gjson.GetBytes(body, "input").Array() {
			require.NotEqual(t, kind, item.Get("type").String(), kind+" 原样项不能送上游")
		}
	}
}

// S1：强制调一个没声明的工具 ⇒ 请求级拒收（参考实现 prepareResponsesBody 同一条）。
// 不能只写进提示：那样模型拿到「随便挑的目录」+「只准调 x」两句打架的话，整轮照样烧掉。
func TestOpenAIBasisPoints_ForcedUndeclaredToolIsRejected(t *testing.T) {
	declared := `"tools":[{"type":"function","name":"exec_command","parameters":{"type":"object","properties":{}}}]`
	rejected := map[string]string{
		"required 但没声明任何工具":           `{"model":"m","input":"hi","tool_choice":"required"}`,
		"点名一个不存在的工具":                  `{"model":"m","input":"hi","tool_choice":{"type":"function","name":"nope"},` + declared + `}`,
		"点名的 namespace 对不上":           `{"model":"m","input":"hi","tool_choice":{"type":"function","name":"exec_command","namespace":"a"},` + declared + `}`,
		"allowed_tools required 全不存在": `{"model":"m","input":"hi","tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"nope"}]},` + declared + `}`,
	}
	for name, raw := range rejected {
		_, err := newBasisPointsTestBridge(t, "[]").prepare([]byte(raw), "m")
		reason, native := openAIBasisPointsNativeReason(err)
		require.True(t, native, name)
		require.Equal(t, "tool_choice", reason, name)
	}
	accepted := map[string]string{
		"required + 有声明":                 `{"model":"m","input":"hi","tool_choice":"required",` + declared + `}`,
		"点名一个真声明了的":                      `{"model":"m","input":"hi","tool_choice":{"type":"function","name":"exec_command"},` + declared + `}`,
		"Chat 风格 function.name":          `{"model":"m","input":"hi","tool_choice":{"type":"function","function":{"name":"exec_command"}},` + declared + `}`,
		"allowed_tools auto 不算 required": `{"model":"m","input":"hi","tool_choice":{"type":"allowed_tools","tools":[{"type":"function","name":"nope"}]},` + declared + `}`,
		"托管工具不算 required":                `{"model":"m","input":"hi","tool_choice":{"type":"web_search"},` + declared + `}`,
		"none":                           `{"model":"m","input":"hi","tool_choice":"none",` + declared + `}`,
	}
	for name, raw := range accepted {
		_, err := newBasisPointsTestBridge(t, "[]").prepare([]byte(raw), "m")
		require.NoError(t, err, name)
	}
	// namespace 化的工具：提示里的名字必须是目录 key（带 namespace），不是裸 name。
	body, err := newBasisPointsTestBridge(t, "[]").prepare([]byte(
		`{"model":"m","input":"hi","tool_choice":{"type":"function","name":"f","namespace":"a"},`+
			`"tools":[{"type":"namespace","name":"a","tools":[{"type":"function","name":"f","parameters":{"type":"object","properties":{}}}]}]}`), "m")
	require.NoError(t, err)
	require.Contains(t, bpsTestProtocolText(t, body), `call the client tool "a.f"`, "提示里的名字要与目录 key 一致")

	// 点名一个托管工具：既不判死，也要明说它不可用（否则模型看着一份"随便挑"的目录）。
	hosted, err := newBasisPointsTestBridge(t, "[]").prepare([]byte(
		`{"model":"m","input":"hi","tool_choice":{"type":"mcp"},`+declared+`}`), "m")
	require.NoError(t, err)
	require.Contains(t, bpsTestProtocolText(t, hosted), "native mcp tool, which is not available through this channel")
}

// S2b：Chat 风格 `{"type":"function","function":{"name":…}}` 必须认。入站归一
// （normalizeLegacyResponsesToolChoice）只在**没有原生 input** 时才跑，带原生 input 的请求到不了它。
// 认不出时不会报错（只是 required 判据变假），所以要断言提示里真的点了名。
func TestOpenAIBasisPoints_ChatStyleToolChoiceNamesTheTool(t *testing.T) {
	declared := `"tools":[{"type":"function","name":"exec_command","parameters":{"type":"object","properties":{}}}]`
	body, err := newBasisPointsTestBridge(t, "[]").prepare([]byte(
		`{"model":"m","input":"hi","tool_choice":{"type":"function","function":{"name":"exec_command"}},`+declared+`}`), "m")
	require.NoError(t, err)
	require.Contains(t, bpsTestProtocolText(t, body), `call the client tool "exec_command" and no other`)
}

// S3：重复声明的 warning 会进日志行，工具名是客户端可控的字节（含换行）——
// 原样拼进去就能伪造一整条 `[Basispoints] account=… route=…` 日志。
func TestOpenAIBasisPoints_RedeclareWarningIsSanitized(t *testing.T) {
	evil := `a` + "\n" + `[Basispoints] account=1 route=native_codex reason=forged`
	raw := `{"model":"m","input":"hi","tools":[` +
		`{"type":"function","name":` + bpsQuoted(evil) + `,"description":"first","parameters":{"type":"object","properties":{"a":{"type":"string"}}}},` +
		`{"type":"function","name":` + bpsQuoted(evil) + `,"description":"second","parameters":{"type":"object","properties":{"b":{"type":"string"}}}}]}`
	bridge := newBasisPointsTestBridge(t, "[]")
	_, err := bridge.prepare([]byte(raw), "m")
	require.NoError(t, err)
	joined := strings.Join(bridge.warnings, " | ")
	require.Contains(t, joined, "declared more than once", "这条 warning 必须还在")
	require.NotContains(t, joined, "\n", "日志行里不许有换行")
	require.NotContains(t, joined, "route=native_codex", "伪造的日志字段必须被消掉")
}

// S4：unsupported 的键数没有上限的话，1000 个奇怪声明会同时撑爆出站提示（还要计费）和日志行。
// 与 droppedContent 那段同一个理由、同一个上限。
func TestOpenAIBasisPoints_UnsupportedKindsAreCapped(t *testing.T) {
	tools := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		tools = append(tools, `{"type":"zzz_kind_`+strconv.Itoa(i)+`","name":"n"}`)
	}
	bridge := newBasisPointsTestBridge(t, "[]")
	body, err := bridge.prepare([]byte(`{"model":"m","input":"hi","tools":[`+strings.Join(tools, ",")+`]}`), "m")
	require.NoError(t, err)
	protocol := bpsTestProtocolText(t, body)
	require.Contains(t, protocol, "…", "超过上限要截断")
	require.LessOrEqual(t, strings.Count(protocol, "zzz_kind_"), 16, "出站提示里最多 16 种")
	require.LessOrEqual(t, strings.Count(strings.Join(bridge.warnings, " "), "zzz_kind_"), 16, "日志行同上限")
}

// droppedContent 的 16 种上限同样要钉住。第二轮变异挖错地方时无意间证明了它**原来没有任何测试**
// （删掉上限全仓照旧绿），而这个功能历史上已经多次「注释写的不变式是假的」。
func TestOpenAIBasisPoints_DroppedContentKindsAreCapped(t *testing.T) {
	parts := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		parts = append(parts, `{"type":"zzz_part_`+strconv.Itoa(i)+`"}`)
	}
	bridge := newBasisPointsTestBridge(t, "[]")
	_, err := bridge.prepare([]byte(`{"model":"m","input":[{"type":"message","role":"user","content":[`+
		`{"type":"input_text","text":"hi"},`+strings.Join(parts, ",")+`]}]}`), "m")
	require.NoError(t, err)
	joined := strings.Join(bridge.warnings, " | ")
	require.Contains(t, joined, "Content parts replaced with a placeholder over this channel")
	require.Contains(t, joined, "…", "超过上限要截断")
	require.LessOrEqual(t, strings.Count(joined, "zzz_part_"), 16, "运维日志行里最多 16 种")
}
