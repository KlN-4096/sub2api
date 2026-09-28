package service

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/google/uuid"
)

// Basis Points 的线协议翻译：把标准 Responses 请求改写成 Excel 加载项的形态，再把 BPS 的
// 原生工具调用还原成客户端声明的工具。BPS 不收客户端 tools：目录写进 developer 消息，模型
// 经 BPS 原生的 run_officejs 回传调用，本层从其 code 字段解出真实工具再转给客户端；下一轮
// 客户端回传结果时，把 BPS 原生的 run_officejs 项（含 id / summary 等模型字段）按 call_id
// 从缓存回放给上游，缓存丢了就按客户端历史重建。参考 hloolx/codex2api、
// Kaixxrua/excel-codex-bridge 与 JaxsonWang/cpa-plugin-oai-basispoints 三者的交集。

type bpsObject = map[string]any

const (
	openAIBasisPointsTransportTool = "run_officejs"
	// 自定义工具原文直传的 summary 标记：少一层 JSON 转义，apply_patch / exec 这类大段原文
	// 不容易被模型写坏。前缀刻意不带本服务名字。
	openAIBasisPointsCustomMarker = "client.custom/"
	openAIBasisPointsMaxEnvelope  = 1 << 20
	openAIBasisPointsEmptyOutput  = "(tool call succeeded with no output)"
)

// openAIBasisPointsBridge 一次请求的翻译上下文。
type openAIBasisPointsBridge struct {
	requestedEffort string
	effort          string
	warnings        []string
	tools           map[string]bpsTool
	unsupported     map[string]bool
	replay          *openAIBasisPointsReplayCache
	scope           string
	// parallelToolCalls 是客户端自己请求的值（缺省 true，与 Responses API 同默认），
	// 回写进转写后的 response，别让声明和实际补发的工具数自相矛盾。
	parallelToolCalls bool
	// upload 把用户消息里的 data: 图片传成 BPS 附件，返回 file id；nil 表示本请求不能上传。
	upload func(mediaType string, data []byte) (string, error)
}

type bpsTool struct {
	name       string
	namespace  string
	kind       string
	definition string
	parameters bpsObject
}

func newOpenAIBasisPointsBridge(scope string, replay *openAIBasisPointsReplayCache, upload func(string, []byte) (string, error)) *openAIBasisPointsBridge {
	return &openAIBasisPointsBridge{
		tools:             map[string]bpsTool{},
		unsupported:       map[string]bool{},
		replay:            replay,
		scope:             scope,
		upload:            upload,
		parallelToolCalls: true, // Responses API 的默认值；prepare 里按客户端请求覆盖
	}
}

func bpsDecode(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("expected one JSON value")
	}
	return nil
}

func bpsText(value any) string {
	s, _ := value.(string)
	return s
}

func bpsFingerprint(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:16])
}

func bpsMessage(role, content string) bpsObject {
	return bpsObject{"type": "message", "role": role, "content": []any{bpsObject{"type": "input_text", "text": content}}}
}

// normalizeOpenAIBasisPointsEffort：BPS 只认 low / medium / high / xhigh。
//
// 原来这里对 gpt-6-astra 把 low 钳到 medium（注释引「ghcp_proxy 实测低于 medium 会 422」）。
// 2026-09-29 直连 bps.openai.com 实测 astra + reasoning_effort=low 是 **200**，四家参考实现
// 也只有本地有这条钳制 —— 钳掉等于白吃掉客户端更快更便宜的低档，已移除。
func normalizeOpenAIBasisPointsEffort(effort, _ string) string {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "low", "none", "minimal":
		return "low"
	case "high":
		return "high"
	case "xhigh", "x-high", "extra-high", "extra_high", "max", "ultra":
		return "xhigh"
	default:
		return "medium"
	}
}

// prepare 生成发给 BPS 的请求体。任何 BPS 承载不了的形态都返回 openAIBasisPointsNativeError，
// 由调用方落回原 Codex 路径。
func (b *openAIBasisPointsBridge) prepare(raw []byte, upstreamModel string) ([]byte, error) {
	var source bpsObject
	if err := bpsDecode(raw, &source); err != nil || source == nil {
		return nil, bpsNative("request_json")
	}
	// previous_response_id / tool_choice / text.format / reasoning.mode 的判定**只在
	// openAIBasisPointsRouteReason 里**（openai_basispoints.go，beginOpenAIBasisPoints 里先跑）。
	// 以前这里也各抄一份，两份已经开始漂移（那边查 conversation / prompt / response_format，
	// 这边查 reasoning.mode），而「同一判定写在两处、其中一处看的是不同的 body」正是
	// Fast 策略闸门连续两轮出 bug 的同一个模式。单一来源。
	if parallel, ok := source["parallel_tool_calls"].(bool); ok {
		b.parallelToolCalls = parallel
	}
	b.requestedEffort = bpsText(source["reasoning_effort"])
	if reasoning, ok := source["reasoning"].(bpsObject); ok {
		// reasoning:{} 或 reasoning:{summary:"auto"} 不该把顶层 reasoning_effort 清零。
		if effort := bpsText(reasoning["effort"]); effort != "" {
			b.requestedEffort = effort
		}
	}
	b.effort = normalizeOpenAIBasisPointsEffort(b.requestedEffort, upstreamModel)
	var input []any
	switch v := source["input"].(type) {
	case string:
		input = []any{bpsMessage("user", v)}
	case []any:
		input = v
	default:
		return nil, bpsNative("request_json")
	}
	var catalog []bpsObject
	// tool_choice 的**拒绝**判定在 openAIBasisPointsRouteReason；这里只用它的 "none" 做行为判断
	// （客户端说了别调工具，就别把目录写进提示）。能走到这里的取值只剩 auto / none / 缺失。
	if bpsText(source["tool_choice"]) != "none" {
		var err error
		if catalog, err = b.collectTools(source["tools"], ""); err != nil {
			return nil, err
		}
		for _, raw := range input {
			if item, ok := raw.(bpsObject); ok && bpsText(item["type"]) == "additional_tools" {
				additional, err := b.collectTools(item["tools"], "")
				if err != nil {
					return nil, err
				}
				catalog = append(catalog, additional...)
			}
		}
	}
	// 回合身份在改写 input 之前算：图片换成 file_id 之后再算，重传（缓存过期 / 重启）会让同一回合中途换 turn_id。
	cacheKey := bpsText(source["prompt_cache_key"])
	conversation := cacheKey
	if conversation == "" && len(input) > 0 {
		// 只按 input[0] 取指纹会让「首条 user 相同、后续不同」的两个会话算成同一个 task_id，
		// BPS 侧的 task 状态 / 计划就串了。Codex CLI 总带 prompt_cache_key，裸 SDK 客户端不带，
		// 所以这条兜底是真会被走到的。指纹取全量 input：同一会话下一轮 input 会变长 ⇒ turn_id
		// 照旧每轮变（它本来就是每轮变的），而 task_id 只在这一支里用，宁可偏细不要串会话。
		conversation = bpsFingerprint(input)
	}
	if conversation == "" {
		conversation = "anonymous"
	}
	// Excel 加载项在一个用户回合里跑多次工具时 turn_id 不变、只递增 agent_iteration；把每次
	// 工具回合当成新回合会让 BPS 丢掉上一轮的计划状态重新规划。
	turnEnd := 0
	if len(input) > 0 {
		turnEnd = 1
	}
	iteration := 1
	for i := len(input) - 1; i >= 0; i-- {
		item, _ := input[i].(bpsObject)
		if bpsText(item["role"]) == "user" {
			turnEnd = i + 1
			break
		}
		if strings.HasSuffix(bpsText(item["type"]), "_call_output") {
			iteration++
		}
	}
	taskSeed := "basispoints/" + b.scope + "/" + conversation
	turnFingerprint := bpsFingerprint(input[:turnEnd])

	translated, err := b.translateHistory(input)
	if err != nil {
		return nil, err
	}
	prologue := make([]any, 0, 2)
	// BPS 自己会把 instructions 换成 Excel 那套人格，客户端的系统提示只能降级成 developer 消息。
	// 客户端一个都没给时补上原路径的默认值（openai_gateway_forward.go 里 markPatchSet("instructions", ...)
	// 做的事）——BPS 分派点排在那之前，不补的话同一个裸客户端在两条路上的输出风格会不一样。
	//
	// 但真实 Codex Lite 不能补：它的基础提示已经在 input 的 developer 消息里，原路径为此刻意只
	// 记进 liteFallbackInstructions 而不写进体（openai_gateway_forward.go 的 realCodexLite）。
	// `input.0.type == "additional_tools"` 是这种客户端的签名形态，补一份等于两份系统提示叠加。
	instructions := bpsText(source["instructions"])
	if instructions == "" && !bpsCarriesOwnBaseInstructions(source) {
		instructions = defaultCodexSynthInstructions(upstreamModel)
	}
	if instructions != "" {
		prologue = append(prologue, bpsMessage("developer", instructions))
	}
	prologue = append(prologue, bpsMessage("developer", b.protocolInstructions(catalog)))
	output := bpsObject{
		"model":            upstreamModel,
		"model_selection":  "explicit",
		"stream":           true,
		"store":            false,
		"input":            append(prologue, translated...),
		"reasoning_effort": b.effort,
		// 872000 = BPS 实测上限 918,000 的 95%（excel-codex-bridge 的 _compaction_limits 口径）。
		// 上限来自它的原话：「Measured on the real backend for gpt-5.6-sol, gpt-6-sol, gpt-6-luna
		// and gpt-6-astra alike: 918,843 input tokens accepted, ~921,375 refused with
		// context_length_exceeded」。它的 DEFAULT_CONTEXT_WINDOW=500_000 只是它给 Codex CLI 的
		// 保守别名（同源码注释：-1m 别名才「runs at the longest input the Excel backend accepts」），
		// 不是后端窗口 —— 按 500k 算出来的 475000 同样是拍的。本仓库独立地也有
		// configuredCodexGPT56MaxContext = 872_000（openai_codex_models_service.go:342）。
		// 写小了会让 BPS 在还能装的时候就开始摘要，而这条路 store=false 又拿不回密文，丢掉的历史
		// 本层看不见也补不回。
		"context_management": []any{bpsObject{"type": "compaction", "compact_threshold": openAIBasisPointsCompactThreshold}},
		"metadata": bpsObject{
			"task_id":         uuid.NewSHA1(uuid.NameSpaceURL, []byte(taskSeed)).String(),
			"turn_id":         uuid.NewSHA1(uuid.NameSpaceURL, []byte(taskSeed+"/turn/"+turnFingerprint)).String(),
			"agent_iteration": fmt.Sprint(iteration),
		},
	}
	if cacheKey != "" {
		output["prompt_cache_key"] = cacheKey
	}
	// 这里不能再加字段：BPS 的请求 schema 是封闭的，多一个它不认的键就整条 422
	// "Invalid request body."（2026-09-29 直连实测，include 和 max_output_tokens 各自单独加都 422）。
	// 代价记在 docs/conventions/basispoints-route.md：拿不回推理密文，客户端的输出上限也传不过去。
	// 空数组必须挡住：BPS 回 400 "Invalid 'context_management': empty array. Expected an array with
	// minimum length 1"（2026-09-29 直连实测）。完全不发这个字段是 200，所以客户端给了空数组时
	// 保留本层的默认 compaction。
	//
	// 非空数组也不能原样转发：出站体的其它每一个字段都是白名单，客户端塞一个 BPS 不认的
	// context_management 形态（truncation / 未知键）只会换来一条 422，而在硬报错口径下那是
	// 客户端可见的失败。所以只接 compaction 这一种、且把阈值钳进实测上限。
	if management, ok := source["context_management"].([]any); ok && len(management) > 0 {
		normalized, err := normalizeOpenAIBasisPointsContextManagement(management)
		if err != nil {
			return nil, err
		}
		output["context_management"] = normalized
	}
	return marshalOpenAIUpstreamJSON(output)
}

// openAIBasisPointsCompactThreshold：默认让 BPS 开始摘要的 input token 数。见 prepare 里的推导。
const openAIBasisPointsCompactThreshold = 872000

// bpsCarriesOwnBaseInstructions 报告客户端是否已经把自己的基础提示放进了 input。
// 真实 Codex Lite 的签名：input 首项是 additional_tools。
func bpsCarriesOwnBaseInstructions(source bpsObject) bool {
	input, ok := source["input"].([]any)
	if !ok || len(input) == 0 {
		return false
	}
	first, ok := input[0].(bpsObject)
	return ok && bpsText(first["type"]) == "additional_tools"
}

// normalizeOpenAIBasisPointsContextManagement 把客户端给的 context_management 归一成 BPS 认的形态。
// 只接 compaction；阈值缺失或超过实测上限就换成本层默认值。其它 type 一律判死（落回信号），
// 不赌上游会不会收 —— 赌错了是一条 422，而 422 在硬报错口径下就是客户端可见的失败。
func normalizeOpenAIBasisPointsContextManagement(management []any) ([]any, error) {
	if len(management) != 1 {
		return nil, bpsNative("context_management")
	}
	entry, ok := management[0].(bpsObject)
	if !ok {
		return nil, bpsNative("context_management")
	}
	if bpsText(entry["type"]) != "compaction" {
		return nil, bpsNative("context_management")
	}
	threshold := openAIBasisPointsCompactThreshold
	// `raw != nil` 是刻意的：JSON 里 null 是「没设置」的常见惯用法（多数 SDK 把未填的可选字段
	// 序列化成 null），不是格式错误。要拦的是 "200000" / 1.5 / -5 这类真错值。
	if raw, exists := entry["compact_threshold"]; exists && raw != nil {
		// 客户端给了这个键就必须解得出一个正整数。以前只试 Int64() 且失败就静默用默认值，
		// 于是 200000.0 / 2e5 / "200000" / -5 全被**抬高**到 872000 —— 客户端要早压缩，
		// 结果被改成晚压缩，而 type 写错反倒是硬报错，两种坏输入处置不一致。
		number, ok := raw.(json.Number)
		if !ok {
			return nil, bpsNative("context_management")
		}
		parsed, err := number.Int64()
		if err != nil {
			// bpsDecode 开了 UseNumber()，200000.0 / 2e5 这类合法 JSON 数字 Int64() 会报错。
			asFloat, floatErr := number.Float64()
			if floatErr != nil || asFloat != math.Trunc(asFloat) {
				return nil, bpsNative("context_management")
			}
			parsed = int64(asFloat)
		}
		if parsed <= 0 {
			return nil, bpsNative("context_management")
		}
		// 超上限只钳不报错：值本身是合法的，钳下来只是比客户端要求的更早压缩（良性降级），
		// 而参考实现也是钳。解不出正整数才判死 —— 那是格式问题，赌不得。
		if parsed > openAIBasisPointsCompactThreshold {
			parsed = openAIBasisPointsCompactThreshold
		}
		threshold = int(parsed)
	}
	return []any{bpsObject{"type": "compaction", "compact_threshold": threshold}}, nil
}

// isOpenAIBasisPointsHostedTool：BPS 上转发不了的托管工具。auto 模式下略过并在提示里说明；
// 显式点名（tool_choice / external_web_access）的在入口就落回原路径。
func isOpenAIBasisPointsHostedTool(kind string) bool {
	switch kind {
	case "web_search", "web_search_preview", "web_search_preview_2025_03_11", "web_search_2025_08_26",
		"tool_search", "image_generation", "file_search", "code_interpreter", "computer", "computer_use_preview", "mcp":
		return true
	default:
		return false
	}
}

func (b *openAIBasisPointsBridge) collectTools(value any, namespace string) ([]bpsObject, error) {
	var catalog []bpsObject
	items, _ := value.([]any)
	for _, raw := range items {
		item, ok := raw.(bpsObject)
		if !ok {
			return nil, bpsNative("tool_catalog")
		}
		kind, name := bpsText(item["type"]), bpsText(item["name"])
		if kind == "namespace" {
			if name == "" {
				return nil, bpsNative("tool_catalog")
			}
			nested := name
			if namespace != "" {
				nested = namespace + "." + name
			}
			entries, err := b.collectTools(item["tools"], nested)
			if err != nil {
				return nil, err
			}
			catalog = append(catalog, entries...)
			continue
		}
		if isOpenAIBasisPointsHostedTool(kind) {
			b.unsupported[kind] = true
			continue
		}
		if (kind != "function" && kind != "custom") || name == "" {
			return nil, bpsNative("tool_catalog")
		}
		// key 就是模型看到的名字（namespace 用点拼）。客户端同时声明顶层 function `a.f` 和
		// namespace `a` 里的 `f` 时两者扁平后同名、定义不同 ⇒ 判目录冲突、硬报错。**这是刻意的**：
		// 模型在线上只能说出 `a.f` 这一个名字，没有任何线上表示能区分这两者，把两条都收进目录
		// 只会把「发请求前可判的歧义」推迟成「模型调用时无法解析」。
		key := name
		if namespace != "" {
			key = namespace + "." + name
		}
		entry := bpsObject{"type": kind, "name": key}
		for _, field := range []string{"description", "format", "parameters"} {
			if v, exists := item[field]; exists {
				entry[field] = v
			}
		}
		if kind == "function" && entry["parameters"] == nil {
			entry["parameters"] = item["inputSchema"]
			if entry["parameters"] == nil {
				entry["parameters"] = item["input_schema"]
			}
		}
		definition := bpsFingerprint(item)
		if previous, exists := b.tools[key]; exists {
			if previous.definition != definition || previous.namespace != namespace || previous.name != name {
				return nil, bpsNative("tool_catalog")
			}
			continue
		}
		parameters, _ := entry["parameters"].(bpsObject)
		b.tools[key] = bpsTool{name: name, namespace: namespace, kind: kind, definition: definition, parameters: parameters}
		catalog = append(catalog, entry)
	}
	return catalog, nil
}

func (b *openAIBasisPointsBridge) protocolInstructions(catalog []bpsObject) string {
	protocol := "This request comes from an external Responses client. Return assistant text. Do not call Excel, Office, workbook, connector, list_skills or native web-search tools."
	if len(catalog) > 0 {
		protocol = "This request comes from an external Responses client. Use only the client tools in the catalog below. " +
			"There is no live Excel workbook for this request. The proxy intercepts run_officejs as a transport and never executes Office code. " +
			"To call one client tool, call native run_officejs using the transport matching its catalog type. " +
			"FUNCTION: code must contain one serialized JSON object {\"name\":\"CATALOG_NAME\",\"arguments\":{...}}. Arguments is an object, not an extra JSON string. " +
			"CUSTOM: set summary to exactly " + openAIBasisPointsCustomMarker + "CATALOG_NAME and put the exact raw tool input directly in code. Do not wrap custom input in another JSON object or add Markdown fences. " +
			"For example, custom apply_patch uses summary=" + openAIBasisPointsCustomMarker + "apply_patch and code containing its exact patch text. The marker is mandatory for raw input. " +
			"CATALOG_NAME includes its exact namespace. Outer arguments also include extended_summary, destructive=false and references=[]. For FUNCTION transport, use an ordinary descriptive summary. " +
			"Never nest run_officejs inside code. Serialize outer native arguments with proper JSON escaping. For FUNCTION envelopes also escape all quotes, backslashes, newline, carriage return and tab characters within JSON string values. " +
			"Call one client tool at a time, including update_plan through this transport. After receiving its result continue the task; do not repeat completed calls. " +
			"Tool results replayed under run_officejs are the named client tool's results. When a tool is needed, emit its call in this response instead of only announcing it. " +
			"Do not call other native tools or claim that shell, filesystem or workspace access is unavailable when a suitable catalog tool exists. " +
			"Other native server-injected Excel, Office, connector, workbook, list_skills and web-search tools are unavailable here; calling one fails the whole turn. " +
			"If no tool is needed, answer as assistant text. Client tool catalog:\n" + describeOpenAIBasisPointsCatalog(catalog) +
			"\nEnd of catalog. Invoke native run_officejs once. FUNCTION uses a JSON envelope in code. CUSTOM uses the exact " + openAIBasisPointsCustomMarker + "CATALOG_NAME summary marker and raw input in code. No Office code is executed by the proxy."
	}
	if len(b.unsupported) > 0 {
		kinds := make([]string, 0, len(b.unsupported))
		for kind := range b.unsupported {
			kinds = append(kinds, kind)
		}
		sort.Strings(kinds)
		warning := "Hosted tools unavailable through this channel: " + strings.Join(kinds, ", ")
		b.warnings = append(b.warnings, warning)
		protocol += "\n" + warning + ". These declarations were omitted. Do not claim to have used them. If the task requires one, explain the limitation or use a suitable declared client tool."
		for kind := range b.unsupported {
			if strings.HasPrefix(kind, "web_search") {
				protocol += " If the user needs current web information, say that web search is off on this channel and that enabling Codex live web search (for example the --search flag or web_search = \"live\") turns it on."
				break
			}
		}
	}
	return protocol
}

// describeOpenAIBasisPointsCatalog 把工具契约写成文档，不写成原生工具定义。
func describeOpenAIBasisPointsCatalog(catalog []bpsObject) string {
	lines := make([]string, 0, len(catalog))
	for _, entry := range catalog {
		line := "Client tool " + bpsQuoted(entry["name"]) + " (" + bpsText(entry["type"]) + ")."
		if description := bpsText(entry["description"]); description != "" {
			line += " " + description
		}
		if bpsText(entry["type"]) == "custom" {
			line += " Set run_officejs summary to " + bpsQuoted(openAIBasisPointsCustomMarker+bpsText(entry["name"])) + " and pass its exact raw text directly in code."
			if format := entry["format"]; format != nil {
				line += " Input format: " + bpsQuoted(format) + "."
			}
		} else {
			line += " Pass a JSON object in the envelope's arguments field. Argument contract: " + describeOpenAIBasisPointsSchema(entry["parameters"], 0)
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n\n")
}

func bpsQuoted(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

func describeOpenAIBasisPointsSchema(value any, depth int) string {
	schema, ok := value.(bpsObject)
	if !ok || depth >= 8 {
		if value == nil {
			return "Use the arguments described by the tool."
		}
		return bpsQuoted(value)
	}
	var parts []string
	if kind := schema["type"]; kind != nil {
		parts = append(parts, "Value type: "+bpsQuoted(kind)+".")
	}
	if description := bpsText(schema["description"]); description != "" {
		parts = append(parts, description)
	}
	required := map[string]bool{}
	if names, ok := schema["required"].([]any); ok {
		for _, name := range names {
			required[bpsText(name)] = true
		}
	}
	if properties, ok := schema["properties"].(bpsObject); ok {
		names := make([]string, 0, len(properties))
		for name := range properties {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			presence := "optional"
			if required[name] {
				presence = "required"
			}
			parts = append(parts, fmt.Sprintf("Field %s (%s): %s", bpsQuoted(name), presence, describeOpenAIBasisPointsSchema(properties[name], depth+1)))
		}
	}
	if items := schema["items"]; items != nil {
		parts = append(parts, "Each array item: "+describeOpenAIBasisPointsSchema(items, depth+1))
	}
	constraints := bpsObject{}
	for key, v := range schema {
		switch key {
		case "type", "description", "properties", "items":
		default:
			constraints[key] = v
		}
	}
	if len(constraints) > 0 {
		parts = append(parts, "Additional constraints: "+bpsQuoted(constraints)+".")
	}
	if len(parts) == 0 {
		return "Any JSON value."
	}
	return strings.Join(parts, " ")
}

// ---- 历史翻译 ----

func (b *openAIBasisPointsBridge) translateHistory(input []any) ([]any, error) {
	result := make([]any, 0, len(input))
	seenCalls := map[string]bool{}
	var trigger any
	for _, raw := range input {
		item, ok := raw.(bpsObject)
		if !ok {
			return nil, bpsNative("request_json")
		}
		// Codex 给每个 item 盖的私有元数据不在 BPS 的词汇表里，且每个新会话都不同，会让逐字节
		// 相同的提示前缀在目录之后立刻分叉、命不中缓存。缓存回放的原生项不经过这里，原样保留。
		delete(item, "internal_chat_message_metadata_passthrough")
		switch bpsText(item["type"]) {
		case "additional_tools":
			continue
		case "item_reference":
			return nil, bpsNative("history_reference")
		case "configuration_update":
			return nil, bpsNative("reasoning_configuration")
		case "compaction_trigger":
			// 真 Codex 与本仓 openai_compact_body_signal.go 产的都是裸 {"type":"compaction_trigger"}，
			// 但它是唯一一个逐字透传客户端对象的 item —— 收成白名单才让下面那段「其余项都由本层新建」
			// 的说法成立，也省掉一个未知字段让整单 400 的可能。
			openAIBasisPointsKeepItemKeys(item, "type")
			trigger = item
			continue
		case "reasoning":
			// store:false 下上游拒收没有密文的 reasoning 项；带密文的原样回放保持连续性。
			if encrypted := bpsText(item["encrypted_content"]); encrypted != "" {
				result = append(result, bpsObject{"type": "reasoning", "summary": []any{}, "encrypted_content": encrypted})
			}
			continue
		case "compaction", "compaction_summary":
			// 压缩回合改走 BPS（f200bd1ad）之后，上游会产出一个 compaction 项，客户端把它存进历史、
			// **此后每一轮都原样回放**。原来这里落到 default 判死成 input_content ⇒ 会话从压缩那一刻
			// 起每轮都 502，而失败在出站之前、lineage 自愈压根不触发 —— 没有任何恢复路径。
			//
			// **2026-09-29 直连实测（bpstest-pro1，两发）：收得回去，而且密文真被解开了。**
			// 第一发触发压缩拿到 `{type:"compaction", id:"cmp_…", encrypted_content:<3320 字符>}`；
			// 第二发把它原样放回 input（不带 trigger）+ 一句「列出我让你记的水果」→ HTTP 200，
			// 答出全部四种（apple/banana/cherry/watermelon）。所以透传，不是替换成明文说明。
			//
			// 字段按上游自己产出的那三个白名单，其余丢掉（与 reasoning 同口径）。出站类型一律用
			// BPS 自己产的 `compaction`：`compaction_summary` 是 Codex 侧的同胞类型，BPS 没见过，
			// 而出站体的规矩是「不赌」。那种项里的密文本来也是 Codex 铸的、BPS 解不开 ⇒ 会被拒 ⇒
			// lineage 记下 ⇒ 下一轮 sanitizeEncryptedReasoningInputItem 整项删除，一次失败后自愈
			// （openAIEncryptedLineageItemType 与那个清理函数都已覆盖这两个类型）。
			//
			// **BPS 自己铸的那颗被拒时也自愈**（会话换到另一个开着开关的账号、blob 过期、跨网关），
			// 靠的是 openAIBasisPointsSuspectEncryptedDigests 里那条「过滤把候选清空就回退」——
			// 少了它，这一格就是「压缩之后每轮硬错、永不自愈」。
			if encrypted := bpsText(item["encrypted_content"]); encrypted != "" {
				compaction := bpsObject{"type": "compaction", "encrypted_content": encrypted}
				if id := bpsText(item["id"]); id != "" {
					compaction["id"] = id
				}
				result = append(result, compaction)
			}
			continue
		case "function_call", "custom_tool_call":
			id := bpsText(item["call_id"])
			native := b.replay.getForCall(b.scope, id, item)
			if native == nil {
				rebuilt, err := rebuildOpenAIBasisPointsHistoryCall(item)
				if err != nil {
					return nil, err
				}
				b.replay.put(b.scope, id, rebuilt, item)
				native = rebuilt
			}
			seenCalls[id] = true
			result = append(result, native)
			continue
		case "function_call_output", "custom_tool_call_output":
			id := bpsText(item["call_id"])
			native := b.replay.get(b.scope, id)
			if !seenCalls[id] {
				if native == nil {
					return nil, bpsNative("tool_history")
				}
				result = append(result, native)
				seenCalls[id] = true
			}
			item["type"] = "function_call_output"
			// 输出项的 id 一律 fc_<call_id>：Codex 自己不带 id，custom 输出带的 ctco_ 前缀 BPS 不认。
			item["id"] = openAIBasisPointsFunctionItemID(id)
			if native != nil && bpsIsNativePlan(native) {
				// BPS 自带的 update_plan 只认 {"status":"ok"}；Codex 回的 "Plan updated" 会让模型重新规划。
				item["output"] = `{"status":"ok"}`
				openAIBasisPointsKeepItemKeys(item, "type", "call_id", "id", "output", "status", "name", "namespace")
				result = append(result, item)
				continue
			}
			if parts, ok := item["output"].([]any); ok {
				// 文本部件出 `input_text` 而不是 `output_text`：工具结果是**给模型的输入**，官方
				// Responses schema 在这个位置的联合类型是 input_text|input_image|input_file。
				// 6fa7dfbfc 把 textKind 从「只给 encrypted_content 那句明文说明用」扩到全部文本部件，
				// 顺带把这里原样转发的 input_text 改成了 output_text —— 那是参数复用的意外副作用，
				// 而这个文件的口径是「换掉类型本身就是赌」（多一个合法字段都稳定 422）。
				rewritten, err := b.rewriteContent(parts, false, "input_text")
				if err != nil {
					return nil, err
				}
				item["output"] = rewritten
			} else if strings.TrimSpace(bpsText(item["output"])) == "" {
				// 空输出会被当成失败而反复重试；把成功说明白。
				item["output"] = openAIBasisPointsEmptyOutput
			}
			// item 级字段白名单，理由见下面 message 那条。`name` / `namespace` **留着**：
			// 2026-09-29 直连实测它们在这个 item 上是合法字段（带着也 200），删掉才是改行为。
			openAIBasisPointsKeepItemKeys(item, "type", "call_id", "id", "output", "status", "name", "namespace")
		case "message", "":
			if parts, ok := item["content"].([]any); ok {
				rewritten, err := b.rewriteContent(parts, true, openAIBasisPointsMessageTextKind(bpsText(item["role"])))
				if err != nil {
					return nil, err
				}
				item["content"] = rewritten
			}
			// **item 级字段也是白名单。** 上游对未知 item 字段是整单 400（实测
			// "Unknown parameter: 'input[1].zzz_unknown_field'"），而客户端每轮回放同一批历史 ⇒
			// 一个多出来的键就是「这个会话从此每轮都失败」，且失败点在上游、本层还白发一次 22.5K
			// 提示词。原来只闸门 item **类型**，其余键原样出站 —— 顺带把 message 项上的 item 级
			// `encrypted_content`（Codex 多智能体 v2 会挂在这儿）也带出去了。
			openAIBasisPointsKeepItemKeys(item, "type", "role", "content", "id", "status")
		default:
			// 原来这里是 default 原样转发。BPS 的 item 级 schema 也是封闭的（实测 400
			// "Unknown parameter: 'input[1].zzz_unknown_field'"），所以任何它不认的 item 类型
			// 出站就是一条 400 —— 硬报错口径下那是客户端可见的失败，而且失败点在上游、
			// 本层还白发一次 22.5K 提示词。与 context_management 同一个口径：不赌，发请求前判死。
			//
			// 判死的范围比「跑过原生 web_search / MCP / 生图的历史」更宽，写清楚免得下次误判：
			// 本层只能中继 function_call / custom_tool_call 这一对（它们经 run_officejs 信封转译，
			// 见上面的 case），而 isCodexToolCallItemType（openai_codex_transform.go:1738）里
			// 另外六种一等 Codex 工具项 —— tool_call / local_shell_call / tool_search_call /
			// mcp_tool_call 及其 output —— 在 BPS 上没有对应物，同样落到这里判死。
			// **刻意不做「静默丢弃」**：丢掉一个 call 会让它的 output 变成孤儿，模型看到一个
			// 没有来由的工具结果，比直接报错更糟。要中继它们得先有各自的信封映射。
			return nil, bpsNative("input_content")
		}
		result = append(result, item)
	}
	if trigger != nil {
		result = append(result, trigger)
	}
	return result, nil
}

// openAIBasisPointsKeepItemKeys 就地把 item 收窄到给定的键集合。
//
// 只有 message / function_call_output 这两类需要它：它们是就地改写客户端给的对象。其余项由本层
// 新建 —— reasoning / compaction 在 translateHistory 里现造，function_call / custom_tool_call 走
// rebuildOpenAIBasisPointsHistoryCall 现造。
//
// **一个例外，别当它也是白名单**：回放缓存命中时（`b.replay.getForCall`）出站的是**上游原样的**
// 那个原生项，字段集由 BPS 说了算（它已经在自己的 function_call 上挂 encrypted_function_args）。
// 刻意不过滤：那个项是从 BPS 的输出里原样收下来的，我们一直这么发回去也一直被收 —— 反过来按本层
// 的白名单裁它，才是拿「我们枚举得全」去赌。代价是万一上游哪天加一个它自己输入校验器不认的字段，
// 所有正在进行的工具会话会每轮 400 且不会因失败而失效缓存（确定性命中，直到 LRU 淘汰或重启）。
func openAIBasisPointsKeepItemKeys(item bpsObject, keys ...string) {
	keep := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keep[key] = struct{}{}
	}
	for key := range item {
		if _, ok := keep[key]; !ok {
			delete(item, key)
		}
	}
}

// bpsIsNativePlan：回放缓存里的原生项是 BPS 自带的 update_plan（不是 run_officejs 信封）。
func bpsIsNativePlan(native bpsObject) bool {
	name := bpsText(native["name"])
	return bpsText(native["type"]) == "function_call" && (name == "update_plan" || name == "functions.update_plan")
}

// openAIBasisPointsFunctionItemID 给输出项造 id。**64 字符上限对两条分支都生效**：原来带 fc_ 前缀
// 的直接原样返回、跳过上限，于是同一个 call 的调用项 id 被指纹化、输出项 id 却超长原样出站
// （:851 那条是无条件截的）。
func openAIBasisPointsFunctionItemID(callID string) string {
	if strings.HasPrefix(callID, "fc_") {
		if len(callID) > 64 {
			return "fc_" + bpsFingerprint(callID)
		}
		return callID
	}
	itemID := "fc_" + callID
	if len(itemID) > 64 {
		itemID = "fc_" + bpsFingerprint(callID)
	}
	return itemID
}

// openAIBasisPointsEncryptedContentNotice 替换掉递送不出去的 encrypted_content 部件。
// 英文：模型看到的上下文其余部分（含 BPS 自己那套 Excel 人格）也是英文。
const openAIBasisPointsEncryptedContentNotice = "[omitted: an encrypted segment of this message could not be carried over this channel]"

// openAIBasisPointsMessageTextKind 给出这条消息里文本部件该用的 type。
// 两种本层都已经在原样转发（见 rewriteContent 的第一个 case），所以按角色选是安全的。
func openAIBasisPointsMessageTextKind(role string) string {
	if strings.EqualFold(strings.TrimSpace(role), "assistant") {
		return "output_text"
	}
	return "input_text"
}

// rewriteContent 校验内容块并处理图片：用户消息里的 data: 图片 BPS 不收，传成附件后按 file_id
// 引用（Excel 加载项「上传文件」按钮的做法）；工具结果里的图片加载项自己就是内嵌发的，原样保留。
// textKind 是本层自己造的文本部件该用的 type（调用方按位置/角色定）。
func (b *openAIBasisPointsBridge) rewriteContent(parts []any, uploadImages bool, textKind string) ([]any, error) {
	result := make([]any, 0, len(parts))
	for _, raw := range parts {
		part, ok := raw.(bpsObject)
		if !ok {
			return nil, bpsNative("input_content")
		}
		switch bpsText(part["type"]) {
		case "input_text", "output_text", "text":
			// **部件级字段也是白名单，原来是原样入队。** 上游对部件上多出来的字段同样是拒 ——
			// 实测 `{"type":"input_image","file_id":…,"detail":"auto"}` 稳定 422、去掉 detail 就 200
			// （那是一个完全合法的 Responses 字段）。而 `output_text` 部件天生带 `annotations`
			// （新版还带 `logprobs`），把 response.output 的 assistant 消息原样回放进下一轮 input
			// 是 Responses API 官方的多轮写法；顶层 messages 那条 legacy 入站路径还会产出带
			// `prompt_cache_breakpoint` 的 input_text。这些都是常规客户端形态，而后果是上游 400 +
			// 客户端每轮回放同一批 ⇒ 永久失败。重建只留 {type, text}，那就是这三类的完整形状。
			//
			// 顺带把 `"text"` 归一成 textKind：它不是 Responses 的 input 部件类型（闸门放它过是
			// 因为确实有客户端这么发），原样出站等于赌上游认。
			result = append(result, bpsObject{"type": textKind, "text": bpsText(part["text"])})
		case "refusal":
			// refusal 的文本在 `refusal` 字段上，不是 `text`。
			result = append(result, bpsObject{"type": "refusal", "refusal": bpsText(part["refusal"])})
		case "input_image":
			picture, err := b.rewriteImage(part, uploadImages)
			if err != nil {
				return nil, err
			}
			result = append(result, picture)
		case "encrypted_content":
			// Codex 多智能体 v2 历史里的密文部件。**原样替换成一句明文说明，保留部件位置。**
			//
			// 这一批密文在 BPS 上无论如何递送不出去（出站体是严格白名单，这个部件类型本身就不在
			// 其中），而客户端每一轮都原样回放同一批 —— 原来在这里判死成 bpsNative("input_content")
			// 的后果是「开着开关的账号上这个会话**永久**失败」，且失败发生在出站之前、那套
			// lineage 自愈路径压根不会被触发。用户 2026-09-29 拍板：让模型知道少了一段。
			//
			// 密文值本身一个字节都不带出去：它对 BPS 是垃圾 token，而且本层无从判断里面是什么。
			result = append(result, bpsObject{"type": textKind, "text": openAIBasisPointsEncryptedContentNotice})
		default:
			return nil, bpsNative("input_content")
		}
	}
	return result, nil
}

// rewriteImage 把客户端的 input_image 翻成 BPS 能收的形态。
//
// **带 file_id 时只发 {type, file_id}，一个 detail 都不许多。** 上游对这个形状的校验很严：
// 同一张已上传的图、同一请求体，`{"type":"input_image","file_id":"…","detail":"auto"}` 稳定 422
// `Invalid request body.`，去掉 detail 就 200（JaxsonWang/cpa-plugin-oai-basispoints#17 的 A/B 对照，
// 维护者已在 v0.2.4 复现并按同样口径修掉）。在硬报错口径下这不是"偶发失败"——历史里一旦带上
// 一张图，这个会话每一轮回放都会 422，等于永久失败。
func (b *openAIBasisPointsBridge) rewriteImage(part bpsObject, upload bool) (bpsObject, error) {
	detail := bpsText(part["detail"])
	if detail != "low" && detail != "high" {
		detail = "auto"
	}
	url := strings.TrimSpace(bpsText(part["image_url"]))
	fileID := strings.TrimSpace(bpsText(part["file_id"]))
	switch {
	case url != "" && fileID != "":
		// 两个都给了就判死，不替客户端挑一个：挑错的那一半是静默换掉了它要的那张图。
		return nil, bpsNative("image_input")
	case fileID != "":
		return bpsObject{"type": "input_image", "file_id": fileID}, nil
	case strings.HasPrefix(url, "https://"):
		// **`detail` 只在 file_id 形态被实测拒过**（#17 的 A/B）。`image_url` 这两条分支带 detail
		// 是沿用 Responses API 语义，**没有任何实测支撑**；BPS 对 `image_url` 形态到底收不收
		// （以及收不收 detail）都还没有现场数据。别读成「三条都验过」。
		return bpsObject{"type": "input_image", "image_url": url, "detail": detail}, nil
	case strings.HasPrefix(url, "data:"):
		// 内联那条路也必须过同一道解码校验。原来它直接 return，于是按字节识别、`image/*` 声明
		// 校验、20 MB 上限三道闸门一条都不生效 —— 实测把 21 MB 的 `application/x-msdownload`
		// 放进 function_call_output 的 input_image 里会原样出站到 bps.openai.com。
		// 这套设计的口径是「封闭 schema，不赌，发请求前判死」，这里不该是唯一一处在赌的。
		mediaType, data, ok := decodeOpenAIBasisPointsDataURL(url)
		if !ok {
			return nil, bpsNative("image_input")
		}
		if !upload {
			return bpsObject{"type": "input_image", "image_url": url, "detail": detail}, nil
		}
		if b.upload == nil {
			return nil, bpsNative("image_upload")
		}
		uploaded, err := b.upload(mediaType, data)
		if err != nil {
			// 上传函数已经判过原因的（代理/网络死了 → image_upload_transport，那是账号侧故障、
			// 要罚分要摘池）就原样透出，别一律重打成形态类的 image_upload。
			if _, native := openAIBasisPointsNativeReason(err); native {
				return nil, err
			}
			return nil, bpsNative("image_upload")
		}
		return bpsObject{"type": "input_image", "file_id": uploaded}, nil
	default:
		return nil, bpsNative("image_input")
	}
}

// rebuildOpenAIBasisPointsHistoryCall 缓存里找不到原生项时，按客户端给的完整调用重建一个
// run_officejs 传输项。不恢复丢失的原生说明文字；有缓存时缓存优先。
func rebuildOpenAIBasisPointsHistoryCall(item bpsObject) (bpsObject, error) {
	id, name := bpsText(item["call_id"]), bpsText(item["name"])
	if id == "" || strings.TrimSpace(id) != id || name == "" || strings.TrimSpace(name) != name {
		return nil, bpsNative("tool_history")
	}
	if value, exists := item["namespace"]; exists {
		namespace, ok := value.(string)
		if !ok || strings.TrimSpace(namespace) != namespace {
			return nil, bpsNative("tool_history")
		}
		if namespace != "" {
			name = namespace + "." + name
		}
	}
	envelope := bpsObject{"name": name}
	switch bpsText(item["type"]) {
	case "function_call":
		arguments := item["arguments"]
		if encoded, ok := arguments.(string); ok {
			if bpsDecode([]byte(encoded), &arguments) != nil {
				return nil, bpsNative("tool_history")
			}
		}
		if args, ok := arguments.(bpsObject); !ok || args == nil {
			return nil, bpsNative("tool_history")
		}
		envelope["arguments"] = arguments
	case "custom_tool_call":
		input, ok := item["input"].(string)
		if !ok {
			return nil, bpsNative("tool_history")
		}
		envelope["input"] = input
	default:
		return nil, bpsNative("tool_history")
	}
	code, err := json.Marshal(envelope)
	if err != nil {
		return nil, bpsNative("tool_history")
	}
	arguments, err := json.Marshal(bpsObject{
		"code": string(code), "summary": "Replay a previously requested client tool",
		"extended_summary": "The supplied client history contains this tool call; consume its recorded result without repeating it.",
		"destructive":      false, "references": []any{},
	})
	if err != nil {
		return nil, bpsNative("tool_history")
	}
	itemID := bpsText(item["id"])
	if !strings.HasPrefix(itemID, "fc_") || len(itemID) > 64 {
		itemID = "fc_" + bpsFingerprint(id)
	}
	return bpsObject{
		"type": "function_call", "id": itemID, "call_id": id, "name": openAIBasisPointsTransportTool,
		"arguments": string(arguments), "status": "completed",
	}, nil
}

// ---- LRU ----

// bpsLRU：按条数与字节数双上限淘汰，回放缓存与图片缓存共用。
type bpsLRU struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int
	entries    map[string]*list.Element
	order      list.List
	bytes      int
}

type bpsLRUEntry struct {
	key   string
	value any
	size  int
}

func newBPSLRU(maxEntries, maxBytes int) *bpsLRU {
	return &bpsLRU{maxEntries: maxEntries, maxBytes: maxBytes, entries: map[string]*list.Element{}}
}

func (c *bpsLRU) get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.entries[key]
	if element == nil {
		return nil, false
	}
	c.order.MoveToBack(element)
	entry, _ := element.Value.(*bpsLRUEntry)
	return entry.value, true
}

func (c *bpsLRU) put(key string, value any, size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.entries[key]; old != nil {
		c.remove(old)
	}
	c.entries[key] = c.order.PushBack(&bpsLRUEntry{key: key, value: value, size: size})
	c.bytes += size
	for len(c.entries) > c.maxEntries || (c.maxBytes > 0 && c.bytes > c.maxBytes) {
		// 单条就超 maxBytes 时字节条件永远为真，淘干了还继续就会 remove(nil)。
		front := c.order.Front()
		if front == nil {
			break
		}
		c.remove(front)
	}
}

func (c *bpsLRU) remove(element *list.Element) {
	entry, _ := element.Value.(*bpsLRUEntry)
	delete(c.entries, entry.key)
	c.bytes -= entry.size
	c.order.Remove(element)
}

// ---- 原生项回放缓存 ----

type openAIBasisPointsReplayEntry struct {
	raw             []byte
	callFingerprint string
}

// openAIBasisPointsReplayCache 按「账号 × API key」作用域记住 BPS 原生工具项。条数和字节都封顶，
// 工具参数可能很大。
type openAIBasisPointsReplayCache struct{ lru *bpsLRU }

func newOpenAIBasisPointsReplayCache() *openAIBasisPointsReplayCache {
	return &openAIBasisPointsReplayCache{lru: newBPSLRU(1024, 16<<20)}
}

var openAIBasisPointsReplay = newOpenAIBasisPointsReplayCache()

func (c *openAIBasisPointsReplayCache) put(scope, id string, item bpsObject, clientCall bpsObject) {
	if c == nil || id == "" {
		return
	}
	raw, err := json.Marshal(item)
	if err != nil || len(raw) > 1<<20 {
		return
	}
	signature := ""
	if clientCall != nil {
		signature = openAIBasisPointsCallFingerprint(clientCall)
	}
	c.lru.put(scope+"\x00"+id, openAIBasisPointsReplayEntry{raw: raw, callFingerprint: signature}, len(raw))
}

func (c *openAIBasisPointsReplayCache) get(scope, id string) bpsObject {
	return c.getMatching(scope, id, "", false)
}

// getForCall：客户端给了完整调用时，call_id 相同还不够，参数也得对得上才算同一次调用。
func (c *openAIBasisPointsReplayCache) getForCall(scope, id string, clientCall bpsObject) bpsObject {
	signature := openAIBasisPointsCallFingerprint(clientCall)
	if signature == "" {
		return nil
	}
	return c.getMatching(scope, id, signature, true)
}

func (c *openAIBasisPointsReplayCache) getMatching(scope, id, signature string, requireSignature bool) bpsObject {
	if c == nil {
		return nil
	}
	value, ok := c.lru.get(scope + "\x00" + id)
	if !ok {
		return nil
	}
	entry, _ := value.(openAIBasisPointsReplayEntry)
	if requireSignature && entry.callFingerprint != signature {
		return nil
	}
	var item bpsObject
	if bpsDecode(entry.raw, &item) != nil {
		return nil
	}
	return item
}

// openAIBasisPointsCallFingerprint 忽略线上才有的 item id / status 与键序，保留工具类型、命名空间、
// 参数值和自定义工具原文。
func openAIBasisPointsCallFingerprint(item bpsObject) string {
	kind, id, name := bpsText(item["type"]), bpsText(item["call_id"]), bpsText(item["name"])
	if id == "" || name == "" || strings.TrimSpace(id) != id || strings.TrimSpace(name) != name {
		return ""
	}
	namespace := ""
	if raw, exists := item["namespace"]; exists {
		var ok bool
		namespace, ok = raw.(string)
		if !ok || strings.TrimSpace(namespace) != namespace {
			return ""
		}
	}
	canonical := bpsObject{"type": kind, "call_id": id, "name": name, "namespace": namespace}
	switch kind {
	case "function_call":
		arguments := item["arguments"]
		if raw, ok := arguments.(string); ok {
			if bpsDecode([]byte(raw), &arguments) != nil {
				return ""
			}
		}
		if args, ok := arguments.(bpsObject); !ok || args == nil {
			return ""
		}
		canonical["arguments"] = arguments
	case "custom_tool_call":
		input, ok := item["input"].(string)
		if !ok {
			return ""
		}
		canonical["input"] = input
	default:
		return ""
	}
	return bpsFingerprint(canonical)
}

// ---- 响应侧：原生调用 → 客户端工具 ----

func bpsIsTool(item bpsObject) bool {
	return bpsText(item["type"]) == "function_call" || bpsText(item["type"]) == "custom_tool_call"
}

func bpsIsTransportName(name string) bool {
	return name == openAIBasisPointsTransportTool || name == "functions."+openAIBasisPointsTransportTool
}

// translateResponse 把 completed 响应里的原生工具项换成客户端声明的工具项。
func (b *openAIBasisPointsBridge) translateResponse(response bpsObject) error {
	if response == nil {
		return nil
	}
	output, _ := response["output"].([]any)
	tools := 0
	for i, raw := range output {
		item, _ := raw.(bpsObject)
		if bpsIsTool(item) {
			translated, err := b.translateCall(item)
			if err != nil {
				return err
			}
			output[i] = translated
			tools++
		}
	}
	response["reasoning"] = bpsObject{"effort": b.effort}
	// 这个字段描述的是「这一轮实际发生了什么」，不是「客户端要求了什么」：原来恒写 false 而下面
	// 把所有工具都补发出去，客户端拿到的是自相矛盾的响应。
	//
	// 刻意不因此报错。`parallel_tool_calls` 不在出站白名单里（封闭 schema），BPS 只从 developer
	// 提示里看到一句软约束，拿一个从没转发出去的约束去判上游「协议违规」并杀掉整轮，结果是这轮
	// 零产出；而客户端对「声明 false 却来了两个调用」绝大多数是照样逐个执行。
	response["parallel_tool_calls"] = b.parallelToolCalls || tools > 1
	return nil
}

// translateCall 只接受声明过的传输工具和客户端声明的工具，不执行任何代码。
func (b *openAIBasisPointsBridge) translateCall(native bpsObject) (bpsObject, error) {
	name := bpsText(native["name"])
	if bpsText(native["type"]) == "function_call" && (name == "update_plan" || name == "functions.update_plan") {
		return b.translateNativePlan(native)
	}
	if !bpsIsTransportName(name) {
		return b.translateDirectCatalogCall(native)
	}
	var arguments bpsObject
	if value, ok := native["arguments"].(bpsObject); ok {
		arguments = value
	} else if err := bpsDecode([]byte(bpsText(native["arguments"])), &arguments); err != nil {
		return nil, errors.New("basispoints returned invalid tool transport arguments")
	}
	if arguments == nil {
		return nil, errors.New("basispoints returned empty tool transport arguments")
	}
	envelope, marked, err := openAIBasisPointsCustomEnvelope(arguments)
	if !marked && err == nil {
		envelope, err = decodeOpenAIBasisPointsTransportEnvelope(arguments["code"])
	}
	if err != nil {
		return nil, err
	}
	toolName, err := bpsEnvelopeName(envelope)
	if err != nil {
		return nil, err
	}
	info, allowed := b.tools[toolName]
	if !allowed {
		return nil, errors.New("basispoints returned a tool outside the client's catalog")
	}
	result, err := b.finishClientToolCall(native, info, envelope, marked)
	if err != nil {
		return nil, err
	}
	// run_officejs 是 BPS 真实存在的原生工具，原生项原样回放。
	b.replay.put(b.scope, bpsText(native["call_id"]), native, result)
	return result, nil
}

// translateDirectCatalogCall：模型偶尔跳过 run_officejs 直接按目录名调工具；接住而不是整轮报错。
func (b *openAIBasisPointsBridge) translateDirectCatalogCall(native bpsObject) (bpsObject, error) {
	name := bpsText(native["name"])
	info, ok := b.tools[name]
	if !ok {
		if trimmed := strings.TrimPrefix(name, "functions."); trimmed != name {
			info, ok = b.tools[trimmed]
		}
	}
	if !ok {
		return nil, errors.New("basispoints returned an unsupported native tool; no tool was executed")
	}
	kind := bpsText(native["type"])
	var envelope bpsObject
	switch info.kind {
	case "function":
		if kind != "function_call" {
			return nil, fmt.Errorf("basispoints returned client function tool as a %q; no tool was executed", kind)
		}
		envelope = bpsObject{"name": info.name, "arguments": native["arguments"]}
	case "custom":
		if kind != "custom_tool_call" {
			return nil, fmt.Errorf("basispoints returned client custom tool as a %q; no tool was executed", kind)
		}
		input, ok := native["input"].(string)
		if !ok {
			return nil, errors.New("basispoints direct custom tool input must be a string")
		}
		envelope = bpsObject{"name": info.name, "input": input}
	default:
		return nil, errors.New("basispoints returned an unsupported native tool; no tool was executed")
	}
	result, err := b.finishClientToolCall(native, info, envelope, false)
	if err != nil {
		return nil, err
	}
	// 直接按目录名调用时原生项上可能带着真实的 encrypted_function_args，原样带回客户端，
	// 不要用 finishClientToolCall 那个空列表盖掉。
	if encrypted, ok := native["encrypted_function_args"]; ok {
		result["encrypted_function_args"] = encrypted
	}
	// 裸的目录名不是 BPS 认识的工具，下一轮得以 run_officejs 传输项回放。
	wrapped, err := rebuildOpenAIBasisPointsHistoryCall(result)
	if err != nil {
		return nil, err
	}
	b.replay.put(b.scope, bpsText(native["call_id"]), wrapped, result)
	return result, nil
}

func (b *openAIBasisPointsBridge) finishClientToolCall(native bpsObject, info bpsTool, envelope bpsObject, marked bool) (bpsObject, error) {
	if marked && info.kind != "custom" {
		return nil, errors.New("basispoints raw transport requires a declared custom tool")
	}
	id := bpsText(native["call_id"])
	if id == "" {
		return nil, errors.New("basispoints tool call is missing call_id")
	}
	itemID := bpsText(native["id"])
	if itemID == "" {
		itemID = "fc_" + bpsFingerprint(id)
	}
	result := bpsObject{"type": info.kind + "_call", "id": itemID, "call_id": id, "name": info.name, "status": "completed"}
	if info.namespace != "" {
		result["namespace"] = info.namespace
	}
	if info.kind == "custom" {
		value, hasInput := envelope["input"]
		if alias, hasAlias := envelope["args"]; hasAlias {
			if hasInput {
				return nil, errors.New("basispoints custom tool envelope contains conflicting input fields")
			}
			value = alias
		}
		if _, exists := envelope["arguments"]; exists {
			return nil, errors.New("basispoints custom tools require input text, not arguments")
		}
		input, ok := value.(string)
		if !ok {
			return nil, errors.New("basispoints custom tool input must be a string")
		}
		result["type"] = "custom_tool_call"
		result["id"] = "ctc_" + bpsFingerprint(id)
		result["input"] = input
		return result, nil
	}
	args, err := bpsEnvelopeArguments(envelope)
	if err != nil {
		return nil, err
	}
	if raw, ok := args.(string); ok {
		if bpsDecode([]byte(raw), &args) != nil {
			return nil, errors.New("basispoints function arguments are invalid JSON")
		}
	}
	if _, ok := args.(bpsObject); !ok {
		return nil, errors.New("basispoints function arguments must be an object")
	}
	encoded, _ := json.Marshal(args)
	result["arguments"] = string(encoded)
	// 中继信封里的参数一律是明文，即使目录里声明了 encrypted:true。Codex 的协作 / 多智能体工具
	// 靠「显式空列表」和「字段缺失」区分明文与密文 —— 缺了它，客户端会把明文当 encrypted_content
	// 塞给子 agent（WsureDev 的参考实现在这一点上写了同样的理由）。
	result["encrypted_function_args"] = []any{}
	return result, nil
}

// translateNativePlan：BPS 自带 update_plan，模型有时直接调它；客户端也声明了 update_plan 时按
// Codex 的参数形态转过去。ponytail: 不校验客户端 schema，形态不合客户端会自己报工具错误。
func (b *openAIBasisPointsBridge) translateNativePlan(native bpsObject) (bpsObject, error) {
	selected, ok := b.tools["update_plan"]
	if !ok || selected.kind != "function" {
		return nil, errors.New("basispoints returned native update_plan but the client declared none; no tool was executed")
	}
	arguments, ok := native["arguments"].(bpsObject)
	if !ok {
		if err := bpsDecode([]byte(bpsText(native["arguments"])), &arguments); err != nil {
			return nil, errors.New("basispoints native update_plan arguments must be one JSON object")
		}
	}
	steps, ok := arguments["plan"].([]any)
	if !ok {
		return nil, errors.New("basispoints native update_plan requires a plan array")
	}
	plan := make([]any, 0, len(steps))
	for _, value := range steps {
		step, ok := value.(bpsObject)
		if !ok {
			return nil, errors.New("basispoints native update_plan entries must be objects")
		}
		description := ""
		for _, key := range []string{"step", "description", "title"} {
			if description = bpsText(step[key]); description != "" {
				break
			}
		}
		if strings.TrimSpace(description) == "" {
			return nil, errors.New("basispoints native update_plan has a step without description")
		}
		status := normalizeOpenAIBasisPointsPlanStatus(bpsText(step["status"]))
		if status == "" {
			return nil, errors.New("basispoints native update_plan has an unsupported step status")
		}
		plan = append(plan, bpsObject{"step": description, "status": status})
	}
	translated := bpsObject{"plan": plan}
	for _, key := range []string{"explanation", "summary"} {
		if explanation := bpsText(arguments[key]); explanation != "" {
			translated["explanation"] = explanation
			break
		}
	}
	callID := bpsText(native["call_id"])
	if callID == "" {
		return nil, errors.New("basispoints native update_plan is missing call_id")
	}
	itemID := bpsText(native["id"])
	if itemID == "" {
		itemID = "fc_" + bpsFingerprint(callID)
	}
	encoded, _ := json.Marshal(translated)
	// encrypted_function_args 是**显式空列表**，与 finishClientToolCall 的 function 分支同口径：
	// Codex 的协作工具靠「显式空列表 vs 字段缺失」区分明文与密文，缺了它客户端会把这份明文
	// arguments 当 encrypted_content 塞给子 agent。这里 emit 的确实是明文 JSON。
	result := bpsObject{"type": "function_call", "id": itemID, "call_id": callID, "name": selected.name,
		"arguments": string(encoded), "status": "completed", "encrypted_function_args": []any{}}
	if selected.namespace != "" {
		result["namespace"] = selected.namespace
	}
	b.replay.put(b.scope, callID, native, result)
	return result, nil
}

func normalizeOpenAIBasisPointsPlanStatus(status string) string {
	switch strings.NewReplacer("-", "_", " ", "_").Replace(strings.ToLower(strings.TrimSpace(status))) {
	case "pending", "not_started", "todo", "planned", "queued", "blocked":
		return "pending"
	case "in_progress", "active", "started", "doing", "current":
		return "in_progress"
	case "completed", "complete", "done", "finished":
		return "completed"
	default:
		return ""
	}
}

// ---- 信封解码 ----

// openAIBasisPointsCustomEnvelope 只认显式打了标记的自定义工具原文直传；目录名、类型和 call_id
// 仍由调用方核对。普通 run_officejs 调用继续走 JSON 解码。
func openAIBasisPointsCustomEnvelope(arguments bpsObject) (bpsObject, bool, error) {
	summary, ok := arguments["summary"].(string)
	if !ok {
		return nil, false, nil
	}
	// 标记的近失：大小写不一致、前后空白、标记与工具名之间多一个空格。这些都是模型自然会写出的
	// 变体，而原文逐字比较会把它们当成「没打标记」→ 拿 patch 原文去解 JSON → 必败 → 整轮死
	// （落回已改成硬报错，代价更大）。只放宽到能精确识别的近失，绝不猜：summary 完全是别的东西
	// 时仍然走 FUNCTION 信封那条路。
	trimmed := strings.TrimSpace(summary)
	if len(trimmed) < len(openAIBasisPointsCustomMarker) ||
		!strings.EqualFold(trimmed[:len(openAIBasisPointsCustomMarker)], openAIBasisPointsCustomMarker) {
		return nil, false, nil
	}
	name := strings.TrimSpace(trimmed[len(openAIBasisPointsCustomMarker):])
	if name == "" || strings.ContainsAny(name, "/\\") || strings.IndexFunc(name, unicode.IsSpace) >= 0 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return nil, true, errors.New("basispoints raw custom transport requires an exact nonempty catalog tool name in summary")
	}
	input, ok := arguments["code"].(string)
	if !ok {
		return nil, true, errors.New("basispoints raw custom transport code must be a string")
	}
	if len(input) > openAIBasisPointsMaxEnvelope {
		return nil, true, errors.New("basispoints raw custom transport code exceeds the size limit")
	}
	return bpsObject{"name": name, "input": input}, true, nil
}

// decodeOpenAIBasisPointsTransportCode 剥掉模型常见的包装（重复 JSON 编码、```围栏、短前缀）而不执行
// 任何代码；每层必须恰好是一个完整 JSON 值。
func decodeOpenAIBasisPointsTransportCode(value any) (bpsObject, error) {
	original := value
	for depth := 0; depth < 4; depth++ {
		if item, ok := value.(bpsObject); ok && item != nil {
			return item, nil
		}
		raw, ok := value.(string)
		if !ok || len(raw) > openAIBasisPointsMaxEnvelope {
			break
		}
		raw = strings.TrimSpace(raw)
		if raw == "" {
			break
		}
		if decoded, ok := decodeOpenAIBasisPointsEnvelopeValue(raw); ok {
			value = decoded
			continue
		}
		if start := strings.Index(raw, "```"); start >= 0 && bpsProsePrefix(raw[:start]) {
			fenced := raw[start:]
			newline := strings.IndexByte(fenced, '\n')
			if newline >= 0 && strings.HasSuffix(fenced, "```") {
				language := strings.TrimSpace(fenced[3:newline])
				if language == "" || strings.EqualFold(language, "json") {
					value = strings.TrimSpace(fenced[newline+1 : len(fenced)-3])
					continue
				}
			}
		}
		if start := strings.IndexByte(raw, '{'); start > 0 && bpsProsePrefix(raw[:start]) {
			if decoded, ok := decodeOpenAIBasisPointsEnvelopeValue(raw[start:]); ok {
				value = decoded
				continue
			}
		}
		if inner, ok := bpsStripJSAssignment(raw); ok {
			value = inner
			continue
		}
		break
	}
	return nil, fmt.Errorf("basispoints tool transport code must contain one JSON client-tool envelope (%s)", bpsTransportShape(original))
}

// bpsStripJSAssignment 剥掉 `const request = {...};` 这种外壳（ghcp_proxy 测试里模型真会这么写），
// 只接受「声明 标识符 = 对象 ;」，其余一概不认。
func bpsStripJSAssignment(raw string) (string, bool) {
	keyword, rest, found := strings.Cut(strings.TrimSpace(raw), " ")
	if !found || (keyword != "const" && keyword != "let" && keyword != "var") {
		return "", false
	}
	name, object, found := strings.Cut(rest, "=")
	name = strings.TrimSpace(name)
	if !found || name == "" {
		return "", false
	}
	for i, r := range name {
		if r != '_' && r != '$' && !unicode.IsLetter(r) && (i == 0 || !unicode.IsDigit(r)) {
			return "", false
		}
	}
	object = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(object), ";"))
	if !strings.HasPrefix(object, "{") || !strings.HasSuffix(object, "}") {
		return "", false
	}
	return object, true
}

// bpsTransportShape 只报结构事实，不带客户端代码、提示词或参数。
func bpsTransportShape(value any) string {
	raw, ok := value.(string)
	if !ok {
		if value == nil {
			return "format=missing"
		}
		return fmt.Sprintf("format=non_string_%T", value)
	}
	trimmed := strings.TrimSpace(raw)
	format := "text_or_code"
	switch {
	case trimmed == "":
		format = "empty"
	case strings.HasPrefix(trimmed, "```"):
		format = "markdown"
	case strings.HasPrefix(trimmed, "{"):
		format = "json_object"
	case strings.HasPrefix(trimmed, "["):
		format = "json_array"
	case strings.HasPrefix(trimmed, `"`):
		format = "json_string"
	}
	return fmt.Sprintf("format=%s; bytes=%d", format, len(raw))
}

func decodeOpenAIBasisPointsTransportEnvelope(value any) (bpsObject, error) {
	for depth := 0; depth < 3; depth++ {
		envelope, err := decodeOpenAIBasisPointsTransportCode(value)
		if err != nil {
			return nil, err
		}
		name, err := bpsEnvelopeName(envelope)
		if err != nil {
			return nil, err
		}
		if !bpsIsTransportName(name) {
			return envelope, nil
		}
		args, err := bpsEnvelopeArguments(envelope)
		if err != nil {
			return nil, err
		}
		if raw, ok := args.(string); ok {
			var parsed bpsObject
			if bpsDecode([]byte(raw), &parsed) != nil {
				return nil, errors.New("basispoints nested transport arguments must be one JSON object")
			}
			args = parsed
		}
		outer, ok := args.(bpsObject)
		if !ok {
			return nil, errors.New("basispoints nested transport arguments must be an object")
		}
		value = outer["code"]
	}
	return nil, errors.New("basispoints tool transport exceeds two nested wrappers")
}

func bpsEnvelopeName(envelope bpsObject) (string, error) {
	name := bpsText(envelope["name"])
	alias := bpsText(envelope["tool"])
	if name != "" && alias != "" && name != alias {
		return "", errors.New("basispoints tool envelope contains conflicting names")
	}
	if name == "" {
		name = alias
	}
	return name, nil
}

func bpsEnvelopeArguments(envelope bpsObject) (any, error) {
	args, exists := envelope["arguments"]
	alias, hasAlias := envelope["args"]
	if exists && hasAlias {
		return nil, errors.New("basispoints tool envelope contains conflicting argument fields")
	}
	if !exists {
		args = alias
	}
	return args, nil
}

func bpsProsePrefix(prefix string) bool {
	return len(prefix) <= 512 && !strings.ContainsAny(prefix, "{}[]();=`\"")
}

func decodeOpenAIBasisPointsEnvelopeValue(raw string) (any, bool) {
	var value any
	if bpsDecode([]byte(raw), &value) == nil {
		return value, true
	}
	// 严格解码失败后才修：字符串里的裸换行 / 制表符转义、非法反斜杠补成字面反斜杠。
	// 不猜缺失的引号、分隔符、闭合符或值。
	fixed := repairOpenAIBasisPointsJSONStrings(raw)
	if fixed != raw && bpsDecode([]byte(fixed), &value) == nil {
		return value, true
	}
	return nil, false
}

func repairOpenAIBasisPointsJSONStrings(raw string) string {
	var out strings.Builder
	out.Grow(len(raw))
	quoted := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '"' {
			quoted = !quoted
		}
		if quoted {
			switch ch {
			case '\n':
				_, _ = out.WriteString(`\n`)
				continue
			case '\r':
				_, _ = out.WriteString(`\r`)
				continue
			case '\t':
				_, _ = out.WriteString(`\t`)
				continue
			}
		}
		if ch != '\\' || !quoted || i+1 >= len(raw) {
			_ = out.WriteByte(ch)
			continue
		}
		next := raw[i+1]
		valid := strings.ContainsRune(`"\/bfnrt`, rune(next))
		if next == 'u' && i+5 < len(raw) {
			valid = true
			for _, digit := range raw[i+2 : i+6] {
				if !strings.ContainsRune("0123456789abcdefABCDEF", digit) {
					valid = false
				}
			}
		}
		_ = out.WriteByte('\\')
		if valid {
			_ = out.WriteByte(next)
			i++
		} else {
			_ = out.WriteByte('\\')
		}
	}
	return out.String()
}
