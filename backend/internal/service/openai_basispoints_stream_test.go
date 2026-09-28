package service

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// readOpenAIBasisPointsTestEvent 读一条 SSE 事件（event 行 + data 行 + 空行），返回 data。
func readOpenAIBasisPointsTestEvent(t *testing.T, r *bufio.Reader) (string, error) {
	t.Helper()
	data := ""
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return data, err
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if data != "" {
				return data, nil
			}
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		}
	}
}

func bpsTestEvent(kind, rest string) string {
	return "event: " + kind + "\ndata: {\"type\":\"" + kind + "\"" + rest + "}\n\n"
}

func TestOpenAIBasisPointsStreamPassesTextThroughAndHoldsToolsUntilCompleted(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	bridge.effort = "xhigh"
	upstreamReader, upstreamWriter := io.Pipe()
	body := bridge.stream(upstreamReader, 0, 0)
	defer func() { _ = body.Close() }()
	release := make(chan struct{})
	go func() {
		defer func() { _ = upstreamWriter.Close() }()
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.created", `,"sequence_number":0,"response":{"id":"r","status":"in_progress","output":[]}`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_text.delta", `,"sequence_number":1,"output_index":0,"item_id":"msg","delta":"hi"`))
		// 原生工具事件：全部扣住。
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.added", `,"sequence_number":2,"output_index":1,"item":{"type":"function_call","id":"fc_c","call_id":"c","name":"run_officejs","arguments":""}`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.function_call_arguments.delta", `,"sequence_number":3,"output_index":1,"item_id":"fc_c","delta":"{\"code\""`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.in_progress", `,"sequence_number":4,"response":{"id":"r","status":"in_progress","output":[{"type":"function_call","id":"fc_c","call_id":"c","name":"run_officejs","arguments":"{}"}]}`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.done", `,"sequence_number":5,"output_index":1,"item":`+bpsTestTransportCall("c", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)))
		<-release
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.completed", `,"sequence_number":6,"response":{"id":"r","status":"completed","output":[`+bpsTestTransportCall("c", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)+`],"usage":{"input_tokens":1,"output_tokens":1}}`))
		_, _ = io.WriteString(upstreamWriter, "data: [DONE]\n\n")
	}()

	reader := bufio.NewReader(body)
	var early []string
	for i := 0; i < 3; i++ {
		data, err := readOpenAIBasisPointsTestEvent(t, reader)
		require.NoError(t, err)
		early = append(early, data)
	}
	require.Equal(t, "response.created", gjson.Get(early[0], "type").String())
	require.Equal(t, "hi", gjson.Get(early[1], "delta").String(), "文本在 completed 之前就到客户端")
	require.Equal(t, "response.in_progress", gjson.Get(early[2], "type").String())
	require.Len(t, gjson.Get(early[2], "response.output").Array(), 0, "非终态快照里的原生工具项被过滤")
	require.Equal(t, "xhigh", gjson.Get(early[2], "response.reasoning.effort").String(), "")
	close(release)

	var rest []string
	for {
		data, err := readOpenAIBasisPointsTestEvent(t, reader)
		if data != "" {
			rest = append(rest, data)
		}
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
	}
	kinds := make([]string, 0, len(rest))
	for _, data := range rest {
		kinds = append(kinds, gjson.Get(data, "type").String())
	}
	require.Equal(t, []string{
		"response.output_item.added", "response.function_call_arguments.delta", "response.function_call_arguments.done",
		"response.output_item.done", "response.completed",
	}, kinds)
	require.Equal(t, "exec_command", gjson.Get(rest[0], "item.name").String())
	require.Equal(t, "in_progress", gjson.Get(rest[0], "item.status").String())
	require.Equal(t, "", gjson.Get(rest[0], "item.arguments").String())
	require.Equal(t, `{"cmd":"pwd"}`, gjson.Get(rest[1], "delta").String())
	require.Equal(t, `{"cmd":"pwd"}`, gjson.Get(rest[3], "item.arguments").String())
	require.Equal(t, "completed", gjson.Get(rest[3], "item.status").String())
	require.Equal(t, "exec_command", gjson.Get(rest[4], "response.output.0.name").String())
	// 客户端这份体没给 parallel_tool_calls，按 Responses API 的默认 true 回写，
	// 不再无条件写死 false（那会和「补发多个工具」自相矛盾）。
	require.True(t, gjson.Get(rest[4], "response.parallel_tool_calls").Bool())
	require.Equal(t, []any{}, gjson.Get(rest[4], "response.output.0.encrypted_function_args").Value(),
		"显式空列表：客户端靠它区分明文与密文")
	all := strings.Join(append(early, rest...), "\n")
	require.NotContains(t, all, "run_officejs")
	for i, data := range append(early, rest...) {
		require.Equal(t, int64(i), gjson.Get(data, "sequence_number").Int(), "序号连续重排")
	}
}

// 模型先调工具、再写解释时，上游 output = [function_call(0), message(1)]：消息边来边透传，
// 工具项要等 completed 才补发。
//
// output_index 的规范语义是「该 item 在 output 数组里的下标」，所以补发用真实下标 0，
// 数组也不重排。代价是到达顺序变成 1 → 0；换来的是 index 与数组位置严格对齐、不留空洞。
// 以前那版反过来（重排数组 + 单调递增），结果两个 item 的号都比数组位置大 1、index=0
// 一次都没发过 —— 等于让 output_index 撒谎。JaxsonWang 与 WsureDev 两家参考实现都取这一条。
func TestOpenAIBasisPointsStreamHeldToolIndexMatchesOutputPosition(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	upstreamReader, upstreamWriter := io.Pipe()
	body := bridge.stream(upstreamReader, 0, 0)
	defer func() { _ = body.Close() }()
	call := bpsTestTransportCall("c", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)
	go func() {
		defer func() { _ = upstreamWriter.Close() }()
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`))
		// 工具在前（output_index=0），消息在后（output_index=1）。
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.added", `,"output_index":0,"item":{"type":"function_call","id":"fc_c","call_id":"c","name":"run_officejs","arguments":""}`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+call))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_text.delta", `,"output_index":1,"item_id":"msg","delta":"done"`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.completed",
			`,"response":{"id":"r","status":"completed","output":[`+call+`,{"type":"message","id":"msg","role":"assistant","content":[]}]}`))
		_, _ = io.WriteString(upstreamWriter, "data: [DONE]\n\n")
	}()

	reader := bufio.NewReader(body)
	var indexes []int64
	var final string
	for {
		data, err := readOpenAIBasisPointsTestEvent(t, reader)
		if data != "" {
			if v := gjson.Get(data, "output_index"); v.Exists() {
				indexes = append(indexes, v.Int())
			}
			if gjson.Get(data, "type").String() == "response.completed" {
				final = data
			}
		}
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
	}
	// 消息用上游的 1，补发的工具用它在数组里的真实下标 0。
	require.Equal(t, []int64{1, 0, 0, 0, 0}, indexes)
	// 数组一个字节不动：工具仍在 0、消息仍在 1，与上面发出去的号一一对应。
	require.Equal(t, "function_call", gjson.Get(final, "response.output.0.type").String())
	require.Equal(t, "exec_command", gjson.Get(final, "response.output.0.name").String())
	require.Equal(t, "message", gjson.Get(final, "response.output.1.type").String())
	// 每个发出去的 output_index 都能在 output 数组里对上同一个 item，一个空洞都没有。
	for _, index := range indexes {
		require.True(t, gjson.Get(final, fmt.Sprintf("response.output.%d", index)).Exists(),
			"output_index=%d 在 output 数组里找不到对应项", index)
	}
}

// incomplete 以前走的是「过滤掉工具项」的非终态分支：扣住的工具项既不翻译也不补发，
// 客户端收到一条 output 里什么都没有的 incomplete，既不会执行也不会报错。
func TestOpenAIBasisPointsStreamIncompleteStillEmitsHeldTool(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	upstreamReader, upstreamWriter := io.Pipe()
	body := bridge.stream(upstreamReader, 0, 0)
	defer func() { _ = body.Close() }()
	call := bpsTestTransportCall("c", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)
	go func() {
		defer func() { _ = upstreamWriter.Close() }()
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.added", `,"output_index":0,"item":{"type":"function_call","id":"fc_c","call_id":"c","name":"run_officejs","arguments":""}`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+call))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.incomplete",
			`,"response":{"id":"r","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[`+call+`]}`))
		_, _ = io.WriteString(upstreamWriter, "data: [DONE]\n\n")
	}()

	reader := bufio.NewReader(body)
	var kinds []string
	final := ""
	for {
		data, err := readOpenAIBasisPointsTestEvent(t, reader)
		if data != "" {
			kinds = append(kinds, gjson.Get(data, "type").String())
			if gjson.Get(data, "type").String() == "response.incomplete" {
				final = data
			}
		}
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
	}
	require.Contains(t, kinds, "response.output_item.done", "截断也要把扣住的工具项补发出去")
	require.Equal(t, "exec_command", gjson.Get(final, "response.output.0.name").String())
	require.Equal(t, "max_output_tokens", gjson.Get(final, "response.incomplete_details.reason").String())
}

// cancelled / canceled / done 以前不在终态集里（本仓库另外四处终态表都含它们）。后果有三层：
// 扣住的工具项被静默吞掉、emitter.terminal 留 false 让一次正常取消被伪装成 io.ErrUnexpectedEOF、
// peek 还会把取消流当「已经开始输出」而不落回。
func TestOpenAIBasisPointsStreamCancelledIsTerminalAndEmitsHeldTool(t *testing.T) {
	for _, kind := range []string{"response.cancelled", "response.canceled", "response.done"} {
		t.Run(kind, func(t *testing.T) {
			bridge := newBasisPointsTestBridge(t, bpsTestTools)
			upstreamReader, upstreamWriter := io.Pipe()
			body := bridge.stream(upstreamReader, 0, 0)
			defer func() { _ = body.Close() }()
			call := bpsTestTransportCall("c", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)
			go func() {
				defer func() { _ = upstreamWriter.Close() }()
				_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`))
				_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.added", `,"output_index":0,"item":{"type":"function_call","id":"fc_c","call_id":"c","name":"run_officejs","arguments":""}`))
				_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+call))
				_, _ = io.WriteString(upstreamWriter, bpsTestEvent(kind,
					`,"response":{"id":"r","status":"cancelled","output":[`+call+`]}`))
			}()

			reader := bufio.NewReader(body)
			var kinds []string
			final := ""
			var readErr error
			for {
				data, err := readOpenAIBasisPointsTestEvent(t, reader)
				if data != "" {
					kinds = append(kinds, gjson.Get(data, "type").String())
					if gjson.Get(data, "type").String() == kind {
						final = data
					}
				}
				if err != nil {
					readErr = err
					break
				}
			}
			require.ErrorIs(t, readErr, io.EOF, "正常取消不该变成 io.ErrUnexpectedEOF")
			require.Contains(t, kinds, "response.output_item.done", "取消也要把扣住的工具项补发出去")
			require.Equal(t, "exec_command", gjson.Get(final, "response.output.0.name").String())
			require.NotContains(t, final, "run_officejs", "原生传输工具名不能漏给客户端")
		})
	}
}

// 截断的那一轮上游可能根本不带上工具项：这只是正常截断，不该变成协议失败。
func TestOpenAIBasisPointsStreamIncompleteWithoutToolItemDoesNotFail(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	upstreamReader, upstreamWriter := io.Pipe()
	body := bridge.stream(upstreamReader, 0, 0)
	defer func() { _ = body.Close() }()
	go func() {
		defer func() { _ = upstreamWriter.Close() }()
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+bpsTestTransportCall("c", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.incomplete", `,"response":{"id":"r","status":"incomplete","output":[]}`))
		_, _ = io.WriteString(upstreamWriter, "data: [DONE]\n\n")
	}()

	reader := bufio.NewReader(body)
	sawIncomplete := false
	for {
		data, err := readOpenAIBasisPointsTestEvent(t, reader)
		if gjson.Get(data, "type").String() == "response.incomplete" {
			sawIncomplete = true
		}
		require.NotEqual(t, "basispoints_protocol_error", gjson.Get(data, "response.error.code").String())
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
	}
	require.True(t, sawIncomplete)
}

func TestOpenAIBasisPointsStreamKeepaliveWhileUpstreamThinks(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	bridge.effort = "high"
	upstreamReader, upstreamWriter := io.Pipe()
	body := bridge.stream(upstreamReader, 40*time.Millisecond, 0)
	defer func() { _ = body.Close() }()
	release := make(chan struct{})
	go func() {
		defer func() { _ = upstreamWriter.Close() }()
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`))
		<-release
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.completed", `,"response":{"id":"r","status":"completed","output":[]}`))
	}()
	reader := bufio.NewReader(body)
	created, err := readOpenAIBasisPointsTestEvent(t, reader)
	require.NoError(t, err)
	require.Equal(t, "response.created", gjson.Get(created, "type").String())
	keepalive, err := readOpenAIBasisPointsTestEvent(t, reader)
	require.NoError(t, err)
	require.Equal(t, "response.in_progress", gjson.Get(keepalive, "type").String(), "上游沉默时补 in_progress")
	require.Equal(t, "r", gjson.Get(keepalive, "response.id").String())
	require.Equal(t, int64(1), gjson.Get(keepalive, "sequence_number").Int())
	close(release)
	for {
		data, err := readOpenAIBasisPointsTestEvent(t, reader)
		require.NoError(t, err)
		if kind := gjson.Get(data, "type").String(); kind == "response.completed" {
			break
		} else {
			require.Equal(t, "response.in_progress", kind, "completed 之前只允许再出现保活")
		}
	}
	_, err = readOpenAIBasisPointsTestEvent(t, reader)
	require.ErrorIs(t, err, io.EOF)
}

func TestOpenAIBasisPointsStreamWithoutTerminalEventIsUnexpectedEOF(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	body := bridge.stream(io.NopCloser(strings.NewReader(bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`))), 0, 0)
	_, err := io.ReadAll(body)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestOpenAIBasisPointsStreamMissingHeldToolItemFails(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	stream := bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`) +
		bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+bpsTestTransportCall("held", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)) +
		bpsTestEvent("response.completed", `,"response":{"id":"r","status":"completed","output":[]}`)
	raw, err := io.ReadAll(bridge.stream(io.NopCloser(strings.NewReader(stream)), 0, 0))
	require.NoError(t, err)
	out := string(raw)
	require.Contains(t, out, "event: response.failed")
	require.Contains(t, out, "basispoints_protocol_error")
	require.NotContains(t, out, "event: response.completed")
	require.NotContains(t, out, "run_officejs")
}

// 终态事件**没带** response 快照时，扣留检查 / 翻译 / 补发整段都不执行 —— 扣住的工具项被静默吞掉，
// 客户端拿到一条「模型什么都没做」的 200，没有任何日志、错误码或读数。上面那条用例（快照在、
// output 里缺工具项）盖不住这一格：它走的是快照存在那条分支。
func TestOpenAIBasisPointsStreamTerminalWithoutResponseSnapshotFails(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, bpsTestTools)
	stream := bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`) +
		bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+bpsTestTransportCall("held", `{"name":"exec_command","arguments":{"cmd":"pwd"}}`)) +
		bpsTestEvent("response.completed", `,"sequence_number":2`)
	raw, err := io.ReadAll(bridge.stream(io.NopCloser(strings.NewReader(stream)), 0, 0))
	require.NoError(t, err)
	out := string(raw)
	require.Contains(t, out, "event: response.failed")
	require.Contains(t, out, "basispoints_protocol_error")
	require.NotContains(t, out, "event: response.completed", "不许把工具调用静默吞掉当成功")
	require.NotContains(t, out, "run_officejs")
}

func TestOpenAIBasisPointsStreamCloseInterruptsUpstream(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	upstreamReader, upstreamWriter := io.Pipe()
	body := bridge.stream(upstreamReader, 0, 0)
	_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`))
	require.NoError(t, body.Close())
	_, err := upstreamWriter.Write([]byte("x"))
	require.True(t, errors.Is(err, io.ErrClosedPipe), "关闭下游后上游读端也被关掉")
}

func TestOpenAIBasisPointsStreamDeadUpstreamAfterOutputFails(t *testing.T) {
	previous := openAIBasisPointsUpstreamSilence
	openAIBasisPointsUpstreamSilence = 60 * time.Millisecond
	defer func() { openAIBasisPointsUpstreamSilence = previous }()
	bridge := newBasisPointsTestBridge(t, "[]")
	upstreamReader, upstreamWriter := io.Pipe()
	body := bridge.stream(upstreamReader, 20*time.Millisecond, 0)
	defer func() { _ = body.Close() }()
	go func() {
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.created", `,"response":{"id":"r","model":"m","status":"in_progress","output":[]}`))
		_, _ = io.WriteString(upstreamWriter, bpsTestEvent("response.output_text.delta", `,"output_index":0,"item_id":"msg","delta":"hi"`))
		// 然后再也不写：保活只喂下游，不能替上游续命。
	}()
	raw, err := io.ReadAll(body)
	require.NoError(t, err)
	out := string(raw)
	require.Contains(t, out, `"delta":"hi"`)
	require.Contains(t, out, "event: response.failed")
	require.Contains(t, out, "basispoints_upstream_timeout")
	require.Contains(t, out, `"id":"r"`)
	_, writeErr := upstreamWriter.Write([]byte("x"))
	require.Error(t, writeErr, "上游读端已被关掉")
}

func TestOpenAIBasisPointsStreamBareErrorKeepsReadingForFailed(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	stream := bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`) +
		bpsTestEvent("error", `,"code":"server_error","message":"hiccup"`) +
		bpsTestEvent("response.failed", `,"response":{"id":"r","status":"failed","output":[],"usage":{"input_tokens":3,"output_tokens":0},"error":{"code":"server_error","message":"hiccup"}}`)
	raw, err := io.ReadAll(bridge.stream(io.NopCloser(strings.NewReader(stream)), 0, 0))
	require.NoError(t, err)
	out := string(raw)
	require.Contains(t, out, "event: error")
	require.Contains(t, out, "event: response.failed")
	require.Contains(t, out, `"input_tokens":3`, "裸 error 之后带 usage 的 failed 不能被截掉")
}

// 两道空闲上界都必须顺着运维填的 stream_data_interval_timeout 走：我们补的 response.in_progress
// 会刷新处理器的 lastReadAt，网关自己那道闸门永远不触发，硬编码的 15 分钟就成了唯一上界 ——
// 而槽位全程持有、开关账号建议并发 1，一发卡死就是整段零吞吐。
func TestOpenAIBasisPointsIdleBudgetsFollowGatewayConfig(t *testing.T) {
	svc := func(seconds int) *OpenAIGatewayService {
		return &OpenAIGatewayService{cfg: &config.Config{
			Gateway: config.GatewayConfig{StreamDataIntervalTimeout: seconds},
		}}
	}
	// 关掉配置（0）才回落到兜底上限。
	require.Equal(t, openAIBasisPointsUpstreamSilence, svc(0).openAIBasisPointsUpstreamSilenceBudget())
	// 合法下限 30：2× 只有 60s，被 peek 的等待上限抬到 3 分钟（接受之前都愿意等这么久）。
	require.Equal(t, openAIBasisPointsPeekSilence, svc(30).openAIBasisPointsUpstreamSilenceBudget())
	// 默认 180：2× = 6 分钟，远低于兜底的 15 分钟。
	require.Equal(t, 6*time.Minute, svc(180).openAIBasisPointsUpstreamSilenceBudget())
	// 合法上限 300：2× = 10 分钟，仍在兜底之下。
	require.Equal(t, 10*time.Minute, svc(300).openAIBasisPointsUpstreamSilenceBudget())
	require.Less(t, svc(300).openAIBasisPointsUpstreamSilenceBudget(), openAIBasisPointsUpstreamSilence,
		"合法配置范围内永远不该退到硬编码上界")
	// 保活周期同理按配置取小（配 30 时 30/3=10s，不能还用 30s 常量）。
	require.Equal(t, 10*time.Second, svc(30).openAIBasisPointsKeepaliveInterval())
	require.Equal(t, openAIBasisPointsKeepalive, svc(180).openAIBasisPointsKeepaliveInterval())
}

// BPS 偶发只把正文放在终态项里，一条 response.output_text.delta 都不发
// （JaxsonWang/cpa-plugin-oai-basispoints#12，issue 里点名了把这条响应转给下游的 sub2api）。
// 只拼 delta 的客户端会拿到一条 HTTP 200 的空回答 —— 这不在硬报错口径的覆盖范围里，报错收口
// 一条都拦不住，所以必须在转写器里补。
func TestOpenAIBasisPointsStreamBackfillsMissingTextDelta(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	msg := `{"type":"message","id":"msg_1","role":"assistant","status":"completed",` +
		`"content":[{"type":"output_text","text":"pong"}]}`
	stream := bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`) +
		bpsTestEvent("response.output_item.added", `,"output_index":0,"item":`+msg) +
		bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+msg) +
		bpsTestEvent("response.completed", `,"response":{"id":"r","status":"completed","output":[`+msg+`]}`)
	raw, err := io.ReadAll(bridge.stream(io.NopCloser(strings.NewReader(stream)), 0, 0))
	require.NoError(t, err)
	out := string(raw)
	require.Contains(t, out, "event: response.output_text.delta")
	require.Equal(t, 1, strings.Count(out, "event: response.output_text.delta"), "只补一条")
	require.Contains(t, out, `"delta":"pong"`)
	require.Contains(t, out, `"content_index":0`)
	require.Contains(t, out, `"item_id":"msg_1"`)
	// 补的那条必须排在 done 之前，否则只拼 delta 的客户端在 done 时正文还是空的。
	require.Less(t, strings.Index(out, "response.output_text.delta"),
		strings.Index(out, "event: response.output_item.done"))
	require.Contains(t, out, "event: response.completed")
}

// issue #12 的第二种形态：正文**只**出现在终态快照的 output[] 里，上游连 output_item.done 都不发。
// 上面那条用例走的是 done 那条路，盖不住这一格（变异实测：删掉终态快照里那次 backfillText，
// 上面那条照旧绿）。回归后果是 HTTP 200 空回答 —— 完全静默，客户端拿不到任何信号。
func TestOpenAIBasisPointsStreamBackfillsTextThatOnlyExistsInTheTerminalSnapshot(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	msg := `{"type":"message","id":"msg_1","role":"assistant","status":"completed",` +
		`"content":[{"type":"output_text","text":"snapshot only"}]}`
	stream := bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`) +
		bpsTestEvent("response.completed", `,"response":{"id":"r","status":"completed","output":[`+msg+`]}`)
	raw, err := io.ReadAll(bridge.stream(io.NopCloser(strings.NewReader(stream)), 0, 0))
	require.NoError(t, err)
	out := string(raw)
	require.Contains(t, out, "event: response.output_text.delta", "正文只在快照里也要补出来")
	require.Equal(t, 1, strings.Count(out, "event: response.output_text.delta"), "只补一条")
	require.Contains(t, out, `"delta":"snapshot only"`)
	require.Contains(t, out, `"item_id":"msg_1"`)
	require.Contains(t, out, "event: response.completed")
}

// 上游自己发了 delta 就一条都不补：两边都发会让同时读 delta 和终态项的客户端看到双份正文。
func TestOpenAIBasisPointsStreamDoesNotDuplicateExistingTextDelta(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	msg := `{"type":"message","id":"msg_1","role":"assistant","status":"completed",` +
		`"content":[{"type":"output_text","text":"pong"}]}`
	stream := bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`) +
		bpsTestEvent("response.output_item.added", `,"output_index":0,"item":`+msg) +
		bpsTestEvent("response.output_text.delta", `,"output_index":0,"item_id":"msg_1","content_index":0,"delta":"pong"`) +
		bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+msg) +
		bpsTestEvent("response.completed", `,"response":{"id":"r","status":"completed","output":[`+msg+`]}`)
	raw, err := io.ReadAll(bridge.stream(io.NopCloser(strings.NewReader(stream)), 0, 0))
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(raw), "event: response.output_text.delta"),
		"上游那条原样透传，不再补")
}

// refusal 与空文本不补（issue #12 的修复口径），reasoning 项同样不进这条路。
func TestOpenAIBasisPointsStreamBackfillSkipsNonTextParts(t *testing.T) {
	bridge := newBasisPointsTestBridge(t, "[]")
	msg := `{"type":"message","id":"msg_1","role":"assistant","status":"completed",` +
		`"content":[{"type":"output_text","text":""},{"type":"refusal","refusal":"no"}]}`
	reasoning := `{"type":"reasoning","id":"rs_1","summary":[]}`
	stream := bpsTestEvent("response.created", `,"response":{"id":"r","status":"in_progress","output":[]}`) +
		bpsTestEvent("response.output_item.done", `,"output_index":0,"item":`+reasoning) +
		bpsTestEvent("response.output_item.done", `,"output_index":1,"item":`+msg) +
		bpsTestEvent("response.completed", `,"response":{"id":"r","status":"completed","output":[`+reasoning+`,`+msg+`]}`)
	raw, err := io.ReadAll(bridge.stream(io.NopCloser(strings.NewReader(stream)), 0, 0))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "response.output_text.delta")
}
