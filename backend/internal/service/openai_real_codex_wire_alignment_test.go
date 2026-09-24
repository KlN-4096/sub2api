package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// realCodexLiteRequestBody 按真实 Codex 0.155+ 的 Lite 形状造请求：不发 instructions，
// 基础提示在 input 的 developer 消息里，input[0] 是 additional_tools。
func realCodexLiteRequestBody(t *testing.T, model string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"model": model, "stream": false, "store": false,
		"tool_choice": "auto", "parallel_tool_calls": false,
		"include": []any{"reasoning.encrypted_content"},
		"input": []any{
			map[string]any{"type": "additional_tools", "id": "at_offline", "role": "developer", "tools": []any{
				map[string]any{"type": "function", "name": "shell", "parameters": map[string]any{"type": "object"}},
			}},
			map[string]any{"type": "message", "id": "msg_base", "role": "developer", "content": []any{
				map[string]any{"type": "input_text", "text": "OFFLINE BASE PROMPT"},
			}},
			map[string]any{"type": "message", "id": "msg_user", "role": "user", "content": []any{
				map[string]any{"type": "input_text", "text": "hi"},
			}},
		},
	})
	require.NoError(t, err)
	return raw
}

func TestForwardRealCodexLiteOmitsInstructions(t *testing.T) {
	cases := []struct {
		name                string
		model               string
		clientModel         string // handler 在渠道映射之前记下的客户端原始模型
		lite                bool
		compact             bool
		compactionTrigger   bool // 原生 v2 压缩回合：普通 /responses + 末尾 compaction_trigger
		thirdParty          bool
		dropAdditionalTools bool
		noWireProfile       bool
		mapping             map[string]any
		wantInstructions    bool
	}{
		{name: "real lite", lite: true},
		{name: "real lite with client model recorded", clientModel: "gpt-6-astra", lite: true},
		{name: "no lite header", wantInstructions: true},
		{name: "lite gpt-5.5 requested directly", model: "gpt-5.5", lite: true, wantInstructions: true},
		{name: "lite mapped to gpt-5.5", lite: true, mapping: map[string]any{"gpt-6-astra": "gpt-5.5"}, wantInstructions: true},
		{name: "lite mapped to another model", lite: true, mapping: map[string]any{"gpt-6-astra": "gpt-5.4"}, wantInstructions: true},
		{name: "lite channel mapped", clientModel: "gpt-6-sol", lite: true, wantInstructions: true},
		{name: "lite compact", lite: true, compact: true, wantInstructions: true},
		{name: "lite native v2 compaction", lite: true, compactionTrigger: true},
		{name: "lite third party", lite: true, thirdParty: true, wantInstructions: true},
		{name: "lite without additional_tools first", lite: true, dropAdditionalTools: true, wantInstructions: true},
		{name: "lite without wire profile", lite: true, noWireProfile: true, wantInstructions: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := tc.model
			if model == "" {
				model = "gpt-6-astra"
			}
			body := realCodexLiteRequestBody(t, model)
			basePromptPath := "input.1.content.0.text"
			if tc.dropAdditionalTools {
				var err error
				body, err = sjson.DeleteBytes(body, "input.0")
				require.NoError(t, err)
				basePromptPath = "input.0.content.0.text"
			}
			if tc.compactionTrigger {
				var err error
				body, err = sjson.SetRawBytes(body, "input.-1", []byte(`{"type":"compaction_trigger"}`))
				require.NoError(t, err)
			}
			c := newConvTestContext(t, body)
			if tc.lite {
				c.Request.Header.Set(responsesLiteHeader, "true")
			}
			if tc.compact {
				c.Request.URL.Path = "/v1/responses/compact"
			}
			if tc.compactionTrigger {
				MarkOpenAINativeCompactionV2(c)
			}
			if tc.thirdParty {
				c.Request.Header.Set("User-Agent", "OpenAI/Python 1.99.0")
				c.Request.Header.Del("originator")
			}
			if tc.clientModel != "" {
				c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.Model, tc.clientModel))
			}
			account := wireProfileTestAccount(!tc.noWireProfile)
			if tc.mapping != nil {
				account.Credentials["model_mapping"] = tc.mapping
			}
			svc, up := wireProfileTestService()
			_, _ = svc.Forward(context.Background(), c, account, body)
			require.NotNil(t, up.lastReq)
			out := up.lastBody
			require.Equal(t, tc.wantInstructions, gjson.GetBytes(out, "instructions").Exists(), string(out))
			require.Equal(t, "OFFLINE BASE PROMPT", gjson.GetBytes(out, basePromptPath).String())
			if !tc.wantInstructions {
				require.Equal(t, "true", up.lastReq.Header.Get(responsesLiteHeader))
			}
		})
	}
}

// 原生 v2 压缩回合（普通 /responses + compaction_trigger）与普通 Lite 轮次同形：首发照真客户端不带
// instructions（整个会话形态不切换、前缀缓存不断）。上游报上下文超限时换兜底模型重试，兜底落到
// gpt-5.5 时出站摘掉 Lite 头，重试体必须补回 instructions。三条兜底分支（HTTP 4xx、非流式与流式
// 的 SSE 失败终态）各走一遍。
func TestForwardRealCodexLiteCompactionFallbackKeepsInstructions(t *testing.T) {
	sseFailed := "event: response.failed\n" +
		`data: {"type":"response.failed","response":{"status":"failed","error":{"code":"context_length_exceeded","message":"context window exceeded"}}}` + "\n\n"
	for _, tc := range []struct {
		name        string
		stream      bool
		status      int
		contentType string
		payload     string
	}{
		{name: "http 400", status: http.StatusBadRequest, contentType: "application/json",
			payload: `{"error":{"code":"context_length_exceeded","message":"context window exceeded"}}`},
		{name: "sse failure non-stream", status: http.StatusOK, contentType: "text/event-stream", payload: sseFailed},
		{name: "sse failure stream", stream: true, status: http.StatusOK, contentType: "text/event-stream", payload: sseFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := sjson.SetRawBytes(realCodexLiteRequestBody(t, "gpt-6-astra"), "input.-1", []byte(`{"type":"compaction_trigger"}`))
			require.NoError(t, err)
			body, err = sjson.SetBytes(body, "stream", tc.stream)
			require.NoError(t, err)
			c := newConvTestContext(t, body)
			c.Request.Header.Set(responsesLiteHeader, "true")
			MarkOpenAINativeCompactionV2(c)
			svc, up := wireProfileTestService()
			svc.cfg.Gateway.OpenAICompactModel = "gpt-5.5"
			up.responses = []*http.Response{{
				StatusCode: tc.status,
				Header:     http.Header{"Content-Type": []string{tc.contentType}},
				Body:       io.NopCloser(strings.NewReader(tc.payload)),
			}}
			_, _ = svc.Forward(context.Background(), c, wireProfileTestAccount(true), body)
			require.Len(t, up.bodies, 2)
			require.False(t, gjson.GetBytes(up.bodies[0], "instructions").Exists(), string(up.bodies[0]))
			require.Equal(t, "true", up.requests[0].Header.Get(responsesLiteHeader))
			require.Equal(t, "gpt-5.5", gjson.GetBytes(up.bodies[1], "model").String())
			require.Empty(t, up.requests[1].Header.Get(responsesLiteHeader))
			retryInstructions := gjson.GetBytes(up.bodies[1], "instructions")
			require.Equal(t, defaultCodexSynthInstructions("gpt-6-astra"), retryInstructions.String(),
				"补回的是首发模型那份，不是兜底模型的")
			require.Contains(t, retryInstructions.Raw, "<")
			wantRaw, err := marshalOpenAIUpstreamJSON(defaultCodexSynthInstructions("gpt-6-astra"))
			require.NoError(t, err)
			require.Equal(t, string(wantRaw), retryInstructions.Raw, "注入值按原字节写入，不做 HTML 转义（真客户端 serde_json 不转义）")
		})
	}
}

// 只补缺失的：首发体里已有的 instructions（例如 role:system 提升上来的）不能被默认文本盖掉。
func TestWithCompactFallbackInstructions(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","input":[]}`)
	out, err := withCompactFallbackInstructions(body, "")
	require.NoError(t, err)
	require.Equal(t, string(body), string(out))

	out, err = withCompactFallbackInstructions(body, "BASE")
	require.NoError(t, err)
	require.Equal(t, "BASE", gjson.GetBytes(out, "instructions").String())

	// 带换行时 sjson.SetBytes 会改走 encoding/json（转义 HTML），必须照原字节写入。
	out, err = withCompactFallbackInstructions(body, "a<b>\n&c")
	require.NoError(t, err)
	require.Contains(t, string(out), `"instructions":"a<b>\n&c"`)

	kept := []byte(`{"model":"gpt-5.5","instructions":"CLIENT SYSTEM","input":[]}`)
	out, err = withCompactFallbackInstructions(kept, "BASE")
	require.NoError(t, err)
	require.Equal(t, string(kept), string(out))
}

// 真实 Codex 的非 /responses 请求都不显式设 Accept，出站的是 reqwest 默认的 */*
// （backend-client 的 headers() 只放 UA/鉴权/账号/FedRAMP；search 与 models 同理）。
// 与其它对齐项一样只对双开账号，未双开的额度面字节不变（原先不带 Accept）。
func TestCodexSideRequestsAcceptMatchesRealClient(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		want    string
	}{{true, "*/*"}, {false, ""}} {
		account := wireProfileTestAccount(tc.enabled)
		headers, _, err := (&OpenAIQuotaService{}).buildCodexQuotaHeaders(&openAIQuotaCall{
			account: account, forwardedRow: account, accessToken: "t", chatGPTAccountID: "a",
		})
		require.NoError(t, err)
		require.Equal(t, tc.want, headers["accept"], "额度面（wham）enabled=%v", tc.enabled)
	}

	body := []byte(`{"id":"session","model":"gpt-5.5","input":[]}`)
	for _, tc := range []struct {
		enabled bool
		want    string
	}{{true, "*/*"}, {false, "application/json"}} {
		c := newConvTestContext(t, body)
		c.Request.URL.Path = "/v1/alpha/search"
		svc, _ := wireProfileTestService()
		req, err := svc.buildOpenAIAlphaSearchRequest(context.Background(), c, wireProfileTestAccount(tc.enabled), body, "offline-token")
		require.NoError(t, err)
		require.Equal(t, tc.want, req.Header.Get("Accept"), "alpha/search enabled=%v", tc.enabled)

		req, err = (&AccountTestService{}).buildOpenAIOAuthUpstreamModelsRequest(context.Background(), wireProfileTestAccount(tc.enabled))
		require.NoError(t, err)
		require.Equal(t, tc.want, req.Header.Get("Accept"), "管理端模型同步 enabled=%v", tc.enabled)
	}
}

// 真实 Codex 原样回放上游签发的 call_*；第三方客户端与旧网关污染的其它前缀照旧归一成 fc_/ctc_。
func TestForwardCodexCLIPreservesUpstreamCallIDs(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"model": "gpt-6-astra", "stream": true, "store": false, "tool_choice": "auto",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "function_call", "call_id": "call_A1", "name": "shell", "arguments": "{}"},
			map[string]any{"type": "function_call_output", "call_id": "call_A1", "output": "ok"},
			map[string]any{"type": "custom_tool_call", "call_id": "call_B2", "name": "apply_patch", "input": "x"},
			map[string]any{"type": "custom_tool_call_output", "call_id": "call_B2", "output": "ok"},
			map[string]any{"type": "custom_tool_call", "call_id": "fc_C3", "name": "apply_patch", "input": "y"},
			map[string]any{"type": "custom_tool_call_output", "call_id": "fc_C3", "output": "ok"},
		},
	})
	require.NoError(t, err)
	const codexUA = "codex_cli_rs/0.155.1 (Windows 10.0.26200; x86_64) WindowsTerminal"
	cases := []struct {
		name, ua, originator, wantFC, wantCTC string
		noWireProfile                         bool
	}{
		{name: "codex cli", ua: codexUA, originator: "codex_cli_rs", wantFC: "call_A1", wantCTC: "call_B2"},
		{name: "third party", ua: "OpenAI/Python 1.99.0", wantFC: "fc_A1", wantCTC: "ctc_B2"},
		{name: "codex cli without wire profile", ua: codexUA, originator: "codex_cli_rs", noWireProfile: true, wantFC: "fc_A1", wantCTC: "ctc_B2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newConvTestContext(t, body)
			c.Request.Header.Set("User-Agent", tc.ua)
			c.Request.Header.Set("originator", tc.originator)
			svc, up := wireProfileTestService()
			_, _ = svc.Forward(context.Background(), c, wireProfileTestAccount(!tc.noWireProfile), body)
			require.NotNil(t, up.lastReq)
			out := up.lastBody
			for path, want := range map[string]string{
				"input.1.call_id": tc.wantFC, "input.2.call_id": tc.wantFC,
				"input.3.call_id": tc.wantCTC, "input.4.call_id": tc.wantCTC,
				"input.5.call_id": "ctc_C3", "input.6.call_id": "ctc_C3",
			} {
				require.Equal(t, want, gjson.GetBytes(out, path).String(), path)
			}
		})
	}
}
