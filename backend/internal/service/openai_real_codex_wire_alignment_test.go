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
			c := newConvTestContext(t, body)
			if tc.lite {
				c.Request.Header.Set(responsesLiteHeader, "true")
			}
			if tc.compact {
				c.Request.URL.Path = "/v1/responses/compact"
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

// 原生 v2 压缩回合（普通 /responses + compaction_trigger，形状与普通 Lite 轮次相同）上游报
// 上下文超限时会换兜底模型重试；兜底落到 gpt-5.5 时出站摘掉 Lite 头，重试体必须仍带 instructions。
func TestForwardRealCodexLiteCompactionFallbackKeepsInstructions(t *testing.T) {
	body, err := sjson.SetRawBytes(realCodexLiteRequestBody(t, "gpt-6-astra"), "input.-1", []byte(`{"type":"compaction_trigger"}`))
	require.NoError(t, err)
	c := newConvTestContext(t, body)
	c.Request.Header.Set(responsesLiteHeader, "true")
	MarkOpenAINativeCompactionV2(c)
	svc, up := wireProfileTestService()
	svc.cfg.Gateway.OpenAICompactModel = "gpt-5.5"
	up.responses = []*http.Response{{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"context_length_exceeded","message":"context window exceeded"}}`)),
	}}
	_, _ = svc.Forward(context.Background(), c, wireProfileTestAccount(true), body)
	require.Len(t, up.bodies, 2)
	require.Equal(t, "gpt-5.5", gjson.GetBytes(up.bodies[1], "model").String())
	require.Empty(t, up.requests[1].Header.Get(responsesLiteHeader))
	require.True(t, gjson.GetBytes(up.bodies[1], "instructions").Exists(), string(up.bodies[1]))
}

// 真实 Codex 的非 /responses 请求都不显式设 Accept，出站的是 reqwest 默认的 */*
// （backend-client 的 headers() 只放 UA/鉴权/账号/FedRAMP；search 与 models 同理）。
func TestCodexSideRequestsAcceptMatchesRealClient(t *testing.T) {
	require.Equal(t, "*/*", buildCodexCommonHeaders("t", "a", false)["accept"], "额度面（wham）")

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
