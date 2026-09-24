package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
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
		name             string
		lite             bool
		compact          bool
		noWireProfile    bool
		mapping          map[string]any
		wantInstructions bool
	}{
		{name: "real lite", lite: true},
		{name: "no lite header", wantInstructions: true},
		{name: "lite mapped to gpt-5.5", lite: true, mapping: map[string]any{"gpt-6-astra": "gpt-5.5"}, wantInstructions: true},
		{name: "lite compact", lite: true, compact: true, wantInstructions: true},
		{name: "lite without wire profile", lite: true, noWireProfile: true, wantInstructions: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := realCodexLiteRequestBody(t, "gpt-6-astra")
			c := newConvTestContext(t, body)
			if tc.lite {
				c.Request.Header.Set(responsesLiteHeader, "true")
			}
			if tc.compact {
				c.Request.URL.Path = "/v1/responses/compact"
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
			require.Equal(t, "OFFLINE BASE PROMPT", gjson.GetBytes(out, "input.1.content.0.text").String())
			if !tc.wantInstructions {
				require.Equal(t, "true", up.lastReq.Header.Get(responsesLiteHeader))
			}
		})
	}
}
