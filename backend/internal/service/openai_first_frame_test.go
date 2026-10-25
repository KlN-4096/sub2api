package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIFirstFrameSkipsNonResponseFrames(t *testing.T) {
	start := time.Now().Add(-time.Second)
	var f openAIFirstFrame
	f.observe("", "response.created", start)
	f.observe(" [DONE] ", "", start)
	for _, eventType := range []string{"", " ", "keepalive", "error", "response.failed"} {
		f.observe(`{"type":"`+eventType+`"}`, eventType, start)
		f.observeEvent(eventType, start)
	}
	require.Nil(t, f.ms)

	f.observe(`{"type":"codex.response.metadata"}`, "codex.response.metadata", start)
	require.NotNil(t, f.ms)
	first := *f.ms
	require.GreaterOrEqual(t, first, 1000)

	f.observeEvent("response.created", time.Now().Add(-time.Hour))
	require.Equal(t, first, *f.ms, "only the first response frame counts")
}

// openAIStreamDataStartsTTFT 不再决定 first_token_ms，但原生路径流内的首字变量还靠它判
// 首个可见输出要不要强制 flush，这里守住两种口径的判据。
func TestOpenAIStreamDataStartsTTFTKeepsInStreamModes(t *testing.T) {
	emptyReasoning := `{"type":"response.output_item.added","item":{"id":"item_test","type":"reasoning","summary":[]}}`
	textDelta := `{"type":"response.output_text.delta","delta":"hi"}`
	failed := `{"type":"response.failed","response":{"id":"resp_test","status":"failed"}}`
	overloaded := `{"type":"error","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`
	tests := []struct {
		name, data, mode string
		force, want      bool
	}{
		{"semantic skips created", `{"type":"response.created","response":{"id":"resp_test"}}`, OpenAITTFTModeSemantic, false, false},
		{"semantic skips keepalive", `{"type":"keepalive"}`, OpenAITTFTModeSemantic, false, false},
		{"semantic counts empty reasoning item", emptyReasoning, OpenAITTFTModeSemantic, false, true},
		{"semantic counts text delta", textDelta, OpenAITTFTModeSemantic, false, true},
		{"semantic skips failed", failed, OpenAITTFTModeSemantic, false, false},
		{"semantic skips retryable error", overloaded, OpenAITTFTModeSemantic, false, false},
		{"semantic force counts failed", failed, OpenAITTFTModeSemantic, true, true},
		{"semantic skips done", "[DONE]", OpenAITTFTModeSemantic, false, false},
		{"visible skips empty reasoning item", emptyReasoning, OpenAITTFTModeVisible, false, false},
		{"visible counts text delta", textDelta, OpenAITTFTModeVisible, false, true},
		{"visible ignores force", failed, OpenAITTFTModeVisible, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventType := gjson.Get(tt.data, "type").String()
			require.Equal(t, tt.want, openAIStreamDataStartsTTFT(tt.data, eventType, tt.force, tt.mode))
		})
	}
}
