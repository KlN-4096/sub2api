package dto

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUsageLogFromService_MapsTPSInputs(t *testing.T) {
	t.Parallel()

	firstAnswer, reasoning := 4200, 0
	log := &service.UsageLog{RequestID: "resp_tps", Model: "gpt-6-astra", FirstAnswerMs: &firstAnswer, ReasoningTokens: &reasoning}
	for _, out := range []any{UsageLogFromService(log), UsageLogFromServiceAdmin(log)} {
		raw, err := json.Marshal(out)
		require.NoError(t, err)
		require.Contains(t, string(raw), `"first_answer_ms":4200`)
		require.Contains(t, string(raw), `"reasoning_tokens":0`, "上游报了 0 也要输出")
	}

	raw, err := json.Marshal(UsageLogFromService(&service.UsageLog{RequestID: "resp_plain", Model: "gpt-6-astra"}))
	require.NoError(t, err)
	require.NotContains(t, string(raw), "first_answer_ms")
	require.NotContains(t, string(raw), "reasoning_tokens")
}
