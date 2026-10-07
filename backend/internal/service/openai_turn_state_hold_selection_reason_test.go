//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 降智暂停必须在 **OpenAI 侧** 的两个过滤点单独成类。Codex 走的是 OpenAIGatewayService 的调度，
// 错误串由 openAISelectionFilterStats.summary 产出（"pool=1, filtered: turn_state_hold=1"）；
// 归进 model_rate_limited 的话 handler 回 429「所有账号都在限流」，Codex 当限流硬重试并吞掉正文，
// 用户只看得到 "exceeded retry limit, last status: 429"。
func newHeldTestAccount(t *testing.T, limitKey string, reason string) *Account {
	t.Helper()
	a := newTestOAuthAccount(9301, map[string]any{
		"model_rate_limits": map[string]any{
			limitKey: map[string]any{
				"rate_limited_at":     time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
				"rate_limit_reset_at": time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339),
				"reason":              reason,
			},
		},
	})
	a.Status, a.Schedulable, a.Concurrency = StatusActive, true, 1
	a.Credentials = map[string]any{"access_token": "offline-token", "chatgpt_account_id": "offline-account"}
	return a
}

func TestOpenAISelectionReason_TurnStateHoldSeparatedFromRateLimit(t *testing.T) {
	ctx := context.Background()
	sched := &defaultOpenAIAccountScheduler{}

	cases := []struct {
		name       string
		limitKey   string
		reason     string
		reqModel   string
		wantReason string
	}{
		{"降智暂停单独成类", "gpt-6-astra", openAITurnStateHoldLimitReason, "gpt-6-astra", openAITurnStateHoldLimitReason},
		{"真限流仍是限流", "gpt-6-astra", "", "gpt-6-astra", "model_rate_limited"},
		// B2：写入侧落的是规范名 gpt-6-astra，客户端发的是别名 gpt-6。按裸模型名精确查会漏判，
		// 分类掉回 model_rate_limited，用户又收到 429。
		{"别名请求命中规范键", "gpt-6-astra", openAITurnStateHoldLimitReason, "gpt-6", openAITurnStateHoldLimitReason},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			acc := newHeldTestAccount(t, tc.limitKey, tc.reason)

			// 前提：两种原因都让账号在调度上被挡下，差别只在归类。
			require.False(t, acc.IsSchedulableForModelWithContext(ctx, tc.reqModel), "账号应被模型级限流挡下")

			got := openAICompatibleAccountEligibilityFailureReasonBeforeProfit(
				ctx, acc, PlatformOpenAI, tc.reqModel, false, OpenAIEndpointCapability(""),
			)
			require.Equal(t, tc.wantReason, got, "legacy 选号的归类")

			_, reason := sched.isAccountRequestCompatibleReason(ctx, acc, OpenAIAccountScheduleRequest{RequestedModel: tc.reqModel})
			require.Equal(t, tc.wantReason, reason, "TopK 主过滤的归类")

			// summary 的实际渲染形状：handler 靠这个串认计数，没人钉住的话改个 reason 名
			// 两边测试都绿而生产静默回 429。
			stats := openAISelectionFilterStats{pool: 1}
			stats.exclude(got)
			require.Equal(t,
				"pool=1, filtered: "+tc.wantReason+"=1",
				stats.summary(""),
				"空池错误里客户端看到的那串",
			)
		})
	}
}

// 降智暂停的 reason 必须与 model_rate_limits 条目的 reason 是同一个串，也必须与 handler
// 正则拼用的导出常量是同一个串——三处任何一处漂移，分类都会静默失效。
func TestOpenAITurnStateHoldReasonIsOneString(t *testing.T) {
	require.Equal(t, OpenAITurnStateHoldSelectionReason, openAITurnStateHoldLimitReason)
	require.Equal(t, "turn_state_hold", OpenAITurnStateHoldSelectionReason)
}

// 暂停到期后不再是暂停：条目还在（releaseHold 写的是 now-1s 的过期条目），但既不挡调度也不该归类。
func TestOpenAISelectionReason_ExpiredHoldIsNotHeld(t *testing.T) {
	ctx := context.Background()
	acc := newTestOAuthAccount(9302, map[string]any{
		"model_rate_limits": map[string]any{
			"gpt-6-astra": map[string]any{
				"rate_limit_reset_at": time.Now().Add(-time.Second).UTC().Format(time.RFC3339),
				"reason":              openAITurnStateHoldLimitReason,
			},
		},
	})
	acc.Status, acc.Schedulable, acc.Concurrency = StatusActive, true, 1

	for _, model := range []string{"gpt-6-astra", "gpt-6"} {
		limited, held := acc.modelRateLimitStateForRequest(ctx, model, time.Now())
		require.False(t, limited, model)
		require.False(t, held, model)
	}
}
