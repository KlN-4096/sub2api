//go:build unit

package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// hunterAPIKeys 是探测记账的 API Key 桩：按 ID 返回带 User 的 key，并记下额度更新。
type hunterAPIKeys struct {
	key        *APIKey
	err        error
	quotaCalls int
}

func (k *hunterAPIKeys) GetByID(_ context.Context, id int64) (*APIKey, error) {
	if k.err != nil {
		return nil, k.err
	}
	if k.key == nil || k.key.ID != id {
		return nil, errors.New("api key not found")
	}
	return k.key, nil
}

func (k *hunterAPIKeys) UpdateQuotaUsed(context.Context, int64, float64) error {
	k.quotaCalls++
	return nil
}

func (k *hunterAPIKeys) UpdateRateLimitUsage(context.Context, int64, float64) error { return nil }

type probeUsageCapture struct {
	inputs []*OpenAIRecordUsageInput
}

func (p *probeUsageCapture) record(_ context.Context, input *OpenAIRecordUsageInput) error {
	p.inputs = append(p.inputs, input)
	return nil
}

func newProbeUsageHarness(t *testing.T, cfg map[string]any) (*hunterHarness, *probeUsageCapture, *hunterAPIKeys) {
	t.Helper()
	h := newHunterHarness(hunterTestAccount(cfg), hunterWebshareProxy)
	keys := &hunterAPIKeys{key: &APIKey{ID: 77, User: &User{ID: 5}, Group: &Group{ID: 3, RateMultiplier: 1}}}
	capture := &probeUsageCapture{}
	h.svc.SetAPIKeys(keys)
	h.svc.recordUsage = capture.record
	return h, capture, keys
}

// TestOpenAITurnStateHunterRecordsProbeUsage 钉住：配置了 usage_api_key_id 的账号，每次 200
// 探测按标准用量路径落一行——挂在那把 key 下、request_type=probe、输入 token 本地估算
// （含 base prompt，远不止 "hi" 两个字）、输出 0、上游响应头原样带上（用量表的 Turn-State 列
// 由此显示 292/312）、请求 ID 用上游 x-request-id。
func TestOpenAITurnStateHunterRecordsProbeUsage(t *testing.T) {
	now := time.Now().UTC()
	h, capture, _ := newProbeUsageHarness(t, hunterConfig(map[string]any{"usage_api_key_id": float64(77), "reasoning_effort": "medium"}))
	degraded, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	degraded.Header.Set("X-Request-Id", "req_probe_1")
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{degraded, healthy}

	h.run(t)

	require.Len(t, h.up.requests, 2)
	require.Len(t, capture.inputs, 2, "312 与 292 都是 200，都计费")
	first := capture.inputs[0]
	require.Equal(t, int64(77), first.APIKey.ID)
	require.Equal(t, int64(5), first.User.ID)
	require.Equal(t, h.account.ID, first.Account.ID)
	require.Equal(t, RequestTypeTurnStateProbe, first.RequestType)
	require.Equal(t, "req_probe_1", first.Result.RequestID)
	require.Equal(t, hunterTestModel, first.Result.Model)
	require.True(t, first.Result.Stream)
	require.Greater(t, first.Result.Usage.InputTokens, 500, "估算含该模型的 base prompt")
	require.Zero(t, first.Result.Usage.OutputTokens)
	require.NotNil(t, first.Result.ReasoningEffort)
	require.Equal(t, "medium", *first.Result.ReasoningEffort)
	require.Equal(t, openAIDegradedTurnStateLen, len(first.Result.UpstreamHeaders.Get("x-codex-turn-state")))
	require.Empty(t, first.TurnStateSource, "探测裸发，不是注入")
	require.NotNil(t, first.APIKeyService, "额度更新走同一把 key")
	require.Equal(t, "turn-state-probe", first.InboundEndpoint)
	require.NotEmpty(t, first.UpstreamEndpoint)
	require.NotEmpty(t, first.SessionID)

	second := capture.inputs[1]
	require.Contains(t, second.Result.RequestID, "turn_state_probe:", "上游没给 x-request-id 就自造，计费幂等键不能撞")
	require.NotEqual(t, first.Result.RequestID, second.Result.RequestID)
}

// TestOpenAITurnStateHunterProbeUsageSkips 列出不记的情形：没配 key、key 取不到、探测非 200。
func TestOpenAITurnStateHunterProbeUsageSkips(t *testing.T) {
	now := time.Now().UTC()
	t.Run("没配 usage_api_key_id", func(t *testing.T) {
		h, capture, _ := newProbeUsageHarness(t, hunterConfig(nil))
		healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
		h.up.queue = []*http.Response{healthy}
		h.run(t)
		require.Len(t, h.up.requests, 1)
		require.Empty(t, capture.inputs)
	})
	t.Run("key 取不到", func(t *testing.T) {
		h, capture, keys := newProbeUsageHarness(t, hunterConfig(map[string]any{"usage_api_key_id": float64(78)}))
		keys.err = errors.New("db down")
		healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
		h.up.queue = []*http.Response{healthy}
		h.run(t)
		require.Len(t, h.up.requests, 1, "记账失败不影响探测本身")
		require.Empty(t, capture.inputs)
		require.True(t, h.state().Last[0].Healthy, "票照样入池")
	})
	t.Run("探测非 200", func(t *testing.T) {
		h, capture, _ := newProbeUsageHarness(t, hunterConfig(map[string]any{"usage_api_key_id": float64(77)}))
		// 429 与别的失败一视同仁地重试，队列要够 strike 次。
		for range openAITurnStateHuntFailureStrikes {
			limited, _ := hunterResp(http.StatusTooManyRequests, "", `{"error":{"message":"slow down"}}`)
			h.up.queue = append(h.up.queue, limited)
		}
		h.run(t)
		require.Len(t, h.up.requests, openAITurnStateHuntFailureStrikes)
		require.Empty(t, capture.inputs, "失败请求与人工流量一样不进使用记录")
	})
}

type hunterSubscriptions struct {
	sub   *UserSubscription
	err   error
	calls int
}

func (s *hunterSubscriptions) GetActiveSubscription(context.Context, int64, int64) (*UserSubscription, error) {
	s.calls++
	return s.sub, s.err
}

// TestOpenAITurnStateHunterProbeUsageSubscriptionGroup 钉住订阅型分组：有有效订阅就带上按订阅
// 计费；没有就不记（RecordUsage 拿不到订阅会退到余额扣费还允许透支）。
func TestOpenAITurnStateHunterProbeUsageSubscriptionGroup(t *testing.T) {
	now := time.Now().UTC()
	mk := func(t *testing.T, subs *hunterSubscriptions) (*hunterHarness, *probeUsageCapture) {
		t.Helper()
		h, capture, keys := newProbeUsageHarness(t, hunterConfig(map[string]any{"usage_api_key_id": float64(77)}))
		keys.key.Group = &Group{ID: 3, RateMultiplier: 1, SubscriptionType: SubscriptionTypeSubscription}
		if subs != nil {
			h.svc.SetSubscriptions(subs)
		}
		healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
		h.up.queue = []*http.Response{healthy}
		return h, capture
	}
	t.Run("有有效订阅", func(t *testing.T) {
		subs := &hunterSubscriptions{sub: &UserSubscription{ID: 9}}
		h, capture := mk(t, subs)
		h.run(t)
		require.Len(t, capture.inputs, 1)
		require.NotNil(t, capture.inputs[0].Subscription)
		require.Equal(t, int64(9), capture.inputs[0].Subscription.ID)
	})
	t.Run("没有有效订阅", func(t *testing.T) {
		h, capture := mk(t, &hunterSubscriptions{})
		h.run(t)
		require.Empty(t, capture.inputs, "订阅型分组没订阅：不记，别扣到余额上")
		require.True(t, h.state().Last[0].Healthy, "票照样入池")
	})
	t.Run("没装配订阅服务", func(t *testing.T) {
		h, capture := mk(t, nil)
		h.run(t)
		require.Empty(t, capture.inputs)
	})
}

func TestOpenAITurnStateProbeInputTokensCountsBasePrompt(t *testing.T) {
	n := openAITurnStateProbeInputTokens(hunterTestModel, "high")
	require.Greater(t, n, 500)
}

func TestRequestTypeProbeRoundTrip(t *testing.T) {
	parsed, err := ParseUsageRequestType("probe")
	require.NoError(t, err)
	require.Equal(t, RequestTypeTurnStateProbe, parsed)
	require.Equal(t, "probe", RequestTypeTurnStateProbe.String())
	require.True(t, RequestTypeTurnStateProbe.IsValid())
}
