//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetOpenAIUsage_OrdinaryOAuthUsesQuotaEndpoint(t *testing.T) {
	window := &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{
		PrimaryWindow:   &OpenAIRateLimitWindow{UsedPercent: 42, LimitWindowSeconds: 18000, ResetAfterSeconds: 3600},
		SecondaryWindow: &OpenAIRateLimitWindow{UsedPercent: 10, LimitWindowSeconds: 604800, ResetAfterSeconds: 86400},
	}}
	account := &Account{
		ID: 701, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{"chatgpt_account_id": "test-quota-account"},
		Extra:       buildCodexPrimaryWindowExtraUpdates(window, time.Now().Add(-time.Hour)),
	}
	require.False(t, account.IsOpenAIResponsesWebSocketV2Enabled())
	account.Extra["codex_5h_used_percent"] = float64(1)
	account.Extra["codex_7d_used_percent"] = float64(2)
	updates := make(chan map[string]any, 2)
	repo := &sparkShadowUsageTestRepo{
		accounts: map[int64]*Account{account.ID: account}, updateExtraCh: updates,
	}
	tokens := &stubQuotaTokenCache{tokens: map[string]string{OpenAITokenCacheKey(account): "test-quota-token"}}
	var usageCalls atomic.Int32
	var failUsage atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("quota refresh must never send inference/consume requests: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected write", http.StatusBadRequest)
			return
		}
		if r.Header.Get("authorization") != "Bearer test-quota-token" || r.Header.Get("chatgpt-account-id") != "test-quota-account" {
			t.Error("quota request did not use the ordinary account credentials")
		}
		w.Header().Set("content-type", "application/json")
		switch r.URL.Path {
		case "/backend-api/wham/usage":
			usageCalls.Add(1)
			if failUsage.Load() {
				http.Error(w, "test quota failure", http.StatusServiceUnavailable)
				return
			}
			if err := json.NewEncoder(w).Encode(window); err != nil {
				t.Error(err)
			}
		case "/backend-api/wham/rate-limit-reset-credits":
			// QueryUsage also reads credit details; it must not consume a credit.
			_, _ = w.Write([]byte(`{"available_count":0,"credits":[]}`))
		default:
			t.Errorf("unexpected quota refresh endpoint: %s", r.URL.Path)
			http.Error(w, "unexpected endpoint", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	svc := &AccountUsageService{
		accountRepo: repo,
		openAIQuotaService: NewOpenAIQuotaService(repo, nil, NewOpenAITokenProvider(repo, tokens, nil),
			newQuotaRedirectingFactory(server)),
		cache: &UsageCache{},
	}
	waitForPersistence := func() {
		t.Helper()
		select {
		case extra := <-updates:
			require.Equal(t, float64(42), extra["codex_5h_used_percent"])
			require.Equal(t, float64(10), extra["codex_7d_used_percent"])
		case <-time.After(2 * time.Second):
			t.Fatal("quota refresh did not persist the ordinary account windows")
		}
	}
	usage, err := svc.getOpenAIUsage(context.Background(), account, false)
	require.NoError(t, err)
	require.NotNil(t, usage.FiveHour)
	require.NotNil(t, usage.SevenDay)
	require.Equal(t, float64(42), usage.FiveHour.Utilization)
	require.Equal(t, int32(1), usageCalls.Load())
	waitForPersistence()

	_, err = svc.getOpenAIUsage(context.Background(), account, false)
	require.NoError(t, err)
	require.Equal(t, int32(1), usageCalls.Load(), "fresh snapshots must not issue another request")
	_, err = svc.getOpenAIUsage(context.Background(), account, true)
	require.NoError(t, err)
	require.Equal(t, int32(2), usageCalls.Load(), "explicit force must bypass the deadline")
	waitForPersistence()

	failUsage.Store(true)
	_, _ = svc.getOpenAIUsage(context.Background(), account, true)
	require.Equal(t, int32(3), usageCalls.Load(), "quota failure must not fall back to inference")
}

// 额度改从 /wham/usage 取（真实 Codex 客户端读的就是这个接口），不再向 /responses
// 发合成推理请求蹭响应头。这里钉住顶层 rate_limit 能映射出与旧路径同名的 extra 键。
func TestBuildCodexPrimaryWindowExtraUpdates(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	usage := &OpenAIQuotaUsage{
		RateLimit: &OpenAIRateLimit{
			PrimaryWindow:   &OpenAIRateLimitWindow{UsedPercent: 12, LimitWindowSeconds: 5 * 3600, ResetAfterSeconds: 900},
			SecondaryWindow: &OpenAIRateLimitWindow{UsedPercent: 34, LimitWindowSeconds: 7 * 24 * 3600, ResetAfterSeconds: 86400},
		},
	}
	updates := buildCodexPrimaryWindowExtraUpdates(usage, now)
	require.Equal(t, float64(12), updates["codex_5h_used_percent"])
	require.Equal(t, 900, updates["codex_5h_reset_after_seconds"])
	require.Equal(t, 300, updates["codex_5h_window_minutes"])
	require.Equal(t, float64(34), updates["codex_7d_used_percent"])
	require.Equal(t, 86400, updates["codex_7d_reset_after_seconds"])
	require.Equal(t, now.Add(900*time.Second).Format(time.RFC3339), updates["codex_5h_reset_at"])
	require.NotEmpty(t, updates["codex_usage_updated_at"])

	// 顶层没有 rate_limit 时不写任何键，交给调用方保持上一次的快照。
	require.Nil(t, buildCodexPrimaryWindowExtraUpdates(&OpenAIQuotaUsage{}, now))
	require.Nil(t, buildCodexPrimaryWindowExtraUpdates(nil, now))
}

// 固定 10 分钟的刷新节奏本身就是可识别的自动化特征，改成 [10,30] 分钟随机。
func TestNextOpenAIProbeAllowedAtJitters(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 200; i++ {
		d := nextOpenAIProbeAllowedAt(now).Sub(now)
		require.GreaterOrEqual(t, d, openAIProbeCacheTTL, "不得早于最小间隔")
		require.LessOrEqual(t, d, openAIProbeCacheTTLMax, "不得超过最大间隔")
		seen[d] = struct{}{}
	}
	require.Greater(t, len(seen), 1, "必须有抖动，否则和固定间隔没区别")
}

// shouldProbeOpenAICodexSnapshot 现在存的是"下次允许刷新的时刻"而不是"上次刷新时刻"。
func TestShouldProbeOpenAICodexSnapshotHonoursStoredDeadline(t *testing.T) {
	s := &AccountUsageService{cache: &UsageCache{}}
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	require.True(t, s.shouldProbeOpenAICodexSnapshot(1, now), "首次必须放行")
	require.False(t, s.shouldProbeOpenAICodexSnapshot(1, now.Add(openAIProbeCacheTTL-time.Second)),
		"最小间隔内不得再刷")
	require.True(t, s.shouldProbeOpenAICodexSnapshot(1, now.Add(openAIProbeCacheTTLMax+time.Second)),
		"超过最大间隔必定放行")
	require.True(t, s.shouldProbeOpenAICodexSnapshot(1, now, true), "force 无视间隔")
}
