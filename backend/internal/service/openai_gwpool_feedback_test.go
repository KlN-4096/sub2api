package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

func TestGatewayPoolRecommendationsRefreshCurrentCycleButNotFixed(t *testing.T) {
	store := &openAICodexCookieStore{}
	rec := &gwpool.CooldownRecommendation{Seconds: 7200, Samples: 2, Source: "account"}
	store.noteGatewayPoolRecommendation(gwpoolTestIdentity, "unified-142", rec)
	require.True(t, store.beginGatewayPoolAttempt(gwpoolTestIdentity, "unified-142", time.Hour, true))
	c, _ := store.cooldownEntry(gwpoolTestIdentity, "unified-142")
	require.Equal(t, 7200, c.WindowSeconds, "不是只显示建议，必须真正进入冷却状态")
	require.Equal(t, "account", c.RecommendationSource)
	until := c.Until
	store.noteGatewayPoolRecommendation(gwpoolTestIdentity, "unified-142",
		&gwpool.CooldownRecommendation{Seconds: 3600, Samples: 2, Source: "account"})
	store.gatewayPoolUsedAt(gwpoolTestIdentity, "unified-142", time.Hour, true)
	unchanged, _ := store.cooldownEntry(gwpoolTestIdentity, "unified-142")
	require.Equal(t, until.Add(-time.Hour), unchanged.Until, "only the recommendation's added hour is removed")
	c.FixedSeconds, c.Successes = 3600, map[int]int{3600: 2}
	store.poolCooldown[gatewayPoolLedgerKey(gwpoolTestIdentity, "unified-142")] = c
	store.noteGatewayPoolRecommendation(gwpoolTestIdentity, "unified-142", rec)
	store.observeGatewayPoolCooldown(gwpoolTestIdentity, "unified-142", openAIGatewayVerdictFull, time.Hour, time.Time{}, true)
	c, _ = store.cooldownEntry(gwpoolTestIdentity, "unified-142")
	require.Equal(t, 3600, c.WindowSeconds, "本地已学到的固定档优先")
}

func TestGatewayPoolNewRecommendationUpdatesExistingNextWindow(t *testing.T) {
	now := time.Now().UTC()
	c := gatewayPoolCooldown{WindowSeconds: 3600, Until: now, Outcome: openAIGatewayVerdictFull}
	require.True(t, c.begin(now, time.Time{}, 7200))
	require.Equal(t, 3600, c.AttemptSeconds, "上一档实际等了1h，不能伪报等了2h")
	require.Equal(t, 7200, c.WindowSeconds, "新推荐必须用于已有网关的下一窗口")
	require.Equal(t, now.Add(2*time.Hour), c.Until)
	c = gatewayPoolCooldown{WindowSeconds: 14400, Until: now, Outcome: openAIGatewayVerdictDegraded}
	require.True(t, c.begin(now, time.Time{}, 7200))
	require.Equal(t, 14400, c.WindowSeconds, "推荐不能覆盖本地失败后的更长退避")
}

func TestGatewayPoolDegradedObservationKeepsLongerNewRecommendation(t *testing.T) {
	now := time.Now().UTC()
	for _, fixed := range []int{0, 3600} {
		c := gatewayPoolCooldown{WindowSeconds: 3600, FixedSeconds: fixed, Until: now}
		require.True(t, c.begin(now, time.Time{}, 14400))
		c.observe(now, openAIGatewayVerdictDegraded, 14400)
		require.Zero(t, c.FixedSeconds)
		require.Equal(t, 14400, c.WindowSeconds)
		require.Equal(t, now.Add(4*time.Hour), c.Until, "失败不应把新推荐4h倒退为2h")
	}
}

func TestGatewayPoolCooldownReportRetriesSameEvent(t *testing.T) {
	got := make(chan gwpool.CooldownReport, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var report gwpool.CooldownReport
		require.NoError(t, json.NewDecoder(r.Body).Decode(&report))
		got <- report
		if len(got) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-consumer"
	svc, _ := gatewayRuntimeService(account)
	svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
		Gateway: "unified-142", AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
	})
	svc.flushGatewayPoolReports(context.Background())
	require.NoError(t, svc.changeGatewayPoolOutbox(context.Background(), 1, func(_ *Account, box *gatewayPoolOutbox) {
		box.Pending[0].NextAt = time.Time{}
	}))
	svc.flushGatewayPoolReports(context.Background())
	require.Equal(t, 2, len(got))
	require.Equal(t, <-got, <-got, "重试必须保持ID和不可变载荷一致")
}

func TestGatewayPoolCooldownReportStopsOnPermanentFailureAndBacksOffTemporary(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(status)
			}))
			defer server.Close()
			account := gwpoolTestAccount(1)
			account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
			account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
			svc, repo := gatewayRuntimeService(account)
			svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
				Gateway: "g", AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
			})
			svc.flushGatewayPoolReports(context.Background())
			svc.flushGatewayPoolReports(context.Background())
			require.Equal(t, 1, calls, "永久失败停发；临时失败退避内也不得立即重发")
			current, _ := repo.GetByID(context.Background(), 1)
			box := readGatewayPoolOutbox(current, time.Now())
			require.Len(t, box.Pending, 1)
			require.Equal(t, status == http.StatusConflict, box.Pending[0].Permanent)
			require.True(t, box.Pending[0].NextAt.After(time.Now()))
		})
	}
}

func TestGatewayPoolCooldownReportIsAnonymousAndUsesExistingObservation(t *testing.T) {
	got := make(chan gwpool.CooldownReport, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/cooldown/report", r.URL.Path)
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NotContains(t, string(raw), "acc-a")
		require.NotContains(t, string(raw), "test-consumer")
		require.NotContains(t, string(raw), "cookie")
		var report gwpool.CooldownReport
		require.NoError(t, json.Unmarshal(raw, &report))
		got <- report
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-consumer"
	svc, _ := gatewayRuntimeService(account)
	svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
		Gateway: "unified-142", AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3601, Full: true,
	})
	svc.flushGatewayPoolReports(context.Background())
	select {
	case report := <-got:
		require.Len(t, report.ID, 64)
		require.Len(t, report.AccountTag, 64)
		require.Equal(t, "full", report.Result)
	case <-time.After(time.Second):
		t.Fatal("没有上报已有观察")
	}
	require.Equal(t, gatewayPoolAccountTag(account, "chatgpt:acc-a:user:1"),
		gatewayPoolAccountTag(account, "chatgpt:acc-a:user:2"))
	require.NotEqual(t, gatewayPoolAccountTag(account, "chatgpt:acc-a"), gatewayPoolAccountTag(account, "chatgpt:acc-b"))
	require.NotContains(t, gatewayPoolAccountTag(account, gwpoolTestIdentity), "acc-a")
}

func TestGatewayPoolRecommendationListActuallyChangesBareTakeExclusion(t *testing.T) {
	tagSeen := false
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gateways":
			tagSeen = len(r.Header.Get("X-Gwpool-Account-Tag")) == 64
			_, _ = io.WriteString(w, `{"gateways":[{"name":"unified-142","pair_ready":true,
				"cooldown":{"seconds":7200,"samples":2,"source":"account"}}]}`)
		case "/cookie":
			query = r.URL.RawQuery
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"all_cooling"}}`)
		}
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolUseRecommendationKey] = true
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-consumer"
	store := &openAICodexCookieStore{}
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "unified-142"), time.Now().Add(-90*time.Minute))
	_, err := store.AttachRoute(context.Background(), account, gwpoolTestURL, http.Header{})
	require.Error(t, err)
	require.True(t, tagSeen)
	require.True(t, strings.Contains(query, "exclude=unified-142"), "1h 默认已过，但2h推荐应真正进入排除")
	require.NotContains(t, query, "gateway=unified-142", "不得强行点名仍在推荐冷却里的网关")
}
