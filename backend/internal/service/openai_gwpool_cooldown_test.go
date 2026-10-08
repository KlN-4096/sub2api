package service

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

func TestGatewayPoolCooldownBackoffResetAndLock(t *testing.T) {
	now := time.Date(2026, 10, 3, 1, 0, 0, 0, time.UTC)
	base := 3600
	var c gatewayPoolCooldown
	require.True(t, c.begin(now, time.Time{}, base))
	sample, _ := c.observe(now, openAIGatewayVerdictDegraded, base)
	require.Nil(t, sample, "第一次接触失败没有冷却样本，不能直接升到 2 小时")
	require.Equal(t, 3600, c.WindowSeconds)
	for _, want := range []int{7200, 14400, 21600, 28800, 36000, 43200, 57600, 72000, 86400, 86400} {
		now = c.Until
		require.True(t, c.begin(now, time.Time{}, base))
		sample, changed := c.observe(now, openAIGatewayVerdictDegraded, base)
		require.True(t, changed)
		require.NotNil(t, sample)
		require.False(t, sample.Full)
		require.Equal(t, want, c.WindowSeconds)
		_, changed = c.observe(now.Add(time.Second), openAIGatewayVerdictDegraded, base)
		require.False(t, changed, "同一尝试重复回报不能连跳档位")
	}

	// 在两个独立周期都等满 2 小时后成功，第二次才固定。
	c = gatewayPoolCooldown{WindowSeconds: 7200, Until: now.Add(2 * time.Hour)}
	for cycle := 1; cycle <= 2; cycle++ {
		now = c.Until
		require.True(t, c.begin(now, time.Time{}, base))
		sample, changed := c.observe(now, openAIGatewayVerdictFull, base)
		require.True(t, changed)
		require.Equal(t, 7200, sample.SuccessSeconds)
		_, changed = c.observe(now.Add(time.Minute), openAIGatewayVerdictFull, base)
		require.False(t, changed, "同一满血窗口不能学两遍")
		if cycle == 1 {
			require.Zero(t, c.FixedSeconds)
			require.Equal(t, base, c.WindowSeconds)
			// 正常满血窗口结束，从基础档重开；测试直接模拟退避已升到 2h。
			c.observe(now.Add(3*time.Minute), openAIGatewayVerdictDegraded, base)
			c.WindowSeconds, c.Until = 7200, now.Add(2*time.Hour)
		}
	}
	require.Equal(t, 7200, c.FixedSeconds)
	require.Equal(t, 7200, c.WindowSeconds)
	// 固定后的正常窗口耗尽不解除；真正等了固定时长仍失败才解除。
	c.observe(now.Add(3*time.Minute), openAIGatewayVerdictDegraded, base)
	require.Equal(t, 7200, c.FixedSeconds)
	now = c.Until
	require.True(t, c.begin(now, time.Time{}, base))
	c.observe(now, openAIGatewayVerdictDegraded, base)
	require.Zero(t, c.FixedSeconds)
	require.Equal(t, 14400, c.WindowSeconds)
	require.Zero(t, c.Successes[7200])
}

func TestGatewayPoolCooldownRejectsInvalidPersistedState(t *testing.T) {
	now := time.Now().UTC()
	valid := gatewayPoolCooldown{WindowSeconds: 3600, UpdatedAt: now, Until: now.Add(time.Hour)}
	for name, mutate := range map[string]func(*gatewayPoolCooldown){
		"far future deadline": func(c *gatewayPoolCooldown) { c.Until = now.AddDate(100, 0, 0) },
		"future revision":     func(c *gatewayPoolCooldown) { c.UpdatedAt = now.Add(24 * time.Hour) },
		"invalid fixed":       func(c *gatewayPoolCooldown) { c.FixedSeconds = -1 },
		"unproved fixed":      func(c *gatewayPoolCooldown) { c.FixedSeconds = 7200 },
		"unknown step":        func(c *gatewayPoolCooldown) { c.WindowSeconds = 7199 },
		"future attempt":      func(c *gatewayPoolCooldown) { c.AttemptAt = now.Add(time.Hour) },
		"polluted counts":     func(c *gatewayPoolCooldown) { c.Successes = map[int]int{3600: 99} },
		"unknown verdict":     func(c *gatewayPoolCooldown) { c.Outcome = "network_error" },
		"unbounded elapsed":   func(c *gatewayPoolCooldown) { c.ElapsedSeconds = int(^uint(0) >> 1) },
		"unbounded success":   func(c *gatewayPoolCooldown) { c.LastSuccessSeconds = int(^uint(0) >> 1) },
		"stale pending attempt": func(c *gatewayPoolCooldown) {
			c.AttemptAt, c.AttemptSeconds, c.ElapsedSeconds = now.Add(-time.Hour), 3600, 3600
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := valid.clone()
			mutate(&c)
			store := &openAICodexCookieStore{}
			store.hydrateCooldown(gwpoolTestIdentity, "unified-142", &c, 3600)
			_, found := store.cooldownEntry(gwpoolTestIdentity, "unified-142")
			require.False(t, found, "坏运行态不能成为冷却或学习依据")
		})
	}
	pending := valid
	pending.AttemptAt, pending.AttemptSeconds, pending.ElapsedSeconds = now, 3600, 3600
	store := &openAICodexCookieStore{}
	store.hydrateCooldown(gwpoolTestIdentity, "unified-142", &pending, 3600)
	sample := store.observeGatewayPoolCooldown(gwpoolTestIdentity, "unified-142", openAIGatewayVerdictFull, time.Hour, time.Time{})
	require.Nil(t, sample, "恢复的是冷却，不是别的请求的在途学习分数")
	c, _ := store.cooldownEntry(gwpoolTestIdentity, "unified-142")
	require.Empty(t, c.Successes)
}

func TestGatewayPoolCooldownCannotBeBypassedByBareTakeOrConcurrentFetch(t *testing.T) {
	t.Run("legacy steering opt-out and pool ignores exclude", func(t *testing.T) {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
		account := fake.account(1)
		account.Extra["openai_gwpool_steering"] = false
		store := &openAICodexCookieStore{}
		store.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-142")
		headers := http.Header{}
		err := attachRoute(context.Background(), store, account, gwpoolTestURL, headers)
		require.ErrorIs(t, err, gwpool.ErrNoSlot)
		require.Empty(t, headers.Get("Cookie"))
	})

	t.Run("candidate becomes cooling while fetch is in flight", func(t *testing.T) {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-22"), 150)
		fake.listGateways = []gwpoolFakeGateway{{Name: "unified-22", PairReady: true}}
		account := fake.account(1)
		store := &openAICodexCookieStore{}
		fake.beforeCookie = func() { store.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-22") }
		headers := http.Header{}
		err := attachRoute(context.Background(), store, account, gwpoolTestURL, headers)
		require.ErrorIs(t, err, gwpool.ErrNoSlot)
		require.Empty(t, headers.Get("Cookie"))
		require.EqualValues(t, 1, fake.hits.Load())
		require.Zero(t, fake.releaseHits.Load())
	})

}

func TestGatewayPoolCooldownDoesNotLearnFromErrorsOrSparseTraffic(t *testing.T) {
	now := time.Now().UTC()
	c := gatewayPoolCooldown{WindowSeconds: 3600, Until: now}
	require.True(t, c.begin(now.Add(2*time.Hour), time.Time{}, 3600))
	before := c.clone()
	sample, changed := c.observe(now, "", 3600)
	require.False(t, changed)
	require.Nil(t, sample)
	require.Equal(t, before, c)
	sample, _ = c.observe(now.Add(2*time.Hour), openAIGatewayVerdictFull, 3600)
	require.Zero(t, sample.SuccessSeconds, "实际静置 3h 不能冒充 1h 或未验证的 4h 档参与固定")
	require.Equal(t, 10800, c.LastSuccessSeconds)

	require.Zero(t, successfulGatewayPoolCooldown(3600, 86400))
	require.Equal(t, 3600, successfulGatewayPoolCooldown(3600, 3601))
}

func TestGatewayPoolCooldownPersistsAndScopesByUpstreamMember(t *testing.T) {
	store := &openAICodexCookieStore{}
	identity := "gwpool-member:acct-a/one"
	require.True(t, store.beginGatewayPoolAttempt(identity, "unified-142", time.Hour))
	store.observeGatewayPoolCooldown(identity, "unified-142", openAIGatewayVerdictDegraded, time.Hour, time.Time{})
	require.False(t, store.beginGatewayPoolAttempt(identity, "unified-142", time.Hour))
	require.True(t, store.beginGatewayPoolAttempt("gwpool-member:acct-a/two", "unified-142", time.Hour))
	require.True(t, store.beginGatewayPoolAttempt("gwpool-member:acct-b/one", "unified-142", time.Hour))
	require.True(t, store.beginGatewayPoolAttempt(identity, "unified-143", time.Hour))
	c, ok := store.cooldownEntry(identity, "unified-142")
	require.True(t, ok)
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	var restored gatewayPoolCooldown
	require.NoError(t, json.Unmarshal(raw, &restored))
	restarted := &openAICodexCookieStore{}
	restarted.hydrateCooldown(identity, "unified-142", &restored, 3600)
	require.False(t, restarted.beginGatewayPoolAttempt(identity, "unified-142", time.Hour))
	stale := restored
	stale.UpdatedAt = stale.UpdatedAt.Add(-time.Hour)
	stale.Until = time.Time{}
	restarted.hydrateCooldown(identity, "unified-142", &stale, 3600)
	got, _ := restarted.cooldownEntry(identity, "unified-142")
	require.Equal(t, restored.Until, got.Until)
}
