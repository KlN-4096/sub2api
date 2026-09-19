//go:build unit

package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recoveryConfig 造一份恢复探测配置：默认区间 30–90 分钟、连胜 5、冷却 16 小时。
func recoveryConfig(overrides map[string]any) map[string]any {
	cfg := map[string]any{"enabled": true, "model": hunterTestModel}
	for k, v := range overrides {
		cfg[k] = v
	}
	return cfg
}

// recoveryAccount 造一个只开恢复探测、**不开猎手**的账号：这个功能是独立开关。
func recoveryAccount(recovery map[string]any) *Account {
	a := hunterTestAccount(map[string]any{"enabled": false})
	a.Extra[openAITurnStateRecoveryExtraKey] = recovery
	proxy := hunterCoxProxy
	a.ProxyID, a.Proxy = &proxy.ID, &proxy
	return a
}

func recoveryState(a *Account) openAITurnStateRecoveryState {
	return readOpenAITurnStateRecoveryState(a)
}

// setRecoveryState 按 JSONB 读回来的形态写运行态（time.Time 变字符串），与生产落库一致。
func setRecoveryState(a *Account, st openAITurnStateRecoveryState) {
	encoded, err := json.Marshal(st)
	if err != nil {
		panic(err)
	}
	var generic map[string]any
	if err := json.Unmarshal(encoded, &generic); err != nil {
		panic(err)
	}
	a.Extra[openAITurnStateRecoveryStateExtraKey] = generic
}

// queueBlobs 往替身上游排一串响应：healthy=true 就是 292，false 就是 312。
func queueBlobs(h *hunterHarness, minted time.Time, healthy ...bool) {
	for _, ok := range healthy {
		blocks := openAIHealthyTurnStateBlocks
		if !ok {
			blocks++
		}
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(minted, blocks), "")
		h.up.queue = append(h.up.queue, resp)
	}
}

// TestOpenAITurnStateRecoveryMarksAfterStreak 钉住主判据：连续 5 次 292 → 标记已恢复；
// 猎手关着也照跑；每次只探一次、间隔落在配置区间内；标记之后不再探。
func TestOpenAITurnStateRecoveryMarksAfterStreak(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	clock := now
	h.svc.now = func() time.Time { return clock }
	queueBlobs(h, now, true, true, true, true, true, true)

	for i := 1; i <= defaultOpenAITurnStateRecoveryStreak; i++ {
		h.run(t)
		st := recoveryState(h.account)
		require.Len(t, h.up.requests, i, "一个 tick 只探一次")
		require.Equal(t, i, st.Streak)
		require.Zero(t, st.FailStreak)
		if i < defaultOpenAITurnStateRecoveryStreak {
			require.True(t, st.RecoveredAt.IsZero(), "没攒够不许判定恢复")
			wait := st.NextAt.Sub(clock)
			require.GreaterOrEqual(t, wait, time.Duration(defaultOpenAITurnStateRecoveryMinMinutes)*time.Minute)
			require.LessOrEqual(t, wait, time.Duration(defaultOpenAITurnStateRecoveryMaxMinutes)*time.Minute)
			h.run(t)
			require.Len(t, h.up.requests, i, "没到点不探")
			clock = st.NextAt
		}
	}

	st := recoveryState(h.account)
	require.False(t, st.RecoveredAt.IsZero(), "连续 5 次 292 = 降智恢复")
	require.Equal(t, defaultOpenAITurnStateRecoveryStreak, st.Streak)

	// 判定之后不再探：结论已经有了，继续探只是白付额度。
	clock = clock.Add(24 * time.Hour)
	h.run(t)
	require.Len(t, h.up.requests, defaultOpenAITurnStateRecoveryStreak)
}

// TestOpenAITurnStateRecoveryStreakResetsOn312 钉住：中间任意一次失败把连胜清零，重新数。
func TestOpenAITurnStateRecoveryStreakResetsOn312(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	clock := now
	h.svc.now = func() time.Time { return clock }
	queueBlobs(h, now, true, true, false, true)

	advance := func() {
		h.run(t)
		clock = recoveryState(h.account).NextAt
	}
	advance()
	advance()
	require.Equal(t, 2, recoveryState(h.account).Streak)

	advance() // 312
	st := recoveryState(h.account)
	require.Zero(t, st.Streak, "一次 312 就从头数")
	require.Equal(t, 1, st.FailStreak)
	require.True(t, st.RecoveredAt.IsZero())

	advance() // 292
	st = recoveryState(h.account)
	require.Equal(t, 1, st.Streak)
	require.Zero(t, st.FailStreak, "一次成功清掉失败计数")
}

// TestOpenAITurnStateRecoveryCooldownAfterFailures 钉住用户要的 CD：连续 5 次失败 → 冷却 16 小时，
// 冷却期内不探；醒来后失败计数从零开始。传输错误与 312 同样算失败。
func TestOpenAITurnStateRecoveryCooldownAfterFailures(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	clock := now
	h.svc.now = func() time.Time { return clock }
	queueBlobs(h, now, false, false, false, false)
	h.up.queue = append(h.up.queue, nil) // 第五次：传输错误
	h.up.errOnNil = errors.New("dial tcp: proxy refused")

	lastProbeAt := clock
	for i := 1; i <= defaultOpenAITurnStateRecoveryStreak; i++ {
		lastProbeAt = clock
		h.run(t)
		require.Len(t, h.up.requests, i)
		clock = recoveryState(h.account).NextAt
	}

	st := recoveryState(h.account)
	require.Zero(t, st.FailStreak, "进冷却时计数归零，醒来不能一探就又满")
	require.True(t, st.RecoveredAt.IsZero())
	require.Equal(t, lastProbeAt.Add(defaultOpenAITurnStateRecoveryCooldownHours*time.Hour), st.CoolingUntil,
		"连续失败够数 → 从最后一次探测起冷却 16 小时")
	require.Equal(t, st.CoolingUntil, st.NextAt)

	// 冷却期内不探。
	clock = st.CoolingUntil.Add(-time.Minute)
	h.run(t)
	require.Len(t, h.up.requests, defaultOpenAITurnStateRecoveryStreak)

	// 醒来接着探。
	queueBlobs(h, now, true)
	h.up.errOnNil = nil
	clock = st.CoolingUntil
	h.run(t)
	require.Len(t, h.up.requests, defaultOpenAITurnStateRecoveryStreak+1)
	require.Equal(t, 1, recoveryState(h.account).Streak)
}

// TestOpenAITurnStateRecoveryUsesAccountExit 钉住「走账号自己的出口」：不是猎手代理、不关连接
// （关了会把真实流量正在用的那条隧道带走），铸出的 292 照常入池。
func TestOpenAITurnStateRecoveryUsesAccountExit(t *testing.T) {
	now := time.Now().UTC()
	account := recoveryAccount(recoveryConfig(nil))
	account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"proxy_ids": []any{float64(20)}})
	h := newHunterHarness(account, hunterWebshareProxy, hunterCoxProxy)
	h.svc.now = func() time.Time { return now }
	queueBlobs(h, now, true)

	h.run(t)

	require.NotEmpty(t, h.up.requests)
	require.Contains(t, h.up.proxyURLs[0], hunterCoxProxy.Host, "走账号绑定的出口，不是猎手代理")
	require.NotContains(t, h.up.proxyURLs[0], hunterWebshareProxy.Host)
	require.False(t, h.up.requests[0].Close, "复用连接：关连接会踢掉真实流量的 HTTP/2 隧道")
	require.Empty(t, h.prober.calls, "自己的出口不需要回声")
	require.Equal(t, hunterCoxProxy.ID, recoveryState(h.account).Last[0].ProxyID)
	require.Len(t, readOpenAITurnStatePool(h.account), 1, "同出口铸的 292 照常入池")
}

// TestOpenAITurnStateRecoveryDirectAccount 钉住没绑代理的账号也能探（直连，空代理 URL）。
func TestOpenAITurnStateRecoveryDirectAccount(t *testing.T) {
	now := time.Now().UTC()
	account := recoveryAccount(recoveryConfig(nil))
	account.ProxyID, account.Proxy = nil, nil
	h := newHunterHarness(account)
	h.svc.now = func() time.Time { return now }
	queueBlobs(h, now, true)

	h.run(t)

	require.Len(t, h.up.requests, 1)
	require.Empty(t, h.up.proxyURLs[0], "直连")
	require.Equal(t, 1, recoveryState(h.account).Streak)
}

// TestOpenAITurnStateRecoveryResetOnNatural312 钉住标记的失效：真实流量又自然铸出 312 →
// 清掉「已恢复」与连胜，从头攒；注入回声（带票的请求）不算。
func TestOpenAITurnStateRecoveryResetOnNatural312(t *testing.T) {
	now := time.Now().UTC()
	repo := newTurnStateAutoRepo()
	account := recoveryAccount(recoveryConfig(nil))
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo}
	st := openAITurnStateRecoveryState{Streak: 5, RecoveredAt: now}
	setRecoveryState(account, st)

	// 注入过的响应不算数：92% 的带票请求上游原样回带，拿它判定就是拿自己的票当证据。
	c := turnStateAutoCtxModel("echo", hunterTestModel)
	c.Set(ctxKeyTurnStateInjected, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks))
	gw.observeOpenAITurnStateMint(c, account, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	require.False(t, recoveryState(account).RecoveredAt.IsZero(), "回声不清标记")

	// 自然铸造的 312 才算。
	gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("natural", hunterTestModel), account,
		turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	got := recoveryState(account)
	require.True(t, got.RecoveredAt.IsZero(), "又铸 312 = 之前判定的恢复不作数")
	require.Zero(t, got.Streak)

	// 标记已经清掉之后不再重复写库（响应热路径）。
	before := repo.extraWrites
	gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("natural-2", hunterTestModel), account,
		turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	require.Len(t, repo.extraWrites, len(before), "标记不在时不写库")
}

// TestOpenAITurnStateRecoverySkips 钉住不该探的场景。
func TestOpenAITurnStateRecoverySkips(t *testing.T) {
	now := time.Now().UTC()
	cases := map[string]func(*Account){
		"关着": func(a *Account) {
			a.Extra[openAITurnStateRecoveryExtraKey] = recoveryConfig(map[string]any{"enabled": false})
		},
		"没配置":         func(a *Account) { delete(a.Extra, openAITurnStateRecoveryExtraKey) },
		"账号不是 active": func(a *Account) { a.Status = StatusError },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			account := recoveryAccount(recoveryConfig(nil))
			mutate(account)
			h := newHunterHarness(account, hunterCoxProxy)
			h.svc.now = func() time.Time { return now }
			queueBlobs(h, now, true)

			h.run(t)

			require.Empty(t, h.up.requests)
		})
	}
}

// TestOpenAITurnStateRecoveryConfigBounds 钉住配置兜底：区间填反按固定间隔走，非法值取默认。
func TestOpenAITurnStateRecoveryConfigBounds(t *testing.T) {
	account := recoveryAccount(recoveryConfig(map[string]any{
		"min_minutes": float64(90), "max_minutes": float64(30),
		"streak_target": float64(0), "cooldown_hours": float64(0), "reasoning_effort": "bogus",
	}))
	cfg, ok := readOpenAITurnStateRecoveryConfig(account)
	require.True(t, ok)
	require.Equal(t, 90, cfg.MinMinutes)
	require.Equal(t, 90, cfg.MaxMinutes, "填反了按固定间隔走，别让区间变负数")
	require.Equal(t, 90*time.Minute, cfg.interval())
	require.Equal(t, defaultOpenAITurnStateRecoveryStreak, cfg.StreakTarget)
	require.Equal(t, defaultOpenAITurnStateRecoveryCooldownHours*time.Hour, cfg.cooldown())
	require.Equal(t, defaultOpenAITurnStateHuntReasoningEffort, cfg.ReasoningEffort)

	capped := recoveryAccount(recoveryConfig(map[string]any{
		"min_minutes": float64(99999), "max_minutes": float64(99999), "cooldown_hours": float64(99999),
	}))
	cfg, _ = readOpenAITurnStateRecoveryConfig(capped)
	require.Equal(t, openAITurnStateRecoveryMaxMinutes, cfg.MaxMinutes)
	require.Equal(t, openAITurnStateRecoveryMaxCooldownHours, cfg.CooldownHours)
}

// TestValidateOpenAITurnStateRecoveryExtra 钉住管理员配置的校验（随猎手配置一起在 handler 入口调）。
func TestValidateOpenAITurnStateRecoveryExtra(t *testing.T) {
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateRecoveryExtraKey: recoveryConfig(nil)}))

	nulled := map[string]any{openAITurnStateRecoveryExtraKey: nil}
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(nulled))
	require.NotContains(t, nulled, openAITurnStateRecoveryExtraKey, "null 等价于未配置")

	for _, bad := range []map[string]any{
		{openAITurnStateRecoveryExtraKey: "on"},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"enabled": "yes"})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"model": float64(1)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"streak_target": float64(0)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"cooldown_hours": float64(1.5)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"reasoning_effort": "bogus"})},
	} {
		require.Error(t, ValidateOpenAITurnStateHunterExtra(bad))
	}
}

// TestOpenAITurnStateRecoveryNotResetByHunterOrEcho 钉住第一轮评审 B1：清标记只认真实流量自己
// 铸出的 312。猎手走的是**别的出口**，恢复探测的失败自己记；带票请求 92% 是上游原样回带——
// 三者都清一次连胜的话，降智账号永远攒不满。
func TestOpenAITurnStateRecoveryNotResetByHunterOrEcho(t *testing.T) {
	now := time.Now().UTC()
	degraded := turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1)

	t.Run("猎手探测的 312 不清连胜", func(t *testing.T) {
		h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy, hunterWebshareProxy)
		// 猎手猎另一个模型：否则恢复探测铸出的 292 入池后，猎手看到票新鲜就不再探了。
		h.account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{
			"proxy_ids": []any{float64(20)}, "models": []any{"gpt-5.6-luna"},
		})
		h.svc.now = func() time.Time { return now }
		queueBlobs(h, now, true)                           // 恢复探测：292
		miss, _ := hunterResp(http.StatusOK, degraded, "") // 猎手：312
		h.up.queue = append(h.up.queue, miss)

		h.run(t)

		require.GreaterOrEqual(t, len(h.up.requests), 2, "同一个 tick 里两种探测都跑了")
		require.Equal(t, 1, recoveryState(h.account).Streak, "猎手在别的出口上撞 312，不是账号自己出口的证据")
	})

	t.Run("回带的 312 不清标记", func(t *testing.T) {
		repo := newTurnStateAutoRepo()
		account := recoveryAccount(recoveryConfig(nil))
		repo.latest = account
		gw := &OpenAIGatewayService{accountRepo: repo}
		setRecoveryState(account, openAITurnStateRecoveryState{Streak: 4})

		c := turnStateAutoCtxModel("carried", hunterTestModel)
		markOpenAITurnStateSent(c, account, degraded) // 出站带了票（客户端自带透传，非我们注入）
		gw.observeOpenAITurnStateMint(c, account, degraded)

		require.Equal(t, 4, recoveryState(account).Streak, "回声不是证据")
	})

	t.Run("裸请求的 312 才清", func(t *testing.T) {
		repo := newTurnStateAutoRepo()
		account := recoveryAccount(recoveryConfig(nil))
		repo.latest = account
		gw := &OpenAIGatewayService{accountRepo: repo}
		setRecoveryState(account, openAITurnStateRecoveryState{Streak: 4})

		gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("bare", hunterTestModel), account, degraded)

		require.Zero(t, recoveryState(account).Streak)
	})
}

// TestOpenAITurnStateRecoveryStateOmitsZeroTimes 钉住第一轮评审 B2：零值时间不能落库成
// 0001-01-01（JS 的 Date 认这个字符串，页面会从第一次探测起就写「已恢复」）。
func TestOpenAITurnStateRecoveryStateOmitsZeroTimes(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	h.svc.now = func() time.Time { return now }
	queueBlobs(h, now, true)

	h.run(t)

	raw, ok := h.account.Extra[openAITurnStateRecoveryStateExtraKey].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, raw, "recovered_at", "没判定恢复就不该有这个键")
	require.NotContains(t, raw, "cooling_until")
	require.Contains(t, raw, "next_at")
}

// TestOpenAITurnStateRecoveryModelFallback 钉住选模型：窗口内**最近**一次流量的模型（不是字母序
// 第一个），画图模型一律排除（它们结构上只铸 312，探它必然连败进冷却）。
func TestOpenAITurnStateRecoveryModelFallback(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(map[string]any{"model": ""})), hunterCoxProxy)
	h.svc.now = func() time.Time { return now }
	// 两个模型都铸过票，luna 字母序在前、astra 是最近的一次。水位时间必须岔开：
	// 选最近用的是 `mark.at.After(best.at)`，同一时刻的两条谁赢由 sync.Map.Range 的
	// 顺序定，写成同一个 now 的话这条断言是掷骰子。
	for i, m := range []string{"gpt-5.6-luna", "gpt-6-astra"} {
		c := turnStateAutoCtxModel("seed-"+m, m)
		h.gw.observeOpenAITurnStateMint(c, h.account, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks))
		h.gw.noteOpenAITurnStateTraffic(h.account.ID, m, now.Add(time.Duration(i)*time.Second))
	}
	queueBlobs(h, now, true)

	h.run(t)

	require.Len(t, h.up.requests, 1)
	require.Equal(t, "gpt-6-astra", recoveryState(h.account).Last[0].Model, "取最近有流量的，不是字母序第一个")

	// 观测兜底也排除画图模型：没有流量记录时，最近一次观测来自 gpt-image-2 就什么都不探。
	img := newHunterHarness(recoveryAccount(recoveryConfig(map[string]any{"model": ""})), hunterCoxProxy)
	img.svc.now = func() time.Time { return now }
	img.account.Extra[openAITurnStateObservedExtraKey] = map[string]any{
		"model": "gpt-image-2", "blocks": openAIHealthyTurnStateBlocks + 1, "chars": 312,
		"minted_at": now.Format(time.RFC3339), "observed_at": now.Format(time.RFC3339),
	}
	queueBlobs(img, now, true)
	img.run(t)
	require.Empty(t, img.up.requests, "画图模型只铸 312，探它等于每轮必然连败")
	require.Equal(t, "no model to probe", recoveryState(img.account).LastError)
	require.Equal(t, 1, recoveryState(img.account).FailStreak, "配不出模型也记失败，免得每窗白写一次库")
}

// TestOpenAITurnStateRecoveryOneAccountPerTick 钉住第一轮评审 S4：恢复探测一个 tick 只做一个账号，
// 不能几十个账号一起在收集阶段各阻塞 60 秒、把猎手的 15 分钟预算吃光。
func TestOpenAITurnStateRecoveryOneAccountPerTick(t *testing.T) {
	now := time.Now().UTC()
	first := recoveryAccount(recoveryConfig(nil))
	second := recoveryAccount(recoveryConfig(nil))
	second.ID = 9202
	second.Credentials = map[string]any{"access_token": "second-token", "chatgpt_account_id": "second-account"}
	h := newHunterHarness(first, hunterCoxProxy)
	h.repo.others = []*Account{second}
	h.svc.now = func() time.Time { return now }
	queueBlobs(h, now, true, true)

	h.run(t)
	require.Len(t, h.up.requests, 1, "一个 tick 只探一个账号")

	h.run(t)
	require.Len(t, h.up.requests, 2, "下个 tick 轮到另一个账号")
	require.Equal(t, "Bearer second-token", h.up.requests[1].Header.Get("Authorization"))
}

// TestOpenAITurnStateRecoveryIntervalIsRandom 钉住「间隔不固定」：同一份配置多次取值不能恒等。
func TestOpenAITurnStateRecoveryIntervalIsRandom(t *testing.T) {
	cfg := openAITurnStateRecoveryConfig{MinMinutes: 30, MaxMinutes: 90}
	seen := map[time.Duration]bool{}
	for range 30 {
		d := cfg.interval()
		require.GreaterOrEqual(t, d, 30*time.Minute)
		require.LessOrEqual(t, d, 90*time.Minute)
		seen[d] = true
	}
	require.Greater(t, len(seen), 1, "间隔要随机，不能每次都一样")
}
