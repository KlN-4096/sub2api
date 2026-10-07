//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func holdHunterConfig(overrides map[string]any) map[string]any {
	cfg := hunterConfig(map[string]any{"hold_when_degraded": true})
	for k, v := range overrides {
		cfg[k] = v
	}
	return cfg
}

// markHeld 把 hunterTestModel 标成因缺票停着（到 until）。
func markHeld(account *Account, until time.Time) {
	setAccountModelRateLimitSnapshot(account, hunterTestModel, until, openAITurnStateHoldLimitReason, time.Now())
}

// TestOpenAITurnStateHoldBlocksUnfilledHuntedModel 钉住注入点：猎手管的模型拿不出票 →
// 该模型停一个空闲窗口（model_rate_limits，reason 标本功能）+ 换号错误；换到下一个账号时
// 拦截标记要清掉；停着期间再来的请求只换号不再写库；到期后再来的请求重新拉起。
func TestOpenAITurnStateHoldBlocksUnfilledHuntedModel(t *testing.T) {
	repo := newTurnStateAutoRepo()
	account := hunterTestAccount(holdHunterConfig(map[string]any{"idle_minutes": 30}))
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo}

	c := turnStateAutoCtxModel("real", hunterTestModel)
	gw.applyOpenAICodexTurnStateOverrideHeader(c, account, http.Header{})

	var failover *UpstreamFailoverError
	require.ErrorAs(t, openAITurnStateHoldError(c), &failover)
	require.Equal(t, OpenAITurnStateHoldReason, failover.Reason)
	require.True(t, failover.ShouldRetryNextAccount(), "本账号排除、下一个账号继续")
	require.Equal(t, http.StatusServiceUnavailable, failover.ClientStatusCode)
	require.Contains(t, failover.ClientMessage, hunterTestModel)
	require.Equal(t, []string{hunterTestModel}, repo.holds)
	resetAt, ok := openAITurnStateHoldResetAt(account, hunterTestModel)
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(30*time.Minute), resetAt, time.Minute, "停一个空闲窗口，不是 24 小时")
	require.True(t, openAITurnStateModelHeld(account, hunterTestModel, time.Now()))
	require.Nil(t, account.TempUnschedulableUntil, "停的是模型不是账号")
	require.True(t, gw.openAITurnStateTrafficSince(account.ID, hunterTestModel, time.Now().Add(-time.Minute)),
		"被拦下的请求也是真实流量：不记水位的话空闲门槛会把猎手刹住，暂停就永远解不开")

	// 停着期间再来一条：只换号，不再写库。
	c2 := turnStateAutoCtxModel("real-2", hunterTestModel)
	gw.applyOpenAICodexTurnStateOverrideHeader(c2, account, http.Header{})
	require.Error(t, openAITurnStateHoldError(c2))
	require.Len(t, repo.holds, 1)

	// 同一个 gin 上下文换到下一个账号（没开暂停）：上一轮的拦截标记必须先清。
	other := hunterTestAccount(hunterConfig(nil))
	other.ID = 9202
	gw.applyOpenAICodexTurnStateOverrideHeader(c, other, http.Header{})
	require.NoError(t, openAITurnStateHoldError(c))

	// 到期后再来一条：还缺票就再停一次（有人用就一直处于「停着 → 到期 → 再停」）。
	markHeld(account, time.Now().Add(-time.Second))
	c3 := turnStateAutoCtxModel("real-3", hunterTestModel)
	gw.applyOpenAICodexTurnStateOverrideHeader(c3, account, http.Header{})
	require.Error(t, openAITurnStateHoldError(c3))
	require.Len(t, repo.holds, 2)
	require.True(t, openAITurnStateModelHeld(account, hunterTestModel, time.Now()))
}

// TestOpenAITurnStateHoldIsModelScoped 钉住这次改法的核心：sol 缺票只停 sol，调度器对 astra
// 照常把这个账号算在内；账号级的可调度性不动。
func TestOpenAITurnStateHoldIsModelScoped(t *testing.T) {
	repo := newTurnStateAutoRepo()
	account := hunterTestAccount(holdHunterConfig(map[string]any{"models": []any{"gpt-5.6-sol", hunterTestModel}}))
	account.Schedulable = true
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo}
	require.True(t, account.IsSchedulable())

	gw.applyOpenAICodexTurnStateOverrideHeader(turnStateAutoCtxModel("sol", "gpt-5.6-sol"), account, http.Header{})
	require.Equal(t, []string{"gpt-5.6-sol"}, repo.holds)

	ctx := context.Background()
	require.False(t, account.IsSchedulableForModelWithContext(ctx, "gpt-5.6-sol"), "sol 在本账号上停着")
	require.True(t, account.IsSchedulableForModelWithContext(ctx, hunterTestModel), "astra 不连坐")
	require.True(t, account.IsSchedulable(), "账号本身没停")
	require.Equal(t, []string{"gpt-5.6-sol"}, openAITurnStateHeldModels(account, time.Now()))
}

// TestOpenAITurnStateHoldSkips 列出不该拦的情形：一条都不能写库。
func TestOpenAITurnStateHoldSkips(t *testing.T) {
	now := time.Now().UTC()
	realCtx := func() *gin.Context { return turnStateAutoCtxModel("real", hunterTestModel) }
	cases := []struct {
		name    string
		account func() *Account
		ctx     func() *gin.Context
	}{
		{"暂停开关关着", func() *Account { return hunterTestAccount(hunterConfig(nil)) }, realCtx},
		{"猎手不管这个模型", func() *Account {
			return hunterTestAccount(holdHunterConfig(map[string]any{"models": []any{"gpt-6-other"}}))
		}, realCtx},
		{"猎手关着", func() *Account { return hunterTestAccount(holdHunterConfig(map[string]any{"enabled": false})) }, realCtx},
		{"接管关着", func() *Account {
			a := hunterTestAccount(holdHunterConfig(nil))
			a.Extra[openAITurnStateAutoExtraKey] = false
			return a
		}, realCtx},
		{"探测上下文", func() *Account { return hunterTestAccount(holdHunterConfig(nil)) }, func() *gin.Context {
			c := realCtx()
			c.Set(ctxKeyTurnStateProbe, true)
			return c
		}},
		{"池里有票", func() *Account {
			a := hunterTestAccount(holdHunterConfig(nil))
			a.Extra[openAITurnStatePoolExtraKey] = []any{map[string]any{
				"blob": turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "model": hunterTestModel, "minted_at": now,
			}}
			return a
		}, realCtx},
		{"客户端回带本账号新鲜的 292", func() *Account { return hunterTestAccount(holdHunterConfig(nil)) }, func() *gin.Context {
			c := realCtx()
			c.Request.Header.Set(openAICodexTurnStateHeader, turnStateFernetBlob(now.Add(-10*time.Minute), openAIHealthyTurnStateBlocks))
			return c
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newTurnStateAutoRepo()
			account := tc.account()
			repo.latest = account
			gw := &OpenAIGatewayService{accountRepo: repo}
			gw.applyOpenAICodexTurnStateOverrideHeader(tc.ctx(), account, http.Header{})
			require.NoError(t, openAITurnStateHoldError(tc.ctx()))
			require.Empty(t, repo.holds)
			require.False(t, openAITurnStateModelHeld(account, hunterTestModel, time.Now()))
		})
	}
}

// TestOpenAITurnStateHoldIgnoresStaleOrForeignEcho 钉住回带判定的两道闸：过期的回带和
// 异账号铸的回带都不能当放行依据。
func TestOpenAITurnStateHoldIgnoresStaleOrForeignEcho(t *testing.T) {
	now := time.Now().UTC()
	t.Run("过期回带", func(t *testing.T) {
		repo := newTurnStateAutoRepo()
		account := hunterTestAccount(holdHunterConfig(nil))
		repo.latest = account
		gw := &OpenAIGatewayService{accountRepo: repo}
		c := turnStateAutoCtxModel("real", hunterTestModel)
		c.Request.Header.Set(openAICodexTurnStateHeader, turnStateFernetBlob(now.Add(-2*time.Hour), openAIHealthyTurnStateBlocks))
		gw.applyOpenAICodexTurnStateOverrideHeader(c, account, http.Header{})
		require.Error(t, openAITurnStateHoldError(c))
		require.Len(t, repo.holds, 1)
	})
	t.Run("异账号铸的回带", func(t *testing.T) {
		repo := newTurnStateAutoRepo()
		account := hunterTestAccount(holdHunterConfig(nil))
		repo.latest = account
		gw := &OpenAIGatewayService{accountRepo: repo}
		blob := turnStateFernetBlob(now, openAIHealthyTurnStateBlocks)
		other := hunterTestAccount(hunterConfig(nil))
		other.ID = 9202
		other.Credentials = map[string]any{"access_token": "other-token", "chatgpt_account_id": "other-workspace"}
		gw.noteOpenAICodexTurnStateOrigin(turnStateAutoCtxModel("prev", hunterTestModel), other, blob)
		c := turnStateAutoCtxModel("real", hunterTestModel)
		c.Request.Header.Set(openAICodexTurnStateHeader, blob)
		gw.applyOpenAICodexTurnStateOverrideHeader(c, account, http.Header{})
		require.Error(t, openAITurnStateHoldError(c))
		require.Len(t, repo.holds, 1)
	})
}

// TestOpenAITurnStateHoldReleasedWhenHunterHits 钉住主流程：停着的模型猎到 292 入池，同一
// tick 内放回，不等到期。
func TestOpenAITurnStateHoldReleasedWhenHunterHits(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	markHeld(h.account, now.Add(30*time.Minute))
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}

	h.run(t)

	require.Len(t, h.up.requests, 1, "停着的模型照样猎")
	require.Equal(t, 1, h.repo.clears)
	require.False(t, openAITurnStateModelHeld(h.account, hunterTestModel, time.Now()))
}

// TestOpenAITurnStateHoldWrittenMidRoundReleasedOnHit 钉住「命中即放回」对轮次中才写的暂停也成立：
// 猎手手里的账号是 tick 开头的快照，票过期后第一条真实请求在本轮里写的暂停不在快照里，命中时
// 必须重读再放回，不能等整轮结束（第二轮评审 B1：多账号交错后整轮可能还有十几分钟）。
func TestOpenAITurnStateHoldWrittenMidRoundReleasedOnHit(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	degraded, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{degraded, healthy}
	held := false
	h.repo.onUpdateExtra = func() {
		if held {
			return
		}
		held = true // 第一次落库 = 第一次探测（312）之后：这时真实请求把模型停了
		markHeld(h.account, now.Add(30*time.Minute))
	}

	h.run(t)

	require.Len(t, h.up.requests, 2)
	require.Equal(t, 1, h.repo.clears, "命中当场放回，不等下个 tick")
	require.False(t, openAITurnStateModelHeld(h.account, hunterTestModel, time.Now()))
}

// TestOpenAITurnStateHoldReleasesOnlyHitAccount 钉住多账号交错下放回落在对的账号上：两个账号
// 各自停着同一个模型，只有命中的那个放回（第二轮评审 S4）。
func TestOpenAITurnStateHoldReleasesOnlyHitAccount(t *testing.T) {
	now := time.Now().UTC()
	first := hunterTestAccount(holdHunterConfig(nil)) // gap 1s
	second := hunterTestAccount(holdHunterConfig(map[string]any{"gap_seconds": 10}))
	second.ID = 9202
	second.Credentials = map[string]any{"access_token": "second-token", "chatgpt_account_id": "second-account"}
	h := newHunterHarness(first, hunterWebshareProxy)
	h.repo.others = []*Account{second}
	markHeld(first, now.Add(30*time.Minute))
	markHeld(second, now.Add(30*time.Minute))
	clock := now
	h.svc.now = func() time.Time { return clock }
	h.svc.sleep = func(_ context.Context, d time.Duration) error {
		clock = clock.Add(d)
		return nil
	}
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	degraded, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	// first 292 命中；second 312，之后队列空 = 传输错误，连续两次后本轮结束。
	h.up.queue = []*http.Response{healthy, degraded}

	h.run(t)

	require.Equal(t, "Bearer offline-token", h.up.requests[0].Header.Get("Authorization"))
	require.Equal(t, 1, h.repo.clears, "只放回命中的那个")
	require.False(t, openAITurnStateModelHeld(first, hunterTestModel, time.Now()))
	require.True(t, openAITurnStateModelHeld(second, hunterTestModel, time.Now()), "没命中的账号照旧停着")
}

// TestOpenAITurnStateHoldNotRenewedWhileUnfilled 钉住「到期靠请求再拉起」：没摇到票猎手既不
// 放回也不续期，到期前一直停着，到期后由下一条请求决定要不要再停。
func TestOpenAITurnStateHoldNotRenewedWhileUnfilled(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	markHeld(h.account, now.Add(30*time.Minute))
	degraded, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	h.up.queue = []*http.Response{degraded}

	h.run(t)

	// -rotate 代理摇到 312 会继续换出口再摇，直到队列空（传输错误）结束本轮；这里只关心暂停没被动。
	require.NotEmpty(t, h.up.requests)
	require.Zero(t, h.repo.clears)
	require.Empty(t, h.repo.holds, "猎手不续期")
	require.True(t, openAITurnStateModelHeld(h.account, hunterTestModel, time.Now()))
	resetAt, _ := openAITurnStateHoldResetAt(h.account, hunterTestModel)
	require.WithinDuration(t, now.Add(30*time.Minute), resetAt, time.Minute)
}

// TestOpenAITurnStateHoldExpiresWithoutDemand 钉住 terra 偶尔一次的情形：暂停到期、又没有新
// 流量，猎手就不再为它烧额度（空闲门槛照常生效）；停着期间无视门槛。
func TestOpenAITurnStateHoldExpiresWithoutDemand(t *testing.T) {
	now := time.Now().UTC()
	cfg := holdHunterConfig(map[string]any{"models": []any{"gpt-5.6-terra"}, "idle_minutes": 60})

	expired := newHunterHarness(hunterTestAccount(cfg), hunterWebshareProxy)
	setAccountModelRateLimitSnapshot(expired.account, "gpt-5.6-terra", now.Add(-time.Minute), openAITurnStateHoldLimitReason, now)
	expired.run(t)
	require.Empty(t, expired.up.requests, "到期又没流量：不猎")
	require.Equal(t, openAITurnStateHuntGateIdle, expired.state().Gate)
	require.Zero(t, expired.repo.clears, "已到期的条目不用再放回")

	active := newHunterHarness(hunterTestAccount(cfg), hunterWebshareProxy)
	setAccountModelRateLimitSnapshot(active.account, "gpt-5.6-terra", now.Add(30*time.Minute), openAITurnStateHoldLimitReason, now)
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	active.up.queue = []*http.Response{healthy}
	active.run(t)
	require.Len(t, active.up.requests, 1, "停着期间无视空闲门槛")
	require.Equal(t, "gpt-5.6-terra", active.state().Last[0].Model)
}

// TestOpenAITurnStateHoldReleasedWhenDisabled 钉住退路：暂停开关、猎手或接管任一关掉，
// 停着的模型下个 tick 就放回，且不再探测。
func TestOpenAITurnStateHoldReleasedWhenDisabled(t *testing.T) {
	now := time.Now().UTC()
	t.Run("暂停开关关掉", func(t *testing.T) {
		h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
		markHeld(h.account, now.Add(30*time.Minute))
		h.run(t)
		// 猎手本身还开着，放回之后照常猎；这里只钉放回。
		require.Equal(t, 1, h.repo.clears)
		require.False(t, openAITurnStateModelHeld(h.account, hunterTestModel, time.Now()))
	})
	t.Run("猎手关掉", func(t *testing.T) {
		h := newHunterHarness(hunterTestAccount(holdHunterConfig(map[string]any{"enabled": false})), hunterWebshareProxy)
		markHeld(h.account, now.Add(30*time.Minute))
		h.run(t)
		require.Empty(t, h.up.requests)
		require.Equal(t, 1, h.repo.clears)
		require.False(t, openAITurnStateModelHeld(h.account, hunterTestModel, time.Now()))
	})
	t.Run("接管关掉", func(t *testing.T) {
		account := hunterTestAccount(holdHunterConfig(nil))
		account.Extra[openAITurnStateAutoExtraKey] = false
		h := newHunterHarness(account, hunterWebshareProxy)
		markHeld(h.account, now.Add(30*time.Minute))
		h.run(t)
		require.Empty(t, h.up.requests)
		require.Equal(t, 1, h.repo.clears)
	})
}

// TestOpenAITurnStateHoldReleasedWhenTicketAlreadyPooled 钉住：池里已经有可用票（比如真实
// 流量自然铸出的）就直接放回，不用再探测。
func TestOpenAITurnStateHoldReleasedWhenTicketAlreadyPooled(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	markHeld(h.account, now.Add(30*time.Minute))
	h.account.Extra[openAITurnStatePoolExtraKey] = []any{map[string]any{
		"blob": turnStateFernetBlob(now.Add(-5*time.Minute), openAIHealthyTurnStateBlocks), "model": hunterTestModel, "minted_at": now.Add(-5 * time.Minute),
	}}

	h.run(t)

	require.Empty(t, h.up.requests, "票还够用，不探测")
	require.Equal(t, 1, h.repo.clears)
	require.False(t, openAITurnStateModelHeld(h.account, hunterTestModel, time.Now()))
}

// TestOpenAITurnStateHeldModelsIgnoreOtherReasons 钉住：别的原因写的模型级限流（spark 429、
// 画图能力丢失）不归本功能管；到期的也不算。
func TestOpenAITurnStateHeldModelsIgnoreOtherReasons(t *testing.T) {
	now := time.Now()
	a := hunterTestAccount(holdHunterConfig(nil))
	setAccountModelRateLimitSnapshot(a, "gpt-5.3-codex-spark", now.Add(time.Hour), openAICodexSparkRateLimitReason, now)
	setAccountModelRateLimitSnapshot(a, "gpt-5.6-terra", now.Add(-time.Minute), openAITurnStateHoldLimitReason, now)
	require.Empty(t, openAITurnStateHeldModels(a, now))
	require.False(t, openAITurnStateModelHeld(a, "gpt-5.3-codex-spark", now))

	setAccountModelRateLimitSnapshot(a, hunterTestModel, now.Add(time.Hour), openAITurnStateHoldLimitReason, now)
	setAccountModelRateLimitSnapshot(a, "gpt-5.6-sol", now.Add(time.Hour), openAITurnStateHoldLimitReason, now)
	require.Equal(t, []string{"gpt-5.6-sol", hunterTestModel}, openAITurnStateHeldModels(a, now), "按名排序")
	require.Empty(t, openAITurnStateHeldModels(a, now.Add(2*time.Hour)), "到期就不算停着")
	require.Empty(t, openAITurnStateHeldModels(nil, now))
}

// TestOpenAITurnStateHoldTTLFollowsIdleWindow 钉住暂停时长 = 空闲窗口；门槛关掉时取默认 1 小时。
func TestOpenAITurnStateHoldTTLFollowsIdleWindow(t *testing.T) {
	cfg, _ := readOpenAITurnStateHunterConfig(hunterTestAccount(holdHunterConfig(map[string]any{"idle_minutes": 45})))
	require.Equal(t, 45*time.Minute, openAITurnStateHoldTTL(cfg))
	cfg, _ = readOpenAITurnStateHunterConfig(hunterTestAccount(holdHunterConfig(nil))) // idle_minutes=-1：门槛关
	require.Equal(t, openAITurnStateHoldDefaultTTL, openAITurnStateHoldTTL(cfg))
}

// TestOpenAITurnStateHoldKeyMatchesAliasSpelling 钉住评审 S2：暂停按规范化后的上游模型名写，
// 客户端换个写法（大小写、openai/ 前缀）请求同一个模型，调度器也要认出它停着。
func TestOpenAITurnStateHoldKeyMatchesAliasSpelling(t *testing.T) {
	account := hunterTestAccount(holdHunterConfig(nil))
	account.Schedulable = true
	setAccountModelRateLimitSnapshot(account, hunterTestModel, time.Now().Add(30*time.Minute), openAITurnStateHoldLimitReason, time.Now())
	ctx := context.Background()
	for _, spelling := range []string{hunterTestModel, "GPT-6-Astra", "openai/gpt-6-astra"} {
		require.False(t, account.IsSchedulableForModelWithContext(ctx, spelling), spelling)
	}
	require.True(t, account.IsSchedulableForModelWithContext(ctx, "gpt-5.6-sol"))
}

// TestOpenAITurnStateHoldReleaseSkipsWhenAlreadyCleared 钉住放回前的重读：这一轮里条目已被
// 换成别的原因（管理员清限流后真实请求撞了 spark 429），猎手不能把那条冷却盖成已到期。
func TestOpenAITurnStateHoldReleaseSkipsWhenAlreadyCleared(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	markHeld(h.account, now.Add(30*time.Minute))
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}
	// 探测入池那一刻（UpdateExtra）模拟别的路径把条目换成了 spark 429 冷却。
	h.repo.onUpdateExtra = func() {
		setAccountModelRateLimitSnapshot(h.account, hunterTestModel, now.Add(time.Hour), openAICodexSparkRateLimitReason, now)
	}

	h.run(t)

	require.Zero(t, h.repo.clears, "原因已不是本功能的，不能写")
	resetAt := h.account.modelRateLimitResetAt(hunterTestModel)
	require.NotNil(t, resetAt)
	require.WithinDuration(t, now.Add(time.Hour), *resetAt, time.Minute, "spark 冷却原样保留")
}

// TestOpenAITurnStateAutoModeHuntsEverHeldModelAfterRestart 钉住评审 S1：自动定模型下，暂停到期
// 或被清掉之后、重启（铸造记忆为空、池空）再来一条请求，仍要算「猎手在管」——否则既不注入也
// 不拦，直接裸奔。留在 model_rate_limits 里的本功能条目就是持久化的记忆。
func TestOpenAITurnStateAutoModeHuntsEverHeldModelAfterRestart(t *testing.T) {
	repo := newTurnStateAutoRepo()
	account := hunterTestAccount(holdHunterConfig(map[string]any{"auto_models": true}))
	setAccountModelRateLimitSnapshot(account, hunterTestModel, time.Now().Add(-time.Minute), openAITurnStateHoldLimitReason, time.Now())
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo} // 新进程：没有铸造记忆

	require.True(t, gw.openAITurnStateHuntedModel(account, hunterTestModel))
	c := turnStateAutoCtxModel("real", hunterTestModel)
	gw.applyOpenAICodexTurnStateOverrideHeader(c, account, http.Header{})
	require.Error(t, openAITurnStateHoldError(c), "到期后再来一条：再停一次，不裸奔")
	require.Equal(t, []string{hunterTestModel}, repo.holds)
	require.False(t, gw.openAITurnStateHuntedModel(account, "gpt-5.6-sol"), "没停过、没铸过的模型仍不算")
}
