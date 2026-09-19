//go:build unit

package service

import (
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

// TestOpenAITurnStateHoldBlocksUnfilledHuntedModel 钉住注入点：猎手管的模型拿不出票 →
// 停调度 24 小时（原因串带模型名）+ 换号错误；换到下一个账号时拦截标记要清掉。
func TestOpenAITurnStateHoldBlocksUnfilledHuntedModel(t *testing.T) {
	repo := newTurnStateAutoRepo()
	account := hunterTestAccount(holdHunterConfig(nil))
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
	require.Equal(t, []string{openAITurnStateHoldReasonPrefix + hunterTestModel}, repo.holds)
	require.NotNil(t, account.TempUnschedulableUntil)
	require.WithinDuration(t, time.Now().Add(openAITurnStateHoldTTL), *account.TempUnschedulableUntil, time.Minute)
	require.Equal(t, hunterTestModel, openAITurnStateHeldModel(account, time.Now()))
	require.True(t, gw.openAITurnStateTrafficSince(account.ID, hunterTestModel, time.Now().Add(-time.Minute)),
		"被拦下的请求也是真实流量：不记水位的话空闲门槛会把猎手刹住，暂停就永远解不开")

	// 同一个 gin 上下文换到下一个账号（没开暂停）：上一轮的拦截标记必须先清。
	other := hunterTestAccount(hunterConfig(nil))
	other.ID = 9202
	gw.applyOpenAICodexTurnStateOverrideHeader(c, other, http.Header{})
	require.NoError(t, openAITurnStateHoldError(c))
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
			require.Nil(t, account.TempUnschedulableUntil)
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

func markHeld(account *Account, until time.Time) {
	account.TempUnschedulableUntil, account.TempUnschedulableReason = &until, openAITurnStateHoldReasonPrefix+hunterTestModel
}

// TestOpenAITurnStateHoldReleasedWhenHunterHits 钉住主流程：停着的账号猎到 292 入池，同一
// tick 内放回，不等下一轮。
func TestOpenAITurnStateHoldReleasedWhenHunterHits(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	markHeld(h.account, now.Add(20*time.Hour))
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}

	h.run(t)

	require.Len(t, h.up.requests, 1, "停调度的账号照样猎")
	require.Equal(t, 1, h.repo.clears)
	require.Nil(t, h.account.TempUnschedulableUntil)
	require.Empty(t, h.account.TempUnschedulableReason)
}

// TestOpenAITurnStateHoldRenewedWhileUnfilled 钉住「一直暂停」：没摇到票就续期，不放回。
func TestOpenAITurnStateHoldRenewedWhileUnfilled(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	markHeld(h.account, now.Add(time.Hour))
	degraded, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	h.up.queue = []*http.Response{degraded}

	h.run(t)

	// -rotate 代理摇到 312 会继续换出口再摇，直到队列空（传输错误）退避；这里只关心停调度没被放回。
	require.NotEmpty(t, h.up.requests)
	require.Zero(t, h.repo.clears)
	require.Equal(t, []string{openAITurnStateHoldReasonPrefix + hunterTestModel}, h.repo.holds)
	require.WithinDuration(t, now.Add(openAITurnStateHoldTTL), *h.account.TempUnschedulableUntil, time.Minute)

	// 剩余还长就不写库。
	h.run(t)
	require.Len(t, h.repo.holds, 1)
}

// TestOpenAITurnStateHoldReleasedWhenDisabled 钉住退路：暂停开关、猎手或接管任一关掉，
// 停着的账号下个 tick 就放回，且不再探测。
func TestOpenAITurnStateHoldReleasedWhenDisabled(t *testing.T) {
	now := time.Now().UTC()
	t.Run("暂停开关关掉", func(t *testing.T) {
		h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
		markHeld(h.account, now.Add(20*time.Hour))
		h.run(t)
		// 猎手本身还开着，放回之后照常猎；这里只钉放回。
		require.Equal(t, 1, h.repo.clears)
		require.Nil(t, h.account.TempUnschedulableUntil)
	})
	t.Run("猎手关掉", func(t *testing.T) {
		h := newHunterHarness(hunterTestAccount(holdHunterConfig(map[string]any{"enabled": false})), hunterWebshareProxy)
		markHeld(h.account, now.Add(20*time.Hour))
		h.run(t)
		require.Empty(t, h.up.requests)
		require.Equal(t, 1, h.repo.clears)
		require.Nil(t, h.account.TempUnschedulableUntil)
	})
	t.Run("接管关掉", func(t *testing.T) {
		account := hunterTestAccount(holdHunterConfig(nil))
		account.Extra[openAITurnStateAutoExtraKey] = false
		h := newHunterHarness(account, hunterWebshareProxy)
		markHeld(h.account, now.Add(20*time.Hour))
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
	markHeld(h.account, now.Add(20*time.Hour))
	h.account.Extra[openAITurnStatePoolExtraKey] = []any{map[string]any{
		"blob": turnStateFernetBlob(now.Add(-5*time.Minute), openAIHealthyTurnStateBlocks), "model": hunterTestModel, "minted_at": now.Add(-5 * time.Minute),
	}}

	h.run(t)

	require.Empty(t, h.up.requests, "票还够用，不探测")
	require.Equal(t, 1, h.repo.clears)
	require.Nil(t, h.account.TempUnschedulableUntil)
}

// TestOpenAITurnStateHeldModelIgnoresOtherReasons 钉住：别的原因停的调度不归本功能管。
func TestOpenAITurnStateHeldModelIgnoresOtherReasons(t *testing.T) {
	now := time.Now()
	a := hunterTestAccount(holdHunterConfig(nil))
	until := now.Add(time.Hour)
	a.TempUnschedulableUntil, a.TempUnschedulableReason = &until, "429"
	require.Empty(t, openAITurnStateHeldModel(a, now))
	a.TempUnschedulableReason = openAITurnStateHoldReasonPrefix + hunterTestModel
	require.Equal(t, hunterTestModel, openAITurnStateHeldModel(a, now))
	require.Empty(t, openAITurnStateHeldModel(a, now.Add(2*time.Hour)), "到期就不算停着")
	require.Empty(t, openAITurnStateHeldModel(nil, now))
}
