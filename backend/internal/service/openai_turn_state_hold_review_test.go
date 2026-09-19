//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// TestOpenAITurnStateHoldHuntsDespiteIdleGate 钉住评审抓到的死锁：默认空闲门槛（60 分钟）下，
// 停着的账号收不到真实流量、水位永远不刷，门槛会把猎手刹住 → 永远放不回。被停的模型必须
// 无视空闲门槛。
func TestOpenAITurnStateHoldHuntsDespiteIdleGate(t *testing.T) {
	now := time.Now().UTC()
	cfg := holdHunterConfig(nil)
	delete(cfg, "idle_minutes") // 生产默认 60
	h := newHunterHarness(hunterTestAccount(cfg), hunterWebshareProxy)
	markHeld(h.account, now.Add(20*time.Hour))
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}

	h.run(t)

	require.Len(t, h.up.requests, 1, "没有真实流量也要猎：暂停本身就是需求信号")
	require.Equal(t, 1, h.repo.clears)
	require.Nil(t, h.account.TempUnschedulableUntil)

	// 对照：同样的配置、没被停着、没流量 → 门槛照常生效。
	idle := newHunterHarness(hunterTestAccount(cfg), hunterWebshareProxy)
	idle.run(t)
	require.Empty(t, idle.up.requests)
	require.Equal(t, openAITurnStateHuntGateIdle, idle.state().Gate)
}

// TestOpenAITurnStateHoldSurfacesFromBuildUpstreamRequest 钉住生产接线：拦截必须从
// buildUpstreamRequest 以换号错误的形态冒出来——只测叶子函数的话，删掉接线全套照样绿。
func TestOpenAITurnStateHoldSurfacesFromBuildUpstreamRequest(t *testing.T) {
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	ctx := context.Background()
	c := turnStateAutoCtxModel("real", hunterTestModel)
	_, err := h.gw.prepareCodexAccountIdentitySource(ctx, c, h.account)
	require.NoError(t, err)
	decoded := openAITurnStateProbeBody(hunterTestModel, "high", newOpenAITurnStateProbeIdentity(h.account))
	stageCodexOAuthIdentity(c, h.account, decoded, false)
	body, err := json.Marshal(decoded)
	require.NoError(t, err)

	req, err := h.gw.buildUpstreamRequest(ctx, c, h.account, body, "offline-token", true, "", true)
	require.Nil(t, req)
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.Equal(t, OpenAITurnStateHoldReason, failover.Reason)
	require.False(t, failover.ShouldReportAccountScheduleFailure(), "本地判定缺票不是账号出错，不进调度器错误率")
	require.Equal(t, []string{openAITurnStateHoldReasonPrefix + hunterTestModel}, h.repo.holds)

	// 透传构造同样要拦。
	c2 := turnStateAutoCtxModel("real-2", hunterTestModel)
	_, err = h.gw.prepareCodexAccountIdentitySource(ctx, c2, h.account)
	require.NoError(t, err)
	req, err = h.gw.buildUpstreamRequestOpenAIPassthrough(ctx, c2, h.account, body, "offline-token")
	require.Nil(t, req)
	require.ErrorAs(t, err, &failover)
	require.Equal(t, OpenAITurnStateHoldReason, failover.Reason)
	require.Len(t, h.repo.holds, 1, "已经停着就只换号，不再写库")
}

// TestOpenAITurnStateHoldReleaseGoesThroughRateLimitService 钉住放回要连 Redis 副本一起清：
// 管理页的「临时不可调度」弹窗读的是副本，只清 DB 会让它把已放回的账号继续显示成停着。
func TestOpenAITurnStateHoldReleaseGoesThroughRateLimitService(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	cache := &tempUnschedCacheStub{}
	h.gw.rateLimitService = &RateLimitService{accountRepo: h.repo, tempUnschedCache: cache}
	markHeld(h.account, now.Add(20*time.Hour))
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}

	h.run(t)

	require.Equal(t, 1, h.repo.clears)
	require.Equal(t, 1, cache.deleteCalls)
	require.Nil(t, h.account.TempUnschedulableUntil)
}

// TestOpenAITurnStateHoldReleaseSkipsWhenAlreadyCleared 钉住放回前的重读：探测这一轮里管理员
// 已「恢复调度」（或换了原因）的账号，猎手不再动它。
func TestOpenAITurnStateHoldReleaseSkipsWhenAlreadyCleared(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(holdHunterConfig(nil)), hunterWebshareProxy)
	markHeld(h.account, now.Add(20*time.Hour))
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}
	// 探测入池那一刻（UpdateExtra）模拟别的路径把停调度换成了 401 冷却。
	until := now.Add(30 * time.Minute)
	h.repo.onUpdateExtra = func() {
		h.account.TempUnschedulableUntil, h.account.TempUnschedulableReason = &until, "401"
	}

	h.run(t)

	require.Zero(t, h.repo.clears, "原因已不是本功能的，不能清")
	require.Equal(t, "401", h.account.TempUnschedulableReason)
}

// TestTokenRefreshKeepsTurnStateHold 钉住评审抓到的第二个坑：token 刷新成功后的「清临时停调度」
// 是给 401 恢复用的，不能把降智暂停一起放回——否则每个 token 周期都把缺票账号推回轮转。
func TestTokenRefreshKeepsTurnStateHold(t *testing.T) {
	repo := &tokenRefreshAccountRepo{}
	cache := &tempUnschedCacheStub{}
	cfg := &config.Config{TokenRefresh: config.TokenRefreshConfig{MaxRetries: 1}}
	svc := NewTokenRefreshService(repo, nil, nil, nil, nil, &tokenCacheInvalidatorStub{}, nil, cfg, cache)
	until := time.Now().Add(20 * time.Hour)
	account := &Account{ID: 15, Platform: PlatformOpenAI, Type: AccountTypeOAuth, TempUnschedulableUntil: &until, TempUnschedulableReason: openAITurnStateHoldReasonPrefix + hunterTestModel}
	refresher := &tokenRefresherStub{credentials: map[string]any{"access_token": "new-token"}}

	require.NoError(t, svc.refreshWithRetry(context.Background(), account, refresher, refresher, time.Hour))
	require.Equal(t, 1, repo.updateCalls)
	require.Zero(t, repo.clearTempCalls)
	require.Zero(t, cache.deleteCalls)
}

// TestRecoverAccountStateKeepsTurnStateHold 钉住：定时测试 / 配额重置 / 管理页测试成功后的
// 自动恢复只清限流列，不把降智暂停放回（那是猎手的事）。
func TestRecoverAccountStateKeepsTurnStateHold(t *testing.T) {
	resetAt := time.Now().Add(time.Hour)
	held := time.Now().Add(20 * time.Hour)
	repo := &rateLimitClearRepoStub{
		getByIDAccount: &Account{
			ID: 45, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
			RateLimitedAt: &resetAt, RateLimitResetAt: &resetAt,
			TempUnschedulableUntil: &held, TempUnschedulableReason: openAITurnStateHoldReasonPrefix + hunterTestModel,
		},
	}
	cache := &tempUnschedCacheRecorder{}
	svc := NewRateLimitService(repo, nil, &config.Config{}, nil, cache)

	result, err := svc.RecoverAccountAfterSuccessfulTest(context.Background(), 45, false)
	require.NoError(t, err)
	require.True(t, result.ClearedRateLimit)
	require.Equal(t, 1, repo.clearRateLimitCalls, "限流列照清")
	require.Equal(t, 0, repo.clearTempUnschedCalls, "降智暂停不放回")
	require.Empty(t, cache.deletedIDs)
}

// TestShadowCredentialUsableUnderTurnStateHold 钉住：降智暂停是模型级缺票，不是凭据坏死，
// spark 影子不连坐。
func TestShadowCredentialUsableUnderTurnStateHold(t *testing.T) {
	until := time.Now().Add(20 * time.Hour)
	a := &Account{ID: 46, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, TempUnschedulableUntil: &until}
	a.TempUnschedulableReason = "401"
	require.False(t, a.IsCredentialUsableForShadow())
	a.TempUnschedulableReason = openAITurnStateHoldReasonPrefix + hunterTestModel
	require.True(t, a.IsCredentialUsableForShadow())
}
