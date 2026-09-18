//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOpenAITurnStateHoldHuntsDespiteIdleGate 钉住评审抓到的死锁：默认空闲门槛（60 分钟）下，
// 停着的模型收不到真实流量、水位永远不刷，门槛会把猎手刹住 → 永远放不回。被停的模型必须
// 无视空闲门槛。
func TestOpenAITurnStateHoldHuntsDespiteIdleGate(t *testing.T) {
	now := time.Now().UTC()
	cfg := holdHunterConfig(nil)
	delete(cfg, "idle_minutes") // 生产默认 60
	h := newHunterHarness(hunterTestAccount(cfg), hunterWebshareProxy)
	markHeld(h.account, now.Add(30*time.Minute))
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}

	h.run(t)

	require.Len(t, h.up.requests, 1, "没有真实流量也要猎：暂停本身就是需求信号")
	require.Equal(t, 1, h.repo.clears)
	require.False(t, openAITurnStateModelHeld(h.account, hunterTestModel, time.Now()))

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
	require.Equal(t, []string{hunterTestModel}, h.repo.holds)

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
