package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Retired keys are kept only as test inputs proving they cannot bypass policy.
const (
	openAIGatewayPoolGuardEnabledExtraKey        = "openai_gwpool_guard_enabled"
	openAIGatewayPoolProbeModelExtraKey          = "openai_gwpool_probe_model"
	openAIGatewayPoolRotationExtraKey            = "openai_gwpool_rotation"
	openAIGatewayPoolRotationMinGatewaysExtraKey = "openai_gwpool_rotation_min_gateways"
	openAIGatewayPoolWaitEnabledExtraKey         = "openai_gwpool_auto_wait"
	openAIGatewayPoolWaitSecondsExtraKey         = "openai_gwpool_max_wait_s"
	openAIGatewayPoolWarmTicketsExtraKey         = "openai_gwpool_warm_tickets"
)

func TestGatewayPoolLegacyGuardFalseCannotSkipLunaVerification(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	account.Extra["openai_gwpool_guard_enabled"] = false
	account.Extra["openai_gwpool_prewarm"] = true // Legacy setting is ignored.
	account.Extra["openai_gwpool_probe_model"] = "business"
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "probe-state"},
		{status: http.StatusOK, minted: "probe-state"},
		{status: http.StatusOK, minted: "changed-state"},
		{status: http.StatusOK, minted: "changed-state"},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(request.Context(), ginCtx)
	response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
	require.NoError(t, err)
	require.NotNil(t, response)
	_ = response.Body.Close()
	require.Len(t, upstream.sentBodies, 4, "A/B、原业务和后置确认都必须执行")
	require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[2], "业务正文不变")
	for _, i := range []int{0, 1, 3} {
		require.Equal(t, "gpt-6-luna", gjson.Get(upstream.sentBodies[i], "model").String())
		require.NotEqual(t, "x", gjson.Get(upstream.sentBodies[i], "input").String(), "验证不带业务正文")
	}
	require.EqualValues(t, 1, fake.hits.Load(), "取票和缓存仍然生效")
	require.True(t, svc.codexCookies.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-142", time.Hour))
	require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity))
}

func TestGatewayPoolStrictGuardBlocksExpiredBudgetBeforeFetching(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	shooter := &gwpoolWarmShooter{}
	err = svc.gatewayPoolWarmUpWith(request.WithContext(ctx), fake.account(1), gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot)
	require.Error(t, err, "预算不足必须拒绝业务，而不是放行")
	require.Empty(t, shooter.shots)
	require.Zero(t, fake.hits.Load())
}

func TestGatewayPoolStrictGuardChecksTheActualPairBeforeBusinessSend(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := fake.account(1)
	// 上一张虽验过，但随后已经换到新票；末端必须核对实际附带的票。
	svc.codexCookies.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "old-verified")
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(request.Context(), ginCtx)
	response, degraded, err := svc.doOpenAIUpstreamOnce(request.WithContext(ctx), "", account)
	require.Error(t, err)
	require.Nil(t, response)
	require.False(t, degraded, "未经验证不是明确降级")
	require.Empty(t, upstream.sentBodies)
}

func TestGatewayPoolLegacyProbeModelCannotChangeLuna(t *testing.T) {
	for _, setting := range []any{nil, "business", "gpt-6-astra", "gpt-6-sol", "invalid", true} {
		t.Run(fmt.Sprint(setting), func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			account.Extra["openai_gwpool_probe_model"] = setting
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
				{status: http.StatusOK, minted: "probe-state"},
				{status: http.StatusOK, minted: "probe-state"},
			}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
			require.NoError(t, err)
			ctx, _ := withOpenAIGatewayPoolSink(request.Context(), nil)
			require.NoError(t, svc.gatewayPoolWarmUp(request.WithContext(ctx), "", account))
			require.Len(t, upstream.sentBodies, 2)
			for _, body := range upstream.sentBodies {
				require.Equal(t, "gpt-6-luna", gjson.Get(body, "model").String())
			}
		})
	}
}
