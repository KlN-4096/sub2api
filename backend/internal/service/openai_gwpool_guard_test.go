package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolGuardOffSkipsVerificationAndResponseJudgment(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	account.Extra["openai_gwpool_guard_enabled"] = false
	account.Extra["openai_gwpool_prewarm"] = true // Legacy setting is ignored.
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "changed-state"},
		{status: http.StatusOK, minted: "changed-state"},
		{status: http.StatusOK, minted: "changed-state"},
		{status: http.StatusOK, minted: "changed-state"},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	for range 4 {
		request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		request.Header.Set(openAICodexTurnStateHeader, "client-state")
		ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx, _ := withOpenAIGatewayPoolSink(request.Context(), ginCtx)
		response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
		require.NoError(t, err, "防护关闭时不要求模型可读，也不运行预检/响应截断")
		require.NotNil(t, response)
		_ = response.Body.Close()
	}
	require.Len(t, upstream.sentBodies, 4, "只发业务，不夹带验证")
	require.EqualValues(t, 1, fake.hits.Load(), "取票和缓存仍然生效")
	require.True(t, svc.codexCookies.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-142", time.Hour))
	require.False(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity), "关防护不等于验证成功")
}

func TestGatewayPoolStrictGuardBlocksInsufficientBudgetBeforeFetching(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func TestGatewayPoolGuardOnlyExplicitBooleanFalseDisables(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra["openai_gwpool_guard"] = "off"
	for _, setting := range []any{nil, true, "false", 0} {
		account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = setting
		require.True(t, account.gatewayPoolGuardEnabled())
	}
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = false
	require.False(t, account.gatewayPoolGuardEnabled())
	require.Equal(t, gatewayPoolWarmUnverifiedClientMsg, gatewayPoolClientMessage(errOpenAIGatewayPoolWarmUnverified))
}
