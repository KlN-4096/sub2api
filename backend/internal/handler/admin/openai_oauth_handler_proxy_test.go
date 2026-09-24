package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type proxyLookupFailingAdminService struct{ *stubAdminService }

func (proxyLookupFailingAdminService) GetProxy(context.Context, int64) (*service.Proxy, error) {
	return nil, errors.New("proxy lookup failed")
}

type openAIOAuthClientCallRecorder struct{ refreshCalls int }

func (r *openAIOAuthClientCallRecorder) ExchangeCode(context.Context, string, string, string, string, string) (*openai.TokenResponse, error) {
	return nil, errors.New("unexpected")
}

func (r *openAIOAuthClientCallRecorder) RefreshToken(context.Context, string, string) (*openai.TokenResponse, error) {
	r.refreshCalls++
	return nil, errors.New("unexpected")
}

func (r *openAIOAuthClientCallRecorder) RefreshTokenWithClientID(context.Context, string, string, string) (*openai.TokenResponse, error) {
	r.refreshCalls++
	return nil, errors.New("unexpected")
}

// 绑了代理却查不到时必须失败：RT 换 token 若退回直连，就是从网关 IP 直达 auth.openai.com。
// 与 PAT 导入（ImportCodexPersonalAccessToken）同口径。
func TestOpenAIRefreshTokenFailsClosedWhenProxyLookupFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	client := &openAIOAuthClientCallRecorder{}
	h := &OpenAIOAuthHandler{
		openaiOAuthService: service.NewOpenAIOAuthService(nil, client),
		adminService:       proxyLookupFailingAdminService{newStubAdminService()},
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/admin/openai/refresh-token", strings.NewReader(`{"refresh_token":"rt","proxy_id":7}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.RefreshToken(c)

	require.NotEqual(t, http.StatusOK, rec.Code)
	require.Zero(t, client.refreshCalls, "代理查不到时不得发起 token 交换")
}
