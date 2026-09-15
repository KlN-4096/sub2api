//go:build unit

package admin

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// codexProbeHandlerRepo 只记录恢复链会碰到的仓库写入；其余方法未实现，被意外调用会直接 panic。
type codexProbeHandlerRepo struct {
	service.AccountRepository
	account             *service.Account
	clearErrorCalls     int
	clearRateLimitCalls int
	setErrorMessages    []string
}

func (r *codexProbeHandlerRepo) GetByID(_ context.Context, id int64) (*service.Account, error) {
	if r.account != nil && r.account.ID == id {
		return r.account, nil
	}
	return nil, service.ErrAccountNotFound
}

func (r *codexProbeHandlerRepo) ClearError(context.Context, int64) error {
	r.clearErrorCalls++
	return nil
}

func (r *codexProbeHandlerRepo) ClearRateLimit(context.Context, int64) error {
	r.clearRateLimitCalls++
	return nil
}

func (r *codexProbeHandlerRepo) ClearAntigravityQuotaScopes(context.Context, int64) error { return nil }
func (r *codexProbeHandlerRepo) ClearModelRateLimits(context.Context, int64) error        { return nil }
func (r *codexProbeHandlerRepo) ClearTempUnschedulable(context.Context, int64) error      { return nil }
func (r *codexProbeHandlerRepo) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

func (r *codexProbeHandlerRepo) SetError(_ context.Context, _ int64, message string) error {
	r.setErrorMessages = append(r.setErrorMessages, message)
	return nil
}

// codexProbeOfflineUpstream 是探针的 /responses 出口：双开探针不该走到这里（走到就报错并计数）；
// 非双开的原 /responses 探针从这里拿一条离线的成功响应。
type codexProbeOfflineUpstream struct {
	calls   int32
	succeed bool
}

func (u *codexProbeOfflineUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	atomic.AddInt32(&u.calls, 1)
	if !u.succeed {
		return nil, errors.New("offline: no real upstream in tests")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\"}\n\n")),
	}, nil
}

func (u *codexProbeOfflineUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, accountConcurrency)
}

// 管理端"测试账号"那条腿：handler 必须把 service.AccountTestCredentialsOnly(c) 原样传给
// RecoverAccountAfterSuccessfulTest——双开账号的探针只证明凭据可用（只清 StatusError，不清限流窗口）；
// 非双开账号的原 /responses 探针成功后照旧连限流窗口一起清。整条链在真 AccountTestService +
// 真 RateLimitService 上离线跑：/models 指到本地假上游，/responses 出口是离线桩。
func TestAccountHandlerTestRecoveryFollowsProbeKind(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name                string
		convergence         bool
		wantModelsCalls     int32
		wantResponsesCalls  int32
		wantClearRateLimits int
	}{
		{name: "device/enabled", convergence: true, wantModelsCalls: 1, wantResponsesCalls: 0, wantClearRateLimits: 0},
		{name: "device/disabled", convergence: false, wantModelsCalls: 0, wantResponsesCalls: 1, wantClearRateLimits: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var modelsCalls int32
			models := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt32(&modelsCalls, 1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"models":[{"slug":"gpt-5.5","visibility":"list"}]}`))
			}))
			t.Cleanup(models.Close)
			t.Cleanup(service.SetCodexModelsURLForTest(models.URL + "/backend-api/codex/models"))

			rateLimitedAt := time.Now().Add(-time.Minute)
			account := &service.Account{
				ID:            9101,
				Name:          "codex-device",
				Platform:      service.PlatformOpenAI,
				Type:          service.AccountTypeOAuth,
				Status:        service.StatusError,
				Schedulable:   true,
				Concurrency:   1,
				RateLimitedAt: &rateLimitedAt,
				Credentials:   map[string]any{"access_token": "offline-token", "chatgpt_account_id": "offline-account"},
				Extra: map[string]any{
					"codex_fingerprint_mode":                     "device",
					"codex_fingerprint_seed":                     "11111111-1111-4111-8111-111111111111",
					"codex_experimental_fingerprint_convergence": tc.convergence,
				},
			}
			repo := &codexProbeHandlerRepo{account: account}
			upstream := &codexProbeOfflineUpstream{succeed: !tc.convergence}
			cfg := &config.Config{}
			tests := service.NewAccountTestService(repo, nil, nil, nil, nil, upstream, cfg, nil)
			tests.SetOpenAIGatewayService(&service.OpenAIGatewayService{})
			handler := &AccountHandler{
				accountTestService: tests,
				rateLimitService:   service.NewRateLimitService(repo, nil, cfg, nil, nil),
			}

			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/9101/test", strings.NewReader(`{"model_id":"gpt-5.5"}`))
			c.Request.Header.Set("Content-Type", "application/json")
			c.Params = gin.Params{{Key: "id", Value: "9101"}}
			handler.Test(c)

			require.Contains(t, rec.Body.String(), `"type":"test_complete"`, "探针成功: %s", rec.Body.String())
			require.Empty(t, c.Errors)
			require.Equal(t, tc.wantModelsCalls, atomic.LoadInt32(&modelsCalls), "GET /models 次数")
			require.Equal(t, tc.wantResponsesCalls, atomic.LoadInt32(&upstream.calls), "/responses 次数")
			require.Equal(t, 1, repo.clearErrorCalls, "探针成功 → 清 StatusError")
			require.Equal(t, tc.wantClearRateLimits, repo.clearRateLimitCalls, "限流窗口：双开凭据探针不清（GET /models 的 200 证明不了推理配额已恢复），非双开原探针清")
			require.Empty(t, repo.setErrorMessages)
		})
	}
}
