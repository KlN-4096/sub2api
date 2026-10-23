//go:build unit

package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestCPRQuotaActionHandlersValidateIDsBodiesAndUnavailableService(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &AccountHandler{}
	router := gin.New()
	router.POST("/accounts/:id/cpr/quota/refresh", handler.RefreshCPRQuota)
	router.POST("/accounts/:id/cpr/reset-quota", handler.ResetCPRQuota)
	for _, tc := range []struct {
		path string
		body string
		code int
	}{
		{"/accounts/invalid/cpr/quota/refresh", "", 400},
		{"/accounts/0/cpr/quota/refresh", "", 400},
		{"/accounts/56/cpr/quota/refresh", "", 503},
		{"/accounts/-1/cpr/reset-quota", `{}`, 400},
		{"/accounts/56/cpr/reset-quota", `{}`, 400},
		{"/accounts/56/cpr/reset-quota", `{"redeem_request_id":"8fbf302d-11df-4bd5-82e4-08e4b3df7874"}`, 503},
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(rec, req)
		require.Equal(t, tc.code, rec.Code, "%s: %s", tc.path, rec.Body.String())
	}
}

func TestCPRStoredAdminKeyNeverGoesToChangedAddress(t *testing.T) {
	existing := &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeCPR, Credentials: map[string]any{"base_url": "http://127.0.0.1:18081", "admin_api_key": "test-stored-admin"}}
	for _, tc := range []struct {
		url     string
		allowed bool
	}{
		{"http://127.0.0.1:18081/", true},
		{"http://other.invalid:18081", false},
		{"http://127.0.0.1:18082", false},
	} {
		t.Run(tc.url, func(t *testing.T) {
			next := &service.Account{Type: service.AccountTypeCPR, Credentials: map[string]any{"base_url": tc.url}}
			err := resolveCPRAdminKey(next, existing)
			if tc.allowed {
				require.NoError(t, err)
				require.Equal(t, "test-stored-admin", next.GetCPRAdminAPIKey())
			} else {
				require.Error(t, err)
				require.Empty(t, next.GetCPRAdminAPIKey())
			}
		})
	}
}
