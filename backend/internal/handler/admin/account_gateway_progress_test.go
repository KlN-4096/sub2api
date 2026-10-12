package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type gatewayProgressAdmin struct{ service.AdminService }

func (gatewayProgressAdmin) GatewayPoolProgress(ids []int64) map[int64]service.GatewayPoolProgress {
	return map[int64]service.GatewayPoolProgress{ids[0]: {Phase: "verifying", Attempt: 2, Limit: 5}}
}

type gatewayRetryAdmin struct {
	service.AdminService
	fail bool
}

func (s gatewayRetryAdmin) RetryGatewayPool(context.Context, int64) (service.GatewayPoolRetryResult, error) {
	if s.fail {
		return service.GatewayPoolRetryResult{}, errors.New("private upstream diagnostic")
	}
	return service.GatewayPoolRetryResult{State: "retained"}, nil
}

func TestAccountGatewayPoolRetryValidatesAndSanitizes(t *testing.T) {
	for _, tc := range []struct {
		id     string
		fail   bool
		status int
	}{{"1", false, 200}, {"-1", false, 400}, {"cookie", false, 400}, {"1", true, 503}} {
		handler := &AccountHandler{adminService: gatewayRetryAdmin{fail: tc.fail}}
		w := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(w)
		ctx.Params = gin.Params{{Key: "id", Value: tc.id}}
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", nil)
		handler.RetryGatewayPool(ctx)
		require.Equal(t, tc.status, w.Code)
		require.NotContains(t, w.Body.String(), "private upstream")
	}
}

func TestAccountGatewayPoolProgressValidatesIDsAndReturnsRuntimeOnly(t *testing.T) {
	handler := &AccountHandler{adminService: gatewayProgressAdmin{}}
	for _, tc := range []struct {
		ids    string
		status int
	}{{"1,2", 200}, {"", 400}, {"-1", 400}, {"cookie", 400}} {
		w := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(w)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/?ids="+tc.ids, nil)
		handler.GatewayPoolProgress(ctx)
		require.Equal(t, tc.status, w.Code)
		if tc.status == 200 {
			require.Contains(t, w.Body.String(), `"attempt":2`)
			require.NotContains(t, w.Body.String(), "cookie")
		}
	}
}

type gatewayEstimateAdmin struct {
	service.AdminService
	estimates *[]bool
}

func (a gatewayEstimateAdmin) GatewayPoolRuntimeProgress(_ context.Context, ids []int64, estimate bool) (map[int64]service.GatewayPoolProgress, error) {
	*a.estimates = append(*a.estimates, estimate)
	return map[int64]service.GatewayPoolProgress{ids[0]: {Phase: "idle"}}, nil
}

func TestAccountGatewayPoolProgressAsksForEstimateOnlyWhenRequested(t *testing.T) {
	var estimates []bool
	handler := &AccountHandler{adminService: gatewayEstimateAdmin{estimates: &estimates}}
	for _, query := range []string{"?ids=1", "?ids=1&estimate=1", "?ids=1&estimate=0"} {
		w := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(w)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/"+query, nil)
		handler.GatewayPoolProgress(ctx)
		require.Equal(t, 200, w.Code)
	}
	require.Equal(t, []bool{false, true, false}, estimates)
}

type gatewayDisplayAdmin struct{ *stubAdminService }

func (gatewayDisplayAdmin) ResolveGatewayPoolDisplay(_ context.Context, accounts []*service.Account) {
	for _, account := range accounts {
		account.Extra["openai_gwpool_gateways"] = map[string]any{"current": "listed"}
	}
}

func TestAccountListAttachesGatewayPoolDisplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admin := gatewayDisplayAdmin{newStubAdminService()}
	router := gin.New()
	router.GET("/accounts", NewAccountHandler(admin, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil).List)
	for _, lite := range []string{"0", "1"} {
		// A fresh row each time: the stub hands out its own slice.
		admin.accounts = []service.Account{{ID: 9, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Extra: map[string]any{}}}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/accounts?lite="+lite, nil))
		require.Equal(t, http.StatusOK, w.Code)
		var payload struct {
			Data struct {
				Items []struct {
					Extra map[string]map[string]any `json:"extra"`
				} `json:"items"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
		require.Len(t, payload.Data.Items, 1)
		require.Equal(t, "listed", payload.Data.Items[0].Extra["openai_gwpool_gateways"]["current"], "lite=%s", lite)
	}
}

func TestAccountSingleRowResponseAttachesGatewayPoolDisplay(t *testing.T) {
	// Update and the other single-row endpoints share GetByID's response
	// builder, and the page swaps the row's extra for what they return.
	gin.SetMode(gin.TestMode)
	admin := gatewayDisplayAdmin{newStubAdminService()}
	admin.getAccountResult = &service.Account{ID: 9, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Extra: map[string]any{}}
	router := gin.New()
	router.GET("/accounts/:id", NewAccountHandler(admin, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil).GetByID)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/accounts/9", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var payload struct {
		Data struct {
			Extra map[string]map[string]any `json:"extra"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
	require.Equal(t, "listed", payload.Data.Extra["openai_gwpool_gateways"]["current"])
}
