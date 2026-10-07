package admin

import (
	"context"
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
