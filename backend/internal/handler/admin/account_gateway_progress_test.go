package admin

import (
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
