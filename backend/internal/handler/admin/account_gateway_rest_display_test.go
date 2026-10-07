package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type restDisplayRepo struct {
	service.AccountRepository
	account *service.Account
}

func (r restDisplayRepo) GetByID(context.Context, int64) (*service.Account, error) {
	return r.account, nil
}

type restDisplayAdmin struct {
	service.AdminService
	rest service.GatewayPoolRestView
	fail bool
}

func (a restDisplayAdmin) GatewayPoolRuntimeProgress(context.Context, []int64) (map[int64]service.GatewayPoolProgress, error) {
	if a.fail {
		return nil, errors.New("private diagnostic")
	}
	return map[int64]service.GatewayPoolProgress{1: {Runtime: &service.GatewayPoolRuntimeView{Rest: a.rest}}}, nil
}

func TestAccountGatewayRestStatusPersistsPastRecheckWithoutChangingOtherBlocks(t *testing.T) {
	for _, mode := range []string{"expired", "cleared", "inactive", "ordinary", "ordinary-unavailable", "expired-ordinary", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			past, future := time.Now().Add(-time.Minute), time.Now().Add(time.Hour)
			account := &service.Account{ID: 1, TempUnschedulableUntil: &past}
			if mode == "cleared" {
				account.TempUnschedulableUntil = nil
			}
			if mode == "ordinary" || mode == "ordinary-unavailable" {
				account.TempUnschedulableUntil, account.TempUnschedulableReason = &future, "auth-block"
			}
			if mode == "expired-ordinary" {
				account.TempUnschedulableReason = "auth-block"
			}
			admin := restDisplayAdmin{rest: service.GatewayPoolRestView{
				Active: mode != "inactive", NextCheck: past, ChangedAt: past.Add(-time.Hour),
				Reason: "网关候选低于10，休息后达到40才恢复",
			}, fail: mode == "unavailable" || mode == "ordinary-unavailable"}
			h := &AccountHandler{adminService: admin, rateLimitService: service.NewRateLimitService(restDisplayRepo{account: account}, nil, nil, nil, nil)}
			w := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(w)
			ctx.Params = gin.Params{{Key: "id", Value: "1"}}
			ctx.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			h.GetTempUnschedulable(ctx)
			switch mode {
			case "unavailable":
				require.Equal(t, 503, w.Code)
				require.NotContains(t, w.Body.String(), "private diagnostic")
			case "ordinary", "ordinary-unavailable":
				require.Equal(t, 200, w.Code)
				require.Contains(t, w.Body.String(), "auth-block")
				require.NotContains(t, w.Body.String(), `"gateway_pool_rest":true`)
			case "inactive":
				require.Contains(t, w.Body.String(), `"active":false`)
			default:
				require.Equal(t, 200, w.Code)
				require.Contains(t, w.Body.String(), `"active":true`)
				require.Contains(t, w.Body.String(), `"gateway_pool_rest":true`)
			}
		})
	}
}
