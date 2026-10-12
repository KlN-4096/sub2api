package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// An in-process account write outside the pool write paths (429 rate limit,
// auth error, admin unschedule) lands right after the attempt's locked
// rest-admission read. In ordinary (non-continuous) mode the waiting-auth
// refresh is the last health check before the business send, so it must read
// the row again and see that write (a per-attempt row memo did not, ed8bd8b6c).
type freshnessGapRepo struct {
	*gatewayLocalRestRepo
	reads     atomic.Int64
	afterRead map[int64]func()
}

func (r *freshnessGapRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	row, err := r.gatewayLocalRestRepo.GetByID(ctx, id)
	if hook := r.afterRead[r.reads.Add(1)]; hook != nil {
		hook()
	}
	return row, err
}

func TestGatewayPoolHealthCheckBeforeSendSeesInProcessWrite(t *testing.T) {
	writes := map[string]func(*Account){
		"rate-limited":  func(a *Account) { until := time.Now().Add(time.Hour); a.RateLimitResetAt = &until },
		"auth-error":    func(a *Account) { a.Status = StatusError },
		"unschedulable": func(a *Account) { a.Schedulable = false },
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			repo := &freshnessGapRepo{gatewayLocalRestRepo: &gatewayLocalRestRepo{&gatewayRuntimeRepo{account: *account}}}
			upstream := &gwpoolEchoUpstream{}
			svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}
			svc.codexCookies.accountByID = svc.gatewayPoolLedgerRow
			gwpoolEchoSeedVerified(t, svc, account) // fast path: live ticket already verified full
			// Reads 1-2 are request level (usage start, waiting policy);
			// read 3 is the attempt's locked rest admission.
			var atAdmission atomic.Bool
			repo.reads.Store(0)
			repo.afterRead = map[int64]func(){3: func() {
				atAdmission.Store(strings.Contains(string(debug.Stack()), ").loadGatewayPoolRest("))
				repo.mu.Lock()
				write(&repo.account)
				repo.mu.Unlock()
			}}
			request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
			require.NoError(t, err)
			ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx, _ := withOpenAIGatewayPoolSink(request.Context(), ginCtx)
			response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			require.True(t, atAdmission.Load(), "read 3 must be the attempt's rest admission; re-anchor the write if the read order changed")
			require.Empty(t, upstream.sentBodies, "business request sent after this process marked the account unschedulable")
			require.Error(t, err)
		})
	}
}
