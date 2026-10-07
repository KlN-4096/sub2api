package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolFirstOutputBudgetStartsOnlyAtDispatchAndSurvivesRepair(t *testing.T) {
	budget := &gatewayPoolFirstOutputBudget{timeout: 20 * time.Millisecond}
	base := context.WithValue(context.Background(), gatewayPoolFirstOutputBudgetKey{}, budget)
	ctx, guard := newGatewayPoolFirstOutputGuard(base, func() {}, budget)
	defer guard.close()
	time.Sleep(40 * time.Millisecond)
	require.NoError(t, ctx.Err(), "preparation cannot spend the business first-output budget")
	require.True(t, budget.started.IsZero())
	startGatewayPoolFirstOutputGuard(ctx)
	started := gatewayPoolFirstOutputStart(base, time.Time{})
	require.False(t, started.IsZero())
	startGatewayPoolFirstOutputGuard(ctx)
	require.Equal(t, started, gatewayPoolFirstOutputStart(base, time.Time{}))
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("dispatch did not arm the header timeout")
	}
	require.True(t, guard.stopHeaderWait())
	retry, next := newGatewayPoolFirstOutputGuard(base, func() {}, budget)
	defer next.close()
	select {
	case <-retry.Done():
	case <-time.After(time.Second):
		t.Fatal("repair incorrectly renewed the original business deadline")
	}
	require.True(t, next.stopHeaderWait())
}

func TestGatewayPoolFirstOutputUnsentCancelDoesNotStartTimer(t *testing.T) {
	budget := &gatewayPoolFirstOutputBudget{timeout: time.Second}
	base, cancel := context.WithCancel(context.Background())
	ctx, guard := newGatewayPoolFirstOutputGuard(base, func() {}, budget)
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.False(t, guard.stopHeaderWait())
	guard.close()
	startGatewayPoolFirstOutputGuard(ctx)
	require.True(t, budget.started.IsZero(), "late dispatch cannot revive a closed guard")
}

type gatewayPoolSlowAdmissionRepo struct{ *gatewayRuntimeRepo }

func (r *gatewayPoolSlowAdmissionRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	if err := gatewayPoolSleep(ctx, 60*time.Millisecond); err != nil {
		return nil, err
	}
	return r.gatewayRuntimeRepo.GetByID(ctx, id)
}

func TestGatewayPoolFirstOutputWaitsForFinalFreshAdmission(t *testing.T) {
	account := gwpoolTestAccount(1)
	repo := &gatewayPoolSlowAdmissionRepo{&gatewayRuntimeRepo{account: *account}}
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}
	budget := &gatewayPoolFirstOutputBudget{timeout: 20 * time.Millisecond}
	ctx, guard := newGatewayPoolFirstOutputGuard(context.Background(), func() {}, budget)
	defer guard.close()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	before := time.Now()
	response, _, err := svc.gatewayPoolObservedRoundTrip(request, "", account, true)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.NoError(t, response.Body.Close())
	require.False(t, guard.stopHeaderWait())
	require.GreaterOrEqual(t, budget.started.Sub(before), 50*time.Millisecond,
		"the final fresh rest check must not spend the first-output budget")
	require.Len(t, upstream.sentBodies, 1)
}

type gatewayPoolPreparedOutputUpstream struct {
	gwpoolEchoUpstream
	stallBusiness bool
}

func (u *gatewayPoolPreparedOutputUpstream) Do(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
	if len(u.sentBodies) == 0 {
		// Preparation takes longer than the configured 1s first-output timeout.
		if err := gatewayPoolSleep(req.Context(), 1100*time.Millisecond); err != nil {
			return nil, err
		}
	}
	if len(u.sentBodies) == 2 && u.stallBusiness {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	resp, err := u.gwpoolEchoUpstream.Do(req, proxy, id, concurrency)
	if err == nil && len(u.sentBodies) == 3 {
		resp.Body = io.NopCloser(strings.NewReader(
			"data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n" +
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_offline\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"))
	}
	return resp, err
}

func (u *gatewayPoolPreparedOutputUpstream) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, id, concurrency)
}

func TestGatewayPoolForwardPreparationDoesNotSpendBusinessFirstOutputTimeout(t *testing.T) {
	for _, stalled := range []bool{false, true} {
		t.Run(map[bool]string{false: "semantic-output", true: "business-header-timeout"}[stalled], func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			account.Credentials["access_token"] = "offline-token"
			upstream := &gatewayPoolPreparedOutputUpstream{
				gwpoolEchoUpstream: gwpoolEchoUpstream{replies: []gwpoolEchoReply{
					{status: 200, minted: "same-state"}, {status: 200, minted: "same-state"}, {status: 200},
				}},
				stallBusiness: stalled,
			}
			svc := &OpenAIGatewayService{httpUpstream: upstream, cfg: &config.Config{Gateway: config.GatewayConfig{
				OpenAIFirstOutputTimeoutSeconds: 1, MaxLineSize: defaultMaxLineSize,
			}}}
			body := []byte(`{"model":"gpt-6-astra","stream":true,"reasoning":{"effort":"low"},"input":"business"}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := svc.Forward(ctx, c, account, body)
			if stalled {
				var failure *UpstreamFailoverError
				require.ErrorAs(t, err, &failure)
				require.Equal(t, http.StatusGatewayTimeout, failure.StatusCode)
				require.Contains(t, string(failure.ResponseBody), "first_output_timeout")
				require.Empty(t, recorder.Body.String())
				require.Len(t, upstream.sentBodies, 2, "preparation must finish before business timeout")
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Contains(t, recorder.Body.String(), `"delta":"ok"`)
				require.Len(t, upstream.sentBodies, 3, "two verification shots, exactly one business")
			}
		})
	}
}
