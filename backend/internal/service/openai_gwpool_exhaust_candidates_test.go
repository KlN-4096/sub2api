package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type exhaustCandidatesRepo struct{ *gatewayRuntimeRepo }

func (r exhaustCandidatesRepo) SetTempUnschedulable(_ context.Context, _ int64, until time.Time, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.account.TempUnschedulableUntil = &until
	r.account.TempUnschedulableReason = reason
	return nil
}

func TestGatewayPoolExhaustCandidatesOutlivesBudgetAndUsesLastTicket(t *testing.T) {
	svc, repo, fake, account, _, _ := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = 1
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 10
	repo.account = *account
	first, last := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-143")
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}, {Name: "unified-143", PairReady: true}}
	fake.cookieForHit = func(hit int64) string {
		if hit > 1 {
			return last
		}
		return first
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	ctx, _ = withOpenAIGatewayPoolSink(ctx, nil)
	ctx = svc.gatewayPoolWaitContext(ctx, account)
	wait := gatewayPoolWaitFrom(ctx)
	require.NotNil(t, wait)
	deadline, _ := ctx.Deadline()
	require.Greater(t, time.Until(deadline), time.Second, "only the real caller deadline applies")
	wait.sleep = func(context.Context, time.Duration) error {
		t.Error("conclusive rejection must continue directly to the next candidate")
		return nil
	}
	require.False(t, svc.gatewayPoolNoRemainingRoutes(ctx, account), "a stored threshold of 10 cannot discard two remaining candidates")
	shots := 0
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	err = svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel,
		func(ctx context.Context, _, _ string) (int, string, error) {
			shots++
			switch shots {
			case 1:
				select {
				case <-time.After(1100 * time.Millisecond):
				case <-ctx.Done():
					return 0, "", ctx.Err()
				}
				return 200, "first-a", nil
			case 2:
				return 200, "first-b", nil
			default:
				return 200, "last-proof", nil
			}
		})
	require.NoError(t, err)
	require.Equal(t, 4, shots)
	require.EqualValues(t, 2, fake.hits.Load())
}

func TestGatewayPoolExhaustCandidatesStopsAtLastFailureInsteadOfWaitingForCooldown(t *testing.T) {
	svc, repo, fake, account, _, _ := ticketWaitFixture(t)
	svc.accountRepo = exhaustCandidatesRepo{repo}
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	repo.account = *account
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx, _ = withOpenAIGatewayPoolSink(ctx, nil)
	ctx = svc.gatewayPoolWaitContext(ctx, account)
	gatewayPoolWaitFrom(ctx).sleep = func(context.Context, time.Duration) error {
		t.Error("exhausted ordinary requests must not wait for cooldown")
		return nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	shots := 0
	err = svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel,
		func(context.Context, string, string) (int, string, error) {
			shots++
			if shots == 1 {
				return 200, "first", nil
			}
			return 200, "changed", nil
		})
	require.ErrorIs(t, err, errGatewayPoolWarmAttemptsFinished)
	require.Equal(t, 2, shots)
	require.True(t, svc.gatewayPoolNoRemainingRoutes(ctx, account))
	require.NoError(t, svc.restGatewayPoolAccount(ctx, account, gwpoolTestIdentity, 7))
	fresh, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.True(t, readGatewayPoolRest(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity)).Active)
}

func TestGatewayPoolExhaustCandidatesUnknownSupplyDoesNotBecomeRest(t *testing.T) {
	svc, repo, fake, account, _, _ := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	repo.account = *account
	fake.listStatus = http.StatusBadGateway
	require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account))
	require.False(t, readGatewayPoolRest(account, gatewayPoolLedgerTag(gwpoolTestIdentity)).Active)
}

func TestGatewayPoolExhaustCandidatesStillHonors429Deadline(t *testing.T) {
	svc, repo, _, account, request, _ := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	repo.account = *account
	group := int64(7)
	failure := &UpstreamFailoverError{StatusCode: http.StatusTooManyRequests, RetryableOnSameAccount: true,
		SameAccountRetryDeadline: time.Now().Add(time.Minute)}
	ctx := svc.PrepareGatewayPoolAccountRotation(request.Context(), &group, account, failure)
	require.True(t, failure.SameAccountRetryOnly)
	retry := gatewayPoolRetryOnlyFrom(ctx)
	require.Equal(t, failure.SameAccountRetryDeadline, retry.deadline)
	retry.deadline = time.Now().Add(-time.Second)
	ctx = context.WithValue(ctx, gatewayPoolRetryOnlyKey{}, retry)
	require.NotNil(t, GatewayPoolRetryFailure(ctx), "unbounded candidate search must not extend a real 429 window")
}

func TestGatewayPoolExhaustCandidatesRefreshesProbeCredentials(t *testing.T) {
	svc, repo, fake, account, request, _ := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	account.Credentials["access_token"] = "old-offline-token"
	repo.account = *account
	account, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	fake.beforeCookie = func() {
		repo.mu.Lock()
		repo.account.Credentials["access_token"] = "new-offline-token"
		repo.mu.Unlock()
	}
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "proof"}, {status: http.StatusOK, minted: "proof"}, {status: http.StatusOK},
	}}
	var auth []string
	upstream.beforeReply = func(req *http.Request, _ int) error {
		auth = append(auth, req.Header.Get("Authorization"))
		return nil
	}
	svc.httpUpstream = upstream
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx, _ = withOpenAIGatewayPoolSink(ctx, nil)
	response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, []string{"Bearer new-offline-token", "Bearer new-offline-token", "Bearer new-offline-token"}, auth)
}
