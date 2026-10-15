package service

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolStopDefaultFiveAtNextTicketBoundary(t *testing.T) {
	for _, count := range []int{4, 5} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.listGateways = []gwpoolFakeGateway{}
			for i := 0; i < count; i++ {
				fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{Name: fmt.Sprintf("unified-%d", i), PairReady: true})
			}
			account := rotationAccount(1, 7)
			delete(account.Extra, "openai_gwpool_stop_gateways")
			fake.configure(account)
			svc := rotationService(account)
			failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
			group := int64(7)
			svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure, nil)
			require.Equal(t, count < 5, failure.ShouldRetryNextAccount())
			require.Zero(t, fake.hits.Load(), "checking stop policy never takes a ticket")
		})
	}
}

func TestGatewayPoolStopOldWaiterHandsOffAfterPoolRest(t *testing.T) {
	group := int64(7)
	source := rotationAccount(1, group)
	fresh := *source
	fresh.Extra = maps.Clone(source.Extra)
	until := time.Now().Add(time.Hour)
	tag := gatewayPoolLedgerTag(openAIGatewayPoolAccountKey(source))
	fresh.Extra[gatewayPoolRestStateKey] = gatewayPoolRestState{
		Tag: tag, Active: true, ChangedAt: time.Now(), StartedAt: time.Now(), ResumeAt: until,
	}
	fresh.Extra[gatewayPoolRestTagKey] = tag
	fresh.TempUnschedulableUntil = &until
	fresh.TempUnschedulableReason = gatewayPoolRestReason(&fresh)
	svc := rotationService(&fresh)
	// This waiter was already selected before another request entered rest.
	failure := &UpstreamFailoverError{
		StatusCode: http.StatusBadGateway, Reason: OpenAIGatewayPoolReason, NextAccountAction: NextAccountStop,
	}
	ctx := svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, source, failure, nil)
	require.True(t, failure.ShouldRetryNextAccount(), "a proven pool-rest latch must not strand old safe-to-replay requests")
	require.NotNil(t, gatewayPoolRotationFrom(ctx))
}

func TestGatewayPoolStopPreparationBoundaryPreservesCurrentTicketAndContinuousWait(t *testing.T) {
	for _, mode := range []string{"below", "equal", "live", "continuous", "unknown", "busy"} {
		t.Run(mode, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-0"), 150)
			account := fake.account(1)
			account.Extra[openAIGatewayPoolStopGatewaysExtraKey] = 5
			count := 4
			if mode == "equal" {
				count = 5
			}
			fake.listGateways = []gwpoolFakeGateway{}
			for i := 0; i < count; i++ {
				fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{Name: fmt.Sprintf("unified-%d", i), PairReady: true})
			}
			svc := &OpenAIGatewayService{}
			if mode == "live" {
				gwpoolEchoSeedVerified(t, svc, account)
			}
			if mode == "continuous" {
				account.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
			}
			if mode == "unknown" {
				fake.listStatus = http.StatusBadGateway
			}
			if mode == "busy" {
				finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
				defer finish()
			}
			plan := svc.gatewayPoolStopCatalog(context.Background(), account)
			require.Equal(t, mode == "below", plan != nil)
			if mode == "unknown" || mode == "busy" {
				return
			}
			shoots, before := 0, fake.hits.Load()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(`{"model":"gpt-6-astra"}`))
			require.NoError(t, err)
			err = svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gatewayPoolProbeModelLuna,
				func(context.Context, string, string) (int, string, error) { shoots++; return 200, "same", nil })
			if mode == "below" {
				require.ErrorIs(t, err, errGatewayPoolStopThreshold)
				require.Zero(t, shoots)
				require.Equal(t, before, fake.hits.Load())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGatewayPoolStopCountsDistinctReadyLocalEligibleGateways(t *testing.T) {
	fake := newGwpoolFakePool(t, "offline", 150)
	account := fake.account(1)
	account.Extra[openAIGatewayPoolStopGatewaysExtraKey] = 5
	fake.listGateways = []gwpoolFakeGateway{
		{Name: "a", PairReady: true}, {Name: "a", PairReady: true},
		{Name: "b", PairReady: true}, {Name: "c", PairReady: true},
		{Name: "d", PairReady: true}, {Name: "cooling", PairReady: true},
		{Name: "not-ready"},
	}
	svc := &OpenAIGatewayService{}
	svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "cooling")
	require.NotNil(t, svc.gatewayPoolStopCatalog(context.Background(), account))
	require.Equal(t, 5, account.gatewayPoolStopGateways())
	delete(account.Extra, openAIGatewayPoolStopGatewaysExtraKey)
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 99
	require.Equal(t, 5, account.gatewayPoolStopGateways(), "retired configuration must not revive")
}

func TestGatewayPoolStopHandoffDoesNotBypassUnrelatedRestrictions(t *testing.T) {
	for _, mode := range []string{"manual", "disabled", "auth", "rate-limit", "identity", "proxy", "pool-key", "continuous", "no-latch", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			source := rotationAccount(1, 7)
			fresh := *source
			fresh.Extra = maps.Clone(source.Extra)
			fresh.Credentials = maps.Clone(source.Credentials)
			until := time.Now().Add(time.Hour)
			tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
			fresh.Extra[gatewayPoolRestStateKey] = gatewayPoolRestState{Tag: tag, Active: true, ChangedAt: time.Now(), ResumeAt: until}
			fresh.Extra[gatewayPoolRestTagKey] = tag
			fresh.TempUnschedulableUntil, fresh.TempUnschedulableReason = &until, gatewayPoolRestReason(&fresh)
			switch mode {
			case "manual":
				fresh.TempUnschedulableReason = "manual"
			case "disabled":
				fresh.Schedulable = false
			case "auth":
				fresh.Status = StatusError
			case "rate-limit":
				fresh.RateLimitResetAt = &until
			case "identity":
				fresh.Credentials["chatgpt_user_id"] = "different"
			case "proxy":
				proxy := int64(9)
				fresh.ProxyID = &proxy
			case "pool-key":
				fresh.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "changed"
			case "continuous":
				fresh.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
			case "no-latch":
				delete(fresh.Extra, gatewayPoolRestStateKey)
			}
			svc := rotationService(&fresh)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			failure := &UpstreamFailoverError{StatusCode: http.StatusBadGateway, Reason: OpenAIGatewayPoolReason, NextAccountAction: NextAccountStop}
			group := int64(7)
			svc.PrepareGatewayPoolAccountRotation(ctx, &group, source, failure, nil)
			require.False(t, failure.ShouldRetryNextAccount())
		})
	}
}

// A credential found resting after its 429 window was spent still hands the
// request on; the spent window only stops new same-account retries.
func TestGatewayPoolStopRestHandsOffAfterSpent429Window(t *testing.T) {
	source := rotationAccount(1, 7)
	fresh := *source
	fresh.Extra = maps.Clone(source.Extra)
	until := time.Now().Add(time.Hour)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	fresh.Extra[gatewayPoolRestStateKey] = gatewayPoolRestState{Tag: tag, Active: true, ChangedAt: time.Now(), ResumeAt: until}
	fresh.Extra[gatewayPoolRestTagKey] = tag
	fresh.TempUnschedulableUntil, fresh.TempUnschedulableReason = &until, gatewayPoolRestReason(&fresh)
	svc := rotationService(&fresh)
	group := int64(7)
	ctx := context.WithValue(context.Background(), gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{
		accountID: source.ID, groupID: group, deadline: time.Now().Add(-time.Second),
	})
	failure := GatewayPoolRestFailure()
	ctx = svc.PrepareGatewayPoolAccountRotation(ctx, &group, source, failure, nil)
	require.True(t, failure.ShouldRetryNextAccount())
	require.NotNil(t, gatewayPoolRotationFrom(ctx))
}

func TestGatewayPoolStopSupersededBySharedWorkRejoinsPreparation(t *testing.T) {
	for _, mode := range []string{"busy", "live", "changed-during-confirmation", "replenished"} {
		t.Run(mode, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-0"), 150)
			fake.listGateways = []gwpoolFakeGateway{{Name: "unified-0", PairReady: true}}
			account := rotationAccount(1, 7)
			account.Extra[openAIGatewayPoolStopGatewaysExtraKey] = 5
			fake.configure(account)
			svc := rotationService(account)
			ctx, stop := svc.codexCookies.watchGatewayPoolPreparationPolicy(context.Background(), gwpoolTestIdentity, account)
			defer stop()
			svc.applyGatewayPoolPreparationPolicies(ctx, gwpoolTestIdentity)
			require.ErrorIs(t, context.Cause(ctx), errGatewayPoolStopThreshold)

			// Deterministic ordering: the high-threshold waiter has stopped, then
			// its lower-threshold sibling starts work before rotation confirms rest.
			switch mode {
			case "busy":
				finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
				defer finish()
			case "live":
				gwpoolEchoSeedVerified(t, svc, account)
			case "changed-during-confirmation":
				fake.onList = func() {
					finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
					finish()
				}
			case "replenished":
				for i := 1; i < 5; i++ {
					fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{Name: fmt.Sprintf("unified-%d", i), PairReady: true})
				}
			}
			failure := &UpstreamFailoverError{
				StatusCode: http.StatusServiceUnavailable, Reason: OpenAIGatewayPoolReason,
				GatewayPoolRotation: true, NextAccountAction: NextAccountStop,
			}
			group := int64(7)
			next := svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure, nil)
			require.True(t, failure.GatewayPoolRetry, "invalidated stop proof must not become a client failure")
			require.True(t, failure.SameAccountRetryOnly)
			require.False(t, failure.ShouldRetryNextAccount(), "do not rest shared work without a valid proof")
			require.NotNil(t, gatewayPoolWaitFrom(next))
		})
	}
}

type gatewayRestReadCountingRepo struct {
	gatewayRotationRepo
	rows, peers atomic.Int32
}

func (r *gatewayRestReadCountingRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	r.rows.Add(1)
	return r.gatewayRotationRepo.GetByID(ctx, id)
}

func (r *gatewayRestReadCountingRepo) FindByExtraField(ctx context.Context, key string, value any) ([]Account, error) {
	r.peers.Add(1)
	return r.gatewayRotationRepo.FindByExtraField(ctx, key, value)
}

// Every queued pool request asks; without a published rest or a pool-owned
// block only the row is read, outside the rest lock.
func TestGatewayPoolRestHandoffWithoutRestReadsOnlyTheRow(t *testing.T) {
	account := rotationAccount(1, 7)
	repo := &gatewayRestReadCountingRepo{gatewayRotationRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}}
	svc := withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: repo})
	require.Nil(t, svc.GatewayPoolRestHandoff(context.Background(), account))
	require.EqualValues(t, 1, repo.rows.Load())
	require.Zero(t, repo.peers.Load())
}

// A pool failure on a member that is not resting costs rotation no rest-lock
// read: the same row read and no peer scan as before the handoff existed.
func TestGatewayPoolRotationWithoutRestSkipsRestReads(t *testing.T) {
	account := rotationAccount(1, 7)
	repo := &gatewayRestReadCountingRepo{gatewayRotationRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}}
	svc := withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: repo})
	group := int64(7)
	failure := &UpstreamFailoverError{StatusCode: http.StatusBadGateway, Reason: OpenAIGatewayPoolReason, NextAccountAction: NextAccountStop}
	svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure, nil)
	require.EqualValues(t, 1, repo.rows.Load())
	require.Zero(t, repo.peers.Load())
}

// A superseded fetch loops back to the next-ticket boundary, which applies
// the stop threshold again before taking another ticket.
func TestGatewayPoolStopRechecksBeforeEveryTicketFetch(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-0"), 150)
	for i := 0; i < 5; i++ {
		fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{Name: fmt.Sprintf("unified-%d", i), PairReady: true})
	}
	account := fake.account(1)
	account.Extra[openAIGatewayPoolStopGatewaysExtraKey] = 5
	svc := &OpenAIGatewayService{}
	fake.beforeCookie = func() {
		// A manual clear supersedes this fetch, then a gateway starts cooling.
		svc.codexCookies.applyGatewayPoolCooldownReset(gwpoolTestIdentity, time.Now().UTC().Add(-time.Second), 0)
		svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-4")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(`{"model":"gpt-6-astra"}`))
	require.NoError(t, err)
	err = svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gatewayPoolProbeModelLuna,
		func(context.Context, string, string) (int, string, error) { return 200, "same", nil })
	require.ErrorIs(t, err, errGatewayPoolStopThreshold)
	require.EqualValues(t, 1, fake.hits.Load(), "no second ticket below the threshold")
}

// A waiter joining a worker that sleeps between pool refusals is checked
// before it joins, not at the worker's next poll.
func TestGatewayPoolStopFollowerStopsBeforeJoiningWaitingWorker(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-0"), 150)
	for i := 0; i < 4; i++ {
		fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{Name: fmt.Sprintf("unified-%d", i), PairReady: true})
	}
	fake.refuseStatus, fake.refuseCode, fake.refuseRetryAfter = http.StatusServiceUnavailable, gwpool.CodeNoLivePair, 3
	refused := make(chan struct{})
	var once sync.Once
	fake.beforeCookie = func() { once.Do(func() { close(refused) }) }
	leader, follower := fake.account(1), fake.account(2) // clones: thresholds 1 and 5
	follower.Extra[openAIGatewayPoolStopGatewaysExtraKey] = 5
	svc := &OpenAIGatewayService{}
	shoot := func(context.Context, string, string) (int, string, error) { return 200, "same", nil }
	request := func(ctx context.Context) *http.Request {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(`{"model":"gpt-6-astra"}`))
		require.NoError(t, err)
		return request
	}
	leaderCtx, stopLeader := context.WithTimeout(context.Background(), 10*time.Second)
	leaderRequest := request(leaderCtx)
	leaderDone := make(chan error, 1)
	go func() {
		leaderDone <- svc.gatewayPoolWarmUpWith(leaderRequest, leader, gwpoolTestIdentity, gatewayPoolProbeModelLuna, shoot)
	}()
	t.Cleanup(func() { stopLeader(); <-leaderDone })
	select {
	case <-refused:
	case <-leaderCtx.Done():
		t.Fatal("leader never asked for a ticket")
	}
	time.Sleep(100 * time.Millisecond) // the worker is now sleeping until its next poll
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	err := svc.gatewayPoolWarmUpWith(request(ctx), follower, gwpoolTestIdentity, gatewayPoolProbeModelLuna, shoot)
	require.ErrorIs(t, err, errGatewayPoolStopThreshold)
	require.Less(t, time.Since(started), 1500*time.Millisecond)
}
