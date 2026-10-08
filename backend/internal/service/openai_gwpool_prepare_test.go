package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolPreparationSharesWholeRun(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	svc := &OpenAIGatewayService{}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var shots atomic.Int32
	shoot := func(ctx context.Context, _, _ string) (int, string, error) {
		if shots.Add(1) == 1 {
			close(started)
			select {
			case <-ctx.Done():
				return 0, "", ctx.Err()
			case <-release:
			}
		}
		return http.StatusOK, "same-state", nil
	}
	call := func(ctx context.Context, done chan<- error) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
		ctx, _ = withOpenAIGatewayPoolSink(request.Context(), nil)
		done <- svc.gatewayPoolWarmUpWith(request.WithContext(ctx), account, gwpoolTestIdentity, gwpoolWarmModel, shoot)
	}
	firstCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go call(firstCtx, first)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("preparation did not start")
	}
	go call(context.Background(), second)
	require.Eventually(t, func() bool {
		svc.codexCookies.poolPrepare.mu.Lock()
		defer svc.codexCookies.poolPrepare.mu.Unlock()
		call := svc.codexCookies.poolPrepare.calls[gwpoolTestIdentity]
		return call != nil && call.waiters == 2
	}, time.Second, time.Millisecond)
	svc.codexCookies.poolProgress.mu.Lock()
	runs := svc.codexCookies.poolProgress.next
	svc.codexCookies.poolProgress.mu.Unlock()
	require.EqualValues(t, 1, runs, "one preparation run, not one loop per business waiter")
	cancel()
	require.ErrorIs(t, <-first, context.Canceled)
	unblock()
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("remaining waiter lost its shared preparation")
	}
	require.EqualValues(t, 2, shots.Load())
}

func TestGatewayPoolSharedWorkIndependentDeadlinesAndLastWaiter(t *testing.T) {
	var group gatewayPoolSharedWork[int]
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	work := func(ctx context.Context) (int, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-ctx.Done():
			close(cancelled)
			<-release // simulate a transport still unwinding
			return 0, ctx.Err()
		case <-release:
			return 7, nil
		}
	}
	first, cancelFirst := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancelFirst()
	second, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	errors := make(chan error, 3)
	go func() { _, _, err := group.do(first, "identity", 0, work); errors <- err }()
	<-started
	go func() { _, _, err := group.do(second, "identity", 0, work); errors <- err }()
	require.Eventually(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return group.calls["identity"].waiters == 2
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, <-errors, context.DeadlineExceeded)
	select {
	case <-cancelled:
		t.Fatal("the first waiter's deadline cancelled the second waiter's work")
	default:
	}
	cancelSecond()
	require.ErrorIs(t, <-errors, context.Canceled)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("last waiter did not cancel work")
	}
	third, cancelThird := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelThird()
	go func() { _, _, err := group.do(third, "identity", 0, work); errors <- err }()
	require.ErrorIs(t, <-errors, context.DeadlineExceeded)
	require.EqualValues(t, 1, calls.Load(), "an abandoned transport must finish before another starts")
	close(release)
	require.Eventually(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return len(group.calls) == 0
	}, time.Second, time.Millisecond)
}

func TestGatewayPoolCandidateQueueRetainsOrderAndRejoinsAtTail(t *testing.T) {
	var queue gatewayPoolCandidateQueue
	gateways := func(names ...string) []gwpool.Gateway {
		var out []gwpool.Gateway
		for _, name := range names {
			out = append(out, gwpool.Gateway{Name: name, PairReady: true})
		}
		return out
	}
	require.Equal(t, "a", queue.pick(gateways("a", "b", "c", "a")))
	require.Equal(t, "b", queue.pick(gateways("c", "b")))      // a enters cooldown
	require.Equal(t, "c", queue.pick(gateways("a", "b", "c"))) // a returns, after existing names
	require.Equal(t, "b", queue.pick(gateways("a", "b")))
	require.Equal(t, "a", queue.pick(gateways("b", "a")))
	queue.reset()
	require.Equal(t, "b", queue.pick(gateways("b", "a")))
	require.Empty(t, queue.pick(nil))
}

func TestGatewayPoolPreparationSettingsDefaultsAndRecoveryBounds(t *testing.T) {
	account := gwpoolTestAccount(1)
	require.True(t, account.GatewayPoolLongWaitEnabled())
	account.Extra[openAIGatewayPoolWaitEnabledExtraKey] = false
	require.True(t, account.GatewayPoolLongWaitEnabled(), "obsolete auto-wait switch does not disable queue waiting")
	require.Zero(t, account.gatewayPoolPreparationRecoveries())
	for _, value := range []any{0, float64(0), int64(0)} {
		account.Extra[openAIGatewayPoolRecoveryExtraKey] = value
		require.Zero(t, account.gatewayPoolPreparationRecoveries())
	}
	account.Extra[openAIGatewayPoolRecoveryExtraKey] = 2
	require.Equal(t, 2, account.gatewayPoolPreparationRecoveries())
	for _, value := range []any{-1, 1.5, "2", 11} {
		account.Extra[openAIGatewayPoolRecoveryExtraKey] = value
		require.Zero(t, account.gatewayPoolPreparationRecoveries())
	}
}

func TestGatewayPoolPreparationDoesNotImposeDeadlineOnSuccessfulBusiness(t *testing.T) {
	svc, repo, _, account, request, _ := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	account.Credentials["access_token"] = "offline"
	repo.account = *account
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "probe-state"}, {status: http.StatusOK}, {status: http.StatusOK},
	}}
	var businessCtx context.Context
	upstream.beforeReply = func(request *http.Request, call int) error {
		if call == 3 {
			businessCtx = request.Context()
		}
		return nil
	}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.NotNil(t, businessCtx)
	require.NoError(t, businessCtx.Err())
	_, bounded := businessCtx.Deadline()
	require.False(t, bounded, "the preparation deadline must not be placed on the business stream")
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Len(t, upstream.sentBodies, 3)
}

func TestGatewayPoolPreparationContinuesPastLegacyTicketLimit(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	for i := 1; i <= 10; i++ {
		fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{
			Name: fmt.Sprintf("unified-%d", 140+i), PairReady: true,
		})
	}
	fake.cookieForHit = func(hit int64) string {
		return gwpoolTestPairCookie(t, fmt.Sprintf("unified-%d", 140+hit))
	}
	account := fake.account(1)
	account.Extra[openAIGatewayPoolWarmTicketsExtraKey] = 2 // obsolete saved setting
	shooter := &gwpoolWarmShooter{}
	for i := 0; i < 9; i++ {
		shooter.replies = append(shooter.replies,
			gwpoolWarmReply{status: http.StatusOK, minted: "state"},
			gwpoolWarmReply{status: http.StatusOK, minted: "different-state"})
	}
	svc := &OpenAIGatewayService{}
	require.NoError(t, gwpoolWarmRun(t, svc, account, shooter))
	require.Len(t, shooter.shots, 20, "the tenth candidate is full; a saved 2/5/8 limit must not truncate the queue")
}
