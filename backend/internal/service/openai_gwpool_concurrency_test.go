package service

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolSharedVerificationCallerCancellation(t *testing.T) {
	for _, cancelled := range []string{"leader", "follower"} {
		t.Run(cancelled, func(t *testing.T) {
			store := &openAICodexCookieStore{}
			account := gwpoolTestAccount(1)
			applied := OpenAIGatewayPoolApplied{Gateway: "unified-142", Version: "shared-version"}
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
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
			base := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, gwpoolWarmModel)
			leader, cancelLeader := context.WithCancel(base)
			defer cancelLeader()
			follower, cancelFollower := context.WithCancel(base)
			defer cancelFollower()
			type result struct {
				full bool
				err  error
			}
			call := func(ctx context.Context, done chan<- result) {
				full, _, _, _, err := store.gatewayPoolWarmVerdict(ctx, account, gwpoolTestIdentity, applied, "offline-cookie", 1, shoot)
				done <- result{full, err}
			}
			first, second := make(chan result, 1), make(chan result, 1)
			go call(leader, first)
			<-started
			go call(follower, second)
			require.Eventually(t, func() bool {
				store.poolWarm.mu.Lock()
				defer store.poolWarm.mu.Unlock()
				for _, call := range store.poolWarm.calls {
					if call.waiters == 2 {
						return true
					}
				}
				return false
			}, time.Second, time.Millisecond)
			waiting := first
			if cancelled == "leader" {
				cancelLeader()
			} else {
				cancelFollower()
				waiting = second
			}
			select {
			case got := <-waiting:
				require.ErrorIs(t, got.err, context.Canceled)
			case <-time.After(300 * time.Millisecond):
				t.Fatal("a cancelled waiter must return without waiting for the shared probe")
			}
			// The remaining caller must still be waiting for the one shared A/B.
			remaining := second
			if cancelled == "follower" {
				remaining = first
			}
			select {
			case got := <-remaining:
				t.Fatalf("one caller cancelled another caller's verification: %+v", got)
			case <-time.After(20 * time.Millisecond):
			}
			unblock()
			select {
			case got := <-remaining:
				require.NoError(t, got.err)
				require.True(t, got.full)
			case <-time.After(time.Second):
				t.Fatal("remaining caller did not receive the shared result")
			}
			require.EqualValues(t, 2, shots.Load(), "one A/B for both callers")
		})
	}
}

func TestGatewayPoolProbeFlightsCancelOnlyAfterLastWaiter(t *testing.T) {
	var flights gatewayPoolProbeFlights
	started, stopped := make(chan struct{}), make(chan struct{})
	work := func(ctx context.Context) (gatewayPoolProbeResult, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return gatewayPoolProbeResult{}, ctx.Err()
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelFirst()
	defer cancelSecond()
	done := make(chan error, 2)
	go func() { _, err := flights.do(firstCtx, "same", time.Second, work); done <- err }()
	<-started
	go func() { _, err := flights.do(secondCtx, "same", time.Second, work); done <- err }()
	require.Eventually(t, func() bool {
		flights.mu.Lock()
		defer flights.mu.Unlock()
		return flights.calls["same"] != nil && flights.calls["same"].waiters == 2
	}, time.Second, time.Millisecond)
	cancelFirst()
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-stopped:
		t.Fatal("work stopped with a live waiter")
	default:
	}
	cancelSecond()
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("abandoned probe continued after the last waiter left")
	}
}

func TestGatewayPoolProbeFlightsCallerCancellationReportsSent(t *testing.T) {
	var flights gatewayPoolProbeFlights
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	work := func(ctx context.Context) (gatewayPoolProbeResult, error) {
		close(started)
		<-ctx.Done()
		<-release // the abandoned transport is still unwinding
		return gatewayPoolProbeResult{}, ctx.Err()
	}
	gone, cancelGone := context.WithCancel(context.Background())
	cancelGone()
	got, err := flights.do(gone, "ticket", time.Second, work)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, got.sent, "a caller cancelled before joining must not release the ticket")

	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		got gatewayPoolProbeResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		got, err := flights.do(ctx, "ticket", time.Second, work)
		done <- outcome{got, err}
	}()
	<-started
	cancel()
	left := <-done
	require.ErrorIs(t, left.err, context.Canceled)
	require.True(t, left.got.sent, "the shared transport may already have sent")

	late, cancelLate := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelLate()
	got, err = flights.do(late, "ticket", time.Second, work)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, got.sent, "a caller waiting out an abandoned transport must not release the ticket")
}

func TestGatewayPoolSharedHistoryLoadSurvivesFirstCallerCancellation(t *testing.T) {
	account := gwpoolTestAccount(1)
	entered, release := make(chan struct{}), make(chan struct{})
	var loads atomic.Int32
	store := &openAICodexCookieStore{
		accountByID: func(context.Context, int64, ...string) (*Account, error) { return account, nil },
		historyByTag: func(ctx context.Context, _ string, _ ...string) ([]Account, error) {
			if loads.Add(1) == 1 {
				close(entered)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return nil, nil
			}
		},
	}
	first, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- store.hydrateGatewayPoolSharedHistory(first, account, gwpoolTestIdentity) }()
	<-entered
	go func() {
		secondDone <- store.hydrateGatewayPoolSharedHistory(context.Background(), account, gwpoolTestIdentity)
	}()
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	require.Eventually(t, func() bool { // the second caller joined the in-flight load
		store.poolHistoryLoad.mu.Lock()
		defer store.poolHistoryLoad.mu.Unlock()
		call := store.poolHistoryLoad.calls[tag]
		return call != nil && call.waiters == 2
	}, time.Second, time.Millisecond)
	cancelFirst()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	close(release)
	select {
	case err := <-secondDone:
		require.NoError(t, err, "the first caller's cancellation failed another caller's shared load")
	case <-time.After(time.Second):
		t.Fatal("the remaining caller never finished its shared load")
	}
	require.EqualValues(t, 1, loads.Load(), "both callers shared one peer read")
}

func TestGatewayPoolProgressDoesNotJumpToNewerActiveRequest(t *testing.T) {
	var tracker gatewayPoolProgressTracker
	first := tracker.start(1)
	gatewayPoolTestTried(&tracker, first, 4)
	tracker.update(first, "verifying", "first", false, false)
	second := tracker.start(1)
	gatewayPoolTestTried(&tracker, second, 2)
	tracker.update(second, "fetching", "", false, false)
	got := tracker.snapshot([]int64{1}, time.Now())[1]
	require.Equal(t, first.progress.RunID, got.RunID)
	require.Equal(t, 4, got.Attempt)
	require.Equal(t, 2, got.ActiveRequests)
	gatewayPoolTestTried(&tracker, first, 1)
	tracker.update(first, "cancelled", "", false, true)
	require.Equal(t, 4, first.progress.Attempt, "a late lower update must not rewind one run")
	got = tracker.snapshot([]int64{1}, time.Now())[1]
	require.Equal(t, second.progress.RunID, got.RunID)
	require.NotEqual(t, first.progress.RunID, got.RunID, "a different attempt count must carry a different run identity")
	require.Equal(t, 2, got.Attempt)
}

func TestGatewayPoolRecoverableProbeFailureClassification(t *testing.T) {
	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF, net.ErrClosed,
		&url.Error{Op: "Post", URL: "https://example.test", Err: syscall.ECONNRESET},
		errGatewayPoolProbeMissingState, &gatewayPoolProbeHTTPError{status: 503}} {
		require.True(t, gatewayPoolRetryableProbeError(err), "%v", err)
	}
	for _, err := range []error{errors.New("bad configuration"), &gatewayPoolProbeHTTPError{status: 401},
		&gatewayPoolProbeHTTPError{status: 403}, &gatewayPoolProbeHTTPError{status: 429}} {
		require.False(t, gatewayPoolRetryableProbeError(err), "%v", err)
	}
}

func TestGatewayPoolProbeInventoryIsRegisteredBeforeWorkerRuns(t *testing.T) {
	store := &openAICodexCookieStore{}
	ctx, cancel := context.WithCancel(context.Background())
	registered, started, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = store.poolWarm.do(ctx, "ticket", time.Second,
			func(context.Context) (gatewayPoolProbeResult, error) {
				close(started)
				<-release
				return gatewayPoolProbeResult{}, nil
			}, func() func() {
				finish := store.gatewayPoolInventoryOperation("identity")
				close(registered)
				cancel()
				return finish
			})
	}()
	<-registered
	<-done
	_, pending := store.gatewayPoolInventorySnapshot("identity")
	require.True(t, pending)
	<-started
	close(release)
	require.Eventually(t, func() bool {
		_, pending := store.gatewayPoolInventorySnapshot("identity")
		return !pending
	}, time.Second, time.Millisecond)
}
