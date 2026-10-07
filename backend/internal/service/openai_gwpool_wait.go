package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	openAIGatewayPoolWaitEnabledExtraKey = "openai_gwpool_auto_wait"
	openAIGatewayPoolWaitSecondsExtraKey = "openai_gwpool_max_wait_s"
	openAIGatewayPoolRecoveryExtraKey    = "openai_gwpool_prepare_retries"
	gatewayPoolRecoveryDefault           = 0
	gatewayPoolRecoveryMax               = 10
	gatewayPoolWaitDefaultSeconds        = 120
	gatewayPoolWaitMaxSeconds            = 3600
	gatewayPoolWaitDefaultGap            = 30 * time.Second
	gatewayPoolWaitMinGap                = time.Second
	gatewayPoolWaitMaxGap                = time.Minute
)

type gatewayPoolWaitKey struct{}
type gatewayPoolWaitWorkKey struct{}
type gatewayPoolWaitHolder struct {
	mu    sync.Mutex
	state *gatewayPoolWaitState
}

const gatewayPoolWaitGinKey = "openai_gwpool_wait_budget"

type gatewayPoolWaitState struct {
	mu       sync.Mutex
	max      time.Duration
	deadline time.Time
	waited   time.Duration
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
}

func (s *gatewayPoolWaitState) snapshot() (time.Duration, time.Time, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max, s.deadline, s.waited
}

// retry never refreshes the original request's absolute preparation deadline.
// A shared preparation has only per-operation deadlines and is cancelled when
// the last business waiter leaves, not when its first waiter's budget expires.
func (s *OpenAIGatewayService) waitGatewayPoolRetry(ctx context.Context, account *Account, gap time.Duration) (bool, error) {
	if shared, _ := ctx.Value(gatewayPoolPreparationKey{}).(bool); shared {
		fresh, err := s.freshGatewayPoolPreparationAccount(ctx, account)
		if err != nil || !gatewayPoolWaitAccountMatches(fresh, account) {
			return false, err
		}
		if gap <= 0 {
			gap = gatewayPoolWaitDefaultGap
		}
		gap = min(gap, gatewayPoolWaitMaxGap)
		if gap < gatewayPoolWaitMinGap {
			gap = gatewayPoolWaitMinGap
		}
		sleep, _ := ctx.Value(gatewayPoolPreparationSleepKey{}).(func(context.Context, time.Duration) error)
		if sleep == nil {
			sleep = gatewayPoolSleep
		}
		progress, _ := ctx.Value(gatewayPoolProgressRunKey{}).(*gatewayPoolProgressRun)
		if progress != nil {
			s.codexCookies.poolProgress.update(progress, "waiting", 0, "", false, false)
		}
		if err := sleep(ctx, gap); err != nil {
			return false, err
		}
		if progress != nil {
			s.codexCookies.poolProgress.update(progress, "fetching", 0, "", false, false)
		}
		fresh, err = s.freshGatewayPoolPreparationAccount(ctx, account)
		return err == nil && gatewayPoolWaitAccountMatches(fresh, account), err
	}
	state := gatewayPoolWaitFrom(ctx)
	if state == nil {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if remaining, ok := ctx.Value(gatewayPoolWaitWorkKey{}).(func() time.Duration); ok && remaining() <= 0 {
		return false, context.DeadlineExceeded
	}
	fresh, err := s.freshGatewayPoolPreparationAccount(ctx, account)
	if err != nil || !gatewayPoolWaitAccountMatches(fresh, account) {
		return false, err
	}
	state.mu.Lock()
	maxWait := min(state.max, fresh.gatewayPoolMaxWait())
	now := state.now()
	if state.deadline.IsZero() {
		state.deadline = now.Add(maxWait)
	}
	remaining := min(state.deadline.Sub(now), maxWait-state.waited)
	state.mu.Unlock()
	if remaining <= 0 {
		return false, nil
	}
	if gap <= 0 {
		gap = gatewayPoolWaitDefaultGap
	}
	if gap < gatewayPoolWaitMinGap {
		gap = gatewayPoolWaitMinGap
	}
	gap = min(gap, gatewayPoolWaitMaxGap, remaining)
	before := state.now()
	sleepErr := state.sleep(ctx, gap)
	state.mu.Lock()
	state.waited += state.now().Sub(before)
	expired := !state.now().Before(state.deadline) || state.waited >= maxWait
	state.mu.Unlock()
	if sleepErr != nil || expired {
		return false, sleepErr
	}
	fresh, err = s.freshGatewayPoolPreparationAccount(ctx, account)
	return err == nil && gatewayPoolWaitAccountMatches(fresh, account), err
}

func gatewayPoolWaitSeconds(raw any) (int, bool) {
	return gatewayPoolInteger(raw, gatewayPoolWaitMaxSeconds)
}

func gatewayPoolInteger(raw any, limit int) (int, bool) {
	var value float64
	switch n := raw.(type) {
	case int:
		value = float64(n)
	case int64:
		value = float64(n)
	case float64:
		value = n
	case json.Number:
		var err error
		value, err = n.Float64()
		if err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value != math.Trunc(value) ||
		value < 1 || value > float64(limit) {
		return 0, false
	}
	return int(value), true
}

func (a *Account) gatewayPoolMaxWait() time.Duration {
	if a == nil || !a.UsesGatewayPool() {
		return 0
	}
	seconds := gatewayPoolWaitDefaultSeconds
	if raw, exists := a.Extra[openAIGatewayPoolWaitSecondsExtraKey]; exists && raw != nil {
		var ok bool
		seconds, ok = gatewayPoolWaitSeconds(raw)
		if !ok {
			return 0 // bad runtime data must never create an unbounded retry loop
		}
	}
	return time.Duration(seconds) * time.Second
}

func (a *Account) gatewayPoolPreparationRecoveries() int {
	if a == nil {
		return gatewayPoolRecoveryDefault
	}
	raw, exists := a.Extra[openAIGatewayPoolRecoveryExtraKey]
	if !exists || raw == nil {
		return gatewayPoolRecoveryDefault
	}
	if value, ok := gatewayPoolCooldownResetHours(raw); ok && value <= gatewayPoolRecoveryMax {
		return value
	}
	return 0
}

// Check only before dispatch. Do not put this deadline on the request that owns
// a successful business response body: inference can legitimately outlive it.
func gatewayPoolPreparationDeadlineError(ctx context.Context) error {
	if wait := gatewayPoolWaitFrom(ctx); wait != nil {
		_, deadline, _ := wait.snapshot()
		if !deadline.IsZero() && !wait.now().Before(deadline) {
			return errOpenAIGatewayPoolWarmExhausted
		}
	}
	return nil
}

func gatewayPoolWaitFrom(ctx context.Context) *gatewayPoolWaitState {
	state, _ := ctx.Value(gatewayPoolWaitKey{}).(*gatewayPoolWaitState)
	return state
}

func gatewayPoolSleep(ctx context.Context, gap time.Duration) error {
	timer := time.NewTimer(gap)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *OpenAIGatewayService) gatewayPoolWaitContext(ctx context.Context, account *Account) context.Context {
	if account == nil || !account.IsOpenAIOAuthLike() || gatewayPoolWaitFrom(ctx) != nil {
		return ctx
	}
	fresh, err := s.freshGatewayPoolPreparationAccount(ctx, account)
	if err != nil || fresh == nil || fresh.gatewayPoolMaxWait() == 0 {
		return ctx
	}
	newState := &gatewayPoolWaitState{
		max: fresh.gatewayPoolMaxWait(), deadline: time.Now().Add(fresh.gatewayPoolMaxWait()),
		now: time.Now, sleep: gatewayPoolSleep,
	}
	if sink := openAIGatewayPoolSinkFrom(ctx); sink != nil && sink.waitBudget != nil {
		holder := sink.waitBudget
		holder.mu.Lock()
		if holder.state == nil {
			holder.state = newState
		}
		newState = holder.state
		holder.mu.Unlock()
	}
	return context.WithValue(ctx, gatewayPoolWaitKey{}, newState)
}

func gatewayPoolRetryableShortage(err error) (*gwpool.PoolError, bool) {
	var poolErr *gwpool.PoolError
	if !errors.As(err, &poolErr) {
		return nil, false
	}
	switch poolErr.Code {
	case gwpool.CodeNoGateway, gwpool.CodeNoLivePair, gwpool.CodeAllCooling:
		return poolErr, true
	default:
		return nil, false
	}
}

func gatewayPoolWaitAccountMatches(fresh, original *Account) bool {
	return fresh != nil && fresh.IsSchedulable() && fresh.gatewayPoolMaxWait() > 0 &&
		fresh.gatewayPoolBaseURL() == original.gatewayPoolBaseURL() &&
		fresh.gatewayPoolConsumerKey() == original.gatewayPoolConsumerKey() &&
		fresh.getExtraBool(openAIGatewayPoolMemberIsolationKey) == original.getExtraBool(openAIGatewayPoolMemberIsolationKey) &&
		codexAccountIdentityNamespace(fresh) == codexAccountIdentityNamespace(original)
}

func gatewayPoolFetchTimeoutForContext(ctx context.Context, account *Account) time.Duration {
	timeout := account.gatewayPoolFetchTimeout()
	state := gatewayPoolWaitFrom(ctx)
	if state == nil {
		return timeout
	}
	if remaining, ok := ctx.Value(gatewayPoolWaitWorkKey{}).(func() time.Duration); ok {
		timeout = min(timeout, remaining())
	}
	maxWait, deadline, waited := state.snapshot()
	if !deadline.IsZero() {
		timeout = min(timeout, deadline.Sub(state.now()), maxWait-waited)
	}
	return timeout
}

// Only repeats ticket acquisition, BEFORE business transmission. HTTP/auth,
// model verdicts and ambiguous business transport errors never reach this loop.
func (s *OpenAIGatewayService) attachGatewayPoolRouteWithWait(
	ctx context.Context, account *Account, rawURL string, headers http.Header,
) (func(), error) {
	state := gatewayPoolWaitFrom(ctx)
	shared, _ := ctx.Value(gatewayPoolPreparationKey{}).(bool)
	workRemaining, _ := ctx.Value(gatewayPoolWaitWorkKey{}).(func() time.Duration)
	waited := false
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if workRemaining != nil && workRemaining() <= 0 {
			return nil, context.DeadlineExceeded
		}
		release, err := s.codexCookies.AttachRoute(ctx, account, rawURL, headers)
		if err == nil && waited {
			fresh, readErr := s.codexCookies.freshGatewayPoolAccount(ctx, account)
			if readErr != nil || !gatewayPoolWaitAccountMatches(fresh, account) {
				gatewayPoolReleaseUnsent(release)
				return nil, fmt.Errorf("%w: account configuration changed during ticket wait", gwpool.ErrPool)
			}
		}
		poolErr, shortage := gatewayPoolRetryableShortage(err)
		if err == nil || (state == nil && !shared) || !shortage || ctx.Err() != nil {
			return release, err
		}
		// Fresh exhaustion is actionable immediately. Waiting on a dead queue
		// for every request would prevent the normal account-rest transition.
		if shared && s.gatewayPoolNoRemainingRoutes(ctx, account) {
			return release, errGatewayPoolWarmAttemptsFinished
		}
		if workRemaining != nil && workRemaining() <= 0 {
			return release, context.DeadlineExceeded
		}
		retry, sleepErr := s.waitGatewayPoolRetry(ctx, account, poolErr.RetryAfter)
		waited = true
		if sleepErr != nil {
			return release, sleepErr
		}
		if !retry {
			return release, err
		}
	}
}
