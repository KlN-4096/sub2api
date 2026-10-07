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
	max      time.Duration
	deadline time.Time
	waited   time.Duration
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
}

func gatewayPoolWaitSeconds(raw any) (int, bool) {
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
		value < 1 || value > gatewayPoolWaitMaxSeconds {
		return 0, false
	}
	return int(value), true
}

func (a *Account) gatewayPoolMaxWait() time.Duration {
	if a == nil || !a.UsesGatewayPool() || !a.getExtraBool(openAIGatewayPoolWaitEnabledExtraKey) {
		return 0
	}
	seconds := gatewayPoolWaitDefaultSeconds
	if raw, exists := a.Extra[openAIGatewayPoolWaitSecondsExtraKey]; exists {
		var ok bool
		seconds, ok = gatewayPoolWaitSeconds(raw)
		if !ok {
			return 0 // bad runtime data must never create an unbounded retry loop
		}
	}
	return time.Duration(seconds) * time.Second
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
	fresh, err := s.codexCookies.freshGatewayPoolAccount(ctx, account)
	if err != nil || fresh == nil || fresh.gatewayPoolMaxWait() == 0 {
		return ctx
	}
	newState := &gatewayPoolWaitState{
		max: fresh.gatewayPoolMaxWait(), now: time.Now, sleep: gatewayPoolSleep,
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
	if !state.deadline.IsZero() {
		timeout = min(timeout, state.deadline.Sub(state.now()), state.max-state.waited)
	}
	return timeout
}

// Only repeats ticket acquisition, BEFORE business transmission. HTTP/auth,
// model verdicts and ambiguous business transport errors never reach this loop.
func (s *OpenAIGatewayService) attachGatewayPoolRouteWithWait(
	ctx context.Context, account *Account, rawURL string, headers http.Header,
) (func(), error) {
	state := gatewayPoolWaitFrom(ctx)
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
		if err == nil || state == nil || !shortage || ctx.Err() != nil {
			return release, err
		}
		if workRemaining != nil && workRemaining() <= 0 {
			return release, context.DeadlineExceeded
		}
		fresh, readErr := s.codexCookies.freshGatewayPoolAccount(ctx, account)
		// Do not revive a disabled account or replay stale credentials after a
		// config/identity change while waiting.
		if readErr != nil || !gatewayPoolWaitAccountMatches(fresh, account) {
			return release, err
		}
		maxWait := min(state.max, fresh.gatewayPoolMaxWait())
		now := state.now()
		if state.deadline.IsZero() {
			state.deadline = now.Add(maxWait)
		}
		remaining := min(state.deadline.Sub(now), maxWait-state.waited)
		if remaining <= 0 {
			return release, err
		}
		gap := poolErr.RetryAfter
		if gap <= 0 {
			gap = gatewayPoolWaitDefaultGap
		}
		gap = min(gap, gatewayPoolWaitMaxGap)
		if gap < gatewayPoolWaitMinGap {
			gap = gatewayPoolWaitMinGap
		}
		if gap > remaining {
			gap = remaining
		}
		before := state.now()
		sleepErr := state.sleep(ctx, gap)
		waited = true
		state.waited += state.now().Sub(before)
		if sleepErr != nil {
			return release, sleepErr
		}
		if !state.now().Before(state.deadline) {
			return release, err
		}
		// Re-read before retry, not just after the next failed fetch.
		fresh, readErr = s.codexCookies.freshGatewayPoolAccount(ctx, account)
		if readErr != nil || !gatewayPoolWaitAccountMatches(fresh, account) {
			return release, err
		}
	}
}
