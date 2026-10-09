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
	openAIGatewayPoolRecoveryExtraKey = "openai_gwpool_prepare_retries"
	gatewayPoolRecoveryDefault        = 0
	gatewayPoolRecoveryMax            = 10
	gatewayPoolWaitDefaultGap         = 30 * time.Second
	gatewayPoolWaitMinGap             = time.Second
	gatewayPoolWaitMaxGap             = time.Minute
)

type gatewayPoolWaitKey struct{}
type gatewayPoolWaitHolder struct {
	mu    sync.Mutex
	state *gatewayPoolWaitState
}

const gatewayPoolWaitGinKey = "openai_gwpool_wait_budget"

type gatewayPoolWaitState struct {
	accountID  int64
	identity   string
	continuous bool
	sleep      func(context.Context, time.Duration) error
}

// Shared preparation has only per-operation deadlines and is cancelled when
// the last business waiter leaves. Each waiter retains its own caller context.
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
	fresh, err := s.freshGatewayPoolPreparationAccount(ctx, account)
	if err != nil {
		if gatewayPoolIdentityFailure(err) {
			return false, err
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		// A read outage blocks sending, not waiting; dispatch still needs a fresh read.
	} else if !gatewayPoolWaitAccountMatches(fresh, account) {
		return false, nil
	}
	if gap <= 0 {
		gap = gatewayPoolWaitDefaultGap
	}
	gap = min(gap, gatewayPoolWaitMaxGap)
	if gap < gatewayPoolWaitMinGap {
		gap = gatewayPoolWaitMinGap
	}
	if err := state.sleep(ctx, gap); err != nil {
		return false, err
	}
	fresh, err = s.freshGatewayPoolPreparationAccount(ctx, account)
	if gatewayPoolIdentityFailure(err) {
		return false, err
	}
	if err != nil && ctx.Err() == nil {
		return true, nil
	}
	return err == nil && gatewayPoolWaitAccountMatches(fresh, account), err
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
	if err != nil {
		// This snapshot supplies waiting policy only. Every dispatch still
		// requires fresh identity/health/configuration admission.
		fresh = account
	}
	if !fresh.GatewayPoolLongWaitEnabled() {
		return ctx
	}
	identity, identityErr := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if identityErr != nil {
		return ctx // preparation reports the identity error; never invent a shared domain
	}
	newState := &gatewayPoolWaitState{
		accountID: fresh.ID, identity: identity,
		sleep: gatewayPoolSleep,
	}
	newState.continuous = fresh.GatewayPoolContinuousWaitEnabled()
	if sink := openAIGatewayPoolSinkFrom(ctx); sink != nil && sink.waitBudget != nil {
		holder := sink.waitBudget
		holder.mu.Lock()
		if holder.state == nil || holder.state.accountID != newState.accountID ||
			holder.state.identity != newState.identity || holder.state.continuous != newState.continuous {
			holder.state = newState
		}
		newState = holder.state
		holder.mu.Unlock()
	}
	return context.WithValue(ctx, gatewayPoolWaitKey{}, newState)
}

func gatewayPoolRetryableShortage(err error) (*gwpool.PoolError, bool) {
	if errors.Is(err, gwpool.ErrCatalogUnavailable) {
		return &gwpool.PoolError{}, true
	}
	var poolErr *gwpool.PoolError
	if !errors.As(err, &poolErr) {
		return nil, false
	}
	switch poolErr.Code {
	case gwpool.CodeNoGateway, gwpool.CodeNoLivePair, gwpool.CodeAllCooling, gwpool.CodeConsumerRateLimited:
		return poolErr, true
	default:
		return nil, false
	}
}

func gatewayPoolWaitAccountMatches(fresh, original *Account) bool {
	return fresh != nil && gatewayPoolWaitHealth(fresh) && fresh.GatewayPoolLongWaitEnabled() &&
		fresh.GatewayPoolContinuousWaitEnabled() == original.GatewayPoolContinuousWaitEnabled() &&
		gatewayPoolWaitTransportMatches(fresh, original) &&
		fresh.gatewayPoolBaseURL() == original.gatewayPoolBaseURL() &&
		fresh.gatewayPoolConsumerKey() == original.gatewayPoolConsumerKey() &&
		codexAccountIdentityNamespace(fresh) == codexAccountIdentityNamespace(original)
}

// Only repeats ticket acquisition, BEFORE business transmission. HTTP/auth,
// model verdicts and ambiguous business transport errors never reach this loop.
func (s *OpenAIGatewayService) attachGatewayPoolRouteWithWait(
	ctx context.Context, account *Account, rawURL string, headers http.Header,
) (func(), error) {
	state := gatewayPoolWaitFrom(ctx)
	shared, _ := ctx.Value(gatewayPoolPreparationKey{}).(bool)
	waited := false
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		release, err := s.codexCookies.AttachRoute(ctx, account, rawURL, headers)
		if err == nil && waited {
			fresh, readErr := s.codexCookies.freshGatewayPoolAccount(ctx, account)
			if readErr != nil || !gatewayPoolWaitAccountMatches(fresh, account) {
				gatewayPoolCleanupUnsent(release)
				return nil, fmt.Errorf("%w: account configuration changed during ticket wait", gwpool.ErrPool)
			}
		}
		poolErr, shortage := gatewayPoolRetryableShortage(err)
		if err == nil || (state == nil && !shared) || !shortage || ctx.Err() != nil {
			return release, err
		}
		// Fresh exhaustion is actionable immediately. Waiting on a dead queue
		// for every request would prevent the normal account-rest transition.
		if shared && poolErr.Code != gwpool.CodeConsumerRateLimited {
			if identity, ok := ctx.Value(gatewayPoolPreparationIdentityKey{}).(string); ok {
				s.applyGatewayPoolPreparationPolicies(ctx, identity)
			}
			if s.gatewayPoolNoRemainingRoutes(ctx, account) {
				return release, errGatewayPoolWarmAttemptsFinished
			}
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
