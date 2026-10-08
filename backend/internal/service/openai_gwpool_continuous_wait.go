package service

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"
)

var errGatewayPoolPreparationOwnerChanged = errors.New("gateway preparation owner changed")

const (
	OpenAIGatewayPoolContinuousWaitKey = "openai_gwpool_continuous_wait"
	gatewayPoolWaitConfigPoll          = 5 * time.Second
	gatewayPoolAuthRefreshTimeout      = 35 * time.Second
)

// Only gateway shortages may wait indefinitely. Account health, request
// cancellation and each individual upstream operation retain their limits.
func (a *Account) GatewayPoolContinuousWaitEnabled() bool {
	return a != nil && a.UsesGatewayPool() && a.getExtraBool(OpenAIGatewayPoolContinuousWaitKey)
}

// Quality-protected traffic searches until eligible candidates are exhausted.
// Continuous wait additionally waits across exhaustion/cooldown instead of resting.
func (a *Account) GatewayPoolLongWaitEnabled() bool {
	return a != nil && a.UsesGatewayPool()
}

func gatewayPoolWaitHealth(account *Account) bool {
	if account == nil {
		return false
	}
	if account.GatewayPoolContinuousWaitEnabled() && gatewayPoolOwnsTempBlock(account) {
		copy := *account
		copy.TempUnschedulableUntil = nil
		return copy.IsSchedulable()
	}
	return account.IsSchedulable()
}

func gatewayPoolWaitTransportMatches(fresh, original *Account) bool {
	if !original.GatewayPoolLongWaitEnabled() {
		return true
	}
	if (fresh.ProxyID == nil) != (original.ProxyID == nil) {
		return false
	}
	if fresh.ProxyID != nil && *fresh.ProxyID != *original.ProxyID {
		return false
	}
	if fresh.Proxy != nil && original.Proxy != nil && fresh.Proxy.URL() != original.Proxy.URL() {
		return false
	}
	return (fresh.Proxy == nil) == (original.Proxy == nil)
}

// A request can wait longer than an access token's lifetime. Re-resolve only
// its credential just before dispatch, retaining the body/session/route and
// the same configured transport; this is not a business retry.
func (s *OpenAIGatewayService) refreshGatewayPoolWaitingAuth(request *http.Request, account *Account) error {
	wait := gatewayPoolWaitFrom(request.Context())
	if wait == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(request.Context(), gatewayPoolAuthRefreshTimeout)
	defer cancel()
	fresh, err := s.freshGatewayPoolPreparationAccount(ctx, account)
	if err != nil {
		return err
	}
	if !gatewayPoolWaitAccountMatches(fresh, account) {
		return errOpenAIGatewayPoolWarmUnverified
	}
	token, authMode, err := s.GetAccessToken(ctx, fresh)
	if err != nil {
		return err
	}
	if authMode == "oauth" && token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return nil
}

// Changing one row's policy must not publish a shared recovery tombstone: its
// ordinary-mode clones may still be resting on the same cooldown ledger.
func (s *OpenAIGatewayService) allowGatewayPoolContinuousWait(ctx context.Context, account *Account) (bool, error) {
	if !gatewayPoolWaitHealth(account) {
		return false, nil
	}
	if gatewayPoolOwnsTempBlock(account) && s.accountRepo != nil {
		repo, ok := s.accountRepo.(gatewayPoolClearRestRepository)
		if !ok {
			return false, errors.New("conditional gateway rest cleanup unavailable")
		}
		if err := repo.ClearGatewayPoolRest(ctx, account.ID, map[string]any{}); err != nil {
			return false, err
		}
	}
	return true, nil
}

func gatewayPoolContinuousRetry(err error) bool {
	if errors.Is(err, errGatewayPoolWarmAttemptsFinished) {
		return true
	}
	_, shortage := gatewayPoolRetryableShortage(err)
	return shortage
}

// A follower can outlive its preparation leader. Observe this waiter's own
// row even while shared work is in a bounded probe or using a different clone.
func (s *OpenAIGatewayService) continuousGatewayPoolContext(ctx context.Context, account *Account) (context.Context, context.CancelFunc) {
	work, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(gatewayPoolWaitConfigPoll)
		defer ticker.Stop()
		for {
			select {
			case <-work.Done():
				return
			case <-ticker.C:
				fresh, err := s.freshGatewayPoolPreparationAccount(work, account)
				if err != nil {
					if gatewayPoolIdentityFailure(err) {
						cancel(err)
						return
					}
					continue // fresh admission still blocks dispatch; a read outage is not a policy change
				}
				if !gatewayPoolWaitAccountMatches(fresh, account) {
					cancel(errOpenAIGatewayPoolWarmUnverified)
					return
				}
			}
		}
	}()
	return work, func() { cancel(context.Canceled); <-done }
}

func (s *OpenAIGatewayService) gatewayPoolPrepareUntilReady(
	ctx context.Context, request *http.Request, account *Account, identity, model string, shoot gatewayPoolWarmShooter,
) error {
	ctx, stopPolicy := s.codexCookies.watchGatewayPoolPreparationPolicy(ctx, identity, account)
	defer stopPolicy()
	wait := gatewayPoolWaitFrom(ctx)
	for {
		_, _, err := s.codexCookies.poolPrepare.do(ctx, identity, 0, func(work context.Context) (struct{}, error) {
			if budget := gatewayPoolWaitFrom(work); budget != nil {
				work = context.WithValue(work, gatewayPoolPreparationSleepKey{}, budget.sleep)
			}
			work = context.WithValue(work, gatewayPoolWaitKey{}, (*gatewayPoolWaitState)(nil))
			work, _ = withOpenAIGatewayPoolSink(work, nil)
			work = context.WithValue(work, gatewayPoolPreparationKey{}, true)
			preparation := request.Clone(work)
			preparation.Body, preparation.GetBody = nil, nil
			return struct{}{}, s.gatewayPoolPrepare(preparation, account, identity, model, shoot)
		})
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if errors.Is(err, errGatewayPoolPreparationOwnerChanged) {
			fresh, readErr := s.freshGatewayPoolPreparationAccount(ctx, account)
			if readErr != nil || !gatewayPoolWaitAccountMatches(fresh, account) {
				return errOpenAIGatewayPoolWarmUnverified
			}
			// Only the shared owner became invalid. A healthy follower starts
			// a new run with its own row/transport, never by reusing that owner.
			continue
		}
		if wait == nil || !wait.continuous || !gatewayPoolContinuousRetry(err) {
			return err
		}
		gap := gatewayPoolWaitDefaultGap
		if shortage, ok := gatewayPoolRetryableShortage(err); ok && shortage.RetryAfter > 0 {
			gap = shortage.RetryAfter
		}
		counter, _ := s.codexCookies.poolContinuousWaiters.LoadOrStore(identity, &atomic.Int32{})
		waiters, valid := counter.(*atomic.Int32)
		if !valid {
			return errors.New("gateway wait counter unavailable")
		}
		waiters.Add(1)
		retry, waitErr := s.waitGatewayPoolRetry(ctx, account, gap)
		waiters.Add(-1)
		if waitErr != nil {
			return waitErr
		}
		if !retry {
			return errOpenAIGatewayPoolWarmUnverified
		}
	}
}

func (s *openAICodexCookieStore) gatewayPoolContinuousWaiters(identity string) int {
	if value, ok := s.poolContinuousWaiters.Load(identity); ok {
		if count, valid := value.(*atomic.Int32); valid {
			return int(count.Load())
		}
	}
	return 0
}
