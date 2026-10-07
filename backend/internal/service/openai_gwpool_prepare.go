package service

import (
	"context"
	"errors"
	"net/http"
)

type gatewayPoolPreparationKey struct{}
type gatewayPoolPreparationSleepKey struct{}

func (s *openAICodexCookieStore) gatewayPoolPreparationWaiters(identity string) int {
	s.poolPrepare.mu.Lock()
	defer s.poolPrepare.mu.Unlock()
	if call := s.poolPrepare.calls[identity]; call != nil && !call.abandoned {
		return call.waiters
	}
	return 0
}

func (s *OpenAIGatewayService) freshGatewayPoolPreparationAccount(ctx context.Context, account *Account) (*Account, error) {
	readCtx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
	defer cancel()
	return s.codexCookies.freshGatewayPoolAccount(readCtx, account)
}

// Only the preparation worker owns progress, retry counts and verdict writes.
// Its sink is not any business request's sink: late probes cannot overwrite a
// completed/cancelled request's route evidence.
func (s *OpenAIGatewayService) gatewayPoolWarmUpWith(
	request *http.Request, account *Account, identity, model string, shoot gatewayPoolWarmShooter,
) error {
	ctx := s.gatewayPoolWaitContext(request.Context(), account)
	wait := gatewayPoolWaitFrom(ctx)
	if wait == nil {
		return errOpenAIGatewayPoolWarmUnverified
	}
	wait.mu.Lock()
	if wait.deadline.IsZero() {
		wait.deadline = wait.now().Add(wait.max)
	}
	deadline := wait.deadline
	wait.mu.Unlock()
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	_, _, err := s.codexCookies.poolPrepare.do(ctx, identity, 0, func(work context.Context) (struct{}, error) {
		if wait := gatewayPoolWaitFrom(work); wait != nil {
			work = context.WithValue(work, gatewayPoolPreparationSleepKey{}, wait.sleep)
		}
		work = context.WithValue(work, gatewayPoolWaitKey{}, (*gatewayPoolWaitState)(nil))
		work = context.WithValue(work, gatewayPoolWaitWorkKey{}, struct{}{})
		work, _ = withOpenAIGatewayPoolSink(work, nil)
		work = context.WithValue(work, gatewayPoolPreparationKey{}, true)
		// No client body or mutable early reservation belongs to shared work.
		if account.gatewayPoolEarlyEnabled() {
			work = context.WithValue(work, gatewayPoolEarlyIntentKey{}, &gatewayPoolEarlyIntent{
				ctx: work, model: gatewayPoolProbeModelLuna,
			})
		}
		preparation := request.Clone(work)
		preparation.Body, preparation.GetBody = nil, nil
		return struct{}{}, s.gatewayPoolPrepare(preparation, account, identity, model, shoot)
	})
	if errors.Is(err, context.DeadlineExceeded) && request.Context().Err() == nil {
		return errOpenAIGatewayPoolWarmExhausted
	}
	return err
}
