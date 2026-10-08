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
	fresh, err := s.codexCookies.freshGatewayPoolAccount(readCtx, account)
	if err != nil && ctx.Err() == nil {
		return nil, &gatewayPoolAttemptRetryError{cause: err}
	}
	if err != nil || fresh == nil {
		return fresh, err
	}
	expected, _ := ctx.Value(gatewayPoolPreparationIdentityKey{}).(string)
	if wait := gatewayPoolWaitFrom(ctx); expected == "" && wait != nil && wait.accountID == fresh.ID {
		expected = wait.identity
	}
	if expected != "" {
		current, identityErr := s.codexCookies.gatewayPoolIdentity(readCtx, fresh)
		if identityErr != nil {
			if gatewayPoolIdentityFailure(identityErr) {
				return nil, identityErr
			}
			return nil, &gatewayPoolAttemptRetryError{cause: identityErr}
		}
		if current != expected {
			return nil, errGatewayPoolPreparationOwnerChanged
		}
	}
	return fresh, err
}

func gatewayPoolIdentityFailure(err error) bool {
	return errors.Is(err, errGatewayPoolMemberIdentity) || errors.Is(err, errGatewayPoolPreparationOwnerChanged)
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
	// A real 429 retry deadline bounds this waiter's preparation, never the
	// shared worker's other waiters or a successful business response stream.
	if retry := gatewayPoolRetryOnlyFrom(ctx); !retry.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, retry.deadline)
		defer cancel()
	}
	ctx, cancel := s.continuousGatewayPoolContext(ctx, account)
	defer cancel()
	err := s.gatewayPoolPrepareUntilReady(ctx, request, account, identity, model, shoot)
	if failure := GatewayPoolRetryFailure(request.Context()); failure != nil {
		return failure
	}
	return err
}
