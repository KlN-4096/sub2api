package service

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/chatgptcookies"
	"github.com/gin-gonic/gin"
)

const gatewayPoolTransientAttemptsPerTicket = 2

// Only classified, pre-delivery failures opt into replay. WarmUnverified alone
// also covers authentication/configuration changes and must never mean retry.
type gatewayPoolAttemptRetryError struct {
	cause  error
	retire bool
}

func (e *gatewayPoolAttemptRetryError) Error() string { return e.cause.Error() }
func (e *gatewayPoolAttemptRetryError) Unwrap() []error {
	return []error{errOpenAIGatewayPoolWarmUnverified, e.cause}
}

func retryGatewayPoolAttempt(cause error) error {
	return &gatewayPoolAttemptRetryError{cause: cause, retire: true}
}

func gatewayPoolDegradedAttemptError(ctx context.Context, marked bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if marked {
		return errOpenAIGatewayPoolRouteDegraded
	}
	// Another request may have already invalidated/replaced this version
	// between confirmation and CAS. Rejoin preparation without retiring the
	// winning replacement or turning that race into a client-visible failure.
	return &gatewayPoolAttemptRetryError{cause: errOpenAIGatewayPoolWarmUnverified}
}

// Keep the downstream cancellation for pool preparation and internal replay.
// Other providers retain their existing detached billing/draining behavior.
func gatewayPoolUpstreamContext(ctx context.Context, account *Account) (context.Context, context.CancelFunc) {
	if account.UsesGatewayPool() {
		return ctx, func() {}
	}
	return detachUpstreamContext(ctx)
}

func gatewayPoolBusinessRetryStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func (s *OpenAIGatewayService) retireGatewayPoolBusinessAttempt(request *http.Request, account *Account) bool {
	applied := openAIGatewayPoolSinkFrom(request.Context()).snapshot()
	if applied.AccountID != account.ID || applied.Version == "" {
		return false
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(request.Context(), account)
	if err != nil || !s.codexCookies.retireGatewayPoolFailedProbe(identity, applied) {
		return false
	}
	s.noteWarmVerdict(request, account, applied, "", false)
	return true
}

// A busy WS turn keeps the same member and caller cancellation as subsequent
// ticket preparation. No extra slot or independent total wait budget is added.
func (s *OpenAIGatewayService) WaitGatewayPoolCapacity(ctx context.Context, c *gin.Context, account *Account) (bool, error) {
	if !account.UsesGatewayPool() {
		return false, nil
	}
	holder := &gatewayPoolWaitHolder{}
	if c != nil {
		if value, ok := c.Get(gatewayPoolWaitGinKey); ok {
			if saved, valid := value.(*gatewayPoolWaitHolder); valid {
				holder = saved
			}
		} else {
			c.Set(gatewayPoolWaitGinKey, holder)
		}
	}
	ctx = context.WithValue(ctx, gatewayPoolWaitKey{}, (*gatewayPoolWaitState)(nil))
	ctx = context.WithValue(ctx, openAIGatewayPoolSinkCtxKey{}, &openAIGatewayPoolSink{waitBudget: holder})
	return s.waitGatewayPoolRetry(s.gatewayPoolWaitContext(ctx, account), account, gatewayPoolWaitMinGap)
}

// This boundary owns no downstream writer. A response is returned only after
// verification; failed attempts can therefore be replaced without splicing
// streams or leaking a discarded response's IDs, state or tool calls.
func (s *OpenAIGatewayService) doOpenAIUpstream(request *http.Request, proxyURL string, account *Account) (response *http.Response, responseErr error) {
	pool := request != nil && request.URL != nil && account.UsesGatewayPool() &&
		chatgptcookies.IsChatGPTURL(request.URL) && request.URL.Path == openAIGatewayPoolInferencePath
	if !pool {
		return s.doOpenAIUpstreamAttempt(request, proxyURL, account)
	}
	ctx, finish := s.beginGatewayPoolUsageRequest(request.Context(), account)
	defer func() {
		if finish == nil {
			return
		}
		if response != nil && response.Body != nil && responseErr == nil {
			response.Body = &gatewayPoolUsageBody{ReadCloser: response.Body, finish: finish}
		} else {
			finish()
		}
	}()
	ctx = s.gatewayPoolWaitContext(ctx, account)
	budget, _ := ctx.Value(gatewayPoolFirstOutputBudgetKey{}).(*gatewayPoolFirstOutputBudget)
	lastFailedVersion, failuresOnTicket := "", 0
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attemptCtx, finishAttempt := beginGatewayPoolUsageAttempt(ctx)
		current := request.Clone(attemptCtx) // includes an independent header map
		if attempt > 0 {
			if request.GetBody == nil {
				finishAttempt()
				return nil, errors.New("gateway request body cannot be replayed")
			}
			body, err := request.GetBody()
			if err != nil {
				finishAttempt()
				return nil, err
			}
			current.Body = body
		}
		var guard *openAIFirstOutputHeaderGuard
		if budget != nil {
			guardCtx, newGuard := newGatewayPoolFirstOutputGuard(attemptCtx, func() {}, budget)
			guard = newGuard
			current = current.WithContext(guardCtx)
		}
		resp, err := s.doOpenAIUpstreamAttempt(current, proxyURL, account)
		timedOut := guard != nil && guard.stopHeaderWait()
		if timedOut && ctx.Err() == nil {
			err = retryGatewayPoolAttempt(context.DeadlineExceeded)
		}
		var retryError *gatewayPoolAttemptRetryError
		classifiedRetry := errors.As(err, &retryError)
		degraded := errors.Is(err, errOpenAIGatewayPoolRouteDegraded)
		retire := classifiedRetry && retryError.retire
		applied := openAIGatewayPoolSinkFrom(current.Context()).snapshot()
		if err == nil && resp != nil && gatewayPoolBusinessRetryStatus(resp.StatusCode) {
			if applied.Version != lastFailedVersion {
				lastFailedVersion, failuresOnTicket = applied.Version, 0
			}
			failuresOnTicket++
			retire = failuresOnTicket >= gatewayPoolTransientAttemptsPerTicket
		}
		retry := classifiedRetry || degraded ||
			(err != nil && !classifyUpstreamTransportError(err).Persistent && gatewayPoolRetryablePreparationError(err)) ||
			(err == nil && resp != nil && gatewayPoolBusinessRetryStatus(resp.StatusCode))
		if !retry || ctx.Err() != nil || request.GetBody == nil {
			if guard != nil {
				if err == nil && resp != nil && resp.Body != nil {
					resp.Body = &openAIRequestContextReadCloser{ReadCloser: resp.Body, cleanup: guard.close}
				} else {
					guard.close()
				}
			}
			if err == nil && resp != nil && resp.Body != nil {
				resp.Body = &gatewayPoolUsageBody{ReadCloser: resp.Body, finish: finishAttempt}
			} else {
				finishAttempt()
			}
			return resp, err
		}
		// No response byte was delivered. Close the rejected body and its
		// attempt context before waiting or claiming another ticket.
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if guard != nil {
			guard.close()
		}
		if current.Body != nil {
			_ = current.Body.Close()
		}
		finishAttempt()
		if retire {
			s.retireGatewayPoolBusinessAttempt(current.WithContext(ctx), account)
		}
		slog.Info("gwpool_business_retry", "account_id", account.ID, "attempt", attempt+1,
			"ticket_retired", retire || degraded)
		// A confirmed rejection has already retired this ticket. Re-enter
		// preparation now; a real shortage still waits in ticket acquisition.
		// Transport/unknown failures retain their normal retry backoff.
		if !degraded {
			again, waitErr := s.waitGatewayPoolRetry(ctx, account, gatewayPoolVerificationRetryGap)
			if waitErr != nil {
				return nil, waitErr
			}
			if !again {
				return nil, errOpenAIGatewayPoolWarmExhausted
			}
		}
		if budget != nil {
			budget.mu.Lock()
			budget.started = time.Time{}
			budget.mu.Unlock()
		}
	}
}
