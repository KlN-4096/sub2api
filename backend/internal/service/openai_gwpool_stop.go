package service

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const (
	openAIGatewayPoolStopGatewaysExtraKey = "openai_gwpool_stop_gateways"
	gatewayPoolStopGatewaysDefault        = 5
	gatewayPoolStopGatewaysMax            = 512
)

const gatewayPoolStopThresholdClientMsg = "可用网关低于账号停止门槛，本次不发送业务请求，请稍后重试" +
	" / Available gateways are below this account's stop threshold; no business request was sent. Retry later."

var errGatewayPoolStopThreshold = fmt.Errorf("%s: %w", gatewayPoolStopThresholdClientMsg, errOpenAIGatewayPoolWarmExhausted)

// GatewayPoolRestFailure is the pre-output failure of a request whose pool
// credential entered rest, with the forward path's client message and
// Retry-After. It never asks for a new shortage check.
func GatewayPoolRestFailure() *UpstreamFailoverError {
	err := gatewayPoolRestError()
	return &UpstreamFailoverError{
		StatusCode: http.StatusServiceUnavailable, Reason: OpenAIGatewayPoolReason, NextAccountAction: NextAccountStop,
		ClientMessage: gatewayPoolClientMessage(err), ClientStatusCode: http.StatusServiceUnavailable,
		ResponseHeaders: http.Header{"Retry-After": []string{strconv.Itoa(int(gatewayPoolRetryAfter(err) / time.Second))}},
	}
}

func (a *Account) gatewayPoolStopGateways() int {
	if a != nil {
		if n, ok := gatewayPoolInteger(a.Extra[openAIGatewayPoolStopGatewaysExtraKey], gatewayPoolStopGatewaysMax); ok {
			return n
		}
	}
	return gatewayPoolStopGatewaysDefault
}

func (s *OpenAIGatewayService) gatewayPoolStopCatalog(ctx context.Context, account *Account) *gatewayPoolExhaustion {
	if account == nil || account.GatewayPoolContinuousWaitEnabled() {
		return nil
	}
	exhausted, _ := s.gatewayPoolCatalogBelow(ctx, account, account.gatewayPoolStopGateways())
	return exhausted
}

// Only an established, identity-matched pool rest can rescue an old waiter.
// Ignore the pool-owned temporary block, never manual/auth/quota/other blocks,
// and retain every transport/configuration match required for ordinary waiting.
func (s *OpenAIGatewayService) gatewayPoolRestHandoffIdentity(ctx context.Context, fresh, original *Account) string {
	if ctx.Err() != nil || fresh == nil || original == nil || fresh.GatewayPoolContinuousWaitEnabled() {
		return ""
	}
	matches := func(account *Account) bool {
		health := *account
		if gatewayPoolOwnsTempBlock(&health) {
			health.TempUnschedulableUntil = nil
		}
		return gatewayPoolWaitAccountMatches(&health, original)
	}
	if !matches(fresh) {
		return ""
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if err != nil {
		return ""
	}
	previous, err := s.codexCookies.gatewayPoolIdentity(ctx, original)
	if err != nil || previous != identity {
		return ""
	}
	unlock, err := s.lockGatewayPoolRest(ctx, gatewayPoolLedgerTag(identity))
	if err != nil {
		return ""
	}
	defer unlock()
	state, current, err := s.loadGatewayPoolRest(ctx, fresh, identity)
	if err != nil || current == nil || !matches(current) || !state.Active {
		return ""
	}
	return identity
}

// GatewayPoolRestHandoff only classifies a no-output waiting boundary. The
// caller must still preserve replay safety, group scope and ordinary failover
// limits; it must not treat arbitrary unschedulability as pool rest.
func (s *OpenAIGatewayService) GatewayPoolRestHandoff(ctx context.Context, original *Account) *UpstreamFailoverError {
	if s == nil || s.accountRepo == nil || original == nil || !original.UsesGatewayPool() || original.GatewayPoolContinuousWaitEnabled() || ctx.Err() != nil {
		return nil
	}
	readCtx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
	defer cancel()
	fresh, err := s.accountRepo.GetByID(readCtx, original.ID)
	if err != nil || fresh == nil || !s.gatewayPoolRestMaybeActive(readCtx, fresh) ||
		s.gatewayPoolRestHandoffIdentity(readCtx, fresh, original) == "" {
		return nil
	}
	return GatewayPoolRestFailure()
}

// Rests are published in-process and block the row that entered them, so
// anything else skips the locked durable read. A rest from another process
// still stops the request at preparation.
func (s *OpenAIGatewayService) gatewayPoolRestMaybeActive(ctx context.Context, account *Account) bool {
	if gatewayPoolOwnsTempBlock(account) {
		return true
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return false
	}
	cached, _ := s.codexCookies.poolRestState.Load(gatewayPoolLedgerTag(identity))
	state, _ := cached.(gatewayPoolRestState)
	return state.Active
}

// WatchGatewayPoolSlotRest interrupts only a pending slot acquisition, never
// its parent request or an already-running business stream.
func (s *OpenAIGatewayService) WatchGatewayPoolSlotRest(ctx context.Context, account *Account) (context.Context, func()) {
	if s == nil || s.accountRepo == nil || account == nil || !account.UsesGatewayPool() || account.GatewayPoolContinuousWaitEnabled() {
		return ctx, func() {}
	}
	work, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(gatewayPoolWaitConfigPoll)
		defer ticker.Stop()
		for {
			if failure := s.GatewayPoolRestHandoff(work, account); failure != nil {
				cancel(failure)
				return
			}
			select {
			case <-work.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return work, func() { cancel(context.Canceled); <-done }
}
