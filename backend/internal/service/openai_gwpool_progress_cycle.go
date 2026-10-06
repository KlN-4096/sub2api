package service

import (
	"context"
	"time"
)

// Allocate only when warm creates a new run, not for cached-ticket business or
// preflight re-entry. Persistence precedes publishing the display sequence.
func (s *OpenAIGatewayService) startGatewayPoolProgress(ctx context.Context, account *Account, identity string) *gatewayPoolProgressRun {
	start, _ := ctx.Value(gatewayPoolUsageRequestKey{}).(time.Time)
	if start.IsZero() {
		start = time.Now().UTC()
	}
	tag := gatewayPoolLedgerTag(identity)
	if original, _ := ctx.Value(gatewayPoolUsageIdentityKey{}).(string); original != "" {
		tag = original
	}
	scope := gatewayPoolProgressScope{tag: tag, requestStarted: start, identity: identity}
	if tag == gatewayPoolLedgerTag(identity) {
		var sequence uint64
		committed := s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
			if !start.After(state.ClosedBefore[gatewayPoolUsageSharedModel]) {
				return false
			}
			next := state.VerificationSequence + 1
			if next == 0 {
				return false // corrupt/exhausted counter: show no invented number
			}
			state.VerificationSequence = next
			if state.VerificationStartedAt.IsZero() || start.Before(state.VerificationStartedAt) {
				state.VerificationStartedAt = start
			}
			// Direct warm callers can be first fetches without a usage round.
			if start.After(state.LastRequestStartedAt) {
				state.LastRequestStartedAt = start
			}
			sequence = next
			return true
		})
		if committed {
			scope.sequence = sequence
		}
	}
	return s.codexCookies.poolProgress.start(account.ID, 0, scope)
}
