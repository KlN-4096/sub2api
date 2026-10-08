package service

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func (s *openAICodexCookieStore) gatewayPoolUsageSession() string {
	s.poolUsageSessionOnce.Do(func() { s.poolUsageSession = uuid.NewString() })
	return s.poolUsageSession
}

func gatewayPoolUsageTicketKey(gateway, version string) string {
	return gatewayPoolUsageDigest(gateway + "\x00" + version)
}

// Read-only legacy archival projection. Never accrue an open historical tail
// to now, and never write derived bounds back into the original ticket record.
func (r *GatewayPoolUsageRound) legacyFullUseDuration() int64 {
	var total int64
	for _, ticket := range r.Tickets {
		total += ticket.UseMS
		end := ticket.UseEndedAt
		if end.IsZero() {
			end = ticket.UseObservedAt
		}
		for _, bound := range []time.Time{ticket.UseExpiresAt, r.EndedAt} {
			if !bound.IsZero() && bound.Before(end) {
				end = bound
			}
		}
		if !ticket.UseStartedAt.IsZero() && end.After(ticket.UseStartedAt) {
			total += end.Sub(ticket.UseStartedAt).Milliseconds()
		}
	}
	return total
}

func (s *OpenAIGatewayService) noteGatewayPoolFullUse(ctx context.Context, account *Account, identity string,
	applied OpenAIGatewayPoolApplied, at time.Time,
) bool {
	if at.IsZero() || applied.Version == "" || account == nil || applied.AccountID != account.ID {
		return false
	}
	at = gatewayPoolUsageEventAt(ctx, at)
	pair, _ := s.codexCookies.cachedPoolPair(identity)
	mark, ok := s.codexCookies.gatewayPoolVerifiedMarkOf(identity)
	if pair.version != applied.Version || pair.invalidated || !ok || mark.version != applied.Version {
		return false
	}
	ticket := gatewayPoolUsageTicketKey(applied.Gateway, applied.Version)
	active, already := s.registerGatewayPoolActiveUse(ctx, account, identity, ticket, at, pair.routeExpiresAt)
	if already {
		return true
	}
	s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
		if state.requestClosed(ctx) {
			return false
		}
		noted := state.note(gatewayPoolUsageSharedModel, ticket, at, true)
		active.bind(state)
		return noted
	}, func(state *gatewayPoolUsageLedger) bool {
		if ctx.Value(gatewayPoolUsageActivityKey{}) != nil {
			return false // each physical attempt must register even on the same ticket
		}
		for _, round := range state.Rounds {
			if round.Model != gatewayPoolUsageSharedModel || !round.EndedAt.IsZero() {
				continue
			}
			if seen, exists := round.Tickets[ticket]; exists && seen.Full && !at.Before(seen.At) {
				return true
			}
		}
		return false
	})
	// This mutation includes the attempt as well as full-use accounting. Do not
	// issue another ledger mutation for the same event, including on DB error.
	return true
}

func (r GatewayPoolUsageRound) fullUsageView(session string, now time.Time, events ...gatewayPoolActiveUseEvent) GatewayPoolUsageRound {
	if r.Model != gatewayPoolUsageSharedModel {
		r.Tickets = nil
		return r
	}
	r.FullDurationMS, r.FullActiveUntil, r.FullUsageMode = 0, nil, ""
	if !r.EndedAt.IsZero() && r.EndedAt.Before(now) {
		now = r.EndedAt
	}
	r.syncActiveUsage(events, session, now)
	if r.ActiveUsage != nil {
		r.FullDurationMS = r.ActiveUsage.duration(now)
		r.FullUsageMode = gatewayPoolActiveUsageMode
		r.DurationIncomplete = r.ActiveUsage.Incomplete
	}
	r.ActiveUsage = nil // interval details stay in storage, not the polling API
	r.Tickets = nil
	return r
}
