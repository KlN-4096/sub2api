package service

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const gatewayPoolFullUseCheckpointGap = time.Minute

func (s *openAICodexCookieStore) gatewayPoolUsageSession() string {
	s.poolUsageSessionOnce.Do(func() { s.poolUsageSession = uuid.NewString() })
	return s.poolUsageSession
}

func gatewayPoolUsageTicketKey(gateway, version string) string {
	return gatewayPoolUsageDigest(gateway + "\x00" + version)
}

func (s *openAICodexCookieStore) gatewayPoolUsageLive(identity string) map[string]openAIGatewayPoolPair {
	out := map[string]openAIGatewayPoolPair{}
	domain := gatewayPoolLedgerIdentity(identity)
	s.poolPairs.Range(func(key, value any) bool {
		other, valid := key.(string)
		pair, ok := value.(openAIGatewayPoolPair)
		if valid && ok && pair.version != "" && gatewayPoolLedgerIdentity(other) == domain {
			out[gatewayPoolUsageTicketKey(pair.gateway, pair.version)] = pair
		}
		return true
	})
	return out
}

// A restart or lost route has an unknown tail. Keep the last durable observed
// interval, never count downtime or missing-ticket waiting as full use.
func (t gatewayPoolUsageTicket) useEnd(pair openAIGatewayPoolPair, known bool, session string, now time.Time) (time.Time, bool) {
	if t.UseStartedAt.IsZero() || !t.UseEndedAt.IsZero() {
		return t.UseEndedAt, false
	}
	if t.UseSession != session || !known {
		return t.UseObservedAt, true
	}
	if pair.invalidated {
		if !pair.invalidatedAt.IsZero() {
			return pair.invalidatedAt, false
		}
		return t.UseObservedAt, true
	}
	if !t.UseExpiresAt.IsZero() && !now.Before(t.UseExpiresAt) {
		return t.UseExpiresAt, false
	}
	return time.Time{}, false
}

func (t gatewayPoolUsageTicket) duration(now time.Time) int64 {
	end := t.UseEndedAt
	if end.IsZero() {
		end = now
	}
	if !t.UseExpiresAt.IsZero() && t.UseExpiresAt.Before(end) {
		end = t.UseExpiresAt
	}
	if t.UseStartedAt.IsZero() || !end.After(t.UseStartedAt) {
		return t.UseMS
	}
	return t.UseMS + end.Sub(t.UseStartedAt).Milliseconds()
}

func (r *GatewayPoolUsageRound) fullUseDuration(now time.Time) int64 {
	if !r.EndedAt.IsZero() && r.EndedAt.Before(now) {
		now = r.EndedAt
	}
	var total int64
	for _, ticket := range r.Tickets {
		total += ticket.duration(now)
	}
	return total
}

func (r *gatewayPoolUsageLedger) settleFullUsage(live map[string]openAIGatewayPoolPair, session string, now time.Time) bool {
	changed := false
	for i := range r.Rounds {
		round := &r.Rounds[i]
		if round.Model != gatewayPoolUsageSharedModel {
			continue
		}
		for key, ticket := range round.Tickets {
			pair, known := live[key]
			if end, incomplete := ticket.useEnd(pair, known, session, now); !end.IsZero() && ticket.UseEndedAt.IsZero() {
				ticket.UseEndedAt = end
				round.Tickets[key] = ticket
				round.DurationIncomplete = round.DurationIncomplete || incomplete
				changed = true
			}
		}
	}
	return changed
}

func (r *gatewayPoolUsageLedger) startFullUse(ticketKey string, at, expires time.Time, session string) bool {
	for i := range r.Rounds {
		round := &r.Rounds[i]
		if round.Model != gatewayPoolUsageSharedModel || !round.EndedAt.IsZero() {
			continue
		}
		ticket, ok := round.Tickets[ticketKey]
		if !ok || !ticket.Full || at.IsZero() || (!expires.IsZero() && !at.Before(expires)) {
			continue
		}
		if !ticket.UseEndedAt.IsZero() {
			// Only a new process may resume the same physical ticket after an
			// unknown tail; a conclusively ended interval never reopens.
			if ticket.UseSession == session {
				return false
			}
			ticket.UseMS = ticket.duration(ticket.UseEndedAt)
			ticket.UseStartedAt, ticket.UseEndedAt = time.Time{}, time.Time{}
		}
		if ticket.UseStartedAt.IsZero() {
			ticket.UseStartedAt, ticket.UseExpiresAt, ticket.UseSession = at, expires, session
		} else if ticket.UseSession == session && at.Before(ticket.UseStartedAt) {
			ticket.UseStartedAt = at
		}
		if at.After(ticket.UseObservedAt) {
			ticket.UseObservedAt = at
		}
		if round.FullStartedAt.IsZero() || at.Before(round.FullStartedAt) {
			round.FullStartedAt = at
		}
		round.Tickets[ticketKey] = ticket
		return true
	}
	return false
}

func (r *gatewayPoolUsageLedger) endFullUse(ticketKey string, at time.Time) bool {
	for i := range r.Rounds {
		round := &r.Rounds[i]
		if round.Model != gatewayPoolUsageSharedModel {
			continue
		}
		ticket, ok := round.Tickets[ticketKey]
		if ok && !ticket.UseStartedAt.IsZero() && ticket.UseEndedAt.IsZero() {
			ticket.UseEndedAt = at
			round.Tickets[ticketKey] = ticket
			return true
		}
	}
	return false
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
	session := s.codexCookies.gatewayPoolUsageSession()
	s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
		if state.requestClosed(ctx) {
			return false
		}
		noted := state.note(gatewayPoolUsageSharedModel, ticket, at, true)
		return state.startFullUse(ticket, at, pair.routeExpiresAt, session) || noted
	}, func(state *gatewayPoolUsageLedger) bool {
		for _, round := range state.Rounds {
			if round.Model != gatewayPoolUsageSharedModel || !round.EndedAt.IsZero() {
				continue
			}
			if seen, exists := round.Tickets[ticket]; exists && seen.UseSession == session && seen.UseEndedAt.IsZero() &&
				!seen.UseStartedAt.IsZero() && !at.Before(seen.UseStartedAt) &&
				at.Sub(seen.UseObservedAt) >= 0 && at.Sub(seen.UseObservedAt) < gatewayPoolFullUseCheckpointGap {
				return true
			}
		}
		return false
	})
	// This mutation includes the attempt as well as full-use accounting. Do not
	// issue another ledger mutation for the same event, including on DB error.
	return true
}

func (s *OpenAIGatewayService) settleGatewayPoolFullUsage(ctx context.Context, account *Account, identity string) {
	s.changeGatewayPoolUsage(ctx, account, identity, func(*gatewayPoolUsageLedger) bool { return false })
}

func (s *OpenAIGatewayService) endGatewayPoolFullUse(ctx context.Context, account *Account, identity string,
	applied OpenAIGatewayPoolApplied, at time.Time,
) {
	ticket := gatewayPoolUsageTicketKey(applied.Gateway, applied.Version)
	s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.endFullUse(ticket, at)
	})
}

func (r GatewayPoolUsageRound) fullUsageView(live map[string]openAIGatewayPoolPair, session string, now time.Time) GatewayPoolUsageRound {
	if r.Model != gatewayPoolUsageSharedModel {
		r.Tickets = nil
		return r
	}
	r.FullDurationMS, r.FullActiveUntil = 0, nil
	if !r.EndedAt.IsZero() && r.EndedAt.Before(now) {
		now = r.EndedAt
	}
	for key, ticket := range r.Tickets {
		pair, known := live[key]
		end, incomplete := ticket.useEnd(pair, known, session, now)
		r.DurationIncomplete = r.DurationIncomplete || incomplete
		if !end.IsZero() {
			ticket.UseEndedAt = end
		}
		if !r.EndedAt.IsZero() && (ticket.UseEndedAt.IsZero() || ticket.UseEndedAt.After(r.EndedAt)) {
			ticket.UseEndedAt = r.EndedAt
		}
		r.FullDurationMS += ticket.duration(now)
		if !ticket.UseStartedAt.IsZero() && ticket.UseEndedAt.IsZero() && r.EndedAt.IsZero() {
			r.FullActiveUntil = append(r.FullActiveUntil, ticket.UseExpiresAt)
		}
	}
	r.Tickets = nil
	return r
}
