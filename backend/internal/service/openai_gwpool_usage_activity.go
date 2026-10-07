package service

import (
	"context"
	"io"
	"sync"
	"time"
)

const (
	// Repository writes this event atomically with every temporary block. It
	// survives clearing/expiry, so a short block cannot disappear between scans.
	GatewayPoolUsageBlockedAtKey = "openai_gwpool_usage_blocked_at"
	gatewayPoolUsageIdleTimeout  = 30 * time.Minute
)

type gatewayPoolUsageRequestKey struct{}
type gatewayPoolUsageIdentityKey struct{}
type gatewayPoolUsageActivityKey struct{}

type gatewayPoolUsageActivity struct {
	inventory *gatewayPoolInventoryState
	sending   bool // Protected, with closed, by inventory.mu.
	closed    bool
}

// Waiting for a ticket is not an inventory operation. Only a real business
// dispatch pins the inventory until its response body has finished.
func gatewayPoolUsageMarkSending(ctx context.Context, identity string) {
	activity, _ := ctx.Value(gatewayPoolUsageActivityKey{}).(*gatewayPoolUsageActivity)
	tag, _ := ctx.Value(gatewayPoolUsageIdentityKey{}).(string)
	if activity == nil || tag != gatewayPoolLedgerTag(identity) {
		return
	}
	inventory := activity.inventory
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	if !activity.closed && !activity.sending {
		activity.sending = true
		inventory.active++
		inventory.generation++
	}
}

func gatewayPoolUsageEventAt(ctx context.Context, at time.Time) time.Time {
	start, _ := ctx.Value(gatewayPoolUsageRequestKey{}).(time.Time)
	if !at.IsZero() && start.After(at) {
		return start // preserve logical order on clocks with coarse resolution
	}
	return at
}

func gatewayPoolUsageBlockedAt(account *Account) time.Time {
	if account == nil {
		return time.Time{}
	}
	at, _ := time.Parse(time.RFC3339Nano, account.GetExtraString(GatewayPoolUsageBlockedAtKey))
	if at.After(time.Now().Add(gatewayPoolCooldownGrace)) {
		return time.Time{}
	}
	return at
}

func (r *gatewayPoolUsageLedger) applyGatewayPoolBlock(at time.Time) bool {
	if at.IsZero() || !at.After(r.BlockedAt) {
		return false
	}
	r.BlockedAt = at
	r.end(at, "temporarily_unschedulable")
	return true
}

func (r *gatewayPoolUsageLedger) requestClosed(ctx context.Context) bool {
	start, _ := ctx.Value(gatewayPoolUsageRequestKey{}).(time.Time)
	return !start.IsZero() && !start.After(r.ClosedBefore[gatewayPoolUsageSharedModel])
}

// Only used with no in-flight business/fetch/probe operation. On restart an
// unfinished request has an unknown tail: retain the last durable observation.
func (r *gatewayPoolUsageLedger) idleCutoff(now time.Time) time.Time {
	last := r.LastRequestCompletedAt
	if r.LastRequestStartedAt.After(last) {
		last = r.LastRequestStartedAt
	}
	if last.IsZero() {
		for _, round := range r.Rounds {
			if round.Model != gatewayPoolUsageSharedModel || !round.EndedAt.IsZero() {
				continue
			}
			for _, ticket := range round.Tickets {
				for _, at := range []time.Time{ticket.At, ticket.UseObservedAt} {
					if at.After(last) {
						last = at
					}
				}
			}
		}
	}
	if last.IsZero() || now.Sub(last) <= gatewayPoolUsageIdleTimeout {
		return time.Time{}
	}
	return last
}

// Start before preflight; finish after body EOF/error/Close, not response headers.
// The inventory domain is the same domain used by clone accounting and shared
// fetch/probe work. No new timer or upstream request is created here.
func (s *OpenAIGatewayService) beginGatewayPoolUsageRequest(ctx context.Context, account *Account) (context.Context, func()) {
	if s == nil || s.accountRepo == nil || !s.codexCookies.gatewayPoolTakeover(account) {
		return ctx, nil
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return ctx, nil
	}
	at := time.Now().UTC()
	var knownClosed time.Time
	if cached, ok := s.codexCookies.poolUsageCache.Load(gatewayPoolLedgerTag(identity)); ok {
		if state, valid := cached.(*gatewayPoolUsageLedger); valid {
			knownClosed = state.ClosedBefore[gatewayPoolUsageSharedModel]
		}
	}
	inventory := s.codexCookies.gatewayPoolInventory(identity)
	inventory.mu.Lock()
	wasIdle := inventory.active == 0 && inventory.requests == 0
	inventory.requests++
	inventory.mu.Unlock()
	activity := &gatewayPoolUsageActivity{inventory: inventory}
	s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
		if wasIdle {
			if end := state.idleCutoff(at); !end.IsZero() {
				state.end(end, "idle_timeout")
			}
		}
		// Only order an exact same-tick completion already observed before
		// this request. A block discovered while reading fresh state must
		// still close this old request, never rewrite it into a new one.
		if end := state.ClosedBefore[gatewayPoolUsageSharedModel]; at.Equal(end) && end.Equal(knownClosed) {
			at = end.Add(time.Nanosecond)
		}
		if at.After(state.LastRequestStartedAt) {
			state.LastRequestStartedAt = at
			return true
		}
		return false
	})
	ctx = context.WithValue(ctx, gatewayPoolUsageRequestKey{}, at)
	ctx = context.WithValue(ctx, gatewayPoolUsageIdentityKey{}, gatewayPoolLedgerTag(identity))
	ctx = context.WithValue(ctx, gatewayPoolUsageActivityKey{}, activity)
	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			completed := gatewayPoolUsageEventAt(ctx, time.Now().UTC())
			s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
				if completed.After(state.LastRequestCompletedAt) {
					state.LastRequestCompletedAt = completed
					return true
				}
				return false
			})
			inventory.mu.Lock()
			activity.closed = true
			inventory.requests--
			if activity.sending {
				inventory.active--
				inventory.generation++
			}
			inventory.mu.Unlock()
			s.finishGatewayPoolUsageIfExhausted(ctx, account)
		})
	}
}

func (s *OpenAIGatewayService) maintainGatewayPoolUsage(ctx context.Context, account *Account, now time.Time) {
	if s.accountRepo == nil || !s.codexCookies.gatewayPoolTakeover(account) {
		return
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return
	}
	// Quiet periodic scans do not need a DB write for accounts without a cycle.
	state := readGatewayPoolUsage(account, gatewayPoolLedgerTag(identity))
	if len(state.Rounds) == 0 && state.VerificationSequence == 0 {
		return
	}
	s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
		inventory := s.codexCookies.gatewayPoolInventory(identity)
		inventory.mu.Lock()
		defer inventory.mu.Unlock()
		if inventory.active != 0 || inventory.requests != 0 {
			return false
		}
		if at := state.idleCutoff(now); !at.IsZero() {
			return state.end(at, "idle_timeout")
		}
		return false
	})
}

type gatewayPoolUsageBody struct {
	io.ReadCloser
	finish func()
	once   sync.Once
}

func (b *gatewayPoolUsageBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(b.finish)
	}
	return n, err
}

func (b *gatewayPoolUsageBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.finish)
	return err
}
