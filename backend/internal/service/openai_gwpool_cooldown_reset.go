package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

const (
	openAIGatewayPoolCooldownResetHoursKey = "openai_gwpool_cooldown_reset_hours"
	gatewayPoolCooldownResetDefaultHours   = 24
	gatewayPoolCooldownResetMaxHours       = 365 * 24
)

// The schedule and reset barrier belong to the credential ledger, not a process.
// Keeping the barrier after disabling prevents an old clone from restoring learning.
type gatewayPoolCooldownResetState struct {
	IntervalHours int       `json:"interval_hours,omitempty"`
	StartedAt     time.Time `json:"started_at,omitzero"`
	LastAt        time.Time `json:"last_at,omitzero"`
	ClearedAt     time.Time `json:"cleared_at,omitzero"`
}

func gatewayPoolCooldownResetHours(raw any) (int, bool) {
	if raw == nil {
		return gatewayPoolCooldownResetDefaultHours, true
	}
	if hours, ok := gatewayPoolInteger(raw, gatewayPoolCooldownResetMaxHours); ok {
		return hours, true
	}
	switch n := raw.(type) {
	case int:
		return 0, n == 0
	case int64:
		return 0, n == 0
	case float64:
		return 0, n == 0
	case json.Number:
		value, err := n.Float64()
		return 0, err == nil && value == 0
	default:
		return 0, false
	}
}

func (a *Account) gatewayPoolCooldownResetHours() int {
	if a == nil || !a.UsesGatewayPool() {
		return 0
	}
	hours, _ := gatewayPoolCooldownResetHours(a.Extra[openAIGatewayPoolCooldownResetHoursKey])
	return hours
}

func (r gatewayPoolCooldownResetState) valid(now time.Time) bool {
	return r.IntervalHours >= 0 && r.IntervalHours <= gatewayPoolCooldownResetMaxHours &&
		!r.StartedAt.After(now.Add(gatewayPoolCooldownGrace)) &&
		!r.ClearedAt.After(r.LastAt) &&
		!r.LastAt.After(now.Add(gatewayPoolCooldownGrace))
}

func (r *gatewayPoolCooldownResetState) advance(hours int, now, latest time.Time) bool {
	changed := false
	if latest.After(r.LastAt) {
		r.LastAt, changed = latest, true
	}
	if hours != r.IntervalHours {
		r.IntervalHours, changed = hours, true
	}
	if hours > 0 && r.StartedAt.IsZero() {
		r.StartedAt = now
		return true // the first enable arms the timer, without clearing learning
	}
	// Disabling stops execution, not the durable clock. Re-enabling an overdue
	// schedule catches up once, whether or not a worker observed the disabled state.
	anchor := r.StartedAt
	if r.LastAt.After(anchor) {
		anchor = r.LastAt
	}
	if hours > 0 && !now.Before(anchor.Add(time.Duration(hours)*time.Hour)) {
		r.LastAt = now // missed periods collapse into one reset, not a catch-up loop
		return true
	}
	return changed
}

// Reset learning, not contact history. In particular, do NOT start a fresh hour
// at reset time and do NOT make a gateway touched 10 minutes ago immediately free.
func (c *gatewayPoolCooldown) resetBackoff(at, touched time.Time, base int) bool {
	if at.IsZero() || !at.After(c.ResetAt) {
		return false
	}
	c.ResetAt = at
	if c.Cleared {
		return true // a timer reset must not re-arm a manually cleared contact
	}
	if c.WindowSeconds == 0 && c.UpdatedAt.IsZero() {
		return true // an unseen gateway has no cooling period to invent
	}
	anchor := touched
	for _, observed := range []time.Time{c.CycleAt, c.UpdatedAt} {
		if observed.After(anchor) {
			anchor = observed
		}
	}
	c.SourcesKnown, c.BaseSeconds, c.CycleAt = true, base, anchor
	c.WindowSeconds, c.LocalFloorSeconds, c.FixedSeconds = base, 0, 0
	c.Until, c.ScheduleUpdatedAt = anchor.Add(time.Duration(base)*time.Second), at
	c.AttemptAt, c.AttemptSeconds, c.ElapsedSeconds = time.Time{}, 0, 0
	c.Successes = nil
	c.LastSuccessAt, c.LastSuccessSeconds = time.Time{}, 0
	c.LegacyRecommendedSeconds, c.LegacyRecommendationSource, c.LegacyRecommendationUntil = 0, "", time.Time{}
	return true
}

func (s *openAICodexCookieStore) gatewayPoolCooldownResetAt(identity string) time.Time {
	value, _ := s.poolCooldownReset.Load(identity)
	at, _ := value.(time.Time)
	return at
}

func (s *openAICodexCookieStore) applyGatewayPoolCooldownReset(identity string, at time.Time, base int) {
	if at.IsZero() || at.After(time.Now().UTC().Add(gatewayPoolCooldownGrace)) {
		return
	}
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	ledger := identity
	if !at.After(s.gatewayPoolCooldownResetAt(identity)) {
		return
	}
	s.poolCooldownReset.Store(ledger, at)
	for key, cooldown := range s.poolCooldown {
		if !strings.HasPrefix(key, ledger+"\x00") {
			continue
		}
		value, _ := s.poolUsed.Load(key)
		touched, _ := value.(time.Time)
		cooldown.resetBackoff(at, touched, base)
		s.poolCooldown[key] = cooldown
	}
	// No pair/proof/connection is removed; a live full-strength route stays live.
}

func newerGatewayPoolCooldown(next, previous *gatewayPoolCooldown) bool {
	if next == nil {
		return false
	}
	if previous == nil || next.ResetAt.After(previous.ResetAt) {
		return true
	}
	return next.ResetAt.Equal(previous.ResetAt) && next.changedAt().After(previous.changedAt())
}

// Called with the account history lock, never while holding poolCooldownMu.
func (s *openAICodexCookieStore) syncGatewayPoolCooldownResetHistory(rec *openAIGatewayHistory, identity string, base int) bool {
	s.applyGatewayPoolCooldownClear(identity, rec.CooldownReset.ClearedAt, base)
	s.applyGatewayPoolCooldownReset(identity, rec.CooldownReset.LastAt, base)
	at := s.gatewayPoolCooldownResetAt(identity)
	changed := false
	if cleared := s.gatewayPoolCooldownClearAt(identity); cleared.After(rec.CooldownReset.ClearedAt) {
		rec.CooldownReset.ClearedAt, changed = cleared, true
	}
	if at.After(rec.CooldownReset.LastAt) {
		rec.CooldownReset.LastAt, changed = at, true
	}
	for gateway, seen := range rec.Seen {
		if seen.Cooldown == nil {
			continue
		}
		cooldown := seen.Cooldown.clone()
		// Compare generations BEFORE normalizing a stale database record. A late
		// old-epoch timestamp must not erase learning made after the reset.
		if current, ok := s.cooldownEntry(identity, gateway); ok && newerGatewayPoolCooldown(&current, &cooldown) {
			cooldown = current
		}
		value, _ := s.poolUsed.Load(gatewayPoolLedgerKey(identity, gateway))
		touched, _ := value.(time.Time)
		if seen.At.After(touched) {
			touched = seen.At
		}
		reset := cooldown.clearCooldown(s.gatewayPoolCooldownClearAt(identity), base)
		reset = cooldown.resetBackoff(at, touched, base) || reset
		if reset || newerGatewayPoolCooldown(&cooldown, seen.Cooldown) {
			seen.Cooldown = &cooldown
			rec.Seen[gateway], changed = seen, true
		}
	}
	return changed
}

func gatewayPoolCooldownResetHistory(rec *openAIGatewayHistory, tag string) *openAIGatewayHistory {
	if rec.LedgerTag == tag {
		return rec
	}
	if rec.Previous != nil && rec.Previous.LedgerTag == tag {
		return rec.Previous
	}
	return nil
}

func (s *OpenAIGatewayService) maintainGatewayPoolCooldownReset(ctx context.Context, account *Account, now time.Time) error {
	if s.accountRepo == nil || account == nil || !account.UsesGatewayPool() {
		return nil
	}
	rec, _ := readOpenAIGatewayHistory(account)
	hours := account.gatewayPoolCooldownResetHours()
	if hours == 0 && rec.CooldownReset.IntervalHours == 0 {
		return nil
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return err
	}
	rec = gatewayPoolHistoryForTag(rec, gatewayPoolLedgerTag(identity))
	if !rec.CooldownReset.valid(now) {
		return errors.New("invalid gateway cooldown reset clock")
	}
	schedule := rec.CooldownReset
	if !schedule.advance(hours, now, s.codexCookies.gatewayPoolCooldownResetAt(identity)) {
		return nil // ordinary feedback wakes must not cause extra DB scans/writes
	}
	lock, _ := s.codexCookies.poolCooldownResetLocks.LoadOrStore(identity, &sync.Mutex{})
	mu, ok := lock.(*sync.Mutex)
	if !ok || mu == nil {
		return errors.New("invalid gateway cooldown reset lock")
	}
	if !gatewayPoolLockWithin(ctx, mu) {
		return ctx.Err()
	}
	defer mu.Unlock()

	tag := gatewayPoolLedgerTag(identity)
	peers, err := s.gatewayPoolHistoryPeers(ctx, tag)
	if err != nil {
		return err
	}
	latest := s.codexCookies.gatewayPoolCooldownResetAt(identity)
	for i := range peers {
		history, _ := readOpenAIGatewayHistory(&peers[i])
		if view := gatewayPoolCooldownResetHistory(&history, tag); view != nil {
			if !view.CooldownReset.valid(now) {
				return errors.New("invalid gateway cooldown reset clock")
			}
			if view.CooldownReset.LastAt.After(latest) {
				latest = view.CooldownReset.LastAt
			}
			// A clone may have a newer contact than this process's memory. Merge
			// those facts before deriving the shared reset deadline.
			s.codexCookies.gatewayPoolHydrateUsed(&peers[i], identity)
		}
	}
	if err := s.writeGatewayPoolCooldownReset(ctx, account.ID, identity, now, latest, true); err != nil {
		return err
	}
	// The durable owner barrier is committed first. Partial peer-write failure
	// cannot resurrect old learning: fresh shared-history hydration sees that barrier.
	for i := range peers {
		if peers[i].ID != account.ID {
			if err := s.writeGatewayPoolCooldownReset(ctx, peers[i].ID, identity, now,
				s.codexCookies.gatewayPoolCooldownResetAt(identity), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *OpenAIGatewayService) writeGatewayPoolCooldownReset(ctx context.Context, id int64, identity string,
	now, latest time.Time, advance bool,
) error {
	lock, _ := s.codexCookies.poolHistoryLocks.LoadOrStore(id, &sync.Mutex{})
	mu, ok := lock.(*sync.Mutex)
	if !ok || mu == nil {
		return errors.New("invalid gateway cooldown history lock")
	}
	if !gatewayPoolLockWithin(ctx, mu) {
		return ctx.Err()
	}
	defer mu.Unlock()
	fresh, err := s.accountRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if fresh == nil {
		return errors.New("gateway cooldown reset account missing")
	}
	rec, _ := readOpenAIGatewayHistory(fresh)
	tag := gatewayPoolLedgerTag(identity)
	base := gatewayPoolCooldownBase(fresh.gatewayPoolGatewayWindow())
	var view *openAIGatewayHistory
	if advance {
		current, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
		if err != nil || current != identity || !fresh.UsesGatewayPool() {
			return errors.New("gateway cooldown reset identity changed")
		}
		rec = gatewayPoolHistoryForTag(rec, tag)
		rec.LedgerTag = tag
		view = &rec
	} else {
		view = gatewayPoolCooldownResetHistory(&rec, tag)
		if view == nil {
			return nil
		}
	}
	if !view.CooldownReset.valid(now) {
		return errors.New("invalid gateway cooldown reset clock")
	}
	changed := false
	if cleared := s.codexCookies.gatewayPoolCooldownClearAt(identity); cleared.After(view.CooldownReset.ClearedAt) {
		view.CooldownReset.ClearedAt, changed = cleared, true
	}
	if advance {
		changed = view.CooldownReset.advance(fresh.gatewayPoolCooldownResetHours(), now, latest) || changed
	} else if latest.After(view.CooldownReset.LastAt) {
		view.CooldownReset.LastAt, changed = latest, true
	}
	at := view.CooldownReset.LastAt
	for gateway, seen := range view.Seen {
		cooldown := seen.Cooldown
		if inMemory, exists := s.codexCookies.cooldownEntry(identity, gateway); exists && newerGatewayPoolCooldown(&inMemory, cooldown) {
			cooldown = &inMemory
		}
		if cooldown == nil {
			continue
		}
		next := cooldown.clone()
		value, _ := s.codexCookies.poolUsed.Load(gatewayPoolLedgerKey(identity, gateway))
		touched, _ := value.(time.Time)
		if seen.At.After(touched) {
			touched = seen.At
		}
		reset := next.clearCooldown(view.CooldownReset.ClearedAt, base)
		reset = next.resetBackoff(at, touched, base) || reset
		if reset || newerGatewayPoolCooldown(&next, seen.Cooldown) {
			seen.Cooldown = &next
			view.Seen[gateway], changed = seen, true
		}
	}
	if !changed {
		s.codexCookies.applyGatewayPoolCooldownReset(identity, at, base)
		return nil
	}
	previousTag := ""
	if rec.Previous != nil {
		previousTag = rec.Previous.LedgerTag
	}
	if err := s.accountRepo.UpdateExtra(ctx, id, map[string]any{
		openAIGatewayHistoryExtraKey: rec, openAIGatewayLedgerTagExtraKey: rec.LedgerTag,
		openAIGatewayPreviousLedgerTagExtraKey: previousTag,
	}); err != nil {
		return err
	}
	s.codexCookies.applyGatewayPoolCooldownReset(identity, at, base)
	return nil
}
