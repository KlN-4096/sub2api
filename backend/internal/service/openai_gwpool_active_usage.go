package service

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"time"
)

const (
	gatewayPoolActiveUsageMode          = "business_active_v1"
	gatewayPoolActiveUsageIntervalLimit = 8192
	gatewayPoolActiveUsagePendingLimit  = 8192
)

type gatewayPoolUsageInterval struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Separate from the old ticket-lifetime fields: those records cannot be
// retrospectively converted into business-active time.
type gatewayPoolActiveUsage struct {
	Intervals []gatewayPoolUsageInterval `json:"intervals,omitempty"`
}

func (u *gatewayPoolActiveUsage) add(interval gatewayPoolUsageInterval) {
	if !interval.End.After(interval.Start) {
		return
	}
	all := append(append([]gatewayPoolUsageInterval(nil), u.Intervals...), interval)
	sort.Slice(all, func(i, j int) bool { return all[i].Start.Before(all[j].Start) })
	merged := all[:0]
	for _, item := range all {
		if len(merged) > 0 && !item.Start.After(merged[len(merged)-1].End) {
			last := &merged[len(merged)-1]
			if item.End.After(last.End) {
				last.End = item.End
			}
		} else {
			merged = append(merged, item)
		}
	}
	if len(merged) > gatewayPoolActiveUsageIntervalLimit {
		merged = merged[:gatewayPoolActiveUsageIntervalLimit] // never fill idle gaps to compress
	}
	u.Intervals = merged
}

func (u *gatewayPoolActiveUsage) duration(until time.Time) int64 {
	var duration time.Duration
	for _, interval := range u.Intervals {
		end := interval.End
		if !until.IsZero() && until.Before(end) {
			end = until
		}
		if end.After(interval.Start) {
			duration += end.Sub(interval.Start)
		}
	}
	return duration.Milliseconds()
}

type gatewayPoolActiveUseEvent struct {
	roundID, ticket string
	start, end      time.Time
	expires         time.Time
	invalidatedAt   time.Time
	sentAt          time.Time
	requestStarted  time.Time
	attempt         *gatewayPoolActiveUseAttempt
	segments        []gatewayPoolUsageInterval
}

type gatewayPoolActiveUseAttempt struct {
	mu      sync.Mutex
	event   gatewayPoolActiveUseEvent
	stop    func() bool
	persist func()
}

func (a *gatewayPoolActiveUseAttempt) finish(at time.Time) {
	a.mu.Lock()
	if !a.event.end.IsZero() {
		a.mu.Unlock()
		return
	}
	a.event.end = at
	stop, persist := a.stop, a.persist
	a.mu.Unlock()
	if stop != nil {
		stop()
	}
	if persist != nil {
		persist()
	}
}

func pauseGatewayPoolActiveUse(ctx context.Context) func() {
	activity, _ := ctx.Value(gatewayPoolUsageActivityKey{}).(*gatewayPoolUsageActivity)
	if activity == nil {
		return func() {}
	}
	activity.inventory.mu.Lock()
	attempt := activity.fullUse
	activity.inventory.mu.Unlock()
	if attempt == nil {
		return func() {}
	}
	attempt.mu.Lock()
	if !attempt.event.start.IsZero() && attempt.event.end.IsZero() {
		attempt.event.segments = append(attempt.event.segments, gatewayPoolUsageInterval{
			Start: attempt.event.start, End: time.Now().UTC(),
		})
		attempt.event.start = time.Time{}
	}
	attempt.mu.Unlock()
	return func() {
		attempt.mu.Lock()
		defer attempt.mu.Unlock()
		if attempt.event.end.IsZero() && ctx.Err() == nil {
			attempt.event.start = time.Now().UTC()
		}
	}
}

type gatewayPoolActiveUsageTracker struct {
	mu       sync.Mutex
	attempts map[*gatewayPoolActiveUseAttempt]struct{}
}

func (s *openAICodexCookieStore) activeUsageTracker(identity string) *gatewayPoolActiveUsageTracker {
	tag := gatewayPoolLedgerTag(identity)
	value, _ := s.poolActiveUsage.LoadOrStore(tag, &gatewayPoolActiveUsageTracker{
		attempts: make(map[*gatewayPoolActiveUseAttempt]struct{}),
	})
	tracker, ok := value.(*gatewayPoolActiveUsageTracker)
	if !ok {
		panic("invalid gateway active-usage tracker")
	}
	return tracker
}

func (t *gatewayPoolActiveUsageTracker) snapshot() []gatewayPoolActiveUseEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]gatewayPoolActiveUseEvent, 0, len(t.attempts))
	for attempt := range t.attempts {
		attempt.mu.Lock()
		event := attempt.event
		event.segments = append([]gatewayPoolUsageInterval(nil), event.segments...)
		out = append(out, event)
		attempt.mu.Unlock()
	}
	return out
}

// Keep exact invalidation evidence with active attempts, including failed-write
// pending entries. No persistence or Body cancellation under the tracker lock.
// A later pair replacement cannot erase this cutoff or affect another ticket.
func (s *openAICodexCookieStore) invalidateGatewayPoolActiveUse(identity, gateway, version string, at time.Time) {
	value, ok := s.poolActiveUsage.Load(gatewayPoolLedgerTag(identity))
	if !ok {
		return
	}
	tracker, ok := value.(*gatewayPoolActiveUsageTracker)
	if !ok {
		return
	}
	ticket := gatewayPoolUsageTicketKey(gateway, version)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	for attempt := range tracker.attempts {
		attempt.mu.Lock()
		if attempt.event.ticket == ticket && (attempt.event.invalidatedAt.IsZero() || at.Before(attempt.event.invalidatedAt)) {
			attempt.event.invalidatedAt = at
		}
		attempt.mu.Unlock()
	}
}

func (t *gatewayPoolActiveUsageTracker) acknowledge(events []gatewayPoolActiveUseEvent, state *gatewayPoolUsageLedger) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, event := range events {
		if event.attempt == nil || event.end.IsZero() {
			continue
		}
		if !event.requestStarted.After(state.ClosedBefore[gatewayPoolUsageSharedModel]) {
			delete(t.attempts, event.attempt) // explicitly closed old request
			continue
		}
		for _, round := range state.Rounds {
			if round.ID == event.roundID && round.ActiveUsage != nil {
				delete(t.attempts, event.attempt)
				break
			}
		}
	}
}

// Called only on the real business-write callback. An attempt owns one entry
// even when post-response quality accounting observes that send again.
func (s *OpenAIGatewayService) registerGatewayPoolActiveUse(ctx context.Context, account *Account, identity, ticket string,
	at, expires time.Time,
) (*gatewayPoolActiveUseAttempt, bool) {
	activity, _ := ctx.Value(gatewayPoolUsageActivityKey{}).(*gatewayPoolUsageActivity)
	if activity == nil {
		return nil, false
	}
	activity.inventory.mu.Lock()
	defer activity.inventory.mu.Unlock()
	if activity.fullUse != nil {
		return activity.fullUse, true
	}
	if activity.closed || ctx.Err() != nil {
		return nil, true // late writes must not revive a finished attempt
	}
	attempt := &gatewayPoolActiveUseAttempt{}
	requestStarted, _ := ctx.Value(gatewayPoolUsageRequestKey{}).(time.Time)
	if requestStarted.IsZero() {
		requestStarted = at
	}
	attempt.event = gatewayPoolActiveUseEvent{ticket: ticket, start: at, sentAt: at, requestStarted: requestStarted,
		expires: expires, attempt: attempt}
	attempt.persist = func() {
		s.changeGatewayPoolUsage(ctx, account, identity, func(*gatewayPoolUsageLedger) bool { return false })
	}
	tracker := s.codexCookies.activeUsageTracker(identity)
	tracker.mu.Lock()
	// Registration and invalidation share the tracker lock. If the route
	// changed after the verified-send snapshot, never invent its lost tail.
	current, _ := s.codexCookies.cachedPoolPair(identity)
	if gatewayPoolUsageTicketKey(current.gateway, current.version) != ticket {
		attempt.event.invalidatedAt = at
	} else if current.invalidated {
		attempt.event.invalidatedAt = current.invalidatedAt
		if current.invalidatedAt.IsZero() {
			attempt.event.invalidatedAt = at
		}
	}
	if len(tracker.attempts) >= gatewayPoolActiveUsagePendingLimit {
		tracker.mu.Unlock()
		return nil, false // bounded: an untracked attempt is not counted, never block business
	}
	tracker.attempts[attempt] = struct{}{}
	tracker.mu.Unlock()
	activity.fullUse = attempt
	// AfterFunc does not hold inventory.mu; cancellation also stops a body
	// whose owner never gets as far as Read/Close.
	stop := context.AfterFunc(ctx, func() { attempt.finish(time.Now().UTC()) })
	attempt.mu.Lock()
	attempt.stop = stop
	ended := !attempt.event.end.IsZero()
	attempt.mu.Unlock()
	if ended {
		stop()
	}
	return attempt, false
}

func (a *gatewayPoolActiveUseAttempt) bind(state *gatewayPoolUsageLedger) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, round := range state.Rounds {
		if round.Model == gatewayPoolUsageSharedModel && round.EndedAt.IsZero() {
			if _, exists := round.Tickets[a.event.ticket]; exists || round.Incomplete {
				a.event.roundID = round.ID
				return
			}
		}
	}
}

// Replay the attested send after a failed lock/read/initial write. Never
// fabricate a later request or bind an old event into a new cycle.
func (s *gatewayPoolUsageLedger) bindActiveUsage(events []gatewayPoolActiveUseEvent) bool {
	changed := false
	for _, event := range events {
		if event.attempt == nil || !event.requestStarted.After(s.ClosedBefore[gatewayPoolUsageSharedModel]) {
			continue
		}
		// A round can already exist while this particular ticket's first
		// write was lost. Replay the send idempotently, not just the binding.
		changed = s.note(gatewayPoolUsageSharedModel, event.ticket, event.sentAt, true) || changed
		event.attempt.bind(s)
	}
	return changed
}

// Merge observed intervals, not elapsed ticket lifetime. Completed entries
// remain in memory until a durable write succeeds, so a failed stop write
// cannot make admin polling continue to accrue idle time.
// Time never observed (a restart mid-request, an untracked attempt) is simply
// not counted.
func (r *GatewayPoolUsageRound) syncActiveUsage(events []gatewayPoolActiveUseEvent, now time.Time) {
	if r.Model != gatewayPoolUsageSharedModel {
		return
	}
	usage, matched := gatewayPoolActiveUsage{}, false
	if r.ActiveUsage != nil {
		usage.Intervals = append([]gatewayPoolUsageInterval(nil), r.ActiveUsage.Intervals...)
	}
	for _, event := range events {
		if event.roundID != r.ID {
			continue
		}
		matched = true
		for _, interval := range event.observed(now, r.EndedAt) {
			usage.add(interval)
		}
	}
	if r.ActiveUsage == nil && !matched {
		return
	}
	r.ActiveUsage = &usage
}

// observed is what this attempt has used so far, stopped at its ticket's
// expiry or invalidation and at limit (zero = none).
func (e gatewayPoolActiveUseEvent) observed(now, limit time.Time) []gatewayPoolUsageInterval {
	end := now
	if !e.end.IsZero() {
		end = e.end
	}
	for _, bound := range []time.Time{e.expires, e.invalidatedAt, limit} {
		if !bound.IsZero() && bound.Before(end) {
			end = bound
		}
	}
	intervals := make([]gatewayPoolUsageInterval, 0, len(e.segments)+1)
	for _, segment := range e.segments {
		if segment.End.After(end) {
			segment.End = end
		}
		intervals = append(intervals, segment)
	}
	if !e.start.IsZero() {
		intervals = append(intervals, gatewayPoolUsageInterval{Start: e.start, End: end})
	}
	return intervals
}

// gatewayPoolTicketFullUse is how long business used this pair at full strength
// in the current cycle, up to until. A pair is replaced only once stale and every
// attempt stops at its pair's expiry or invalidation, so the cycle's active time
// since this pair's first send is this ticket's alone.
func (s *openAICodexCookieStore) gatewayPoolTicketFullUse(identity string, pair openAIGatewayPoolPair, until time.Time) time.Duration {
	if pair.firstSent.IsZero() || !until.After(pair.firstSent) {
		return 0
	}
	tag := gatewayPoolLedgerTag(identity)
	// Pending attempts before the persisted cycle: one acknowledged in between
	// is then counted twice, which the union absorbs, rather than not at all.
	var events []gatewayPoolActiveUseEvent
	if value, ok := s.poolActiveUsage.Load(tag); ok {
		if tracker, ok := value.(*gatewayPoolActiveUsageTracker); ok {
			events = tracker.snapshot()
		}
	}
	var usage gatewayPoolActiveUsage
	var closed time.Time
	if value, ok := s.poolUsageCache.Load(tag); ok {
		if state, ok := value.(*gatewayPoolUsageLedger); ok {
			// As in binding: a request started before the last close is an earlier cycle's.
			if at := state.ClosedBefore[gatewayPoolUsageSharedModel]; at.Before(until) {
				closed = at
			}
			for i := range state.Rounds {
				r := &state.Rounds[i]
				if r.Model != gatewayPoolUsageSharedModel || r.ActiveUsage == nil || (!r.EndedAt.IsZero() && r.EndedAt.Before(until)) {
					continue
				}
				// Only this pair's span, so the interval cap never drops its latest use.
				for _, interval := range r.ActiveUsage.Intervals {
					if interval.End.After(pair.firstSent) && interval.Start.Before(until) {
						usage.Intervals = append(usage.Intervals, interval)
					}
				}
			}
		}
	}
	ticket := gatewayPoolUsageTicketKey(pair.gateway, pair.version)
	for _, event := range events {
		if event.ticket != ticket || !event.requestStarted.After(closed) {
			continue
		}
		for _, interval := range event.observed(until, time.Time{}) {
			usage.add(interval)
		}
	}
	var used time.Duration
	for _, interval := range usage.Intervals {
		start, end := interval.Start, interval.End
		if start.Before(pair.firstSent) {
			start = pair.firstSent
		}
		if end.After(until) {
			end = until
		}
		if end.After(start) {
			used += end.Sub(start)
		}
	}
	return used
}

func (s *gatewayPoolUsageLedger) syncActiveUsage(events []gatewayPoolActiveUseEvent, now time.Time) bool {
	changed := false
	for i := range s.Rounds {
		before := s.Rounds[i].ActiveUsage
		s.Rounds[i].syncActiveUsage(events, now)
		changed = changed || !reflect.DeepEqual(before, s.Rounds[i].ActiveUsage)
	}
	return changed
}
