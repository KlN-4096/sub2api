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
	Intervals  []gatewayPoolUsageInterval `json:"intervals,omitempty"`
	Session    string                     `json:"session"`
	Open       bool                       `json:"open,omitempty"`
	Incomplete bool                       `json:"incomplete,omitempty"`
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
		merged = merged[:gatewayPoolActiveUsageIntervalLimit]
		u.Incomplete = true // a lower bound, never fill idle gaps to compress
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
	invalidatedAt   time.Time // independent of legacy ticket-lifetime fields
	sentAt          time.Time
	requestStarted  time.Time
	attempt         *gatewayPoolActiveUseAttempt
	segments        []gatewayPoolUsageInterval
	dropped         bool
	incomplete      bool
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
	dropped  gatewayPoolUsageInterval
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
	if !t.dropped.Start.IsZero() {
		out = append(out, gatewayPoolActiveUseEvent{start: t.dropped.Start, end: t.dropped.End, dropped: true})
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
		attempt.event.invalidatedAt, attempt.event.incomplete = at, true
	} else if current.invalidated {
		attempt.event.invalidatedAt = current.invalidatedAt
		if current.invalidatedAt.IsZero() {
			attempt.event.invalidatedAt, attempt.event.incomplete = at, true
		}
	}
	if len(tracker.attempts) >= gatewayPoolActiveUsagePendingLimit {
		if tracker.dropped.Start.IsZero() || at.Before(tracker.dropped.Start) {
			tracker.dropped.Start = at
		}
		if at.After(tracker.dropped.End) {
			tracker.dropped.End = at
		}
		tracker.mu.Unlock()
		return nil, false // bounded lower-bound reporting, never block business
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
func (r *GatewayPoolUsageRound) syncActiveUsage(events []gatewayPoolActiveUseEvent, session string, now time.Time) {
	if r.Model != gatewayPoolUsageSharedModel {
		return
	}
	var matching []gatewayPoolActiveUseEvent
	incomplete := false
	for _, event := range events {
		if event.dropped {
			if !event.end.Before(r.StartedAt) && (r.EndedAt.IsZero() || !event.start.After(r.EndedAt)) {
				incomplete = true
			}
			continue
		}
		if event.roundID == r.ID {
			matching = append(matching, event)
			incomplete = incomplete || event.incomplete
		}
	}
	if r.ActiveUsage == nil && len(matching) == 0 && !incomplete {
		return
	}
	usage := gatewayPoolActiveUsage{}
	if r.ActiveUsage != nil {
		usage = *r.ActiveUsage
		usage.Intervals = append([]gatewayPoolUsageInterval(nil), usage.Intervals...)
	}
	if usage.Open && (usage.Session != session || len(matching) == 0) {
		usage.Incomplete = true // retain the last observation, not downtime
	}
	usage.Session, usage.Open = session, false
	usage.Incomplete = usage.Incomplete || incomplete
	r.FullActiveUntil = nil
	for _, event := range matching {
		end := now
		if !event.end.IsZero() {
			end = event.end
		}
		for _, bound := range []time.Time{event.expires, event.invalidatedAt, r.EndedAt} {
			if !bound.IsZero() && bound.Before(end) {
				end = bound
			}
		}
		for _, segment := range event.segments {
			if segment.End.After(end) {
				segment.End = end
			}
			usage.add(segment)
		}
		if !event.start.IsZero() {
			usage.add(gatewayPoolUsageInterval{Start: event.start, End: end})
		}
		if event.end.IsZero() && !end.Before(now) {
			// Open tracks uncommitted process state, including a paused
			// confirmation. It must not itself add elapsed time.
			usage.Open = true
			if !event.start.IsZero() && !end.Before(now) {
				r.FullActiveUntil = append(r.FullActiveUntil, event.expires)
			}
		}
	}
	r.ActiveUsage = &usage
}

func (s *gatewayPoolUsageLedger) syncActiveUsage(events []gatewayPoolActiveUseEvent, session string, now time.Time) bool {
	changed := false
	for i := range s.Rounds {
		before := s.Rounds[i].ActiveUsage
		s.Rounds[i].syncActiveUsage(events, session, now)
		changed = changed || !reflect.DeepEqual(before, s.Rounds[i].ActiveUsage)
	}
	return changed
}
