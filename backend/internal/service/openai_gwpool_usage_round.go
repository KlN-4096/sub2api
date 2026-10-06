package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"time"
)

const (
	gatewayPoolUsageExtraKey       = "openai_gwpool_usage_rounds"
	gatewayPoolUsageTagKey         = "openai_gwpool_usage_tag"
	gatewayPoolUsagePreviousTagKey = "openai_gwpool_usage_previous_tag"
	gatewayPoolUsageHistoryLimit   = 100
	gatewayPoolUsageTicketLimit    = 8192
	gatewayPoolUsageSharedModel    = "all"
)

type gatewayPoolUsageTicket struct {
	At            time.Time `json:"at"`
	Full          bool      `json:"full"`
	UseStartedAt  time.Time `json:"use_started_at,omitzero"`
	UseObservedAt time.Time `json:"use_observed_at,omitzero"`
	UseEndedAt    time.Time `json:"use_ended_at,omitzero"`
	UseExpiresAt  time.Time `json:"use_expires_at,omitzero"`
	UseSession    string    `json:"use_session,omitempty"`
	UseMS         int64     `json:"use_ms,omitempty"`
}

type GatewayPoolUsageRound struct {
	ID                 string                            `json:"id"`
	Model              string                            `json:"model"`
	StartedAt          time.Time                         `json:"started_at"`
	EndedAt            time.Time                         `json:"ended_at,omitzero"`
	EndReason          string                            `json:"end_reason,omitempty"`
	Attempted          int                               `json:"attempted"`
	Full               int                               `json:"full"`
	Incomplete         bool                              `json:"incomplete,omitempty"`
	Tickets            map[string]gatewayPoolUsageTicket `json:"tickets,omitempty"`
	FullStartedAt      time.Time                         `json:"full_started_at,omitzero"`
	FullDurationMS     int64                             `json:"full_duration_ms"`
	FullActiveUntil    []time.Time                       `json:"full_active_until,omitempty"`
	DurationIncomplete bool                              `json:"duration_incomplete,omitempty"`
}

type GatewayPoolUsageArchive struct {
	Rounds             int64 `json:"rounds"`
	Attempted          int64 `json:"attempted"`
	Full               int64 `json:"full"`
	DurationMS         int64 `json:"duration_ms"`
	Incomplete         bool  `json:"incomplete,omitempty"`
	DurationIncomplete bool  `json:"duration_incomplete,omitempty"`
}

type gatewayPoolUsageLedger struct {
	Tag                    string                             `json:"tag"`
	UpdatedAt              time.Time                          `json:"updated_at"`
	Rounds                 []GatewayPoolUsageRound            `json:"rounds"`
	Archived               map[string]GatewayPoolUsageArchive `json:"archived"`
	ClosedBefore           map[string]time.Time               `json:"closed_before"`
	Incomplete             bool                               `json:"incomplete,omitempty"`
	Previous               *gatewayPoolUsageLedger            `json:"previous,omitempty"`
	LastRequestStartedAt   time.Time                          `json:"last_request_started_at,omitzero"`
	LastRequestCompletedAt time.Time                          `json:"last_request_completed_at,omitzero"`
	BlockedAt              time.Time                          `json:"blocked_at,omitzero"`
	VerificationSequence   uint64                             `json:"verification_sequence,omitempty"`
	VerificationStartedAt  time.Time                          `json:"verification_started_at,omitzero"`
}

func gatewayPoolUsageDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func readGatewayPoolUsage(account *Account, tag string) gatewayPoolUsageLedger {
	var state gatewayPoolUsageLedger
	if account != nil {
		raw, _ := json.Marshal(account.Extra[gatewayPoolUsageExtraKey])
		_ = json.Unmarshal(raw, &state)
	}
	if state.Tag == tag {
		return state
	}
	if state.Previous != nil && state.Previous.Tag == tag {
		previous := state
		state = *state.Previous
		previous.Previous = nil
		state.Previous = &previous
		return state
	}
	previous := state
	previous.Previous = nil
	state = gatewayPoolUsageLedger{Tag: tag}
	if previous.Tag != "" {
		state.Previous = &previous
	}
	return state
}

// The entire snapshot is serialized by ledger, including clones. Each write
// first adopts the newest durable snapshot; no per-row totals are ever summed.
func (s *OpenAIGatewayService) changeGatewayPoolUsage(ctx context.Context, account *Account, identity string,
	change func(*gatewayPoolUsageLedger) bool,
	alreadyRecorded ...func(*gatewayPoolUsageLedger) bool,
) bool {
	if s == nil || s.accountRepo == nil || account == nil || identity == "" {
		return false
	}
	tag := gatewayPoolLedgerTag(identity)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer cancel()
	value, _ := s.codexCookies.poolUsageLocks.LoadOrStore(tag, &sync.Mutex{})
	lock, ok := value.(*sync.Mutex)
	if !ok || lock == nil {
		slog.Warn("gwpool_usage_lock_unavailable", "account_id", account.ID)
		return false
	}
	if !gatewayPoolLockWithin(ctx, lock) {
		slog.Warn("gwpool_usage_lock_timeout", "account_id", account.ID)
		return false
	}
	defer lock.Unlock()
	if cached, exists := s.codexCookies.poolUsageCache.Load(tag); exists && len(alreadyRecorded) > 0 {
		if state, ok := cached.(*gatewayPoolUsageLedger); ok && alreadyRecorded[0](state) {
			return true
		}
	}
	value, _ = s.codexCookies.poolHistoryLocks.LoadOrStore(account.ID, &sync.Mutex{})
	historyLock, ok := value.(*sync.Mutex)
	if !ok || historyLock == nil {
		slog.Warn("gwpool_usage_history_lock_unavailable", "account_id", account.ID)
		return false
	}
	if !gatewayPoolLockWithin(ctx, historyLock) {
		slog.Warn("gwpool_usage_history_lock_timeout", "account_id", account.ID)
		return false
	}
	defer historyLock.Unlock()
	fresh, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || fresh == nil {
		slog.Warn("gwpool_usage_read_failed", "account_id", account.ID)
		return false
	}
	current, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if err != nil || gatewayPoolLedgerTag(current) != tag {
		return false // identity changed while an old request was in flight
	}
	state := readGatewayPoolUsage(fresh, tag)
	blockedAt := gatewayPoolUsageBlockedAt(fresh)
	peers, err := s.gatewayPoolStatePeers(ctx, tag, "usage")
	if err != nil {
		slog.Warn("gwpool_usage_peers_unavailable", "account_id", account.ID)
		return false // do not overwrite a possibly newer clone snapshot
	}
	for i := range peers {
		if blocked := gatewayPoolUsageBlockedAt(&peers[i]); blocked.After(blockedAt) {
			blockedAt = blocked
		}
		other := readGatewayPoolUsage(&peers[i], tag)
		if other.UpdatedAt.After(state.UpdatedAt) {
			previous := state.Previous
			state = other
			state.Previous = previous
		}
	}
	settled := state.settleFullUsage(s.codexCookies.gatewayPoolUsageLive(identity), s.codexCookies.gatewayPoolUsageSession(), time.Now())
	boundary := state.applyGatewayPoolBlock(blockedAt)
	if !change(&state) && !settled && !boundary {
		s.codexCookies.poolUsageCache.Store(tag, &state)
		return true
	}
	state.prune()
	updated := time.Now().UTC()
	if !updated.After(state.UpdatedAt) {
		updated = state.UpdatedAt.Add(time.Nanosecond)
	}
	state.UpdatedAt = updated
	previousTag := ""
	if state.Previous != nil {
		previousTag = state.Previous.Tag
	}
	if err := s.accountRepo.UpdateExtra(ctx, fresh.ID, map[string]any{
		gatewayPoolUsageExtraKey: state, gatewayPoolUsageTagKey: tag, gatewayPoolUsagePreviousTagKey: previousTag,
	}); err != nil {
		slog.Warn("gwpool_usage_write_failed", "account_id", account.ID)
		return false
	}
	s.codexCookies.poolUsageCache.Store(tag, &state)
	return true
}

func (r *gatewayPoolUsageLedger) note(model, ticket string, at time.Time, full bool) bool {
	if model == "" || len(model) > 128 || ticket == "" || at.IsZero() {
		return false
	}
	model = gatewayPoolUsageSharedModel
	for i := range r.Rounds {
		round := &r.Rounds[i]
		if round.Model != model {
			continue
		}
		if !round.EndedAt.IsZero() && at.After(round.EndedAt) {
			continue // the same physical ticket may be used in a later cycle
		}
		if seen, exists := round.Tickets[ticket]; exists {
			changed := false
			if full && !seen.Full {
				seen.Full, changed = true, true
				round.Full++
			}
			if at.Before(seen.At) {
				seen.At, changed = at, true
			}
			if at.Before(round.StartedAt) {
				round.StartedAt, changed = at, true
			}
			round.Tickets[ticket] = seen
			return changed
		}
	}
	if end := r.ClosedBefore[model]; !end.IsZero() && !at.After(end) {
		return false // a late event is never the first event of a new round
	}
	var round *GatewayPoolUsageRound
	for i := range r.Rounds {
		if r.Rounds[i].Model == model && r.Rounds[i].EndedAt.IsZero() {
			round = &r.Rounds[i]
			break
		}
	}
	if round == nil {
		r.Rounds = append(r.Rounds, GatewayPoolUsageRound{ID: gatewayPoolUsageDigest(r.Tag + "\x00" + model + "\x00" + ticket + "\x00" + at.UTC().Format(time.RFC3339Nano)),
			Model: model, StartedAt: at, Tickets: map[string]gatewayPoolUsageTicket{}})
		round = &r.Rounds[len(r.Rounds)-1]
	}
	if len(round.Tickets) >= gatewayPoolUsageTicketLimit {
		changed := !round.Incomplete
		round.Incomplete = true
		return changed // explicitly lower-bound counts, never pretend truncated data is exact
	}
	if round.Tickets == nil {
		round.Tickets = map[string]gatewayPoolUsageTicket{}
	}
	round.Tickets[ticket] = gatewayPoolUsageTicket{At: at, Full: full}
	round.Attempted++
	if full {
		round.Full++
	}
	if at.Before(round.StartedAt) {
		round.StartedAt = at
	}
	return true
}

// Unlike Mutex.Lock, the complete wait is covered by the persistence budget.
func gatewayPoolLockWithin(ctx context.Context, lock *sync.Mutex) bool {
	const retryGap = 5 * time.Millisecond
	if ctx.Err() != nil {
		return false
	}
	if lock.TryLock() {
		return true
	}
	ticker := time.NewTicker(retryGap)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if ctx.Err() == nil && lock.TryLock() {
				return true
			}
		}
	}
}

func (r *gatewayPoolUsageLedger) end(at time.Time, reasons ...string) bool {
	reason := "observed_exhausted"
	if len(reasons) > 0 {
		reason = reasons[0]
	}
	changed := false
	if r.ClosedBefore == nil {
		r.ClosedBefore = map[string]time.Time{}
	}
	if at.After(r.ClosedBefore[gatewayPoolUsageSharedModel]) {
		r.ClosedBefore[gatewayPoolUsageSharedModel] = at
		changed = true
	}
	if !r.VerificationStartedAt.IsZero() && !at.Before(r.VerificationStartedAt) {
		r.VerificationSequence = 0
		r.VerificationStartedAt = time.Time{}
		changed = true
	}
	for i := range r.Rounds {
		round := &r.Rounds[i]
		if round.Model != gatewayPoolUsageSharedModel || !round.EndedAt.IsZero() || at.Before(round.StartedAt) {
			continue
		}
		round.EndedAt, round.EndReason = at, reason
		for key, ticket := range round.Tickets {
			if !ticket.UseStartedAt.IsZero() && (ticket.UseEndedAt.IsZero() || ticket.UseEndedAt.After(at)) {
				ticket.UseEndedAt = at
				round.Tickets[key] = ticket
			}
		}
		r.ClosedBefore[round.Model] = at
		changed = true
	}
	return changed
}

func (r *gatewayPoolUsageLedger) prune() {
	sort.SliceStable(r.Rounds, func(i, j int) bool { return r.Rounds[i].StartedAt.After(r.Rounds[j].StartedAt) })
	kept := make([]GatewayPoolUsageRound, 0, min(len(r.Rounds), gatewayPoolUsageHistoryLimit))
	for _, round := range r.Rounds {
		if len(kept) < gatewayPoolUsageHistoryLimit || round.EndedAt.IsZero() {
			kept = append(kept, round)
			continue
		}
		if r.Archived == nil {
			r.Archived = map[string]GatewayPoolUsageArchive{}
		}
		total := r.Archived[round.Model]
		total.Rounds++
		total.Attempted += int64(round.Attempted)
		total.Full += int64(round.Full)
		if round.Model == gatewayPoolUsageSharedModel {
			total.DurationMS += round.fullUseDuration(round.EndedAt)
		} else if duration := round.EndedAt.Sub(round.StartedAt).Milliseconds(); duration > 0 {
			total.DurationMS += duration // legacy wall-clock data remains separate
		}
		total.DurationIncomplete = total.DurationIncomplete || round.DurationIncomplete
		total.Incomplete = total.Incomplete || round.Incomplete
		r.Archived[round.Model] = total
	}
	r.Rounds = kept
}

func (s *OpenAIGatewayService) noteGatewayPoolUsage(ctx context.Context, account *Account, identity, model string,
	applied OpenAIGatewayPoolApplied, at time.Time, full bool,
) {
	if at.IsZero() || applied.AccountID != account.ID || applied.Version == "" || applied.Gateway == "" {
		return
	}
	at = gatewayPoolUsageEventAt(ctx, at)
	ticket := gatewayPoolUsageDigest(applied.Gateway + "\x00" + applied.Version)
	s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
		if state.requestClosed(ctx) {
			return false
		}
		return state.note(model, ticket, at, full)
	}, func(state *gatewayPoolUsageLedger) bool {
		for _, round := range state.Rounds {
			if round.Model != gatewayPoolUsageSharedModel || !round.EndedAt.IsZero() {
				continue
			}
			if seen, exists := round.Tickets[ticket]; exists {
				return (!full || seen.Full) && !at.Before(seen.At)
			}
		}
		return false
	})
}

// Listing is only done at an existing foreground completion/error boundary,
// never on the admin polling path. Zero candidates is not a rotation threshold.
func (s *OpenAIGatewayService) finishGatewayPoolUsageIfExhausted(ctx context.Context, account *Account) {
	if s == nil || s.accountRepo == nil || !s.codexCookies.gatewayPoolTakeover(account) {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), account.gatewayPoolListTimeout())
	defer cancel()
	fresh, err := s.codexCookies.freshGatewayPoolAccount(ctx, account)
	if err != nil || fresh == nil {
		return
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if err != nil {
		return
	}
	generation, active, candidates := s.codexCookies.gatewayPoolInventoryCandidates(identity, fresh)
	if active || len(candidates) != 0 {
		return
	}
	if err := s.codexCookies.hydrateGatewayPoolSharedHistory(ctx, fresh, identity); err != nil {
		return
	}
	pool, err := s.codexCookies.poolClient(fresh)
	if err != nil {
		return
	}
	listedAt := time.Now()
	gateways, err := pool.Gateways(ctx, gatewayPoolUpstreamAccountID(identity), gatewayPoolAccountTag(fresh, identity))
	if err != nil {
		return
	}
	for _, gateway := range gateways {
		s.codexCookies.noteGatewayPoolRecommendationAt(identity, gateway.Name, gateway.Cooldown, listedAt)
		_, cooling := s.codexCookies.gatewayPoolUsedAt(identity, gateway.Name, fresh.gatewayPoolGatewayWindow(), fresh.gatewayPoolUseRecommendation())
		if gateway.PairReady && !cooling {
			return
		}
	}
	// An unreserved experimental early opportunity is not a normal candidate
	// or work in flight. Its next foreground attempt starts a new cycle.
	s.changeGatewayPoolUsage(ctx, fresh, identity, func(state *gatewayPoolUsageLedger) bool {
		after, busy, remaining := s.codexCookies.gatewayPoolInventoryCandidates(identity, fresh)
		if after != generation || busy || len(remaining) != 0 {
			return false
		}
		inventory := s.codexCookies.gatewayPoolInventory(identity)
		inventory.mu.Lock()
		defer inventory.mu.Unlock()
		if inventory.generation != generation || inventory.active != 0 {
			return false
		}
		return state.end(time.Now().UTC())
	})
}
