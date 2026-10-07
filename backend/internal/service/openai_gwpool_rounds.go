package service

import (
	"context"
	"sync"
	"time"
)

const gatewayPoolSelectionRechecks = 3

// Rounds are process-local and group-scoped. Restarting a round only changes
// account eligibility: gateway cooldown, rate limits and request budgets survive.
type gatewayPoolRounds struct {
	mu      sync.Mutex
	groups  map[int64]*gatewayPoolRound
	touched map[string]time.Time
}

type gatewayPoolRound struct {
	generation uint64
	exhausted  map[string]struct{}
	current    string
	resting    map[string]time.Time
}

func (r *gatewayPoolRounds) groupLocked(group int64) *gatewayPoolRound {
	if r.groups == nil {
		r.groups = map[int64]*gatewayPoolRound{}
	}
	if r.groups[group] == nil {
		r.groups[group] = &gatewayPoolRound{exhausted: map[string]struct{}{}, resting: map[string]time.Time{}}
	}
	state := r.groups[group]
	for domain, until := range state.resting {
		if !time.Now().Before(until) {
			delete(state.resting, domain)
			delete(state.exhausted, domain)
		}
	}
	return state
}

func (r *gatewayPoolRounds) rest(group int64, identity string, until time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.groupLocked(group)
	domain := gatewayPoolLedgerIdentity(identity)
	state.resting[domain] = until
	state.exhausted[domain] = struct{}{}
	if state.current == domain {
		state.current = ""
	}
}

func (r *gatewayPoolRounds) generation(group int64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.groupLocked(group).generation
}

func (r *gatewayPoolRounds) exhaust(group int64, identity string, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.groupLocked(group)
	// A confirmation started before a concurrent reset cannot exhaust the new round.
	if identity != "" && state.generation == generation {
		domain := gatewayPoolLedgerIdentity(identity)
		state.exhausted[domain] = struct{}{}
		if state.current == domain {
			state.current = ""
		}
	}
}

// Keep the selected credential across independent sessions. Unavailable or
// request-incompatible credentials do not prevent a healthy replacement.
func (r *gatewayPoolRounds) current(group int64, domains map[int64]string, complete bool) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.groupLocked(group)
	for _, domain := range domains {
		if domain == state.current {
			return state.current
		}
	}
	if complete {
		state.current = ""
	}
	return state.current
}

func (r *gatewayPoolRounds) claim(group int64, identity string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.groupLocked(group)
	domain := gatewayPoolLedgerIdentity(identity)
	if _, exhausted := state.exhausted[domain]; exhausted {
		return false
	}
	if state.current == "" {
		state.current = domain
	}
	return state.current == domain
}

func (r *gatewayPoolRounds) touch(identity string, at time.Time) {
	if identity == "" || at.IsZero() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.touched == nil {
		r.touched = map[string]time.Time{}
	}
	key := gatewayPoolLedgerIdentity(identity)
	if at.After(r.touched[key]) {
		r.touched[key] = at
	}
}

func (r *gatewayPoolRounds) lastTouch(identity string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.touched[gatewayPoolLedgerIdentity(identity)]
}

func (r *gatewayPoolRounds) blocked(group int64, identity string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, blocked := r.groupLocked(group).exhausted[gatewayPoolLedgerIdentity(identity)]
	return blocked
}

// Eligibility is computed without per-request exclusions, so an attempted or
// excluded account cannot make the remaining cohort appear fully exhausted.
func (r *gatewayPoolRounds) snapshot(group int64, domains map[int64]string, complete bool) (map[int64]struct{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.groupLocked(group)
	all := complete && len(domains) > 0
	for _, domain := range domains {
		if _, exhausted := state.exhausted[domain]; !exhausted {
			all = false
		}
	}
	if all {
		state.generation++
		state.exhausted = map[string]struct{}{}
		state.current = ""
		for domain := range state.resting {
			state.exhausted[domain] = struct{}{}
		}
	}
	excluded := map[int64]struct{}{}
	for id, domain := range domains {
		if _, exhausted := state.exhausted[domain]; exhausted {
			excluded[id] = struct{}{}
		}
	}
	return excluded, state.generation > 0
}

type gatewayPoolRoundExclusionsKey struct{}

func gatewayPoolRoundExclusions(ctx context.Context, excluded map[int64]struct{}) map[int64]struct{} {
	shared, _ := ctx.Value(gatewayPoolRoundExclusionsKey{}).(map[int64]struct{})
	if len(shared) == 0 {
		return excluded
	}
	merged := cloneExcludedAccountIDs(excluded)
	if merged == nil {
		merged = map[int64]struct{}{}
	}
	for id := range shared {
		merged[id] = struct{}{}
	}
	return merged
}

func (s *OpenAIGatewayService) gatewayPoolRoundSelectionAllowed(ctx context.Context, group *int64, selection *AccountSelectionResult) bool {
	if group == nil || selection == nil || selection.Account == nil || !selection.Account.IsOpenAIOAuthLike() {
		return true
	}
	account := selection.Account
	known, _ := s.codexCookies.poolRotationAccounts.Load(account.ID)
	if !gatewayPoolRotationAccount(account, *group) && known != true {
		return true // ordinary OAuth rows must not gain a new failure-prone DB gate
	}
	if s.accountRepo != nil {
		fresh, err := s.accountRepo.GetByID(ctx, account.ID)
		if err != nil || fresh == nil {
			return false
		}
		account = fresh
	}
	if !gatewayPoolRotationAccount(account, *group) {
		return true
	}
	if !account.IsSchedulable() {
		return false
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil || s.codexCookies.poolRounds.blocked(*group, identity) {
		return false
	}
	if s.codexCookies.gatewayPoolVerifiedFull(identity) {
		s.codexCookies.poolRounds.claim(*group, identity)
		return true // don't evict an already running session's verified window
	}
	if s.gatewayPoolNoRemainingRoutes(ctx, account) {
		s.codexCookies.poolRounds.exhaust(*group, identity, s.codexCookies.poolRounds.generation(*group))
		s.restGatewayPoolAccount(ctx, account, identity, *group)
		return false
	}
	return s.codexCookies.poolRounds.claim(*group, identity)
}
