package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	gatewayPoolRestStateKey       = "openai_gwpool_rest_state"
	gatewayPoolRestTagKey         = "openai_gwpool_rest_tag"
	gatewayPoolRestPreviousTagKey = "openai_gwpool_rest_previous_tag"
)

type gatewayPoolRestState struct {
	Tag       string                `json:"tag"`
	Active    bool                  `json:"active"`
	ChangedAt time.Time             `json:"changed_at"`
	NextCheck time.Time             `json:"next_check,omitzero"`
	Previous  *gatewayPoolRestState `json:"previous,omitempty"`
}

func readGatewayPoolRest(account *Account, tag string) gatewayPoolRestState {
	var state gatewayPoolRestState
	if account != nil {
		raw, _ := json.Marshal(account.Extra[gatewayPoolRestStateKey])
		_ = json.Unmarshal(raw, &state)
	}
	if state.Tag == tag {
		return state
	}
	previous := state
	previous.Previous = nil
	if state.Previous != nil && state.Previous.Tag == tag {
		state = *state.Previous
	} else {
		state = gatewayPoolRestState{Tag: tag}
	}
	if previous.Tag != "" {
		state.Previous = &previous
	}
	return state
}

func (s *OpenAIGatewayService) lockGatewayPoolRest(ctx context.Context, tag string) (func(), error) {
	value, _ := s.codexCookies.poolRestLocks.LoadOrStore(tag, &sync.Mutex{})
	lock, ok := value.(*sync.Mutex)
	if !ok || lock == nil || !gatewayPoolLockWithin(ctx, lock) {
		return nil, errors.New("gateway rest lock unavailable")
	}
	return lock.Unlock, nil
}

// Caller holds the domain rest lock. Peers carry snapshots, not additive data;
// an inactive record is a durable tombstone, so old clone rows cannot revive it.
func (s *OpenAIGatewayService) loadGatewayPoolRest(ctx context.Context, account *Account, identity string) (gatewayPoolRestState, *Account, error) {
	tag := gatewayPoolLedgerTag(identity)
	fresh := account
	if s.accountRepo != nil {
		var err error
		fresh, err = s.accountRepo.GetByID(ctx, account.ID)
		if err != nil || fresh == nil {
			return gatewayPoolRestState{}, nil, errors.New("gateway rest account unavailable")
		}
		current, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
		if err != nil || gatewayPoolLedgerTag(current) != tag || !fresh.UsesGatewayPool() {
			return gatewayPoolRestState{}, nil, errors.New("gateway rest identity changed")
		}
	}
	state := readGatewayPoolRest(fresh, tag)
	adopt := func(other gatewayPoolRestState) {
		if other.ChangedAt.After(state.ChangedAt) {
			previous := state.Previous
			state = other
			state.Previous = previous
		}
	}
	if s.accountRepo != nil {
		peers, err := s.gatewayPoolStatePeers(ctx, tag, "rest")
		if err != nil {
			return state, fresh, err
		}
		for i := range peers {
			adopt(readGatewayPoolRest(&peers[i], tag))
		}
	}
	if cached, ok := s.codexCookies.poolRestState.Load(tag); ok {
		if other, valid := cached.(gatewayPoolRestState); valid {
			adopt(other)
		}
	}
	return state, fresh, nil
}

// Production repositories commit the rest latch and temporary block together.
// Small test repositories may use the ordered fallback (latch before block).
type gatewayPoolRestRepository interface {
	SetGatewayPoolRest(context.Context, int64, time.Time, string, map[string]any) error
}

func (s *OpenAIGatewayService) writeGatewayPoolRest(ctx context.Context, account *Account, identity string, state gatewayPoolRestState, generation ...uint64) error {
	lockInventory := func() (func(), error) {
		if len(generation) == 0 {
			return func() {}, nil
		}
		inventory := s.codexCookies.gatewayPoolInventory(identity)
		if !gatewayPoolLockWithin(ctx, &inventory.mu) {
			return nil, errors.New("gateway recovery inventory lock unavailable")
		}
		if inventory.active != 0 || inventory.generation != generation[0] {
			inventory.mu.Unlock()
			return nil, errors.New("gateway recovery inventory changed")
		}
		return inventory.mu.Unlock, nil
	}
	if s.accountRepo == nil {
		unlock, err := lockInventory()
		if err != nil {
			return err
		}
		defer unlock()
		s.codexCookies.poolRestState.Store(state.Tag, state)
		return nil
	}
	value, _ := s.codexCookies.poolHistoryLocks.LoadOrStore(account.ID, &sync.Mutex{})
	lock, ok := value.(*sync.Mutex)
	if !ok || lock == nil || !gatewayPoolLockWithin(ctx, lock) {
		return errors.New("gateway rest history lock unavailable")
	}
	defer lock.Unlock()
	fresh, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || fresh == nil {
		return errors.New("gateway rest write account unavailable")
	}
	current, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if err != nil || gatewayPoolLedgerTag(current) != state.Tag || !fresh.UsesGatewayPool() {
		return errors.New("gateway rest write identity changed")
	}
	// Lock order matches usage maintenance: history, then inventory. Hold the
	// final generation check through durable publication, not just the listing.
	unlockInventory, err := lockInventory()
	if err != nil {
		return err
	}
	defer unlockInventory()
	previousTag := ""
	if state.Previous != nil {
		previousTag = state.Previous.Tag
	}
	patch := map[string]any{
		gatewayPoolRestStateKey: state, gatewayPoolRestTagKey: state.Tag, gatewayPoolRestPreviousTagKey: previousTag,
	}
	if atomicRepo, ok := s.accountRepo.(gatewayPoolRestRepository); ok && state.Active {
		err = atomicRepo.SetGatewayPoolRest(ctx, account.ID, state.NextCheck, gatewayPoolRestReason(fresh), patch)
	} else {
		err = s.accountRepo.UpdateExtra(ctx, account.ID, patch)
		if err == nil && state.Active {
			err = s.accountRepo.SetTempUnschedulable(ctx, account.ID, state.NextCheck, gatewayPoolRestReason(fresh))
		}
	}
	if err != nil {
		return err
	}
	s.codexCookies.poolRestState.Store(state.Tag, state)
	return nil
}

func gatewayPoolRestReason(account *Account) string {
	return fmt.Sprintf("网关候选低于%d，休息后达到%d才恢复 / Gateway candidates below %d; resume at %d",
		account.gatewayPoolRotationMinGateways(), account.gatewayPoolResumeGateways(),
		account.gatewayPoolRotationMinGateways(), account.gatewayPoolResumeGateways())
}

func (s *gatewayPoolRestState) advance(now time.Time) {
	if !now.After(s.ChangedAt) {
		now = s.ChangedAt.Add(time.Nanosecond)
	}
	s.ChangedAt = now
}

func (s *OpenAIGatewayService) enterGatewayPoolRest(ctx context.Context, account *Account, identity string, at, until time.Time) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer cancel()
	tag := gatewayPoolLedgerTag(identity)
	unlock, err := s.lockGatewayPoolRest(ctx, tag)
	if err != nil {
		return err
	}
	defer unlock()
	state, fresh, err := s.loadGatewayPoolRest(ctx, account, identity)
	if err != nil {
		// A known shortage stays blocked locally even when persistence fails.
		state = gatewayPoolRestState{Tag: tag, Active: true, ChangedAt: at, NextCheck: until}
		s.codexCookies.poolRestState.Store(tag, state)
		return err
	}
	if at.Before(state.ChangedAt) {
		return nil // late shortage result cannot undo a newer recovery
	}
	state.Active = true
	state.NextCheck = until
	state.advance(at)
	s.codexCookies.poolRestState.Store(tag, state) // fail closed even on a write failure
	return s.writeGatewayPoolRest(ctx, fresh, identity, state)
}

// allowResume is only used at a pre-send scheduling boundary, before this
// request registers work. Last-mile sends and early reservations only check;
// they cannot turn a resting domain into an active one while work is in flight.
func (s *OpenAIGatewayService) gatewayPoolResumeAllowed(ctx context.Context, account *Account, allowResume bool) (bool, error) {
	if s == nil || !s.codexCookies.gatewayPoolTakeover(account) {
		return true, nil
	}
	if s.accountRepo == nil {
		// Lightweight services without a repository have no durable states.
		// Preserve the existing resolved-identity path when no local rest has
		// ever been registered; in-memory rest still uses the same gate.
		hasState := false
		s.codexCookies.poolRestState.Range(func(_, _ any) bool { hasState = true; return false })
		if !hasState {
			return true, nil
		}
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout+account.gatewayPoolListTimeout())
	defer cancel()
	unlock, err := s.lockGatewayPoolRest(ctx, gatewayPoolLedgerTag(identity))
	if err != nil {
		return false, err
	}
	defer unlock()
	state, fresh, err := s.loadGatewayPoolRest(ctx, account, identity)
	if err != nil || !state.Active {
		return err == nil, err
	}
	now := time.Now().UTC()
	if !allowResume || now.Before(state.NextCheck) || !fresh.IsSchedulable() {
		return false, nil
	}
	generation, enough := s.gatewayPoolHasRecoveryCandidates(ctx, fresh, identity)
	if enough {
		state.Active, state.NextCheck = false, time.Time{}
	} else {
		state.NextCheck = now.Add(s.codexCookies.gatewayPoolRestDuration(identity, fresh, now))
	}
	state.advance(now)
	var expected []uint64
	if enough {
		expected = []uint64{generation}
	}
	if err := s.writeGatewayPoolRest(ctx, fresh, identity, state, expected...); err != nil {
		return false, err // recovery is published only after the tombstone is durable
	}
	if !enough {
		return false, nil
	}
	s.codexCookies.poolRounds.recovered(identity)
	return true, nil
}

func (s *OpenAIGatewayService) gatewayPoolHasRecoveryCandidates(ctx context.Context, account *Account, identity string) (uint64, bool) {
	if err := s.codexCookies.hydrateGatewayPoolSharedHistory(ctx, account, identity); err != nil {
		return 0, false
	}
	generation, active, _ := s.codexCookies.gatewayPoolInventoryCandidates(identity, account)
	if active {
		return generation, false
	}
	pool, err := s.codexCookies.poolClient(account)
	if err != nil {
		return generation, false
	}
	listCtx, cancel := context.WithTimeout(ctx, account.gatewayPoolListTimeout())
	defer cancel()
	gateways, err := pool.Gateways(listCtx, gatewayPoolUpstreamAccountID(identity), gatewayPoolAccountTag(account, identity))
	if err != nil {
		return generation, false
	}
	available := make(map[string]struct{})
	for _, gateway := range gateways {
		if gateway.Name != "" && gateway.PairReady {
			if _, cooling := s.codexCookies.gatewayPoolUsedAt(identity, gateway.Name,
				account.gatewayPoolGatewayWindow(), account.gatewayPoolUseRecommendation()); !cooling {
				available[gateway.Name] = struct{}{}
			}
		}
	}
	after, pending, _ := s.codexCookies.gatewayPoolInventoryCandidates(identity, account)
	return generation, !pending && after == generation && len(available) >= account.gatewayPoolResumeGateways()
}

func gatewayPoolRestError() error {
	return &gwpool.PoolError{Code: gwpool.CodeAllCooling, Status: http.StatusServiceUnavailable, RetryAfter: gatewayPoolRestMin}
}
