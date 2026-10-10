package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
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
	Tag       string    `json:"tag"`
	Active    bool      `json:"active"`
	ChangedAt time.Time `json:"changed_at"`
	StartedAt time.Time `json:"started_at,omitzero"`
	ResumeAt  time.Time `json:"resume_at,omitzero"`
	// Frozen on entry, shared by clones and durable across restarts. Legacy
	// records without a cohort retain the configured count-only recovery rule.
	QualityGateways []string `json:"quality_gateways,omitempty"`
	// Decode old records, but never use a legacy recheck as a local deadline.
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
	if fresh.GatewayPoolContinuousWaitEnabled() {
		return state, fresh, nil // this row does not consume another clone's rest latch
	}
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

func (s *openAICodexCookieStore) lockGatewayPoolRestInventory(ctx context.Context, identity string, generation *uint64) (func(), error) {
	if generation == nil {
		return func() {}, nil
	}
	inventory := s.gatewayPoolInventory(identity)
	if !gatewayPoolLockWithin(ctx, &inventory.mu) {
		return nil, errors.New("gateway rest inventory lock unavailable")
	}
	if inventory.active != 0 || inventory.generation != *generation {
		inventory.mu.Unlock()
		return nil, errors.New("gateway rest inventory changed")
	}
	return inventory.mu.Unlock, nil
}

func (s *OpenAIGatewayService) writeGatewayPoolRest(ctx context.Context, account *Account, identity string, state gatewayPoolRestState, plans ...gatewayPoolRestPlan) error {
	var plan gatewayPoolRestPlan
	if len(plans) > 0 {
		plan = plans[0]
	}
	publishActive := func() {
		if state.Active {
			// Keep a proven rest fail-closed on a database write failure, but
			// never publish it before the final inventory guard.
			s.codexCookies.poolRestState.Store(state.Tag, state)
			if plan.group > 0 {
				s.codexCookies.poolRounds.rest(plan.group, identity, state.ResumeAt)
			}
		}
	}
	if s.accountRepo == nil {
		unlock, err := s.codexCookies.lockGatewayPoolRestInventory(ctx, identity, plan.generation)
		if err != nil {
			return err
		}
		defer unlock()
		publishActive()
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
	// final generation check through durable publication, not just the deadline calculation.
	unlockInventory, err := s.codexCookies.lockGatewayPoolRestInventory(ctx, identity, plan.generation)
	if err != nil {
		return err
	}
	defer unlockInventory()
	publishActive()
	previousTag := ""
	if state.Previous != nil {
		previousTag = state.Previous.Tag
	}
	patch := map[string]any{
		gatewayPoolRestStateKey: state, gatewayPoolRestTagKey: state.Tag, gatewayPoolRestPreviousTagKey: previousTag,
	}
	if atomicRepo, ok := s.accountRepo.(gatewayPoolRestRepository); ok && state.Active {
		err = atomicRepo.SetGatewayPoolRest(ctx, account.ID, state.ResumeAt, gatewayPoolRestReason(fresh, state.QualityGateways...), patch)
	} else if clearRepo, ok := s.accountRepo.(gatewayPoolClearRestRepository); ok && !state.Active {
		err = clearRepo.ClearGatewayPoolRest(ctx, account.ID, patch)
	} else {
		err = s.accountRepo.UpdateExtra(ctx, account.ID, patch)
		if err == nil && state.Active {
			err = s.accountRepo.SetTempUnschedulable(ctx, account.ID, state.ResumeAt, gatewayPoolRestReason(fresh, state.QualityGateways...))
		} else if err == nil && gatewayPoolOwnsTempBlock(fresh) {
			err = s.accountRepo.ClearTempUnschedulable(ctx, account.ID)
		}
	}
	if err != nil {
		return err
	}
	s.codexCookies.poolRestState.Store(state.Tag, state)
	return nil
}

func gatewayPoolRestReason(account *Account, qualityGateways ...string) string {
	if len(qualityGateways) > 0 {
		return fmt.Sprintf("网关候选低于1，等%d个优质网关全部冷却或%d个本地冷却截止恢复 / Gateway candidates below 1; resume after all %d quality gateways or %d local cooldowns",
			len(qualityGateways), account.gatewayPoolResumeGateways(), len(qualityGateways), account.gatewayPoolResumeGateways())
	}
	return fmt.Sprintf("网关候选低于%d，按%d个本地冷却截止恢复 / Gateway candidates below %d; resume after %d local cooldowns",
		1, account.gatewayPoolResumeGateways(),
		1, account.gatewayPoolResumeGateways())
}

func gatewayPoolOwnsTempBlock(account *Account) bool {
	return account != nil && strings.HasPrefix(account.TempUnschedulableReason, "网关候选低于") &&
		strings.Contains(account.TempUnschedulableReason, " / Gateway candidates below ")
}

func (s *gatewayPoolRestState) advance(now time.Time) {
	if !now.After(s.ChangedAt) {
		now = s.ChangedAt.Add(time.Nanosecond)
	}
	s.ChangedAt = now
}

func (s *OpenAIGatewayService) enterGatewayPoolRest(ctx context.Context, account *Account, identity string, at, until time.Time, plans ...gatewayPoolRestPlan) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer cancel()
	var plan gatewayPoolRestPlan
	if len(plans) > 0 {
		plan = plans[0]
	}
	tag := gatewayPoolLedgerTag(identity)
	unlock, err := s.lockGatewayPoolRest(ctx, tag)
	if err != nil {
		return err
	}
	defer unlock()
	state, fresh, err := s.loadGatewayPoolRest(ctx, account, identity)
	if err != nil {
		// A partial row/peer read cannot establish the newest frozen cohort
		// or tombstone. Preserve all known state and reject this admission.
		return err
	}
	if at.Before(state.ChangedAt) {
		if plan.generation != nil {
			return errors.New("gateway rest plan predates recovery")
		}
		return nil // late shortage result cannot undo a newer recovery
	}
	if fresh.GatewayPoolContinuousWaitEnabled() {
		if plan.generation != nil {
			return errors.New("gateway rest policy changed")
		}
		return nil
	}
	if !state.Active {
		state.StartedAt = at
		state.QualityGateways = append([]string(nil), plan.qualityGateways...)
	} else {
		until = s.codexCookies.gatewayPoolLocalResumeAt(identity, fresh, state.QualityGateways...)
	}
	state.Active, state.ResumeAt, state.NextCheck = true, until, time.Time{}
	state.advance(at)
	err = s.writeGatewayPoolRest(ctx, fresh, identity, state, plan)
	if cached, ok := s.codexCookies.poolRestState.Load(tag); ok {
		if published, valid := cached.(gatewayPoolRestState); valid && published.Active && published.ChangedAt.Equal(state.ChangedAt) {
			s.wakeGatewayPoolRestTimer()
		}
	}
	return err
}

// allowResume is only used at a pre-send scheduling boundary, before this
// request registers work. Last-mile sends and early reservations only check;
// they cannot turn a resting domain into an active one while work is in flight.
func (s *OpenAIGatewayService) gatewayPoolResumeAllowed(ctx context.Context, account *Account, allowResume bool) (bool, error) {
	if s == nil || !s.codexCookies.gatewayPoolTakeover(account) {
		return true, nil
	}
	if s.accountRepo == nil {
		if account.GatewayPoolContinuousWaitEnabled() {
			return gatewayPoolWaitHealth(account), nil
		}
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
	ctx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
	defer cancel()
	unlock, err := s.lockGatewayPoolRest(ctx, gatewayPoolLedgerTag(identity))
	if err != nil {
		return false, err
	}
	defer unlock()
	state, fresh, err := s.loadGatewayPoolRest(ctx, account, identity)
	if err != nil {
		return false, err
	}
	if fresh.GatewayPoolContinuousWaitEnabled() {
		return s.allowGatewayPoolContinuousWait(ctx, fresh)
	}
	if !state.Active {
		// A clone can still carry a pool-owned block after another row published
		// the shared inactive tombstone. Retry its cleanup independently.
		if !state.ChangedAt.IsZero() && gatewayPoolOwnsTempBlock(fresh) {
			if err := s.writeGatewayPoolRest(ctx, fresh, identity, state); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if !allowResume {
		return false, nil
	}
	if err := s.codexCookies.hydrateGatewayPoolSharedHistory(ctx, fresh, identity); err != nil {
		return false, err
	}
	generation, active, _ := s.codexCookies.gatewayPoolInventoryCandidates(identity)
	if active {
		return false, errors.New("gateway local recovery work still in flight")
	}
	now := time.Now().UTC()
	until := s.codexCookies.gatewayPoolLocalResumeAt(identity, fresh, state.QualityGateways...)
	if until.After(now) {
		if state.ResumeAt.Equal(until) && state.NextCheck.IsZero() {
			return false, nil
		}
		state.ResumeAt, state.NextCheck = until, time.Time{}
		state.advance(now)
		err = s.writeGatewayPoolRest(ctx, fresh, identity, state, gatewayPoolRestPlan{generation: &generation})
		if err == nil {
			s.wakeGatewayPoolRestTimer()
		}
		return false, err
	}
	state.Active, state.ResumeAt, state.NextCheck = false, time.Time{}, time.Time{}
	state.QualityGateways = nil
	state.advance(now)
	if err := s.writeGatewayPoolRest(ctx, fresh, identity, state, gatewayPoolRestPlan{generation: &generation}); err != nil {
		return false, err // recovery is published only after the tombstone is durable
	}
	s.codexCookies.poolRounds.recovered(identity)
	health := *fresh
	if gatewayPoolOwnsTempBlock(fresh) {
		health.TempUnschedulableUntil = nil
	}
	return health.IsSchedulable(), nil
}

func gatewayPoolRestError() error {
	return &gwpool.PoolError{Code: gwpool.CodeAllCooling, Status: http.StatusServiceUnavailable, RetryAfter: gatewayPoolRestMin}
}
