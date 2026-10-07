package service

import (
	"sync"
	"time"
)

// The pool marks a batch delivered before these tickets are actually tried.
// Rotation must consider local inventory and concurrent fetch/probe transitions,
// not treat the pool's UsedByYou flag as proof of consumption.
type gatewayPoolInventoryState struct {
	mu         sync.Mutex
	generation uint64
	active     int
}

func (s *openAICodexCookieStore) gatewayPoolInventory(identity string) *gatewayPoolInventoryState {
	value, _ := s.poolInventory.LoadOrStore(identity, &gatewayPoolInventoryState{})
	state, _ := value.(*gatewayPoolInventoryState)
	return state
}

func (s *openAICodexCookieStore) gatewayPoolInventoryOperation(identity string) func() {
	state := s.gatewayPoolInventory(identity)
	state.mu.Lock()
	state.active++
	state.generation++
	state.mu.Unlock()
	return func() {
		state.mu.Lock()
		state.active--
		state.generation++
		state.mu.Unlock()
	}
}

// No network and no ticket consumption. active serializes this inspection with
// batch cursor mutation; the generation lets callers reject a listing observed
// across an intervening fetch/probe, including one that has already completed.
func (s *openAICodexCookieStore) gatewayPoolInventorySnapshot(identity string, account *Account) (generation uint64, pending bool) {
	generation, active, candidates := s.gatewayPoolInventoryCandidates(identity, account)
	return generation, active || len(candidates) > 0
}

func (s *openAICodexCookieStore) gatewayPoolInventoryCandidates(identity string, account *Account) (generation uint64, active bool, candidates map[string]struct{}) {
	state := s.gatewayPoolInventory(identity)
	state.mu.Lock()
	defer state.mu.Unlock()
	candidates = map[string]struct{}{}
	if state.active > 0 {
		return state.generation, true, candidates
	}
	if pair, live := s.cachedPoolPair(identity); live == openAIGatewayPoolPairLive {
		candidates[pair.gateway] = struct{}{} // verified OR not yet conclusively tested
	}
	if value, ok := s.poolSpare.Load(identity); ok {
		if batch, valid := value.(*gatewayPoolTicketBatch); valid {
			for _, pair := range batch.pairs[batch.idx:] {
				if pair.cookie == "" || time.Until(pair.until) < openAIGatewayPoolMinRemaining {
					continue
				}
				if _, cooling := s.gatewayPoolUsedAt(identity, pair.gateway, account.gatewayPoolGatewayWindow()); !cooling {
					candidates[pair.gateway] = struct{}{}
				}
			}
		}
	}
	return state.generation, false, candidates
}
