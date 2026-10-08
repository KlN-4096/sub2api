package service

import (
	"sync"
)

// Rotation considers the current ticket and concurrent fetch/probe transitions,
// never treating the pool's UsedByYou flag as proof of local consumption.
type gatewayPoolInventoryState struct {
	mu         sync.Mutex
	generation uint64
	active     int
	requests   int // Includes preparation waiters; only used for usage idle detection.
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

// No network and no ticket consumption. The generation rejects a listing observed
// across an intervening fetch/probe, including one that has already completed.
func (s *openAICodexCookieStore) gatewayPoolInventorySnapshot(identity string) (generation uint64, pending bool) {
	generation, active, candidates := s.gatewayPoolInventoryCandidates(identity)
	return generation, active || len(candidates) > 0
}

func (s *openAICodexCookieStore) gatewayPoolInventoryCandidates(identity string) (generation uint64, active bool, candidates map[string]struct{}) {
	state := s.gatewayPoolInventory(identity)
	state.mu.Lock()
	defer state.mu.Unlock()
	candidates = map[string]struct{}{}
	if state.active > 0 {
		return state.generation, true, candidates
	}
	domain := identity
	s.poolPairs.Range(func(key, _ any) bool {
		other, ok := key.(string)
		if ok && other == domain {
			if pair, live := s.cachedPoolPair(other); live == openAIGatewayPoolPairLive {
				candidates[pair.gateway] = struct{}{}
			}
		}
		return true
	})
	return state.generation, false, candidates
}
