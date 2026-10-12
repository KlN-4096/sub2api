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
	return gatewayPoolLoadOrNew[gatewayPoolInventoryState](&s.poolInventory, identity)
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
func (s *openAICodexCookieStore) gatewayPoolInventoryCandidates(identity string) (generation uint64, active bool, candidates map[string]struct{}) {
	state := s.gatewayPoolInventory(identity)
	state.mu.Lock()
	defer state.mu.Unlock()
	candidates = map[string]struct{}{}
	if state.active > 0 {
		return state.generation, true, candidates
	}
	if pair, live := s.cachedPoolPair(identity); live == openAIGatewayPoolPairLive {
		candidates[pair.gateway] = struct{}{}
	}
	return state.generation, false, candidates
}
