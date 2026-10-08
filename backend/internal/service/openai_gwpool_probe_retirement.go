package service

// Keep manual-clear generations and replacement tickets immune to late probe
// failures. The attempt already reserved its ordinary cooldown at acquisition;
// retiring a ticket must not call the quality-learning path.
func (s *openAICodexCookieStore) retireGatewayPoolFailedProbe(identity string, applied OpenAIGatewayPoolApplied) bool {
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	if !applied.cooldownResetAt.Equal(s.gatewayPoolCooldownResetAt(identity)) {
		return false
	}
	pair, state := s.cachedPoolPair(identity)
	if state == openAIGatewayPoolPairNone {
		return true // a definitely-unsent ticket may already have been returned
	}
	if pair.version != applied.Version {
		return false
	}
	_, retired := s.gatewayPoolMarkStaleMatched(identity, applied.Version)
	return retired
}
