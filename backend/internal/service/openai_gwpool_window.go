package service

import "time"

// gatewayPoolVerifiedMark is the point at which a ticket was verified full.
// It remains part of foreground verification and state-echo accounting.
type gatewayPoolVerifiedMark struct {
	version string
	at      time.Time
	// Immutable after publication; pointer keeps the sync.Map CAS value comparable.
	models *map[string]time.Time
}

// The caller persists this duration in gateway/contact history after marking
// the matching ticket stale. It is an observation, not a guaranteed lifetime.
func (s *openAICodexCookieStore) gatewayPoolNoteFullWindow(identity, version string) time.Duration {
	if s == nil || identity == "" || version == "" {
		return 0
	}
	mark, ok := s.gatewayPoolVerifiedMarkOf(identity)
	if !ok || mark.version != version || mark.at.IsZero() {
		return 0
	}
	held := time.Since(mark.at)
	if held < 0 {
		return 0
	}
	return held
}
