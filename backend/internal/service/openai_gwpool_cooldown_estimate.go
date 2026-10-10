package service

import "time"

// Display-only local cooldown estimate. It neither predicts pool inventory nor
// changes the durable rest latch.
type GatewayPoolCooldownEstimate struct {
	ResumeGateways int       `json:"resume_gateways"`
	EligibleAt     time.Time `json:"eligible_at,omitzero"`
}

func (s *openAICodexCookieStore) gatewayPoolCooldownEstimate(identity string, account *Account, history openAIGatewayHistory, now time.Time, peers ...Account) GatewayPoolCooldownEstimate {
	result := GatewayPoolCooldownEstimate{ResumeGateways: account.gatewayPoolResumeGateways()}
	projected := s.gatewayPoolDisplayCooldownDeadlines(identity, account, history, now)
	var qualityGateways []string
	if state := s.gatewayPoolRestSnapshot(account, identity, peers); state.Active && !account.GatewayPoolContinuousWaitEnabled() {
		qualityGateways = state.QualityGateways
	}
	result.EligibleAt = gatewayPoolRecoveryDeadline(projected, result.ResumeGateways, qualityGateways)
	return result
}

func (s *openAICodexCookieStore) gatewayPoolDisplayCooldownDeadlines(identity string, account *Account, history openAIGatewayHistory, now time.Time) map[string]time.Time {
	deadlines := make(map[string]time.Time, len(history.Seen))
	window := account.gatewayPoolGatewayWindow()
	base := gatewayPoolCooldownBase(window)
	clearAt := s.gatewayPoolCooldownClearAt(identity)
	reset := history.CooldownReset
	if !reset.valid(now) {
		reset = gatewayPoolCooldownResetState{}
	}
	if reset.ClearedAt.After(clearAt) {
		clearAt = reset.ClearedAt
	}
	for gateway, seen := range history.Seen {
		if gateway == "" {
			continue
		}
		touched := seen.At
		if !touched.After(clearAt) {
			touched = time.Time{}
		}
		until := time.Time{}
		if validGatewayPoolCooldown(seen.Cooldown, now, base) {
			// The same effective-window calculation as scheduling, applied only
			// to a private clone. Polling never hydrates/persists shared state.
			c := seen.Cooldown.clone()
			c.clearCooldown(reset.ClearedAt, base)
			c.resetBackoff(reset.LastAt, touched, base)
			s.refreshGatewayPoolCooldown(&c, identity, window, touched, now)
			until = c.Until
			if touchedUntil := touched.Add(time.Duration(c.WindowSeconds) * time.Second); !c.Cleared && !touched.IsZero() && touchedUntil.After(until) {
				until = touchedUntil
			}
		} else if !touched.IsZero() {
			until = touched.Add(time.Duration(base) * time.Second)
		} else if !seen.At.IsZero() && !clearAt.IsZero() {
			until = clearAt
		}
		if !until.IsZero() {
			deadlines[gateway] = until
		}
	}
	return deadlines
}
