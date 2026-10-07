package service

import (
	"sort"
	"time"
)

// Display-only local cooldown estimate. It neither predicts pool inventory nor
// changes the bounded rest recheck deadline.
type GatewayPoolCooldownEstimate struct {
	ResumeGateways int       `json:"resume_gateways"`
	EligibleAt     time.Time `json:"eligible_at,omitzero"`
}

func (s *openAICodexCookieStore) gatewayPoolCooldownEstimate(identity string, account *Account, history openAIGatewayHistory, now time.Time) GatewayPoolCooldownEstimate {
	result := GatewayPoolCooldownEstimate{ResumeGateways: account.gatewayPoolResumeGateways()}
	deadlines := make([]time.Time, 0, len(history.Seen))
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
			s.refreshGatewayPoolCooldown(&c, identity, gateway, window, touched, now, account.gatewayPoolUseRecommendation())
			until = c.Until
			if touchedUntil := touched.Add(time.Duration(c.WindowSeconds) * time.Second); !c.Cleared && !touched.IsZero() && touchedUntil.After(until) {
				until = touchedUntil
			}
		} else if !touched.IsZero() {
			initial, _ := s.gatewayPoolInitialCooldown(identity, gateway, window, account.gatewayPoolUseRecommendation())
			until = touched.Add(time.Duration(initial) * time.Second)
		} else if !seen.At.IsZero() && !clearAt.IsZero() {
			until = clearAt
		}
		if !until.IsZero() {
			deadlines = append(deadlines, until)
		}
	}
	if len(deadlines) == 0 {
		return result // no local cooldown to wait for
	}
	sort.Slice(deadlines, func(i, j int) bool { return deadlines[i].Before(deadlines[j]) })
	result.EligibleAt = deadlines[min(result.ResumeGateways, len(deadlines))-1]
	return result
}
