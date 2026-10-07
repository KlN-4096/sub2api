package service

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	gatewayPoolFeedbackPolicyLimit         = 512
	gatewayPoolFeedbackPolicyPruneInterval = time.Minute
)

// Persisted with the outbox, not with scheduling cooldowns. Immediate invalidates
// a stable policy until the next successful report acknowledgement.
type gatewayPoolFeedbackPolicy struct {
	Binding        string                        `json:"binding"`
	Recommendation gwpool.CooldownRecommendation `json:"recommendation"`
	UpdatedAt      time.Time                     `json:"updated_at"`
	Immediate      bool                          `json:"immediate,omitempty"`
}

func (p gatewayPoolFeedbackPolicy) stable(binding string, now time.Time) bool {
	return p.Binding == binding && !p.Immediate && p.Recommendation.StableReporting(now)
}

func (s *openAICodexCookieStore) noteGatewayPoolFeedbackPolicy(account *Account, identity, gateway string, recommendation *gwpool.CooldownRecommendation) {
	binding := gatewayPoolReportBinding(account, gatewayPoolAccountTag(account, identity))
	update := gatewayPoolRecommendation{at: time.Now().UTC()}
	if recommendation != nil {
		update.CooldownRecommendation = *recommendation
	}
	s.poolFeedbackPolicies.Store(binding+"\x00"+gateway, update)
	previous := s.poolFeedbackPolicyPrune.Load()
	if update.at.Unix() >= previous && s.poolFeedbackPolicyPrune.CompareAndSwap(previous, update.at.Add(gatewayPoolFeedbackPolicyPruneInterval).Unix()) {
		s.poolFeedbackPolicies.Range(func(key, value any) bool {
			policy, valid := value.(gatewayPoolRecommendation)
			if !valid || update.at.Sub(policy.at) > gwpool.CooldownReportPolicyTTL {
				s.poolFeedbackPolicies.CompareAndDelete(key, value)
			}
			return true
		})
	}
}

func clearGatewayPoolReportHold(box *gatewayPoolOutbox, binding, gateway string) {
	for i := range box.Pending {
		pending := &box.Pending[i]
		if pending.Kind == "" && pending.Binding == binding && pending.Report.Gateway == gateway {
			pending.HoldUntil = time.Time{}
		}
	}
}

// A fresh catalogue recommendation can revoke a policy immediately. It cannot
// create or prolong a lease without an actual report acknowledgement.
func (s *openAICodexCookieStore) syncGatewayPoolFeedbackPolicies(box *gatewayPoolOutbox, now time.Time) bool {
	changed := false
	for gateway, policy := range box.Policies {
		raw, ok := s.poolFeedbackPolicies.Load(policy.Binding + "\x00" + gateway)
		update, valid := raw.(gatewayPoolRecommendation)
		// Equal timestamps can be separate operations on coarse Windows clocks;
		// when ordering is ambiguous, conservatively resume immediate reporting.
		if !ok || !valid || update.at.Before(policy.UpdatedAt) ||
			(update.Seconds == policy.Recommendation.Seconds && update.StableReporting(now)) {
			continue
		}
		if policy.Immediate && update.at.Equal(policy.UpdatedAt) {
			continue // This revocation was already applied; do not write it every poll.
		}
		policy.Immediate, policy.UpdatedAt = true, update.at
		box.Policies[gateway] = policy
		clearGatewayPoolReportHold(box, policy.Binding, gateway)
		changed = true
	}
	return changed
}

func prepareGatewayPoolReportHold(box *gatewayPoolOutbox, pending *gatewayPoolPendingReport, now time.Time) {
	if pending.Kind != "" {
		return // Contact initial/final payloads remain immediate and independent.
	}
	gateway := pending.Report.Gateway
	policy, known := box.Policies[gateway]
	if !known || policy.Binding != pending.Binding {
		return
	}
	if pending.Report.Result != "full" || gwpool.CooldownSuccessBucket(pending.Report) != policy.Recommendation.Seconds {
		policy.Immediate = true
		box.Policies[gateway] = policy
		clearGatewayPoolReportHold(box, pending.Binding, gateway)
		return
	}
	if !policy.stable(pending.Binding, now) {
		return
	}
	deadline := now.Add(gwpool.CooldownReportInterval)
	if policy.Recommendation.ReportPolicyExpiresAt.Before(deadline) {
		deadline = policy.Recommendation.ReportPolicyExpiresAt
	}
	for _, old := range box.Pending {
		if old.Kind == "" && old.Binding == pending.Binding && old.Report.Gateway == gateway &&
			!old.HoldUntil.IsZero() && old.HoldUntil.Before(deadline) {
			deadline = old.HoldUntil // New arrivals never extend the first pending deadline.
		}
	}
	pending.HoldUntil = deadline
}

func gatewayPoolReportReady(box gatewayPoolOutbox, pending gatewayPoolPendingReport, binding string, now time.Time) bool {
	if pending.Permanent || now.Before(pending.NextAt) {
		return false // Immediate feedback cannot bypass transport retry backoff.
	}
	if pending.Kind != "" || pending.Binding != binding || len(box.Pending) >= gwpool.CooldownBatchLimit {
		return true
	}
	return !now.Before(pending.HoldUntil) || !box.Policies[pending.Report.Gateway].stable(pending.Binding, now)
}

func acknowledgeGatewayPoolFeedbackPolicy(box *gatewayPoolOutbox, pending gatewayPoolPendingReport, rec *gwpool.CooldownRecommendation, now, startedAt time.Time) {
	if box.Policies == nil {
		box.Policies = map[string]gatewayPoolFeedbackPolicy{}
	}
	gateway := pending.Report.Gateway
	if rec == nil || !rec.StableReporting(now) {
		delete(box.Policies, gateway)
		clearGatewayPoolReportHold(box, pending.Binding, gateway)
		return
	}
	// Order the ACK at request start, not completion. A later catalogue update
	// can otherwise be overwritten by an old response delayed in flight.
	policy := gatewayPoolFeedbackPolicy{Binding: pending.Binding, Recommendation: *rec, UpdatedAt: startedAt}
	for _, other := range box.Pending {
		if other.Kind == "" && other.Binding == pending.Binding && other.Report.Gateway == gateway &&
			(other.Report.Result != "full" || gwpool.CooldownSuccessBucket(other.Report) != rec.Seconds) {
			policy.Immediate = true // A newer failure arrived while this success was in flight.
		}
	}
	if old, exists := box.Policies[gateway]; exists && old.Recommendation.Seconds != rec.Seconds {
		clearGatewayPoolReportHold(box, pending.Binding, gateway)
	}
	box.Policies[gateway] = policy
}
