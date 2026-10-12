package service

import (
	"context"
	"log/slog"
	"sort"
	"time"
)

const (
	gatewayPoolRestMin = 30 * time.Second // client retry hint, not a recovery deadline
)

// No pool I/O. With fewer than N known gateways, wait for all known cooldowns.
// No history means no local cooldown rest, not an invented retry interval.
func (s *openAICodexCookieStore) gatewayPoolLocalResumeAt(identity string, account *Account, qualityGateways ...string) time.Time {
	deadlines := map[string]time.Time{}
	known := map[string]time.Time{}
	s.rangeGatewayPoolKnown(identity, func(gateway string) { known[gateway] = time.Time{} })
	s.rangeGatewayPoolUsed(identity, func(gateway string, at time.Time) { known[gateway] = at })
	for gateway, at := range known {
		clearAt := s.gatewayPoolCooldownClearAt(identity)
		if !at.After(clearAt) && !clearAt.IsZero() {
			deadlines[gateway] = clearAt
			continue
		}
		if at.IsZero() {
			continue
		}
		_, deadlines[gateway] = s.gatewayPoolCooldownUntil(identity, gateway, account.gatewayPoolGatewayWindow())
	}
	return gatewayPoolRecoveryDeadline(deadlines, account.gatewayPoolResumeGateways(), qualityGateways)
}

// Both admission and its read-only estimate use the same OR rule. A missing
// quality deadline is unknown, not proof that the frozen cohort has cooled.
func gatewayPoolRecoveryDeadline(deadlines map[string]time.Time, threshold int, qualityGateways []string) time.Time {
	var eligibleAt []time.Time
	for _, until := range deadlines {
		if !until.IsZero() {
			eligibleAt = append(eligibleAt, until)
		}
	}
	sort.Slice(eligibleAt, func(i, j int) bool { return eligibleAt[i].Before(eligibleAt[j]) })
	if len(eligibleAt) == 0 {
		return time.Time{}
	}
	countAt := eligibleAt[min(threshold, len(eligibleAt))-1]
	if len(qualityGateways) == 0 {
		return countAt
	}
	var qualityAt time.Time
	for _, name := range qualityGateways {
		until, known := deadlines[name]
		if !known || until.IsZero() {
			return countAt
		}
		if until.After(qualityAt) {
			qualityAt = until
		}
	}
	if qualityAt.Before(countAt) {
		return qualityAt
	}
	return countAt
}

func (s *OpenAIGatewayService) restGatewayPoolAccount(ctx context.Context, account *Account, identity string, group int64, plans ...gatewayPoolRestPlan) error {
	if account.GatewayPoolContinuousWaitEnabled() {
		return nil
	}
	var plan gatewayPoolRestPlan
	if len(plans) > 0 {
		plan = plans[0]
	}
	plan.group = group
	now := time.Now()
	until := s.codexCookies.gatewayPoolLocalResumeAt(identity, account, plan.qualityGateways...)
	if !until.After(now) {
		// Supply-only exhaustion can finish this rotation round, but must not
		// invent a cooldown rest or use a proof invalidated by new work.
		if plan.generation != nil {
			unlock, err := s.codexCookies.lockGatewayPoolRestInventory(ctx, identity, plan.generation)
			if err != nil {
				return err
			}
			defer unlock()
			s.codexCookies.poolRounds.exhaust(group, identity, plan.roundGeneration)
		}
		return nil
	}
	if err := s.enterGatewayPoolRest(ctx, account, identity, now, until, plan); err != nil {
		slog.Warn("gwpool_rest_state_persist_failed", "account_id", account.ID)
		return err
	}
	if s.accountRepo == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer cancel()
	s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.end(now.UTC(), "temporarily_unschedulable")
	})
	return nil
}
