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
// since is when the rest began (now when entering one).
func (s *openAICodexCookieStore) gatewayPoolLocalResumeAt(identity string, account *Account, since time.Time, qualityGateways ...string) time.Time {
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
	return gatewayPoolRecoveryDeadline(deadlines, account.gatewayPoolResumeGateways(), qualityGateways, since)
}

// Both admission and its read-only estimate use the same OR rule. A missing
// quality deadline is unknown, not proof that the frozen cohort has cooled.
// Neither list waits for its long-wait outliers, so one slow gateway cannot
// hold the whole rest.
func gatewayPoolRecoveryDeadline(deadlines map[string]time.Time, threshold int, qualityGateways []string, since time.Time) time.Time {
	var eligibleAt []time.Time
	for _, until := range deadlines {
		if !until.IsZero() {
			eligibleAt = append(eligibleAt, until)
		}
	}
	eligibleAt = gatewayPoolWithoutLongWaits(eligibleAt, since)
	sort.Slice(eligibleAt, func(i, j int) bool { return eligibleAt[i].Before(eligibleAt[j]) })
	if len(eligibleAt) == 0 {
		return time.Time{}
	}
	countAt := eligibleAt[min(threshold, len(eligibleAt))-1]
	if len(qualityGateways) == 0 {
		return countAt
	}
	cohort := make([]time.Time, 0, len(qualityGateways))
	for _, name := range qualityGateways {
		until := deadlines[name]
		if until.IsZero() {
			return countAt
		}
		cohort = append(cohort, until)
	}
	var qualityAt time.Time
	for _, until := range gatewayPoolWithoutLongWaits(cohort, since) {
		if until.After(qualityAt) {
			qualityAt = until
		}
	}
	if qualityAt.Before(countAt) {
		return qualityAt
	}
	return countAt
}

// gatewayPoolWithoutLongWaits drops the deadlines whose remaining wait at since
// is over 1.5x the median remaining wait of the gateways still cooling then.
// Those are always the latest deadlines, so skipping them can only bring
// recovery earlier. An even count takes the upper middle wait, so the skipped
// are always a strict minority and two gateways are never split. since stays fixed for a whole rest: measured from now, the
// slow gateways would become the median once the others had cooled. The
// cooldown grace keeps a verdict's lag behind its touch from deciding the side
// for ladder steps exactly 1.5x apart (4h/6h, 8h/12h, 16h/24h touched together).
func gatewayPoolWithoutLongWaits(deadlines []time.Time, since time.Time) []time.Time {
	var waits []time.Duration
	for _, until := range deadlines {
		if until.After(since) {
			waits = append(waits, until.Sub(since))
		}
	}
	if len(waits) == 0 {
		return deadlines
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	median := waits[len(waits)/2]
	cut := since.Add(median + median/2 + gatewayPoolCooldownGrace)
	kept := make([]time.Time, 0, len(deadlines))
	for _, until := range deadlines {
		if !until.After(cut) {
			kept = append(kept, until)
		}
	}
	return kept
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
	until := s.codexCookies.gatewayPoolLocalResumeAt(identity, account, now, plan.qualityGateways...)
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
