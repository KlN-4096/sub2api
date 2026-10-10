package service

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"
)

const (
	gatewayPoolRestMin = 30 * time.Second // client retry hint, not a recovery deadline
)

// No pool I/O. With fewer than N known gateways, wait for all known cooldowns.
// No history means no local cooldown rest, not an invented retry interval.
func (s *openAICodexCookieStore) gatewayPoolLocalResumeAt(identity string, account *Account, qualityGateways ...string) time.Time {
	prefix := identity + "\x00"
	deadlines := map[string]time.Time{}
	known := map[string]time.Time{}
	s.poolKnown.Range(func(key, _ any) bool {
		if name, ok := key.(string); ok && strings.HasPrefix(name, prefix) {
			known[name] = time.Time{}
		}
		return true
	})
	s.poolUsed.Range(func(key, value any) bool {
		name, ok := key.(string)
		at, timeOK := value.(time.Time)
		if ok && timeOK && strings.HasPrefix(name, prefix) && !at.IsZero() {
			known[name] = at
		}
		return true
	})
	for name, at := range known {
		gateway, matches := strings.CutPrefix(name, prefix)
		if !matches || gateway == "" {
			continue
		}
		clearAt := s.gatewayPoolCooldownClearAt(identity)
		if !at.After(clearAt) && !clearAt.IsZero() {
			deadlines[gateway] = clearAt
			continue
		}
		if at.IsZero() {
			continue
		}
		_, _ = s.gatewayPoolUsedAt(identity, gateway, account.gatewayPoolGatewayWindow())
		base := gatewayPoolCooldownBase(account.gatewayPoolGatewayWindow())
		until := at.Add(time.Duration(base) * time.Second)
		if cooldown, found := s.cooldownEntry(identity, gateway); found {
			until = cooldown.Until
			if touched := at.Add(time.Duration(cooldown.WindowSeconds) * time.Second); !cooldown.Cleared && touched.After(until) {
				until = touched
			}
		}
		deadlines[gateway] = until
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

func (s *openAICodexCookieStore) gatewayPoolRestDuration(identity string, account *Account, now time.Time) time.Duration {
	if until := s.gatewayPoolLocalResumeAt(identity, account); until.After(now) {
		return until.Sub(now)
	}
	return 0
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
