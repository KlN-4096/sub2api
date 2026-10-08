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
func (s *openAICodexCookieStore) gatewayPoolLocalResumeAt(identity string, account *Account) time.Time {
	prefix := identity + "\x00"
	var eligibleAt []time.Time
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
			eligibleAt = append(eligibleAt, clearAt)
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
		eligibleAt = append(eligibleAt, until)
	}
	sort.Slice(eligibleAt, func(i, j int) bool { return eligibleAt[i].Before(eligibleAt[j]) })
	if len(eligibleAt) == 0 {
		return time.Time{}
	}
	return eligibleAt[min(account.gatewayPoolResumeGateways(), len(eligibleAt))-1]
}

func (s *openAICodexCookieStore) gatewayPoolRestDuration(identity string, account *Account, now time.Time) time.Duration {
	if until := s.gatewayPoolLocalResumeAt(identity, account); until.After(now) {
		return until.Sub(now)
	}
	return 0
}

func (s *OpenAIGatewayService) restGatewayPoolAccount(ctx context.Context, account *Account, identity string, group int64) {
	if account.GatewayPoolContinuousWaitEnabled() {
		return
	}
	now := time.Now()
	until := s.codexCookies.gatewayPoolLocalResumeAt(identity, account)
	if !until.After(now) {
		return // supply-only shortages remain ordinary ticket-acquisition failures
	}
	s.codexCookies.poolRounds.rest(group, identity, until)
	if err := s.enterGatewayPoolRest(ctx, account, identity, now, until); err != nil {
		slog.Warn("gwpool_rest_state_persist_failed", "account_id", account.ID)
		return
	}
	if s.accountRepo == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer cancel()
	s.changeGatewayPoolUsage(ctx, account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.end(now.UTC(), "temporarily_unschedulable")
	})
}
