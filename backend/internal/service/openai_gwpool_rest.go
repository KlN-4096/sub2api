package service

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"
)

const (
	gatewayPoolRestMin     = 30 * time.Second
	gatewayPoolRestMax     = 10 * time.Minute
	gatewayPoolRestDefault = time.Minute
)

// Estimate when X distinct locally known gateways could be retried. Inventory
// can improve independently, so even a distant cooldown is re-evaluated within
// ten minutes. This never declares those future candidates verified.
func (s *openAICodexCookieStore) gatewayPoolRestDuration(identity string, account *Account, now time.Time) time.Duration {
	prefix := gatewayPoolLedgerIdentity(identity) + "\x00"
	var eligibleAt []time.Time
	s.poolUsed.Range(func(key, value any) bool {
		name, ok := key.(string)
		at, timeOK := value.(time.Time)
		gateway, matches := strings.CutPrefix(name, prefix)
		if !ok || !timeOK || !matches || gateway == "" {
			return true
		}
		_, _ = s.gatewayPoolUsedAt(identity, gateway, account.gatewayPoolGatewayWindow(), account.gatewayPoolUseRecommendation())
		base, _ := s.gatewayPoolInitialCooldown(identity, gateway, account.gatewayPoolGatewayWindow(), account.gatewayPoolUseRecommendation())
		until := at.Add(time.Duration(base) * time.Second)
		if cooldown, found := s.cooldownEntry(identity, gateway); found {
			until = cooldown.Until
			if touched := at.Add(time.Duration(cooldown.WindowSeconds) * time.Second); touched.After(until) {
				until = touched
			}
		}
		eligibleAt = append(eligibleAt, until)
		return true
	})
	sort.Slice(eligibleAt, func(i, j int) bool { return eligibleAt[i].Before(eligibleAt[j]) })
	delay := gatewayPoolRestDefault
	threshold := account.gatewayPoolResumeGateways()
	if len(eligibleAt) >= threshold {
		estimated := eligibleAt[threshold-1].Sub(now)
		if estimated > 0 {
			delay = estimated
		}
	}
	if delay < gatewayPoolRestMin {
		delay = gatewayPoolRestMin
	}
	if delay > gatewayPoolRestMax {
		delay = gatewayPoolRestMax
	}
	return delay
}

func (s *OpenAIGatewayService) restGatewayPoolAccount(ctx context.Context, account *Account, identity string, group int64) {
	now := time.Now()
	until := now.Add(s.codexCookies.gatewayPoolRestDuration(identity, account, now))
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
