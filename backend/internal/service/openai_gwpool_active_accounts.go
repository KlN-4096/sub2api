package service

import (
	"context"
	"slices"
	"strconv"
	"strings"
)

const (
	gatewayPoolActiveAccountsDefault = 1
	gatewayPoolActiveAccountsMax     = 64
)

func normalizeGatewayPoolActiveAccounts(limit int) int {
	if limit < 1 || limit > gatewayPoolActiveAccountsMax {
		return gatewayPoolActiveAccountsDefault
	}
	return limit
}

func parseGatewayPoolActiveAccounts(raw string) int {
	limit, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return gatewayPoolActiveAccountsDefault
	}
	return normalizeGatewayPoolActiveAccounts(limit)
}

func (s *OpenAIGatewayService) gatewayPoolActiveAccountLimit(ctx context.Context) int {
	return normalizeGatewayPoolActiveAccounts(s.openAIAdvancedSchedulerRuntimeSettings(ctx).gatewayPoolActiveAccounts)
}

func (state *gatewayPoolRound) retire(domain string) {
	state.active = slices.DeleteFunc(state.active, func(value string) bool { return value == domain })
	state.current = ""
	if len(state.active) > 0 {
		state.current = state.active[0]
	}
}

// Reserve ordered account names, not tickets. A standby is not probed until it
// actually receives traffic. Per-request exclusions must not open extra slots.
func (r *gatewayPoolRounds) admit(group int64, candidates []string, present map[int64]string, complete bool, limit int) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.groupLocked(group)
	limit = normalizeGatewayPoolActiveAccounts(limit)
	if len(state.active) == 0 && state.current != "" {
		state.active = []string{state.current}
	}
	known := map[string]bool{}
	for _, domain := range present {
		known[domain] = true
	}
	state.active = slices.DeleteFunc(state.active, func(domain string) bool {
		_, exhausted := state.exhausted[domain]
		return exhausted || (complete && !known[domain])
	})
	if len(state.active) > limit {
		state.active = state.active[:limit] // does not cancel already dispatched work
	}
	for _, domain := range candidates {
		if len(state.active) >= limit {
			break
		}
		if _, exhausted := state.exhausted[domain]; domain != "" && !exhausted && !slices.Contains(state.active, domain) {
			state.active = append(state.active, domain)
		}
	}
	ranks := make(map[string]int, len(state.active))
	state.current = ""
	for index, domain := range state.active {
		if index == 0 {
			state.current = domain
		}
		ranks[domain] = index + 1
	}
	return ranks
}
