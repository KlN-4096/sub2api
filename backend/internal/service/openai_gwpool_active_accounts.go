package service

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const (
	gatewayPoolActiveAccountsDefault = 1
	gatewayPoolActiveAccountsMax     = 64
	gatewayPoolGroupReadTimeout      = 2 * time.Second
)

func normalizeGatewayPoolActiveAccounts(limit int) int {
	if limit < 1 || limit > gatewayPoolActiveAccountsMax {
		return gatewayPoolActiveAccountsDefault
	}
	return limit
}

func validateGatewayPoolActiveAccounts(limit int) error {
	if limit < 1 || limit > gatewayPoolActiveAccountsMax {
		return infraerrors.BadRequest("INVALID_GATEWAY_POOL_ACTIVE_ACCOUNTS", "openai_gwpool_active_accounts must be an integer from 1 to 64")
	}
	return nil
}

func (g *Group) GatewayPoolActiveAccountLimit() int {
	if g == nil {
		return gatewayPoolActiveAccountsDefault
	}
	return normalizeGatewayPoolActiveAccounts(g.OpenAIGatewayPoolActiveAccounts)
}

type gatewayPoolGroupLimitsKey struct{}
type gatewayPoolGroupLimits struct {
	mu     sync.Mutex
	values map[int64]int
}

// A selection and its late claim must use the same actual scheduling group,
// including composite/fallback routes. The ordinary authenticated group needs
// no extra query; off-group configuration is loaded once per selection.
func (s *OpenAIGatewayService) gatewayPoolActiveAccountLimit(ctx context.Context, groupID *int64) int {
	if groupID == nil || *groupID <= 0 {
		return gatewayPoolActiveAccountsDefault
	}
	if group, ok := ctx.Value(ctxkey.Group).(*Group); ok && IsGroupContextValid(group) && group.ID == *groupID {
		return group.GatewayPoolActiveAccountLimit()
	}
	cache, _ := ctx.Value(gatewayPoolGroupLimitsKey{}).(*gatewayPoolGroupLimits)
	if cache != nil {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if limit, exists := cache.values[*groupID]; exists {
			return limit
		}
	}
	limit := gatewayPoolActiveAccountsDefault
	if s != nil && s.schedulerSnapshot != nil {
		readCtx, cancel := context.WithTimeout(ctx, gatewayPoolGroupReadTimeout)
		defer cancel()
		group, err := s.schedulerSnapshot.GetGroupByIDLite(readCtx, *groupID)
		if err != nil {
			slog.Warn("gwpool_group_limit_unavailable", "group_id", *groupID)
		} else if group != nil && group.ID == *groupID {
			limit = group.GatewayPoolActiveAccountLimit()
		}
	}
	if cache != nil {
		if cache.values == nil {
			cache.values = map[int64]int{}
		}
		cache.values[*groupID] = limit
	}
	return limit
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
		return (exhausted && !state.continuousDomain(domain)) || (complete && !known[domain])
	})
	if len(state.active) > limit {
		state.active = state.active[:limit] // does not cancel already dispatched work
	}
	for _, domain := range candidates {
		if len(state.active) >= limit {
			break
		}
		if _, exhausted := state.exhausted[domain]; domain != "" && (!exhausted || state.continuousDomain(domain)) && !slices.Contains(state.active, domain) {
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
