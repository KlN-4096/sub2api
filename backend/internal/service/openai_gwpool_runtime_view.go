package service

import (
	"context"
	"sort"
	"time"
)

type GatewayPoolLiveTicket struct {
	Gateway        string    `json:"gateway"`
	Region         string    `json:"region"`
	ExpiresAt      time.Time `json:"expires_at,omitzero"`
	VerifiedAt     time.Time `json:"verified_at,omitzero"`
	VerifiedModels []string  `json:"verified_models"`
}

type GatewayPoolRuntimeView struct {
	ObservedAt time.Time                          `json:"observed_at"`
	Tickets    []GatewayPoolLiveTicket            `json:"tickets"`
	Rounds     []GatewayPoolUsageRound            `json:"rounds"`
	Archived   map[string]GatewayPoolUsageArchive `json:"archived"`
	Incomplete bool                               `json:"incomplete,omitempty"`
}

// Read-only local snapshot. This endpoint never fetches tickets, lists gateways,
// ends a round, or probes an upstream model.
func (s *OpenAIGatewayService) GatewayPoolRuntimeProgress(ctx context.Context, ids []int64) (map[int64]GatewayPoolProgress, error) {
	if s.accountRepo == nil {
		return s.GatewayPoolProgress(ids), nil
	}
	out := map[int64]GatewayPoolProgress{}
	accounts, err := s.accountRepo.GetByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, account := range accounts {
		if account == nil || !s.codexCookies.gatewayPoolTakeover(account) {
			continue
		}
		identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
		if err != nil {
			continue
		}
		tag := gatewayPoolLedgerTag(identity)
		state := readGatewayPoolUsage(account, tag)
		if cached, ok := s.codexCookies.poolUsageCache.Load(tag); ok {
			if other, valid := cached.(*gatewayPoolUsageLedger); valid && other.UpdatedAt.After(state.UpdatedAt) {
				state = *other
			}
		} else {
			for _, key := range []string{gatewayPoolUsageTagKey, gatewayPoolUsagePreviousTagKey} {
				peers, err := s.accountRepo.FindByExtraField(ctx, key, tag)
				if err != nil {
					return nil, err
				}
				for i := range peers {
					other := readGatewayPoolUsage(&peers[i], tag)
					if other.UpdatedAt.After(state.UpdatedAt) {
						state = other
					}
				}
			}
			s.codexCookies.poolUsageCache.LoadOrStore(tag, &state)
		}
		runtime := &GatewayPoolRuntimeView{ObservedAt: time.Now().UTC(), Tickets: []GatewayPoolLiveTicket{},
			Rounds: make([]GatewayPoolUsageRound, len(state.Rounds)), Archived: state.Archived, Incomplete: state.Incomplete}
		live := s.codexCookies.gatewayPoolUsageLive(identity)
		session := s.codexCookies.gatewayPoolUsageSession()
		blockedAt := gatewayPoolUsageBlockedAt(account)
		var idleAt time.Time
		inventory := s.codexCookies.gatewayPoolInventory(identity)
		inventory.mu.Lock()
		if inventory.active == 0 {
			idleAt = state.idleCutoff(runtime.ObservedAt)
		}
		inventory.mu.Unlock()
		for i := range state.Rounds {
			round := state.Rounds[i] // read-only projection; never mutate cached ticket maps
			if round.Model == gatewayPoolUsageSharedModel && round.EndedAt.IsZero() {
				if !idleAt.IsZero() && !idleAt.Before(round.StartedAt) {
					round.EndedAt, round.EndReason = idleAt, "idle_timeout"
				}
				if !blockedAt.IsZero() && !blockedAt.Before(round.StartedAt) &&
					(round.EndedAt.IsZero() || blockedAt.Before(round.EndedAt)) {
					round.EndedAt, round.EndReason = blockedAt, "temporarily_unschedulable"
				}
			}
			runtime.Rounds[i] = round.fullUsageView(live, session, runtime.ObservedAt)
		}
		if pair, live := s.codexCookies.cachedPoolPair(identity); live == openAIGatewayPoolPairLive {
			ticket := GatewayPoolLiveTicket{Gateway: pair.gateway, Region: pair.region, ExpiresAt: pair.routeExpiresAt, VerifiedModels: []string{}}
			if mark, ok := s.codexCookies.gatewayPoolVerifiedMarkOf(identity); ok && mark.version == pair.version && mark.models != nil {
				ticket.VerifiedAt = mark.at
				for model := range *mark.models {
					ticket.VerifiedModels = append(ticket.VerifiedModels, model)
				}
				sort.Strings(ticket.VerifiedModels)
			}
			runtime.Tickets = append(runtime.Tickets, ticket)
		}
		closedBefore := state.ClosedBefore[gatewayPoolUsageSharedModel]
		for _, boundary := range []time.Time{blockedAt, idleAt} {
			if boundary.After(closedBefore) {
				closedBefore = boundary
			}
		}
		// Scope filtering must precede selection and retention: otherwise a
		// late old run can evict the new cycle's last completed progress.
		progress := s.codexCookies.poolProgress.snapshot([]int64{account.ID}, runtime.ObservedAt,
			map[int64]gatewayPoolProgressScope{account.ID: {tag: tag, closedBefore: closedBefore}})[account.ID]
		if progress.Phase == "" {
			progress.Phase = "idle"
		}
		progress.Runtime = runtime
		out[account.ID] = progress
	}
	return out, nil
}

func (s *adminServiceImpl) GatewayPoolRuntimeProgress(ctx context.Context, ids []int64) (map[int64]GatewayPoolProgress, error) {
	if reader, ok := s.runtimeBlocker.(interface {
		GatewayPoolRuntimeProgress(context.Context, []int64) (map[int64]GatewayPoolProgress, error)
	}); ok {
		return reader.GatewayPoolRuntimeProgress(ctx, ids)
	}
	return s.GatewayPoolProgress(ids), nil
}
