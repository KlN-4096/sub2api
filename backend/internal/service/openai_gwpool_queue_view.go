package service

import (
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const gatewayPoolQueuePreviewLimit = 3

type GatewayPoolQueueGroup struct {
	Count     int      `json:"count"`
	Gateways  []string `json:"gateways"`
	Positions []int    `json:"positions"`
}

type GatewayPoolQueueView struct {
	Model       string                `json:"model"`
	ValidUntil  time.Time             `json:"valid_until"`
	ObservedAt  time.Time             `json:"observed_at"`
	Stale       bool                  `json:"stale"`
	NextGateway string                `json:"next_gateway"`
	Quality     GatewayPoolQueueGroup `json:"quality"`
	Ordinary    GatewayPoolQueueGroup `json:"ordinary"`
}

// Read only already-fetched metadata for this exact URL/Key/member/model. The
// grouping functions are shared with dispatch, but polling never calls pick,
// hydrates cooldowns, increments the exploration counter, or contacts the pool.
func (s *openAICodexCookieStore) gatewayPoolQueueView(account *Account, identity string,
	history openAIGatewayHistory, contacts gatewayPoolContacts, now time.Time,
) *GatewayPoolQueueView {
	cached, ok := s.poolClients.Load(gatewayPoolClientCacheKey(account))
	if !ok {
		return nil
	}
	pool, ok := cached.(*gwpool.Client)
	if !ok {
		return nil
	}
	model := gatewayPoolProbeModelLuna
	catalog, known := pool.PeekCatalogSnapshot(gatewayPoolUpstreamAccountID(identity), gatewayPoolAccountTag(account, identity), model)
	if !known {
		return nil
	}
	deadlines := s.gatewayPoolDisplayCooldownDeadlines(identity, account, history, now)
	pair, pairState := s.cachedPoolPair(identity)
	eligible := make([]gwpool.Gateway, 0, len(catalog.Gateways))
	for _, gateway := range catalog.Gateways {
		if gateway.ReadyAt(catalog.ObservedAt) && !now.Before(deadlines[gateway.Name]) &&
			(pairState == openAIGatewayPoolPairNone || gateway.Name != pair.gateway) {
			eligible = append(eligible, gateway)
		}
	}
	if value, exists := s.poolCandidateQueues.Load(identity); exists {
		if queue, valid := value.(*gatewayPoolCandidateQueue); valid && queue != nil {
			eligible = queue.preview(eligible)
		}
	} else {
		eligible = gatewayPoolReconcileCandidates(nil, eligible)
	}
	projection := gatewayPoolRankProjection{
		state: contacts, model: model, source: "foreground", now: now, complete: true,
		retryAfter: func(gateway string) time.Duration {
			if cooldown, ok := s.cooldownEntry(identity, gateway); ok {
				return time.Duration(cooldown.WindowSeconds) * time.Second
			}
			return time.Duration(gatewayPoolCooldownBase(account.gatewayPoolGatewayWindow())) * time.Second
		},
	}
	ranking := projection.rank(eligible)
	var picks uint64
	if value, exists := s.poolContactPicks.Load(gatewayPoolLedgerTag(identity)); exists {
		if counter, ok := value.(*atomic.Uint64); ok {
			picks = counter.Load()
		}
	}
	view := &GatewayPoolQueueView{
		Model: model, ValidUntil: catalog.ValidUntil, ObservedAt: catalog.ObservedAt,
		Stale:    catalog.Stale || !now.Before(catalog.ValidUntil),
		Quality:  GatewayPoolQueueGroup{Gateways: []string{}, Positions: []int{}},
		Ordinary: GatewayPoolQueueGroup{Gateways: []string{}, Positions: []int{}},
	}
	for _, gateway := range eligible {
		if ranking.preferred[gateway.Name] {
			view.Quality.Count++
		} else {
			view.Ordinary.Count++
		}
	}
	projection.visitPreview(eligible, picks, func(position int, gateway gwpool.Gateway) bool {
		if position == 0 {
			view.NextGateway = gateway.Name
		}
		group := &view.Ordinary
		if ranking.preferred[gateway.Name] {
			group = &view.Quality
		}
		if len(group.Gateways) < gatewayPoolQueuePreviewLimit {
			group.Gateways = append(group.Gateways, gateway.Name)
			group.Positions = append(group.Positions, position+1)
		}
		return len(view.Quality.Gateways) < min(gatewayPoolQueuePreviewLimit, view.Quality.Count) ||
			len(view.Ordinary.Gateways) < min(gatewayPoolQueuePreviewLimit, view.Ordinary.Count)
	})
	return view
}
