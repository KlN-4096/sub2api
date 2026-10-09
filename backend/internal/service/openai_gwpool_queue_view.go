package service

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const gatewayPoolQueuePreviewLimit = 3

type GatewayPoolQueueGroup struct {
	Count    int      `json:"count"`
	Gateways []string `json:"gateways"`
}

type GatewayPoolQueueView struct {
	Model      string                `json:"model"`
	ValidUntil time.Time             `json:"valid_until"`
	Quality    GatewayPoolQueueGroup `json:"quality"`
	Ordinary   GatewayPoolQueueGroup `json:"ordinary"`
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
	catalog, until, known := pool.PeekCatalog(gatewayPoolUpstreamAccountID(identity), gatewayPoolAccountTag(account, identity), model)
	if !known || !now.Before(until) {
		return nil
	}
	deadlines := s.gatewayPoolDisplayCooldownDeadlines(identity, account, history, now)
	pair, pairState := s.cachedPoolPair(identity)
	eligible := make([]gwpool.Gateway, 0, len(catalog.Gateways))
	for _, gateway := range catalog.Gateways {
		if gateway.ReadyAt(now) && !now.Before(deadlines[gateway.Name]) &&
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
	const source = "foreground"
	local := gatewayPoolLocalContactStats(contacts, model, source, now)
	effective := gatewayPoolPreferLocalContacts(eligible, local)
	ranking := gatewayPoolCandidateRanking{
		preferred: gatewayPoolPreferredCandidates(effective, contacts, model, source, now),
		quality:   gatewayPoolQualityScores(effective, contacts, local, model, source, now),
		deferUS:   gatewayPoolUSBackoffActive(contacts.LastUSAt, now),
		adaptive: gatewayPoolAdaptiveScores(effective, contacts, model, source, now,
			func(gateway string) time.Duration {
				if cooldown, ok := s.cooldownEntry(identity, gateway); ok {
					return time.Duration(cooldown.WindowSeconds) * time.Second
				}
				return time.Duration(gatewayPoolCooldownBase(account.gatewayPoolGatewayWindow())) * time.Second
			}),
	}
	view := &GatewayPoolQueueView{
		Model: model, ValidUntil: until,
		Quality: GatewayPoolQueueGroup{Gateways: []string{}}, Ordinary: GatewayPoolQueueGroup{Gateways: []string{}},
	}
	for _, gateway := range ranking.order(eligible) {
		group := &view.Ordinary
		if ranking.preferred[gateway.Name] {
			group = &view.Quality
		}
		group.Count++
		if len(group.Gateways) < gatewayPoolQueuePreviewLimit {
			group.Gateways = append(group.Gateways, gateway.Name)
		}
	}
	return view
}
