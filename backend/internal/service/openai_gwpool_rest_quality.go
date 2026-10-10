package service

import (
	"context"
	"sort"
	"time"
)

// Transient evidence for one rest entry, not persisted policy. The generation
// travels with the cohort until all rest state has been published under lock.
type gatewayPoolRestPlan struct {
	qualityGateways []string
	generation      *uint64
	group           int64
	roundGeneration uint64
}

// Capture quality from the same fresh directory that proved exhaustion, before
// cooldown filtering. Display queues are empty at this boundary and stale
// catalog snapshots must never decide a durable recovery policy.
func (s *OpenAIGatewayService) gatewayPoolExhaustedRestQuality(ctx context.Context, account *Account) *gatewayPoolRestPlan {
	exhausted := s.gatewayPoolExhaustedCatalog(ctx, account)
	if exhausted == nil {
		return nil
	}
	fresh := account
	if s.accountRepo != nil {
		var err error
		fresh, err = s.accountRepo.GetByID(ctx, account.ID)
		if err != nil || fresh == nil {
			return nil
		}
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if err != nil || identity != exhausted.identity || !fresh.UsesGatewayPool() || fresh.GatewayPoolContinuousWaitEnabled() {
		return nil
	}
	tag := gatewayPoolLedgerTag(identity)
	contacts := readGatewayPoolContacts(fresh, tag)
	if s.accountRepo != nil {
		peers, err := s.gatewayPoolHistoryPeers(ctx, tag)
		if err != nil {
			return nil // unavailable evidence is not an empty quality set
		}
		for i := range peers {
			mergeGatewayPoolContacts(&contacts, readGatewayPoolContacts(&peers[i], tag))
		}
	}
	now := time.Now()
	pruneGatewayPoolContacts(&contacts, now)
	projection := gatewayPoolRankProjection{
		state: contacts, model: gatewayPoolProbeModelLuna, source: "foreground",
		now: now, complete: s.accountRepo != nil,
		retryAfter: func(gateway string) time.Duration {
			if cooldown, found := s.codexCookies.cooldownEntry(identity, gateway); found {
				return time.Duration(cooldown.WindowSeconds) * time.Second
			}
			return time.Duration(gatewayPoolCooldownBase(fresh.gatewayPoolGatewayWindow())) * time.Second
		},
	}
	ranking := projection.rank(exhausted.gateways)
	quality := make([]string, 0, len(ranking.preferred))
	for name, preferred := range ranking.preferred {
		if preferred {
			quality = append(quality, name)
		}
	}
	sort.Strings(quality)
	after, pending, _ := s.codexCookies.gatewayPoolInventoryCandidates(identity)
	if ctx.Err() != nil || pending || after != exhausted.generation {
		return nil
	}
	return &gatewayPoolRestPlan{qualityGateways: quality, generation: &exhausted.generation}
}
