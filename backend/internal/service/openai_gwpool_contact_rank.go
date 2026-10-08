package service

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	gatewayPoolContactMinResults   = 5
	gatewayPoolContactExploreEvery = 5
)

type gatewayPoolCandidateRanking struct {
	candidates []gwpool.Gateway // unscored FIFO admission baseline
	quality    map[string]float64
	adaptive   map[string]float64
	deferUS    bool
}

// Apply scores to this pick only, not to the FIFO's stored order. US deferral
// is final and soft: an all-US eligible set remains usable.
func (r gatewayPoolCandidateRanking) order(candidates []gwpool.Gateway) []gwpool.Gateway {
	out := rankGatewayPoolAdaptive(candidates, r.quality)
	out = rankGatewayPoolAdaptive(out, r.adaptive)
	if r.deferUS {
		out = append([]gwpool.Gateway(nil), out...)
		sort.SliceStable(out, func(i, j int) bool {
			return out[i].DatacenterCountry != "US" && out[j].DatacenterCountry == "US"
		})
	}
	return out
}

func (s *openAICodexCookieStore) gatewayPoolRankCandidates(ctx context.Context, account *Account, identity string, candidates []gwpool.Gateway) gatewayPoolCandidateRanking {
	ranking := gatewayPoolCandidateRanking{candidates: candidates}
	if len(candidates) < 2 {
		return ranking
	}
	model, _ := ctx.Value(gatewayPoolProbeModelKey{}).(string)
	source, now := gatewayPoolProbeSource(ctx), time.Now()
	state := gatewayPoolContacts{}
	freshComplete := false
	fresh, err := s.freshGatewayPoolAccount(ctx, account)
	if err == nil && fresh != nil && s.accountByID != nil && ctx.Err() == nil {
		current, identityErr := s.gatewayPoolIdentity(ctx, fresh)
		if identityErr == nil && current == identity {
			tag := gatewayPoolLedgerTag(identity)
			state = readGatewayPoolContacts(fresh, tag)
			if s.historyByTag != nil {
				peers, err := s.historyByTag(ctx, tag)
				if err == nil && ctx.Err() == nil {
					freshComplete = true
					for i := range peers {
						mergeGatewayPoolContacts(&state, readGatewayPoolContacts(&peers[i], tag))
					}
					pruneGatewayPoolContacts(&state, now)
				}
			}
		}
	}
	ranking.deferUS = gatewayPoolUSBackoffActive(state.LastUSAt, now)
	var local map[string][]gwpool.ContactStats
	if freshComplete {
		local = gatewayPoolLocalContactStats(state, model, source, now)
	}
	effective := gatewayPoolPreferLocalContacts(candidates, local)
	quality := gatewayPoolQualityScores(effective, state, local, model, source, now)
	if len(quality) == 0 {
		return ranking
	}
	value, _ := s.poolContactPicks.LoadOrStore(gatewayPoolLedgerTag(identity), &atomic.Uint64{})
	counter, ok := value.(*atomic.Uint64)
	if !ok {
		panic("gwpool contact pick counter has an invalid type")
	}
	if counter.Add(1)%gatewayPoolContactExploreEvery == 0 {
		return ranking
	}
	ranking.quality = quality
	if freshComplete {
		ranking.adaptive = gatewayPoolAdaptiveScores(effective, state, model, source, now,
			func(gateway string) time.Duration {
				if cooldown, ok := s.cooldownEntry(identity, gateway); ok {
					return time.Duration(cooldown.WindowSeconds) * time.Second
				}
				return time.Duration(gatewayPoolCooldownBase(fresh.gatewayPoolGatewayWindow())) * time.Second
			})
	}
	return ranking
}

const gatewayPoolUSSoftBackoff = 4 * time.Hour

func gatewayPoolUSBackoffActive(last, now time.Time) bool {
	return !last.IsZero() && !last.After(now) && now.Sub(last) < gatewayPoolUSSoftBackoff
}

func (s *openAICodexCookieStore) noteGatewayPoolCountries(account *Account, gateways []gwpool.Gateway) {
	for _, gateway := range gateways {
		if gateway.DatacenterCountry != "" {
			s.poolDatacenterCountries.Store(account.gatewayPoolBaseURL()+"\x00"+gateway.Name, gateway.DatacenterCountry)
		}
	}
}
