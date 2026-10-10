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
	gatewayPoolNeutralQualityScore = 0.5
)

type gatewayPoolCandidateRanking struct {
	candidates []gwpool.Gateway // unscored FIFO admission baseline
	quality    map[string]float64
	adaptive   map[string]float64
	preferred  map[string]bool // existing quality score strictly above neutral
	explore    bool
	deferUS    bool
	scored     bool // this choice advances the existing exploration counter
}

// Rebuild two logical queues at each pick. A recovered quality candidate can
// bypass the ordinary backlog; bounded exploration still rotates the ordinary
// FIFO. Scores never overwrite the stored FIFO. US deferral remains final/soft.
func (r gatewayPoolCandidateRanking) order(candidates []gwpool.Gateway) []gwpool.Gateway {
	rank := func(group []gwpool.Gateway) []gwpool.Gateway {
		return rankGatewayPoolAdaptive(rankGatewayPoolAdaptive(group, r.quality), r.adaptive)
	}
	var preferred, ordinary []gwpool.Gateway
	for _, candidate := range candidates {
		if r.preferred[candidate.Name] {
			preferred = append(preferred, candidate)
		} else {
			ordinary = append(ordinary, candidate)
		}
	}
	var out []gwpool.Gateway
	if r.explore {
		out = append(ordinary, preferred...)
	} else {
		out = append(rank(preferred), rank(ordinary)...)
	}
	if r.deferUS {
		out = append([]gwpool.Gateway(nil), out...)
		sort.SliceStable(out, func(i, j int) bool {
			return out[i].DatacenterCountry != "US" && out[j].DatacenterCountry == "US"
		})
	}
	return out
}

func (s *openAICodexCookieStore) gatewayPoolRankCandidates(ctx context.Context, account *Account, identity string, candidates []gwpool.Gateway) gatewayPoolCandidateRanking {
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
	projection := gatewayPoolRankProjection{
		state: state, model: model, source: source, now: now, complete: freshComplete,
		retryAfter: func(gateway string) time.Duration {
			if cooldown, ok := s.cooldownEntry(identity, gateway); ok {
				return time.Duration(cooldown.WindowSeconds) * time.Second
			}
			return time.Duration(gatewayPoolCooldownBase(fresh.gatewayPoolGatewayWindow())) * time.Second
		},
	}
	ranking := projection.rank(candidates)
	if !ranking.scored {
		return ranking
	}
	value, _ := s.poolContactPicks.LoadOrStore(gatewayPoolLedgerTag(identity), &atomic.Uint64{})
	counter, ok := value.(*atomic.Uint64)
	if !ok {
		panic("gwpool contact pick counter has an invalid type")
	}
	ranking.explore = counter.Add(1)%gatewayPoolContactExploreEvery == 0
	if ranking.explore {
		ranking.quality, ranking.adaptive = nil, nil
	}
	return ranking
}

// The same pure projection powers dispatch and its read-only preview. Only the
// dispatch caller above advances the real exploration counter.
type gatewayPoolRankProjection struct {
	state      gatewayPoolContacts
	model      string
	source     string
	now        time.Time
	complete   bool
	retryAfter func(string) time.Duration
}

func (p gatewayPoolRankProjection) rank(candidates []gwpool.Gateway) gatewayPoolCandidateRanking {
	var local map[string][]gwpool.ContactStats
	if p.complete {
		local = gatewayPoolLocalContactStats(p.state, p.model, p.source, p.now)
	}
	effective := gatewayPoolPreferLocalContacts(candidates, local)
	ranking := gatewayPoolCandidateRanking{
		candidates: candidates, preferred: make(map[string]bool),
		quality: gatewayPoolQualityScores(effective, p.state, local, p.model, p.source, p.now),
		deferUS: gatewayPoolUSBackoffActive(p.state.LastUSAt, p.now),
	}
	for name, score := range ranking.quality {
		if score > gatewayPoolNeutralQualityScore {
			ranking.preferred[name] = true
		}
	}
	ranking.scored = len(candidates) > 1 && len(ranking.quality) > 0
	if p.complete {
		ranking.adaptive = gatewayPoolAdaptiveScores(effective, p.state, p.model, p.source, p.now, p.retryAfter)
	}
	return ranking
}

// Forecast the attempt order under this snapshot, removing each selected
// candidate locally. Inventory/verdict changes can revise the next forecast.
func (p gatewayPoolRankProjection) visitPreview(candidates []gwpool.Gateway, picks uint64, visit func(int, gwpool.Gateway) bool) {
	remaining := append([]gwpool.Gateway(nil), candidates...)
	for position := 0; len(remaining) > 0; position++ {
		ranking := p.rank(remaining)
		if ranking.scored {
			picks++
			ranking.explore = picks%gatewayPoolContactExploreEvery == 0
		}
		next := ranking.order(remaining)[0]
		if !visit(position, next) {
			return
		}
		for i, candidate := range remaining {
			if candidate.Name == next.Name {
				remaining = append(remaining[:i], remaining[i+1:]...)
				break
			}
		}
	}
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
