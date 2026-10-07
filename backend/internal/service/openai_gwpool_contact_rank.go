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

// Only exchange positions of candidates with comparable evidence. Unmeasured
// candidates keep their baseline positions; statistics never bypass eligibility,
// extend cooldowns, or blacklist a gateway.
func rankGatewayPoolContacts(candidates []gwpool.Gateway, seen map[string]gatewayPoolContactSeen,
	model, source string, now time.Time) []gwpool.Gateway {
	type measured struct {
		candidate gwpool.Gateway
		full, n   int
	}
	var indexes []int
	var ranked []measured
	for i, candidate := range candidates {
		last := seen[candidate.Name].LastAt
		if last.IsZero() || last.After(now) || now.Sub(last) > 30*24*time.Hour {
			continue
		}
		interval := gwpool.ContactInterval(true, int64(now.Sub(last)/time.Second))
		for _, stats := range candidate.Contacts {
			if !stats.Valid() || stats.Gateway != candidate.Name || stats.Model != model ||
				stats.Criterion != gwpool.ContactCriterion || stats.Source != source || stats.First != "repeat" ||
				stats.Interval != interval || stats.Full+stats.Refreshed < gatewayPoolContactMinResults {
				continue
			}
			indexes = append(indexes, i)
			ranked = append(ranked, measured{candidate: candidate, full: stats.Full, n: stats.Full + stats.Refreshed})
			break
		}
	}
	if len(ranked) < 2 {
		return candidates
	}
	evidence := make([]gatewayPoolRankEvidence, 0, len(ranked))
	for _, candidate := range ranked {
		evidence = append(evidence, gatewayPoolRankEvidence{candidate.full, candidate.n})
	}
	prior := gatewayPoolRankPrior(evidence)
	sort.SliceStable(ranked, func(i, j int) bool {
		return gatewayPoolRankScore(gatewayPoolRankEvidence{ranked[i].full, ranked[i].n}, prior) >
			gatewayPoolRankScore(gatewayPoolRankEvidence{ranked[j].full, ranked[j].n}, prior)
	})
	out := append([]gwpool.Gateway(nil), candidates...)
	for i, index := range indexes {
		out[index] = ranked[i].candidate
	}
	return out
}

type gatewayPoolCandidateRanking struct {
	candidates []gwpool.Gateway
	adaptive   map[string]float64
}

func (s *openAICodexCookieStore) gatewayPoolRankContacts(ctx context.Context, account *Account, identity string, candidates []gwpool.Gateway) []gwpool.Gateway {
	ranking := s.gatewayPoolRankCandidates(ctx, account, identity, candidates)
	return rankGatewayPoolAdaptive(ranking.candidates, ranking.adaptive)
}

func (s *openAICodexCookieStore) gatewayPoolRankCandidates(ctx context.Context, account *Account, identity string, candidates []gwpool.Gateway) gatewayPoolCandidateRanking {
	model, _ := ctx.Value(gatewayPoolProbeModelKey{}).(string)
	if len(candidates) < 2 {
		return gatewayPoolCandidateRanking{candidates: candidates}
	}
	available, global := false, false
	for _, candidate := range candidates {
		if len(candidate.Contacts) > 0 {
			available = true
		}
		global = global || candidate.Priority.Valid(model)
	}
	state := gatewayPoolContacts{}
	freshComplete := false
	fresh, err := s.freshGatewayPoolAccount(ctx, account)
	if err == nil && fresh != nil {
		tag := gatewayPoolLedgerTag(identity)
		state = readGatewayPoolContacts(fresh, tag)
		freshComplete = s.accountByID != nil && s.historyByTag != nil && ctx.Err() == nil
		if s.historyByTag != nil {
			peers, err := s.historyByTag(ctx, tag)
			if err == nil {
				for i := range peers {
					mergeGatewayPoolContacts(&state, readGatewayPoolContacts(&peers[i], tag))
				}
			} else {
				freshComplete = false
			}
		}
	}
	out := append([]gwpool.Gateway(nil), candidates...)
	optimize := false
	if model != "" && (available || global) {
		value, _ := s.poolContactPicks.LoadOrStore(gatewayPoolLedgerTag(identity), &atomic.Uint64{})
		counter, ok := value.(*atomic.Uint64)
		if !ok {
			panic("gwpool contact pick counter has an invalid type")
		}
		if counter.Add(1)%gatewayPoolContactExploreEvery != 0 {
			optimize = true
			if global {
				// Shrink only comparable same-model evidence toward this
				// eligible cohort's pooled rate. Unknown candidates retain the
				// existing neutral 1/2 score, not an invented failure count.
				evidence := make([]gatewayPoolRankEvidence, 0, len(out))
				for _, candidate := range out {
					if candidate.Priority.Valid(model) {
						evidence = append(evidence, gatewayPoolRankEvidence{
							candidate.Priority.Full, candidate.Priority.Samples})
					}
				}
				prior := gatewayPoolRankPrior(evidence)
				score := func(candidate gwpool.Gateway) float64 {
					if candidate.Priority.Valid(model) {
						return gatewayPoolRankScore(gatewayPoolRankEvidence{
							candidate.Priority.Full, candidate.Priority.Samples}, prior)
					}
					return 0.5
				}
				sort.SliceStable(out, func(i, j int) bool {
					return score(out[i]) > score(out[j])
				})
			} else {
				// Backward compatibility with pools without global priorities.
				out = rankGatewayPoolContacts(out, state.Seen, model, gatewayPoolProbeSource(ctx), time.Now())
			}
		}
	}
	var adaptive map[string]float64
	if optimize && freshComplete {
		current, identityErr := s.gatewayPoolIdentity(ctx, fresh)
		if identityErr == nil && gatewayPoolLedgerTag(current) == gatewayPoolLedgerTag(identity) {
			adaptive = gatewayPoolAdaptiveScores(out, state, model, gatewayPoolProbeSource(ctx), time.Now(),
				func(gateway string) time.Duration {
					if cooldown, ok := s.cooldownEntry(identity, gateway); ok {
						return time.Duration(cooldown.WindowSeconds) * time.Second
					}
					base, _ := s.gatewayPoolInitialCooldown(identity, gateway, fresh.gatewayPoolGatewayWindow(),
						fresh.gatewayPoolUseRecommendation())
					return time.Duration(base) * time.Second
				})
		}
	}
	if gatewayPoolUSBackoffActive(state.LastUSAt, time.Now()) {
		sort.SliceStable(out, func(i, j int) bool {
			return out[i].DatacenterCountry != "US" && out[j].DatacenterCountry == "US"
		})
	}
	// Queue admission must receive the untouched fallback order. Applying the
	// adaptive order here would permanently bias first admission and re-entry.
	return gatewayPoolCandidateRanking{candidates: out, adaptive: adaptive}
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

// Reselect among untried spares using the newest listing. A failed listing is
// not an empty inventory; keep the original candidates and apply known US history.
func (b *gatewayPoolTicketBatch) rankRemaining(ctx context.Context) {
	if b.idx >= len(b.pairs) {
		return
	}
	candidates := make([]gwpool.Gateway, 0, len(b.pairs)-b.idx)
	catalog := map[string]gwpool.Gateway{}
	listed := false
	if pool, err := b.store.poolClient(b.account); err == nil {
		listCtx, cancel := context.WithTimeout(ctx, b.account.gatewayPoolListTimeout())
		model, _ := ctx.Value(gatewayPoolProbeModelKey{}).(string)
		list, err := pool.GatewaysForModel(listCtx, gatewayPoolUpstreamAccountID(b.identity),
			gatewayPoolAccountTag(b.account, b.identity), model)
		cancel()
		if err == nil {
			listed = true
			b.store.noteGatewayPoolCountries(b.account, list)
			for _, candidate := range list {
				catalog[candidate.Name] = candidate
			}
		}
	}
	held := map[string]bool{}
	for _, pair := range b.pairs[b.idx:] {
		held[pair.gateway] = true
		if pair.invalidated || pair.routeExpired(time.Now()) {
			continue
		}
		if _, cooling := b.store.gatewayPoolUsedAt(b.identity, pair.gateway, b.account.gatewayPoolGatewayWindow(), b.account.gatewayPoolUseRecommendation()); cooling {
			continue
		}
		candidate, ok := catalog[pair.gateway]
		if !ok {
			candidate = gwpool.Gateway{Name: pair.gateway, DatacenterCountry: pair.datacenterCountry}
		}
		candidates = append(candidates, candidate)
	}
	if listed {
		// The newest best route may lie outside the prefetched batch. Do not
		// force a US fallback or stale low-priority spare while a better
		// eligible global candidate is available.
		var additional []gwpool.Gateway
		for _, candidate := range catalog {
			if held[candidate.Name] || !candidate.PairReady {
				continue
			}
			if _, cooling := b.store.gatewayPoolUsedAt(b.identity, candidate.Name, b.account.gatewayPoolGatewayWindow(), b.account.gatewayPoolUseRecommendation()); !cooling {
				additional = append(additional, candidate)
			}
		}
		sort.Slice(additional, func(i, j int) bool {
			if additional[i].LastUsedAt.Equal(additional[j].LastUsedAt) {
				return additional[i].Name < additional[j].Name
			}
			return additional[i].LastUsedAt.Before(additional[j].LastUsedAt)
		})
		candidates = append(candidates, additional...)
	}
	candidates = b.store.gatewayPoolRankContacts(ctx, b.account, b.identity, candidates)
	if len(candidates) == 0 || !held[candidates[0].Name] {
		b.releaseSpare()
		return
	}
	positions := map[string]int{}
	for i, candidate := range candidates {
		positions[candidate.Name] = i
	}
	sort.SliceStable(b.pairs[b.idx:], func(i, j int) bool {
		left, leftOK := positions[b.pairs[b.idx+i].gateway]
		right, rightOK := positions[b.pairs[b.idx+j].gateway]
		if leftOK != rightOK {
			return leftOK
		}
		return left < right
	})
}
