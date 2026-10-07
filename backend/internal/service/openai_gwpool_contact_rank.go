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
	sort.SliceStable(ranked, func(i, j int) bool {
		return int64(ranked[i].full)*int64(ranked[j].n) > int64(ranked[j].full)*int64(ranked[i].n)
	})
	out := append([]gwpool.Gateway(nil), candidates...)
	for i, index := range indexes {
		out[index] = ranked[i].candidate
	}
	return out
}

func (s *openAICodexCookieStore) gatewayPoolRankContacts(ctx context.Context, account *Account, identity string, candidates []gwpool.Gateway) []gwpool.Gateway {
	model, _ := ctx.Value(gatewayPoolProbeModelKey{}).(string)
	if len(candidates) < 2 {
		return candidates
	}
	available, global := false, false
	for _, candidate := range candidates {
		if len(candidate.Contacts) > 0 {
			available = true
		}
		global = global || candidate.Priority.Valid(model)
	}
	state := gatewayPoolContacts{}
	fresh, err := s.freshGatewayPoolAccount(ctx, account)
	if err == nil && fresh != nil {
		tag := gatewayPoolLedgerTag(identity)
		state = readGatewayPoolContacts(fresh, tag)
		if s.historyByTag != nil {
			peers, err := s.historyByTag(ctx, tag)
			if err == nil {
				for i := range peers {
					mergeGatewayPoolContacts(&state, readGatewayPoolContacts(&peers[i], tag))
				}
			}
		}
	}
	out := append([]gwpool.Gateway(nil), candidates...)
	if model != "" && (available || global) {
		value, _ := s.poolContactPicks.LoadOrStore(gatewayPoolLedgerTag(identity), &atomic.Uint64{})
		counter, ok := value.(*atomic.Uint64)
		if !ok {
			panic("gwpool contact pick counter has an invalid type")
		}
		if counter.Add(1)%gatewayPoolContactExploreEvery != 0 {
			if global {
				// Measured pool-wide rate is primary. Insufficient/unknown
				// evidence has a neutral 1/2 score, never an invented zero.
				score := func(candidate gwpool.Gateway) (int64, int64) {
					if candidate.Priority.Valid(model) {
						return int64(candidate.Priority.Full), int64(candidate.Priority.Samples)
					}
					return 1, 2
				}
				sort.SliceStable(out, func(i, j int) bool {
					fi, ni := score(out[i])
					fj, nj := score(out[j])
					return fi*nj > fj*ni
				})
			} else {
				// Backward compatibility with pools without global priorities.
				out = rankGatewayPoolContacts(out, state.Seen, model, gatewayPoolProbeSource(ctx), time.Now())
			}
		}
	}
	if gatewayPoolUSBackoffActive(state.LastUSAt, time.Now()) {
		sort.SliceStable(out, func(i, j int) bool {
			return out[i].DatacenterCountry != "US" && out[j].DatacenterCountry == "US"
		})
	}
	return out
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
		if time.Until(pair.until) < openAIGatewayPoolMinRemaining {
			continue
		}
		if _, cooling := b.store.gatewayPoolUsedAt(b.identity, pair.gateway, b.account.gatewayPoolGatewayWindow()); cooling {
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
			if held[candidate.Name] || !candidate.PairReady || candidate.UsedByYou {
				continue
			}
			if _, cooling := b.store.gatewayPoolUsedAt(b.identity, candidate.Name, b.account.gatewayPoolGatewayWindow()); !cooling {
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
