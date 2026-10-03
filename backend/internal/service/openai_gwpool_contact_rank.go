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
	if model == "" || len(candidates) < 2 {
		return candidates
	}
	available := false
	for _, candidate := range candidates {
		if len(candidate.Contacts) > 0 {
			available = true
			break
		}
	}
	if !available {
		return candidates
	}
	value, _ := s.poolContactPicks.LoadOrStore(gatewayPoolLedgerTag(identity), &atomic.Uint64{})
	counter, ok := value.(*atomic.Uint64)
	if !ok {
		panic("gwpool contact pick counter has an invalid type")
	}
	if counter.Add(1)%gatewayPoolContactExploreEvery == 0 {
		return candidates // deterministic 20% baseline exploration, with no extra requests
	}
	fresh, err := s.freshGatewayPoolAccount(ctx, account)
	if err != nil || fresh == nil {
		return candidates
	}
	tag := gatewayPoolLedgerTag(identity)
	state := readGatewayPoolContacts(fresh, tag)
	if s.historyByTag != nil {
		peers, err := s.historyByTag(ctx, tag)
		if err != nil {
			return candidates
		}
		for i := range peers {
			mergeGatewayPoolContacts(&state, readGatewayPoolContacts(&peers[i], tag))
		}
	}
	return rankGatewayPoolContacts(candidates, state.Seen, model, gatewayPoolProbeSource(ctx), time.Now())
}
