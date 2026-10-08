package service

import (
	"sort"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	gatewayPoolAdaptiveMinWindows = 3
	// Contact windows include idle time. Bound their ranking contribution;
	// this is not a ticket lifetime, expiry, or a recovery guarantee.
	gatewayPoolAdaptiveWindowLimit = 3 * time.Minute
)

// Contacts may be retained local measurements or pool fallback. The rank caller
// already replaced qualified local strata; duplicate strata remain ambiguous.
func gatewayPoolPersonalStats(candidate gwpool.Gateway, model, source, interval string) (gwpool.ContactStats, bool) {
	var result gwpool.ContactStats
	found := false
	for _, row := range candidate.Contacts {
		if row.Gateway != candidate.Name || row.Model != model || row.Source != source ||
			row.Criterion != gwpool.ContactCriterion || row.First != "repeat" || row.Interval != interval {
			continue
		}
		if found || !row.Valid() || row.Full+row.Refreshed < gatewayPoolContactMinResults {
			return gwpool.ContactStats{}, false
		}
		result, found = row, true
	}
	return result, found
}

// Scores are a bounded one-reuse heuristic, not a counterfactual replay or
// calibrated capability probabilities. All input candidates already passed the
// ordinary eligibility gate; this function cannot shorten or learn a cooldown.
func gatewayPoolAdaptiveScores(candidates []gwpool.Gateway, state gatewayPoolContacts,
	model, source string, now time.Time, retryAfter func(string) time.Duration) map[string]float64 {
	if model == "" || source != "foreground" || state.HistoryTruncated {
		return nil
	}
	type observation struct {
		candidate gwpool.Gateway
		evidence  gatewayPoolRankEvidence
		window    time.Duration
	}
	var observations []observation
	var evidence []gatewayPoolRankEvidence
	seenNames := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		if seenNames[candidate.Name] {
			return nil
		}
		seenNames[candidate.Name] = true
		last := state.Seen[candidate.Name].LastAt
		if candidate.Name == "" || !candidate.PairReady || last.IsZero() || last.After(now) ||
			now.Sub(last) > gatewayPoolMaxObservedIdle ||
			(gatewayPoolUSBackoffActive(state.LastUSAt, now) && candidate.DatacenterCountry == "US") {
			continue
		}
		stats, ok := gatewayPoolPersonalStats(candidate, model, source,
			gwpool.ContactInterval(true, int64(now.Sub(last)/time.Second)))
		if !ok || stats.WindowSamples < gatewayPoolAdaptiveMinWindows || stats.WindowMeanMS <= 0 {
			continue
		}
		e := gatewayPoolRankEvidence{full: stats.Full, total: stats.Full + stats.Refreshed}
		window := min(time.Duration(stats.WindowMeanMS)*time.Millisecond, gatewayPoolAdaptiveWindowLimit)
		observations = append(observations, observation{candidate, e, window})
		evidence = append(evidence, e)
	}
	if len(observations) < 2 {
		return nil
	}
	prior := gatewayPoolRankPrior(evidence)
	gains := make(map[string]float64, len(observations))
	total := 0.0
	for _, o := range observations {
		gain := gatewayPoolRankScore(o.evidence, prior) * o.window.Seconds()
		gains[o.candidate.Name] = gain
		total += gain
	}
	scores := make(map[string]float64, len(observations))
	for _, o := range observations {
		gain := gains[o.candidate.Name]
		score := gain
		coverage := total - gain
		if retryAfter != nil {
			delay := retryAfter(o.candidate.Name)
			// Only value a second use if other measured candidates could cover
			// this *local* wait and that future idle stratum has personal evidence.
			if delay > 0 && coverage >= delay.Seconds() {
				stats, ok := gatewayPoolPersonalStats(o.candidate, model, source,
					gwpool.ContactInterval(true, int64(coverage)))
				if ok && stats.WindowSamples >= gatewayPoolAdaptiveMinWindows && stats.WindowMeanMS > 0 {
					reuse := gatewayPoolRankScore(gatewayPoolRankEvidence{
						full: stats.Full, total: stats.Full + stats.Refreshed}, prior)
					reuseWindow := min(time.Duration(stats.WindowMeanMS)*time.Millisecond, o.window)
					score += gain * reuse * reuseWindow.Seconds() / o.window.Seconds()
				}
			}
		}
		scores[o.candidate.Name] = score
	}
	return scores
}

// Unknown positions stay where the fallback policy put them. Only comparable
// measured candidates can exchange positions, including returned queue members.
func rankGatewayPoolAdaptive(candidates []gwpool.Gateway, scores map[string]float64) []gwpool.Gateway {
	if len(scores) < 2 {
		return candidates
	}
	out := append([]gwpool.Gateway(nil), candidates...)
	var indexes []int
	var measured []gwpool.Gateway
	for i, candidate := range out {
		if _, ok := scores[candidate.Name]; ok {
			indexes = append(indexes, i)
			measured = append(measured, candidate)
		}
	}
	sort.SliceStable(measured, func(i, j int) bool {
		return scores[measured[i].Name] > scores[measured[j].Name]
	})
	for i, index := range indexes {
		out[index] = measured[i]
	}
	return out
}
