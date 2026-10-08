package service

import (
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const gatewayPoolContactMaxGap = 30 * 24 * time.Hour

// Aggregate only retained, member-scoped first-contact reports. Later business
// observations and confirmation probes do not become new initial outcomes.
// Clone copies share report IDs; a closed window updates that report, not its count.
func gatewayPoolLocalContactStats(state gatewayPoolContacts, model, source string, now time.Time) map[string][]gwpool.ContactStats {
	if model == "" || source != "foreground" {
		return nil
	}
	reports := make(map[string]gwpool.ContactReport, len(state.Rounds))
	conflicting := make(map[string]bool)
	for _, round := range state.Rounds {
		report := round.Report
		if len(report.ID) != 64 || report.Gateway == "" || report.Model != model ||
			report.Source != source || report.Criterion != gwpool.ContactCriterion ||
			report.First != "repeat" || !report.GapKnown || report.ElapsedSeconds < 0 ||
			report.ElapsedSeconds > int64(gatewayPoolContactMaxGap/time.Second) ||
			report.At.Before(now.Add(-gatewayPoolOutboxRetention)) || report.At.After(now) {
			continue
		}
		if previous, ok := reports[report.ID]; ok {
			// Immutable classification must agree; window completion is the only
			// allowed report update used by this projection.
			a, b := previous, report
			a.WindowFinal, b.WindowFinal = false, false
			a.FullWindowMS, b.FullWindowMS = 0, 0
			if a != b || (previous.WindowFinal && report.WindowFinal && previous.FullWindowMS != report.FullWindowMS) {
				conflicting[report.ID] = true
				continue
			}
			if previous.WindowFinal {
				continue
			}
		}
		reports[report.ID] = report
	}
	type stratum struct{ gateway, interval string }
	type aggregate struct {
		stats    gwpool.ContactStats
		windowMS int64
	}
	rows := make(map[stratum]aggregate)
	for id, report := range reports {
		if conflicting[id] {
			continue
		}
		key := stratum{report.Gateway, gwpool.ContactInterval(true, report.ElapsedSeconds)}
		row := rows[key]
		if row.stats.Gateway == "" {
			row.stats = gwpool.ContactStats{Gateway: key.gateway, Model: model, Criterion: gwpool.ContactCriterion,
				Source: source, First: "repeat", Interval: key.interval}
		}
		switch report.Outcome {
		case "full":
			row.stats.Full++
			if report.WindowFinal && report.FullWindowMS > 0 && report.FullWindowMS <= (10*time.Hour).Milliseconds() {
				row.stats.WindowSamples++
				row.windowMS += report.FullWindowMS
			}
		case "refreshed":
			row.stats.Refreshed++
		case "unknown":
			row.stats.Unknown++
		default:
			continue
		}
		rows[key] = row
	}
	out := make(map[string][]gwpool.ContactStats)
	for key, row := range rows {
		if row.stats.WindowSamples > 0 {
			row.stats.WindowMeanMS = row.windowMS / int64(row.stats.WindowSamples)
		}
		if row.stats.Valid() && row.stats.Full+row.stats.Refreshed >= gatewayPoolContactMinResults {
			out[key.gateway] = append(out[key.gateway], row.stats)
		}
	}
	return out
}

// Local evidence replaces, rather than adds to, the pool's same-stratum counts.
// In particular, a sparse local window cannot borrow a contradictory pool mean.
func gatewayPoolPreferLocalContacts(candidates []gwpool.Gateway, local map[string][]gwpool.ContactStats) []gwpool.Gateway {
	out := append([]gwpool.Gateway(nil), candidates...)
	for i := range out {
		rows := local[out[i].Name]
		if len(rows) == 0 {
			continue
		}
		contacts := append([]gwpool.ContactStats(nil), rows...)
		for _, remote := range out[i].Contacts {
			replaced := false
			for _, row := range rows {
				if row.Model == remote.Model && row.Source == remote.Source && row.First == remote.First &&
					row.Criterion == remote.Criterion && row.Interval == remote.Interval {
					replaced = true
					break
				}
			}
			if !replaced {
				contacts = append(contacts, remote)
			}
		}
		out[i].Contacts = contacts
	}
	return out
}

// Tier admission uses qualified member-scoped repeat observations at the actual
// idle interval. Cross-member Priority and unknown's neutral score cannot grant
// preferred status. This is a reuse heuristic, never proof of faster recovery.
func gatewayPoolPreferredCandidates(candidates []gwpool.Gateway, state gatewayPoolContacts,
	model, source string, now time.Time,
) map[string]bool {
	if model == "" || source != "foreground" || state.HistoryTruncated {
		return nil
	}
	preferred := make(map[string]bool)
	for _, candidate := range candidates {
		last := state.Seen[candidate.Name].LastAt
		if !candidate.PairReady || last.IsZero() || last.After(now) || now.Sub(last) > gatewayPoolContactMaxGap {
			continue
		}
		stats, ok := gatewayPoolPersonalStats(candidate, model, source,
			gwpool.ContactInterval(true, int64(now.Sub(last)/time.Second)))
		if ok && stats.Full > stats.Refreshed {
			preferred[candidate.Name] = true
		}
	}
	return preferred
}

// Rates use the current actual idle stratum. Qualified local measurements win
// over both pool Priority and pool Contacts. Pool data is only a fallback.
func gatewayPoolQualityScores(candidates []gwpool.Gateway, state gatewayPoolContacts,
	local map[string][]gwpool.ContactStats, model, source string, now time.Time,
) map[string]float64 {
	if model == "" {
		return nil
	}
	evidence := make(map[string]gatewayPoolRankEvidence)
	global := false
	for _, candidate := range candidates {
		last := state.Seen[candidate.Name].LastAt
		interval := ""
		if !last.IsZero() && !last.After(now) && now.Sub(last) <= gatewayPoolContactMaxGap {
			interval = gwpool.ContactInterval(true, int64(now.Sub(last)/time.Second))
		}
		stats, localOK := gatewayPoolPersonalStats(gwpool.Gateway{Name: candidate.Name, Contacts: local[candidate.Name]},
			model, source, interval)
		switch {
		case localOK:
			evidence[candidate.Name] = gatewayPoolRankEvidence{stats.Full, stats.Full + stats.Refreshed}
		case candidate.Priority.Valid(model):
			evidence[candidate.Name] = gatewayPoolRankEvidence{candidate.Priority.Full, candidate.Priority.Samples}
			global = true
		default:
			if stats, ok := gatewayPoolPersonalStats(candidate, model, source, interval); ok {
				evidence[candidate.Name] = gatewayPoolRankEvidence{stats.Full, stats.Full + stats.Refreshed}
			}
		}
	}
	if len(evidence) == 0 {
		return nil
	}
	cohort := make([]gatewayPoolRankEvidence, 0, len(evidence))
	for _, row := range evidence {
		cohort = append(cohort, row)
	}
	prior := gatewayPoolRankPrior(cohort)
	scores := make(map[string]float64, len(candidates))
	for _, candidate := range candidates {
		if row, ok := evidence[candidate.Name]; ok {
			scores[candidate.Name] = gatewayPoolRankScore(row, prior)
		} else if global {
			scores[candidate.Name] = 0.5 // existing global fallback, never an invented failure
		}
	}
	return scores
}
