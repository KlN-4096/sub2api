package service

// Ten observations of the comparable cohort's rate keep a tiny sample from
// dominating the ordering. This is a ranking heuristic, not a calibrated
// probability of capability or evidence for shortening a cooldown.
const gatewayPoolRankPriorWeight = 10.0

type gatewayPoolRankEvidence struct {
	full, total int
}

func (e gatewayPoolRankEvidence) valid() bool {
	return e.total > 0 && e.full >= 0 && e.full <= e.total
}

// The caller supplies only comparable, validated observations: same model and
// criterion, and (for the legacy path) the applicable source/interval strata.
// Unknown candidates do not contribute invented successes or failures.
func gatewayPoolRankPrior(evidence []gatewayPoolRankEvidence) float64 {
	full, total := 0.0, 0.0
	for _, e := range evidence {
		if e.valid() {
			full += float64(e.full)
			total += float64(e.total)
		}
	}
	if total == 0 {
		return 0.5
	}
	return full / total
}

func gatewayPoolRankScore(e gatewayPoolRankEvidence, prior float64) float64 {
	if !e.valid() {
		return prior
	}
	return (float64(e.full) + gatewayPoolRankPriorWeight*prior) /
		(float64(e.total) + gatewayPoolRankPriorWeight)
}
