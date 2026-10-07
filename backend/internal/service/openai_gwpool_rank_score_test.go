package service

import (
	"math"
	"testing"
)

func TestGatewayPoolRankScoreShrinksSmallSamples(t *testing.T) {
	tiny := gatewayPoolRankEvidence{full: 4, total: 5}
	established := gatewayPoolRankEvidence{full: 60, total: 100}
	low := gatewayPoolRankEvidence{full: 30, total: 200}
	prior := gatewayPoolRankPrior([]gatewayPoolRankEvidence{tiny, established, low})
	if prior != 94.0/305.0 {
		t.Fatalf("prior = %v; want the pooled observed rate", prior)
	}
	if gatewayPoolRankScore(tiny, prior) >= gatewayPoolRankScore(established, prior) {
		t.Fatal("a small 4/5 sample should not outrank 60/100 in this cohort")
	}
}

func TestGatewayPoolRankScorePreservesOrderWithEqualSupport(t *testing.T) {
	for _, prior := range []float64{0, 0.25, 0.5, 1} {
		if gatewayPoolRankScore(gatewayPoolRankEvidence{4, 5}, prior) <=
			gatewayPoolRankScore(gatewayPoolRankEvidence{1, 5}, prior) {
			t.Fatalf("reversed equal-support evidence with prior %v", prior)
		}
	}
}

func TestGatewayPoolRankScoreUnknownIsNotAFailure(t *testing.T) {
	invalid := []gatewayPoolRankEvidence{{}, {-1, 5}, {6, 5}, {0, -1}}
	if got := gatewayPoolRankPrior(invalid); got != 0.5 {
		t.Fatalf("empty prior = %v; want neutral 0.5", got)
	}
	evidence := append(invalid, gatewayPoolRankEvidence{1, 4})
	if got := gatewayPoolRankPrior(evidence); got != 0.25 {
		t.Fatalf("unknown/invalid evidence polluted prior: %v", got)
	}
	for _, e := range invalid {
		if got := gatewayPoolRankScore(e, 0.25); got != 0.25 {
			t.Fatalf("invalid evidence %+v scored %v; want prior", e, got)
		}
	}
}

func TestGatewayPoolRankScoreFiniteAndConvergesWithSupport(t *testing.T) {
	tiny := gatewayPoolRankScore(gatewayPoolRankEvidence{4, 5}, 0.2)
	large := gatewayPoolRankScore(gatewayPoolRankEvidence{800, 1000}, 0.2)
	if !(0.2 < tiny && tiny < large && large < 0.8) {
		t.Fatalf("unexpected shrinkage: tiny=%v large=%v", tiny, large)
	}
	for _, e := range []gatewayPoolRankEvidence{{0, 8192}, {8192, 8192}} {
		got := gatewayPoolRankScore(e, 0.5)
		if math.IsNaN(got) || math.IsInf(got, 0) || got < 0 || got > 1 {
			t.Fatalf("invalid score %v for %+v", got, e)
		}
	}
}
