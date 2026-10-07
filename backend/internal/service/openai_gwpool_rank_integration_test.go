package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func rankEvidenceGateway(name, model string, full, total int) gwpool.Gateway {
	return gwpool.Gateway{Name: name, PairReady: true,
		Priority: &gwpool.GatewayPriority{Model: model, Full: full, Samples: total}}
}

func TestGatewayPoolRankShrinkageGlobalPreservesExplorationAndInput(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "astra")
	candidates := []gwpool.Gateway{
		rankEvidenceGateway("tiny", "astra", 4, 5),
		rankEvidenceGateway("established", "astra", 60, 100),
		rankEvidenceGateway("low", "astra", 30, 200),
	}
	for i := 1; i <= gatewayPoolContactExploreEvery; i++ {
		want := "established"
		if i == gatewayPoolContactExploreEvery {
			want = "tiny"
		}
		out := svc.codexCookies.gatewayPoolRankContacts(ctx, account, gwpoolTestIdentity, candidates)
		require.Equal(t, want, out[0].Name)
		require.Len(t, out, len(candidates), "ranking must not add/drop eligibility")
	}
	require.Equal(t, "tiny", candidates[0].Name, "shared listing was mutated")
}

func TestGatewayPoolRankShrinkageDoesNotPoolOtherModelsOrInvalidCounts(t *testing.T) {
	for _, other := range []gwpool.Gateway{
		rankEvidenceGateway("different-model", "luna", 0, 8192),
		rankEvidenceGateway("invalid", "astra", -1, 8192),
		rankEvidenceGateway("too-few", "astra", 0, 4),
	} {
		account := gwpoolTestAccount(1)
		svc, _ := gatewayRuntimeService(account)
		ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "astra")
		candidates := []gwpool.Gateway{
			rankEvidenceGateway("tiny", "astra", 4, 5),
			rankEvidenceGateway("established", "astra", 60, 100), other,
		}
		out := svc.codexCookies.gatewayPoolRankContacts(ctx, account, gwpoolTestIdentity, candidates)
		// Without a valid low-rate cohort, these two rates bracket the pooled
		// prior and retain their order. An unrelated large zero must not flip it.
		require.Equal(t, "tiny", out[0].Name, other.Name)
	}
}

func TestGatewayPoolRankShrinkageUnknownKeepsNeutralHalf(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "astra")
	candidates := []gwpool.Gateway{
		rankEvidenceGateway("tiny", "astra", 4, 5),
		rankEvidenceGateway("low", "astra", 0, 100),
		{Name: "unknown"},
	}
	out := svc.codexCookies.gatewayPoolRankContacts(ctx, account, gwpoolTestIdentity, candidates)
	require.Equal(t, "unknown", out[0].Name,
		"unknown must retain 0.5, not inherit the much lower cohort prior")
}

func TestGatewayPoolRankShrinkageLegacyKeepsUnknownPositions(t *testing.T) {
	now := time.Now()
	seen := map[string]gatewayPoolContactSeen{}
	var candidates []gwpool.Gateway
	for _, row := range []struct {
		name        string
		full, total int
	}{{"tiny", 4, 5}, {"established", 60, 100}, {"low", 30, 200}} {
		candidate := contactRankCandidate(row.name, 0)
		candidate.Contacts[0].Full = row.full
		candidate.Contacts[0].Refreshed = row.total - row.full
		candidates = append(candidates, candidate)
		seen[row.name] = gatewayPoolContactSeen{LastAt: now.Add(-90 * time.Minute)}
	}
	candidates = append(candidates[:1], append([]gwpool.Gateway{{Name: "unknown"}}, candidates[1:]...)...)
	out := rankGatewayPoolContacts(candidates, seen, "astra", "foreground", now)
	require.Equal(t, []string{"established", "unknown", "tiny", "low"},
		[]string{out[0].Name, out[1].Name, out[2].Name, out[3].Name})
	require.Equal(t, "tiny", candidates[0].Name)
}
