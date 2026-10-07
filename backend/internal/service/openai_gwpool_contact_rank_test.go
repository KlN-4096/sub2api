package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func contactRankCandidate(name string, full int) gwpool.Gateway {
	return gwpool.Gateway{Name: name, PairReady: true, Contacts: []gwpool.ContactStats{{
		Gateway: name, Model: "astra", Criterion: gwpool.ContactCriterion, Source: "foreground",
		First: "repeat", Interval: "1-2h", Full: full, Refreshed: 5 - full,
	}}}
}

func TestGatewayPoolContactRankingKeepsUnmeasuredPositionsAndOriginalInput(t *testing.T) {
	now := time.Now()
	seen := map[string]gatewayPoolContactSeen{}
	for _, name := range []string{"low", "high"} {
		seen[name] = gatewayPoolContactSeen{LastAt: now.Add(-90 * time.Minute)}
	}
	input := []gwpool.Gateway{contactRankCandidate("low", 1), {Name: "unknown"}, contactRankCandidate("high", 4)}
	out := rankGatewayPoolContacts(input, seen, "astra", "foreground", now)
	require.Equal(t, "high", out[0].Name)
	require.Equal(t, "unknown", out[1].Name, "lack of evidence is neither punished nor promoted")
	require.Equal(t, "low", out[2].Name)
	require.Equal(t, "low", input[0].Name, "do not mutate a shared listing")
}

func TestGatewayPoolContactRankingDoesNotMixStrataOrScoreUnknown(t *testing.T) {
	now := time.Now()
	seen := map[string]gatewayPoolContactSeen{
		"low": {LastAt: now.Add(-90 * time.Minute)}, "high": {LastAt: now.Add(-90 * time.Minute)},
	}
	for _, field := range []string{"model", "source", "criterion", "first", "interval", "gateway", "too_few"} {
		t.Run(field, func(t *testing.T) {
			input := []gwpool.Gateway{contactRankCandidate("low", 1), contactRankCandidate("high", 5)}
			stats := &input[1].Contacts[0]
			switch field {
			case "model":
				stats.Model = "luna"
			case "source":
				stats.Source = "background"
			case "criterion":
				stats.Criterion = "other"
			case "first":
				stats.First = "tracked_first"
			case "interval":
				stats.Interval = "4-6h"
			case "gateway":
				stats.Gateway = "other"
			case "too_few":
				stats.Full, stats.Unknown = 1, 50
			}
			out := rankGatewayPoolContacts(input, seen, "astra", "foreground", now)
			require.Equal(t, "low", out[0].Name)
		})
	}
	require.Equal(t, "low", rankGatewayPoolContacts(
		[]gwpool.Gateway{contactRankCandidate("low", 1), contactRankCandidate("high", 5)},
		nil, "astra", "foreground", now)[0].Name, "configured cooldown cannot stand in for actual elapsed time")
}

func TestGatewayPoolContactRankingFreshReadAndEveryFifthExploration(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	now := time.Now().UTC()
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{
		openAIGatewayPoolContactsExtraKey: gatewayPoolContacts{LedgerTag: tag, Seen: map[string]gatewayPoolContactSeen{
			"low": {LastAt: now.Add(-90 * time.Minute)}, "high": {LastAt: now.Add(-90 * time.Minute)},
		}},
	}))
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "astra")
	candidates := []gwpool.Gateway{contactRankCandidate("low", 1), contactRankCandidate("high", 5)}
	for i := 1; i <= gatewayPoolContactExploreEvery; i++ {
		want := "high"
		if i == gatewayPoolContactExploreEvery {
			want = "low"
		}
		require.Equal(t, want, svc.codexCookies.gatewayPoolRankContacts(ctx, account, gwpoolTestIdentity, candidates)[0].Name)
	}
	require.Equal(t, "low", svc.codexCookies.gatewayPoolRankContacts(context.Background(), account, gwpoolTestIdentity, candidates)[0].Name)
}
