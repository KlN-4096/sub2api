package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolDualQueueUsesExistingHighWeightWithoutPersonalMajorityGate(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return nil, nil }
	candidates := []gwpool.Gateway{
		{Name: "unknown", PairReady: true},
		{Name: "weighted", PairReady: true,
			Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 100, Samples: 100}},
	}
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, gatewayPoolProbeModelLuna)
	ranking := svc.codexCookies.gatewayPoolRankCandidates(ctx, account, gwpoolTestIdentity, candidates)
	require.Greater(t, ranking.quality["weighted"], 0.5)
	require.True(t, ranking.preferred["weighted"], "the fast tier must follow the existing score")
	require.False(t, ranking.preferred["unknown"], "neutral fallback is not a high score")
}

func TestGatewayPoolDualQueueRecoveredQualityBypassesOrdinaryBacklog(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return nil, nil }
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{
		openAIGatewayPoolContactsExtraKey: localRankHistory(time.Now(), "good", "bad", 5),
	}))
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, gatewayPoolProbeModelLuna)
	candidates := []gwpool.Gateway{
		{Name: "unknown-a", PairReady: true}, {Name: "unknown-b", PairReady: true},
		{Name: "bad", PairReady: true}, {Name: "good", PairReady: true},
	}
	queue := svc.codexCookies.gatewayPoolCandidateQueue(gwpoolTestIdentity)
	require.Equal(t, "unknown-a", queue.pick(candidates[:3]), "good is not eligible yet")
	for i := 1; i <= 3*gatewayPoolContactExploreEvery; i++ {
		ranking := svc.codexCookies.gatewayPoolRankCandidates(ctx, account, gwpoolTestIdentity, candidates)
		want := "good"
		if i%gatewayPoolContactExploreEvery == 0 {
			want = []string{"unknown-b", "bad", "unknown-a"}[i/gatewayPoolContactExploreEvery-1]
		}
		require.Equal(t, want, queue.pick(candidates, ranking), "pick %d", i)
	}
	require.Equal(t, "unknown-b", candidates[1].Name, "shared catalog must remain unchanged")
	// Reclassification and readiness are re-evaluated, not cached in the FIFO.
	ranking := svc.codexCookies.gatewayPoolRankCandidates(ctx, account, gwpoolTestIdentity, candidates)
	require.Equal(t, "unknown-b", queue.pick(candidates[:3], ranking), "stale tier must not restore an ineligible name")
	state := localRankHistory(time.Now(), "good", "bad", 5)
	for i := range state.Rounds {
		state.Rounds[i].Report.Outcome = "refreshed"
		state.Rounds[i].Report.WindowFinal = false
		state.Rounds[i].Report.FullWindowMS = 0
	}
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{
		openAIGatewayPoolContactsExtraKey: state,
	}))
	ranking = svc.codexCookies.gatewayPoolRankCandidates(ctx, account, gwpoolTestIdentity, candidates)
	require.NotContains(t, ranking.preferred, "good", "quality may be demoted on the next fresh decision")
	require.Equal(t, "bad", queue.pick(candidates, ranking), "demoted quality rejoins the ordinary FIFO")
	queue.reset()
	require.Equal(t, "unknown-a", queue.pick(candidates))
}

func TestGatewayPoolDualQueueNeutralScoreBoundary(t *testing.T) {
	for _, full := range []int{0, 49, 50, 51, 100} {
		candidates := []gwpool.Gateway{
			{Name: "unknown", PairReady: true},
			{Name: "measured", PairReady: true,
				Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: full, Samples: 100}},
		}
		projection := gatewayPoolRankProjection{model: gatewayPoolProbeModelLuna, source: "foreground", now: time.Now()}
		ranking := projection.rank(candidates)
		require.Equal(t, full > 50, ranking.preferred["measured"], "full=%d", full)
		require.False(t, ranking.preferred["unknown"])
		projection.model = "different-model"
		require.Empty(t, projection.rank(candidates).preferred)
	}
}

func TestGatewayPoolDualQueuePreviewMatchesSuccessiveDispatchDecisions(t *testing.T) {
	projection := gatewayPoolRankProjection{model: gatewayPoolProbeModelLuna, source: "foreground", now: time.Now()}
	candidates := []gwpool.Gateway{{Name: "fifo-a", PairReady: true}, {Name: "fifo-b", PairReady: true}}
	for i, name := range []string{"score-a", "score-b", "score-c"} {
		candidates = append(candidates, gwpool.Gateway{Name: name, PairReady: true,
			Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 90 - 10*i, Samples: 100}})
	}
	for _, picks := range []uint64{0, 3, 4, 9} {
		var forecast []gwpool.Gateway
		projection.visitPreview(candidates, picks, func(_ int, gateway gwpool.Gateway) bool {
			forecast = append(forecast, gateway)
			return true
		})
		remaining := append([]gwpool.Gateway(nil), candidates...)
		counter := picks
		queue := gatewayPoolCandidateQueue{}
		for i, expected := range forecast {
			ranking := projection.rank(remaining)
			if ranking.scored {
				counter++
				ranking.explore = counter%gatewayPoolContactExploreEvery == 0
			}
			actual := queue.pick(remaining, ranking)
			require.Equal(t, expected.Name, actual, "initial picks=%d position=%d", picks, i)
			for j, candidate := range remaining {
				if candidate.Name == actual {
					remaining = append(remaining[:j], remaining[j+1:]...)
					break
				}
			}
		}
		require.Len(t, forecast, len(candidates))
		if picks == 4 || picks == 9 {
			require.Equal(t, "fifo-a", forecast[0].Name, "the fifth decision explores before a fast candidate")
		}
	}
}

func TestGatewayPoolDualQueueRanksWithinTiersAndKeepsSoftUSDeferral(t *testing.T) {
	candidates := []gwpool.Gateway{
		{Name: "ordinary", DatacenterCountry: "JP"}, {Name: "good", DatacenterCountry: "JP"},
		{Name: "best", DatacenterCountry: "US"},
	}
	ranking := gatewayPoolCandidateRanking{
		preferred: map[string]bool{"good": true, "best": true},
		quality:   map[string]float64{"ordinary": 1, "good": 0.8, "best": 0.9},
		adaptive:  map[string]float64{"good": 150, "best": 100},
	}
	require.Equal(t, "good", ranking.order(candidates)[0].Name, "measured yield orders the quality tier")
	ranking.adaptive = nil
	require.Equal(t, "best", ranking.order(candidates)[0].Name)
	ranking.deferUS = true
	require.Equal(t, "good", ranking.order(candidates)[0].Name)
	ranking.explore = true
	require.Equal(t, "ordinary", ranking.order(candidates)[0].Name)
	require.Equal(t, "best", ranking.order(candidates[2:])[0].Name, "all-US is still usable")
}

func TestGatewayPoolDualQueueUnknownIsNotBlockedWhenEvidenceIsInsufficient(t *testing.T) {
	for _, mode := range []string{"few", "other-model", "other-member", "other-interval", "unknown", "global-only", "fresh-failure"} {
		t.Run(mode, func(t *testing.T) {
			account := gwpoolTestAccount(1)
			svc, repo := gatewayRuntimeService(account)
			state := localRankHistory(time.Now(), "good", "bad", 5)
			svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return nil, nil }
			switch mode {
			case "few":
				state = localRankHistory(time.Now(), "good", "bad", 4)
			case "other-model":
				for i := range state.Rounds {
					state.Rounds[i].Report.Model = "other"
				}
			case "other-member":
				state.LedgerTag = "different-member"
			case "other-interval":
				state.Seen["good"] = gatewayPoolContactSeen{LastAt: time.Now().Add(-3 * time.Hour)}
			case "unknown":
				for i := range state.Rounds {
					state.Rounds[i].Report.Outcome = "unknown"
				}
			case "global-only":
				state.Rounds = nil
			case "fresh-failure":
				svc.codexCookies.historyByTag = nil
			}
			require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{
				openAIGatewayPoolContactsExtraKey: state,
			}))
			candidates := []gwpool.Gateway{{Name: "unknown", PairReady: true}, {Name: "good", PairReady: true}}
			ranking := svc.codexCookies.gatewayPoolRankCandidates(context.WithValue(context.Background(),
				gatewayPoolProbeModelKey{}, gatewayPoolProbeModelLuna), account, gwpoolTestIdentity, candidates)
			var queue gatewayPoolCandidateQueue
			require.Equal(t, "unknown", queue.pick(candidates, ranking))
		})
	}
}
