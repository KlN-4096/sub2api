package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func localRankHistory(now time.Time, good, bad string, count int) gatewayPoolContacts {
	state := gatewayPoolContacts{
		LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity),
		Seen:      map[string]gatewayPoolContactSeen{},
	}
	for _, name := range []string{good, bad} {
		last := now.Add(-90 * time.Minute)
		state.Seen[name] = gatewayPoolContactSeen{LastAt: last}
		for i := 0; i < count; i++ {
			id := gatewayPoolContactHash(fmt.Sprintf("%s-%d", name, i))
			report := gwpool.ContactReport{
				ID: id, Gateway: name, Model: gatewayPoolProbeModelLuna,
				Criterion: gwpool.ContactCriterion, Source: "foreground", First: "repeat",
				At:       last.Add(-time.Duration(count-i-1) * 90 * time.Minute),
				GapKnown: true, ElapsedSeconds: 90 * 60, Outcome: "refreshed",
			}
			if name == good {
				report.Outcome, report.WindowFinal, report.FullWindowMS = "full", true, 120_000
			}
			state.Rounds = append(state.Rounds, gatewayPoolContactRound{RoundID: id, Report: report, LastAt: report.At})
		}
	}
	return state
}

func TestGatewayPoolLocalEvidenceOverridesPoolAndExistingQueue(t *testing.T) {
	for _, source := range []string{"priority", "contacts"} {
		t.Run(source, func(t *testing.T) {
			fake := newGwpoolFakePool(t, "", 150)
			for _, name := range []string{"bad", "good"} {
				full := 0
				if name == "bad" {
					full = 10
				}
				candidate := gwpoolFakeGateway{Name: name, PairReady: true}
				if source == "priority" {
					candidate.Priority = &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: full, Samples: 10}
				} else {
					candidate.Contacts = []gwpool.ContactStats{{
						Gateway: name, Model: gatewayPoolProbeModelLuna, Source: "foreground",
						Criterion: gwpool.ContactCriterion, First: "repeat", Interval: "1-2h",
						Full: full, Refreshed: 10 - full,
					}}
				}
				fake.listGateways = append(fake.listGateways, candidate)
			}
			account := fake.account(1)
			svc, repo := gatewayRuntimeService(account)
			svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return nil, nil }
			require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{
				openAIGatewayPoolContactsExtraKey: localRankHistory(time.Now(), "good", "bad", 5),
			}))
			queue := svc.codexCookies.gatewayPoolCandidateQueue(gwpoolTestIdentity)
			// Existing FIFO has bad at its head before new local evidence is read.
			require.Equal(t, "good", queue.pick([]gwpool.Gateway{{Name: "good"}, {Name: "bad"}}))
			pool, err := svc.codexCookies.poolClient(account)
			require.NoError(t, err)
			ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, gatewayPoolProbeModelLuna)
			for i := 1; i <= gatewayPoolContactExploreEvery; i++ {
				want := "good"
				if i == gatewayPoolContactExploreEvery {
					want = "bad"
				}
				require.Equal(t, want, svc.codexCookies.gatewayPoolPick(ctx, pool, account, gwpoolTestIdentity))
			}
			require.Zero(t, fake.hits.Load(), "ranking/exploration must never acquire or probe a ticket")
			require.False(t, svc.codexCookies.gatewayPoolUsedRecently(gwpoolTestIdentity, "good", time.Hour))
		})
	}
}

func TestGatewayPoolLocalEvidenceMustBeFreshScopedAndSufficient(t *testing.T) {
	for _, mode := range []string{"local", "truncated-known-reports", "few", "wrong-tag", "old", "wrong-model", "unknown",
		"duplicate", "fresh-failure", "peer-failure", "changed-identity"} {
		t.Run(mode, func(t *testing.T) {
			account := gwpoolTestAccount(1)
			svc, repo := gatewayRuntimeService(account)
			state := localRankHistory(time.Now(), "good", "bad", 5)
			switch mode {
			case "truncated-known-reports":
				state.HistoryTruncated = true
			case "few":
				state = localRankHistory(time.Now(), "good", "bad", 4)
			case "wrong-tag":
				state.LedgerTag = "old-workspace-ledger"
			case "old":
				for i := range state.Rounds {
					state.Rounds[i].Report.At = time.Now().Add(-8 * 24 * time.Hour)
				}
			case "wrong-model":
				for i := range state.Rounds {
					state.Rounds[i].Report.Model = "other"
				}
			case "unknown":
				for i := range state.Rounds {
					state.Rounds[i].Report.Outcome = "unknown"
				}
			case "duplicate":
				state = localRankHistory(time.Now(), "good", "bad", 1)
				state.Rounds = append(state.Rounds, state.Rounds...)
				state.Rounds = append(state.Rounds, state.Rounds...)
				state.Rounds = append(state.Rounds, state.Rounds...)
			}
			require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{openAIGatewayPoolContactsExtraKey: state}))
			svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return nil, nil }
			if mode == "fresh-failure" {
				svc.codexCookies.accountByID = func(context.Context, int64) (*Account, error) { return nil, errors.New("offline read failure") }
			}
			if mode == "peer-failure" {
				svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return nil, errors.New("offline peer failure") }
			}
			if mode == "changed-identity" {
				svc.codexCookies.identity = func(context.Context, *Account) (string, error) { return "different-member", nil }
			}
			candidates := []gwpool.Gateway{
				{Name: "bad", PairReady: true, Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 10, Samples: 10}},
				{Name: "good", PairReady: true, Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 0, Samples: 10}},
			}
			ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, gatewayPoolProbeModelLuna)
			got := svc.codexCookies.gatewayPoolRankContacts(ctx, account, gwpoolTestIdentity, candidates)
			want := "bad"
			if mode == "local" || mode == "truncated-known-reports" {
				want = "good"
			}
			require.Equal(t, want, got[0].Name)
		})
	}
}

func TestGatewayPoolLocalStatsCountDistinctInitialReportsAndClosedFullWindows(t *testing.T) {
	now := time.Now()
	state := localRankHistory(now, "good", "bad", 5)
	state.Rounds[0].Report.WindowFinal = false
	state.Rounds[1].Report.WindowFinal = false
	// Later probe state is not another initial verdict.
	state.Rounds[0].LastProbeOutcome = "refreshed"
	copyWithFinalWindow := state.Rounds[0]
	copyWithFinalWindow.Report.WindowFinal = true
	state.Rounds = append(state.Rounds, copyWithFinalWindow)
	state.Rounds = append(state.Rounds, state.Rounds...)
	unknown := state.Rounds[0]
	unknown.Report.ID = gatewayPoolContactHash("unknown")
	unknown.Report.Outcome = "unknown"
	state.Rounds = append(state.Rounds, unknown)
	rows := gatewayPoolLocalContactStats(state, gatewayPoolProbeModelLuna, "foreground", now)
	require.Len(t, rows["good"], 1)
	require.Equal(t, 5, rows["good"][0].Full)
	require.Zero(t, rows["good"][0].Refreshed)
	require.Equal(t, 1, rows["good"][0].Unknown)
	require.Equal(t, 4, rows["good"][0].WindowSamples)
	require.EqualValues(t, 120_000, rows["good"][0].WindowMeanMS)
	require.Equal(t, 5, rows["bad"][0].Refreshed)
	require.Zero(t, rows["bad"][0].WindowSamples)
	conflict := state.Rounds[0]
	conflict.Report.Outcome = "refreshed"
	state.Rounds = append(state.Rounds, conflict)
	rows = gatewayPoolLocalContactStats(state, gatewayPoolProbeModelLuna, "foreground", now)
	require.NotContains(t, rows, "good", "conflicting report cannot count toward the five-result minimum")
}

func TestGatewayPoolLocalWindowsReplacePoolAdaptiveWindows(t *testing.T) {
	now := time.Now()
	state := localRankHistory(now, "good", "bad", 5)
	for i := range state.Rounds {
		if state.Rounds[i].Report.Gateway == "bad" {
			state.Rounds[i].Report.Outcome = "full"
			state.Rounds[i].Report.WindowFinal = true
			state.Rounds[i].Report.FullWindowMS = 30_000
		}
	}
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	// Peer copies contain the same reports, not extra samples.
	peer := *account
	peer.ID = 2
	peer.Extra = map[string]any{openAIGatewayPoolContactsExtraKey: state}
	svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return []Account{peer}, nil }
	require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{openAIGatewayPoolContactsExtraKey: state}))
	var candidates []gwpool.Gateway
	for _, name := range []string{"bad", "good"} {
		window := int64(10_000)
		if name == "bad" {
			window = (3 * time.Minute).Milliseconds()
		}
		candidates = append(candidates, gwpool.Gateway{Name: name, PairReady: true, Contacts: []gwpool.ContactStats{{
			Gateway: name, Model: gatewayPoolProbeModelLuna, Criterion: gwpool.ContactCriterion, Source: "foreground",
			First: "repeat", Interval: "1-2h", Full: 5, WindowSamples: 5, WindowMeanMS: window,
		}}})
	}
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, gatewayPoolProbeModelLuna)
	ranking := svc.codexCookies.gatewayPoolRankCandidates(ctx, account, gwpoolTestIdentity, candidates)
	require.Len(t, ranking.adaptive, 2)
	require.Equal(t, "good", ranking.order(candidates)[0].Name)
	require.Equal(t, int64((3 * time.Minute).Milliseconds()), candidates[0].Contacts[0].WindowMeanMS, "pool input remains immutable")
}

func TestGatewayPoolUSDeferralAppliesToAlreadyQueuedNames(t *testing.T) {
	candidates := []gwpool.Gateway{{Name: "us", DatacenterCountry: "US"}, {Name: "jp", DatacenterCountry: "JP"}}
	var queue gatewayPoolCandidateQueue
	require.Equal(t, "jp", queue.pick([]gwpool.Gateway{candidates[1], candidates[0]}))
	policy := gatewayPoolCandidateRanking{
		quality: map[string]float64{"us": 1, "jp": 0}, adaptive: map[string]float64{"us": 180, "jp": 1}, deferUS: true,
	}
	require.Equal(t, "jp", queue.pick(candidates, policy))
	require.Equal(t, "us", queue.pick(candidates[:1], policy), "US is a fallback, not an exclusion")
}
