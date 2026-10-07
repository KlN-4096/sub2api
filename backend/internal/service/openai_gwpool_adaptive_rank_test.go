package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func adaptiveCandidate(name string, full, total int, window time.Duration) gwpool.Gateway {
	return gwpool.Gateway{Name: name, PairReady: true, Contacts: []gwpool.ContactStats{{
		Gateway: name, Model: "luna", Criterion: gwpool.ContactCriterion,
		Source: "foreground", First: "repeat", Interval: "2-4h",
		Full: full, Refreshed: total - full, WindowSamples: 3, WindowMeanMS: window.Milliseconds(),
	}}}
}

func adaptiveHistory(now time.Time, names ...string) gatewayPoolContacts {
	state := gatewayPoolContacts{LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity),
		Seen: map[string]gatewayPoolContactSeen{}}
	for _, name := range names {
		state.Seen[name] = gatewayPoolContactSeen{LastAt: now.Add(-150 * time.Minute)}
	}
	return state
}

func TestGatewayPoolAdaptiveScoresUsePersonalYieldAndBoundWindows(t *testing.T) {
	now := time.Now()
	input := []gwpool.Gateway{adaptiveCandidate("short", 9, 10, 30*time.Second),
		{Name: "unknown", PairReady: true}, adaptiveCandidate("long", 9, 10, 2*time.Minute)}
	state := adaptiveHistory(now, "short", "long")
	scores := gatewayPoolAdaptiveScores(input, state, "luna", "foreground", now, nil)
	out := rankGatewayPoolAdaptive(input, scores)
	require.Equal(t, []string{"long", "unknown", "short"}, []string{out[0].Name, out[1].Name, out[2].Name})
	require.Equal(t, "short", input[0].Name)
	require.NotContains(t, scores, "unknown")
	input[0].Contacts[0].WindowMeanMS = (10 * time.Hour).Milliseconds()
	input[2].Contacts[0].WindowMeanMS = gatewayPoolAdaptiveWindowLimit.Milliseconds()
	scores = gatewayPoolAdaptiveScores(input, state, "luna", "foreground", now, nil)
	require.Equal(t, scores["short"], scores["long"], "idle-inflated windows cannot dominate")
}

func TestGatewayPoolAdaptiveScoresRequireComparableCompleteEvidence(t *testing.T) {
	for _, mode := range []string{"model", "source", "interval", "criterion", "first",
		"few-results", "few-windows", "invalid", "duplicate-stratum", "duplicate-gateway",
		"missing-last", "future-last", "old-last", "not-ready", "truncated", "US"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now()
			input := []gwpool.Gateway{adaptiveCandidate("a", 9, 10, time.Minute),
				adaptiveCandidate("b", 9, 10, 2*time.Minute)}
			state := adaptiveHistory(now, "a", "b")
			stats := &input[1].Contacts[0]
			switch mode {
			case "model":
				stats.Model = "other"
			case "source":
				stats.Source = "background"
			case "interval":
				stats.Interval = "1-2h"
			case "criterion":
				stats.Criterion = "other"
			case "first":
				stats.First = "tracked_first"
			case "few-results":
				stats.Full, stats.Refreshed = 3, 1
			case "few-windows":
				stats.WindowSamples = 2
			case "invalid":
				stats.WindowSamples = 100
			case "duplicate-stratum":
				input[1].Contacts = append(input[1].Contacts, *stats)
			case "duplicate-gateway":
				input = append(input, input[0])
			case "missing-last":
				delete(state.Seen, "b")
			case "future-last":
				state.Seen["b"] = gatewayPoolContactSeen{LastAt: now.Add(time.Second)}
			case "old-last":
				state.Seen["b"] = gatewayPoolContactSeen{LastAt: now.Add(-31 * 24 * time.Hour)}
			case "not-ready":
				input[1].PairReady = false
			case "truncated":
				state.HistoryTruncated = true
			case "US":
				state.LastUSAt, input[1].DatacenterCountry = now, "US"
			}
			require.Nil(t, gatewayPoolAdaptiveScores(input, state, "luna", "foreground", now, nil))
		})
	}
}

func TestGatewayPoolAdaptiveReuseNeedsCoverageAndFuturePersonalStratum(t *testing.T) {
	now := time.Now()
	var input []gwpool.Gateway
	var names []string
	for i := range 30 {
		name := fmt.Sprintf("g%d", i)
		input = append(input, adaptiveCandidate(name, 9, 10, 3*time.Minute))
		names = append(names, name)
	}
	state := adaptiveHistory(now, names...)
	input[0].Contacts[0].WindowMeanMS = (150 * time.Second).Milliseconds()
	retryAfter := func(name string) time.Duration {
		if name == "g0" {
			return time.Hour
		}
		return 6 * time.Hour
	}
	without := gatewayPoolAdaptiveScores(input, state, "luna", "foreground", now, retryAfter)
	require.Less(t, without["g0"], without["g1"], "a short configured CD is not recovery evidence")
	future := input[0].Contacts[0]
	future.Interval = "1-2h"
	input[0].Contacts = append(input[0].Contacts, future)
	with := gatewayPoolAdaptiveScores(input, state, "luna", "foreground", now, retryAfter)
	require.Greater(t, with["g0"], with["g1"])
	require.LessOrEqual(t, with["g0"], 2*without["g0"], "value at most one reuse")
	input[0].Contacts[1].WindowSamples = 0
	noFutureDuration := gatewayPoolAdaptiveScores(input, state, "luna", "foreground", now, retryAfter)
	require.Equal(t, without["g0"], noFutureDuration["g0"])
	input[0].Contacts[1].WindowSamples = 3
	short := gatewayPoolAdaptiveScores(input[:2], state, "luna", "foreground", now, retryAfter)
	require.Less(t, short["g0"], short["g1"], "do not invent bridging time from unselected inventory")
}

func TestGatewayPoolAdaptiveQueueReturnAndExploration(t *testing.T) {
	input := []gwpool.Gateway{{Name: "a", PairReady: true}, {Name: "b", PairReady: true},
		{Name: "c", PairReady: true}}
	var queue gatewayPoolCandidateQueue
	require.Equal(t, "a", queue.pick(input))
	require.Equal(t, "b", queue.pick(input[1:]))
	scores := map[string]float64{"a": 100, "b": 10, "c": 20, "cooling": 9999}
	require.Equal(t, "a", queue.pick(input, scores), "returned a can be selected ahead of the tail")
	require.Equal(t, "c", queue.pick(input), "fallback retained FIFO, not score ordering")
	require.Equal(t, "b", queue.pick(input), "lower-scoring b retains its exploration turn")
	scores = map[string]float64{"b": 10, "c": 20}
	require.Equal(t, "a", queue.pick(input, scores), "unmeasured head is not starved")
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() { defer wg.Done(); queue.pick(input, scores) }()
	}
	wg.Wait()
	queue.mu.Lock()
	require.ElementsMatch(t, []string{"a", "b", "c"}, queue.names)
	queue.mu.Unlock()
}

func TestGatewayPoolAdaptiveFreshIdentityPeersAndFifthFallback(t *testing.T) {
	for _, mode := range []string{"ok", "fresh-error", "peer-error", "identity-change",
		"identity-error", "resolved-identity-change", "snapshot-only", "no-peer-reader", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			account := gwpoolTestAccount(1)
			svc, repo := gatewayRuntimeService(account)
			svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return nil, nil }
			state := adaptiveHistory(time.Now(), "a", "b")
			if mode == "identity-change" {
				state.LedgerTag = "another-identity"
			}
			require.NoError(t, repo.UpdateExtra(context.Background(), 1,
				map[string]any{openAIGatewayPoolContactsExtraKey: state}))
			if mode == "fresh-error" {
				svc.codexCookies.accountByID = func(context.Context, int64) (*Account, error) {
					return nil, errors.New("offline read failed")
				}
			}
			if mode == "peer-error" {
				svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) {
					return nil, errors.New("offline peer read failed")
				}
			}
			if mode == "snapshot-only" {
				svc.codexCookies.accountByID = nil
			}
			if mode == "no-peer-reader" {
				svc.codexCookies.historyByTag = nil
			}
			if mode == "identity-error" || mode == "resolved-identity-change" {
				svc.codexCookies.identity = func(context.Context, *Account) (string, error) {
					if mode == "identity-error" {
						return "", errors.New("offline identity read failed")
					}
					return "another-identity", nil
				}
			}
			ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "luna")
			if mode == "cancelled" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			candidates := []gwpool.Gateway{adaptiveCandidate("a", 9, 10, time.Minute),
				adaptiveCandidate("b", 9, 10, 2*time.Minute)}
			for i := 1; i <= gatewayPoolContactExploreEvery; i++ {
				rank := svc.codexCookies.gatewayPoolRankCandidates(ctx, account, gwpoolTestIdentity, candidates)
				if mode == "ok" && i != gatewayPoolContactExploreEvery {
					require.Len(t, rank.adaptive, 2)
					require.Equal(t, "b", rankGatewayPoolAdaptive(rank.candidates, rank.adaptive)[0].Name)
					require.Equal(t, "a", rank.candidates[0].Name, "preserve queue admission baseline")
				} else {
					require.Nil(t, rank.adaptive)
					require.Equal(t, "a", rank.candidates[0].Name)
				}
			}
		})
	}
}

func TestGatewayPoolAdaptivePickUsesRankingWithoutBypassingCooldown(t *testing.T) {
	candidates := []gwpool.Gateway{adaptiveCandidate("a", 9, 10, time.Minute),
		adaptiveCandidate("b", 9, 10, 2*time.Minute), adaptiveCandidate("c", 9, 10, 3*time.Minute),
		adaptiveCandidate("cooling", 10, 10, 3*time.Minute)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/gateways" || r.Header.Get("X-Gwpool-Account-Tag") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var rows []map[string]any
		for _, c := range candidates {
			rows = append(rows, map[string]any{"name": c.Name, "pair_ready": true, "contacts": c.Contacts})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"gateways": rows})
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "offline"
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) { return nil, nil }
	require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{
		openAIGatewayPoolContactsExtraKey: adaptiveHistory(time.Now(), "a", "b", "c", "cooling"),
	}))
	svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "cooling")
	before, burned := svc.codexCookies.gatewayPoolUsedAt(gwpoolTestIdentity, "cooling", time.Hour)
	require.True(t, burned)
	pool, err := svc.codexCookies.poolClient(account)
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "luna")
	queue := svc.codexCookies.gatewayPoolCandidateQueue(gwpoolTestIdentity)
	for i := 1; i <= gatewayPoolContactExploreEvery; i++ {
		want := "c"
		if i == gatewayPoolContactExploreEvery {
			want = "a"
		}
		require.Equal(t, want, svc.codexCookies.gatewayPoolPick(ctx, pool, account, gwpoolTestIdentity))
		if i == 1 {
			require.Equal(t, []string{"a", "b", "c"}, queue.names,
				"first admission must not persist the adaptive ordering")
		}
	}
	require.Equal(t, "a", queue.pick(candidates[:1]))
	require.Equal(t, "c", svc.codexCookies.gatewayPoolPick(ctx, pool, account, gwpoolTestIdentity))
	require.Equal(t, []string{"a", "b", "c"}, queue.names,
		"multiple returning candidates must also retain fallback admission order")
	after, burned := svc.codexCookies.gatewayPoolUsedAt(gwpoolTestIdentity, "cooling", time.Hour)
	require.True(t, burned)
	require.Equal(t, before, after, "ranking must not touch or shorten the cooldown")
}
