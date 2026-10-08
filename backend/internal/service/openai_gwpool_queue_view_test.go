package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolQueueViewUsesScopedCacheWithoutScheduling(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	fake.listGateways = []gwpoolFakeGateway{
		{Name: "ordinary", PairReady: true, ValidForS: 150}, {Name: "good", PairReady: true, ValidForS: 150},
		{Name: "cooling", PairReady: true, ValidForS: 150}, {Name: "unready"},
	}
	account := fake.account(1)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	account.Extra[openAIGatewayPoolContactsExtraKey] = localRankHistory(time.Now(), "good", "ordinary", 5)
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Seen: map[string]openAIGatewaySeen{"cooling": {At: time.Now()}},
	}
	svc, _ := gatewayRuntimeService(account)
	read := func() map[string]any {
		result, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
		require.NoError(t, err)
		raw, err := json.Marshal(result[account.ID].Runtime)
		require.NoError(t, err)
		var view map[string]any
		require.NoError(t, json.Unmarshal(raw, &view))
		return view
	}
	require.Contains(t, read(), "queues", "unknown queues must have an explicit null, not an invented zero")
	require.Nil(t, read()["queues"])
	require.Zero(t, fake.listHits.Load())
	pool, err := svc.codexCookies.poolClient(account)
	require.NoError(t, err)
	_, err = pool.Catalog(context.Background(), gatewayPoolUpstreamAccountID(gwpoolTestIdentity),
		gatewayPoolAccountTag(account, gwpoolTestIdentity), gatewayPoolProbeModelLuna, 0)
	require.NoError(t, err)
	counter := &atomic.Uint64{}
	counter.Store(4) // UI reads must not consume the fifth/exploration decision.
	svc.codexCookies.poolContactPicks.Store(tag, counter)
	queue := svc.codexCookies.gatewayPoolCandidateQueue(gwpoolTestIdentity)
	queue.pick([]gwpool.Gateway{{Name: "good"}, {Name: "ordinary"}})
	before := append([]string(nil), queue.names...)
	for range 3 {
		view := read()
		queues, ok := view["queues"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, gatewayPoolProbeModelLuna, queues["model"])
		quality, ok := queues["quality"].(map[string]any)
		require.True(t, ok)
		ordinary, ok := queues["ordinary"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, float64(1), quality["count"])
		require.Equal(t, []any{"good"}, quality["gateways"])
		require.Equal(t, float64(1), ordinary["count"])
		require.Equal(t, []any{"ordinary"}, ordinary["gateways"])
	}
	require.Equal(t, int64(1), fake.listHits.Load())
	require.Zero(t, fake.hits.Load())
	require.Equal(t, uint64(4), counter.Load())
	require.Equal(t, before, queue.names)
	require.Empty(t, svc.codexCookies.poolCooldown, "display must not hydrate shared cooldowns")
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "different-key"
	require.Nil(t, read()["queues"], "cached metadata from a different Key must not leak into the display")
	require.Equal(t, int64(1), fake.listHits.Load())
}

func TestGatewayPoolQueueViewBoundsSamplesAndKeepsReadonlyFIFO(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	for i := range 8 {
		fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{Name: fmt.Sprintf("g%d", i), PairReady: true, ValidForS: 150})
	}
	account := fake.account(1)
	svc, _ := gatewayRuntimeService(account)
	store := &svc.codexCookies
	pool, err := store.poolClient(account)
	require.NoError(t, err)
	_, err = pool.Catalog(context.Background(), gatewayPoolUpstreamAccountID(gwpoolTestIdentity),
		gatewayPoolAccountTag(account, gwpoolTestIdentity), gatewayPoolProbeModelLuna, 0)
	require.NoError(t, err)
	now := time.Now()
	history := openAIGatewayHistory{Seen: map[string]openAIGatewaySeen{"g1": {At: now}}}
	queue := store.gatewayPoolCandidateQueue(gwpoolTestIdentity)
	queue.names = []string{"g4", "gone", "g5"}
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		gateway: "g0", cookie: "offline", version: "ticket", until: now.Add(time.Minute),
	})
	view := store.gatewayPoolQueueView(account, gwpoolTestIdentity, history, gatewayPoolContacts{}, now)
	require.NotNil(t, view)
	require.Zero(t, view.Quality.Count)
	require.Empty(t, view.Quality.Gateways)
	require.Equal(t, 6, view.Ordinary.Count, "current ticket and local cooldown are not candidates")
	require.Equal(t, []string{"g4", "g5", "g2"}, view.Ordinary.Gateways)
	require.Equal(t, []string{"g4", "gone", "g5"}, queue.names, "polling does not reconcile or rotate the actual FIFO")
	view.Ordinary.Gateways[0] = "mutated-output"
	require.Equal(t, "g4", store.gatewayPoolQueueView(account, gwpoolTestIdentity, history, gatewayPoolContacts{}, now).Ordinary.Gateways[0])
	require.Nil(t, store.gatewayPoolQueueView(account, gwpoolTestIdentity, history, gatewayPoolContacts{}, view.ValidUntil))
	// Clear only the projection; reading it must not publish a clear/reset event.
	history.CooldownReset.ClearedAt = now.Add(time.Second)
	history.CooldownReset.LastAt = now.Add(time.Second)
	require.Equal(t, 7, store.gatewayPoolQueueView(account, gwpoolTestIdentity, history, gatewayPoolContacts{}, now.Add(2*time.Second)).Ordinary.Count)
	require.True(t, store.gatewayPoolCooldownClearAt(gwpoolTestIdentity).IsZero())
	require.Zero(t, fake.hits.Load())
	require.Equal(t, int64(1), fake.listHits.Load())
}

func TestGatewayPoolQueueViewDoesNotBorrowOtherModelOrMemberEvidence(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "good", PairReady: true, ValidForS: 150}}
	account := fake.account(1)
	svc, _ := gatewayRuntimeService(account)
	pool, err := svc.codexCookies.poolClient(account)
	require.NoError(t, err)
	_, err = pool.Catalog(context.Background(), gatewayPoolUpstreamAccountID(gwpoolTestIdentity),
		gatewayPoolAccountTag(account, gwpoolTestIdentity), gatewayPoolProbeModelLuna, 0)
	require.NoError(t, err)
	now := time.Now()
	state := localRankHistory(now, "good", "bad", 5)
	view := svc.codexCookies.gatewayPoolQueueView(account, gwpoolTestIdentity, openAIGatewayHistory{}, state, now)
	require.Equal(t, 1, view.Quality.Count, "a single remaining quality candidate keeps its classification")
	for i := range state.Rounds {
		state.Rounds[i].Report.Model = "other-model"
	}
	view = svc.codexCookies.gatewayPoolQueueView(account, gwpoolTestIdentity, openAIGatewayHistory{}, state, now)
	require.Zero(t, view.Quality.Count)
	require.Equal(t, 1, view.Ordinary.Count)
	require.Nil(t, svc.codexCookies.gatewayPoolQueueView(account, "gwpool-member:acc-a/other", openAIGatewayHistory{}, state, now))
	require.Equal(t, int64(1), fake.listHits.Load())
}
