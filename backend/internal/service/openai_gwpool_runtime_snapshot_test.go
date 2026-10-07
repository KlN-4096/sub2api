package service

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

type gatewayPoolSnapshotCounter struct {
	ConcurrencyCache
	counts map[int64]int
	err    error
}

func (c *gatewayPoolSnapshotCounter) GetAccountConcurrencyBatch(context.Context, []int64) (map[int64]int, error) {
	return c.counts, c.err
}

func TestGatewayPoolRuntimeSnapshotIncludesFreshHistoryAndKnownCapacity(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	account := fake.account(1)
	account.Concurrency = 100
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Current: "new-gateway", UpdatedAt: time.Now(),
		Seen: map[string]openAIGatewaySeen{"new-gateway": {At: time.Now(), Region: "east-asia"}},
	}
	svc, _ := gatewayRuntimeService(account)
	counter := &gatewayPoolSnapshotCounter{counts: map[int64]int{1: 7}}
	svc.concurrencyService = NewConcurrencyService(counter)
	read := func() map[string]any {
		snapshot, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
		require.NoError(t, err)
		raw, err := json.Marshal(snapshot[1].Runtime)
		require.NoError(t, err)
		var view map[string]any
		require.NoError(t, json.Unmarshal(raw, &view))
		return view
	}
	view := read()
	require.Equal(t, float64(7), view["current_concurrency"])
	require.Equal(t, float64(100), view["concurrency_limit"])
	history, ok := view["history"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "new-gateway", history["current"])
	counter.err = errors.New("counter unavailable")
	view = read()
	require.Contains(t, view, "current_concurrency")
	require.Nil(t, view["current_concurrency"], "read failure must remain unknown, not look like 0/100")
	counter.err = nil
	counter.counts[1] = 0
	require.Equal(t, float64(0), read()["current_concurrency"], "measured zero remains a real zero")
	require.Zero(t, fake.hits.Load())
	require.Zero(t, fake.listHits.Load(), "the 1s UI snapshot is local-only")
}

func TestGatewayPoolRuntimeSnapshotMergesCloneHistoryWithoutWriting(t *testing.T) {
	account := gwpoolTestAccount(1)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	now := time.Now().UTC()
	newer := gatewayPoolCooldown{ResetAt: now, Until: now.Add(time.Hour), WindowSeconds: 3600}
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Current: "a", UpdatedAt: now.Add(-time.Minute),
		Seen: map[string]openAIGatewaySeen{"a": {At: now.Add(-time.Minute), Cooldown: &newer}},
	}
	clone := *gwpoolTestAccount(2)
	clone.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Current: "b", UpdatedAt: now,
		Seen: map[string]openAIGatewaySeen{"a": {At: now}, "b": {At: now}},
	}
	svc := &OpenAIGatewayService{}
	history, _ := svc.gatewayPoolDisplaySnapshot(account, gwpoolTestIdentity, []Account{clone})
	require.Equal(t, "b", history.Current)
	require.Len(t, history.Seen, 2)
	require.Equal(t, newer.ResetAt, history.Seen["a"].Cooldown.ResetAt,
		"a newer contact timestamp may not erase a newer cooldown generation")
	original, _ := readOpenAIGatewayHistory(account)
	require.Equal(t, "a", original.Current)
	require.Len(t, original.Seen, 1)
}

func TestGatewayPoolDisplayCachePreservesRowFreshnessAndIndependentResults(t *testing.T) {
	now := time.Now().UTC()
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	account := gwpoolTestAccount(1)
	older := *gwpoolTestAccount(1) // same ID, a different database observation
	clone := *gwpoolTestAccount(2)
	for _, row := range []*Account{account, &older, &clone} {
		row.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
			LedgerTag: tag, Current: "a", UpdatedAt: now,
			Seen: map[string]openAIGatewaySeen{"a": {At: now}},
		}
		row.Extra[openAIGatewayPoolContactsExtraKey] = gatewayPoolContacts{
			LedgerTag: tag, Seen: map[string]gatewayPoolContactSeen{"a": {LastAt: now}},
			Rounds: []gatewayPoolContactRound{{
				RoundID: gatewayPoolContactHash("round"), Aliases: []string{gatewayPoolContactHash(strconv.FormatInt(row.ID, 10))},
				Report: gwpool.ContactReport{ID: gatewayPoolContactHash("report"), At: now},
			}},
		}
	}
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Current: "fresh", UpdatedAt: now.Add(time.Second),
		Seen: map[string]openAIGatewaySeen{"fresh": {At: now}},
	}
	peers := []Account{older, clone}
	before, err := json.Marshal(peers)
	require.NoError(t, err)
	svc := &OpenAIGatewayService{}
	cache := gatewayPoolDisplayCache{}
	for _, row := range []*Account{account, &peers[1], account} {
		wantHistory, wantContacts := svc.gatewayPoolDisplaySnapshot(row, gwpoolTestIdentity, peers)
		gotHistory, gotContacts := svc.gatewayPoolDisplaySnapshot(row, gwpoolTestIdentity, peers, cache)
		require.Equal(t, wantHistory, gotHistory)
		require.Equal(t, wantContacts, gotContacts)
		delete(gotHistory.Seen, "a")
		delete(gotContacts.Seen, "a")
		gotContacts.Rounds[0].Aliases[0] = "modified-output-must-not-reach-cache"
	}
	require.Len(t, cache, 3, "each actual snapshot is parsed once, not once per clone output")
	after, err := json.Marshal(peers)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestGatewayPoolRuntimeSnapshotClearsReadyWhenVerifiedTicketDies(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	now := time.Now()
	run := svc.codexCookies.poolProgress.start(account.ID, 0, gatewayPoolProgressScope{
		tag: gatewayPoolLedgerTag(identity), identity: identity, requestStarted: now,
	})
	svc.codexCookies.poolProgress.update(run, "ready", 1, "offline", false, true)
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{
		gateway: "offline", cookie: "fixture", version: "v", until: now.Add(time.Minute),
	})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "gpt-6-luna")
	before, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	require.Equal(t, "ready", before[account.ID].Phase)
	svc.codexCookies.gatewayPoolMarkStale(identity, "v", "offline")
	after, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	require.Equal(t, "pending", after[account.ID].Phase)
	require.Empty(t, after[account.ID].Runtime.Tickets)
	require.Equal(t, before[account.ID].Attempt, after[account.ID].Attempt)
}
