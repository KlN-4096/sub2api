package service

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolHydrateReplacesOnlyNamedLedgersOnACopy(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolOutboxExtraKey] = map[string]any{"pending": []any{}}
	svc, repo := gatewayRuntimeService(account)
	row, err := repo.GetByID(context.Background(), 1)
	require.NoError(t, err)
	shared := row.Extra
	shared[gatewayPoolUsageExtraKey] = "stale copy left in extra"
	require.NoError(t, svc.hydrateGatewayPoolLedgers(context.Background(), []*Account{row},
		openAIGatewayPoolOutboxExtraKey, gatewayPoolUsageExtraKey))
	outbox, ok := row.Extra[openAIGatewayPoolOutboxExtraKey].(json.RawMessage)
	require.True(t, ok)
	require.JSONEq(t, `{"pending":[]}`, string(outbox))
	require.NotContains(t, row.Extra, gatewayPoolUsageExtraKey, "no stored ledger, no ledger")
	require.Equal(t, true, row.Extra[openAIGatewayPoolExtraKey])
	require.Equal(t, "stale copy left in extra", shared[gatewayPoolUsageExtraKey], "the row's original map is not written")
}

func TestGatewayPoolListDisplayMatchesFirstProgressPoll(t *testing.T) {
	ctx := context.Background()
	account := gwpoolTestAccount(1)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	account.Extra[openAIGatewayLedgerTagExtraKey] = tag
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{LedgerTag: tag, Current: "good", UpdatedAt: at,
		Seen: map[string]openAIGatewaySeen{"good": {At: at, Verdict: openAIGatewayVerdictFull, FullUseMs: 1000}}}
	account.Extra[openAIGatewayPoolContactsExtraKey] = localRankHistory(at, "good", "bad", 2)
	svc, repo := gatewayRuntimeService(account)
	admin := &adminServiceImpl{runtimeBlocker: svc} // the handler reaches the gateway through the admin service
	progress, err := admin.GatewayPoolRuntimeProgress(ctx, []int64{1}, false)
	require.NoError(t, err)
	require.Equal(t, "good", progress[1].Runtime.History.Current)
	require.Len(t, progress[1].Runtime.Contacts.Rounds, 4)
	listed, err := repo.GetByID(ctx, 1) // a listed row carries no ledgers
	require.NoError(t, err)
	admin.ResolveGatewayPoolDisplay(ctx, []*Account{listed})
	want, err := json.Marshal([]any{progress[1].Runtime.History, progress[1].Runtime.Contacts})
	require.NoError(t, err)
	got, err := json.Marshal([]any{listed.Extra[openAIGatewayHistoryExtraKey], listed.Extra[openAIGatewayPoolContactsExtraKey]})
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(got), "the page shows the same thing before and after its first poll")
}

func TestGatewayPoolListDisplayKeepsLandingOfRowsOffThePool(t *testing.T) {
	// Every Codex route records its landing gateway, pool or not; only pool rows
	// are polled, so a row off the pool shows only what the list gives it.
	ctx := context.Background()
	account := gwpoolTestAccount(1)
	account.Extra = map[string]any{openAIGatewayHistoryExtraKey: openAIGatewayHistory{Current: "unified-121"}}
	svc, repo := gatewayRuntimeService(account)
	listed, err := repo.GetByID(ctx, 1)
	require.NoError(t, err)
	svc.ResolveGatewayPoolDisplay(ctx, []*Account{listed})
	history, ok := listed.Extra[openAIGatewayHistoryExtraKey].(gatewayPoolHistoryDisplay)
	require.True(t, ok)
	require.Equal(t, "unified-121", history.Current)
	require.NotContains(t, listed.Extra, openAIGatewayPoolContactsExtraKey, "nothing stored, nothing shown")
}

// gatewayPoolLedgerReads records each full ledger read: the rows and ledgers.
type gatewayPoolLedgerReads struct {
	*gatewayPoolAccountsRepo
	ids  [][]int64
	keys [][]string
}

func (r *gatewayPoolLedgerReads) GetByIDs(ctx context.Context, ids []int64) ([]*Account, error) {
	var rows []*Account
	for _, id := range ids {
		row, err := r.GetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (r *gatewayPoolLedgerReads) LoadGatewayPoolLedgers(ctx context.Context, ids []int64, keys []string) (map[int64]map[string]json.RawMessage, error) {
	r.ids, r.keys = append(r.ids, slices.Clone(ids)), append(r.keys, slices.Clone(keys))
	return r.gatewayPoolAccountsRepo.LoadGatewayPoolLedgers(ctx, ids, keys)
}

// Two clone rows of one member, each with a history and contacts ledger.
func gatewayPoolLedgerReadsService(t *testing.T) (*OpenAIGatewayService, *gatewayPoolLedgerReads) {
	t.Helper()
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	rows := map[int64]*gatewayRuntimeRepo{}
	for id, current := range map[int64]string{1: "own", 2: "clone"} {
		account := gwpoolTestAccount(id)
		account.Extra[openAIGatewayLedgerTagExtraKey] = tag
		account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{LedgerTag: tag, Current: current,
			UpdatedAt: at.Add(time.Duration(id) * time.Minute), Seen: map[string]openAIGatewaySeen{current: {At: at}}}
		account.Extra[openAIGatewayPoolContactsExtraKey] = localRankHistory(at, current, "bad", 2)
		rows[id] = &gatewayRuntimeRepo{account: *account}
	}
	repo := &gatewayPoolLedgerReads{gatewayPoolAccountsRepo: &gatewayPoolAccountsRepo{rows: rows}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.codexCookies.accountByID = svc.gatewayPoolLedgerRow
	svc.codexCookies.historyByTag = svc.gatewayPoolHistoryPeers
	return svc, repo
}

func TestGatewayPoolPeerReadsLoadOnlyTheLedgersTheyUse(t *testing.T) {
	ctx := context.Background()
	svc, repo := gatewayPoolLedgerReadsService(t)
	account, err := repo.GetByID(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, svc.codexCookies.hydrateGatewayPoolSharedHistory(ctx, account, gwpoolTestIdentity))
	require.Len(t, repo.keys, 2, "the row, then its peers")
	for _, keys := range repo.keys {
		require.Equal(t, []string{openAIGatewayHistoryExtraKey}, keys, "the shared cooldown history reads no contacts")
	}
	repo.keys = nil
	svc.codexCookies.gatewayPoolRankCandidates(ctx, account, gwpoolTestIdentity, nil)
	require.Len(t, repo.keys, 2, "the row, then its peers")
	for _, keys := range repo.keys {
		require.Equal(t, []string{openAIGatewayPoolContactsExtraKey}, keys, "ranking reads no history")
	}
}

func TestGatewayPoolProgressReadsEachRequestedRowsLedgersOnce(t *testing.T) {
	ctx := context.Background()
	svc, repo := gatewayPoolLedgerReadsService(t)
	alone, err := svc.GatewayPoolRuntimeProgress(ctx, []int64{1}, false)
	require.NoError(t, err)
	require.Equal(t, [][]int64{{1}, {2}}, repo.ids, "the requested row, then only the peer it lacks")
	repo.ids = nil
	both, err := svc.GatewayPoolRuntimeProgress(ctx, []int64{1, 2}, false)
	require.NoError(t, err)
	require.Equal(t, [][]int64{{1, 2}}, repo.ids, "requested rows serve as each other's peers")
	for _, progress := range []map[int64]GatewayPoolProgress{alone, both} {
		history := progress[1].Runtime.History
		require.Equal(t, "clone", history.Current, "the clone's newer history still counts")
		require.Len(t, history.Seen, 2)
		gateways := map[string]bool{}
		for _, round := range progress[1].Runtime.Contacts.Rounds {
			gateways[round.Report.Gateway] = true
		}
		require.Equal(t, map[string]bool{"own": true, "clone": true, "bad": true}, gateways, "both rows' contacts merge")
	}
}
