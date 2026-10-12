package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type gatewayPoolOrderedStateRepo struct {
	*gatewayRuntimeRepo
	peers []Account
}

func (r *gatewayPoolOrderedStateRepo) FindGatewayPoolStatePeers(context.Context, string, string, int64) ([]Account, error) {
	return r.peers, nil
}

func TestGatewayPoolStatePeersIgnoreLegacyNestedPrevious(t *testing.T) {
	account := gwpoolTestAccount(1)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	at := time.Now().UTC()
	current := Account{ID: 2, Extra: map[string]any{
		gatewayPoolRestStateKey:  gatewayPoolRestState{Tag: tag, ChangedAt: at, Active: false},
		gatewayPoolUsageExtraKey: gatewayPoolUsageLedger{Tag: tag, UpdatedAt: at, VerificationSequence: 9},
	}}
	later := at.Add(time.Minute)
	// Rows written before migration 251 nested an old identity's ledger under "previous".
	legacy := Account{ID: 3, Extra: map[string]any{
		gatewayPoolRestStateKey: map[string]any{"tag": "other",
			"previous": map[string]any{"tag": tag, "changed_at": later, "active": true}},
		gatewayPoolUsageExtraKey: map[string]any{"tag": "other",
			"previous": map[string]any{"tag": tag, "updated_at": later, "verification_sequence": 99}},
	}}
	repo := &gatewayPoolOrderedStateRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}, peers: []Account{current, legacy}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	rest, _, err := svc.loadGatewayPoolRest(context.Background(), account, gwpoolTestIdentity)
	require.NoError(t, err)
	require.False(t, rest.Active, "a nested legacy archive is not this ledger's state")
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), account, gwpoolTestIdentity,
		func(state *gatewayPoolUsageLedger) bool {
			require.EqualValues(t, 9, state.VerificationSequence)
			return true
		}))
}

type gatewayPoolLedgerWriteRepo struct {
	*gatewayRuntimeRepo
	ledgers, tags []map[string]any
}

func (r *gatewayPoolLedgerWriteRepo) WriteGatewayPoolLedgers(_ context.Context, _ int64, ledgers, tags map[string]any) error {
	r.ledgers, r.tags = append(r.ledgers, ledgers), append(r.tags, tags)
	return nil
}

func TestGatewayPoolLedgerWritesSplitLedgersFromTags(t *testing.T) {
	ctx := context.Background()
	account := gwpoolTestAccount(1)
	base := &gatewayRuntimeRepo{account: *account}
	repo := &gatewayPoolLedgerWriteRepo{gatewayRuntimeRepo: base}
	svc := &OpenAIGatewayService{accountRepo: repo}
	require.True(t, svc.changeGatewayPoolUsage(ctx, account, gwpoolTestIdentity,
		func(*gatewayPoolUsageLedger) bool { return true }))
	require.Len(t, repo.ledgers, 1)
	require.Contains(t, repo.ledgers[0], gatewayPoolUsageExtraKey)
	require.Len(t, repo.ledgers[0], 1)
	require.Equal(t, map[string]any{gatewayPoolUsageTagKey: gatewayPoolLedgerTag(gwpoolTestIdentity)}, repo.tags[0])
	require.NotContains(t, base.account.Extra, gatewayPoolUsageExtraKey, "a ledger write must not reach extra")

	svc.accountRepo = struct{ AccountRepository }{base} // no ledger table
	require.NoError(t, svc.updateGatewayPoolRuntimeExtra(ctx, account.ID,
		map[string]any{gatewayPoolUsageExtraKey: "fallback", gatewayPoolUsageTagKey: "fallback"}))
	require.Equal(t, "fallback", base.account.Extra[gatewayPoolUsageExtraKey])
	require.Equal(t, "fallback", base.account.Extra[gatewayPoolUsageTagKey])
}
