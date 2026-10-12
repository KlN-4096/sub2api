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

type gatewayPoolRuntimeWriteRepo struct {
	*gatewayRuntimeRepo
	runtime []map[string]any
}

func (r *gatewayPoolRuntimeWriteRepo) UpdateGatewayPoolRuntimeExtra(_ context.Context, _ int64, patch map[string]any) error {
	r.runtime = append(r.runtime, patch)
	return nil
}

func TestGatewayPoolRuntimeWritesSkipUpdateExtraOnlyWhenAllKeysAreRuntime(t *testing.T) {
	account := gwpoolTestAccount(1)
	base := &gatewayRuntimeRepo{account: *account}
	repo := &gatewayPoolRuntimeWriteRepo{gatewayRuntimeRepo: base}
	svc := &OpenAIGatewayService{accountRepo: repo}
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), account, gwpoolTestIdentity,
		func(*gatewayPoolUsageLedger) bool { return true }))
	require.Len(t, repo.runtime, 1)
	require.Contains(t, repo.runtime[0], gatewayPoolUsageExtraKey)
	require.NotContains(t, base.account.Extra, gatewayPoolUsageExtraKey, "a runtime ledger write must not take UpdateExtra")

	mixed := map[string]any{gatewayPoolUsageTagKey: "mixed", openAIGatewayPoolExtraKey: true}
	require.NoError(t, svc.updateGatewayPoolRuntimeExtra(context.Background(), account.ID, mixed))
	require.Len(t, repo.runtime, 1, "a patch with any other key keeps UpdateExtra and its snapshot sync")
	require.Equal(t, "mixed", base.account.Extra[gatewayPoolUsageTagKey])

	svc.accountRepo = base
	require.NoError(t, svc.updateGatewayPoolRuntimeExtra(context.Background(), account.ID,
		map[string]any{gatewayPoolUsageTagKey: "fallback"}))
	require.Equal(t, "fallback", base.account.Extra[gatewayPoolUsageTagKey], "repositories without the method fall back")
}
