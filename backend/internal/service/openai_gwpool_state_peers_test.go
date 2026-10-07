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

func (r *gatewayPoolOrderedStateRepo) FindGatewayPoolStatePeers(context.Context, string, string) ([]Account, error) {
	return r.peers, nil // SQL contract separately asserts current-first ordering.
}

func TestGatewayPoolStatePeersEqualTimeKeepsCurrentBeforePrevious(t *testing.T) {
	account := gwpoolTestAccount(1)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	at := time.Now().UTC()
	current := Account{ID: 2, Extra: map[string]any{
		gatewayPoolRestStateKey:  gatewayPoolRestState{Tag: tag, ChangedAt: at, Active: false},
		gatewayPoolUsageExtraKey: gatewayPoolUsageLedger{Tag: tag, UpdatedAt: at, VerificationSequence: 9},
	}}
	previous := Account{ID: 3, Extra: map[string]any{
		gatewayPoolRestStateKey: gatewayPoolRestState{Tag: "other", Previous: &gatewayPoolRestState{
			Tag: tag, ChangedAt: at, Active: true,
		}},
		gatewayPoolUsageExtraKey: gatewayPoolUsageLedger{Tag: "other", Previous: &gatewayPoolUsageLedger{
			Tag: tag, UpdatedAt: at, VerificationSequence: 99,
		}},
	}}
	repo := &gatewayPoolOrderedStateRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}, peers: []Account{current, previous}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	rest, _, err := svc.loadGatewayPoolRest(context.Background(), account, gwpoolTestIdentity)
	require.NoError(t, err)
	require.False(t, rest.Active, "equal-time previous state must not revive current inactive tombstone")
	require.True(t, svc.changeGatewayPoolUsage(context.Background(), account, gwpoolTestIdentity,
		func(state *gatewayPoolUsageLedger) bool {
			require.EqualValues(t, 9, state.VerificationSequence)
			return true
		}))
}
