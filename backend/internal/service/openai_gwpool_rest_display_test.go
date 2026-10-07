package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolRestDisplayIgnoresRecheckExpiryAndHonorsTombstones(t *testing.T) {
	account := gwpoolTestAccount(1)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	now := time.Now().UTC()
	account.Extra[gatewayPoolRestStateKey] = gatewayPoolRestState{
		Tag: tag, Active: true, ChangedAt: now.Add(-time.Hour), NextCheck: now.Add(-time.Minute),
	}
	svc, repo := gatewayRuntimeService(account)
	view, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
	require.NoError(t, err)
	require.True(t, view[1].Runtime.Rest.Active)
	require.True(t, view[1].Runtime.Rest.NextCheck.Before(now))
	require.Nil(t, repo.account.TempUnschedulableUntil, "read-only display must not recreate a cleared generic block")
	peer := *gwpoolTestAccount(2)
	peer.Extra[gatewayPoolRestStateKey] = gatewayPoolRestState{Tag: tag, ChangedAt: now}
	require.False(t, svc.gatewayPoolRestDisplay(account, gwpoolTestIdentity, []Account{peer}).Active)
	require.False(t, svc.gatewayPoolRestDisplay(account, "different-identity", nil).Active)
	svc.codexCookies.poolRestState.Store(tag, gatewayPoolRestState{Tag: tag, ChangedAt: now})
	require.False(t, svc.gatewayPoolRestDisplay(account, gwpoolTestIdentity, nil).Active)
	require.True(t, readGatewayPoolRest(&repo.account, tag).Active, "projection does not write the persisted latch")
}

func TestGatewayPoolRestDisplayReportsUnreadableIdentity(t *testing.T) {
	svc, _ := gatewayRuntimeService(gwpoolTestAccount(1))
	svc.codexCookies.identity = func(context.Context, *Account) (string, error) {
		return "", errors.New("identity unavailable")
	}
	view, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
	require.Error(t, err)
	require.Nil(t, view)
}
