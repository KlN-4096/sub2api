package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Preparation checks for exhaustion after every rejected ticket; with routes
// left, the list the take just used answers without another request.
func TestGatewayPoolExhaustionCheckReusesACachedRouteList(t *testing.T) {
	ctx := context.Background()
	account := rotationAccount(1, 7)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account)
	fake.listGateways = []gwpoolFakeGateway{{Name: "a", PairReady: true, ValidForS: 3600}, {Name: "b", PairReady: true, ValidForS: 3600}}
	svc := rotationService(account)
	pool, err := svc.codexCookies.poolClient(account)
	require.NoError(t, err)
	_, err = pool.Catalog(ctx, gatewayPoolUpstreamAccountID(gwpoolTestIdentity),
		gatewayPoolAccountTag(account, gwpoolTestIdentity), gatewayPoolProbeModelLuna, 0)
	require.NoError(t, err)
	require.False(t, svc.gatewayPoolNoRemainingRoutes(ctx, account))
	require.EqualValues(t, 1, fake.listHits.Load(), "a cached list with a usable route needs no refresh")
	for _, name := range []string{"a", "b"} {
		svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, name), time.Now())
	}
	require.True(t, svc.gatewayPoolNoRemainingRoutes(ctx, account))
	require.EqualValues(t, 2, fake.listHits.Load(), "zero is confirmed by exactly one fresh directory")
}

// A cached route is no proof at a usage boundary: a stale one would hold the
// round open past its real end.
func TestGatewayPoolUsageEndAlwaysAsksAFreshDirectory(t *testing.T) {
	ctx := context.Background()
	fake := newGwpoolFakePool(t, "", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "g", PairReady: true, ValidForS: 3600}}
	account := fake.account(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	svc.noteGatewayPoolUsage(ctx, account, identity, "gpt-6-luna",
		OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}, time.Now().Add(-time.Minute), false)
	ended := func() bool {
		fresh, _ := repo.stored(1)
		return !readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity)).Rounds[0].EndedAt.IsZero()
	}
	pool, err := svc.codexCookies.poolClient(account)
	require.NoError(t, err)
	_, err = pool.Catalog(ctx, gatewayPoolUpstreamAccountID(identity), gatewayPoolAccountTag(account, identity), "", 0)
	require.NoError(t, err)
	svc.finishGatewayPoolUsageIfExhausted(ctx, account)
	require.False(t, ended())
	require.EqualValues(t, 2, fake.listHits.Load(), "the boundary asks a fresh directory even with a cached route")
	svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(identity, "g"), time.Now())
	svc.finishGatewayPoolUsageIfExhausted(ctx, account)
	require.True(t, ended())
	require.EqualValues(t, 3, fake.listHits.Load())
}
