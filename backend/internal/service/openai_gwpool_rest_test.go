package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolRotationThresholdCountsSpareAndKeepsVerifiedWindow(t *testing.T) {
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}, {Name: "unified-143", PairReady: false, UsedByYou: true}}
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 2
	fake.configure(account)
	svc := rotationService(account)
	require.True(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account))
	spare := openAIGatewayPoolPair{gateway: "unified-143", version: "spare", cookie: "offline", until: time.Now().Add(2 * time.Minute)}
	svc.codexCookies.poolSpare.Store(gwpoolTestIdentity, &gatewayPoolTicketBatch{pairs: []openAIGatewayPoolPair{spare}})
	require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account), "delivered spare counts once even if pool marks used")
	svc.codexCookies.poolSpare.Delete(gwpoolTestIdentity)
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, spare)
	svc.codexCookies.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, spare.version)
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 100
	require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account), "verified window is never preempted")
}

func TestGatewayPoolRestEstimateUsesNthCooldownWithBounds(t *testing.T) {
	store := &openAICodexCookieStore{}
	account := gwpoolTestAccount(1)
	now := time.Now()
	require.Equal(t, time.Minute, store.gatewayPoolRestDuration(gwpoolTestIdentity, account, now))
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "soon"), now.Add(-time.Hour+10*time.Second))
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "later"), now.Add(-time.Hour+5*time.Minute))
	require.Equal(t, 30*time.Second, store.gatewayPoolRestDuration(gwpoolTestIdentity, account, now))
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 2
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 2
	require.Equal(t, 5*time.Minute, store.gatewayPoolRestDuration(gwpoolTestIdentity, account, now))
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "later"), now)
	require.Equal(t, 10*time.Minute, store.gatewayPoolRestDuration(gwpoolTestIdentity, account, now))
}

func TestGatewayPoolRestDoesNotShortenOtherBlockOrDisableAccount(t *testing.T) {
	account := rotationAccount(1, 7)
	later := time.Now().Add(time.Hour)
	account.TempUnschedulableUntil = &later
	account.TempUnschedulableReason = "auth"
	svc := rotationService(account)
	svc.restGatewayPoolAccount(context.Background(), account, gwpoolTestIdentity, 7)
	fresh, err := svc.accountRepo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	require.Equal(t, later, *fresh.TempUnschedulableUntil)
	require.Equal(t, "auth", fresh.TempUnschedulableReason)
	require.True(t, fresh.Schedulable)
}
