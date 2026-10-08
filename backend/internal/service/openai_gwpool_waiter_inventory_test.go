package service

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolPreparingWaiterCannotMaskExhaustion(t *testing.T) {
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.listGateways = nil // true exhaustion, not the retired low-count threshold
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 10
	fake.configure(account)
	svc, _ := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	require.True(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account))
	waiter, finish := svc.beginGatewayPoolUsageRequest(context.Background(), account)
	require.NotNil(t, finish)
	defer finish()
	require.True(t, svc.gatewayPoolNoRemainingRoutes(waiter, account),
		"preparation waiters cannot hide a shortage without any inventory work")
	gatewayPoolUsageMarkSending(waiter, identity)
	gatewayPoolUsageMarkSending(waiter, identity)
	require.False(t, svc.gatewayPoolNoRemainingRoutes(waiter, account),
		"real dispatch must still protect inventory until body completion")
	finish()
	gatewayPoolUsageMarkSending(waiter, identity)
	require.True(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account),
		"repeated send/finish and late callbacks cannot leak or revive active inventory")
	inventory := svc.codexCookies.gatewayPoolInventory(identity)
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	require.Zero(t, inventory.active)
	require.Zero(t, inventory.requests)
}

func TestGatewayPoolBusinessBodyPinsInventoryUntilEOF(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	svc, _ := gatewayRuntimeService(account)
	svc.httpUpstream = &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: 200}}}
	_, response, err := gwpoolEchoRun(t, svc, account, "same-state", true)
	require.NoError(t, err)
	require.NotNil(t, response)
	inventory := svc.codexCookies.gatewayPoolInventory(gwpoolTestIdentity)
	inventory.mu.Lock()
	active, requests := inventory.active, inventory.requests
	inventory.mu.Unlock()
	require.Equal(t, 1, active, "response headers do not end real business activity")
	require.Equal(t, 1, requests)
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	require.Zero(t, inventory.active)
	require.Zero(t, inventory.requests)
}
