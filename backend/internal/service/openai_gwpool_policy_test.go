package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolLegacyEarlySettingCannotBypassCooldown(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}
	account := fake.account(1)
	account.Extra["openai_gwpool_early_probe_enabled"] = true
	svc, _ := gatewayRuntimeService(account)
	svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-142")
	require.True(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account),
		"an obsolete early setting cannot make a cooling gateway eligible")
	require.True(t, svc.codexCookies.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-142", time.Hour))
	require.Zero(t, fake.hits.Load(), "exhaustion checks must not fetch or probe")
}
