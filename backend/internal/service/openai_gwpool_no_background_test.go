package service

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolVerifiedTicketNeverStartsBackgroundPreparation(t *testing.T) {
	fake := newGwpoolFakePool(t, "offline-cookie", 150)
	fake.refuseStatus = http.StatusServiceUnavailable
	account := fake.account(1)
	account.Extra["openai_gwpool_prewarm"] = true // An old saved setting must be inert.
	svc := &OpenAIGatewayService{}
	now := time.Now()
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: "offline-cookie", gateway: "unified-142", version: "live",
		until: now.Add(40 * time.Second), since: now.Add(-110 * time.Second),
	})
	svc.codexCookies.poolVerified.Store(gwpoolTestIdentity, gatewayPoolVerifiedMark{
		version: "live", at: now.Add(-110 * time.Second),
		models: &map[string]time.Time{"gpt-6-luna": now.Add(-110 * time.Second)},
	})
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL,
		strings.NewReader(`{"model":"gpt-6-luna","input":"business"}`))
	require.NoError(t, err)
	require.NoError(t, svc.gatewayPoolWarmUp(req, "", account))
	select {
	case <-fake.queries:
		t.Fatal("a live verified ticket must not fetch a background candidate")
	case <-time.After(100 * time.Millisecond):
	}
	require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity))
}
