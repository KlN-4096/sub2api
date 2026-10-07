package service

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolRouteDeadlineNeverGuessesMissingTTL(t *testing.T) {
	exp := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	cookie := "__cflb=offline; __oailb=e30." + base64.RawURLEncoding.EncodeToString(
		[]byte(fmt.Sprintf(`{"exp":%d}`, exp.Unix()))) + ".offline"
	require.Equal(t, exp, gatewayPoolRouteExpiresAt(cookie, time.Time{}))
	require.Equal(t, exp.Add(-time.Minute), gatewayPoolRouteExpiresAt(cookie, exp.Add(-time.Minute)))
	require.Equal(t, exp, gatewayPoolRouteExpiresAt(cookie, exp.Add(time.Minute)))
	require.Zero(t, gatewayPoolRouteExpiresAt("__cflb=offline; __oailb=unknown", time.Time{}))
	store := &openAICodexCookieStore{}
	pair := openAIGatewayPoolPair{cookie: cookie, version: "v", until: time.Now().Add(-time.Hour), routeExpiresAt: exp}
	store.poolPairs.Store("id", pair)
	store.gatewayPoolMarkVerifiedFull("id", "v", "gpt-6-luna")
	_, state := store.cachedPoolPair("id")
	require.Equal(t, openAIGatewayPoolPairLive, state)
	pair.routeExpiresAt = time.Now().Add(-time.Second)
	store.poolPairs.Store("id", pair)
	_, state = store.cachedPoolPair("id")
	require.Equal(t, openAIGatewayPoolPairStale, state)
	require.False(t, pair.invalidated, "credential expiry is not a degradation verdict")
}
