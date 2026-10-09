package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolRouteDeadlineNeverGuessesMissingTTL(t *testing.T) {
	exp := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	cookie := "__cflb=offline; __oailb=e30." + base64.RawURLEncoding.EncodeToString(
		[]byte(fmt.Sprintf(`{"exp":%d}`, exp.Unix()))) + ".offline"
	require.Equal(t, exp, gatewayPoolRouteExpiresAt(cookie))
	require.Zero(t, gatewayPoolRouteExpiresAt("__cflb=offline; __oailb=unknown"), "unknown provenance cannot become a hard deadline")
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

func TestGatewayPoolAcquisitionUsesCookieExpiryNotPoolEstimate(t *testing.T) {
	for _, mode := range []string{"future-cookie", "unknown-cookie", "expired-cookie"} {
		t.Run(mode, func(t *testing.T) {
			cookie := "__cflb=offline; __oailb=unknown"
			if mode != "unknown-cookie" {
				exp := time.Now().Add(time.Hour)
				if mode == "expired-cookie" {
					exp = time.Now().Add(-time.Minute)
				}
				cookie = "__cflb=offline; __oailb=e30." + base64.RawURLEncoding.EncodeToString(
					[]byte(fmt.Sprintf(`{"exp":%d}`, exp.Unix()))) + ".offline"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/gateways" {
					_, _ = w.Write([]byte(`{"gateways":[{"name":"unified-11","pair_ready":true}]}`))
					return
				}
				require.Equal(t, "/cookie", r.URL.Path)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"gateway": "unified-11", "cookie": cookie, "cookie_version": "v",
					"route_expires_at": time.Now().Add(-time.Minute), "valid_for_s": 0, "pair_remaining_s": 0,
				})
			}))
			defer server.Close()
			account := gwpoolTestAccount(1)
			account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
			account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "offline"
			store := &openAICodexCookieStore{}
			pair, _, err := store.gatewayPoolPair(context.Background(), account, gwpoolTestIdentity)
			if mode == "expired-cookie" {
				require.Error(t, err)
				require.False(t, store.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-11", time.Hour))
			} else {
				require.NoError(t, err)
				require.Equal(t, cookie, pair.cookie)
				require.False(t, pair.routeExpired(time.Now()))
			}
		})
	}
}
