package service

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolReferenceTTLDoesNotRejectOrRefetch(t *testing.T) {
	for _, seconds := range []int{-1, 0, 1, 150} {
		t.Run(strconv.Itoa(seconds), func(t *testing.T) {
			cookie := gwpoolTestPairCookie(t, "unified-142")
			fake := newGwpoolFakePool(t, cookie, seconds)
			store := &openAICodexCookieStore{}
			account := fake.account(1)
			require.NoError(t, attachRoute(context.Background(), store, account, gwpoolTestURL, http.Header{}))
			pair, _ := store.cachedPoolPair(gwpoolTestIdentity)
			pair.until = time.Now().Add(-time.Hour)
			store.poolPairs.Store(gwpoolTestIdentity, pair)
			headers := http.Header{}
			require.NoError(t, attachRoute(context.Background(), store, account, gwpoolTestURL, headers))
			require.Equal(t, cookie, headers.Get("Cookie"))
			require.EqualValues(t, 1, fake.hits.Load(), "reference TTL cannot trigger another pool fetch")
		})
	}
}

func TestStateEchoReferenceTTLDoesNotDiscardAcceptedBusiness(t *testing.T) {
	for _, phase := range []string{"business", "confirmation", "persistence", "route-expired-after-confirmation-send"} {
		t.Run(phase, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			repo := &gatewayEchoBoundaryRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}}
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
				{status: 200, minted: "s1"}, {status: 200, minted: "s1"},
			}}
			svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}
			expireReference := func() {
				pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
				pair.until = time.Now().Add(-time.Hour)
				svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
			}
			upstream.beforeReply = func(_ *http.Request, n int) error {
				if (phase == "business" && n == 1) || (phase == "confirmation" && n == 2) {
					expireReference()
				}
				if phase == "route-expired-after-confirmation-send" && n == 2 {
					pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
					pair.routeExpiresAt = time.Now().Add(-time.Second)
					svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
				}
				return nil
			}
			if phase == "persistence" {
				repo.onContact = expireReference
			}
			_, response, err := gwpoolEchoRun(t, svc, account, "old")
			require.NoError(t, err)
			require.NotNil(t, response)
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.False(t, upstream.bodies[0].closed, "an accepted business response must remain deliverable")
			_ = response.Body.Close()
			if phase != "route-expired-after-confirmation-send" {
				require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity))
			}
		})
	}
}

func TestGatewayPoolReferenceTTLDoesNotLimitWarmProbe(t *testing.T) {
	store := &openAICodexCookieStore{}
	store.poolPairs.Store("identity", openAIGatewayPoolPair{
		cookie: "offline", gateway: "g", version: "v", until: time.Now().Add(-time.Hour),
	})
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "gpt-6-luna")
	full, conclusive, _, _, err := store.gatewayPoolWarmVerdict(ctx, &Account{ID: 1}, "identity",
		OpenAIGatewayPoolApplied{Version: "v", Gateway: "g"}, "offline", 1,
		func(ctx context.Context, _, _ string) (int, string, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Greater(t, time.Until(deadline), time.Second)
			return http.StatusOK, "same-state", nil
		})
	require.NoError(t, err)
	require.True(t, full && conclusive)
}
