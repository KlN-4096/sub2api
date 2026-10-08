package service

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolSingleTicketAcquisitionAndReuse(t *testing.T) {
	for _, sharedPreparation := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "shared_preparation"}[sharedPreparation], func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-11"), 150)
			store := &openAICodexCookieStore{}
			ctx := context.Background()
			if sharedPreparation {
				ctx = context.WithValue(ctx, gatewayPoolPreparationKey{}, true)
			}
			pair, fresh, err := store.gatewayPoolPair(ctx, fake.account(1), gwpoolTestIdentity)
			require.NoError(t, err)
			require.True(t, fresh)
			query, err := url.ParseQuery(fake.nextQuery(t))
			require.NoError(t, err)
			require.False(t, query.Has("count"), "every acquisition uses the single-ticket protocol")
			reused, fresh, err := store.gatewayPoolPair(ctx, fake.account(2), gwpoolTestIdentity)
			require.NoError(t, err)
			require.False(t, fresh)
			require.Equal(t, pair, reused)
			require.EqualValues(t, 1, fake.hits.Load())
		})
	}
}

func TestGatewayPoolUnsentCleanupIsLocalAndExact(t *testing.T) {
	for _, state := range []string{"unsent", "replacement", "already_sent"} {
		t.Run(state, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-11"), 150)
			account := fake.account(1)
			store := &openAICodexCookieStore{}
			pair, _, err := store.gatewayPoolPair(context.Background(), account, gwpoolTestIdentity)
			require.NoError(t, err)
			cleanup := store.gatewayPoolDiscardUnsent(gwpoolTestIdentity, pair)
			require.NotNil(t, cleanup)
			current := pair
			switch state {
			case "replacement":
				current.version = "new-ticket"
			case "already_sent":
				current.firstSent = time.Now()
			}
			store.poolPairs.Store(gwpoolTestIdentity, current)
			cleanup()
			require.Zero(t, fake.releaseHits.Load(), "local cleanup must never call POST /release")
			after, live := store.cachedPoolPair(gwpoolTestIdentity)
			if state == "unsent" {
				require.Equal(t, openAIGatewayPoolPairNone, live)
			} else {
				require.Equal(t, openAIGatewayPoolPairLive, live)
				require.Equal(t, current, after)
			}
			require.True(t, store.gatewayPoolUsedRecently(gwpoolTestIdentity, pair.gateway, account.gatewayPoolGatewayWindow()), "cleanup cannot clear cooldown")
		})
	}
}
