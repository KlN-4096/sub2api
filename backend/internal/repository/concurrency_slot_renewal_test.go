package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestConcurrencySlotRenewalKeepsCountsBeyondOriginalTTL(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache := NewConcurrencyCache(client, 1, 60)
	refresh, ok := cache.(service.ConcurrencySlotRefresher)
	require.True(t, ok)
	ctx, now := context.Background(), time.Now()
	server.SetTime(now)
	owned, err := cache.AcquireAccountSlot(ctx, 1, 1, "account-slot")
	require.NoError(t, err)
	require.True(t, owned)
	owned, err = cache.AcquireUserSlot(ctx, 2, 1, "user-slot")
	require.NoError(t, err)
	require.True(t, owned)
	api, ok := cache.(service.APIKeyConcurrencyCache)
	require.True(t, ok)
	require.NoError(t, api.TrackAPIKeySlot(ctx, 3, "api-slot"))
	for i := 1; i <= 8; i++ {
		server.FastForward(40 * time.Second)
		server.SetTime(now.Add(time.Duration(i) * 40 * time.Second))
		for _, slot := range []struct {
			kind string
			id   int64
			key  string
		}{
			{"account", 1, "account-slot"}, {"user", 2, "user-slot"}, {"api_key", 3, "api-slot"},
		} {
			retained, err := refresh.RefreshConcurrencySlot(ctx, slot.kind, slot.id, slot.key)
			require.NoError(t, err)
			require.True(t, retained)
		}
		acquired, err := cache.AcquireAccountSlot(ctx, 1, 1, "must-not-overbook")
		require.NoError(t, err)
		require.False(t, acquired)
		count, err := cache.GetUserConcurrency(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		counts, err := api.GetAPIKeyConcurrencyBatch(ctx, []int64{3})
		require.NoError(t, err)
		require.Equal(t, 1, counts[3])
	}
	require.NoError(t, cache.ReleaseAccountSlot(ctx, 1, "account-slot"))
	retained, err := refresh.RefreshConcurrencySlot(ctx, "account", 1, "account-slot")
	require.NoError(t, err)
	require.False(t, retained, "release cannot be undone by a late heartbeat")
	server.FastForward(61 * time.Second)
	server.SetTime(now.Add(8*40*time.Second + 61*time.Second))
	retained, err = refresh.RefreshConcurrencySlot(ctx, "user", 2, "user-slot")
	require.NoError(t, err)
	require.False(t, retained, "expired leases are never recreated")
}
