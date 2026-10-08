package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type renewableConcurrencyCache struct {
	ConcurrencyCache
	mu        sync.Mutex
	lost      bool
	refreshes map[concurrencySlotID]int
	releases  int
}

func (c *renewableConcurrencyCache) AcquireAccountSlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}
func (c *renewableConcurrencyCache) AcquireUserSlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}
func (c *renewableConcurrencyCache) TrackAPIKeySlot(context.Context, int64, string) error { return nil }
func (c *renewableConcurrencyCache) GetAPIKeyConcurrencyBatch(context.Context, []int64) (map[int64]int, error) {
	return nil, nil
}
func (c *renewableConcurrencyCache) release() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releases++
	return nil
}
func (c *renewableConcurrencyCache) ReleaseAccountSlot(context.Context, int64, string) error {
	return c.release()
}
func (c *renewableConcurrencyCache) ReleaseUserSlot(context.Context, int64, string) error {
	return c.release()
}
func (c *renewableConcurrencyCache) ReleaseAPIKeySlot(context.Context, int64, string) error {
	return c.release()
}
func (c *renewableConcurrencyCache) RefreshConcurrencySlot(_ context.Context, kind string, id int64, key string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refreshes == nil {
		c.refreshes = map[concurrencySlotID]int{}
	}
	c.refreshes[concurrencySlotID{kind, id, key}]++
	return !c.lost, nil
}

func TestConcurrencySlotRenewalOptInRetainsExactSlotsAndReleasesOnce(t *testing.T) {
	cache := &renewableConcurrencyCache{}
	svc := NewConcurrencyService(cache)
	ctx, stop := WithRenewableConcurrencySlots(context.Background())
	defer stop()
	account, err := svc.AcquireAccountSlot(ctx, 1, 1)
	require.NoError(t, err)
	user, err := svc.AcquireUserSlot(ctx, 2, 1)
	require.NoError(t, err)
	apiRelease := svc.TrackAPIKeySlot(ctx, 3)
	cache.mu.Lock()
	require.Empty(t, cache.refreshes, "ordinary requests do not renew")
	cache.mu.Unlock()
	require.NoError(t, EnableConcurrencySlotRenewal(ctx))
	require.NoError(t, EnableConcurrencySlotRenewal(ctx))
	cache.mu.Lock()
	require.Len(t, cache.refreshes, 3)
	for slot, calls := range cache.refreshes {
		require.NotEmpty(t, slot.key)
		require.Equal(t, 2, calls, "renew the same ID, never obtain additional slots")
	}
	cache.mu.Unlock()
	for range 2 {
		account.ReleaseFunc()
		user.ReleaseFunc()
		apiRelease()
	}
	require.NoError(t, EnableConcurrencySlotRenewal(ctx), "released slots are no longer renewed")
	cache.mu.Lock()
	require.Equal(t, 3, cache.releases)
	cache.mu.Unlock()
}

func TestConcurrencySlotRenewalCancelsOnLostSlot(t *testing.T) {
	cache := &renewableConcurrencyCache{}
	svc := NewConcurrencyService(cache)
	ctx, stop := WithRenewableConcurrencySlots(context.Background())
	defer stop()
	state, ok := ctx.Value(concurrencySlotRenewalKey{}).(*concurrencySlotRenewals)
	require.True(t, ok)
	state.interval = time.Millisecond
	slot, err := svc.AcquireAccountSlot(ctx, 1, 1)
	require.NoError(t, err)
	defer slot.ReleaseFunc()
	require.NoError(t, EnableConcurrencySlotRenewal(ctx))
	cache.mu.Lock()
	cache.lost = true
	cache.mu.Unlock()
	select {
	case <-ctx.Done():
		require.ErrorIs(t, context.Cause(ctx), ErrConcurrencySlotLost)
	case <-time.After(time.Second):
		t.Fatal("lost slot did not cancel waiting")
	}
}

func TestConcurrencySlotRenewalMissingContextFailsClosed(t *testing.T) {
	require.ErrorIs(t, EnableConcurrencySlotRenewal(context.Background()), ErrConcurrencySlotLost)
}
