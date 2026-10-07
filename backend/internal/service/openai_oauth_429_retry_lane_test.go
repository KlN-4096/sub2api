package service

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenAIOAuth429SharedLaneSpacesConcurrentClonesAndBoundsQueue(t *testing.T) {
	svc := &OpenAIGatewayService{}
	now := time.Now()
	deadline := now.Add(time.Minute)
	var mu sync.Mutex
	var delays []time.Duration
	var done sync.WaitGroup
	for i := 0; i < 32; i++ {
		done.Add(1)
		go func(id int64) {
			defer done.Done()
			account := gwpoolTestAccount(id)
			delay, ok := svc.reserveOpenAIOAuth429RetryAt(account, nil, deadline, now)
			if ok {
				mu.Lock()
				delays = append(delays, delay)
				mu.Unlock()
			}
		}(int64(i + 1))
	}
	done.Wait()
	require.Len(t, delays, 32, "a burst must use the existing retry budget, not fail after only 16 reservations")
	sort.Slice(delays, func(i, j int) bool { return delays[i] < delays[j] })
	for i := 1; i < len(delays); i++ {
		require.GreaterOrEqual(t, delays[i]-delays[i-1], openAIOAuth429RetryDelay)
	}
	require.Less(t, delays[len(delays)-1], deadline.Sub(now))
	_, ok := svc.reserveOpenAIOAuth429RetryAt(gwpoolTestAccount(1), nil, now.Add(time.Millisecond), now)
	require.False(t, ok, "do not reserve beyond the original deadline")
}
