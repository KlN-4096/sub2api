package service

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	concurrencySlotRenewalInterval = 20 * time.Second
	concurrencySlotRenewalTimeout  = 2 * time.Second
)

var ErrConcurrencySlotLost = errors.New("request concurrency slot unavailable")

// Optional: ordinary requests keep their existing TTL semantics. Only requests
// that explicitly enable long waiting renew their exact original slot IDs.
type ConcurrencySlotRefresher interface {
	RefreshConcurrencySlot(context.Context, string, int64, string) (bool, error)
}

type concurrencySlotRenewalKey struct{}
type concurrencySlotID struct {
	kind string
	id   int64
	key  string
}
type concurrencySlotRenewals struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelCauseFunc
	slots    map[concurrencySlotID]func(context.Context) (bool, error)
	enabled  bool
	done     chan struct{}
	interval time.Duration
}

// The registry is installed before selection so user, API-key and scheduler-
// acquired account slots all belong to the same request. No timer or cache I/O
// is added until a pool request enables long preparation or continuous waiting.
func WithRenewableConcurrencySlots(ctx context.Context) (context.Context, func()) {
	if _, ok := ctx.Value(concurrencySlotRenewalKey{}).(*concurrencySlotRenewals); ok {
		return ctx, func() {}
	}
	base, cancel := context.WithCancelCause(ctx)
	state := &concurrencySlotRenewals{ctx: base, cancel: cancel, interval: concurrencySlotRenewalInterval,
		slots: map[concurrencySlotID]func(context.Context) (bool, error){}}
	return context.WithValue(base, concurrencySlotRenewalKey{}, state), func() {
		cancel(context.Canceled)
		state.mu.Lock()
		done := state.done
		state.mu.Unlock()
		if done != nil {
			<-done
		}
	}
}

func EnableConcurrencySlotRenewal(ctx context.Context) error {
	state, _ := ctx.Value(concurrencySlotRenewalKey{}).(*concurrencySlotRenewals)
	if state == nil {
		return ErrConcurrencySlotLost
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := state.refreshLocked(); err != nil {
		state.cancel(err)
		return err
	}
	if !state.enabled {
		state.enabled, state.done = true, make(chan struct{})
		go state.run()
	}
	return nil
}

func (state *concurrencySlotRenewals) refreshLocked() error {
	ctx, cancel := context.WithTimeout(state.ctx, concurrencySlotRenewalTimeout)
	defer cancel()
	if ctx.Err() != nil {
		return ErrConcurrencySlotLost
	}
	for _, refresh := range state.slots {
		owned, err := refresh(ctx)
		if err != nil || !owned {
			return ErrConcurrencySlotLost // never resurrect a lost/expired reservation
		}
	}
	return nil
}

func (state *concurrencySlotRenewals) run() {
	defer close(state.done)
	ticker := time.NewTicker(state.interval)
	defer ticker.Stop()
	for {
		select {
		case <-state.ctx.Done():
			return
		case <-ticker.C:
			state.mu.Lock()
			err := state.refreshLocked()
			state.mu.Unlock()
			if err != nil {
				state.cancel(err)
				return
			}
		}
	}
}

func (s *ConcurrencyService) renewableSlotRelease(ctx context.Context, kind string, id int64, key string, release func()) func() {
	state, _ := ctx.Value(concurrencySlotRenewalKey{}).(*concurrencySlotRenewals)
	if state == nil {
		return release
	}
	slot := concurrencySlotID{kind: kind, id: id, key: key}
	state.mu.Lock()
	state.slots[slot] = func(ctx context.Context) (bool, error) {
		cache, ok := s.cache.(ConcurrencySlotRefresher)
		if !ok {
			return false, ErrConcurrencySlotLost
		}
		return cache.RefreshConcurrencySlot(ctx, kind, id, key)
	}
	state.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			// Serialize removal with refresh: release must not be followed by a
			// late renewal of this slot or an erroneous request-wide lease loss.
			state.mu.Lock()
			delete(state.slots, slot)
			state.mu.Unlock()
			release()
		})
	}
}
