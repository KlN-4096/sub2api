package service

import (
	"context"
	"sync"
	"time"
)

// Shared work has no leader-owned deadline. Each caller may leave independently;
// the last departure cancels work. A cancelled transport remains registered until
// it really exits, so a newly arriving caller cannot overlap it.
type gatewayPoolSharedWork[T any] struct {
	mu    sync.Mutex
	calls map[string]*gatewayPoolSharedCall[T]
}

type gatewayPoolSharedCall[T any] struct {
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	abandoned bool
	finished  bool
	shared    bool
	result    T
	err       error
}

func (g *gatewayPoolSharedWork[T]) do(
	ctx context.Context, key string, timeout time.Duration,
	work func(context.Context) (T, error),
	register ...func() func(),
) (result T, shared bool, err error) {
	for {
		if err := ctx.Err(); err != nil {
			return result, false, err
		}
		g.mu.Lock()
		if g.calls == nil {
			g.calls = make(map[string]*gatewayPoolSharedCall[T])
		}
		call := g.calls[key]
		if call != nil && call.abandoned {
			done := call.done
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				return result, false, ctx.Err()
			case <-done:
				continue
			}
		}
		if call == nil {
			base := context.WithoutCancel(ctx)
			workCtx, cancel := context.WithCancel(base)
			if timeout > 0 {
				cancel()
				workCtx, cancel = context.WithTimeout(base, timeout)
			}
			call = &gatewayPoolSharedCall[T]{done: make(chan struct{}), cancel: cancel}
			g.calls[key] = call
			finish := func() {}
			if len(register) > 0 {
				finish = register[0]()
			}
			go func(call *gatewayPoolSharedCall[T]) {
				value, workErr := work(workCtx)
				cancel()
				finish()
				g.mu.Lock()
				call.result, call.err, call.finished = value, workErr, true
				if g.calls[key] == call {
					delete(g.calls, key)
				}
				close(call.done)
				g.mu.Unlock()
			}(call)
		} else {
			call.shared = true
		}
		call.waiters++
		g.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-call.done:
		}
		g.mu.Lock()
		call.waiters--
		if call.waiters == 0 && !call.finished {
			call.abandoned = true
			call.cancel()
		}
		result, shared, err = call.result, call.shared, call.err
		g.mu.Unlock()
		if ctx.Err() != nil {
			return result, shared, ctx.Err()
		}
		return result, shared, err
	}
}
