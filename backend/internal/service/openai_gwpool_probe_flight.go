package service

import (
	"context"
	"sync"
	"time"
)

// A probe is shared work, not work owned by its first HTTP request. Each waiter
// has its own cancellation/deadline; the last departing waiter cancels the work.
// An abandoned flight stays registered until its transport has actually ended.
type gatewayPoolProbeFlights struct {
	mu    sync.Mutex
	calls map[string]*gatewayPoolProbeFlight
}

type gatewayPoolProbeResult struct {
	full, conclusive, sent bool
	firstSent              time.Time
	err                    error
}

type gatewayPoolProbeFlight struct {
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	abandoned bool
	finished  bool
	result    gatewayPoolProbeResult
}

func (g *gatewayPoolProbeFlights) do(
	ctx context.Context, key string, timeout time.Duration,
	probe func(context.Context) (gatewayPoolProbeResult, error),
	register ...func() func(),
) (gatewayPoolProbeResult, error) {
	for {
		if err := ctx.Err(); err != nil {
			return gatewayPoolProbeResult{sent: true}, err
		}
		g.mu.Lock()
		if g.calls == nil {
			g.calls = make(map[string]*gatewayPoolProbeFlight)
		}
		call := g.calls[key]
		if call != nil && call.abandoned {
			done := call.done
			g.mu.Unlock()
			// Do not start a second transport while the abandoned one is
			// still unwinding. This time still counts against the caller.
			select {
			case <-ctx.Done():
				return gatewayPoolProbeResult{sent: true}, ctx.Err()
			case <-done:
				continue
			}
		}
		if call == nil {
			work, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			call = &gatewayPoolProbeFlight{done: make(chan struct{}), cancel: cancel}
			g.calls[key] = call
			finish := func() {}
			if len(register) != 0 {
				// Register before launching: caller cancellation must not
				// expose zero in-flight work while this goroutine is queued.
				finish = register[0]()
			}
			go func(call *gatewayPoolProbeFlight) {
				result, err := probe(work)
				cancel()
				finish()
				result.err = err
				g.mu.Lock()
				call.result, call.finished = result, true
				if g.calls[key] == call {
					delete(g.calls, key)
				}
				close(call.done)
				g.mu.Unlock()
			}(call)
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
		result := call.result
		g.mu.Unlock()
		if err := ctx.Err(); err != nil {
			// The shared transport may already have sent; cancellation is
			// never permission to return another caller's ticket to the pool.
			return gatewayPoolProbeResult{sent: true}, err
		}
		return result, result.err
	}
}
