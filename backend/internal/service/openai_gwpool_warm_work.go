package service

import (
	"context"
	"fmt"
	"time"
)

type gatewayPoolWarmWorkKey struct{}

// Owned by one business invocation. A last-mile ticket change may re-enter
// preflight, but may not buy another set of attempts or another 90 seconds.
type gatewayPoolWarmWork struct {
	started       time.Time
	budget        time.Duration
	waitedAtStart time.Duration
	attempts      int
	limit         int
}

func (w *gatewayPoolWarmWork) remaining(wait *gatewayPoolWaitState) time.Duration {
	waited := time.Duration(0)
	if wait != nil {
		_, _, totalWaited := wait.snapshot()
		waited = totalWaited - w.waitedAtStart
	}
	return w.budget - (time.Since(w.started) - waited)
}

var errGatewayPoolPreflightChanged = fmt.Errorf("%w: route changed before business send", errOpenAIGatewayPoolWarmUnverified)

func gatewayPoolWarmWorkFrom(ctx context.Context) *gatewayPoolWarmWork {
	work, _ := ctx.Value(gatewayPoolWarmWorkKey{}).(*gatewayPoolWarmWork)
	if work == nil {
		work = &gatewayPoolWarmWork{}
	}
	return work
}
