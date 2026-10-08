package service

import (
	"context"
	"sync"
	"time"
)

type gatewayPoolFirstOutputBudgetKey struct{}
type gatewayPoolFirstOutputGuardKey struct{}

// One business attempt budget, shared by its protocol-repair retries. Preparation
// has its own per-caller deadline; the first-output clock begins only when the
// first real business dispatch is about to happen, not during A/B verification.
// A replacement business attempt resets this budget, not the caller's deadline.
type gatewayPoolFirstOutputBudget struct {
	mu      sync.Mutex
	started time.Time
	timeout time.Duration
}

func gatewayPoolFirstOutputStart(ctx context.Context, fallback time.Time) time.Time {
	budget, _ := ctx.Value(gatewayPoolFirstOutputBudgetKey{}).(*gatewayPoolFirstOutputBudget)
	if budget == nil {
		return fallback
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.started.IsZero() {
		return fallback
	}
	return budget.started
}

func startGatewayPoolFirstOutputGuard(ctx context.Context) {
	guard, _ := ctx.Value(gatewayPoolFirstOutputGuardKey{}).(*openAIFirstOutputHeaderGuard)
	if guard == nil {
		return
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.closed || guard.timer != nil || guard.preparedBudget == nil {
		return
	}
	budget := guard.preparedBudget
	budget.mu.Lock()
	if budget.started.IsZero() {
		budget.started = time.Now()
	}
	deadline := budget.started.Add(budget.timeout)
	budget.mu.Unlock()
	guard.armLocked(deadline)
}

func newGatewayPoolFirstOutputGuard(ctx context.Context, release context.CancelFunc,
	budget *gatewayPoolFirstOutputBudget,
) (context.Context, *openAIFirstOutputHeaderGuard) {
	guarded, cancel := context.WithCancel(ctx)
	guard := &openAIFirstOutputHeaderGuard{
		cancel: cancel, release: release, fired: make(chan struct{}), preparedBudget: budget,
	}
	budget.mu.Lock()
	if !budget.started.IsZero() {
		// A repair retry must not reset the deadline of an already-sent request.
		guard.armLocked(budget.started.Add(budget.timeout))
	}
	budget.mu.Unlock()
	return context.WithValue(guarded, gatewayPoolFirstOutputGuardKey{}, guard), guard
}
