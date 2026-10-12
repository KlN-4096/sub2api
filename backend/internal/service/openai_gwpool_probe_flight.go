package service

import (
	"context"
	"time"
)

// A probe is shared work, not work owned by its first HTTP request; waiters,
// last-waiter cancellation and abandoned flights follow gatewayPoolSharedWork.
type gatewayPoolProbeFlights struct {
	gatewayPoolSharedWork[gatewayPoolProbeResult]
}

type gatewayPoolProbeResult struct {
	full, conclusive, sent bool
	firstSent              time.Time
}

func (g *gatewayPoolProbeFlights) do(
	ctx context.Context, key string, timeout time.Duration,
	probe func(context.Context) (gatewayPoolProbeResult, error),
	register ...func() func(),
) (gatewayPoolProbeResult, error) {
	result, _, err := g.gatewayPoolSharedWork.do(ctx, key, timeout, probe, register...)
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The shared transport may already have sent; cancellation is
		// never permission to return another caller's ticket to the pool.
		return gatewayPoolProbeResult{sent: true}, ctxErr
	}
	return result, err
}
