package service

import (
	"context"
	"time"
)

const (
	openAIGatewayPoolProbeTimeoutExtraKey = "openai_gwpool_probe_timeout_s"
	gatewayPoolProbeTimeoutMaxSeconds     = 120
)

type gatewayPoolProbeTimeoutKey struct{}

func (a *Account) gatewayPoolProbeTimeout() time.Duration {
	if a != nil {
		if seconds, ok := gatewayPoolInteger(a.Extra[openAIGatewayPoolProbeTimeoutExtraKey], gatewayPoolProbeTimeoutMaxSeconds); ok {
			return time.Duration(seconds) * time.Second
		}
	}
	return gatewayPoolWarmShotTimeout
}

// Shared probes use the fresh per-candidate settings, not the first waiter's
// potentially stale account object.
func gatewayPoolProbeTimeout(ctx context.Context, account *Account) time.Duration {
	if timeout, ok := ctx.Value(gatewayPoolProbeTimeoutKey{}).(time.Duration); ok && timeout > 0 {
		return timeout
	}
	return account.gatewayPoolProbeTimeout()
}

func gatewayPoolProbeBudget(ctx context.Context, account *Account) time.Duration {
	// Preserve the default envelope while allowing two configured A/B shots.
	if budget := 2 * gatewayPoolProbeTimeout(ctx, account); budget > gatewayPoolWarmBudget {
		return budget
	}
	return gatewayPoolWarmBudget
}
