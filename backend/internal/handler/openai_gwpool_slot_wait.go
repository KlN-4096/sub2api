package handler

import (
	"context"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

const gatewayPoolWSUserSlotRetryGap = time.Second

// Same bound as an HTTP user-slot wait.
var gatewayPoolWSUserSlotWaitMax = maxConcurrencyWait

// During failover no account is selected. Wait only for user capacity, before
// taking any account slot; checking the discarded account's rest would close
// the connection, while holding the next account slot can deadlock HTTP waits.
func acquireGatewayPoolWSFailoverUserSlot(ctx context.Context, acquire func() (func(), bool, error)) (func(), bool, error) {
	deadline := time.Now().Add(gatewayPoolWSUserSlotWaitMax)
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		release, acquired, err := acquire()
		if err != nil || acquired || !time.Now().Before(deadline) {
			return release, acquired, err
		}
		timer := time.NewTimer(min(gatewayPoolWSUserSlotRetryGap, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-timer.C:
		}
	}
}

func (h *OpenAIGatewayHandler) acquireGatewayPoolWSTurnSlot(
	ctx context.Context, c *gin.Context, account *service.Account,
	acquire func() (func(), bool, error),
) (func(), bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		release, acquired, err := acquire()
		if err != nil || acquired || !account.UsesGatewayPool() {
			return release, acquired, err
		}
		retry, err := h.gatewayService.WaitGatewayPoolCapacity(ctx, c, account)
		if err != nil || !retry {
			if failure := h.gatewayService.GatewayPoolRestHandoff(ctx, account); failure != nil {
				return nil, false, failure
			}
			return nil, false, err
		}
	}
}
