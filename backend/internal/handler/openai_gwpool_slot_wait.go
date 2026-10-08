package handler

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

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
			return nil, false, err
		}
	}
}
