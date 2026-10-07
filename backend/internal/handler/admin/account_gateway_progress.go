package admin

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Admin-only runtime snapshot: no database write, pool fetch, or upstream probe.
func (h *AccountHandler) GatewayPoolProgress(c *gin.Context) {
	const maxAccounts = 200
	parts := strings.Split(c.Query("ids"), ",")
	if len(parts) > maxAccounts {
		response.Error(c, http.StatusBadRequest, "too many account IDs")
		return
	}
	ids := make([]int64, 0, len(parts))
	for _, part := range parts {
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil || id <= 0 {
			response.Error(c, http.StatusBadRequest, "invalid account ID")
			return
		}
		ids = append(ids, id)
	}
	progress := map[int64]service.GatewayPoolProgress{}
	if reader, ok := h.adminService.(interface {
		GatewayPoolRuntimeProgress(context.Context, []int64) (map[int64]service.GatewayPoolProgress, error)
	}); ok {
		var err error
		progress, err = reader.GatewayPoolRuntimeProgress(c.Request.Context(), ids)
		if err != nil {
			response.Error(c, http.StatusServiceUnavailable, "gateway runtime snapshot unavailable")
			return
		}
	} else if reader, ok := h.adminService.(interface {
		GatewayPoolProgress([]int64) map[int64]service.GatewayPoolProgress
	}); ok {
		progress = reader.GatewayPoolProgress(ids)
	}
	response.Success(c, progress)
}

func (h *AccountHandler) RetryGatewayPool(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.Error(c, http.StatusBadRequest, "invalid account ID")
		return
	}
	writer, ok := h.adminService.(interface {
		RetryGatewayPool(context.Context, int64) (service.GatewayPoolRetryResult, error)
	})
	if !ok {
		response.Error(c, http.StatusServiceUnavailable, "gateway retry unavailable")
		return
	}
	const actionTimeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(c.Request.Context(), actionTimeout)
	defer cancel()
	result, err := writer.RetryGatewayPool(ctx, id)
	if err != nil {
		response.Error(c, http.StatusServiceUnavailable, "gateway cooldown clear or retry failed")
		return
	}
	response.Success(c, result)
}
