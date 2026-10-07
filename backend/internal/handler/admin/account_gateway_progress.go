package admin

import (
	"net/http"
	"strconv"
	"strings"

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
		GatewayPoolProgress([]int64) map[int64]service.GatewayPoolProgress
	}); ok {
		progress = reader.GatewayPoolProgress(ids)
	}
	response.Success(c, progress)
}
