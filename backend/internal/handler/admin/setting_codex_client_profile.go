package admin

import (
	"encoding/json"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type codexClientProfileRequest struct {
	Profile json.RawMessage `json:"profile"`
}

func codexReleasesOrUnavailable(c *gin.Context) *service.CodexReleaseService {
	svc := service.CodexReleases()
	if svc == nil {
		response.Error(c, http.StatusServiceUnavailable, "Codex client profile service is unavailable")
	}
	return svc
}

// GetCodexClientProfile 全局 Codex 客户端预设、6 个预设默认值与官方发布状态。
// GET /api/v1/admin/settings/codex-client-profile
func (h *SettingHandler) GetCodexClientProfile(c *gin.Context) {
	if svc := codexReleasesOrUnavailable(c); svc != nil {
		response.Success(c, svc.ClientProfileView())
	}
}

// UpdateCodexClientProfile 保存全局预设，profile 为 null 时清除。
// PUT /api/v1/admin/settings/codex-client-profile
func (h *SettingHandler) UpdateCodexClientProfile(c *gin.Context) {
	svc := codexReleasesOrUnavailable(c)
	if svc == nil {
		return
	}
	var req codexClientProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	if err := svc.SaveGlobalClientProfile(c.Request.Context(), req.Profile); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, svc.ClientProfileView())
}

// PreviewCodexClientProfile 解析一份预设得到出站 UA / originator / version（账号与全局共用）。
// POST /api/v1/admin/settings/codex-client-profile/preview
func (h *SettingHandler) PreviewCodexClientProfile(c *gin.Context) {
	svc := codexReleasesOrUnavailable(c)
	if svc == nil {
		return
	}
	var req codexClientProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	preview, err := svc.PreviewClientProfile(req.Profile)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, preview)
}

// RefreshCodexReleases 立即检查官方发布（后台进行，状态见 GET 的 refreshing）。
// POST /api/v1/admin/settings/codex-client-profile/refresh
func (h *SettingHandler) RefreshCodexReleases(c *gin.Context) {
	if svc := codexReleasesOrUnavailable(c); svc != nil {
		svc.TriggerRefresh()
		response.Success(c, svc.ClientProfileView())
	}
}
