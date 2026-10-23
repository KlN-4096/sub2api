package admin

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *AccountHandler) RefreshCPRQuota(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	if h.accountUsageService == nil {
		response.Error(c, http.StatusServiceUnavailable, "CPR quota service is unavailable")
		return
	}
	result, err := h.accountUsageService.RefreshCPRQuota(c.Request.Context(), id)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *AccountHandler) ResetCPRQuota(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	var req struct {
		RedeemRequestID string `json:"redeem_request_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "A redeem request ID is required")
		return
	}
	if h.accountUsageService == nil {
		response.Error(c, http.StatusServiceUnavailable, "CPR quota service is unavailable")
		return
	}
	result, err := h.accountUsageService.ResetCPRQuota(c.Request.Context(), id, req.RedeemRequestID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func cprURL(value string) string { return strings.TrimRight(strings.TrimSpace(value), "/") }

// resolveCPRAdminKey never sends a saved key to a newly entered destination.
func resolveCPRAdminKey(account, existing *service.Account) error {
	if account.GetCPRAdminAPIKey() != "" {
		return nil
	}
	if existing == nil || !existing.IsCPR() || cprURL(account.GetCPRAdminBaseURL()) != cprURL(existing.GetCPRAdminBaseURL()) {
		return infraerrors.BadRequest("CPR_ADMIN_KEY_REQUIRED", "CPR Admin Key is required for this address")
	}
	account.Credentials["admin_api_key"] = existing.GetCPRAdminAPIKey()
	return nil
}

func (h *AccountHandler) configureCPR(ctx context.Context, platform, accountType string, credentials map[string]any, existing *service.Account) (map[string]any, error) {
	if platform != service.PlatformOpenAI || accountType != service.AccountTypeCPR {
		return nil, infraerrors.BadRequest("INVALID_CPR_ACCOUNT", "Automatic CPR setup requires an OpenAI CPR account")
	}
	account := &service.Account{Platform: platform, Type: accountType, Credentials: make(map[string]any)}
	for key, value := range credentials {
		account.Credentials[key] = value
	}
	if err := resolveCPRAdminKey(account, existing); err != nil {
		return nil, err
	}
	key, err := service.NewCPRQuotaService(h.cfg).EnsureClientKey(ctx, account)
	if err != nil {
		return nil, infraerrors.BadRequest("CPR_SETUP_FAILED", err.Error())
	}
	account.Credentials["api_key"] = key
	return account.Credentials, nil
}

// DiscoverCPR is strictly read-only; remote provisioning happens on save.
func (h *AccountHandler) DiscoverCPR(c *gin.Context) {
	var req struct {
		BaseURL           string `json:"base_url" binding:"required"`
		AdminBaseURL      string `json:"admin_base_url"`
		AdminKey          string `json:"admin_api_key"`
		ExistingAccountID int64  `json:"existing_account_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid CPR connection request")
		return
	}
	account := &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeCPR, Credentials: map[string]any{
		"base_url": cprURL(req.BaseURL), "admin_base_url": cprURL(req.AdminBaseURL), "admin_api_key": strings.TrimSpace(req.AdminKey),
	}}
	var existing *service.Account
	if req.ExistingAccountID > 0 {
		var err error
		existing, err = h.adminService.GetAccount(c.Request.Context(), req.ExistingAccountID)
		if err != nil {
			response.ErrorFrom(c, err)
			return
		}
	}
	if err := resolveCPRAdminKey(account, existing); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	accounts, err := service.NewCPRQuotaService(h.cfg).DiscoverAccounts(c.Request.Context(), account.GetCPRAdminBaseURL(), account.GetCPRAdminAPIKey())
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, accounts)
}
