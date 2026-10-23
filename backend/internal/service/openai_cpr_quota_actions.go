package service

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/google/uuid"
)

const cprPostResetTimeout = 30 * time.Second

// These responses share the OAuth card's display contract, not its credential,
// scheduling, cache keys, or post-reset account-recovery workflow.
type CPRQuotaRefreshResult struct {
	OpenAIQuotaUsage
	Usage                 *UsageInfo `json:"usage"`
	CachePersisted        bool       `json:"cache_persisted"`
	CreditsCachePersisted bool       `json:"credits_cache_persisted"`
}

type CPRQuotaResetActionResult struct {
	OpenAIQuotaResetResult
	Quota                 *CPRQuotaRefreshResult `json:"quota,omitempty"`
	CacheRefreshed        bool                   `json:"cache_refreshed"`
	AccountStateRecovered bool                   `json:"account_state_recovered"`
	WarningCode           string                 `json:"warning_code,omitempty"`
}

func cprQuotaBinding(account *Account) string {
	if account == nil || !account.IsCPR() {
		return ""
	}
	return strings.TrimRight(account.GetCPRAdminBaseURL(), "/") + "\n" + account.GetCPRAccountID()
}

func (s *AccountUsageService) cprActionAccount(ctx context.Context, id int64) (*Account, error) {
	if s.accountRepo == nil || s.cprQuotaService == nil {
		return nil, infraerrors.New(http.StatusServiceUnavailable, "CPR_QUOTA_UNAVAILABLE", "CPR quota service is unavailable")
	}
	account, err := s.accountRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if account == nil || !account.IsCPR() {
		return nil, infraerrors.BadRequest("INVALID_CPR_ACCOUNT", "CPR quota operations require an OpenAI CPR account")
	}
	return snapshotOpenAIOutboundAccount(account), nil
}

func (s *AccountUsageService) writeCPRQuotaExtra(ctx context.Context, account *Account, updates map[string]any) error {
	fresh, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil {
		return err
	}
	if cprQuotaBinding(account) == "" || cprQuotaBinding(fresh) != cprQuotaBinding(account) {
		return errors.New("CPR account binding changed")
	}
	return s.accountRepo.UpdateExtra(ctx, account.ID, updates)
}

func (s *CPRQuotaService) RefreshQuota(ctx context.Context, account *Account) error {
	return s.adminJSON(ctx, account.GetCPRAdminBaseURL(), account.GetCPRAdminAPIKey(),
		http.MethodPost, "/api/admin/accounts/quota/refresh", nil,
		map[string]string{"accountId": account.GetCPRAccountID()}, nil)
}

func (s *CPRQuotaService) QueryResetCredits(ctx context.Context, account *Account) (*OpenAIRateLimitResetCredits, error) {
	var data struct {
		AvailableCount *int `json:"availableCount"`
		Credits        []struct {
			Status    string `json:"status"`
			ExpiresAt string `json:"expiresAt"`
		} `json:"credits"`
	}
	err := s.adminJSON(ctx, account.GetCPRAdminBaseURL(), account.GetCPRAdminAPIKey(),
		http.MethodGet, "/api/admin/accounts/reset-credits",
		url.Values{"accountId": {account.GetCPRAccountID()}}, nil, &data)
	if err != nil {
		return nil, err
	}
	if data.AvailableCount == nil || *data.AvailableCount < 0 {
		return nil, errors.New("invalid CPR reset-credit count")
	}
	result := &OpenAIRateLimitResetCredits{AvailableCount: *data.AvailableCount}
	for _, credit := range data.Credits {
		if credit.Status != "available" {
			continue
		}
		detail := OpenAIRateLimitResetCreditDetail{}
		if expires, err := time.Parse(time.RFC3339Nano, credit.ExpiresAt); err == nil {
			detail.ExpiresAt = expires.Format(time.RFC3339Nano)
		}
		result.Credits = append(result.Credits, detail)
	}
	return result, nil
}

func (s *CPRQuotaService) ConsumeResetCredit(ctx context.Context, account *Account, requestID string) (string, error) {
	var data struct {
		Code string `json:"code"`
	}
	err := s.adminJSON(ctx, account.GetCPRAdminBaseURL(), account.GetCPRAdminAPIKey(),
		http.MethodPost, "/api/admin/accounts/reset-credits", nil,
		map[string]string{"accountId": account.GetCPRAccountID(), "redeemRequestId": requestID}, &data)
	return data.Code, err
}

func (s *AccountUsageService) RefreshCPRQuota(ctx context.Context, id int64) (*CPRQuotaRefreshResult, error) {
	defer s.lockCPRSnapshot(id)()
	account, err := s.cprActionAccount(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.refreshCPRQuotaLocked(ctx, account)
}

func (s *AccountUsageService) refreshCPRQuotaLocked(ctx context.Context, account *Account) (*CPRQuotaRefreshResult, error) {
	usage, err := s.getCPRUsageLocked(ctx, account, true)
	if err != nil || usage == nil || usage.CPR == nil ||
		(usage.CPR.QueryError != "" && usage.CPR.QueryError != "snapshot_save_failed") {
		return nil, infraerrors.New(http.StatusBadGateway, "CPR_QUOTA_QUERY_FAILED", "CPR quota query failed")
	}
	snapshot := usage.CPR
	if snapshot.Capabilities.ResetCredits {
		credits, err := s.cprQuotaService.QueryResetCredits(ctx, account)
		if err != nil {
			return nil, infraerrors.New(http.StatusBadGateway, "CPR_RESET_CREDITS_QUERY_FAILED", "CPR reset-credit query failed")
		}
		snapshot.ResetCredits = credits
	}
	// Window statistics are request-local. Do not persist usage-log aggregates
	// or turn the CPR observation into OAuth scheduling state.
	persisted := *snapshot
	persisted.QueryError = ""
	persisted.FiveHour = cloneCPRProgressWithoutStats(snapshot.FiveHour)
	persisted.SevenDay = cloneCPRProgressWithoutStats(snapshot.SevenDay)
	if err := s.writeCPRQuotaExtra(ctx, account, map[string]any{CPRAccountSnapshotExtraKey: &persisted}); err != nil {
		snapshot.QueryError = "snapshot_save_failed"
	} else {
		snapshot.QueryError = ""
	}
	cacheOK := snapshot.QueryError == ""
	return &CPRQuotaRefreshResult{
		OpenAIQuotaUsage: OpenAIQuotaUsage{
			Credits: snapshot.Credits, RateLimitResetCredits: snapshot.ResetCredits,
			FetchedAt: time.Now().Unix(),
		},
		Usage: usage, CachePersisted: cacheOK, CreditsCachePersisted: cacheOK,
	}, nil
}

func cloneCPRProgressWithoutStats(progress *UsageProgress) *UsageProgress {
	if progress == nil {
		return nil
	}
	copy := *progress
	copy.WindowStats = nil
	return &copy
}

func (s *AccountUsageService) ResetCPRQuota(ctx context.Context, id int64, requestID string) (*CPRQuotaResetActionResult, error) {
	parsed, err := uuid.Parse(requestID)
	if err != nil || parsed.Version() != 4 || parsed.Variant() != uuid.RFC4122 || parsed.String() != requestID {
		return nil, infraerrors.BadRequest("CPR_RESET_INVALID_REQUEST_ID", "A canonical UUIDv4 redeem request ID is required")
	}
	defer s.lockCPRSnapshot(id)()
	account, err := s.cprActionAccount(ctx, id)
	if err != nil {
		return nil, err
	}
	state, err := s.cprQuotaService.FetchAccountState(ctx, account)
	if err != nil {
		return nil, infraerrors.New(http.StatusBadGateway, "CPR_QUOTA_QUERY_FAILED", "CPR quota query failed")
	}
	if !state.Capabilities.ConsumeResetCredit {
		return nil, infraerrors.BadRequest("CPR_RESET_UNSUPPORTED", "This CPR account does not support reset credits")
	}
	// Invalidate the old inventory durably before a non-refundable operation.
	// An unknown outcome or a failed readback must stay unknown after remount.
	snapshot := readCPRAccountSnapshot(account)
	if snapshot == nil {
		snapshot = cprSnapshotFromState(state)
	}
	snapshot.Capabilities = state.Capabilities
	snapshot.ResetCredits = nil
	snapshot.FiveHour = cloneCPRProgressWithoutStats(snapshot.FiveHour)
	snapshot.SevenDay = cloneCPRProgressWithoutStats(snapshot.SevenDay)
	if err := s.writeCPRQuotaExtra(ctx, account, map[string]any{CPRAccountSnapshotExtraKey: snapshot}); err != nil {
		return nil, infraerrors.New(http.StatusServiceUnavailable, "CPR_RESET_CACHE_INVALIDATION_FAILED", "CPR reset-credit inventory could not be invalidated")
	}
	code, err := s.cprQuotaService.ConsumeResetCredit(ctx, account, requestID)
	if err != nil || (code != "reset" && code != "already_redeemed" && code != "nothing_to_reset") {
		// Never retry consumption or replace its UUID here. The caller keeps the
		// same UUID across uncertain outcomes; CPR owns upstream idempotency.
		return nil, infraerrors.New(http.StatusBadGateway, "CPR_RESET_RESULT_UNKNOWN", "CPR reset result could not be confirmed; retry the same operation")
	}
	result := &CPRQuotaResetActionResult{OpenAIQuotaResetResult: OpenAIQuotaResetResult{Code: code}}
	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cprPostResetTimeout)
	defer cancel()
	result.Quota, err = s.refreshCPRQuotaLocked(postCtx, account)
	if err != nil {
		result.WarningCode = "cpr_reset_refresh_failed"
		return result, nil
	}
	result.CacheRefreshed = true
	if !result.Quota.CachePersisted {
		result.WarningCode = "cpr_snapshot_save_failed"
	}
	if code == "nothing_to_reset" {
		result.WarningCode = "cpr_reset_not_applied"
	}
	return result, nil
}
