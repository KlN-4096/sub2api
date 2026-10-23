package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

const (
	CPRAccountSnapshotExtraKey = "cpr_account_snapshot"
	cprAccountSnapshotTTL      = 10 * time.Minute
)

// CPRAccountSnapshot is a read-only projection, never a local scheduler verdict.
// CPR 3.29.7 exposes quota age as relative display text, not an observed-at
// timestamp. Keep that text separate from our admin-query timestamps.
type CPRAccountSnapshot struct {
	AccountID             string                       `json:"account_id"`
	Name                  string                       `json:"name,omitempty"`
	Email                 string                       `json:"email,omitempty"`
	PlanType              string                       `json:"plan_type,omitempty"`
	Status                string                       `json:"status,omitempty"`
	Enabled               bool                         `json:"enabled"`
	ErrorReason           string                       `json:"error_reason,omitempty"`
	AccessTokenExpiresAt  *time.Time                   `json:"access_token_expires_at,omitempty"`
	RefreshTokenExpiresAt *time.Time                   `json:"refresh_token_expires_at,omitempty"`
	RateLimitedUntil      *time.Time                   `json:"rate_limited_until,omitempty"`
	OutboundProxy         string                       `json:"outbound_proxy,omitempty"`
	QuotaUpdatedDisplay   string                       `json:"quota_updated_display,omitempty"`
	CheckedAt             *time.Time                   `json:"checked_at,omitempty"`
	QueryAttemptedAt      *time.Time                   `json:"query_attempted_at,omitempty"`
	QueryError            string                       `json:"query_error,omitempty"`
	FiveHour              *UsageProgress               `json:"five_hour,omitempty"`
	SevenDay              *UsageProgress               `json:"seven_day,omitempty"`
	Credits               *OpenAICredits               `json:"credits,omitempty"`
	ResetCredits          *OpenAIRateLimitResetCredits `json:"reset_credits,omitempty"`
	Capabilities          CPRAccountCapabilities       `json:"capabilities"`
}

func readCPRAccountSnapshot(account *Account) *CPRAccountSnapshot {
	if account == nil {
		return nil
	}
	raw, ok := account.Extra[CPRAccountSnapshotExtraKey]
	if !ok {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var snapshot CPRAccountSnapshot
	if json.Unmarshal(data, &snapshot) != nil || snapshot.AccountID != account.GetCPRAccountID() {
		return nil
	}
	return &snapshot
}

func cprSnapshotFromState(state *CPRAccountState) *CPRAccountSnapshot {
	snapshot := &CPRAccountSnapshot{
		AccountID: state.AccountID, Name: state.Name, Email: state.Email,
		PlanType: state.PlanType, Status: state.Status, Enabled: state.Enabled,
		ErrorReason: state.ErrorReason, AccessTokenExpiresAt: state.AccessTokenExpiresAt,
		RefreshTokenExpiresAt: state.RefreshTokenExpiresAt, RateLimitedUntil: state.RateLimitedUntil,
		OutboundProxy: state.OutboundProxyEndpoint, QuotaUpdatedDisplay: state.QuotaUpdatedDisplay,
		CheckedAt: &state.FetchedAt, QueryAttemptedAt: &state.FetchedAt,
		Credits: state.Credits, Capabilities: state.Capabilities,
	}
	if state.RateLimit != nil {
		for _, window := range []*OpenAIRateLimitWindow{state.RateLimit.PrimaryWindow, state.RateLimit.SecondaryWindow} {
			if window == nil {
				continue
			}
			progress := &UsageProgress{Utilization: window.UsedPercent}
			if window.ResetAt > 0 {
				resetAt := time.Unix(window.ResetAt, 0)
				progress.ResetsAt = &resetAt
				progress.RemainingSeconds = max(0, int(resetAt.Sub(state.FetchedAt).Seconds()))
			}
			if window.LimitWindowSeconds < int64((24*time.Hour)/time.Second) {
				snapshot.FiveHour = progress
			} else {
				snapshot.SevenDay = progress
			}
		}
	}
	return snapshot
}

func cprSnapshotQueryError(err error) string {
	switch {
	case errors.Is(err, ErrCPRAdminKeyInvalid):
		return "admin_key_invalid"
	case errors.Is(err, ErrCPRAccountMissing):
		return "account_missing"
	case errors.Is(err, ErrCPRNotConfigured):
		return "not_configured"
	default:
		return "query_failed"
	}
}

func (s *AccountUsageService) getCPRUsage(ctx context.Context, account *Account, force bool) (*UsageInfo, error) {
	// Serialize the fresh read/query/write, so a late failed query cannot replace
	// a concurrent success with an older snapshot. This is process-local, like
	// the usage cache; it does not claim cross-instance CAS semantics.
	defer s.lockCPRSnapshot(account.ID)()
	return s.getCPRUsageLocked(ctx, account, force)
}

// lockCPRSnapshot 锁住一个账号的 CPR 快照读写，返回解锁函数。
func (s *AccountUsageService) lockCPRSnapshot(id int64) func() {
	raw, _ := s.cprSnapshotLocks.LoadOrStore(id, &sync.Mutex{})
	mu, _ := raw.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// The caller holds cprSnapshotLocks for this local account.
func (s *AccountUsageService) getCPRUsageLocked(ctx context.Context, account *Account, force bool) (*UsageInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.accountRepo != nil && account.ID > 0 {
		fresh, err := s.accountRepo.GetByID(ctx, account.ID)
		if err != nil {
			return nil, err
		}
		if fresh == nil || !fresh.IsCPR() {
			return nil, ErrCPRNotConfigured
		}
		if cprQuotaBinding(account) != cprQuotaBinding(fresh) {
			return nil, ErrCPRNotConfigured
		}
		account = fresh
	}
	account = snapshotOpenAIOutboundAccount(account)
	now := time.Now()
	snapshot := readCPRAccountSnapshot(account)
	if force || snapshot == nil || snapshot.QueryAttemptedAt == nil || now.Sub(*snapshot.QueryAttemptedAt) >= cprAccountSnapshotTTL {
		var state *CPRAccountState
		var err error
		if s.cprQuotaService == nil {
			err = ErrCPRNotConfigured
		} else {
			state, err = s.cprQuotaService.FetchAccountState(ctx, account)
			if err == nil && state != nil && force && state.Capabilities.QuotaRefresh {
				err = s.cprQuotaService.RefreshQuota(ctx, account)
				if err == nil {
					state, err = s.cprQuotaService.FetchAccountState(ctx, account)
				}
			}
		}
		updates := make(map[string]any)
		if err != nil || state == nil {
			if snapshot == nil {
				snapshot = &CPRAccountSnapshot{AccountID: account.GetCPRAccountID()}
			}
			snapshot.QueryAttemptedAt, snapshot.QueryError = &now, cprSnapshotQueryError(err)
		} else {
			previous := snapshot
			snapshot = cprSnapshotFromState(state)
			if previous != nil && state.Capabilities.ResetCredits {
				snapshot.ResetCredits = previous.ResetCredits
			}
			// Legacy metadata consumers still read plan/proxy keys. Quota windows
			// belong only in the CPR display projection, not scheduler extras.
			metadata := *state
			metadata.RateLimit = nil
			for key, value := range buildCPRCodexExtraUpdates(account, &metadata) {
				updates[key] = value
			}
			// This is not an upstream observation timestamp. New CPR UI reads the
			// explicit source-age text; never mark old quota as freshly observed.
			updates["codex_usage_updated_at"] = nil
		}
		updates[CPRAccountSnapshotExtraKey] = snapshot
		// Persist before adding request-local window statistics; do not run the
		// OAuth auto-reset hook for a read-only CPR projection.
		if s.accountRepo != nil {
			if err := s.writeCPRQuotaExtra(ctx, account, updates); err != nil && snapshot.QueryError == "" {
				snapshot.QueryError = "snapshot_save_failed"
			}
		}
	}
	usage := &UsageInfo{
		CPR: snapshot, UpdatedAt: snapshot.QueryAttemptedAt,
		FiveHour: snapshot.FiveHour, SevenDay: snapshot.SevenDay,
	}
	// No quota window means unknown, not 0%. Local accounting cannot manufacture
	// an upstream window or reset an old percentage to zero.
	if s.usageLogRepo != nil {
		for _, window := range []struct {
			progress *UsageProgress
			duration time.Duration
		}{{usage.FiveHour, 5 * time.Hour}, {usage.SevenDay, 7 * 24 * time.Hour}} {
			if window.progress == nil {
				continue
			}
			if stats, err := s.usageLogRepo.GetAccountWindowStats(ctx, account.ID, codexWindowStatsStart(window.progress, window.duration, now)); err == nil {
				window.progress.WindowStats = windowStatsFromAccountStats(stats)
			}
		}
	}
	return usage, nil
}
