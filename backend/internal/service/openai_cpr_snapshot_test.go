//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cprSnapshotRepo struct {
	AccountRepository
	mu      sync.Mutex
	account *Account
	updates map[string]any
	err     error
}

func (r *cprSnapshotRepo) GetByID(_ context.Context, _ int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.account == nil {
		return nil, ErrCPRNotConfigured
	}
	copy := *r.account
	copy.Extra = make(map[string]any)
	for key, value := range r.account.Extra {
		copy.Extra[key] = value
	}
	return &copy, nil
}

func (r *cprSnapshotRepo) UpdateExtra(_ context.Context, _ int64, updates map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, err := json.Marshal(updates)
	if err != nil {
		return err
	}
	_ = json.Unmarshal(data, &r.updates)
	if r.err == nil && r.account != nil {
		for key, value := range r.updates {
			r.account.Extra[key] = value
		}
	}
	return r.err
}

func TestCPRSnapshotProjectsMetadataWithoutInventingQuota(t *testing.T) {
	payload := cprTestDetailJSON("normal", "pro", "")
	server, _ := startCPRAdminStub(t, http.StatusOK, payload)
	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL
	account.Status, account.ErrorMessage = StatusError, "local-error-must-remain"
	repo := &cprSnapshotRepo{account: snapshotOpenAIOutboundAccount(account)}
	svc := &AccountUsageService{cprQuotaService: NewCPRQuotaService(cprTestConfig()), accountRepo: repo}
	usage, err := svc.getUsageForAccount(context.Background(), account, true)
	require.NoError(t, err)
	require.NotNil(t, usage.CPR)
	require.Equal(t, "a@b.c", usage.CPR.Email)
	require.Equal(t, "normal", usage.CPR.Status)
	require.Equal(t, "3 分钟前", usage.CPR.QuotaUpdatedDisplay)
	require.Equal(t, cprTestOutboundProxyView, usage.CPR.OutboundProxy)
	require.NotNil(t, usage.CPR.CheckedAt)
	require.Nil(t, usage.FiveHour)
	require.Nil(t, usage.SevenDay)
	require.Contains(t, repo.updates, CPRAccountSnapshotExtraKey)
	require.Nil(t, repo.updates["codex_usage_updated_at"])
	require.Equal(t, StatusError, account.Status)
	require.Equal(t, "local-error-must-remain", account.ErrorMessage)
}

func TestCPRSnapshotFailurePreservesOldObservationAndShowsError(t *testing.T) {
	server, _ := startCPRAdminStub(t, http.StatusUnauthorized, `{"code":40103,"message":"do not expose"}`)
	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL
	checked := time.Now().Add(-24 * time.Hour)
	original := &CPRAccountSnapshot{AccountID: account.GetCPRAccountID(), Status: "normal", Enabled: true, CheckedAt: &checked, QueryAttemptedAt: &checked, FiveHour: &UsageProgress{Utilization: 43}}
	account.Extra[CPRAccountSnapshotExtraKey] = original
	svc := &AccountUsageService{cprQuotaService: NewCPRQuotaService(cprTestConfig())}
	usage, err := svc.getUsageForAccount(context.Background(), account, true)
	require.NoError(t, err)
	require.Equal(t, "admin_key_invalid", usage.CPR.QueryError)
	require.True(t, checked.Equal(*usage.CPR.CheckedAt))
	require.True(t, usage.CPR.QueryAttemptedAt.After(checked))
	require.Equal(t, float64(43), usage.FiveHour.Utilization)
	require.Empty(t, original.QueryError, "request-local mutation must not change an old snapshot")
}

func TestCPRSnapshotCacheIsBoundToCPRAccountID(t *testing.T) {
	account := newCPRTestAccount()
	now := time.Now()
	account.Extra[CPRAccountSnapshotExtraKey] = &CPRAccountSnapshot{AccountID: "old-account", CheckedAt: &now, QueryAttemptedAt: &now, Email: "old@example.invalid"}
	usage, err := (&AccountUsageService{}).getCPRUsage(context.Background(), account, false)
	require.NoError(t, err)
	require.Equal(t, account.GetCPRAccountID(), usage.CPR.AccountID)
	require.Empty(t, usage.CPR.Email)
	require.Equal(t, "not_configured", usage.CPR.QueryError)
}

func TestCPRSnapshotPersistenceFailureIsVisible(t *testing.T) {
	server, _ := startCPRAdminStub(t, http.StatusOK, cprTestDetailJSON("normal", "pro", ""))
	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL
	svc := &AccountUsageService{cprQuotaService: NewCPRQuotaService(cprTestConfig()), accountRepo: &cprSnapshotRepo{account: snapshotOpenAIOutboundAccount(account), err: errors.New("db unavailable")}}
	usage, err := svc.getCPRUsage(context.Background(), account, true)
	require.NoError(t, err)
	require.Equal(t, "snapshot_save_failed", usage.CPR.QueryError)
}

func TestCPRSnapshotRFC3339ExpiryAndLimit(t *testing.T) {
	at := "2026-10-31T15:34:21+08:00"
	view := &cprAccountView{ID: "acct", AccessTokenExpiresAt: at, RefreshTokenExpiresAt: at, Quota: cprAccountQuota{RateLimitedUntil: at}}
	state := buildCPRAccountState(view, time.Now())
	require.NotNil(t, state.AccessTokenExpiresAt)
	require.NotNil(t, state.RefreshTokenExpiresAt)
	require.NotNil(t, state.RateLimitedUntil)
	require.Equal(t, state.AccessTokenExpiresAt.Unix(), state.RateLimitedUntil.Unix())
}

func TestCPRSnapshotCannotBecomeLocalQuotaSchedulingVerdict(t *testing.T) {
	now := time.Now()
	account := newCPRTestAccount()
	account.Extra = map[string]any{
		"codex_5h_used_percent": 99.0, "codex_7d_used_percent": 99.0,
		"codex_5h_reset_at":       now.Add(time.Hour).Format(time.RFC3339),
		"codex_7d_reset_at":       now.Add(24 * time.Hour).Format(time.RFC3339),
		"auto_pause_5h_threshold": 0.8, "codex_usage_updated_at": nil,
	}
	paused, _ := shouldAutoPauseOpenAIAccountByQuota(context.Background(), account)
	require.False(t, paused)
	require.False(t, EvaluateAccountSchedulingThreshold(account, map[string]int{PlatformOpenAI: 80}, now).ShouldPause)
	require.Equal(t, openAIQuotaHeadroomNeutralFactor, openAIQuotaHeadroomFactor(account, now))
	// The exact same quota must still gate direct OAuth accounts.
	account.Type = AccountTypeOAuth
	paused, _ = shouldAutoPauseOpenAIAccountByQuota(context.Background(), account)
	require.True(t, paused)
	require.True(t, EvaluateAccountSchedulingThreshold(account, map[string]int{PlatformOpenAI: 80}, now).ShouldPause)
}

func TestCPRSnapshotConcurrentFailureKeepsLatestSuccessfulValue(t *testing.T) {
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			_, _ = w.Write([]byte(cprTestDetailJSON("normal", "pro", cprTestWindow("primary", 18000, 55, ""))))
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":40103}`))
		}
	}))
	defer server.Close()
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL
	old := time.Now().Add(-time.Hour)
	account.Extra[CPRAccountSnapshotExtraKey] = &CPRAccountSnapshot{AccountID: account.GetCPRAccountID(), CheckedAt: &old, QueryAttemptedAt: &old, FiveHour: &UsageProgress{Utilization: 1}}
	repo := &cprSnapshotRepo{account: snapshotOpenAIOutboundAccount(account)}
	svc := &AccountUsageService{cprQuotaService: NewCPRQuotaService(cprTestConfig()), accountRepo: repo}
	results := make(chan *UsageInfo, 2)
	errs := make(chan error, 2)
	query := func() {
		usage, err := svc.getCPRUsage(context.Background(), account, true)
		results <- usage
		errs <- err
	}
	go query()
	<-entered
	go query() // Carries the original, now stale account snapshot.
	time.Sleep(30 * time.Millisecond)
	require.EqualValues(t, 1, calls.Load(), "second query must wait for the first read/query/write")
	unblock()
	for range 2 {
		require.NoError(t, <-errs)
		usage := <-results
		require.Equal(t, float64(55), usage.FiveHour.Utilization)
	}
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	snapshot := readCPRAccountSnapshot(fresh)
	require.Equal(t, float64(55), snapshot.FiveHour.Utilization)
	require.Equal(t, "admin_key_invalid", snapshot.QueryError)
	require.True(t, snapshot.CheckedAt.After(old))
}
