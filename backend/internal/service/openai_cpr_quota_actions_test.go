//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func cprActionsDetail() string {
	detail := strings.Replace(cprTestDetailJSON("normal", "pro", cprTestWindow("secondary", 604800, 2, "")),
		`"enabled":true`, `"enabled":true,"capabilities":{"quotaRefresh":true,"resetCredits":true,"consumeResetCredit":true}`, 1)
	return strings.Replace(detail, `"quota":{`, `"quota":{"credits":{"hasCredits":true,"unlimited":false,"balance":"74080.1165380000"},`, 1)
}

func TestCPRActiveQueryRefreshesUpstream(t *testing.T) {
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/admin/accounts/quota/refresh" {
			refreshes.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{}}`))
			return
		}
		require.Equal(t, "/api/admin/accounts/detail", r.URL.Path)
		_, _ = w.Write([]byte(cprActionsDetail()))
	}))
	defer server.Close()
	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL
	svc := &AccountUsageService{cprQuotaService: NewCPRQuotaService(cprTestConfig())}
	usage, err := svc.getCPRUsage(context.Background(), account, true)
	require.NoError(t, err)
	require.Empty(t, usage.CPR.QueryError)
	require.EqualValues(t, 1, refreshes.Load(), "manual query must refresh CPR's upstream quota, not only re-read its cached detail")
	require.Equal(t, "74080.1165380000", *usage.CPR.Credits.Balance)
}

func cprActionsService(server *httptest.Server) (*AccountUsageService, *cprSnapshotRepo) {
	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL
	account.Status, account.Schedulable = StatusError, false
	repo := &cprSnapshotRepo{account: account}
	return &AccountUsageService{cprQuotaService: NewCPRQuotaService(cprTestConfig()), accountRepo: repo}, repo
}

func TestCPRQuotaActionsRefreshesPointsAndSanitizedResetCredits(t *testing.T) {
	var refreshes, cardQueries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, cprTestAdminKey(), r.Header.Get(cprAdminAPIKeyHeader))
		switch r.URL.Path {
		case "/api/admin/accounts/detail":
			_, _ = w.Write([]byte(cprActionsDetail()))
		case "/api/admin/accounts/quota/refresh":
			require.Equal(t, http.MethodPost, r.Method)
			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, map[string]string{"accountId": cprTestCPRAccount}, body)
			refreshes.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{}}`))
		case "/api/admin/accounts/reset-credits":
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, cprTestCPRAccount, r.URL.Query().Get("accountId"))
			cardQueries.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{"availableCount":1,"credits":[
				{"id":"private-credit-id","status":"available","expiresAt":"2099-10-30T03:01:00+08:00"},
				{"id":"used-credit-id","status":"redeemed","expiresAt":"2099-11-01T00:00:00Z"}]}}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	svc, repo := cprActionsService(server)
	result, err := svc.RefreshCPRQuota(context.Background(), repo.account.ID)
	require.NoError(t, err)
	require.True(t, result.CachePersisted)
	require.True(t, result.CreditsCachePersisted)
	require.Equal(t, "74080.1165380000", *result.Credits.Balance)
	require.Equal(t, 1, result.RateLimitResetCredits.AvailableCount)
	require.Len(t, result.RateLimitResetCredits.Credits, 1)
	require.Equal(t, "2099-10-30T03:01:00+08:00", result.RateLimitResetCredits.Credits[0].ExpiresAt)
	require.Equal(t, 2.0, result.Usage.SevenDay.Utilization)
	require.EqualValues(t, 1, refreshes.Load())
	require.EqualValues(t, 1, cardQueries.Load())
	raw, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "private-credit-id")
	require.NotContains(t, string(raw), "used-credit-id")
	require.NotContains(t, repo.updates, "codex_reset_credit_snapshot")
	require.NotContains(t, repo.updates, "codex_credits_snapshot")
	require.Equal(t, StatusError, repo.account.Status)
	require.False(t, repo.account.Schedulable)
	snapshot := readCPRAccountSnapshot(repo.account)
	require.Equal(t, 1, snapshot.ResetCredits.AvailableCount)
	// Passive list reads use the fresh CPR snapshot without additional upstream
	// refreshes or reset-card queries, retaining the sanitized card inventory.
	usage, err := svc.getCPRUsage(context.Background(), repo.account, false)
	require.NoError(t, err)
	require.Equal(t, 1, usage.CPR.ResetCredits.AvailableCount)
	require.EqualValues(t, 1, refreshes.Load())
	require.EqualValues(t, 1, cardQueries.Load())
}

const cprResetRequestID = "8fbf302d-11df-4bd5-82e4-08e4b3df7874"

func TestCPRQuotaActionsResetRetriesOnlyWithTheSameCallerUUID(t *testing.T) {
	var consumes, refreshes atomic.Int32
	var ids []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/admin/accounts/detail":
			_, _ = w.Write([]byte(cprActionsDetail()))
		case r.URL.Path == "/api/admin/accounts/quota/refresh":
			refreshes.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{}}`))
		case r.URL.Path == "/api/admin/accounts/reset-credits" && r.Method == http.MethodPost:
			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, map[string]string{"accountId": cprTestCPRAccount, "redeemRequestId": cprResetRequestID}, body)
			ids = append(ids, body["redeemRequestId"])
			if consumes.Add(1) == 1 {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"code":50202,"message":"do not reflect admin-secret"}`))
			} else {
				_, _ = w.Write([]byte(`{"code":200,"data":{"code":"already_redeemed"}}`))
			}
		case r.URL.Path == "/api/admin/accounts/reset-credits" && r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"code":200,"data":{"availableCount":0,"credits":[]}}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	svc, repo := cprActionsService(server)
	_, err := svc.ResetCPRQuota(context.Background(), repo.account.ID, cprResetRequestID)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "admin-secret")
	require.EqualValues(t, 1, consumes.Load(), "an unknown result must not be automatically replayed")
	require.Zero(t, refreshes.Load())
	result, err := svc.ResetCPRQuota(context.Background(), repo.account.ID, cprResetRequestID)
	require.NoError(t, err)
	require.Equal(t, "already_redeemed", result.Code)
	require.True(t, result.CacheRefreshed)
	require.Empty(t, result.WarningCode)
	require.Zero(t, result.Quota.RateLimitResetCredits.AvailableCount)
	require.Equal(t, []string{cprResetRequestID, cprResetRequestID}, ids)
	require.EqualValues(t, 1, refreshes.Load())
	require.False(t, repo.account.Schedulable, "CPR reset must not restore a local manually paused account")
	require.Equal(t, StatusError, repo.account.Status)
}

func TestCPRQuotaActionsRejectsBadUUIDAndUnsupportedCapabilities(t *testing.T) {
	var consumes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/admin/accounts/detail", r.URL.Path)
		if r.Method == http.MethodPost {
			consumes.Add(1)
		}
		_, _ = w.Write([]byte(cprTestDetailJSON("normal", "pro", "")))
	}))
	defer server.Close()
	svc, repo := cprActionsService(server)
	for _, id := range []string{"", "invalid", strings.ToUpper(cprResetRequestID), "8fbf302d-11df-1bd5-82e4-08e4b3df7874", "8fbf302d-11df-4bd5-02e4-08e4b3df7874"} {
		_, err := svc.ResetCPRQuota(context.Background(), repo.account.ID, id)
		require.Error(t, err)
	}
	_, err := svc.ResetCPRQuota(context.Background(), repo.account.ID, cprResetRequestID)
	require.Error(t, err)
	result, err := svc.RefreshCPRQuota(context.Background(), repo.account.ID)
	require.NoError(t, err)
	require.Nil(t, result.RateLimitResetCredits, "unsupported inventory is unknown, not zero")
	require.Nil(t, result.Usage.FiveHour)
	require.Nil(t, result.Usage.SevenDay)
	require.Zero(t, consumes.Load())
}

func TestCPRQuotaActionsConfirmedResetDoesNotHidePostQueryFailure(t *testing.T) {
	var consumed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/admin/accounts/reset-credits" && r.Method == http.MethodPost {
			consumed.Store(true)
			_, _ = w.Write([]byte(`{"code":200,"data":{"code":"reset"}}`))
		} else if consumed.Load() {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"code":50301}`))
		} else {
			_, _ = w.Write([]byte(cprActionsDetail()))
		}
	}))
	defer server.Close()
	svc, repo := cprActionsService(server)
	now := time.Now()
	repo.account.Extra[CPRAccountSnapshotExtraKey] = &CPRAccountSnapshot{
		AccountID: cprTestCPRAccount, CheckedAt: &now, QueryAttemptedAt: &now,
		ResetCredits: &OpenAIRateLimitResetCredits{AvailableCount: 1},
	}
	result, err := svc.ResetCPRQuota(context.Background(), repo.account.ID, cprResetRequestID)
	require.NoError(t, err)
	require.Equal(t, "reset", result.Code)
	require.False(t, result.CacheRefreshed)
	require.Nil(t, result.Quota)
	require.Equal(t, "cpr_reset_refresh_failed", result.WarningCode)
	require.False(t, repo.account.Schedulable)
	require.Nil(t, readCPRAccountSnapshot(repo.account).ResetCredits, "a failed readback must not resurrect pre-consumption inventory")
	usage, err := svc.getCPRUsage(context.Background(), repo.account, false)
	require.NoError(t, err)
	require.Nil(t, usage.CPR.ResetCredits, "a remounted card must see unknown inventory, not the old credit")
	require.True(t, usage.CPR.Capabilities.ConsumeResetCredit, "a remounted card must still allow confirming its pending operation")
}

func TestCPRQuotaActionsDoesNotConsumeWhenInventoryInvalidationFails(t *testing.T) {
	var consumes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/admin/accounts/reset-credits" && r.Method == http.MethodPost {
			consumes.Add(1)
			_, _ = w.Write([]byte(`{"code":200,"data":{"code":"reset"}}`))
			return
		}
		_, _ = w.Write([]byte(cprActionsDetail()))
	}))
	defer server.Close()
	svc, repo := cprActionsService(server)
	repo.err = context.DeadlineExceeded
	result, err := svc.ResetCPRQuota(context.Background(), repo.account.ID, cprResetRequestID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Zero(t, consumes.Load(), "do not spend a credit when stale inventory cannot be invalidated")
}

func TestCPRQuotaActionsQueryAndSnapshotWriteFailureCannotReturnStaleValues(t *testing.T) {
	server, _ := startCPRAdminStub(t, http.StatusServiceUnavailable, `{"code":50301}`)
	svc, repo := cprActionsService(server)
	now := time.Now()
	repo.account.Extra[CPRAccountSnapshotExtraKey] = &CPRAccountSnapshot{
		AccountID: cprTestCPRAccount, CheckedAt: &now, QueryAttemptedAt: &now,
		SevenDay: &UsageProgress{Utilization: 21},
	}
	repo.err = context.DeadlineExceeded
	result, err := svc.RefreshCPRQuota(context.Background(), repo.account.ID)
	require.Error(t, err)
	require.Nil(t, result, "the cached quota is not a successful live query")
}

func TestCPRQuotaActionsBindingChangeDoesNotUpdateTheNewAccount(t *testing.T) {
	var repo *cprSnapshotRepo
	var consumes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/admin/accounts/reset-credits" && r.Method == http.MethodPost {
			consumes.Add(1)
			repo.mu.Lock()
			repo.account.Credentials["cpr_account_id"] = "new-cpr-account"
			repo.mu.Unlock()
			_, _ = w.Write([]byte(`{"code":200,"data":{"code":"reset"}}`))
			return
		}
		_, _ = w.Write([]byte(cprActionsDetail()))
	}))
	defer server.Close()
	svc, repository := cprActionsService(server)
	repo = repository
	result, err := svc.ResetCPRQuota(context.Background(), repo.account.ID, cprResetRequestID)
	require.NoError(t, err)
	require.False(t, result.CacheRefreshed)
	require.Equal(t, "cpr_reset_refresh_failed", result.WarningCode)
	require.Nil(t, readCPRAccountSnapshot(repo.account), "the new binding must not read the old binding's invalidated snapshot")
	require.EqualValues(t, 1, consumes.Load())
}

func TestCPRQuotaActionsSnapshotWriteFailureKeepsTheLiveValues(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/admin/accounts/detail":
			_, _ = w.Write([]byte(cprActionsDetail()))
		case "/api/admin/accounts/reset-credits":
			_, _ = w.Write([]byte(`{"code":200,"data":{"availableCount":1,"credits":[{"status":"available","expiresAt":"2099-01-01T00:00:00Z"}]}}`))
		default:
			_, _ = w.Write([]byte(`{"code":200,"data":{}}`))
		}
	}))
	defer server.Close()
	svc, repo := cprActionsService(server)
	repo.err = context.DeadlineExceeded
	result, err := svc.RefreshCPRQuota(context.Background(), repo.account.ID)
	require.NoError(t, err)
	require.False(t, result.CachePersisted)
	require.False(t, result.CreditsCachePersisted)
	require.Equal(t, "74080.1165380000", *result.Credits.Balance)
	require.Equal(t, 1, result.RateLimitResetCredits.AvailableCount)
	require.Equal(t, 2.0, result.Usage.SevenDay.Utilization)
	require.WithinDuration(t, time.Now(), *result.Usage.CPR.CheckedAt, time.Second)
}
