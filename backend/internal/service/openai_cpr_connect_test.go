//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type cprConnectFixture struct {
	mu             sync.Mutex
	account        map[string]any
	groups         []map[string]any
	keys           []map[string]any
	writes         []string
	lastUpdate     map[string]any
	failUpdateOnce bool
	groupReads     int
	scopeChanged   bool
	server         *httptest.Server
}

func newCPRConnectFixture(t *testing.T) *cprConnectFixture {
	t.Helper()
	f := &cprConnectFixture{
		account: map[string]any{
			"id": "acct_test", "name": "Test account", "email": "test@example.invalid", "provider": "openai",
			"planType": "pro", "status": "normal", "enabled": false, "concurrencyLimit": 16,
			"weight": 3, "notes": "keep me", "modelAccess": map[string]any{"mode": "all"},
			"outboundProxyId": "proxy_keep", "groups": []map[string]any{{"id": "original", "enabled": true}},
		},
		groups: []map[string]any{{"id": "original", "enabled": true, "memberCount": 2}},
		keys:   []map[string]any{{"id": "broad", "enabled": true, "routingScope": "all", "groups": []map[string]any{}}},
	}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("x-api-key") != "test-admin" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":40103,"message":"secret-reflection"}`))
			return
		}
		if r.Method == http.MethodPost {
			f.writes = append(f.writes, r.URL.Path)
		}
		var data any
		switch r.URL.Path {
		case "/api/admin/accounts":
			if r.URL.Query().Get("provider") != "openai" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":40001}`))
				return
			}
			data = map[string]any{"items": []any{f.account, map[string]any{"id": "other", "provider": "xai"}}, "page": map[string]int{"totalPages": 1}}
		case "/api/admin/accounts/detail":
			if r.URL.Query().Get("accountId") != f.account["id"] {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"code":40401}`))
				return
			}
			data = map[string]any{"account": f.account}
		case "/api/admin/account-groups":
			f.groupReads++
			if f.scopeChanged && f.groupReads > 1 {
				for _, group := range f.groups {
					group["memberCount"] = 2
				}
			}
			data = map[string]any{"items": f.groups, "page": map[string]int{"totalPages": 1}}
		case "/api/admin/account-groups/create":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			body["id"], body["enabled"], body["memberCount"] = "dedicated", true, 0
			f.groups = append(f.groups, body)
			data = map[string]any{"id": "dedicated"}
		case "/api/admin/accounts/update":
			if f.failUpdateOnce {
				f.failUpdateOnce = false
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"code":500,"message":"failed"}`))
				return
			}
			_ = json.NewDecoder(r.Body).Decode(&f.lastUpdate)
			groupIDs := f.lastUpdate["groupIds"].([]any)
			refs := []map[string]any{}
			for _, id := range groupIDs {
				refs = append(refs, map[string]any{"id": id, "enabled": true})
			}
			f.account["groups"] = refs
			for _, group := range f.groups {
				if group["id"] == "dedicated" {
					group["memberCount"] = 1
				}
			}
			data = map[string]any{"id": "acct_test"}
		case "/api/admin/client-keys":
			data = map[string]any{"items": f.keys, "nextCursor": nil}
		case "/api/admin/client-keys/create":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			ids := body["groupIds"].([]any)
			f.keys = append(f.keys, map[string]any{"id": "created-key", "enabled": true, "routingScope": "groups", "groups": []map[string]any{{"id": ids[0], "enabled": true}}})
			data = map[string]any{"id": "created-key", "plaintextKey": "test-client"}
		case "/api/admin/client-keys/reveal":
			data = map[string]any{"plaintextKey": "test-client"}
		default:
			t.Errorf("unexpected CPR path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": data})
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *cprConnectFixture) localAccount() *Account {
	return &Account{Platform: PlatformOpenAI, Type: AccountTypeCPR, Credentials: map[string]any{
		"base_url": f.server.URL, "admin_api_key": "test-admin", "cpr_account_id": "acct_test",
	}}
}

func TestCPRConnectDiscoveryIsReadOnlyAndSafe(t *testing.T) {
	f := newCPRConnectFixture(t)
	accounts, err := NewCPRQuotaService(cprTestConfig()).DiscoverAccounts(context.Background(), f.server.URL, "test-admin")
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	require.Equal(t, "acct_test", accounts[0].ID)
	require.Equal(t, 16, *accounts[0].ConcurrencyLimit)
	data, err := json.Marshal(accounts)
	require.NoError(t, err)
	require.NotContains(t, string(data), "test-admin")
	require.NotContains(t, string(data), "test-client")
	require.NotContains(t, string(data), "outboundProxyId")
	require.Empty(t, f.writes)
}

func TestCPRConnectProvisionPreservesSettingsAndRetriesReuse(t *testing.T) {
	f := newCPRConnectFixture(t)
	svc := NewCPRQuotaService(cprTestConfig())
	for range 2 {
		key, err := svc.EnsureClientKey(context.Background(), f.localAccount())
		require.NoError(t, err)
		require.Equal(t, "test-client", key)
	}
	require.Equal(t, []string{"/api/admin/account-groups/create", "/api/admin/accounts/update", "/api/admin/client-keys/create"}, f.writes)
	require.Equal(t, []any{"original", "dedicated"}, f.lastUpdate["groupIds"])
	require.Equal(t, false, f.lastUpdate["enabled"])
	require.EqualValues(t, 16, f.lastUpdate["concurrencyLimit"])
	require.EqualValues(t, 3, f.lastUpdate["weight"])
	require.Equal(t, "keep me", f.lastUpdate["notes"])
	require.Equal(t, "proxy_keep", f.lastUpdate["outboundProxyId"])
	require.Equal(t, map[string]any{"mode": "all"}, f.lastUpdate["modelAccess"])
	require.NotContains(t, f.lastUpdate, "connection")
}

func TestCPRConnectReusesOnlySingleGroupKey(t *testing.T) {
	for _, scope := range []string{"groups", "all", "multiple", "disabled"} {
		t.Run(scope, func(t *testing.T) {
			f := newCPRConnectFixture(t)
			f.groups[0]["memberCount"] = 1
			f.keys = []map[string]any{{"id": "existing", "enabled": scope != "disabled", "routingScope": "groups", "groups": []map[string]any{{"id": "original", "enabled": true}}}}
			if scope == "all" {
				f.keys[0]["routingScope"] = "all"
			}
			if scope == "multiple" {
				f.keys[0]["groups"] = []map[string]any{{"id": "original"}, {"id": "other"}}
			}
			_, err := NewCPRQuotaService(cprTestConfig()).EnsureClientKey(context.Background(), f.localAccount())
			require.NoError(t, err)
			if scope == "groups" {
				require.Empty(t, f.writes)
			} else {
				require.Equal(t, []string{"/api/admin/client-keys/create"}, f.writes)
			}
		})
	}
}

func TestCPRConnectConcurrentSaveReusesResources(t *testing.T) {
	f := newCPRConnectFixture(t)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := NewCPRQuotaService(cprTestConfig()).EnsureClientKey(context.Background(), f.localAccount())
			errs <- err
		}()
	}
	wg.Wait()
	for range 4 {
		require.NoError(t, <-errs)
	}
	require.Len(t, f.writes, 3, fmt.Sprint(f.writes))
}

func TestCPRConnectPartialGroupRecovery(t *testing.T) {
	f := newCPRConnectFixture(t)
	f.failUpdateOnce = true
	f.account["concurrencyLimit"] = nil
	svc := NewCPRQuotaService(cprTestConfig())
	_, err := svc.EnsureClientKey(context.Background(), f.localAccount())
	require.Error(t, err)
	key, err := svc.EnsureClientKey(context.Background(), f.localAccount())
	require.NoError(t, err)
	require.Equal(t, "test-client", key)
	require.Len(t, f.groups, 2, "retry must reuse the managed empty group")
	require.Contains(t, f.lastUpdate, "concurrencyLimit")
	require.Nil(t, f.lastUpdate["concurrencyLimit"])
}

func TestCPRConnectRechecksSingleAccountScopeBeforeSecret(t *testing.T) {
	f := newCPRConnectFixture(t)
	f.groups[0]["memberCount"] = 1
	f.scopeChanged = true
	_, err := NewCPRQuotaService(cprTestConfig()).EnsureClientKey(context.Background(), f.localAccount())
	require.ErrorContains(t, err, "only the selected account")
	require.Empty(t, f.writes, "scope changed: do not create or expand keys")
}

func TestCPRConnectAdminErrorsDoNotReflectSecretsOrFollowRedirects(t *testing.T) {
	f := newCPRConnectFixture(t)
	_, err := NewCPRQuotaService(cprTestConfig()).DiscoverAccounts(context.Background(), f.server.URL, "bad")
	require.ErrorIs(t, err, ErrCPRAdminKeyInvalid)
	require.NotContains(t, err.Error(), "secret-reflection")
	hits := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(200) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer redirect.Close()
	_, err = NewCPRQuotaService(cprTestConfig()).DiscoverAccounts(context.Background(), redirect.URL, "test-admin")
	require.Error(t, err)
	require.Zero(t, hits)
}
