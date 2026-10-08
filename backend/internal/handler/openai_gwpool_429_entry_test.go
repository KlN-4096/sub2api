//go:build unit

package handler

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type gatewayPool429EntryRepo struct{ *grokCredentialHandlerRepo }

func (r gatewayPool429EntryRepo) GetByID(_ context.Context, id int64) (*service.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.missingOnGet[id] {
		return nil, nil
	}
	for _, account := range r.accounts {
		if account.ID == id {
			account.Extra = cloneCredentialMap(account.Extra)
			account.Credentials = cloneCredentialMap(account.Credentials)
			return &account, nil
		}
	}
	return nil, nil
}

func (r gatewayPool429EntryRepo) ListSchedulableByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.selectionCalls++
	var out []service.Account
	for _, account := range r.accounts {
		if account.Platform == platform && account.IsSchedulable() {
			account.Extra = cloneCredentialMap(account.Extra)
			account.Credentials = cloneCredentialMap(account.Credentials)
			out = append(out, account)
		}
	}
	return out, nil
}

func (r gatewayPool429EntryRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, _ int64, platform string) ([]service.Account, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

func (r gatewayPool429EntryRepo) ListSchedulableUngroupedByPlatform(ctx context.Context, platform string) ([]service.Account, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

func (r gatewayPool429EntryRepo) ListByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []service.Account
	for _, account := range r.accounts {
		if account.Platform == platform {
			account.Extra = cloneCredentialMap(account.Extra)
			account.Credentials = cloneCredentialMap(account.Credentials)
			out = append(out, account)
		}
	}
	return out, nil
}

func (r gatewayPool429EntryRepo) FindByExtraField(_ context.Context, key string, value any) ([]service.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []service.Account
	for _, account := range r.accounts {
		if account.Extra[key] == value {
			account.Extra = cloneCredentialMap(account.Extra)
			account.Credentials = cloneCredentialMap(account.Credentials)
			out = append(out, account)
		}
	}
	return out, nil
}

func TestGatewayPool429EntryRetriesSameAccountWithoutInterruptingResponse(t *testing.T) {
	for _, tc := range []struct {
		path, mode  string
		passthrough bool
	}{
		{"/responses", "success", false},
		{"/v1/responses", "success", false},
		{"/responses", "success", true},
		{"/v1/responses", "success", true},
		{"/v1/messages", "success", false},
		{"/v1/chat/completions", "success", false},
		{"/responses", "quota", false},
		{"/responses", "cancel", false},
		{"/responses", "blocked", false},
		{"/responses", "semantic-output", false},
	} {
		t.Run(fmt.Sprintf("%s/%s/passthrough=%t", tc.path, tc.mode, tc.passthrough), func(t *testing.T) {
			payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"host":"unified-142","exp":%d}`, time.Now().Add(time.Hour).Unix())))
			cookie := "__cflb=offline; __oailb=e30." + payload + ".offline"
			sum := sha256.Sum256([]byte(cookie))
			pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/gateways":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"account":  r.URL.Query().Get("account"),
						"gateways": []map[string]any{{"name": "unified-142", "pair_ready": true}},
					})
				case "/cookie":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"gateway": "unified-142", "cookie": cookie, "cookie_version": hex.EncodeToString(sum[:])[:12],
						"valid_for_s": 150, "ttl_is_advisory": true,
					})
				default:
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			defer pool.Close()
			account := codexWireAccount(801, "pool", map[string]any{
				"openai_gwpool": true, "openai_gwpool_base_url": pool.URL,
				"openai_gwpool_consumer_key": "offline", "openai_gwpool_guard_enabled": false,
				"openai_passthrough": tc.passthrough,
			})
			account.GroupIDs = []int64{9001}
			account.Credentials["chatgpt_account_id"] = "offline-account"
			account.Credentials["chatgpt_user_id"] = "offline-user"
			other := codexWireAccount(802, "other", nil)
			other.GroupIDs = []int64{9001}
			accounts := []service.Account{account, other}
			repo := gatewayPool429EntryRepo{&grokCredentialHandlerRepo{accounts: accounts}}
			upstream, router, cleanup := newCodexWireEntry(t, accounts, repo)
			defer cleanup()
			// The retired guard=false setting must not bypass A/B. Business-only
			// failures/callbacks begin after these two successful Luna probes.
			upstream.probeState = "offline-full"
			upstream.sequence = []int{429, 429, 0}
			upstream.errorBody = `{"detail":"Rate limit exceeded"}`
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch tc.mode {
			case "quota":
				upstream.errorBody = `{"error":{"type":"usage_limit_reached","resets_in_seconds":3600}}`
			case "cancel":
				upstream.afterPost = cancel
			case "blocked":
				upstream.afterPost = func() {
					require.NoError(t, repo.SetTempUnschedulable(context.Background(), account.ID, time.Now().Add(time.Minute), "auth"))
				}
			case "semantic-output":
				upstream.sequence = nil
				upstream.streamBody = "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\n" +
					"data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed\",\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"Rate limit exceeded\"}}}\n\n"
			}
			body := codexWireResponsesBody(true)
			if tc.path == "/v1/messages" || tc.path == "/v1/chat/completions" {
				body = `{"model":"gpt-5.4","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`
			}
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(body)).WithContext(ctx)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			calls := upstream.taken()
			require.GreaterOrEqual(t, len(calls), 3)
			for _, probe := range calls[:2] {
				require.Equal(t, account.ID, probe.accountID)
				require.Equal(t, "gpt-6-luna", gjson.GetBytes(probe.body, "model").String())
			}
			calls = calls[2:]
			if tc.mode == "success" {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.Len(t, calls, 3)
				if tc.path == "/responses" || tc.path == "/v1/responses" {
					require.Contains(t, rec.Body.String(), "response.completed")
				}
			} else {
				require.Len(t, calls, 1, "quota, cancellation, blocks and emitted output must prevent replay")
				if tc.mode == "quota" {
					require.Equal(t, http.StatusTooManyRequests, rec.Code)
				}
				if tc.mode == "semantic-output" {
					require.Contains(t, rec.Body.String(), "hello")
				}
			}
			for _, call := range calls {
				require.Equal(t, account.ID, call.accountID, "transient 429 cannot rotate credentials")
				require.Equal(t, "gpt-5.4", gjson.GetBytes(call.body, "model").String(), "business model is unchanged")
			}
		})
	}
}
