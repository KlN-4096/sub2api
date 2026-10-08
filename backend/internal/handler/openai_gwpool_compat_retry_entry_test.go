//go:build unit

package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGatewayPoolCompatRetryEntryPreservesIdentityAndFinalResponse(t *testing.T) {
	for _, tc := range []struct {
		name, path                     string
		stream, refreshed, passthrough bool
	}{
		{name: "messages-stream", path: "/v1/messages", stream: true},
		{name: "messages-buffered", path: "/v1/messages"},
		{name: "chat-buffered", path: "/v1/chat/completions"},
		{name: "responses-all-refreshed", path: "/v1/responses", stream: true, refreshed: true},
		{name: "passthrough-all-refreshed", path: "/v1/responses", stream: true, refreshed: true, passthrough: true},
		{name: "messages-stream-all-refreshed", path: "/v1/messages", stream: true, refreshed: true},
		{name: "messages-buffered-all-refreshed", path: "/v1/messages", refreshed: true},
		{name: "chat-buffered-all-refreshed", path: "/v1/chat/completions", refreshed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tickets atomic.Int64
			pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/gateways":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"account": r.URL.Query().Get("account"),
						"gateways": []map[string]any{
							{"name": "unified-142", "pair_ready": true},
							{"name": "unified-143", "pair_ready": true},
						},
					})
				case "/cookie":
					gateway := fmt.Sprintf("unified-%d", 141+tickets.Add(1))
					payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(
						`{"host":%q,"exp":%d}`, gateway, time.Now().Add(time.Hour).Unix())))
					cookie := "__cflb=offline; __oailb=e30." + payload + ".offline"
					sum := sha256.Sum256([]byte(cookie))
					_ = json.NewEncoder(w).Encode(map[string]any{
						"gateway": gateway, "cookie": cookie, "cookie_version": hex.EncodeToString(sum[:])[:12],
						"valid_for_s": 150, "ttl_is_advisory": true,
					})
				default:
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			defer pool.Close()
			account := codexWireAccount(801, "pool", codexWireConverged)
			account.GroupIDs = []int64{9001}
			account.Extra["openai_gwpool"] = true
			account.Extra["openai_gwpool_base_url"] = pool.URL
			account.Extra["openai_gwpool_consumer_key"] = "offline"
			account.Extra["openai_passthrough"] = tc.passthrough
			account.Credentials["chatgpt_account_id"] = "offline-account"
			account.Credentials["chatgpt_user_id"] = "offline-user"
			accounts := []service.Account{account}
			repo := gatewayPool429EntryRepo{&grokCredentialHandlerRepo{accounts: accounts}}
			usage := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 16)}
			upstream, router, cleanup := newCodexWireEntryWithUsage(t, accounts, usage, repo)
			defer cleanup()
			if tc.refreshed {
				upstream.afterRequest = func(c *gin.Context) {
					events, _ := c.Get(service.OpsUpstreamErrorsKey)
					message, _ := c.Get(service.OpsUpstreamErrorMessageKey)
					require.Empty(t, events, "recovered degradation is not a final Ops request error")
					require.Empty(t, message)
				}
			}
			response := func(body, state string) *http.Response {
				headers := http.Header{"Content-Type": []string{"text/event-stream"}}
				if state != "" {
					headers.Set("x-codex-turn-state", state)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: io.NopCloser(strings.NewReader(body))}
			}
			failed := "data: [DONE]\n\n"
			if tc.stream {
				failed = "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"temporary\"}}}\n\n"
			}
			success := "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_ok\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
				"data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
				"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"answer\"}\n\n" +
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"status\":\"completed\",\"model\":\"gpt-5.4\",\"output\":[{\"id\":\"msg_ok\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
			upstream.postResponses = []*http.Response{
				response("", "warm-first"), response("", "warm-first"), response(failed, ""),
				response("", "warm-next"), response("", "warm-next"), response(success, ""),
				response(success, ""), // a genuinely new logical request
			}
			wantCalls := 6
			if tc.refreshed {
				upstream.postResponses = []*http.Response{
					response("", "warm-first"), response("", "warm-first"),
					response("data: {\"type\":\"response.output_text.delta\",\"delta\":\"must-not-deliver\"}\n\n", "new-1"),
					response("", "new-2"), response("", "new-3"), response("", "new-4"),
					response("", "warm-next"), response("", "warm-next"), response(success, ""),
					response(success, ""),
				}
				wantCalls = 9
			}
			body := fmt.Sprintf(`{"model":"gpt-5.4","stream":%t,"max_tokens":32,"messages":[{"role":"user","content":"hello"}],"metadata":{"user_id":"user_%s_account__session_stable-session"}}`, tc.stream, strings.Repeat("a", 64))
			if tc.path == "/v1/responses" {
				body = codexWireResponsesBody(tc.stream)
			}
			send := func() *httptest.ResponseRecorder {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(body)).WithContext(ctx)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				return rec
			}
			rec := send()
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "answer")
			require.NotContains(t, rec.Body.String(), "temporary")
			require.NotContains(t, rec.Body.String(), "Upstream stream ended")
			require.NotContains(t, rec.Body.String(), "must-not-deliver")
			require.NotContains(t, rec.Body.String(), "All gateway echo confirmations refreshed")
			calls := upstream.taken()
			require.Len(t, calls, wantCalls, "two tickets, each A/B verified; degradation adds three confirmations")
			first, retry := calls[2], calls[wantCalls-1]
			require.Equal(t, account.ID, first.accountID)
			require.Equal(t, first.accountID, retry.accountID)
			require.True(t, bytes.Equal(first.body, retry.body), "outer retry must preserve the final business bytes")
			for _, key := range []string{"Authorization", "session-id", "thread-id", "x-client-request-id", "x-codex-window-id", "x-codex-turn-metadata"} {
				require.Equal(t, first.header.Get(key), retry.header.Get(key), key)
			}
			require.NotEqual(t, first.header.Get("Cookie"), retry.header.Get("Cookie"))
			if tc.refreshed {
				for _, index := range []int{0, 1, 3, 4, 5, 6, 7} {
					require.Equal(t, "gpt-6-luna", gjson.GetBytes(calls[index].body, "model").String())
				}
				degraded, successful := 0, 0
				requestIDs := map[string]struct{}{}
				for range 2 {
					select {
					case row := <-usage.created:
						require.NotEmpty(t, row.RequestID)
						requestIDs[row.RequestID] = struct{}{}
						if row.RequestType == service.RequestTypeGatewayPoolDegraded {
							degraded++
							require.Zero(t, row.InputTokens)
							require.Zero(t, row.OutputTokens)
							require.Zero(t, row.TotalCost)
							require.NotNil(t, row.RoutePairPoolGateway)
							require.Equal(t, "unified-142", *row.RoutePairPoolGateway)
						} else {
							successful++
							wantType := service.RequestTypeSync
							if tc.stream {
								wantType = service.RequestTypeStream
							}
							require.Equal(t, wantType, row.EffectiveRequestType(), "normal usage may reach the repository with legacy stream fields")
							require.Equal(t, 1, row.InputTokens)
							require.Equal(t, 1, row.OutputTokens)
							require.False(t, strings.HasPrefix(row.RequestID, "gwpool_degraded:"))
							require.NotNil(t, row.RoutePairPoolGateway)
							require.Equal(t, "unified-143", *row.RoutePairPoolGateway)
						}
					case <-time.After(2 * time.Second):
						t.Fatal("expected successful business usage and separate discarded-attempt audit")
					}
				}
				require.Equal(t, 1, degraded, "preserve degradation evidence instead of hiding errors")
				require.Equal(t, 1, successful)
				require.Len(t, requestIDs, 2, "discarded and successful usage cannot collide")
			}
			if tc.path == "/v1/messages" {
				require.NotEmpty(t, gjson.GetBytes(first.body, "client_metadata.turn_id").String())
				require.Equal(t, http.StatusOK, send().Code)
				next := upstream.taken()
				require.Len(t, next, wantCalls+1)
				require.NotEqual(t, gjson.GetBytes(first.body, "client_metadata.turn_id").String(),
					gjson.GetBytes(next[wantCalls].body, "client_metadata.turn_id").String(), "new logical requests need new turn identities")
			}
		})
	}
}
