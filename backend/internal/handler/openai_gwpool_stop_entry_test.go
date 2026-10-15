//go:build unit

package handler

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

const gatewayStopChatBody = `{"model":"gpt-5.4","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`

const gatewayStopSuccess = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\n" +
	"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"status\":\"completed\",\"model\":\"gpt-5.4\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"

type gatewayStopPool struct {
	gateways, cookies atomic.Int32
	uncached          atomic.Bool // lists without a lifetime are never reused
}

// newGatewayStopEntryWith lists ready(n) gateways on the n-th (1-based)
// directory request, five when ready is nil.
func newGatewayStopEntryWith(t *testing.T, ids []int64, ready func(call int32) int, extra map[string]any,
	configure func(*config.Config)) (*codexWireUpstream, *gin.Engine, gatewayPool429EntryRepo, []service.Account, *gatewayStopPool) {
	t.Helper()
	counts := &gatewayStopPool{}
	pool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/gateways":
			n := 5
			if call := counts.gateways.Add(1); ready != nil {
				n = ready(call)
			}
			gateways := make([]map[string]any, 0, n)
			for i := 0; i < n; i++ {
				gateway := map[string]any{"name": fmt.Sprintf("unified-%d", i), "pair_ready": true}
				if !counts.uncached.Load() {
					gateway["valid_for_s"] = 150
				}
				gateways = append(gateways, gateway)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"account": r.URL.Query().Get("account"), "gateways": gateways})
		case "/cookie":
			counts.cookies.Add(1)
			gateway := r.URL.Query().Get("gateway")
			payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"host":%q,"exp":%d}`, gateway, time.Now().Add(time.Hour).Unix())))
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
	t.Cleanup(pool.Close)
	var accounts []service.Account
	for _, id := range ids {
		values := map[string]any{"openai_gwpool": true, "openai_gwpool_base_url": pool.URL, "openai_gwpool_consumer_key": "offline"}
		maps.Copy(values, extra)
		account := codexWireAccount(id, fmt.Sprintf("pool-%d", id), values)
		account.GroupIDs = []int64{9001}
		account.Credentials["chatgpt_account_id"] = fmt.Sprintf("offline-%d", id)
		account.Credentials["chatgpt_user_id"] = "user"
		accounts = append(accounts, account)
	}
	repo := gatewayPool429EntryRepo{&grokCredentialHandlerRepo{accounts: accounts}}
	upstream, router, cleanup := newCodexWireEntryWithConfig(t, accounts, nil, configure, true, repo)
	t.Cleanup(cleanup)
	upstream.probeState = "offline-verified"
	upstream.streamBody = gatewayStopSuccess
	return upstream, router, repo, accounts, counts
}

func newGatewayStopEntry(t *testing.T) (*codexWireUpstream, *gin.Engine, gatewayPool429EntryRepo, []service.Account) {
	t.Helper()
	upstream, router, repo, accounts, _ := newGatewayStopEntryWith(t, []int64{801, 802}, nil,
		map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}, func(cfg *config.Config) {
			cfg.Gateway.OpenAIWS.Enabled = true
			cfg.Gateway.OpenAIWS.OAuthEnabled = true
			cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
			cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeCtxPool
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
		})
	return upstream, router, repo, accounts
}

// Requests queue for a busy account instead of failing at once.
func gatewayStopQueueConfig(cfg *config.Config) {
	cfg.Gateway.Scheduling.FallbackWaitTimeout = 15 * time.Second
	cfg.Gateway.Scheduling.FallbackMaxWaiting = 10
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = 15 * time.Second
	cfg.Gateway.Scheduling.StickySessionMaxWaiting = 10
}

func gatewayStopSend(router *gin.Engine, path, body string, timeout time.Duration) *httptest.ResponseRecorder {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, request)
	return rec
}

func gatewayStopBusiness(upstream *codexWireUpstream) []int64 {
	var business []int64
	for _, call := range upstream.taken() {
		if gjson.GetBytes(call.body, "model").String() != "gpt-6-luna" {
			business = append(business, call.accountID)
		}
	}
	return business
}

func setGatewayStopEntryRest(t *testing.T, repo gatewayPool429EntryRepo, account service.Account) {
	t.Helper()
	require.NoError(t, restGatewayStopEntry(repo, account))
}

// restGatewayStopEntry is safe off the test goroutine.
func restGatewayStopEntry(repo gatewayPool429EntryRepo, account service.Account) error {
	identity := "gwpool-member:" + account.GetChatGPTAccountID() + "/" + account.GetCredential("chatgpt_user_id")
	sum := sha256.Sum256([]byte("gwpool-ledger\x00" + identity))
	tag := hex.EncodeToString(sum[:])
	now, until := time.Now().UTC(), time.Now().UTC().Add(time.Hour)
	if err := repo.UpdateExtra(context.Background(), account.ID, map[string]any{
		"openai_gwpool_rest_tag": tag,
		"openai_gwpool_rest_state": map[string]any{
			"tag": tag, "active": true, "changed_at": now, "started_at": now, "resume_at": until,
		},
	}); err != nil {
		return err
	}
	return repo.SetTempUnschedulable(context.Background(), account.ID, until,
		"网关候选低于5 / Gateway candidates below 5")
}

func TestGatewayPoolStopOldHTTPRequestsSwitchWithoutClientError(t *testing.T) {
	for _, tc := range []struct {
		path   string
		stream bool
	}{
		{"/v1/responses", true}, {"/v1/responses", false},
		{"/v1/messages", true}, {"/v1/messages", false},
		{"/v1/chat/completions", true}, {"/v1/chat/completions", false},
	} {
		t.Run(fmt.Sprintf("%s/stream=%v", tc.path, tc.stream), func(t *testing.T) {
			upstream, router, repo, accounts := newGatewayStopEntry(t)
			upstream.sequence = []int{http.StatusServiceUnavailable, 0}
			upstream.afterPost = func() { setGatewayStopEntryRest(t, repo, accounts[0]) }
			body := fmt.Sprintf(`{"model":"gpt-5.4","stream":%t,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`, tc.stream)
			if tc.path == "/v1/responses" {
				body = codexWireResponsesBody(tc.stream)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			request := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(body)).WithContext(ctx)
			request.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, request)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			if tc.stream || tc.path != "/v1/responses" {
				require.Contains(t, rec.Body.String(), "answer")
			}
			require.NotContains(t, rec.Body.String(), "upstream unavailable")
			var business []int64
			for _, call := range upstream.taken() {
				if gjson.GetBytes(call.body, "model").String() != "gpt-6-luna" {
					business = append(business, call.accountID)
				}
			}
			require.Equal(t, []int64{801, 802}, business, "the waiting request must actually change credentials")
		})
	}
}

func TestGatewayPoolStopWSLaterTurnSwitchesWithoutReplayingFirstTurn(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(fmt.Sprintf("user-slot-busy=%v", busy), func(t *testing.T) {
			gatewayStopWSLaterTurn(t, busy)
		})
	}
}

func gatewayStopWSLaterTurn(t *testing.T, busy bool) {
	t.Helper()
	upstream, router, repo, accounts := newGatewayStopEntry(t)
	var userBusy atomic.Bool
	var accountBHeld atomic.Bool
	userWaiting := make(chan struct{})
	var waitingOnce sync.Once
	upstream.cache.acquireUserSlotFn = func(context.Context, int64, int, string) (bool, error) {
		if userBusy.Load() {
			waitingOnce.Do(func() { close(userWaiting) })
			return false, nil
		}
		return true, nil
	}
	upstream.cache.acquireAccountSlotFn = func(_ context.Context, accountID int64, _ int, _ string) (bool, error) {
		if accountID == accounts[1].ID {
			return accountBHeld.CompareAndSwap(false, true), nil
		}
		return true, nil
	}
	upstream.cache.releaseAccountSlotFn = func(_ context.Context, accountID int64, _ string) error {
		if accountID == accounts[1].ID {
			accountBHeld.Store(false)
		}
		return nil
	}
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	competitorDone := make(chan bool, 1)
	if busy {
		go func() {
			select {
			case <-userWaiting:
			case <-ctx.Done():
				competitorDone <- false
				return
			}
			// An HTTP-style request already holds the user slot. It can finish
			// only after taking B's account slot; WS must not hold B while it
			// waits for that user's capacity.
			release, acquired, err := upstream.handler.concurrencyHelper.TryAcquireAccountSlot(ctx, accounts[1].ID, 1)
			if err != nil || !acquired {
				competitorDone <- false
				return
			}
			release()
			userBusy.Store(false)
			competitorDone <- true
		}()
	}
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	require.NoError(t, err)
	defer client.CloseNow()
	for i, text := range []string{"first-turn", "second-turn"} {
		if i == 1 {
			userBusy.Store(busy)
			setGatewayStopEntryRest(t, repo, accounts[0])
		}
		frame := fmt.Sprintf(`{"type":"response.create","model":"gpt-5.4","input":[{"type":"message","role":"user","content":%q}]}`, text)
		if i == 1 { // a continued turn: the replay must carry the first turn's context
			frame = fmt.Sprintf(`{"type":"response.create","model":"gpt-5.4","previous_response_id":"resp_ok","input":[{"type":"message","role":"user","content":%q}]}`, text)
		}
		require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(frame)))
		answer := false
		for {
			_, payload, readErr := client.Read(ctx)
			require.NoError(t, readErr)
			event := gjson.GetBytes(payload, "type").String()
			require.NotEqual(t, "error", event, string(payload))
			if event == "response.output_text.delta" {
				answer = true
			}
			if event == "response.completed" {
				require.True(t, answer)
				break
			}
		}
	}
	_ = client.Close(coderws.StatusNormalClosure, "done")
	var business []codexWireCapture
	for _, call := range upstream.taken() {
		if gjson.GetBytes(call.body, "model").String() != "gpt-6-luna" {
			business = append(business, call)
		}
	}
	require.Len(t, business, 2)
	require.EqualValues(t, 801, business[0].accountID)
	require.EqualValues(t, 802, business[1].accountID)
	require.Contains(t, string(business[1].body), "second-turn")
	require.Contains(t, string(business[1].body), "first-turn")
	require.Contains(t, string(business[1].body), "answer", "the first turn's output travels with the replay")
	require.NotContains(t, string(business[1].body), "previous_response_id")
	if busy {
		require.True(t, <-competitorDone, "user-before-account ordering must allow the competing request to finish")
	}
}

func TestGatewayPoolStopPendingHTTPSlotHandoff(t *testing.T) {
	for _, acquiredDuringRest := range []bool{false, true} {
		t.Run(fmt.Sprintf("acquired-during-rest=%v", acquiredDuringRest), func(t *testing.T) {
			upstream, router, repo, accounts := newGatewayStopEntry(t)
			calls := 0
			var blockedContext context.Context
			upstream.cache.acquireAccountSlotFn = func(waitCtx context.Context, id int64, _ int, _ string) (bool, error) {
				if id != accounts[0].ID {
					return true, nil
				}
				calls++
				if calls == 2 {
					blockedContext = waitCtx
				}
				if calls == 3 {
					// After quick acquisition and the waiter's immediate attempt:
					// the request has entered the queue and sent a heartbeat.
					setGatewayStopEntryRest(t, repo, accounts[0])
					return acquiredDuringRest, nil
				}
				return false, nil
			}
			upstream.handler.concurrencyHelper.pingInterval = time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(codexWireResponsesBody(true))).WithContext(ctx)
			request.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = request
			selection := &service.AccountSelectionResult{Account: &accounts[0], WaitPlan: &service.AccountWaitPlan{
				AccountID: accounts[0].ID, MaxConcurrency: 1, MaxWaiting: 10, Timeout: 7 * time.Second,
			}}
			streamStarted := false
			group := int64(9001)
			release, result := upstream.handler.acquireOpenAIAccountSlot(c, &group, "", selection, true, &streamStarted, zap.NewNop(), nil)
			require.Equal(t, openAISlotAcquirePoolRested, result)
			require.Nil(t, release)
			require.Same(t, request, c.Request, "restore the original request after the child wait ends")
			require.NoError(t, ctx.Err(), "pool rest cancels only the slot wait")
			require.True(t, streamStarted, "a non-semantic heartbeat must not prevent handoff")
			if !acquiredDuringRest {
				var restFailure *service.UpstreamFailoverError
				require.True(t, errors.As(context.Cause(blockedContext), &restFailure), "the rest watcher must interrupt the pending wait, not wait for its timeout")
				require.Equal(t, service.OpenAIGatewayPoolReason, restFailure.Reason)
			}
			require.Zero(t, atomic.LoadInt32(&upstream.cache.accountWaiting))
			expectedReleases := int32(0)
			if acquiredDuringRest {
				expectedReleases = 1
			}
			require.Equal(t, expectedReleases, atomic.LoadInt32(&upstream.cache.releaseAccountCalled))
			require.NotContains(t, rec.Body.String(), "error")

			// The same still-live request can now be delivered by B.
			router.ServeHTTP(rec, request)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Contains(t, rec.Body.String(), "answer")
			require.NotContains(t, rec.Body.String(), "error")
			for _, call := range upstream.taken() {
				require.EqualValues(t, 802, call.accountID)
			}
			cancel()
			require.Equal(t, expectedReleases+1, atomic.LoadInt32(&upstream.cache.releaseAccountCalled), "A and B each release only their acquired slot")
		})
	}
}

// A pool credential that enters rest while the request queues for its slot
// is replaced before any output on every endpoint that can select it. With
// no replacement the client gets the pool's 503 and Retry-After.
func TestGatewayPoolStopRestedSlotWaitFailsOver(t *testing.T) {
	alpha := `{"model":"gpt-5.4","id":"s1","query":"x"}`
	for _, tc := range []struct {
		name, path, body string
		ids              []int64
	}{
		{"responses-last-member", "/v1/responses", codexWireResponsesBody(false), []int64{801}},
		{"chat-handoff", "/v1/chat/completions", gatewayStopChatBody, []int64{801, 802}},
		{"chat-last-member", "/v1/chat/completions", gatewayStopChatBody, []int64{801}},
		{"messages-handoff", "/v1/messages", gatewayStopChatBody, []int64{801, 802}},
		{"messages-last-member", "/v1/messages", gatewayStopChatBody, []int64{801}},
		{"alpha-last-member", "/v1/alpha/search", alpha, []int64{801}},
		{"alpha-handoff", "/v1/alpha/search", alpha, []int64{801, 802}},
		{"images-last-member", "/v1/images/generations", `{"model":"gpt-image-2","prompt":"x"}`, []int64{801}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream, router, repo, accounts, _ := newGatewayStopEntryWith(t, tc.ids, nil, nil, gatewayStopQueueConfig)
			router.POST("/v1/alpha/search", upstream.handler.AlphaSearch)
			router.POST("/v1/images/generations", func(c *gin.Context) {
				if key, ok := c.Get(string(middleware.ContextKeyAPIKey)); ok {
					key.(*service.APIKey).Group.AllowImageGeneration = true
				}
				upstream.handler.Images(c)
			})
			var polls atomic.Int32
			var rested atomic.Bool
			upstream.cache.acquireAccountSlotFn = func(_ context.Context, id int64, _ int, _ string) (bool, error) {
				if id != accounts[0].ID {
					return rested.Load(), nil // 802 is busy too, so the request queues for 801
				}
				if polls.Add(1) == 3 {
					if err := restGatewayStopEntry(repo, accounts[0]); err != nil {
						t.Error(err)
					}
					rested.Store(true)
				}
				return false, nil
			}
			rec := gatewayStopSend(router, tc.path, tc.body, 20*time.Second)
			if len(tc.ids) == 1 {
				require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
				require.NotEmpty(t, rec.Header().Get("Retry-After"))
				require.Empty(t, gatewayStopBusiness(upstream))
				return
			}
			require.NotEmpty(t, rec.Body.String())
			require.Equal(t, []int64{802}, gatewayStopBusiness(upstream))
		})
	}
}

// A stop that the confirming list overturns rejoins its credential at once:
// it is not a verification retry, so it waits no verification gap.
func TestGatewayPoolStopSupersededRejoinHasNoGap(t *testing.T) {
	// The stop check lists four gateways; the rotation confirmation lists five.
	upstream, router, _, _, _ := newGatewayStopEntryWith(t, []int64{801, 802}, func(call int32) int {
		if call <= 2 {
			return 4
		}
		return 5
	}, nil, gatewayStopQueueConfig)
	started := time.Now()
	rec := gatewayStopSend(router, "/v1/responses", codexWireResponsesBody(true), 20*time.Second)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, []int64{801}, gatewayStopBusiness(upstream))
	require.Less(t, time.Since(started), 3*time.Second)
}

// A credential that enters pool rest while its request waits out a real 429
// no longer holds that request: another usable member serves it.
func TestGatewayPoolStopRestHandsOffDuringReal429Retry(t *testing.T) {
	gap := func(path, body string) func(t *testing.T) {
		return func(t *testing.T) {
			upstream, router, repo, accounts, _ := newGatewayStopEntryWith(t, []int64{801, 802}, nil,
				map[string]any{"openai_gwpool_stop_gateways": 1}, nil)
			upstream.sequence = []int{http.StatusTooManyRequests, 0}
			upstream.errorBody = `{"detail":"Rate limit exceeded"}`
			upstream.afterPost = func() {
				// Rest lands inside the 429 retry gap.
				time.AfterFunc(150*time.Millisecond, func() {
					if err := restGatewayStopEntry(repo, accounts[0]); err != nil {
						t.Error(err)
					}
				})
			}
			rec := gatewayStopSend(router, path, body, 20*time.Second)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, []int64{801, 802}, gatewayStopBusiness(upstream))
		}
	}
	t.Run("responses-gap", gap("/v1/responses", codexWireResponsesBody(true)))
	t.Run("chat-gap", gap("/v1/chat/completions", gatewayStopChatBody))
	t.Run("messages-gap", gap("/v1/messages", gatewayStopChatBody))
	t.Run("responses-slot-wait", func(t *testing.T) {
		upstream, router, repo, accounts, _ := newGatewayStopEntryWith(t, []int64{801, 802}, nil,
			map[string]any{"openai_gwpool_stop_gateways": 1}, gatewayStopQueueConfig)
		upstream.sequence = []int{http.StatusTooManyRequests, 0}
		upstream.errorBody = `{"detail":"Rate limit exceeded"}`
		var calls atomic.Int32
		upstream.cache.acquireAccountSlotFn = func(_ context.Context, id int64, _ int, _ string) (bool, error) {
			if id != accounts[0].ID {
				return true, nil
			}
			switch n := calls.Add(1); {
			case n == 1:
				return true, nil // the first attempt receives the real 429
			case n < 4:
				return false, nil // the retry queues for 801
			case n == 4:
				if err := restGatewayStopEntry(repo, accounts[0]); err != nil {
					t.Error(err)
				}
				return false, nil
			}
			return true, nil
		}
		rec := gatewayStopSend(router, "/v1/responses", codexWireResponsesBody(true), 20*time.Second)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, []int64{801, 802}, gatewayStopBusiness(upstream))
	})
}

// Supply flapping around the stop threshold earns one same-account retry per
// request; the next stop moves the request on.
func TestGatewayPoolStopFlappingSupplyRetriesSameAccountOnce(t *testing.T) {
	// Every check lists afresh; only the rotation confirmation sees five.
	upstream, router, repo, _, pool := newGatewayStopEntryWith(t, []int64{801, 802}, func(call int32) int {
		if call%4 == 0 {
			return 5
		}
		return 4
	}, nil, gatewayStopQueueConfig)
	pool.uncached.Store(true)
	repo.mu.Lock()
	repo.accounts[1].Extra["openai_gwpool_stop_gateways"] = 1
	repo.mu.Unlock()
	started := time.Now()
	rec := gatewayStopSend(router, "/v1/responses", codexWireResponsesBody(true), 20*time.Second)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, []int64{802}, gatewayStopBusiness(upstream))
	require.Less(t, time.Since(started), 12*time.Second)
}

// With every member below its stop threshold each request still gets the
// pool's 503, but backed-off members answer from their unexpired lists, at
// the stop check and at the usage-cycle boundary alike.
func TestGatewayPoolStopAllMembersBelowThresholdListOnce(t *testing.T) {
	_, router, _, _, pool := newGatewayStopEntryWith(t, []int64{801, 802, 803}, func(int32) int { return 4 }, nil, gatewayStopQueueConfig)
	for i := 0; i < 3; i++ {
		before := pool.gateways.Load()
		rec := gatewayStopSend(router, "/v1/responses", codexWireResponsesBody(true), 20*time.Second)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "stop threshold")
		require.NotEmpty(t, rec.Header().Get("Retry-After"))
		if i > 0 {
			require.Zero(t, pool.gateways.Load()-before, "request %d", i)
		}
	}
	require.Zero(t, pool.cookies.Load())
}

// The failover user-slot wait is bounded like the HTTP one.
func TestGatewayPoolWSFailoverUserSlotWaitIsBounded(t *testing.T) {
	previous := gatewayPoolWSUserSlotWaitMax
	gatewayPoolWSUserSlotWaitMax = 50 * time.Millisecond
	t.Cleanup(func() { gatewayPoolWSUserSlotWaitMax = previous })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release, acquired, err := acquireGatewayPoolWSFailoverUserSlot(ctx, func() (func(), bool, error) { return nil, false, nil })
	require.NoError(t, err)
	require.False(t, acquired)
	require.Nil(t, release)
	require.NoError(t, ctx.Err())
}
