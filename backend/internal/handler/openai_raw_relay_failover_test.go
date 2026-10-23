//go:build unit

package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 原样中继账号换号耗尽：客户端拿到的是上游原始响应，不经错误映射。
func TestOpenAIRawRelayFailoverExhausted_WritesUpstreamVerbatim(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(nil))
	body := []byte(`{"error":{"code":"concurrency_queue_full","message":"busy"}}`)

	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode:       http.StatusTooManyRequests,
		ResponseBody:     body,
		ResponseHeaders:  http.Header{"Content-Type": []string{"application/json"}, "X-Gateway-Request-Id": []string{"gw-9"}},
		RawRelayResponse: true,
	}, false)

	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, string(body), rec.Body.String())
	require.Equal(t, "gw-9", rec.Header().Get("X-Gateway-Request-Id"))
}

type cprDelegationHTTPUpstream struct {
	service.HTTPUpstream
	body           []byte
	calls          int
	delay          time.Duration
	firstAccountID int64
	firstDelay     time.Duration
}

func (u *cprDelegationHTTPUpstream) Do(r *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.body, _ = io.ReadAll(r.Body)
	u.calls++
	delay := u.delay
	if accountID == u.firstAccountID {
		delay = u.firstDelay
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return &http.Response{StatusCode: 503, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"CPR unavailable"}}`))}, nil
}

func TestCPRDelegationImagesSlowErrorDoesNotBecomeJSONKeepaliveSSE(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mixed      bool
		firstDelay time.Duration
		wantStatus int
	}{
		{"cpr_only", false, 0, 503},
		{"apikey_to_cpr_before_heartbeat", true, 0, 503},
		{"apikey_to_cpr_after_heartbeat", true, 1100 * time.Millisecond, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := service.Account{ID: 9968, Name: "cpr-images", Platform: service.PlatformOpenAI, Type: service.AccountTypeCPR,
				Status: service.StatusActive, Schedulable: true, Concurrency: 1, Priority: 1,
				Credentials: map[string]any{"base_url": "https://fake.invalid", "api_key": "fake-client"}}
			cfg := &config.Config{RunMode: config.RunModeSimple}
			cfg.Default.RateMultiplier = 1
			cfg.Gateway.ImageNonstreamKeepaliveInterval = 1
			cfg.Gateway.MaxAccountSwitches = 2
			repo := openAIImagesFailoverAccountRepo{accounts: []service.Account{account}}
			upstream := &cprDelegationHTTPUpstream{delay: 1100 * time.Millisecond}
			if tc.mixed {
				first := account
				first.ID, first.Type, first.Priority = 9967, service.AccountTypeAPIKey, 0
				repo.accounts = append([]service.Account{first}, repo.accounts...)
				upstream.firstAccountID, upstream.firstDelay = first.ID, tc.firstDelay
			}
			billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billing.Stop)
			gateway := service.NewOpenAIGatewayService(repo, nil, nil, nil, nil, nil, nil, cfg, nil, nil,
				service.NewBillingService(cfg, nil), nil, billing, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-1","prompt":"cat"}`))
			c.Request.Header.Set("Content-Type", "application/json")
			groupID := int64(4289)
			c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: 1899, GroupID: &groupID, User: &service.User{ID: 1799, Status: service.StatusActive},
				Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, AllowImageGeneration: true}})
			c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1799, Concurrency: 0})
			h.Images(c)
			wantCalls := 1
			if tc.mixed {
				wantCalls = 2
			}
			require.Equal(t, wantCalls, upstream.calls)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			require.JSONEq(t, `{"error":{"message":"CPR unavailable"}}`, rec.Body.String())
			require.NotContains(t, rec.Body.String(), "event:")
		})
	}
}

func TestCPRDelegationHTTPHandlerDefersEffortDenyUntilAccountSelected(t *testing.T) {
	for _, accountType := range []string{service.AccountTypeCPR, service.AccountTypeAPIKey, service.AccountTypeOAuth} {
		for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
			t.Run(accountType+path, func(t *testing.T) {
				gin.SetMode(gin.TestMode)
				groupID := int64(4289)
				account := service.Account{
					ID: 9957, Name: "delegation", Platform: service.PlatformOpenAI, Type: accountType,
					Status: service.StatusActive, Schedulable: true, Concurrency: 1,
					Credentials: map[string]any{"base_url": "https://fake.invalid", "api_key": "fake-client", "access_token": "fake-access"},
				}
				cfg := &config.Config{RunMode: config.RunModeSimple}
				cfg.Default.RateMultiplier = 1
				repo := &openAIWSFailoverHandlerAccountRepoStub{accounts: []service.Account{account}}
				upstream := &cprDelegationHTTPUpstream{}
				billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
				t.Cleanup(billing.Stop)
				gateway := service.NewOpenAIGatewayService(repo, nil, nil, nil, nil, nil, nil, cfg, nil, nil,
					service.NewBillingService(cfg, nil), nil, billing, upstream, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil)
				h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing,
					service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
				require.True(t, account.IsModelSupported("gpt-5.4"), "fixture must reach account selection")
				body := `{"model":"gpt-5.4","input":"hi","messages":[{"role":"user","content":"hi"}],"reasoning":{"effort":"xhigh"}}`
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				observedLogger, observed := newObservedLogger(t)
				c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(logger.IntoContext(context.Background(), observedLogger))
				c.Request.Header.Set("Content-Type", "application/json")
				c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{
					ID: 1899, GroupID: &groupID, User: &service.User{ID: 1799, Status: service.StatusActive},
					Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, MaxReasoningEffort: "low", MaxReasoningEffortOverLimit: "deny"},
				})
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1799, Concurrency: 0})
				if path == "/v1/responses" {
					h.Responses(c)
				} else {
					h.ChatCompletions(c)
				}
				for _, entry := range observed.FilterMessage("openai.responses_panic_recovered").All() {
					t.Fatalf("handler panic: %v", entry.ContextMap())
				}
				if accountType == service.AccountTypeCPR {
					require.Equal(t, 1, upstream.calls)
					require.Equal(t, body, string(upstream.body))
					require.Equal(t, 503, rec.Code)
				} else {
					require.Zero(t, upstream.calls)
					require.Equal(t, 403, rec.Code, "body=%s errors=%v", rec.Body.String(), c.Errors)
				}
			})
		}
	}
}

func TestCPRDelegationWSHandlerDoesNotDenyEffortBeforeRawRelay(t *testing.T) {
	first := `{"type":"response.create","model":"gpt-5.4","reasoning":{"effort":"xhigh"},"input":"hi"}`
	got := runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
		firstPayload: first, cpr: true, userAgent: testStringPtr("codex_cli_rs/0.160.0"),
		group: &service.Group{ID: 42, Platform: service.PlatformOpenAI, Status: service.StatusActive, MaxReasoningEffort: "low", MaxReasoningEffortOverLimit: "deny"},
	})
	require.Equal(t, first, string(got.upstreamFirstPayload))
	require.Len(t, got.logs, 1)
}

// WS 下换号耗尽：先把上游那条错误帧原样发给客户端，再按状态码关连接。
func TestOpenAIRawRelayWSFailoverExhausted_SendsUpstreamErrorFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)
	frame := []byte(`{"type":"error","status":503,"error":{"code":"no_available_provider","message":"no provider"}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		closeOpenAIWSFailoverExhausted(c, conn, &service.UpstreamFailoverError{
			StatusCode:       http.StatusServiceUnavailable,
			ResponseBody:     frame,
			RawRelayResponse: true,
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer func() { _ = client.CloseNow() }()
	_, got, err := client.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, string(frame), string(got))
	_, _, err = client.Read(ctx)
	require.Equal(t, coderws.StatusTryAgainLater, coderws.CloseStatus(err), "%v", err)
}

// 端到端：handler 选中原样中继账号（apikey 开关 / cpr），两轮帧逐字节到上游，每轮各记一条用量。
func TestOpenAIResponsesWebSocket_RawRelayRelaysTurnsVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name string
		cpr  bool
	}{{"apikey_switch", false}, {"cpr", true}} {
		t.Run(tc.name, func(t *testing.T) {
			first := `{"type":"response.create","model":"gpt-5.4",  "stream":false,"zeta":1,"input":[{"type":"message","role":"user","content":"a <b> & c"}]}`
			second := `{"type":"response.create","model":"gpt-5.4","previous_response_id":"resp_usage_e2e_1","input":[]}`
			got := runOpenAIResponsesWebSocketUsageLogCase(t, openAIResponsesWSUsageLogCase{
				firstPayload:  first,
				secondPayload: second,
				userAgent:     testStringPtr("codex_cli_rs/0.130.0"),
				rawRelay:      !tc.cpr,
				cpr:           tc.cpr,
				clientHeaders: map[string]string{"X-Klno-Probe": "raw"},
			})
			// 客户端自定义头只有原样中继会带到上游，借它确认走的是原样中继而不是旧透传。
			require.Equal(t, "raw", got.upstreamHeader.Get("X-Klno-Probe"))
			require.Equal(t, []string{first, second}, []string{string(got.upstreamPayloads[0]), string(got.upstreamPayloads[1])})
			require.Len(t, got.logs, 2)
			for _, usageLog := range got.logs {
				require.Equal(t, 2, usageLog.InputTokens)
				require.Equal(t, 1, usageLog.OutputTokens)
				require.True(t, usageLog.OpenAIWSMode)
			}
		})
	}
}

// 原样中继的用户侧拒绝、握手拒绝透传不算账号失败，不拉低调度统计；普通代理失败照旧上报。
func TestShouldReportOpenAIWSProxyAccountFailure_RawRelayNotAccountFault(t *testing.T) {
	closeErr := service.NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "upstream websocket handshake rejected", nil)
	require.False(t, shouldReportOpenAIWSProxyAccountFailure(fmt.Errorf("%w: %w", service.ErrOpenAIRawRelayNotAccountFault, closeErr)))
	require.True(t, shouldReportOpenAIWSProxyAccountFailure(closeErr))
}
