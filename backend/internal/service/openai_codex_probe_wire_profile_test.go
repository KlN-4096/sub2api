//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/httpclient"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// codexModelsProbeServer 顶替 chatgptCodexModelsURL 的上游：记录每个到达的请求，
// 返回一份合法的模型清单（或指定状态码）。
type codexModelsProbeServer struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	status   int
	headers  http.Header // 随任意状态码一起返回的响应头
}

func newCodexModelsProbeServer(t *testing.T) *codexModelsProbeServer {
	t.Helper()
	s := &codexModelsProbeServer{status: http.StatusOK}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.requests = append(s.requests, r.Clone(context.Background()))
		s.bodies = append(s.bodies, body)
		status := s.status
		for name, values := range s.headers {
			w.Header()[name] = append([]string(nil), values...)
		}
		s.mu.Unlock()
		if status != http.StatusOK {
			http.Error(w, "upstream says no", status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"probe-etag"`)
		_, _ = w.Write([]byte(`{"models":[{"slug":"gpt-5.5","visibility":"list"},{"slug":"gpt-5.6","visibility":"list"}]}`))
	}))
	t.Cleanup(server.Close)
	original := chatgptCodexModelsURL
	chatgptCodexModelsURL = server.URL + "/backend-api/codex/models"
	t.Cleanup(func() { chatgptCodexModelsURL = original })
	return s
}

// codexProbeHeaderNames 返回出站头名集合（小写）；Accept-Encoding/Content-Length/Connection 由 Go transport
// 写出时自动补，不属于身份画像。
func codexProbeHeaderNames(h http.Header) []string {
	got := make([]string, 0, len(h))
	for key := range h {
		lower := strings.ToLower(key)
		if lower == "accept-encoding" || lower == "content-length" || lower == "connection" {
			continue
		}
		got = append(got, lower)
	}
	return got
}

func newCodexProbeAdminContext(t *testing.T) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/9101/test", nil)
	// Admin request metadata is not evidence of the synthetic probe's session.
	c.Request.Header.Set("session-id", "admin-session")
	c.Request.Header.Set(openAIWSTurnMetadataHeader, `{"session_id":"admin-session","sandbox":"admin"}`)
	return c, rec
}

// 双开账号的三种探针都伪装成刚启动的新 Codex 会话：唯一出站是
// GET /backend-api/codex/models?client_version=…（codex-rs model-provider/src/models_endpoint.rs
// list_models），头集合 = provider 头（version）+ originator/UA + 鉴权，没有任何 x-codex-*；
// 非双开账号维持既有的自造 /responses、/images 探针。
func TestCodexDeviceWireProfileAccountProbes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{"device", "off", "session", "full"} {
		for _, enabled := range []bool{false, true} {
			for _, probe := range []string{"normal", "compact", "image"} {
				name := mode + "/disabled/" + probe
				if enabled {
					name = mode + "/enabled/" + probe
				}
				t.Run(name, func(t *testing.T) {
					account := wireProfileTestAccount(enabled)
					account.Extra[codexFingerprintModeExtraKey] = mode
					up := &httpUpstreamRecorder{err: errors.New("offline-probe-captured")}
					models := newCodexModelsProbeServer(t)
					svc := &AccountTestService{
						cfg:                  &config.Config{},
						httpUpstream:         up,
						openaiGatewayService: &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up},
					}
					model, testMode := "gpt-5.5", AccountTestModeDefault
					switch probe {
					case "compact":
						testMode = AccountTestModeCompact
					case "image":
						model = "gpt-image-2"
					}
					c, rec := newCodexProbeAdminContext(t)
					err := svc.testOpenAIAccountConnection(c, account, model, "offline", testMode)

					if enabled && mode == "device" {
						require.NoError(t, err)
						require.Empty(t, up.requests, "双开探针不再自造 /responses、/images 请求")
						require.Len(t, models.requests, 1, "唯一出站是模型清单请求")
						req := models.requests[0]
						require.Equal(t, http.MethodGet, req.Method)
						require.Equal(t, "/backend-api/codex/models", req.URL.Path)
						require.Equal(t, CodexCanonicalClientVersion(), req.URL.Query().Get("client_version"))
						require.Empty(t, models.bodies[0])
						identity := resolveCodexOutboundIdentity("")
						require.Equal(t, identity.version, req.Header.Get("version"), "provider 头 version")
						require.Equal(t, identity.originator, req.Header.Get("originator"))
						require.Equal(t, identity.userAgent, req.Header.Get("User-Agent"))
						require.Equal(t, "Bearer offline-token", req.Header.Get("Authorization"))
						require.Equal(t, "offline-account", req.Header.Get("ChatGPT-Account-ID"))
						require.Equal(t, "*/*", req.Header.Get("Accept"), "reqwest 默认 Accept；真客户端不为 /models 显式设置")
						// 集合断言而不是黑名单：多带一个真客户端不发的头就必须红。
						// Accept-Encoding/Content-Length 由 Go transport 在写出时自动补，不属于身份
						// 画像；真客户端（reqwest 未启用压缩 feature）不发 accept-encoding，这是网关
						// 所有出站请求的既有全局差异，不是本探针引入的。
						got := make([]string, 0, len(req.Header))
						for key := range req.Header {
							lower := strings.ToLower(key)
							if lower == "accept-encoding" || lower == "content-length" || lower == "connection" {
								continue
							}
							got = append(got, lower)
						}
						require.ElementsMatch(t,
							[]string{"authorization", "chatgpt-account-id", "accept", "originator", "user-agent", "version"},
							got, "头集合必须与 list_models 一致（provider 头 + 默认客户端头 + 鉴权）")
						events := rec.Body.String()
						require.Contains(t, events, `"type":"test_start"`)
						require.Contains(t, events, `"type":"test_complete"`)
						require.Contains(t, events, `"success":true`)
						require.Contains(t, events, "returned 2 models")
						require.NotContains(t, events, "admin-session")

						// 探针必须每次真的打到上游，不能吃 60s 账号级缓存。
						c2, _ := newCodexProbeAdminContext(t)
						require.NoError(t, svc.testOpenAIAccountConnection(c2, account, model, "offline", testMode))
						require.Len(t, models.requests, 2, "探针不走缓存")
						require.Empty(t, up.requests)
						return
					}

					require.ErrorContains(t, err, "offline-probe-captured")
					require.Empty(t, models.requests, "非双开不发模型清单探针")
					require.Len(t, up.requests, 1, "must inspect the actual send boundary")
					req := up.lastReq
					require.Equal(t, resolveCodexOutboundIdentity("").version, req.Header.Get("version"),
						"version 是 provider 头，所有探针都带且钉到规范身份")
					require.Equal(t, "responses=experimental", req.Header.Get("OpenAI-Beta"))
					require.False(t, gjson.GetBytes(up.lastBody, "client_metadata").Exists())
					if probe == "compact" {
						require.NotEmpty(t, req.Header.Get("x-codex-window-id"), "非双开 compact 探针保留既有的自造窗口头")
						require.Contains(t, string(up.lastBody), `"type":"compaction_trigger"`)
						require.Contains(t, req.Header.Get("x-codex-beta-features"), "remote_compaction_v2")
					}
					require.NotContains(t, string(up.lastBody), "admin-session")
					require.NotContains(t, req.Header.Get(openAIWSTurnMetadataHeader), "admin")
					require.Equal(t, int64(len(up.lastBody)), req.ContentLength)
					require.NotNil(t, req.GetBody)
					replay, err := req.GetBody()
					require.NoError(t, err)
					replayed, err := io.ReadAll(replay)
					require.NoError(t, err)
					require.NoError(t, replay.Close())
					require.Equal(t, up.lastBody, replayed)
				})
			}
		}
	}
}

// 定时测试（RunTestBackground）与管理端点击走同一条新会话探针：双开账号的任何自造 /responses
// 都是真客户端不会发的形态，按 cron 反复发更不行。探针只验证凭据，成功结果标 CredentialsOnly，
// runner 仍做 auto_recover 但只清 StatusError（AccountRecoveryOptions.CredentialsOnly）。非双开维持既有
// /responses 探针。
func TestCodexDeviceWireProfileRunTestBackgroundUsesModelsProbe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name       string
		enabled    bool
		setupToken bool
	}{
		{"device/enabled", true, false},
		{"device/disabled", false, false},
		// setup-token 与 OAuth 共用 Codex 后端与 bearer，双开对它同样成立（凭证域 = setup-token 指纹）。
		{"device/enabled/setup-token", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := wireProfileTestAccount(tc.enabled)
			if tc.setupToken {
				account.Type = AccountTypeSetupToken
				account.Credentials = map[string]any{"access_token": "offline-token"}
			}
			repo := &openAIAccountTestRepo{mockAccountRepoForGemini: mockAccountRepoForGemini{
				accountsByID: map[int64]*Account{account.ID: account},
			}}
			up := &httpUpstreamRecorder{err: errors.New("offline-probe-captured")}
			models := newCodexModelsProbeServer(t)
			svc := &AccountTestService{
				cfg:                  &config.Config{},
				accountRepo:          repo,
				httpUpstream:         up,
				openaiGatewayService: &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up},
			}

			result, err := svc.RunTestBackground(context.Background(), account.ID, "gpt-5.5")
			require.NoError(t, err)
			require.NotNil(t, result)
			if tc.enabled {
				require.Equal(t, "success", result.Status, result.ErrorMessage)
				require.True(t, result.CredentialsOnly, "双开探针只验证凭据，结果必须标出来")
				require.Len(t, models.requests, 1, "定时测试也走 /models 新会话探针")
				require.Equal(t, "Bearer offline-token", models.requests[0].Header.Get("Authorization"))
				require.Empty(t, up.requests, "定时测试不再发自造 /responses")
				if tc.setupToken {
					// setup-token 双开与 OAuth 双开同形：Accept */*、同一头集合；没有 chatgpt_account_id 时
					// ChatGPT-Account-ID 整条省略（model-provider/src/bearer_auth_provider.rs:38-42 只在 Some 时 insert）。
					req := models.requests[0]
					require.Equal(t, "*/*", req.Header.Get("Accept"))
					require.Empty(t, req.Header.Get("ChatGPT-Account-ID"))
					require.ElementsMatch(t, []string{"authorization", "accept", "originator", "user-agent", "version"}, codexProbeHeaderNames(req.Header))
				}
				return
			}
			require.Equal(t, "failed", result.Status)
			require.Contains(t, result.ErrorMessage, "offline-probe-captured")
			require.False(t, result.CredentialsOnly)
			require.Empty(t, models.requests)
			require.Len(t, up.requests, 1, "非双开维持既有 /responses 探针")
		})
	}
}

// 定时任务端到端（runOnePlan）：双开账号的定时测试成功只证明凭据可用，auto_recover 只清
// StatusError；限流窗口按到期自然解除，不会被 GET /models 的 200 提前清掉。
func TestScheduledTestRunnerCredentialsOnlyRecoveryKeepsRateLimitWindows(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := wireProfileTestAccount(true)
	account.Status = StatusError
	resetAt := time.Now().Add(30 * time.Minute)
	account.RateLimitedAt, account.RateLimitResetAt = &resetAt, &resetAt
	repo := &scheduledTestProbeAccountRepo{openAIAccountTestRepo: openAIAccountTestRepo{mockAccountRepoForGemini: mockAccountRepoForGemini{
		accountsByID: map[int64]*Account{account.ID: account},
	}}}
	models := newCodexModelsProbeServer(t)
	up := &httpUpstreamRecorder{err: errors.New("offline-probe-captured")}
	accountTestSvc := &AccountTestService{
		cfg:                  &config.Config{},
		accountRepo:          repo,
		httpUpstream:         up,
		openaiGatewayService: &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up},
	}
	planRepo := &scheduledTestProbePlanRepo{}
	resultRepo := &scheduledTestProbeResultRepo{}
	rateLimitSvc := NewRateLimitService(repo, nil, &config.Config{}, nil, &tempUnschedCacheRecorder{})
	runner := NewScheduledTestRunnerService(planRepo, NewScheduledTestService(planRepo, resultRepo), accountTestSvc, rateLimitSvc, &config.Config{})
	plan := &ScheduledTestPlan{ID: 31, AccountID: account.ID, ModelID: "gpt-5.5", CronExpression: "*/5 * * * *", Enabled: true, MaxResults: 10, AutoRecover: true}

	runner.runOnePlan(context.Background(), plan)

	require.Len(t, models.requests, 1, "定时测试走 /models 探针")
	require.Empty(t, up.requests)
	require.Len(t, resultRepo.results, 1)
	require.Equal(t, "success", resultRepo.results[0].Status, resultRepo.results[0].ErrorMessage)
	require.True(t, resultRepo.results[0].CredentialsOnly)
	require.Equal(t, account.ID, repo.clearedErrorID, "凭据可用 → StatusError 清掉")
	require.Empty(t, repo.clearRateLimitIDs, "凭据探针的 200 证明不了限流已解除，窗口不得被清")
	require.Equal(t, []int64{plan.ID}, planRepo.updatedAfterRun)

	// 正控：同一夹具按"跑通推理"的成功恢复，限流窗口确实会被清——上面没清是 CredentialsOnly 起的作用。
	runner.tryRecoverAccount(context.Background(), account.ID, plan.ID, false)
	require.Equal(t, []int64{account.ID}, repo.clearRateLimitIDs)
}

type scheduledTestProbeAccountRepo struct {
	openAIAccountTestRepo
	clearRateLimitIDs []int64
}

func (r *scheduledTestProbeAccountRepo) ClearRateLimit(_ context.Context, id int64) error {
	r.clearRateLimitIDs = append(r.clearRateLimitIDs, id)
	return nil
}

type scheduledTestProbePlanRepo struct {
	ScheduledTestPlanRepository
	updatedAfterRun []int64
}

func (r *scheduledTestProbePlanRepo) UpdateAfterRun(_ context.Context, id int64, _, _ time.Time) error {
	r.updatedAfterRun = append(r.updatedAfterRun, id)
	return nil
}

type scheduledTestProbeResultRepo struct {
	ScheduledTestResultRepository
	results []*ScheduledTestResult
}

func (r *scheduledTestProbeResultRepo) Create(_ context.Context, result *ScheduledTestResult) (*ScheduledTestResult, error) {
	r.results = append(r.results, result)
	return result, nil
}

func (r *scheduledTestProbeResultRepo) PruneOldResults(context.Context, int64, int) error { return nil }

// 探针失败侧与原 /responses 探针同责：429 同步限流窗口（定时任务靠它把限流账号移出调度）。
func TestCodexDeviceWireProfileFreshSessionProbe429SyncsRateLimitWindow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := wireProfileTestAccount(true)
	repo := &openAIAccountTestRepo{mockAccountRepoForGemini: mockAccountRepoForGemini{
		accountsByID: map[int64]*Account{account.ID: account},
	}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-codex-primary-used-percent", "100")
		w.Header().Set("x-codex-primary-reset-after-seconds", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"usage_limit_reached","plan_type":"plus"}}`))
	}))
	t.Cleanup(server.Close)
	original := chatgptCodexModelsURL
	chatgptCodexModelsURL = server.URL
	t.Cleanup(func() { chatgptCodexModelsURL = original })
	up := &httpUpstreamRecorder{err: errors.New("offline-probe-captured")}
	svc := &AccountTestService{
		cfg:                  &config.Config{},
		accountRepo:          repo,
		httpUpstream:         up,
		openaiGatewayService: &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up},
	}

	result, err := svc.RunTestBackground(context.Background(), account.ID, "gpt-5.5")
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Contains(t, result.ErrorMessage, "GET /models")
	require.False(t, result.CredentialsOnly, "失败不算凭据验证成功")
	require.Equal(t, account.ID, repo.rateLimitedID, "429 必须像原探针一样写回限流窗口")
	require.NotNil(t, repo.rateLimitedAt)
	require.WithinDuration(t, time.Now().Add(time.Hour), *repo.rateLimitedAt, 2*time.Minute)
	require.Empty(t, up.requests)
}

// 从请求构造到 httpclient 的承重线：/models 出站实际拿到的客户端选项必须带 ForceHTTP2（双开），
// 其余账号不带。只断言结构体字段与 transport 字段各自成立是不够的，中间这一段也要钉住。
func TestCodexDeviceWireProfileModelsClientOptions(t *testing.T) {
	var (
		mu   sync.Mutex
		seen []httpclient.Options
	)
	original := openAIModelsHTTPClient
	openAIModelsHTTPClient = func(opts httpclient.Options) (*http.Client, error) {
		mu.Lock()
		seen = append(seen, opts)
		mu.Unlock()
		return original(opts)
	}
	t.Cleanup(func() { openAIModelsHTTPClient = original })

	for _, tc := range []struct {
		name    string
		enabled bool
		want    bool
	}{
		{"device/enabled", true, true},
		{"device/disabled", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			models := newCodexModelsProbeServer(t)
			account := wireProfileTestAccount(tc.enabled)
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			seen = nil
			_, err := svc.ProbeCodexModelsManifest(context.Background(), account)
			require.NoError(t, err)
			require.Len(t, models.requests, 1)
			require.Len(t, seen, 1, "出站恰好取一次客户端")
			require.Equal(t, tc.want, seen[0].ForceHTTP2, "%+v", seen[0])
		})
	}
}

// 双开账号的 /models 必须与它的 /responses 转发同协议：httpclient 的 transport 设了自定义
// DialContext，不显式 ForceAttemptHTTP2 就退回 HTTP/1.1，同一账号同一主机上会出现两种协议
// 画像（对照 repository/http_upstream.go 里 OpenAI 档默认 ForceAttemptHTTP2=true）。
func TestCodexDeviceWireProfileModelsRequestForcesHTTP2(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       string
		enabled    bool
		apiKey     bool
		setupToken bool
		want       bool
	}{
		{"device/enabled", "device", true, false, false, true},
		{"device/disabled", "device", false, false, false, false},
		{"session/enabled", "session", true, false, false, false},
		// API-key 账号走 api.openai.com，不是 Codex 后端；开关字段写了也不算双开。
		{"device/enabled/api-key", "device", true, true, false, false},
		// setup-token 双开与 OAuth 双开同协议。
		{"device/enabled/setup-token", "device", true, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := wireProfileTestAccount(tc.enabled)
			account.Extra[codexFingerprintModeExtraKey] = tc.mode
			if tc.apiKey {
				account.Type = AccountTypeAPIKey
				account.Credentials = map[string]any{"api_key": "sk-offline"}
			}
			if tc.setupToken {
				account.Type = AccountTypeSetupToken
				account.Credentials = map[string]any{"access_token": "offline-token"}
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			request, _, err := svc.buildCodexModelsManifestRequest(context.Background(), account, "")
			require.NoError(t, err)
			require.Equal(t, tc.want, request.forceHTTP2)
		})
	}
}

func TestCodexDeviceWireProfileModelsManifestAccept(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    string
		enabled bool
		want    string
	}{
		{"device/enabled", "device", true, "*/*"},
		{"device/disabled", "device", false, "application/json"},
		{"session/enabled", "session", true, "application/json"},
		{"off/disabled", "off", false, "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			models := newCodexModelsProbeServer(t)
			account := wireProfileTestAccount(tc.enabled)
			account.Extra[codexFingerprintModeExtraKey] = tc.mode
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			manifest, err := svc.FetchCodexModelsManifest(context.Background(), account, "", "")
			require.NoError(t, err)
			require.NotNil(t, manifest)
			require.Len(t, models.requests, 1)
			require.Equal(t, tc.want, models.requests[0].Header.Get("Accept"))
			require.Equal(t, resolveCodexOutboundIdentity("").version, models.requests[0].Header.Get("version"))
		})
	}
}

// 上游拒绝时探针如实报错，不发 test_complete；也不因缺少网关服务而伪装成功。
func TestCodexDeviceWireProfileFreshSessionProbeFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := wireProfileTestAccount(true)

	models := newCodexModelsProbeServer(t)
	models.status = http.StatusServiceUnavailable
	svc := &AccountTestService{cfg: &config.Config{}, openaiGatewayService: &OpenAIGatewayService{cfg: &config.Config{}}}
	c, rec := newCodexProbeAdminContext(t)
	err := svc.testOpenAIAccountConnection(c, account, "gpt-5.5", "offline", AccountTestModeDefault)
	require.ErrorContains(t, err, "503")
	require.Len(t, models.requests, 1)
	require.Contains(t, rec.Body.String(), `"type":"error"`)
	require.NotContains(t, rec.Body.String(), `"type":"test_complete"`)

	noGateway := &AccountTestService{cfg: &config.Config{}}
	c, rec = newCodexProbeAdminContext(t)
	err = noGateway.testOpenAIAccountConnection(c, account, "gpt-5.5", "offline", AccountTestModeDefault)
	require.ErrorContains(t, err, "not configured")
	require.Len(t, models.requests, 1, "no gateway service, no request")
	require.NotContains(t, rec.Body.String(), `"type":"test_complete"`)
}

func TestCodexDeviceWireProfileWSHTTPBridgeTrace(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	c.Request.Header.Set("x-codex-inference-call-id", "ws-handshake-trace")
	SetOpenAIClientTransport(c, OpenAIClientTransportWS)
	up := &httpUpstreamRecorder{err: errors.New("offline-bridge-captured")}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up}
	account := wireProfileTestAccount(true)
	payload := []byte(`{"type":"response.create","model":"gpt-5.5","input":"offline"}`)
	_, err := svc.proxyOpenAIWSHTTPBridgeTurn(
		context.Background(), c, account, "offline-token", payload, len(payload),
		"gpt-5.5", "", "", "", "", 1, func([]byte) error { return nil },
	)
	require.Error(t, err, "the captured transport failure is wrapped as a bridge failover")
	require.Len(t, up.requests, 1)
	require.Empty(t, up.lastReq.Header.Get("x-codex-inference-call-id"),
		"HTTP outbound transport must not turn a WS handshake trace into an HTTP client trace")

	// Also exclude explicitly marked WS contexts if a bridge changes the request method.
	c.Request.Method = http.MethodPost
	headers := make(http.Header)
	applyCodexDeviceWireProfile(c, account, headers, false)
	require.Empty(t, headers.Get("x-codex-inference-call-id"))
}

// 探针只汇报，不改账号状态：同一个 401 上游，FetchCodexModelsManifest（转发）会把账号临时下线，
// ProbeCodexModelsManifest 不会。noteAuthErrors=false 这条线要有人钉住。
// ProbeCodexModelsManifest 自身不写账号状态（不走转发侧的临时下线 + runtime block）：探针的失败侧
// 由 testOpenAICodexFreshSessionProbe 按原 /responses 探针的规则处理（401→SetError、429→限流窗口，
// 见 TestCodexDeviceWireProfileFreshSessionProbe401MarksAccountErrorLikeOriginalProbe）。
func TestProbeCodexModelsManifest401LeavesStateToCaller(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"detail":{"message":"invalid token"}}`))
	}))
	defer server.Close()
	original := chatgptCodexModelsURL
	chatgptCodexModelsURL = server.URL
	t.Cleanup(func() { chatgptCodexModelsURL = original })

	repo := &codexModelsAccountStateRepo{}
	s := newCodexModels401TestService(repo)
	account := newCodexModelsTestAccount()
	account.Credentials["refresh_token"] = "test-refresh-token"

	_, err := s.ProbeCodexModelsManifest(context.Background(), account)
	require.Error(t, err)
	require.Equal(t, 0, repo.setTempUnschedCalls, "探针的 401 不走转发侧的临时下线")
	require.Equal(t, 0, repo.setErrorCalls)
	require.False(t, s.isOpenAIAccountRuntimeBlocked(account))

	// 正控：另一份同构服务走转发取数，状态机确实会动。
	repo2 := &codexModelsAccountStateRepo{}
	s2 := newCodexModels401TestService(repo2)
	account2 := newCodexModelsTestAccount()
	account2.Credentials["refresh_token"] = "test-refresh-token"
	_, err = s2.FetchCodexModelsManifest(context.Background(), account2, "0.137.0", "")
	require.Error(t, err)
	require.Equal(t, 1, repo2.setTempUnschedCalls)
	require.True(t, s2.isOpenAIAccountRuntimeBlocked(account2))
}

// setup-token 双开的 /models 转发 401 与 OAuth 同处理（handleCodexModelsManifestAccountAuthError 的
// 类型闸口按 OAuthLike）：写入项不依赖刷新生命周期。
func TestFetchCodexModelsManifestSetupTokenDeviceWireProfile401FeedsAccountState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"detail":{"message":"invalid token"}}`))
	}))
	defer server.Close()
	original := chatgptCodexModelsURL
	chatgptCodexModelsURL = server.URL
	t.Cleanup(func() { chatgptCodexModelsURL = original })

	repo := &codexModelsAccountStateRepo{}
	s := newCodexModels401TestService(repo)
	account := wireProfileTestAccount(true)
	account.Type = AccountTypeSetupToken
	account.Credentials = map[string]any{"access_token": "offline-token"}

	_, err := s.FetchCodexModelsManifest(context.Background(), account, "0.137.0", "")
	require.Error(t, err)
	require.GreaterOrEqual(t, repo.setTempUnschedCalls+repo.setErrorCalls, 1, "setup-token 双开的转发 401 必须写账号状态")
	require.True(t, s.isOpenAIAccountRuntimeBlocked(account))
}

// 探针失败侧 = 原 /responses 探针：401 标 StatusError（凭据探针成功时按 CredentialsOnly 清掉）。
func TestCodexDeviceWireProfileFreshSessionProbe401MarksAccountErrorLikeOriginalProbe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name       string
		setupToken bool
	}{{"oauth", false}, {"setup-token", true}} {
		t.Run(tc.name, func(t *testing.T) {
			account := wireProfileTestAccount(true)
			if tc.setupToken {
				account.Type = AccountTypeSetupToken
				account.Credentials = map[string]any{"access_token": "offline-token"}
			}
			repo := &openAIAccountTestRepo{mockAccountRepoForGemini: mockAccountRepoForGemini{
				accountsByID: map[int64]*Account{account.ID: account},
			}}
			models := newCodexModelsProbeServer(t)
			models.status = http.StatusUnauthorized
			// 401 响应即使带着限流头，也只标错误、不写限流窗口（基线 757a40570 的原探针只在 429 时
			// reconcileOpenAI429State）；没有这些头，下面的 rateLimitedID == 0 断言是空的。
			models.headers = http.Header{
				"X-Codex-Primary-Used-Percent":        {"100"},
				"X-Codex-Primary-Reset-After-Seconds": {"3600"},
			}
			up := &httpUpstreamRecorder{err: errors.New("offline-probe-captured")}
			svc := &AccountTestService{
				cfg:                  &config.Config{},
				accountRepo:          repo,
				httpUpstream:         up,
				openaiGatewayService: &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up},
			}

			result, err := svc.RunTestBackground(context.Background(), account.ID, "gpt-5.5")
			require.NoError(t, err)
			require.Equal(t, "failed", result.Status)
			require.False(t, result.CredentialsOnly)
			require.Len(t, models.requests, 1)
			require.Empty(t, up.requests)
			require.Equal(t, account.ID, repo.setErrorID, "401 必须像原探针一样把账号标成错误")
			require.Contains(t, repo.setErrorMsg, "Authentication failed (401)")
			require.Equal(t, int64(0), repo.rateLimitedID, "401 带限流头也不写限流窗口（原探针如此）")
			require.Nil(t, repo.rateLimitedAt)
		})
	}
}

// setup-token 只在双开时接入 /models：非双开 setup-token 维持基线的 502 ACCOUNT_TYPE_UNSUPPORTED、零出站
// （非双开出站逐字节不变）；双开 setup-token 真发一条 /models。
func TestCodexModelsSetupTokenOnlyUnderDeviceWireProfile(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "device/disabled", true: "device/enabled"}[enabled], func(t *testing.T) {
			models := newCodexModelsProbeServer(t)
			account := wireProfileTestAccount(enabled)
			account.Type = AccountTypeSetupToken
			account.Credentials = map[string]any{"access_token": "offline-token"}
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			_, err := svc.FetchCodexModelsManifest(context.Background(), account, "0.137.0", "")
			if !enabled {
				require.Error(t, err)
				require.Contains(t, err.Error(), "OPENAI_CODEX_MODELS_ACCOUNT_TYPE_UNSUPPORTED")
				require.Empty(t, models.requests, "非双开 setup-token：零出站（基线行为）")
				_, err = svc.ProbeCodexModelsManifest(context.Background(), account)
				require.Error(t, err)
				require.Empty(t, models.requests)
				return
			}
			require.NoError(t, err)
			require.Len(t, models.requests, 1)
			require.Equal(t, "*/*", models.requests[0].Header.Get("Accept"))
		})
	}
}

// 管理端那条腿读的就是这个标记：只有双开探针成功才为真。
func TestAccountTestCredentialsOnlyReflectsProbeOutcome(t *testing.T) {
	gin.SetMode(gin.TestMode)
	require.False(t, AccountTestCredentialsOnly(nil))
	for _, tc := range []struct {
		name    string
		enabled bool
		status  int
		want    bool
	}{
		{"device/enabled/200", true, http.StatusOK, true},
		{"device/enabled/401", true, http.StatusUnauthorized, false},
		{"device/disabled", false, http.StatusOK, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := wireProfileTestAccount(tc.enabled)
			repo := &openAIAccountTestRepo{mockAccountRepoForGemini: mockAccountRepoForGemini{
				accountsByID: map[int64]*Account{account.ID: account},
			}}
			models := newCodexModelsProbeServer(t)
			models.status = tc.status
			up := &httpUpstreamRecorder{err: errors.New("offline-probe-captured")}
			svc := &AccountTestService{
				cfg:                  &config.Config{},
				accountRepo:          repo,
				httpUpstream:         up,
				openaiGatewayService: &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up},
			}
			c, _ := newCodexProbeAdminContext(t)
			_ = svc.TestAccountConnection(c, account.ID, "gpt-5.5", "", AccountTestModeDefault)
			require.Equal(t, tc.want, AccountTestCredentialsOnly(c))
		})
	}
}

// 探针复用转发侧的 agent identity task 恢复：task 失效 → 注册新 task → 用新鉴权头重试一次。
func TestProbeCodexModelsManifestAgentIdentityRecoversInvalidTaskOnce(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	account := &Account{
		ID:       4,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"auth_mode":          OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":   key.runtimeID,
			"agent_private_key":  privateKey,
			"task_id":            "task-probe-old",
			"chatgpt_account_id": "acc-agent-probe",
		},
	}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	modelsCalls := 0
	registerCalls := 0
	var assertions []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		if strings.Contains(r.URL.Path, "/task/register") {
			registerCalls++
			_, _ = w.Write([]byte(`{"task_id":"task-probe-new"}`))
			return
		}
		modelsCalls++
		assertions = append(assertions, r.Header.Get("Authorization"))
		if modelsCalls == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"invalid_task_id"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"models":[]}`))
	}))
	defer server.Close()
	originalModelsURL := chatgptCodexModelsURL
	chatgptCodexModelsURL = server.URL
	t.Cleanup(func() { chatgptCodexModelsURL = originalModelsURL })
	originalAuthBase := openAIAgentIdentityAuthAPIBaseURL
	openAIAgentIdentityAuthAPIBaseURL = server.URL
	t.Cleanup(func() { openAIAgentIdentityAuthAPIBaseURL = originalAuthBase })

	s := &OpenAIGatewayService{accountRepo: repo}
	manifest, err := s.ProbeCodexModelsManifest(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, `{"models":[]}`, string(manifest.Body))
	require.Equal(t, 2, modelsCalls)
	require.Equal(t, 1, registerCalls)
	require.Len(t, assertions, 2)
	require.Equal(t, "task-probe-old", decodeAgentAssertionTask(t, assertions[0]))
	require.Equal(t, "task-probe-new", decodeAgentAssertionTask(t, assertions[1]))
}
