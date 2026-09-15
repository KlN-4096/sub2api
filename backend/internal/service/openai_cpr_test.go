//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const (
	cprTestGatewayBase = "http://127.0.0.1:18081"
	cprTestClientKey   = "sk_cpr_client_key"
	cprTestCPRAccount  = "acct_0199c0ffee"
)

func cprTestAdminKey() string { return "admin-" + strings.Repeat("a", 64) }

// newCPRTestAccount 造一个配置齐全的 CPR 中继账号。
func newCPRTestAccount() *Account {
	return &Account{
		ID:          4201,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeCPR,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{
			"base_url":       cprTestGatewayBase,
			"api_key":        cprTestClientKey,
			"admin_api_key":  cprTestAdminKey(),
			"cpr_account_id": cprTestCPRAccount,
		},
		Extra: map[string]any{},
	}
}

// cprTestConfig 复刻生产默认：security.url_allowlist.enabled=false、
// allow_insecure_http=true（config.go:2041,2060）。CPR 跑在本机 http://127.0.0.1，
// 零值 Config 会以 "invalid url scheme: http" 拒绝它。
func cprTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Security.URLAllowlist.AllowPrivateHosts = true
	return cfg
}

func cprTestService() *OpenAIGatewayService {
	return &OpenAIGatewayService{cfg: cprTestConfig(), httpUpstream: &httpUpstreamRecorder{}}
}

// cprTestDetailJSON 复刻 CPR v3.7.1 的 GET /api/admin/accounts/detail 响应形状
// （gateway-api/src/admin/accounts/wire.rs 的 AccountView + AccountQuotaWindowView）。
// 期望值写成独立字面量，不引用生产表——改坏生产代码这些断言必须失败。
func cprTestDetailJSON(status, planType string, windows string) string {
	return `{"code":200,"message":"OK","data":{"account":{` +
		`"id":"` + cprTestCPRAccount + `","email":"a@b.c","provider":"openai",` +
		`"planType":"` + planType + `","planTypeDisplay":"Pro","status":"` + status + `",` +
		`"errorReason":null,"enabled":true,` +
		`"quota":{"refreshedAtDisplay":"3 分钟前","limitReached":false,` +
		`"rateLimitedUntil":null,"windows":[` + windows + `]}}}}`
}

func cprTestWindow(role string, windowSeconds int, usedPercent float64, resetAtDisplay string) string {
	return fmt.Sprintf(`{"key":"codex:%ds","group":"shortTerm","limitId":"codex",`+
		`"limitName":"Codex","role":%q,"windowSeconds":%d,"usedPercent":%v,`+
		`"usedPercentDisplay":"x","limitReached":false,"labelDisplay":"l",`+
		`"windowLabelDisplay":"w","resetAtDisplay":%q}`,
		windowSeconds, role, windowSeconds, usedPercent, resetAtDisplay)
}

// startCPRAdminStub 起一个假的 CPR admin API，记录收到的鉴权头与 query。
func startCPRAdminStub(t *testing.T, status int, payload string) (*httptest.Server, *[]*http.Request) {
	t.Helper()
	seen := make([]*http.Request, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Clone(context.Background()))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(payload))
	}))
	t.Cleanup(server.Close)
	return server, &seen
}

// --- 出站路径 ---

func TestCPROutboundTargetsGatewayNotOpenAI(t *testing.T) {
	account := newCPRTestAccount()
	body := convTestBody(t)
	c := newConvTestContext(t, body)
	svc := cprTestService()

	req, err := svc.buildUpstreamRequest(context.Background(), c, account, body, cprTestClientKey, false, "", false)
	require.NoError(t, err)

	require.Equal(t, "http://127.0.0.1:18081/v1/responses", req.URL.String(),
		"CPR 的 Responses 端点是 {base}/v1/responses")
	require.Equal(t, "Bearer "+cprTestClientKey, req.Header.Get("Authorization"),
		"CPR 只认 Authorization: Bearer <client key>")

	// 指纹链整体挂在 UsesOpenAICodexProtocol() 下面，中继账号必须一条都不沾。
	require.False(t, account.UsesOpenAICodexProtocol(), "前置：cpr 不是 Codex 协议账号")
	require.NotEqual(t, "chatgpt.com", req.Host, "不得强改 Host")
	require.Empty(t, req.Header.Get("chatgpt-account-id"), "账号身份由 CPR 自己注入")
	require.NotContains(t, req.Header.Get("user-agent"), "codex-tui",
		"不得把出站 UA 强改成 Codex 身份——那是 CPR 的职责")
	require.Empty(t, req.Header.Get("Content-Encoding"),
		"zstd 压缩属于真上游形态，中继这一跳不做")
}

func TestCPRRequiresBaseURL(t *testing.T) {
	account := newCPRTestAccount()
	delete(account.Credentials, "base_url")
	body := convTestBody(t)
	c := newConvTestContext(t, body)
	svc := cprTestService()

	// 关键安全性质：缺 base_url 必须报错，**不能**回落到官方端点——
	// 那会把 CPR 的 client key 当成 OpenAI API key 发给 OpenAI。
	req, err := svc.buildUpstreamRequest(context.Background(), c, account, body, cprTestClientKey, false, "", false)
	require.Error(t, err)
	require.Nil(t, req)

	_, err = svc.openAIAlphaSearchURL(account)
	require.Error(t, err, "alpha/search 同样不能有默认值")
}

func TestCPRAccessTokenIsClientKey(t *testing.T) {
	svc := cprTestService()
	token, mode, err := svc.GetAccessToken(context.Background(), newCPRTestAccount())
	require.NoError(t, err)
	require.Equal(t, cprTestClientKey, token)
	require.Equal(t, "apikey", mode)

	missing := newCPRTestAccount()
	delete(missing.Credentials, "api_key")
	_, _, err = svc.GetAccessToken(context.Background(), missing)
	require.Error(t, err)
}

func TestCPRAlphaSearchURL(t *testing.T) {
	svc := cprTestService()
	got, err := svc.openAIAlphaSearchURL(newCPRTestAccount())
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:18081/v1/alpha/search", got,
		"CPR 的搜索端点带 /v1 前缀（openai/router.rs:23）")

	require.True(t, newCPRTestAccount().SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityAlphaSearch))
}

// TestCPRDoesNotDisturbOtherAccountTypes 钉住"不影响 cpr 之外的渠道"。
func TestCPRDoesNotDisturbOtherAccountTypes(t *testing.T) {
	svc := cprTestService()
	body := convTestBody(t)

	oauth := wireProfileTestAccount(false)
	req, err := svc.buildUpstreamRequest(context.Background(), newConvTestContext(t, body), oauth, body, "tok", false, "", false)
	require.NoError(t, err)
	require.Equal(t, "https://chatgpt.com/backend-api/codex/responses", req.URL.String())
	require.Equal(t, "chatgpt.com", req.Host, "OAuth 仍然强改 Host")

	apikey := &Account{
		ID: 4202, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "sk-plain", "base_url": "https://relay.example.com"},
		Extra:       map[string]any{},
	}
	req, err = svc.buildUpstreamRequest(context.Background(), newConvTestContext(t, body), apikey, body, "sk-plain", false, "", false)
	require.NoError(t, err)
	require.Equal(t, "https://relay.example.com/v1/responses", req.URL.String())
}

// --- 额度适配器 ---

func TestCPRQuotaAdapterMapsWindows(t *testing.T) {
	now := time.Now()
	// 5h 窗口 2 小时后重置，7d 窗口 3 天后重置。
	reset5h := now.Add(2 * time.Hour)
	reset7d := now.Add(72 * time.Hour)
	loc := time.FixedZone("UTC+8", 8*60*60)

	payload := cprTestDetailJSON("normal", "pro",
		cprTestWindow("primary", 18000, 12.5, reset5h.In(loc).Format("2006-01-02 15:04:05"))+","+
			cprTestWindow("secondary", 604800, 48.25, reset7d.In(loc).Format("2006-01-02 15:04:05"))+","+
			cprTestWindow("monthly", 2592000, 5, "—"))
	server, seen := startCPRAdminStub(t, http.StatusOK, payload)

	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL

	state, err := NewCPRQuotaService(cprTestConfig()).FetchAccountState(context.Background(), account)
	require.NoError(t, err)

	require.Len(t, *seen, 1)
	require.Equal(t, cprTestAdminKey(), (*seen)[0].Header.Get("x-api-key"), "admin 走 x-api-key，不是 Bearer")
	require.Equal(t, cprTestCPRAccount, (*seen)[0].URL.Query().Get("accountId"))
	require.Equal(t, "/api/admin/accounts/detail", (*seen)[0].URL.Path)

	require.Equal(t, "normal", state.Status)
	require.True(t, state.Schedulable())
	require.Equal(t, "pro", state.PlanType)
	require.NotNil(t, state.RateLimit)

	require.NotNil(t, state.RateLimit.PrimaryWindow)
	require.InDelta(t, 12.5, state.RateLimit.PrimaryWindow.UsedPercent, 0.001)
	require.Equal(t, int64(18000), state.RateLimit.PrimaryWindow.LimitWindowSeconds)
	require.InDelta(t, float64(2*60*60), float64(state.RateLimit.PrimaryWindow.ResetAfterSeconds), 5,
		"reset 秒数由 UTC+8 显示串反解而来")

	require.NotNil(t, state.RateLimit.SecondaryWindow)
	require.InDelta(t, 48.25, state.RateLimit.SecondaryWindow.UsedPercent, 0.001)
	require.Equal(t, int64(604800), state.RateLimit.SecondaryWindow.LimitWindowSeconds)

	// monthly 没有对应槽位，丢弃而不是硬塞进 secondary。
	require.InDelta(t, 48.25, state.RateLimit.SecondaryWindow.UsedPercent, 0.001)
}

// TestCPRExtraUpdatesMatchOAuthDisplayKeys 是"展示与 OAuth 一致"的钉子：
// 同一份 primary/secondary 数据，走 CPR 适配器与走 OAuth 的 /wham/usage
// 必须产出同一组 codex_* 键。
func TestCPRExtraUpdatesMatchOAuthDisplayKeys(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	rateLimit := &OpenAIRateLimit{
		PrimaryWindow:   &OpenAIRateLimitWindow{UsedPercent: 30, LimitWindowSeconds: 18000, ResetAfterSeconds: 600},
		SecondaryWindow: &OpenAIRateLimitWindow{UsedPercent: 70, LimitWindowSeconds: 604800, ResetAfterSeconds: 86400},
	}
	oauthShape := buildCodexWindowExtraUpdates(rateLimit, now)
	require.NotEmpty(t, oauthShape)

	cprShape := buildCPRCodexExtraUpdates(&CPRAccountState{
		Status: "normal", PlanType: "pro", RateLimit: rateLimit, FetchedAt: now,
	})

	for key, want := range oauthShape {
		require.Equal(t, want, cprShape[key], "键 %s 必须与 OAuth 路径一致", key)
	}
	require.Contains(t, cprShape, "codex_5h_used_percent")
	require.Contains(t, cprShape, "codex_7d_used_percent")
	require.Contains(t, cprShape, "codex_usage_updated_at")
	require.Len(t, cprShape, len(oauthShape), "不得多出 OAuth 路径没有的键")
}

// TestCPRExtraUpdatesWithoutWindows 钉住"没有窗口时一个字节都不写"：
//   - mergeAccountExtra 只写不删，推进 codex_usage_updated_at 会把上一次残留的
//     codex_5h_* 标成"刚刷新"，界面显示陈旧百分比且看不出它是旧的。
//   - 只要 map 非空，refreshCPRCodexSnapshot 就会走一次 UpdateExtra；而快照判定
//     恒认为缺失，于是每一次 /usage 请求都要写一次库。曾经塞进来的
//     cpr_account_status / cpr_error_reason 全仓库零消费者，正是这个写放大的来源。
func TestCPRExtraUpdatesWithoutWindows(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	updates := buildCPRCodexExtraUpdates(&CPRAccountState{Status: "error", ErrorReason: "credential_invalid", FetchedAt: now})
	require.Empty(t, updates, "没有窗口就什么都不写，与 OAuth 路径一致")
}

func TestCPRQuotaAdapterErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		payload string
		wantErr error
	}{
		{"admin_key_rejected", http.StatusUnauthorized, `{"code":40103,"message":"管理 API Key 无效","data":null}`, ErrCPRAdminKeyInvalid},
		{"session_required", http.StatusUnauthorized, `{"code":40101,"message":"需要管理员登录","data":null}`, ErrCPRAdminKeyInvalid},
		{"account_gone", http.StatusNotFound, `{"code":40401,"message":"Provider 账号不存在","data":null}`, ErrCPRAccountMissing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := startCPRAdminStub(t, tc.status, tc.payload)
			account := newCPRTestAccount()
			account.Credentials["admin_base_url"] = server.URL

			_, err := NewCPRQuotaService(cprTestConfig()).FetchAccountState(context.Background(), account)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}

	t.Run("missing_config", func(t *testing.T) {
		account := newCPRTestAccount()
		delete(account.Credentials, "admin_api_key")
		_, err := NewCPRQuotaService(cprTestConfig()).FetchAccountState(context.Background(), account)
		require.ErrorIs(t, err, ErrCPRNotConfigured)
	})

	t.Run("not_a_cpr_account", func(t *testing.T) {
		_, err := NewCPRQuotaService(cprTestConfig()).FetchAccountState(context.Background(), wireProfileTestAccount(false))
		require.ErrorIs(t, err, ErrCPRNotConfigured)
	})
}

func TestParseCPRDisplayTime(t *testing.T) {
	got := parseCPRDisplayTime("2026-09-15 20:30:00")
	require.NotNil(t, got)
	// UTC+8 的 20:30 等于 UTC 的 12:30。
	require.Equal(t, "2026-09-15T12:30:00Z", got.UTC().Format(time.RFC3339))

	// CPR 用 "—" 表示没有重置时间；格式变了也必须退化成 nil 而不是猜一个。
	require.Nil(t, parseCPRDisplayTime("—"))
	require.Nil(t, parseCPRDisplayTime(""))
	require.Nil(t, parseCPRDisplayTime("2026-09-15T20:30:00+08:00"))
	require.Nil(t, parseCPRDisplayTime("not a time"))
}

// TestCPRPastResetClampsToZero：快照比重置时间还旧时不能报负数，
// 否则下游会算出一个过去的 reset_at。
func TestCPRPastResetClampsToZero(t *testing.T) {
	now := time.Now()
	loc := time.FixedZone("UTC+8", 8*60*60)
	window := cprQuotaWindow{
		Role:           "primary",
		WindowSeconds:  ptrInt64(18000),
		UsedPercent:    ptrFloat64(10),
		ResetAtDisplay: now.Add(-time.Hour).In(loc).Format("2006-01-02 15:04:05"),
	}
	converted := convertCPRWindow(window, now)
	require.NotNil(t, converted)
	require.Equal(t, int64(0), converted.ResetAfterSeconds)
}

// TestCPRWindowWithoutPercentIsDropped：没有百分比的窗口没有展示价值。
func TestCPRWindowWithoutPercentIsDropped(t *testing.T) {
	now := time.Now()
	require.Nil(t, convertCPRWindow(cprQuotaWindow{Role: "primary", WindowSeconds: ptrInt64(18000)}, now))
	require.Nil(t, convertCPRWindow(cprQuotaWindow{Role: "primary", UsedPercent: ptrFloat64(10)}, now))

	// 一个窗口都对不上时不造空壳。
	require.Nil(t, buildCPRRateLimit(cprAccountQuota{Windows: []cprQuotaWindow{
		{Role: "monthly", WindowSeconds: ptrInt64(2592000), UsedPercent: ptrFloat64(5)},
	}}, now))
}

func TestCPRRateLimitedUntilParsed(t *testing.T) {
	now := time.Now()
	loc := time.FixedZone("UTC+8", 8*60*60)
	until := now.Add(30 * time.Minute)
	var view cprAccountView
	require.NoError(t, json.Unmarshal([]byte(`{"id":"acct_x","status":"rate_limited","quota":{"limitReached":true,`+
		`"rateLimitedUntil":"`+until.In(loc).Format("2006-01-02 15:04:05")+`","windows":[]}}`), &view))

	state := buildCPRAccountState(&view, now)
	require.Equal(t, "rate_limited", state.Status)
	require.False(t, state.Schedulable(), "CPR 的调度器只放行 normal")
	require.NotNil(t, state.RateLimitedUntil)
	require.InDelta(t, until.Unix(), state.RateLimitedUntil.Unix(), 1)
}

// --- 以下测试对应第一轮对抗审查点名的"生产分支零覆盖"---

// TestCPRPassthroughNeverTargetsOpenAI：透传 builder 原来没有 cpr 分支，
// 静默沿用 openaiPlatformAPIURL，会把 CPR 的 client key 明文发给 OpenAI。
func TestCPRPassthroughNeverTargetsOpenAI(t *testing.T) {
	body := convTestBody(t)
	svc := cprTestService()

	req, err := svc.buildUpstreamRequestOpenAIPassthrough(
		context.Background(), newConvTestContext(t, body), newCPRTestAccount(), body, cprTestClientKey)
	require.NoError(t, err)
	require.NotContains(t, req.URL.Host, "openai.com", "中继凭据绝不能发往官方端点")
	require.NotContains(t, req.URL.Host, "chatgpt.com")
	require.Equal(t, "http://127.0.0.1:18081/v1/responses", req.URL.String())

	// 未适配的类型改为显式报错，而不是静默走官方。
	unknown := newCPRTestAccount()
	unknown.Type = "some-future-type"
	_, err = svc.buildUpstreamRequestOpenAIPassthrough(
		context.Background(), newConvTestContext(t, body), unknown, body, "tok")
	require.Error(t, err)
}

// TestCPRInputTokensNeverTargetsOpenAI：/v1/responses/input_tokens 是 Codex CLI
// 每轮都发的正式端点，原来对 cpr 零配置就会把 client key 发给 api.openai.com。
func TestCPRInputTokensNeverTargetsOpenAI(t *testing.T) {
	account := newCPRTestAccount()

	// 第一道：CPR 的路由表没有 input_tokens，应当本地估算、根本不出站。
	require.True(t, shouldEstimateOpenAIInputTokensLocally(account),
		"cpr 没有 input_tokens 端点，必须本地估算")

	// 第二道（纵深防御）：万一还是走到了构造器，也不许回落官方端点。
	body := convTestBody(t)
	svc := cprTestService()
	req, err := svc.buildInputTokensUpstreamRequest(
		context.Background(), newConvTestContext(t, body), account, body, cprTestClientKey)
	require.NoError(t, err)
	require.NotContains(t, req.URL.Host, "openai.com")

	noBase := newCPRTestAccount()
	delete(noBase.Credentials, "base_url")
	_, err = svc.buildInputTokensUpstreamRequest(
		context.Background(), newConvTestContext(t, body), noBase, body, cprTestClientKey)
	require.Error(t, err, "缺 base_url 必须报错而不是回落官方")
}

// TestCPRGetOpenAIBaseURLNeverOfficial 是根因防线：GetOpenAIBaseURL 有二十多个
// 调用点，只要有一个漏适配，回落官方就是凭据泄漏。
func TestCPRGetOpenAIBaseURLNeverOfficial(t *testing.T) {
	require.Equal(t, cprTestGatewayBase, newCPRTestAccount().GetOpenAIBaseURL())

	empty := newCPRTestAccount()
	delete(empty.Credentials, "base_url")
	require.Empty(t, empty.GetOpenAIBaseURL(), "未配置时返回空串，让调用方报错")

	// 既有类型的兜底行为不能变。
	apikey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{}}
	require.Equal(t, "https://api.openai.com", apikey.GetOpenAIBaseURL())
}

// TestCPRUsageDispatchReachesAdapter 是端到端的一条：从 getUsageForAccount 入口
// 进，经真实 JSON 解析，一路到 codex_5h_* / codex_7d_* extra 键。
// 之前 getUsageForAccount 的分派闸只认 oauth，整个适配器是死代码。
func TestCPRUsageDispatchReachesAdapter(t *testing.T) {
	now := time.Now()
	loc := time.FixedZone("UTC+8", 8*60*60)
	payload := cprTestDetailJSON("normal", "pro",
		cprTestWindow("primary", 18000, 12.5, now.Add(2*time.Hour).In(loc).Format("2006-01-02 15:04:05"))+","+
			cprTestWindow("secondary", 604800, 48.25, now.Add(72*time.Hour).In(loc).Format("2006-01-02 15:04:05")))
	server, _ := startCPRAdminStub(t, http.StatusOK, payload)

	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL

	svc := &AccountUsageService{cprQuotaService: NewCPRQuotaService(cprTestConfig())}
	usage, err := svc.getUsageForAccount(context.Background(), account, true)
	require.NoError(t, err, "cpr 必须被分派到 getOpenAIUsage，而不是落到 does-not-support 兜底")
	require.NotNil(t, usage)

	// 断言落在返回给前端的 UsageInfo 上：getOpenAIUsage 内部用的是
	// snapshotOpenAIOutboundAccount 的副本，extra 不会写回调用方的 account，
	// 真正决定界面显示的是这个返回值。
	require.NotNil(t, usage.FiveHour, "CPR 的 primary 窗口要变成 5h 进度条")
	require.InDelta(t, 12.5, usage.FiveHour.Utilization, 0.001)
	require.NotNil(t, usage.SevenDay, "secondary 窗口要变成 7d 进度条")
	require.InDelta(t, 48.25, usage.SevenDay.Utilization, 0.001)
	require.NotNil(t, usage.UpdatedAt)
	// 2 小时后重置（由 UTC+8 显示串反解而来）。
	require.InDelta(t, float64(2*60*60), float64(usage.FiveHour.RemainingSeconds), 30)
}

// TestCPRAdminBaseURLFallsBackToGateway：所有额度测试都显式设了 admin_base_url，
// 缺省回落这条分支原本一次都没跑过。
func TestCPRAdminBaseURLFallsBackToGateway(t *testing.T) {
	account := newCPRTestAccount()
	require.Equal(t, cprTestGatewayBase, account.GetCPRAdminBaseURL())

	account.Credentials["admin_base_url"] = "http://127.0.0.1:19999"
	require.Equal(t, "http://127.0.0.1:19999", account.GetCPRAdminBaseURL())
}

// TestCPRPassthroughSwitchIsForcedOff：透传开关在创建页对 cpr 可见可点、
// 编辑页却不显示，开了就关不掉。后端兜底关死。
func TestCPRPassthroughSwitchIsForcedOff(t *testing.T) {
	account := newCPRTestAccount()
	account.Extra["openai_passthrough"] = true
	require.False(t, account.IsOpenAIPassthroughEnabled())

	// 既有类型不受影响。
	oauth := wireProfileTestAccount(false)
	oauth.Extra["openai_passthrough"] = true
	require.True(t, oauth.IsOpenAIPassthroughEnabled())
}

// TestCPRWebSocketRejected：WS builder 的 default 分支原本指向官方端点。
func TestCPRWebSocketRejected(t *testing.T) {
	svc := cprTestService()
	_, err := svc.buildOpenAIResponsesWSURL(newCPRTestAccount())
	require.Error(t, err, "本版不支持中继账号走 WSv2，必须报错而不是回落官方")

	// OAuth 仍然正常。
	got, err := svc.buildOpenAIResponsesWSURL(wireProfileTestAccount(false))
	require.NoError(t, err)
	require.Contains(t, got, "wss://chatgpt.com")
}

// TestCPRAdminKeyIsRedacted：admin_api_key 是 CPR 网关的全控凭据，
// 权限高于 client key，绝不能回显到前端或落审计日志。
func TestCPRAdminKeyIsRedacted(t *testing.T) {
	require.Contains(t, SensitiveCredentialKeys, "admin_api_key")
	require.True(t, IsSensitiveCredentialKey("admin_api_key"),
		"dto 响应脱敏与审计日志都按这份清单判定")
	require.True(t, IsSensitiveCredentialKey("api_key"))
	require.False(t, IsSensitiveCredentialKey("base_url"), "非敏感字段照常返回")

	// 留空保持不变的语义靠 MergePreservingSensitiveCreds，脱敏后它才能接管。
	merged := MergePreservingSensitiveCreds(
		map[string]any{"admin_api_key": cprTestAdminKey(), "base_url": cprTestGatewayBase},
		map[string]any{"base_url": "http://127.0.0.1:19999"},
	)
	require.Equal(t, cprTestAdminKey(), merged["admin_api_key"], "前端没回传就保留原值")
	require.Equal(t, "http://127.0.0.1:19999", merged["base_url"])
}

// TestCPRPlatformShapeValidated：能造出 anthropic + cpr 的话是个能落库的无效状态。
func TestCPRPlatformShapeValidated(t *testing.T) {
	require.NoError(t, validateCPRAccountShape(PlatformOpenAI, AccountTypeCPR))
	require.Error(t, validateCPRAccountShape(PlatformAnthropic, AccountTypeCPR))
	// 其它类型不受影响。
	require.NoError(t, validateCPRAccountShape(PlatformAnthropic, AccountTypeOAuth))
}

// TestCPRAdminURLHonoursAllowlist：admin_base_url 原本一道 URL 校验都不过，
// 打开白名单后会出现"网关地址被校验、admin 地址随便填"的缺口。
func TestCPRAdminURLHonoursAllowlist(t *testing.T) {
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = true
	cfg.Security.URLAllowlist.UpstreamHosts = []string{"api.openai.com"}

	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = "http://10.0.0.1:18081"

	_, err := NewCPRQuotaService(cfg).FetchAccountState(context.Background(), account)
	require.ErrorIs(t, err, ErrCPRNotConfigured)
}

// --- 第二轮评审补测：这四条对应之前存活的变异 ---

// TestCPRAdminAccountIDIsQueryEscaped：accountId 是管理端自由文本。
// 删掉 url.QueryEscape 后，含 & 的 id 会注入额外 query 参数、含 # 的会被当成 fragment
// 整段丢掉——原 fixture 是纯字母数字，杀不掉这个变异。
func TestCPRAdminAccountIDIsQueryEscaped(t *testing.T) {
	const weirdID = "acct x&role=admin#frag"
	server, seen := startCPRAdminStub(t, http.StatusOK,
		cprTestDetailJSON("normal", "pro", cprTestWindow("primary", 18000, 12.5, "—")))

	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = server.URL
	account.Credentials["cpr_account_id"] = weirdID

	_, err := NewCPRQuotaService(cprTestConfig()).FetchAccountState(context.Background(), account)
	require.NoError(t, err)

	require.Len(t, *seen, 1)
	query := (*seen)[0].URL.Query()
	require.Equal(t, weirdID, query.Get("accountId"), "特殊字符必须原样送达")
	require.Len(t, query, 1, "不能裂出第二个 query 参数")
}

// TestCPRCodexModelsManifestTargetsGateway：Codex CLI 启动的第一条请求就是模型目录，
// 接入 cpr 的全部理由就是这条；而这个分支此前一次都没被跑过。
func TestCPRCodexModelsManifestTargetsGateway(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: cprTestConfig()}

	request, _, err := svc.buildCodexModelsManifestRequest(context.Background(), newCPRTestAccount(), "")
	require.NoError(t, err)
	require.NotContains(t, request.url, "openai.com")
	require.NotContains(t, request.url, "chatgpt.com")
	require.True(t, strings.HasPrefix(request.url, cprTestGatewayBase), "实际目标：%s", request.url)
	require.Contains(t, request.url, "/v1/models")
	require.True(t, request.useAPIKeyUpstream, "cpr 与 apikey 同一上游形状")

	// 缺 base_url 必须报错，绝不回落 chatgptCodexModelsURL。
	broken := newCPRTestAccount()
	delete(broken.Credentials, "base_url")
	_, _, err = svc.buildCodexModelsManifestRequest(context.Background(), broken, "")
	require.Error(t, err)
}

// TestOAuthOnlyGroupPredicate：谓词本身的黑名单边界。
// require_oauth_only 的活判定在 admin_group.go，之前改的 account_service.go 是死代码
// （NewAccountService 从未出现在 wire_gen.go）。
func TestOAuthOnlyGroupPredicate(t *testing.T) {
	require.False(t, accountAllowedInOAuthOnlyGroup(AccountTypeCPR))
	require.False(t, accountAllowedInOAuthOnlyGroup(AccountTypeAPIKey))
	// 不得波及 cpr 之外的既有渠道。
	require.True(t, accountAllowedInOAuthOnlyGroup(AccountTypeOAuth))
	require.True(t, accountAllowedInOAuthOnlyGroup(AccountTypeSetupToken))
	require.True(t, accountAllowedInOAuthOnlyGroup(AccountTypeUpstream))
}

// TestCPRShapeValidatedOnLiveCreatePath：校验必须挂在真正被 wire 构造的
// adminServiceImpl 上。nil 依赖是故意的——校验若没在第一步拦下就会 panic。
func TestCPRShapeValidatedOnLiveCreatePath(t *testing.T) {
	svc := &adminServiceImpl{}

	_, err := svc.CreateAccount(context.Background(), &CreateAccountInput{
		Name:     "bad",
		Platform: PlatformAnthropic,
		Type:     AccountTypeCPR,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cpr")
}

// TestCPRImagesTargetGateway：images 走 {base_url}/v1/images/*，
// 且 base_url 缺失时必须报错而不是回落 api.openai.com。
func TestCPRImagesTargetGateway(t *testing.T) {
	svc := cprTestService()
	account := newCPRTestAccount()
	c := newConvTestContext(t, []byte(`{}`))

	for _, endpoint := range []string{openAIImagesGenerationsEndpoint, openAIImagesEditsEndpoint} {
		req, err := svc.buildOpenAIImagesRequest(
			context.Background(), c, account, []byte(`{"model":"gpt-image-1"}`),
			"application/json", cprTestClientKey, endpoint)
		require.NoError(t, err)
		require.Equal(t, cprTestGatewayBase+endpoint, req.URL.String())
		require.Equal(t, "Bearer "+cprTestClientKey, req.Header.Get("Authorization"))
	}

	broken := newCPRTestAccount()
	delete(broken.Credentials, "base_url")
	_, err := svc.buildOpenAIImagesRequest(
		context.Background(), c, broken, []byte(`{}`), "application/json",
		cprTestClientKey, openAIImagesGenerationsEndpoint)
	require.Error(t, err)

	// 调度侧的能力集也要放行，否则请求根本选不到 cpr 账号。
	require.True(t, account.SupportsOpenAIImageCapability(OpenAIImagesCapabilityBasic))
	require.True(t, account.SupportsOpenAIImageCapability(OpenAIImagesCapabilityNative))
}

// TestCPRUpstreamModelSync：后台「同步上游模型」对 cpr 走 {base_url}/v1/models。
func TestCPRUpstreamModelSync(t *testing.T) {
	validate := func(raw string) (string, error) { return validateOutboundURLWithConfig(cprTestConfig(), raw) }

	req, err := buildOpenAIAPIKeyModelsRequest(context.Background(), newCPRTestAccount(), validate)
	require.NoError(t, err)
	require.Equal(t, cprTestGatewayBase+"/v1/models", req.URL.String())
	require.Equal(t, "Bearer "+cprTestClientKey, req.Header.Get("Authorization"))

	// 缺 base_url 必须报错，绝不回落 api.openai.com。
	broken := newCPRTestAccount()
	delete(broken.Credentials, "base_url")
	_, err = buildOpenAIAPIKeyModelsRequest(context.Background(), broken, validate)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "openai.com")

	// 不影响既有类型：apikey 无 base_url 时仍回落官方。
	apikey := &Account{
		Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "sk-plain"},
	}
	req, err = buildOpenAIAPIKeyModelsRequest(context.Background(), apikey, validate)
	require.NoError(t, err)
	require.Equal(t, "https://api.openai.com/v1/models", req.URL.String())

	// 其它类型仍然 unsupported。
	_, err = buildOpenAIAPIKeyModelsRequest(context.Background(),
		&Account{Platform: PlatformOpenAI, Type: AccountTypeUpstream}, validate)
	require.Error(t, err)
}

// --- 第三轮评审补测 ---

// TestCPRPlatformMismatchStillNeverFallsBackToOfficial：守卫必须用 Type 而非
// IsCPR()。IsCPR() 还要求 platform==openai，而 GetOpenAIBaseURL 对平台错配的
// cpr 账号返回空串——此时 IsCPR() 为 false，守卫不触发，targetURL 停在
// api.openai.com，而 GetAccessToken 会把 client key 发过去。
func TestCPRPlatformMismatchStillNeverFallsBackToOfficial(t *testing.T) {
	svc := cprTestService()
	mismatched := newCPRTestAccount()
	mismatched.Platform = PlatformAnthropic // 写入侧被 validateCPRAccountShape 拦住，这里模拟脏数据
	delete(mismatched.Credentials, "base_url")

	require.False(t, mismatched.IsCPR(), "前置：平台错配时 IsCPR() 为假")
	require.Empty(t, mismatched.GetOpenAIBaseURL(), "前置：拿不到任何回落地址")

	_, err := svc.buildOpenAIImagesRequest(context.Background(), newConvTestContext(t, []byte(`{}`)),
		mismatched, []byte(`{}`), "application/json", cprTestClientKey, openAIImagesGenerationsEndpoint)
	require.Error(t, err, "images 不得回落 api.openai.com")

	_, err = svc.openAIChatCompletionsTargetURL(mismatched)
	require.Error(t, err, "chat/completions 不得回落 api.openai.com")
}

// TestCPRImageTestConnectionNeverTargetsChatGPT：图片「测试连接」的分派此前只认
// apikey，cpr 落到 OAuth 那条——它会设 req.Host = "chatgpt.com" 并发
// GetOpenAIAccessToken()（该 getter 只按 platform 门控、不按 type）。
func TestCPRImageTestConnectionNeverTargetsChatGPT(t *testing.T) {
	for _, platform := range []string{PlatformOpenAI, PlatformAnthropic} {
		t.Run(platform, func(t *testing.T) {
			account := newCPRTestAccount()
			// platform 错配模拟脏数据：分派若用 IsCPR() 会在这里漏到 OAuth 那条。
			account.Platform = platform
			// 改类型后残留的 OAuth 凭据：credentials 是 merge 不是 replace。
			// 分派漏到 OAuth 时，GetOpenAIAccessToken 只按 platform 门控，会把它发出去。
			account.Credentials["access_token"] = "leftover-oauth-token"

			upstream := &queuedHTTPUpstream{responses: []*http.Response{
				newJSONResponse(http.StatusOK, `{"data":[{"b64_json":"aGk="}]}`),
			}}
			svc := &AccountTestService{cfg: cprTestConfig(), httpUpstream: upstream}
			c, _ := newTestContext()

			err := svc.testOpenAIAccountConnection(c, account, "gpt-image-1", "ping", "")

			for _, req := range upstream.requests {
				require.Equal(t, "127.0.0.1:18081", req.URL.Host, "cpr 只能发往自己的网关")
				require.NotEqual(t, "chatgpt.com", req.Host)
			}
			if platform != PlatformOpenAI {
				// 平台错配拿不到 base_url：fail-closed，一个字节都不发。
				require.Error(t, err)
				require.Empty(t, upstream.requests)
				return
			}
			require.NoError(t, err)
			require.Len(t, upstream.requests, 1)
			require.Equal(t, "Bearer "+cprTestClientKey, upstream.requests[0].Header.Get("Authorization"),
				"必须用 client key，不得用残留的 access_token")
		})
	}
}

// TestCPRBlockedFromOAuthOnlyGroupBinding：require_oauth_only 分组不得绑定 cpr 账号。
// CreateGroup 与 UpdateGroup 共用 filterOAuthOnlyGroupAccounts，这里直接测它。
func TestCPRBlockedFromOAuthOnlyGroupBinding(t *testing.T) {
	const oauthID, cprID, apikeyID int64 = 11, 22, 33
	repo := &cprOAuthFilterAccountRepo{accounts: []*Account{
		{ID: oauthID, Platform: PlatformOpenAI, Type: AccountTypeOAuth},
		{ID: cprID, Platform: PlatformOpenAI, Type: AccountTypeCPR},
		{ID: apikeyID, Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
	}}
	svc := &adminServiceImpl{accountRepo: repo}
	group := &Group{ID: 1, Platform: PlatformOpenAI, RequireOAuthOnly: true}

	kept, err := svc.filterOAuthOnlyGroupAccounts(context.Background(), group, []int64{oauthID, cprID, apikeyID})
	require.NoError(t, err)
	require.Equal(t, []int64{oauthID}, kept, "cpr 与 apikey 都必须被挡在 require_oauth_only 之外")

	// 关掉开关就不过滤：本改动只收紧 require_oauth_only，不影响普通分组。
	group.RequireOAuthOnly = false
	kept, err = svc.filterOAuthOnlyGroupAccounts(context.Background(), group, []int64{oauthID, cprID, apikeyID})
	require.NoError(t, err)
	require.Equal(t, []int64{oauthID, cprID, apikeyID}, kept)
}

type cprOAuthFilterAccountRepo struct {
	AccountRepository
	accounts []*Account
}

func (r *cprOAuthFilterAccountRepo) GetByIDs(_ context.Context, ids []int64) ([]*Account, error) {
	out := make([]*Account, 0, len(ids))
	for _, id := range ids {
		for _, acc := range r.accounts {
			if acc.ID == id {
				out = append(out, acc)
			}
		}
	}
	return out, nil
}
