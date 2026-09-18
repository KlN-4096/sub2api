//go:build unit

package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"bytes"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"io"
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
		`"outboundProxyEndpoint":"` + cprTestOutboundProxyRaw + `",` +
		`"quota":{"refreshedAtDisplay":"3 分钟前","limitReached":false,` +
		`"rateLimitedUntil":null,"windows":[` + windows + `]}}}}`
}

// wire fixture 里刻意放一个**带凭据**的出站代理。
//
// 两件事一起钉住：
//  1. JSON 字段名 outboundProxyEndpoint 拼错的话功能会静默什么都不做，而只测
//     buildCPRCodexExtraUpdates 的用例照样全绿——解码那一层根本没被走到。
//  2. 生产调用点（buildCPRAccountState 里那次 sanitizeCPROutboundProxy）真的在剥凭据。
//     fixture 放干净值的话，把那次调用换成裸 TrimSpace 测试也全绿，这道防泄漏闸门等于
//     没有测试。CPR 现在返回的确实是脱敏值，但那是上游的行为、不是我们的不变量。
const (
	cprTestOutboundProxyRaw  = "socks5h://cpruser:cprpass@198.51.100.7:1080"
	cprTestOutboundProxyView = "socks5h://198.51.100.7:1080"
)

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

// TestCPRAdminDoesNotFollowRedirects 钉住：白名单只校验初始地址，admin 客户端不能跟着 3xx 把
// x-api-key 带去别的主机（Go 只在跨域时剥 Authorization，自定义头原样带走）。
func TestCPRAdminDoesNotFollowRedirects(t *testing.T) {
	leak, leaked := startCPRAdminStub(t, http.StatusOK, `{"code":200,"data":{"account":{}}}`)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leak.URL+r.URL.RequestURI(), http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	account := newCPRTestAccount()
	account.Credentials["admin_base_url"] = redirector.URL

	_, err := NewCPRQuotaService(cprTestConfig()).FetchAccountState(context.Background(), account)
	require.Error(t, err)
	require.Empty(t, *leaked, "重定向目标一个请求都不该收到，更不该收到 x-api-key")
}

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
	// 走完整解码路径，钉住 JSON 字段名 outboundProxyEndpoint，以及生产调用点上的脱敏。
	// 只测 buildCPR* 的话，这个名字拼错会让功能静默失效而所有断言照样绿；fixture 放干净
	// 值的话，把那次 sanitize 调用换成裸 TrimSpace 也照样绿。
	require.Equal(t, cprTestOutboundProxyView, state.OutboundProxyEndpoint)
	require.NotContains(t, state.OutboundProxyEndpoint, "cprpass")
	require.NotContains(t, state.OutboundProxyEndpoint, "cpruser")
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

	cprShape := buildCPRCodexExtraUpdates(newCPRTestAccount(), &CPRAccountState{
		Status: "normal", PlanType: "pro", RateLimit: rateLimit,
		OutboundProxyEndpoint: "socks5h://198.51.100.7:1080", FetchedAt: now,
	})

	for key, want := range oauthShape {
		require.Equal(t, want, cprShape[key], "键 %s 必须与 OAuth 路径一致", key)
	}
	require.Contains(t, cprShape, "codex_5h_used_percent")
	require.Contains(t, cprShape, "codex_7d_used_percent")
	require.Contains(t, cprShape, "codex_usage_updated_at")

	// 两个例外，都不是本条守卫要防的「无人消费的状态键」：
	//   - cpr_plan_type：不是展示键（前端不读），是订阅优先调度要用的档位，
	//     OAuth 那边存在 credentials.plan_type 里、不经本函数。
	//   - cpr_outbound_proxy：cpr 独有的出口展示。OAuth 账号没有 CPR 这一层，
	//     压根没有对应概念，所以它「多出来」是正当的。
	// 摘掉再比数量，而不是把期望值加二——否则守卫就形同虚设。
	displayShape := make(map[string]any, len(cprShape))
	for key, value := range cprShape {
		if key == CPRPlanTypeExtraKey || key == CPROutboundProxyExtraKey {
			continue
		}
		displayShape[key] = value
	}
	require.Len(t, displayShape, len(oauthShape), "不得多出 OAuth 路径没有的展示键")
}

// TestCPRExtraUpdatesWithoutWindows 钉住"没有窗口时一个字节都不写"：
//   - mergeAccountExtra 只写不删，推进 codex_usage_updated_at 会把上一次残留的
//     codex_5h_* 标成"刚刷新"，界面显示陈旧百分比且看不出它是旧的。
//   - 只要 map 非空，refreshCPRCodexSnapshot 就会走一次 UpdateExtra；而快照判定
//     恒认为缺失，于是每一次 /usage 请求都要写一次库。曾经塞进来的
//     cpr_account_status / cpr_error_reason 全仓库零消费者，正是这个写放大的来源。
func TestCPRExtraUpdatesWithoutWindows(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	updates := buildCPRCodexExtraUpdates(newCPRTestAccount(), &CPRAccountState{Status: "error", ErrorReason: "credential_invalid", FetchedAt: now})
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

// TestCPROnlyMainCodexLimitLineIsUsed：只认主 Codex 限额线（limitId=codex），
// 其余限额族（codex_bengalfox = GPT-5.3-Codex-Spark 等）一律忽略。
//
// 数据取自 pro1 线上实测：官方页面显示的 7d 是主线的 5%，而 Spark 的周额度是 14%。
// 原实现只按 role 选窗口，三个窗口里后写的覆盖先写的，主线 5% 被 Spark 的 0% 顶掉、
// 7d 变成 Spark 的 14%，展示与调度看到的都是另一条线的数。
func TestCPROnlyMainCodexLimitLineIsUsed(t *testing.T) {
	now := time.Now()
	limit := buildCPRRateLimit(cprAccountQuota{Windows: []cprQuotaWindow{
		{Role: "primary", LimitID: "codex", WindowSeconds: ptrInt64(604800), UsedPercent: ptrFloat64(5)},
		{Role: "primary", LimitID: "codex_bengalfox", WindowSeconds: ptrInt64(18000), UsedPercent: ptrFloat64(0)},
		{Role: "secondary", LimitID: "codex_bengalfox", WindowSeconds: ptrInt64(604800), UsedPercent: ptrFloat64(14)},
	}}, now)
	require.NotNil(t, limit)
	require.NotNil(t, limit.PrimaryWindow)
	require.Equal(t, float64(5), limit.PrimaryWindow.UsedPercent, "必须是主线的 5%，不是 Spark 的 0%")
	require.Equal(t, int64(604800), limit.PrimaryWindow.LimitWindowSeconds)
	require.Nil(t, limit.SecondaryWindow, "Spark 的周额度不能占用 secondary 槽位")

	// 主线同时有 5h 与 7d 时两个槽位都填，Spark 依旧被忽略。
	both := buildCPRRateLimit(cprAccountQuota{Windows: []cprQuotaWindow{
		{Role: "primary", LimitID: "codex", WindowSeconds: ptrInt64(18000), UsedPercent: ptrFloat64(3)},
		{Role: "secondary", LimitID: "codex", WindowSeconds: ptrInt64(604800), UsedPercent: ptrFloat64(5)},
		{Role: "secondary", LimitID: "codex_bengalfox", WindowSeconds: ptrInt64(604800), UsedPercent: ptrFloat64(14)},
	}}, now)
	require.NotNil(t, both)
	require.Equal(t, float64(3), both.PrimaryWindow.UsedPercent)
	require.Equal(t, float64(5), both.SecondaryWindow.UsedPercent)

	// 只有非主线窗口时不造空壳——报一个别的限额线的数比没有数更糟。
	require.Nil(t, buildCPRRateLimit(cprAccountQuota{Windows: []cprQuotaWindow{
		{Role: "primary", LimitID: "codex_bengalfox", WindowSeconds: ptrInt64(18000), UsedPercent: ptrFloat64(0)},
		{Role: "secondary", LimitID: "codex_bengalfox", WindowSeconds: ptrInt64(604800), UsedPercent: ptrFloat64(14)},
	}}, now))

	// limitId 缺失时按主线处理：CPR 老版本的单桶视图不带这个字段。
	legacy := buildCPRRateLimit(cprAccountQuota{Windows: []cprQuotaWindow{
		{Role: "primary", WindowSeconds: ptrInt64(18000), UsedPercent: ptrFloat64(7)},
	}}, now)
	require.NotNil(t, legacy)
	require.Equal(t, float64(7), legacy.PrimaryWindow.UsedPercent)
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

// cprPrivacyGroupRepo 只提供调度链路会用到的 GetByID。
type cprPrivacyGroupRepo struct {
	GroupRepository
	group *Group
}

func (r *cprPrivacyGroupRepo) GetByID(_ context.Context, id int64) (*Group, error) {
	if r.group == nil || r.group.ID != id {
		return nil, nil
	}
	return r.group, nil
}

func (r *cprPrivacyGroupRepo) GetByIDLite(ctx context.Context, id int64) (*Group, error) {
	return r.GetByID(ctx, id)
}

// TestCPRAccountSchedulableInRequirePrivacySetGroup 是线上 503 的回归用例
// （group_id=2，错误是不带过滤统计的裸 "no available accounts"）：
// require_privacy_set 分组里唯一的 cpr 账号曾被 recheck 阶段的隐私门丢掉，
// 因为 IsPrivacySet() 对 openai 平台只认 extra.privacy_mode == training_off，
// 而该键只由 OAuth 探测链路写入，cpr 永远拿不到。现在该门对 cpr 不设防。
func TestCPRAccountSchedulableInRequirePrivacySetGroup(t *testing.T) {
	const groupID int64 = 2
	newAccount := func(privacyMode string) *Account {
		acc := newCPRTestAccount()
		acc.GroupIDs = []int64{groupID}
		if privacyMode != "" {
			acc.Extra = map[string]any{"privacy_mode": privacyMode}
		}
		return acc
	}

	// LoadBatchEnabled 与线上一致（配置默认 true）：走 Layer1/2/3 那条链路，
	// 而不是 concurrencyService 缺席时的 selectAccountForModelWithExclusions。
	cfg := &config.Config{}
	cfg.Gateway.Scheduling.LoadBatchEnabled = true
	cfg.Gateway.Scheduling.FallbackMaxWaiting = 100
	cfg.Gateway.Scheduling.FallbackWaitTimeout = 30 * time.Second

	newService := func(acc *Account) *OpenAIGatewayService {
		return &OpenAIGatewayService{
			accountRepo:      schedulerTestOpenAIAccountRepo{accounts: []Account{*acc}},
			cache:            &schedulerTestGatewayCache{},
			cfg:              cfg,
			rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("false"),
			schedulerSnapshot: &SchedulerSnapshotService{
				cache: &openAISnapshotCacheStub{
					snapshotAccounts: []*Account{acc},
					accountsByID:     map[int64]*Account{acc.ID: acc},
				},
				groupRepo: &cprPrivacyGroupRepo{group: &Group{
					ID:                groupID,
					Platform:          PlatformOpenAI,
					RequirePrivacySet: true,
				}},
			},
			concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
		}
	}

	gid := groupID
	// 没有 privacy_mode 的 cpr 账号也必须能被选出来——这正是线上 503 的场景。
	acc := newAccount("")
	selection, _, err := newService(acc).SelectAccountWithSchedulerForCapability(
		context.Background(), &gid, "", "", "gpt-6-astra", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityResponses, false, false, false,
	)
	require.NoError(t, err, "隐私门必须对 cpr 不设防，否则又是那条裸 no available accounts")
	require.NotNil(t, selection)
	require.NotNil(t, selection.Account)
	require.Equal(t, acc.ID, selection.Account.ID)

	// 只开 cpr 这一个口子：其余 openai 账号仍按 privacy_mode 判定。
	require.False(t, (&Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}).IsPrivacySet())
	require.False(t, (&Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}).IsPrivacySet())
	require.True(t, (&Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{"privacy_mode": PrivacyModeTrainingOff},
	}).IsPrivacySet())
}

// TestCPRModelSupportUsesCodexForeignModelBlacklist：空 model_mapping 的 cpr 账号
// 必须与 oauth 一样排除外厂模型。cpr 的上游就是同一个 ChatGPT/Codex 后端，
// 原样透传 claude-*/deepseek-* 之类必然被以不可重试的 400 拒绝且不触发 failover，
// 请求直接死在该账号上（#3662 的原始故障被 cpr 类型重新引入）。
func TestCPRModelSupportUsesCodexForeignModelBlacklist(t *testing.T) {
	// 取自 openAIOAuthForeignModelPrefixes + bare k3。注意黑名单里没有 claude-，
	// 那是刻意的（保守黑名单，未知/自定义别名保持放行），别往里加。
	foreign := []string{"deepseek-v4-pro", "glm-4.6", "gemini-3-pro", "grok-4", "qwen3-max", "k3"}
	servable := []string{"gpt-5.6-luna", "gpt-6-astra", "gpt-5.3-codex-spark", "gpt-image-2"}

	for _, typ := range []string{AccountTypeCPR, AccountTypeOAuth} {
		t.Run(typ, func(t *testing.T) {
			acc := &Account{Platform: PlatformOpenAI, Type: typ}
			for _, model := range foreign {
				require.False(t, acc.IsModelSupported(model), "外厂模型必须被排除: %s", model)
			}
			for _, model := range servable {
				require.True(t, acc.IsModelSupported(model), "Codex 模型必须放行: %s", model)
			}
		})
	}

	// setup-token 从未参与这道黑名单（原判定是 IsOpenAIOAuth()），本次只加 cpr，
	// 不顺带改它——「不影响 cpr 之外的渠道」。
	setupToken := &Account{Platform: PlatformOpenAI, Type: AccountTypeSetupToken}
	require.True(t, setupToken.IsModelSupported("deepseek-v4-pro"))

	// 显式 model_mapping 仍然说了算，黑名单不参与。
	mapped := &Account{Platform: PlatformOpenAI, Type: AccountTypeCPR,
		Credentials: map[string]any{"model_mapping": map[string]any{"deepseek-v4-pro": "gpt-5.6-luna"}}}
	require.True(t, mapped.IsModelSupported("deepseek-v4-pro"))

	// apikey 上游不是 Codex 后端，不受影响。
	apikey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	require.True(t, apikey.IsModelSupported("deepseek-v4-pro"))
}

// TestCPRTransientUpstreamErrorEntersCooldown：CPR 网关抖动（502/503）必须触发
// 账号+模型级冷却。该冷却只在「上游端点按账号各不相同」时才有意义，所以这里
// 按 apikey 而非 oauth 对齐——cpr 的 base_url 是每个账号自己的自建网关。
func TestCPRTransientUpstreamErrorEntersCooldown(t *testing.T) {
	const model = "gpt-6-astra"
	for _, status := range []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			repo := &oauth429RateLimitRepo{}
			rateLimits := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc := &OpenAIGatewayService{rateLimitService: rateLimits}
			rateLimits.SetAccountRuntimeBlocker(svc)

			cpr := newCPRTestAccount()
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(cpr, model))

			// 冷却在连续第 2 次失败才生效（recordFailure: streak>=2 才给 blockUntil），
			// 第 1 次只记流水——单次抖动不该把账号踢出候选。
			svc.handleOpenAIAccountUpstreamError(context.Background(), cpr, status, http.Header{}, nil, model)
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(cpr, model), "单次失败不冷却")
			svc.handleOpenAIAccountUpstreamError(context.Background(), cpr, status, http.Header{}, nil, model)

			require.True(t, svc.isOpenAIAccountRequestRuntimeBlocked(cpr, model),
				"网关 %d 后必须冷却，否则调度器会反复选中同一个 cpr 账号把 failover 预算烧光", status)
			// 只冷却出问题的模型，其余模型照常。
			require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(cpr, "gpt-5.6-luna"))
		})
	}
}

// TestCPRCompactTestConnectionTargetsGateway：compact 测试连接必须走 CPR 网关，
// 之前 switch 只有 oauth / apikey 两支，cpr 落到 default 报
// "Unsupported account type: cpr"，零出站——导致 extra.openai_compact_supported
// 永远探测不到，compact 调度只能按 unknown 靠运气。
func TestCPRCompactTestConnectionTargetsGateway(t *testing.T) {
	account := newCPRTestAccount()
	upstream := &queuedHTTPUpstream{responses: []*http.Response{
		newJSONResponse(http.StatusOK, `{"output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}]}`),
	}}
	svc := &AccountTestService{cfg: cprTestConfig(), httpUpstream: upstream}
	c, _ := newTestContext()

	_ = svc.testOpenAICompactConnection(c, account, "gpt-5.6-luna")

	require.Len(t, upstream.requests, 1, "必须真的发出一条 compact 探针")
	require.Equal(t, "127.0.0.1:18081", upstream.requests[0].URL.Host, "只能发往自己的 CPR 网关")
	require.NotEqual(t, "chatgpt.com", upstream.requests[0].Host)
	require.Equal(t, "Bearer "+cprTestClientKey, upstream.requests[0].Header.Get("Authorization"))
	probeBody, err := io.ReadAll(upstream.requests[0].Body)
	require.NoError(t, err)
	var probe map[string]any
	require.NoError(t, json.Unmarshal(probeBody, &probe))
	require.Equal(t, false, probe["store"],
		"ChatGPT internal API 要求 store:false，少了探针被拒 → openai_compact_supported 假阴性")
	require.Equal(t, true, probe["stream"])

	// base_url 为空必须 fail-closed，绝不回落 api.openai.com。
	noBase := newCPRTestAccount()
	delete(noBase.Credentials, "base_url")
	upstream2 := &queuedHTTPUpstream{}
	svc2 := &AccountTestService{cfg: cprTestConfig(), httpUpstream: upstream2}
	c2, _ := newTestContext()
	_ = svc2.testOpenAICompactConnection(c2, noBase, "gpt-5.6-luna")
	require.Empty(t, upstream2.requests, "缺 base_url 时一个字节都不该发出去")
}

// TestCPRSharesOAuthUpstreamSemantics：按「上游是谁」分流的判定必须包含 cpr。
// CPR 会改写 model / max_output_tokens / temperature / environment_context 时区 /
// web_search user_location / client_metadata.installation_id / stream，但对
// namespace / reasoning / input item id / service_tier 只读不写（已核
// codex-proxy-rs providers/openai/src/transport/request.rs），所以本层这几类
// 归一化对 cpr 与 oauth 必须同样生效，且与 CPR 的改写不相交。
func TestCPRSharesOAuthUpstreamSemantics(t *testing.T) {
	cpr := &Account{Platform: PlatformOpenAI, Type: AccountTypeCPR}
	oauth := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	apikey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	require.True(t, cpr.TargetsChatGPTCodexUpstream())
	require.True(t, oauth.TargetsChatGPTCodexUpstream())
	require.False(t, apikey.TargetsChatGPTCodexUpstream())
	require.False(t, (&Account{Platform: PlatformGrok, Type: AccountTypeOAuth}).TargetsChatGPTCodexUpstream())
	require.False(t, (*Account)(nil).TargetsChatGPTCodexUpstream())

	// #4 namespace 清理
	require.True(t, shouldStripOpenAIResponsesInputNamespaces(cpr, OpenAIUpstreamTransportHTTPSSE, false))
	// #7 reasoning.effort:"none" 保留（false = 不过滤掉）
	require.True(t, shouldPreserveOpenAIResponsesNoneReasoningEffort(cpr))
	require.Equal(t, shouldPreserveOpenAIResponsesNoneReasoningEffort(oauth), shouldPreserveOpenAIResponsesNoneReasoningEffort(cpr),
		"同一个 Codex 客户端请求打 oauth 和 cpr 必须得到同一个 effort 语义")

	// 工具调用项保留 namespace（非 compact、HTTP）：上游按 namespace 解析历史调用，
	// 缺字段直接 400 "Missing namespace for function_call"。
	callBody := []byte(`{"input":[{"type":"function_call","namespace":"n0","name":"one","call_id":"c1","arguments":"{}"}]}`)
	require.True(t, shouldKeepOpenAIResponsesToolCallNamespaces(cpr, OpenAIUpstreamTransportHTTPSSE, false, false, callBody))
	require.Equal(t,
		shouldKeepOpenAIResponsesToolCallNamespaces(oauth, OpenAIUpstreamTransportHTTPSSE, false, false, callBody),
		shouldKeepOpenAIResponsesToolCallNamespaces(cpr, OpenAIUpstreamTransportHTTPSSE, false, false, callBody))

	// compact 路径：工具声明摊平 + GPT-5.6 的 effort max→xhigh 降级，都按上游判定。
	require.True(t, shouldFlattenOpenAIResponsesNamespaces(cpr, OpenAIUpstreamTransportHTTPSSE, false, true))
	require.False(t, shouldFlattenOpenAIResponsesNamespaces(cpr, OpenAIUpstreamTransportHTTPSSE, false, false),
		"非 compact 且未开账号级摊平开关：与 oauth 一样保持 namespace")
	compactCtx, _ := newTestContext()
	compactCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", nil)
	effortBody := []byte(`{"model":"gpt-5.6-luna","reasoning":{"effort":"max"}}`)
	cprEffort, cprChanged, err := normalizeOpenAICodexCompactReasoningEffortForAccount(compactCtx, cpr, effortBody)
	require.NoError(t, err)
	oauthEffort, oauthChanged, err := normalizeOpenAICodexCompactReasoningEffortForAccount(compactCtx, oauth, effortBody)
	require.NoError(t, err)
	require.True(t, oauthChanged, "夹具必须真的触发降级，否则相等断言是空转")
	require.Equal(t, oauthChanged, cprChanged)
	require.Equal(t, string(oauthEffort), string(cprEffort))

	// Responses Lite 头：cpr 走 Codex 那套工具投影，而不是 API Key 的 parallel_tool_calls 分支。
	liteBody := []byte(`{"model":"gpt-5.6-luna","input":"hi","tools":[{"type":"function","name":"x","parameters":{"type":"object"}}]}`)
	cprLite, _, err := normalizeOpenAIResponsesLitePayloadForAccount(liteBody, cpr)
	require.NoError(t, err)
	oauthLite, _, err := normalizeOpenAIResponsesLitePayloadForAccount(liteBody, oauth)
	require.NoError(t, err)
	apikeyLite, _, err := normalizeOpenAIResponsesLitePayloadForAccount(liteBody, apikey)
	require.NoError(t, err)
	require.Equal(t, string(oauthLite), string(cprLite))
	require.NotEqual(t, string(apikeyLite), string(cprLite), "夹具必须能区分两条分支，否则相等断言是空转")

	// 计费 tier：ChatGPT Codex 后端对 Fast 轮常报 default，oauth 按请求 tier 结算；
	// cpr 收到的是同一份响应，不能被降档计费。
	require.Equal(t, ResolveOpenAIServiceTierBilling(oauth, "priority", "default"), ResolveOpenAIServiceTierBilling(cpr, "priority", "default"))
	require.Equal(t, "priority", ResolveOpenAIServiceTierBilling(cpr, "priority", "default").Billing)
	require.Equal(t, "default", ResolveOpenAIServiceTierBilling(apikey, "priority", "default").Billing, "apikey 上游自报 tier 权威，照常降档")

	// 流终态语义：error / response.failed 按 Codex 后端处理。
	require.True(t, openAICodexFailureTerminal(cpr))
	require.Equal(t, openAICodexFailureTerminal(oauth), openAICodexFailureTerminal(cpr))
	require.False(t, openAICodexFailureTerminal(apikey))

	// 调度成本因子：同一份 ChatGPT 订阅，参考倍率与 oauth 相同。
	now := time.Now()
	multiplier := 0.35
	cprRate, cprOK := openAISchedulingRate(cpr, now, &multiplier)
	oauthRate, oauthOK := openAISchedulingRate(oauth, now, &multiplier)
	require.True(t, cprOK)
	require.Equal(t, oauthOK, cprOK)
	require.Equal(t, oauthRate, cprRate)
	apikeyRate, _ := openAISchedulingRate(apikey, now, &multiplier)
	require.NotEqual(t, multiplier, apikeyRate, "apikey 不吃 oauth 参考倍率（无 probe 时回落账号倍率），对照分支可区分")

	// x-codex-beta-features 会话级补注：真实 Codex 每个请求都带；只在压缩回合
	// 才带是本函数要消除的形态。cpr 与 oauth 同，apikey 不补。
	betaCtx, _ := newTestContext()
	betaCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	cprHeader, oauthHeader, apikeyHeader := http.Header{}, http.Header{}, http.Header{}
	applyOpenAICodexBetaFeatures(betaCtx, cpr, cprHeader)
	applyOpenAICodexBetaFeatures(betaCtx, oauth, oauthHeader)
	applyOpenAICodexBetaFeatures(betaCtx, apikey, apikeyHeader)
	require.NotEmpty(t, cprHeader.Get("x-codex-beta-features"))
	require.Equal(t, oauthHeader.Get("x-codex-beta-features"), cprHeader.Get("x-codex-beta-features"))
	require.Empty(t, apikeyHeader.Get("x-codex-beta-features"))
}

// TestCPRAdminSurfacesMatchOAuth：管理端能力判定。
func TestCPRAdminSurfacesMatchOAuth(t *testing.T) {
	require.True(t, canDuplicateAccountType(AccountTypeCPR), "cpr 凭据是静态的，与 apikey 同构，可复制")
	require.True(t, supportsOpenAILongContextBilling(AccountTypeCPR),
		"单账号编辑已放行，批量也必须放行，否则单改能生效批量改 400")
	require.NoError(t, monitorAccountQuotaCapability(&Account{Platform: PlatformOpenAI, Type: AccountTypeCPR}),
		"渠道监控取数已支持 cpr，这道校验是唯一阻塞点")
}

// TestCPRPlanTypeDrivesSubscriptionPriority：cpr 的真实上游就是一份 ChatGPT 订阅，
// 开了「订阅优先」的分组里必须和 oauth 同梯队。此前 IsOpenAIChatGPTSubscription()
// 第一行就是 !IsOpenAIOAuth() → cpr 永远落到 regularAccounts 被降级。
// 档位来自 CPR admin 的 planType（cpr 凭据里没有 plan_type），落在
// extra.cpr_plan_type：走 UpdateExtra 的 JSONB key 级合并，不会跟管理端改凭据打架。
func TestCPRPlanTypeDrivesSubscriptionPriority(t *testing.T) {
	newCPR := func(plan string) *Account {
		acc := &Account{Platform: PlatformOpenAI, Type: AccountTypeCPR}
		if plan != "" {
			acc.Extra = map[string]any{CPRPlanTypeExtraKey: plan}
		}
		return acc
	}

	require.True(t, newCPR("pro").IsOpenAIChatGPTSubscription())
	require.True(t, newCPR("Plus").IsOpenAIChatGPTSubscription(), "档位比较必须忽略大小写")
	require.False(t, newCPR("free").IsOpenAIChatGPTSubscription())
	require.False(t, newCPR("abnormal").IsOpenAIChatGPTSubscription())
	require.False(t, newCPR("").IsOpenAIChatGPTSubscription(), "没探到档位时不得假定是订阅号")

	// 不影响其余类型：oauth 仍读 credentials.plan_type，apikey 仍恒 false。
	oauth := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{"plan_type": "pro"}}
	require.True(t, oauth.IsOpenAIChatGPTSubscription())
	require.False(t, (&Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"plan_type": "pro"}}).IsOpenAIChatGPTSubscription())

	// 档位必须真的被 CPR 状态刷新写进 extra，否则上面全是空判定。
	updates := buildCPRCodexExtraUpdates(newCPR(""), &CPRAccountState{PlanType: "Pro", FetchedAt: time.Now()})
	require.Equal(t, "Pro", updates[CPRPlanTypeExtraKey])

	// CPR 没返回档位时不写键：mergeAccountExtra 只写不删，写空串会把已知档位抹成未知。
	require.NotContains(t, buildCPRCodexExtraUpdates(newCPR("Pro"), &CPRAccountState{FetchedAt: time.Now()}),
		CPRPlanTypeExtraKey)

	// 档位没变也不写：该键不在 schedulerNeutralExtraKeys 里，无条件写会让每次
	// /usage 刷新都触发一次 UpdateExtra + 调度快照重建。
	require.NotContains(t,
		buildCPRCodexExtraUpdates(newCPR("Pro"), &CPRAccountState{PlanType: "pro", FetchedAt: time.Now()}),
		CPRPlanTypeExtraKey, "大小写不同不算变化")
}

// TestCPRFastPolicyHasOwnScope 锁定 Fast/Flex Policy 的 cpr 独立 scope。
//
// 改动前 betaPolicyScopeMatches 只认 isOAuth/isBedrock 两个布尔，apikey 分支
// 是 !isOAuth && !isBedrock，cpr 两者皆否 → 被 apikey scope 误命中，管理员
// 没有任何办法单独给 cpr 配 fast/flex 规则。
func TestCPRFastPolicyHasOwnScope(t *testing.T) {
	block := func(scope string) *OpenAIFastPolicySettings {
		return &OpenAIFastPolicySettings{Rules: []OpenAIFastPolicyRule{{
			ServiceTier:  OpenAIFastTierAny,
			Action:       BetaPolicyActionBlock,
			Scope:        scope,
			ErrorMessage: "blocked by " + scope,
		}}}
	}
	accounts := map[string]*Account{
		"cpr":     {Platform: PlatformOpenAI, Type: AccountTypeCPR},
		"oauth":   {Platform: PlatformOpenAI, Type: AccountTypeOAuth},
		"apikey":  {Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
		"bedrock": {Platform: PlatformAnthropic, Type: AccountTypeBedrock},
	}
	// scope -> 应当命中的账号集合
	want := map[string]map[string]bool{
		BetaPolicyScopeAll:       {"cpr": true, "oauth": true, "apikey": true, "bedrock": true},
		BetaPolicyScopeOAuth:     {"oauth": true},
		BetaPolicyScopeAPIKey:    {"apikey": true},
		BetaPolicyScopeBedrock:   {"bedrock": true},
		OpenAIFastPolicyScopeCPR: {"cpr": true},
	}
	for scope, hits := range want {
		for name, acc := range accounts {
			action, _ := evaluateOpenAIFastPolicyWithSettings(block(scope), 1, acc, "gpt-5", OpenAIFastTierPriority)
			if hits[name] {
				require.Equal(t, BetaPolicyActionBlock, action, "scope=%s 应命中 %s", scope, name)
			} else {
				require.Equal(t, BetaPolicyActionPass, action, "scope=%s 不应命中 %s", scope, name)
			}
		}
	}

	// Anthropic Beta Policy 共用同一个 matcher，但它永远看不到 cpr 账号
	// （cpr 是 openai 平台），所以那两个调用点固定传 false，行为不变。
	require.False(t, betaPolicyScopeMatches(OpenAIFastPolicyScopeCPR, false, false, false),
		"beta policy 侧 cpr scope 恒不命中（fail-closed，且 validScopes 也不放行）")
	require.True(t, betaPolicyScopeMatches(BetaPolicyScopeAPIKey, false, false, false),
		"beta policy 侧 apikey scope 仍命中普通 api key 账号")
}

// TestCPRForwardAppliesCodexBodyNormalizations 从 Forward 入口驱动，钉住转发主线上
// 按「上游是谁」放行给 cpr 的几处 body 归一化：reasoning.mode、推理内容回放、
// input item ID 清洗、namespace 清理（工具调用项保留）。之前这几处只有谓词层
// 断言，把门控改回旧谓词整个包仍全绿。
func TestCPRForwardAppliesCodexBodyNormalizations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstreamSSE := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_cpr\",\"model\":\"gpt-5.6-sol\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\ndata: [DONE]\n\n"
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(upstreamSSE)),
	}}
	cfg := cprTestConfig()
	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     upstream,
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
	}
	c, _ := newOpenAIImageGenerationControlTestContext(true, "codex_cli_rs/0.144.1")
	account := newCPRTestAccount()

	body := []byte(`{
		"model":"gpt-5.6-sol","stream":true,"instructions":"test",
		"reasoning":{"mode":"pro"},
		"input":[
			{"type":"custom_tool_call","id":"fc_wrong_custom","call_id":"call_custom_1","name":"apply_patch","input":"patch"},
			{"type":"reasoning","id":"rs_1","encrypted_content":"enc","summary":[],"content":[{"type":"reasoning_text","text":"thinking"}]},
			{"type":"message","namespace":"n1","role":"user","content":[{"type":"input_text","text":"hi"}]},
			{"type":"function_call","namespace":"n0","name":"one","call_id":"c1","arguments":"{}"}
		]
	}`)

	result, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, upstream.lastReq)
	require.Equal(t, "127.0.0.1:18081", upstream.lastReq.URL.Host, "只能发往自己的 CPR 网关")
	sent := upstream.lastBody

	// reasoning.mode：pro → effort=max，mode 删除
	require.False(t, gjson.GetBytes(sent, "reasoning.mode").Exists())
	require.Equal(t, "max", gjson.GetBytes(sent, "reasoning.effort").String())

	items := gjson.GetBytes(sent, "input").Array()
	require.Len(t, items, 4)
	byType := map[string]gjson.Result{}
	for _, item := range items {
		byType[item.Get("type").String()] = item
	}
	// 推理内容回放：非空 content 数组必须删掉，其它字段保留
	require.False(t, byType["reasoning"].Get("content").Exists())
	require.Equal(t, "enc", byType["reasoning"].Get("encrypted_content").String())
	// item ID 清洗：custom_tool_call 带 fc_ 前缀的假 id 删除
	require.False(t, byType["custom_tool_call"].Get("id").Exists())
	// namespace：普通 input 项清理，工具调用项保留
	require.False(t, byType["message"].Get("namespace").Exists())
	require.Equal(t, "n0", byType["function_call"].Get("namespace").String())
}

// TestCPRHandle429DefersToSameAccountRetry：ChatGPT Codex 后端的瞬时 429 在有界
// 重试窗口内不落库限流（否则下一次重试就不可选，同账号恢复被静默变成换号）。
// cpr 收到的是同一个后端的 429，语义相同。之前只换了谓词没有行为断言，改回
// 旧谓词整个包仍全绿。
func TestCPRHandle429DefersToSameAccountRetry(t *testing.T) {
	repo := &oauth429RateLimitRepo{}
	rateLimits := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	blocker := &OpenAIGatewayService{}
	rateLimits.SetAccountRuntimeBlocker(blocker)

	cpr := newCPRTestAccount()
	headers := http.Header{"Retry-After": []string{"1"}}
	body := []byte(`{"error":{"type":"rate_limit_error","message":"try again"}}`)
	require.True(t, blocker.ShouldRetryOpenAIOAuth429(cpr, headers, body), "夹具必须落在瞬时 429 的重试窗口内")

	rateLimits.handle429(context.Background(), cpr, headers, body)
	require.Equal(t, 0, repo.setRateLimitedCalls, "重试窗口内不得把 cpr 账号标成限流")

	// 对照：apikey 上游不是 Codex 后端，同一份 429 照常落库。
	apikey := &Account{ID: 4202, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	rateLimits.handle429(context.Background(), apikey, headers, body)
	require.Equal(t, 1, repo.setRateLimitedCalls)
}

// TestCPRManualPlanTypeOverridesProbe：凭据里人工写的 plan_type 是显式覆盖，
// 优先于 CPR 探测落在 extra 的值；空白视为未覆盖。与列表页 getAccountPlanType
// 的回退顺序一致。
func TestCPRManualPlanTypeOverridesProbe(t *testing.T) {
	acc := newCPRTestAccount()
	acc.Extra[CPRPlanTypeExtraKey] = "pro"
	require.True(t, acc.IsOpenAIChatGPTSubscription())
	acc.Credentials["plan_type"] = "free"
	require.False(t, acc.IsOpenAIChatGPTSubscription(), "人工覆盖优先于探测值")
	acc.Credentials["plan_type"] = "  "
	require.True(t, acc.IsOpenAIChatGPTSubscription(), "空白视为未覆盖，回退到探测值")
}

// TestCPRJoinsOpenAIUpstreamCostPool：成本因子与低倍率优先排序的资格闸门要纳入
// cpr。之前只钉了 openAISchedulingRate，这两处闸门改回旧谓词整包仍全绿。
func TestCPRJoinsOpenAIUpstreamCostPool(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	apikey := upstreamCostTestAccount(1, UpstreamBillingProbeStatusOK, 2.0, now.Add(-time.Minute), 30*time.Minute)
	cpr := newCPRTestAccount()

	multiplier := 0.35
	factors := openAIUpstreamCostFactors([]*Account{apikey, cpr}, now, &multiplier)
	require.NotEqual(t, openAIUpstreamCostNeutralFactor, factors[apikey.ID], "cpr 进池后样本≥2，apikey 的因子不再中性")
	require.NotEqual(t, openAIUpstreamCostNeutralFactor, factors[cpr.ID])
	require.Greater(t, factors[cpr.ID], factors[apikey.ID], "0.35 的 cpr 比 2.0 的 apikey 便宜")

	order := newOpenAILegacyUpstreamRateOrder([]*Account{apikey, cpr}, now, &multiplier)
	require.True(t, order.enabled, "两档不同倍率才启用低倍率优先")
	require.Negative(t, order.compare(cpr, apikey))

	// 对照：cpr 被闸门排除（= 改回旧谓词）时只剩 1 个样本，全部中性、排序禁用。
	only := openAIUpstreamCostFactors([]*Account{apikey}, now, &multiplier)
	require.Equal(t, openAIUpstreamCostNeutralFactor, only[apikey.ID])
	require.False(t, newOpenAILegacyUpstreamRateOrder([]*Account{apikey}, now, &multiplier).enabled)
}

// TestCPRImages429CarriesSameAccountRetryWindow：images 路径的 429 闸门与 /responses
// 主线同源。cpr 进了 429 延迟（handle429 / markOpenAIOAuth429RateLimited 在窗口内
// 不落库不熔断），这里若仍按旧谓词判成不可同账号重试，cpr 在 2 分钟窗口内既不
// 冷却也不重试，调度器会反复选中它反复 429。
func TestCPRImages429CarriesSameAccountRetryWindow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"model":"gpt-image-1","prompt":"draw a cat","response_format":"b64_json"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	svc := &OpenAIGatewayService{cfg: cprTestConfig(), httpUpstream: &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": []string{"1"}, "X-Request-Id": []string{"req_img_cpr_429"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"rate limited"}}`)),
	}}}
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	cpr := newCPRTestAccount()

	result, err := svc.ForwardImages(context.Background(), c, cpr, body, parsed, "")

	require.Nil(t, result)
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.True(t, failoverErr.RetryableOnSameAccount, "与 oauth 同：瞬时 429 在窗口内同账号重试")
	require.Equal(t, time.Second, failoverErr.SameAccountRetryDelay)
}

// TestCPR429FastPathSemanticsMatchOAuth：/responses 主线的 429 快路径三件套
// 对 cpr 与 oauth 同语义，apikey 不走这套。
func TestCPR429FastPathSemanticsMatchOAuth(t *testing.T) {
	repo := &oauth429RateLimitRepo{}
	rateLimits := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	svc := &OpenAIGatewayService{rateLimitService: rateLimits}
	rateLimits.SetAccountRuntimeBlocker(svc)
	headers := http.Header{"Retry-After": []string{"1"}}
	body := []byte(`{"error":{"type":"rate_limit_error","message":"try again"}}`)
	cpr := newCPRTestAccount()
	oauth := &Account{ID: 4204, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	apikey := &Account{ID: 4205, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	require.True(t, svc.shouldRetryOpenAIOAuth429OnSameAccountWithResponse(cpr, http.StatusTooManyRequests, false, headers, body))
	require.Equal(t,
		svc.shouldRetryOpenAIOAuth429OnSameAccountWithResponse(oauth, http.StatusTooManyRequests, false, headers, body),
		svc.shouldRetryOpenAIOAuth429OnSameAccountWithResponse(cpr, http.StatusTooManyRequests, false, headers, body))
	require.False(t, svc.shouldRetryOpenAIOAuth429OnSameAccountWithResponse(apikey, http.StatusTooManyRequests, false, headers, body))

	svc.markOpenAIOAuth429RateLimited(context.Background(), cpr, headers, body)
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(cpr), "窗口内不得熔断")
	require.Equal(t, 0, repo.setRateLimitedCalls)

	switches := openAIOAuth429MaxAccountAttempts + openAIOAuth429StormMaxAccountSwitches
	require.True(t, svc.ShouldStopOpenAIOAuth429Failover(cpr, http.StatusTooManyRequests, switches, nil))
	require.Equal(t,
		svc.ShouldStopOpenAIOAuth429Failover(oauth, http.StatusTooManyRequests, switches, nil),
		svc.ShouldStopOpenAIOAuth429Failover(cpr, http.StatusTooManyRequests, switches, nil))
	require.False(t, svc.ShouldStopOpenAIOAuth429Failover(apikey, http.StatusTooManyRequests, switches, nil))
}

// TestCPRAlphaSearch429CarriesSameAccountRetryWindow：/alpha/search 两条分支的 429
// 闸门与 /responses 主线同源（同 images）。
func TestCPRAlphaSearch429CarriesSameAccountRetryWindow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"id":"search-session","model":"gpt-5.6-sol","commands":{}}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", bytes.NewReader(body))
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
			"Retry-After":  []string{"1"},
			"X-Request-Id": []string{"req_alpha_cpr_429"},
		},
		Body: io.NopCloser(strings.NewReader(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"rate limited"}}`)),
	}}
	service := &OpenAIGatewayService{cfg: cprTestConfig(), httpUpstream: upstream}
	cpr := newCPRTestAccount()

	result, err := service.ForwardAlphaSearch(context.Background(), c, cpr, body)

	require.Nil(t, result)
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.True(t, failoverErr.RetryableOnSameAccount, "与 oauth/setup-token 同：瞬时 429 在窗口内同账号重试")
	require.Equal(t, "127.0.0.1:18081", upstream.lastReq.URL.Host)
}

// TestCPRPlanGatedModelCoolsDownLikeOAuth：ChatGPT 账号不支持的模型（400 plan-gated）
// 对 cpr 与 oauth 同样进入按模型冷却；apikey 上游不是 Codex 后端，不认这条文案。
func TestCPRPlanGatedModelCoolsDownLikeOAuth(t *testing.T) {
	repo := &oauth429RateLimitRepo{}
	rateLimits := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	body := []byte(`{"error":{"message":"The 'gpt-5.4' model is not supported when using Codex with a ChatGPT account."}}`)
	cpr := newCPRTestAccount()
	oauth := &Account{ID: 4206, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	apikey := &Account{ID: 4207, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	require.True(t, rateLimits.HandleUpstreamModelNotFound(context.Background(), cpr, "gpt-5.4", http.StatusBadRequest, body))
	require.Equal(t,
		rateLimits.HandleUpstreamModelNotFound(context.Background(), oauth, "gpt-5.4", http.StatusBadRequest, body),
		rateLimits.HandleUpstreamModelNotFound(context.Background(), cpr, "gpt-5.4", http.StatusBadRequest, body))
	require.False(t, rateLimits.HandleUpstreamModelNotFound(context.Background(), apikey, "gpt-5.4", http.StatusBadRequest, body))
}

// TestOpenAITurnStateOverrideAppliesToCodexUpstreams 锁定账号级 turn-state 覆写的适用范围
// 与优先级：oauth / setup-token / cpr 三种落到 ChatGPT Codex 后端的账号都生效，
// 其余上游一个字节都不碰；覆写必须能盖过守卫的剥离结果。
func TestOpenAITurnStateOverrideAppliesToCodexUpstreams(t *testing.T) {
	const blob = "gAAAAABqqrNHYSOlO_EUJI-hlduVBqJ8slR-floDb7J"
	svc := &OpenAIGatewayService{}
	withOverride := func(platform, accType string) *Account {
		return &Account{
			Platform: platform,
			Type:     accType,
			Extra: map[string]any{openAITurnStateOverrideExtraKey: map[string]any{
				turnStateTestModel: blob,
			}},
		}
	}

	// 适用：三种都会落到 ChatGPT Codex 后端
	for _, tc := range []struct{ platform, accType string }{
		{PlatformOpenAI, AccountTypeOAuth},
		{PlatformOpenAI, AccountTypeSetupToken},
		{PlatformOpenAI, AccountTypeCPR},
	} {
		acc := withOverride(tc.platform, tc.accType)
		require.Equal(t, blob, acc.OpenAICodexTurnStateOverride(turnStateTestModel), "%s/%s 应支持覆写", tc.platform, tc.accType)

		h := http.Header{}
		h.Set(openAICodexTurnStateHeader, "客户端自己回带的旧值")
		svc.applyOpenAICodexTurnStateOverrideHeader(newTurnStateTestCtx(), acc, h)
		require.Equal(t, blob, h.Get(openAICodexTurnStateHeader), "覆写必须盖过客户端回带值")

		// 守卫剥光之后（头已不存在）覆写照样要写进去，否则「配了但不生效」
		stripped := http.Header{}
		svc.applyOpenAICodexTurnStateOverrideHeader(newTurnStateTestCtx(), acc, stripped)
		require.Equal(t, blob, stripped.Get(openAICodexTurnStateHeader), "守卫剥离后覆写仍须生效")

		require.Equal(t, blob,
			svc.applyOpenAICodexTurnStateOverrideWSManualOnly(newTurnStateTestCtx(), acc, ""), "值形态（WS 路径）同样生效")
	}

	// 不适用：上游不是 Codex 后端的账号，一个字节都不能碰
	for _, tc := range []struct{ platform, accType string }{
		{PlatformOpenAI, AccountTypeAPIKey},
		{PlatformAnthropic, AccountTypeOAuth},
		{PlatformAnthropic, AccountTypeBedrock},
	} {
		acc := withOverride(tc.platform, tc.accType)
		require.Empty(t, acc.OpenAICodexTurnStateOverride(turnStateTestModel), "%s/%s 不该支持覆写", tc.platform, tc.accType)

		h := http.Header{}
		svc.applyOpenAICodexTurnStateOverrideHeader(newTurnStateTestCtx(), acc, h)
		require.Empty(t, h.Get(openAICodexTurnStateHeader))
		require.Equal(t, "原值", svc.applyOpenAICodexTurnStateOverrideWSManualOnly(newTurnStateTestCtx(), acc, "原值"))
	}

	// 未配置 = 功能不存在，出站行为与改动前逐字节一致
	plain := &Account{Platform: PlatformOpenAI, Type: AccountTypeCPR}
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, "客户端自己回带的值")
	svc.applyOpenAICodexTurnStateOverrideHeader(newTurnStateTestCtx(), plain, h)
	require.Equal(t, "客户端自己回带的值", h.Get(openAICodexTurnStateHeader), "未配置时不得改写")
	require.Equal(t, "原值", svc.applyOpenAICodexTurnStateOverrideWSManualOnly(newTurnStateTestCtx(), plain, "原值"))
	require.Empty(t, (*Account)(nil).OpenAICodexTurnStateOverride(turnStateTestModel), "nil 账号不 panic")
}

// newTurnStateTestCtx 造一个最小 gin 上下文：覆写解析会往里写注入标记。
// 必须带模型——覆写表按模型取票，读不到本次模型就一律不注入。
func newTurnStateTestCtx() *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	SetOpsUpstreamModel(c, turnStateTestModel)
	return c
}

// TestValidateOpenAITurnStateOverrideExtra 钉住写入校验：覆写表是 {模型: blob}，
// 每条 blob 只放行 Fernet 信封形状。
func TestValidateOpenAITurnStateOverrideExtra(t *testing.T) {
	// 真实捕获样本（pro3，292 字符）
	const real = "gAAAAABqqrNHYSOlO_EUJI-hlduVBqJ8slR-floDb7J-ZYvvLXj7WV7dOZ_zk10RDMl_N4dRvG0UqxWR19XdSGbeHFUEAzwv7yQBADQrB1QhpOKkfcUPeSy2qsvZIvq__OHHoF2yCZfSTPq6YvkKahwLUxkeORhQZ9Ug86sMJwkrJXUefsa6fTpRqzZSN7SLphKU-6Ys6FV3GveSXjgk0UcCaKvfShFj4_EmGriyCb-JVoU0D8LJbjsClcivKgDNu1jfZfF-6q8VXHGF1Uck7vDVXdiNh1kRXw=="

	// 两端空白剔掉；空 blob = 清空这条票，丢弃但不报错。
	extra := map[string]any{openAITurnStateOverrideExtraKey: map[string]any{
		"  gpt-6-astra  ": "  " + real + "  ",
		"blank-blob":      "   ",
	}}
	require.NoError(t, ValidateOpenAITurnStateOverrideExtra(extra))
	require.Equal(t, map[string]any{"gpt-6-astra": real}, extra[openAITurnStateOverrideExtraKey])

	// 空模型名是畸形输入而不是「清空」，静默丢掉会让管理员看到「保存成功但未配置」。
	require.Error(t, ValidateOpenAITurnStateOverrideExtra(
		map[string]any{openAITurnStateOverrideExtraKey: map[string]any{"   ": real}}))

	// 取值按 EqualFold 匹配，而 Go map 遍历顺序随机：留着大小写冲突的键，
	// 注出去的是哪条每次调用都可能不同。当场拒掉。
	require.Error(t, ValidateOpenAITurnStateOverrideExtra(
		map[string]any{openAITurnStateOverrideExtraKey: map[string]any{"GPT-5": real, "gpt-5": real}}))

	// 整表空了就把键删掉（否则空对象会被当成"已配置"存进 DB）
	blank := map[string]any{openAITurnStateOverrideExtraKey: map[string]any{"m": "   "}}
	require.NoError(t, ValidateOpenAITurnStateOverrideExtra(blank))
	require.NotContains(t, blank, openAITurnStateOverrideExtraKey)

	// 显式 null 等价于未配置，别在 extra 里留个 null
	nulled := map[string]any{openAITurnStateOverrideExtraKey: nil}
	require.NoError(t, ValidateOpenAITurnStateOverrideExtra(nulled))
	require.NotContains(t, nulled, openAITurnStateOverrideExtraKey)

	// 没这个键 = 不干预
	require.NoError(t, ValidateOpenAITurnStateOverrideExtra(map[string]any{"other": 1}))
	require.NoError(t, ValidateOpenAITurnStateOverrideExtra(nil))

	for name, bad := range map[string]any{
		"旧的单字符串形态": real,
		"整体非对象":    123,
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, ValidateOpenAITurnStateOverrideExtra(
				map[string]any{openAITurnStateOverrideExtraKey: bad}))
		})
	}
	for name, bad := range map[string]any{
		"非字符串":     123,
		"不是base64": "这不是 base64!!",
		"太短":       "gAAA",
		"版本字节不对":   base64.URLEncoding.EncodeToString(append([]byte{0x79}, make([]byte, 80)...)),
		"超长":       strings.Repeat("A", maxOpenAITurnStateOverrideLen+1),
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, ValidateOpenAITurnStateOverrideExtra(
				map[string]any{openAITurnStateOverrideExtraKey: map[string]any{"gpt-6-astra": bad}}))
		})
	}

	// 条目数上限
	tooMany := map[string]any{}
	for i := 0; i <= maxOpenAITurnStateOverrideModels; i++ {
		tooMany[string(rune('a'+i))] = real
	}
	require.Error(t, ValidateOpenAITurnStateOverrideExtra(
		map[string]any{openAITurnStateOverrideExtraKey: tooMany}))
}

// TestUsageCodexTurnStateRecording 锁定使用记录里两列的取值来源。
func TestUsageCodexTurnStateRecording(t *testing.T) {
	const blob = "gAAAAABqqrNHYSOlO_EUJI"

	h := http.Header{}
	require.Nil(t, usageCodexTurnStatePtr(h), "上游没回该头时记 NULL")
	require.Nil(t, usageCodexTurnStatePtr(nil))
	h.Set(openAICodexTurnStateHeader, blob)
	require.Equal(t, blob, *usageCodexTurnStatePtr(h))

	cpr := &Account{Platform: PlatformOpenAI, Type: AccountTypeCPR}
	require.False(t, *usageCodexTurnStateOverriddenPtr(cpr, ""), "本次没注入 = false")
	require.True(t, *usageCodexTurnStateOverriddenPtr(cpr, turnStateSourceManual), "注入了 = true")
	require.Nil(t, usageCodexTurnStateSourcePtr(cpr, ""), "没注入时来源记 NULL")
	require.Equal(t, turnStateSourceAuto, *usageCodexTurnStateSourcePtr(cpr, turnStateSourceAuto))

	apikey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	require.Nil(t, usageCodexTurnStateOverriddenPtr(apikey, turnStateSourceAuto), "不适用的账号类型记 NULL")
	require.Nil(t, usageCodexTurnStateSourcePtr(apikey, turnStateSourceAuto))
	require.Nil(t, usageCodexTurnStateOverriddenPtr(nil, turnStateSourceManual))
	require.Nil(t, usageCodexTurnStateSourcePtr(nil, turnStateSourceManual))
}

// TestCPRExtraUpdatesCarryOutboundProxy 钉住 CPR 侧出站代理写进展示键。
//
// cpr 账号真正的出口 IP 由 CPR 决定：sub2api 账号上绑的 proxy 只作用于
// sub2api→CPR 那一跳，而那一跳是 127.0.0.1。不把 CPR 的出口显示出来，账号页就
// 回答不了「这个号现在从哪出去」——排 turn-state / 降智问题的第一个问题。
//
// CPR 返回的 endpoint 已由它自己脱敏（不含 user:pass），所以可以原样存进 extra。
func TestCPRExtraUpdatesCarryOutboundProxy(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	const endpoint = "socks5h://24.120.102.167:35444"

	updates := buildCPRCodexExtraUpdates(newCPRTestAccount(), &CPRAccountState{
		OutboundProxyEndpoint: endpoint, FetchedAt: now,
	})
	require.Equal(t, endpoint, updates[CPROutboundProxyExtraKey])

	known := newCPRTestAccount()
	if known.Extra == nil {
		known.Extra = map[string]any{}
	}
	known.Extra[CPROutboundProxyExtraKey] = endpoint
	require.NotContains(t,
		buildCPRCodexExtraUpdates(known, &CPRAccountState{OutboundProxyEndpoint: endpoint, FetchedAt: now}),
		CPROutboundProxyExtraKey, "没变就不写，否则每次 /usage 刷新都要写一次库")

	require.NotContains(t,
		buildCPRCodexExtraUpdates(known, &CPRAccountState{FetchedAt: now}),
		CPROutboundProxyExtraKey, "空值不写：mergeAccountExtra 只写不删，写空串会把已知出口抹成未知")
}

// TestSanitizeCPROutboundProxy 钉住:存进 extra 之前一定把凭据剥掉。
//
// CPR 现在返回的是脱敏值,但那是上游的行为、不是我们能保证的不变量。CPR 换版本、
// 换配置,或某个账号配的是带认证的 socks5,`socks5h://user:pass@host:port` 就会落进
// accounts.extra、随账号列表接口下发、在管理页明文显示密码。本仓库对代理 URL 的硬
// 约定(internal/pkg/proxyurl 包文档)本来就不允许这么处理。
func TestSanitizeCPROutboundProxy(t *testing.T) {
	require.Equal(t, "socks5h://host:1080", sanitizeCPROutboundProxy("acct-1", "socks5h://u:p@host:1080"),
		"凭据必须剥掉")
	require.Equal(t, "socks5h://host:1080", sanitizeCPROutboundProxy("acct-1", "socks5h://onlyuser@host:1080"),
		"只有用户名也要剥")
	require.Equal(t, "socks5h://host:1080", sanitizeCPROutboundProxy("acct-1", "  socks5h://host:1080  "))
	// socks5 会被 proxyurl.Parse 升级成 socks5h(防 DNS 泄漏),这里跟着走同一套。
	require.Equal(t, "socks5h://host:1080", sanitizeCPROutboundProxy("acct-1", "socks5://host:1080"))
	require.Equal(t, "http://host:8080", sanitizeCPROutboundProxy("acct-1", "http://host:8080"))

	// 凭据不只藏在 userinfo 里。没有 userinfo 就把原串放行的话，这几条会整串落进
	// accounts.extra、随账号列表接口下发、在管理页明文显示。
	require.Equal(t, "http://host:8080", sanitizeCPROutboundProxy("acct-1", "http://host:8080/?token=secret"),
		"query 里的凭据也要剥")
	require.Equal(t, "socks5h://host:1080", sanitizeCPROutboundProxy("acct-1", "socks5h://host:1080/u:p"),
		"path 也不该带出去")
	require.Equal(t, "http://host:8080", sanitizeCPROutboundProxy("acct-1", "http://host:8080#frag"))

	require.Empty(t, sanitizeCPROutboundProxy("acct-1", ""))
	require.Empty(t, sanitizeCPROutboundProxy("acct-1", "not a url"), "解不开就整个丢弃,不存半个")
	require.Empty(t, sanitizeCPROutboundProxy("acct-1", "ftp://host:21"), "白名单外的协议不存")
}

// TestCPRAccountStateSanitizesOutboundProxy 钉住生产调用点：翻译 CPR 视图的那一步就
// 已经把凭据剥了，落进 extra 的值永远是干净的。
//
// 刻意断言 buildCPRAccountState 而不是 buildCPRCodexExtraUpdates：后者的入参是
// CPRAccountState，测试自己先 sanitize 一遍再喂进去就成了「我洗过的值传过去还是干净的」
// ——把生产端唯一的脱敏调用点换成裸 TrimSpace，那种测试全绿。
func TestCPRAccountStateSanitizesOutboundProxy(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	state := buildCPRAccountState(&cprAccountView{
		ID:                    "acct-1",
		Status:                CPRAccountStatusNormal,
		OutboundProxyEndpoint: "socks5h://u:p@24.120.102.167:35444",
	}, now)
	require.NotNil(t, state)
	require.Equal(t, "socks5h://24.120.102.167:35444", state.OutboundProxyEndpoint)

	// 写入侧只是把已经干净的值原样带过去，一起过一遍确认没有再引入一条旁路。
	updates := buildCPRCodexExtraUpdates(newCPRTestAccount(), state)
	stored, _ := updates[CPROutboundProxyExtraKey].(string)
	require.Equal(t, "socks5h://24.120.102.167:35444", stored)
}
