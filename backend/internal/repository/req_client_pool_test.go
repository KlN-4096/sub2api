package repository

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Wei-Shaw/sub2api/internal/pkg/servertiming"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

func forceHTTPVersion(t *testing.T, client *req.Client) string {
	t.Helper()
	transport := client.GetTransport()
	field := reflect.ValueOf(transport).Elem().FieldByName("forceHttpVersion")
	require.True(t, field.IsValid(), "forceHttpVersion field not found")
	require.True(t, field.CanAddr(), "forceHttpVersion field not addressable")
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().String()
}

func TestGetSharedReqClient_ForceHTTP2SeparatesCache(t *testing.T) {
	sharedReqClients = sync.Map{}
	base := reqClientOptions{
		ProxyURL: "http://proxy.local:8080",
		Timeout:  time.Second,
	}
	clientDefault, err := getSharedReqClient(base)
	require.NoError(t, err)

	force := base
	force.ForceHTTP2 = true
	clientForce, err := getSharedReqClient(force)
	require.NoError(t, err)

	require.NotSame(t, clientDefault, clientForce)
	require.NotEqual(t, buildReqClientKey(base), buildReqClientKey(force))
}

func TestGetSharedReqClient_ReuseCachedClient(t *testing.T) {
	sharedReqClients = sync.Map{}
	opts := reqClientOptions{
		ProxyURL: "http://proxy.local:8080",
		Timeout:  2 * time.Second,
	}
	first, err := getSharedReqClient(opts)
	require.NoError(t, err)
	second, err := getSharedReqClient(opts)
	require.NoError(t, err)
	require.Same(t, first, second)
}

func TestGetSharedReqClient_IgnoresNonClientCache(t *testing.T) {
	sharedReqClients = sync.Map{}
	opts := reqClientOptions{
		ProxyURL: " http://proxy.local:8080 ",
		Timeout:  3 * time.Second,
	}
	key := buildReqClientKey(opts)
	sharedReqClients.Store(key, "invalid")

	client, err := getSharedReqClient(opts)
	require.NoError(t, err)

	require.NotNil(t, client)
	loaded, ok := sharedReqClients.Load(key)
	require.True(t, ok)
	require.IsType(t, "invalid", loaded)
}

func TestGetSharedReqClient_ImpersonateAndProxy(t *testing.T) {
	sharedReqClients = sync.Map{}
	opts := reqClientOptions{
		ProxyURL:    "  http://proxy.local:8080  ",
		Timeout:     4 * time.Second,
		Impersonate: true,
	}
	client, err := getSharedReqClient(opts)
	require.NoError(t, err)

	require.NotNil(t, client)
	require.Equal(t, "http://proxy.local:8080|4s|true|false", buildReqClientKey(opts))
}

func TestGetSharedReqClient_InvalidProxyURL(t *testing.T) {
	sharedReqClients = sync.Map{}
	opts := reqClientOptions{
		ProxyURL: "://missing-scheme",
		Timeout:  time.Second,
	}
	_, err := getSharedReqClient(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid proxy URL")
}

func TestGetSharedReqClient_ProxyURLMissingHost(t *testing.T) {
	sharedReqClients = sync.Map{}
	opts := reqClientOptions{
		ProxyURL: "http://",
		Timeout:  time.Second,
	}
	_, err := getSharedReqClient(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "proxy URL missing host")
}

func TestCreateOpenAIReqClient_Timeout120Seconds(t *testing.T) {
	sharedReqClients = sync.Map{}
	client, err := createOpenAIReqClient("http://proxy.local:8080")
	require.NoError(t, err)
	require.Equal(t, 120*time.Second, client.GetClient().Timeout)
}

func TestCreateGeminiReqClient_ForceHTTP2Disabled(t *testing.T) {
	sharedReqClients = sync.Map{}
	client, err := createGeminiReqClient("http://proxy.local:8080")
	require.NoError(t, err)
	require.Equal(t, "", forceHTTPVersion(t, client))
}

func TestInstrumentReqClientRecordsDependency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	collector := servertiming.New(time.Now())
	ctx := servertiming.WithCollector(context.Background(), collector)
	client := instrumentReqClient(req.C())
	response, err := client.R().SetContext(ctx).Get(server.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode)

	header := collector.HeaderValue(time.Now(), "bypass")
	require.True(t, strings.Contains(header, "dep_http;dur="), header)
}

func TestGetSharedReqClient_ImpersonateUsesFirefoxFingerprint(t *testing.T) {
	sharedReqClients = sync.Map{}
	client, err := getSharedReqClient(reqClientOptions{Timeout: time.Second, Impersonate: true})
	require.NoError(t, err)
	// chatgpt.com 的 Cloudflare 会质询 req 内置的 Chrome/120 伪装，必须保持 Firefox 指纹。
	require.Contains(t, client.Headers.Get("User-Agent"), "Firefox/")
	require.NotContains(t, client.Headers.Get("User-Agent"), "Chrome/")
}

// OpenAI 换/刷 token 的客户端（auth.openai.com）不留 cookie：客户端按代理共享，带 jar 就会把
// 一个账号换 token 时收到的 cookie 随同代理下另一个账号的请求发出去。真实 Codex 的
// Cloudflare cookie 仓只认 ChatGPT 主机，auth.openai.com 上从来不带 cookie。
func TestCreateOpenAIReqClientHasNoCookieJar(t *testing.T) {
	sharedReqClients = sync.Map{}
	client, err := createOpenAIReqClient("")
	require.NoError(t, err)
	require.Nil(t, client.GetClient().Jar)
}

// Codex 客户端面照真实 Codex（http-client/src/chatgpt_cloudflare_cookies.rs）：只在 https 的
// ChatGPT 主机上存取 Cloudflare 类 cookie；oai-did、会话 cookie 之类一律不存，免得跨账号串味。
func TestCodexBackendReqClientKeepsOnlyChatGPTCloudflareCookies(t *testing.T) {
	sharedReqClients = sync.Map{}
	client, err := CreateCodexBackendReqClient("")
	require.NoError(t, err)
	jar := client.GetClient().Jar
	require.NotNil(t, jar)

	set := func(raw string) {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		jar.SetCookies(u, []*http.Cookie{
			{Name: "__cf_bm", Value: "a"}, {Name: "_cfuvid", Value: "b"}, {Name: "cf_chl_rc_m", Value: "c"},
			{Name: "oai-did", Value: "device"}, {Name: "__Secure-next-auth.session-token", Value: "s"},
		})
	}
	names := func(raw string) []string {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		var out []string
		for _, c := range jar.Cookies(u) {
			out = append(out, c.Name)
		}
		sort.Strings(out)
		return out
	}
	set("https://chatgpt.com/backend-api/wham/usage")
	require.Equal(t, []string{"__cf_bm", "_cfuvid", "cf_chl_rc_m"}, names("https://chatgpt.com/backend-api/wham/usage"))
	set("https://auth.openai.com/oauth/token")
	require.Empty(t, names("https://auth.openai.com/oauth/token"))
	require.Empty(t, names("http://chatgpt.com/backend-api/wham/usage"), "只认 https")
	require.Empty(t, names("https://evilchatgpt.com/"))
}

// Gemini CLI 客户端与 Codex 客户端面选项相同（30s、不伪装）：cookie 策略必须进缓存键，
// 否则两者共用一个实例；其余客户端维持 req 默认 jar。
func TestCodexBackendReqClientDoesNotShareInstanceWithSameOptionClients(t *testing.T) {
	sharedReqClients = sync.Map{}
	codex, err := CreateCodexBackendReqClient("")
	require.NoError(t, err)
	gemini, err := createGeminiCliReqClient("")
	require.NoError(t, err)
	require.NotSame(t, codex, gemini)
	require.NotNil(t, gemini.GetClient().Jar)
	require.NotSame(t, codex.GetClient().Jar, gemini.GetClient().Jar)
}

// Codex 客户端面（额度查询）不得带浏览器指纹：浏览器伪装会连带一整套公共头与
// 浏览器 UA，与推理面自报的 codex-tui 身份互相矛盾。
// 对照 CreatePrivacyReqClient 确保本用例有区分力——它的伪装目标由
// getSharedReqClient 决定（当前是 Firefox，历史上是 Chrome），所以对照断言只能盯
// 「UA 是不是浏览器」，不能盯 sec-ch-ua 这种 Chromium 专有头。
func TestCreateCodexBackendReqClientSendsNoBrowserFingerprint(t *testing.T) {
	capture := func(build func(string) (*req.Client, error)) http.Header {
		var got http.Header
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Clone()
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		c, err := build("")
		if err != nil {
			t.Fatalf("build client: %v", err)
		}
		if _, err := c.R().Get(srv.URL); err != nil {
			t.Fatalf("request: %v", err)
		}
		return got
	}

	browserOnly := []string{"sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "upgrade-insecure-requests"}

	codex := capture(CreateCodexBackendReqClient)
	for _, h := range browserOnly {
		if v := codex.Get(h); v != "" {
			t.Errorf("codex backend client 不应发浏览器头 %s=%q", h, v)
		}
	}
	for _, browser := range []string{"Chrome", "Firefox", "Safari"} {
		if ua := codex.Get("User-Agent"); strings.Contains(ua, browser) {
			t.Errorf("codex backend client 不应自报浏览器 UA（含 %s）: %q", browser, ua)
		}
	}

	// 区分力对照：隐私设置那条路径确实在做浏览器伪装。
	privacy := capture(CreatePrivacyReqClient)
	privacyUA := privacy.Get("User-Agent")
	if !strings.Contains(privacyUA, "Firefox") && !strings.Contains(privacyUA, "Chrome") {
		t.Fatalf("对照组失效：CreatePrivacyReqClient 未自报浏览器 UA(%q)，本用例无法证明差异", privacyUA)
	}
}
