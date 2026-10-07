package repository

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
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
