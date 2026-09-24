package repository

import (
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"
	"github.com/Wei-Shaw/sub2api/internal/pkg/servertiming"

	"github.com/imroc/req/v3"
	"golang.org/x/net/publicsuffix"
)

// reqClientOptions 定义 req 客户端的构建参数
type reqClientOptions struct {
	ProxyURL    string          // 代理 URL（支持 http/https/socks5）
	Timeout     time.Duration   // 请求超时时间
	Impersonate bool            // 是否模拟浏览器指纹（当前为 Firefox，Chrome 伪装会被 chatgpt.com 的 Cloudflare 质询）
	ForceHTTP2  bool            // 是否强制使用 HTTP/2
	Cookies     reqCookiePolicy // cookie jar 策略；零值维持 req 默认的内存 jar
}

// reqCookiePolicy：客户端按代理共享，req 默认的内存 jar 会让同一代理下的账号互相带上对方的 cookie。
type reqCookiePolicy int

const (
	reqCookiesDefault           reqCookiePolicy = iota
	reqCookiesNone                              // 不存不发
	reqCookiesChatGPTCloudflare                 // 照真实 Codex：只在 ChatGPT 主机上存取 Cloudflare 类 cookie
)

// sharedReqClients 存储按配置参数缓存的 req 客户端实例
//
// 性能优化说明：
// 原实现在每次 OAuth 刷新时都创建新的 req.Client：
// 1. claude_oauth_service.go: 每次刷新创建新客户端
// 2. openai_oauth_service.go: 每次刷新创建新客户端
// 3. gemini_oauth_client.go: 每次刷新创建新客户端
//
// 新实现使用 sync.Map 缓存客户端：
// 1. 相同配置（代理+超时+模拟设置）复用同一客户端
// 2. 复用底层连接池，减少 TLS 握手开销
// 3. LoadOrStore 保证并发安全，避免重复创建
var sharedReqClients sync.Map

// getSharedReqClient 获取共享的 req 客户端实例
// 性能优化：相同配置复用同一客户端，避免重复创建
func getSharedReqClient(opts reqClientOptions) (*req.Client, error) {
	key := buildReqClientKey(opts)
	if cached, ok := sharedReqClients.Load(key); ok {
		if c, ok := cached.(*req.Client); ok {
			return c, nil
		}
	}

	client := req.C().SetTimeout(opts.Timeout)
	if opts.ForceHTTP2 {
		client = client.EnableForceHTTP2()
	}
	if opts.Impersonate {
		// chatgpt.com 的 Cloudflare 会对 req 内置的 Chrome 伪装（UA 固定为 Chrome/120，
		// 与 sec-ch-ua 等 Client Hints 一起已明显过时）直接返回 403 cf-mitigated=challenge，
		// 导致 accounts/check、subscriptions、隐私设置等 backend-api 调用全部失败，
		// 订阅到期时间因此长期不更新（见 issue #4825）。Firefox 伪装的 UA/头部组合
		// 在同一出口 IP 下稳定通过，故改用 Firefox 指纹。
		client = client.ImpersonateFirefox()
	}
	switch opts.Cookies {
	case reqCookiesNone:
		client.SetCookieJar(nil)
	case reqCookiesChatGPTCloudflare:
		client.SetCookieJar(newChatGPTCloudflareCookieJar())
	}
	trimmed, _, err := proxyurl.Parse(opts.ProxyURL)
	if err != nil {
		return nil, err
	}
	if trimmed != "" {
		client.SetProxyURL(trimmed)
	}
	client = instrumentReqClient(client)

	actual, _ := sharedReqClients.LoadOrStore(key, client)
	if c, ok := actual.(*req.Client); ok {
		return c, nil
	}
	return client, nil
}

func instrumentReqClient(client *req.Client) *req.Client {
	if client == nil {
		return nil
	}
	client.GetTransport().WrapRoundTripFunc(func(rt http.RoundTripper) req.HttpRoundTripFunc {
		timed := servertiming.WrapRoundTripper(rt)
		return timed.RoundTrip
	})
	return client
}

func buildReqClientKey(opts reqClientOptions) string {
	key := fmt.Sprintf("%s|%s|%t|%t",
		strings.TrimSpace(opts.ProxyURL),
		opts.Timeout.String(),
		opts.Impersonate,
		opts.ForceHTTP2,
	)
	if opts.Cookies != reqCookiesDefault {
		key += fmt.Sprintf("|cookies=%d", opts.Cookies)
	}
	return key
}

// chatgptCloudflareCookieJar 照真实 Codex（http-client/src/chatgpt_cloudflare_cookies.rs 与
// chatgpt_hosts.rs）：只在 https 的 ChatGPT 主机上存取 Cloudflare 类 cookie，其余一律不存不发。
type chatgptCloudflareCookieJar struct{ inner *cookiejar.Jar }

func newChatGPTCloudflareCookieJar() http.CookieJar {
	inner, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	return &chatgptCloudflareCookieJar{inner: inner}
}

func (j *chatgptCloudflareCookieJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	if !isChatGPTCookieURL(u) {
		return
	}
	var kept []*http.Cookie
	for _, cookie := range cookies {
		if isCloudflareCookieName(cookie.Name) {
			kept = append(kept, cookie)
		}
	}
	j.inner.SetCookies(u, kept)
}

func (j *chatgptCloudflareCookieJar) Cookies(u *url.URL) []*http.Cookie {
	if !isChatGPTCookieURL(u) {
		return nil
	}
	return j.inner.Cookies(u)
}

func isChatGPTCookieURL(u *url.URL) bool {
	if u == nil || u.Scheme != "https" {
		return false
	}
	switch host := strings.ToLower(u.Hostname()); host {
	case "chatgpt.com", "chat.openai.com", "chatgpt-staging.com":
		return true
	default:
		return strings.HasSuffix(host, ".chatgpt.com") || strings.HasSuffix(host, ".chatgpt-staging.com")
	}
}

func isCloudflareCookieName(name string) bool {
	switch name {
	case "__cf_bm", "__cflb", "__cfruid", "__cfseq", "__cfwaitingroom", "__oailb", "_cfuvid", "cf_clearance", "cf_ob_info", "cf_use_ob":
		return true
	default:
		return strings.HasPrefix(name, "cf_chl_")
	}
}

// CreatePrivacyReqClient creates an HTTP client for OpenAI privacy settings API
// This is exported for use by OpenAIPrivacyService
// Uses Chrome TLS fingerprint impersonation to bypass Cloudflare checks
func CreatePrivacyReqClient(proxyURL string) (*req.Client, error) {
	return getSharedReqClient(reqClientOptions{
		ProxyURL:    proxyURL,
		Timeout:     30 * time.Second,
		Impersonate: true, // Enable browser TLS fingerprint impersonation (Firefox, see getSharedReqClient)
	})
}

// CreateCodexBackendReqClient 供 Codex 客户端面的 chatgpt.com/backend-api 请求
// （额度查询等）使用，刻意不做浏览器伪装。
//
// ImpersonateChrome 改的不只是 TLS：它同时设置 HelloChrome_120 指纹、Chrome 的
// HTTP/2 SETTINGS 帧与头顺序，以及 sec-ch-ua / sec-ch-ua-platform="macOS" /
// user-agent=Chrome 120 on macOS 一整套公共头。用在这里会让同一个账号在额度面
// 自报「macOS 上的 Chrome」、在推理面自报 codex-tui，两者互相矛盾。
//
// 真实 Codex 走 reqwest 直连该端点，只发 User-Agent + Authorization +
// ChatGPT-Account-Id（codex-rs backend-client/src/client.rs 的 headers()）。
// 实测该端点对非浏览器指纹不做拦截，故这里用普通客户端。
// 共享池按 Impersonate 分 key，因此不会影响 CreatePrivacyReqClient 的实例。
func CreateCodexBackendReqClient(proxyURL string) (*req.Client, error) {
	return getSharedReqClient(reqClientOptions{
		ProxyURL: proxyURL,
		Timeout:  30 * time.Second,
		Cookies:  reqCookiesChatGPTCloudflare,
	})
}
