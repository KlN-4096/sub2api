// Package chatgptcookies 照真实 Codex（codex-rs http-client/src/chatgpt_cloudflare_cookies.rs 与
// chatgpt_hosts.rs）实现 ChatGPT 主机的 cookie 罐：只在 https 的 ChatGPT 主机上存取 Cloudflare 类
// 与 OpenAI 路由类（__oailb）cookie，其余一律不存不发。额度面（imroc/req 客户端）与推理面
// （net/http 请求、WS 握手）共用同一套过滤。
package chatgptcookies

import (
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"

	"golang.org/x/net/publicsuffix"
)

// Jar 是带主机与名字过滤的 http.CookieJar。
type Jar struct{ inner *cookiejar.Jar }

// NewJar 建一只空罐。
func NewJar() *Jar {
	inner, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	return &Jar{inner: inner}
}

// SetCookies 只收 ChatGPT https 主机下发的白名单 cookie。
func (j *Jar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	if j == nil || j.inner == nil || !IsChatGPTURL(u) {
		return
	}
	kept := make([]*http.Cookie, 0, len(cookies))
	for _, cookie := range cookies {
		if cookie != nil && IsAllowedName(cookie.Name) {
			kept = append(kept, cookie)
		}
	}
	if len(kept) == 0 {
		return
	}
	j.inner.SetCookies(u, kept)
}

// Cookies 只对 ChatGPT https 主机回放。
func (j *Jar) Cookies(u *url.URL) []*http.Cookie {
	if j == nil || j.inner == nil || !IsChatGPTURL(u) {
		return nil
	}
	return j.inner.Cookies(u)
}

// IsChatGPTURL：https 的 ChatGPT 主机（chatgpt_hosts.rs is_allowed_chatgpt_host）。
func IsChatGPTURL(u *url.URL) bool {
	return u != nil && u.Scheme == "https" && IsChatGPTHost(u.Hostname())
}

// IsChatGPTHost 精确匹配 chatgpt.com / chat.openai.com / chatgpt-staging.com，或它们的子域
// （chat.openai.com 不含子域，与真实客户端一致）。
func IsChatGPTHost(host string) bool {
	switch host = strings.ToLower(strings.TrimSpace(host)); host {
	case "chatgpt.com", "chat.openai.com", "chatgpt-staging.com":
		return true
	default:
		return strings.HasSuffix(host, ".chatgpt.com") || strings.HasSuffix(host, ".chatgpt-staging.com")
	}
}

// IsAllowedName：Cloudflare 类与 OpenAI 路由 cookie 白名单（chatgpt_cloudflare_cookies.rs）。
func IsAllowedName(name string) bool {
	switch name {
	case "__cf_bm", "__cflb", "__cfruid", "__cfseq", "__cfwaitingroom", "__oailb", "_cfuvid", "cf_clearance", "cf_ob_info", "cf_use_ob":
		return true
	default:
		return strings.HasPrefix(name, "cf_chl_")
	}
}
