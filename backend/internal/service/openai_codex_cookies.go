package service

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/chatgptcookies"
)

// openAICodexCookieStore：推理面的 ChatGPT cookie 回放，按账号各一只罐。
//
// 真实 Codex ≥0.156.1 用进程级罐在 HTTP 与 WSS 握手上回放 __cflb / __oailb / __cf_bm 等
// （http-client/src/chatgpt_cloudflare_cookies.rs、websocket-client/src/lib.rs）。一个网关进程
// 服务多个账号，共用一只罐会把两个账号钉到同一条路由 / 同一个 Cloudflare 会话上，所以按
// account.ID 隔离；主机与名字的过滤在 chatgptcookies 里。只对本地持有 ChatGPT 凭据的账号
// 生效（IsOpenAIOAuthLike）：cpr 走原样中继、API Key 账号的上游不是 chatgpt.com。
//
// 2026-09-25 直连实测：__oailb 是钉后端网关主机的 ES256 JWT（1 小时），回放与否不改变服务
// 质量——这里对齐的是真实客户端的形态，不是治降智的手段。
type openAICodexCookieStore struct {
	jars sync.Map // openAICodexCookieJarKey(account) → *chatgptcookies.Jar
}

// openAICodexCookieJarKey：本地行 ID + 凭证域身份。同一行重新授权成另一个 ChatGPT 身份时
// 旧身份的 __oailb（钉后端网关主机）不能跟着新身份出站，所以身份变了就换罐；只按身份不按行则
// 缺 chatgpt_user_id 的工作区会共用一只罐。
func openAICodexCookieJarKey(account *Account) string {
	return strconv.FormatInt(account.ID, 10) + "|" + codexAccountIdentityNamespace(account)
}

func (s *openAICodexCookieStore) jar(account *Account) *chatgptcookies.Jar {
	key := openAICodexCookieJarKey(account)
	if v, ok := s.jars.Load(key); ok {
		if jar, ok := v.(*chatgptcookies.Jar); ok {
			return jar
		}
	}
	fresh := chatgptcookies.NewJar()
	if v, loaded := s.jars.LoadOrStore(key, fresh); loaded {
		if jar, ok := v.(*chatgptcookies.Jar); ok {
			return jar
		}
	}
	return fresh
}

func openAICodexCookiesApply(account *Account) bool {
	return account != nil && account.IsOpenAIOAuthLike()
}

// openAICodexCookieURL 把出站地址归一到罐认的形态：罐只认 https；WS 握手的 wss:// 与 HTTP
// 是同一台主机的同一批 cookie。其它 scheme 返回 nil。
func openAICodexCookieURL(raw string) *url.URL {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u == nil {
		return nil
	}
	switch strings.ToLower(u.Scheme) {
	case "https", "wss":
		clone := *u
		clone.Scheme = "https"
		return &clone
	default:
		return nil
	}
}

// Attach 把该账号罐里适用于 rawURL 的 cookie 写成 headers 的 Cookie 头。覆盖而不追加：入站
// Cookie 本就不在任何出站白名单里，出站不该带客户端的值。罐为空时不写头。
func (s *openAICodexCookieStore) Attach(account *Account, rawURL string, headers http.Header) {
	if s == nil || headers == nil || !openAICodexCookiesApply(account) {
		return
	}
	u := openAICodexCookieURL(rawURL)
	if u == nil {
		return
	}
	cookies := s.jar(account).Cookies(u)
	if len(cookies) == 0 {
		return
	}
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}
	headers.Set("Cookie", strings.Join(parts, "; "))
}

// Store 把响应（HTTP 响应或 WS 握手响应）头里的 Set-Cookie 收进该账号的罐。
func (s *openAICodexCookieStore) Store(account *Account, rawURL string, resp http.Header) {
	if s == nil || len(resp) == 0 || !openAICodexCookiesApply(account) {
		return
	}
	u := openAICodexCookieURL(rawURL)
	if u == nil {
		return
	}
	cookies := (&http.Response{Header: resp}).Cookies()
	if len(cookies) == 0 {
		return
	}
	s.jar(account).SetCookies(u, cookies)
}
