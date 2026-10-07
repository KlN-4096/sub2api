package service

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

func codexCookieTestAccount(id int64, typ string) *Account {
	return &Account{ID: id, Platform: PlatformOpenAI, Type: typ}
}

func codexCookieUpstreamResponse() http.Header {
	return http.Header{"Set-Cookie": []string{
		"__oailb=jwt-1; Path=/; Max-Age=3600; HttpOnly; Secure; SameSite=Lax",
		"__cflb=lb-1; Path=/; Secure; HttpOnly",
		"__cf_bm=bm-1; Domain=chatgpt.com; Path=/; Secure",
		"oai-did=device; Path=/; Secure",
	}}
}

// 罐的键是「行 ID + 凭证域身份」：token 刷新不换罐；同一行重新授权成另一个 ChatGPT 身份时换新罐，
// 旧身份的 __oailb 不跟着新身份出站。
func TestOpenAICodexCookieStore_JarFollowsCredentialIdentity(t *testing.T) {
	store := &openAICodexCookieStore{}
	const httpURL = "https://chatgpt.com/backend-api/codex/responses"
	acct := &Account{ID: 9, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{
		"chatgpt_account_id": "acc-a", "chatgpt_user_id": "user-a", "access_token": "t1",
	}}
	store.Store(acct, httpURL, http.Header{"Set-Cookie": []string{"__oailb=a; Path=/; Secure; HttpOnly"}})
	h := http.Header{}
	store.Attach(acct, httpURL, h)
	require.Equal(t, "__oailb=a", h.Get("Cookie"))

	acct.Credentials["access_token"] = "t2"
	h = http.Header{}
	store.Attach(acct, httpURL, h)
	require.Equal(t, "__oailb=a", h.Get("Cookie"), "token 刷新不换罐")

	acct.Credentials["chatgpt_account_id"] = "acc-b"
	acct.Credentials["chatgpt_user_id"] = "user-b"
	h = http.Header{}
	store.Attach(acct, httpURL, h)
	require.Empty(t, h.Get("Cookie"), "换了 ChatGPT 身份就是新罐")
}

func TestOpenAICodexCookieStore_ReplaysAllowlistedCookiesPerAccount(t *testing.T) {
	store := &openAICodexCookieStore{}
	acct1 := codexCookieTestAccount(1, AccountTypeOAuth)
	acct2 := codexCookieTestAccount(2, AccountTypeOAuth)
	const httpURL = "https://chatgpt.com/backend-api/codex/responses"

	store.Store(acct1, httpURL, codexCookieUpstreamResponse())

	headers := http.Header{}
	store.Attach(acct1, httpURL, headers)
	cookie := headers.Get("Cookie")
	require.Contains(t, cookie, "__oailb=jwt-1")
	require.Contains(t, cookie, "__cflb=lb-1")
	require.Contains(t, cookie, "__cf_bm=bm-1")
	require.NotContains(t, cookie, "oai-did", "白名单外的 cookie 不回放")

	// WS 握手（wss://）与 HTTP 是同一台主机的同一批 cookie
	wsHeaders := http.Header{}
	store.Attach(acct1, "wss://chatgpt.com/backend-api/codex/responses", wsHeaders)
	require.Contains(t, wsHeaders.Get("Cookie"), "__oailb=jwt-1")

	// 按账号隔离
	other := http.Header{}
	store.Attach(acct2, httpURL, other)
	require.Empty(t, other.Get("Cookie"))

	// 不是 ChatGPT 主机不回放
	api := http.Header{}
	store.Attach(acct1, "https://api.openai.com/v1/responses", api)
	require.Empty(t, api.Get("Cookie"))
}

func TestOpenAICodexCookieStore_OnlyForChatGPTCredentialAccounts(t *testing.T) {
	store := &openAICodexCookieStore{}
	const httpURL = "https://chatgpt.com/backend-api/codex/responses"
	for _, acct := range []*Account{nil, codexCookieTestAccount(3, AccountTypeAPIKey)} {
		store.Store(acct, httpURL, codexCookieUpstreamResponse())
		headers := http.Header{}
		store.Attach(acct, httpURL, headers)
		require.Empty(t, headers.Get("Cookie"))
	}

	// nil store / nil headers 安全
	var nilStore *openAICodexCookieStore
	nilStore.Store(codexCookieTestAccount(1, AccountTypeOAuth), httpURL, codexCookieUpstreamResponse())
	nilStore.Attach(codexCookieTestAccount(1, AccountTypeOAuth), httpURL, http.Header{})
	store.Attach(codexCookieTestAccount(1, AccountTypeOAuth), httpURL, nil)
}

func TestOpenAICodexCookieStore_AttachOverwritesInboundCookie(t *testing.T) {
	store := &openAICodexCookieStore{}
	acct := codexCookieTestAccount(1, AccountTypeOAuth)
	const httpURL = "https://chatgpt.com/backend-api/codex/responses"
	store.Store(acct, httpURL, codexCookieUpstreamResponse())

	headers := http.Header{"Cookie": []string{"client=leak"}}
	store.Attach(acct, httpURL, headers)
	require.Len(t, headers.Values("Cookie"), 1)
	require.NotContains(t, headers.Get("Cookie"), "client=leak")

	// 罐为空时不写头，也不动已有值
	empty := &openAICodexCookieStore{}
	untouched := http.Header{"Cookie": []string{"client=leak"}}
	empty.Attach(acct, httpURL, untouched)
	require.Equal(t, "client=leak", untouched.Get("Cookie"))
}

// cookieRecordingUpstream 记录出站 Cookie 头并回一个带 Set-Cookie 的响应。
type cookieRecordingUpstream struct {
	sentCookies []string
	setCookie   http.Header
}

func (u *cookieRecordingUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	u.sentCookies = append(u.sentCookies, req.Header.Get("Cookie"))
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}
	for k, vs := range u.setCookie {
		resp.Header[k] = append([]string(nil), vs...)
	}
	return resp, nil
}

func (u *cookieRecordingUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, accountConcurrency)
}

func TestDoOpenAIUpstream_ReplaysCookiesFromPreviousResponse(t *testing.T) {
	upstream := &cookieRecordingUpstream{setCookie: codexCookieUpstreamResponse()}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := codexCookieTestAccount(1, AccountTypeOAuth)

	for range 2 {
		req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", strings.NewReader("{}"))
		require.NoError(t, err)
		resp, err := svc.doOpenAIUpstream(req, "", acct)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}
	require.Len(t, upstream.sentCookies, 2)
	require.Empty(t, upstream.sentCookies[0], "首条请求罐还是空的")
	require.Contains(t, upstream.sentCookies[1], "__oailb=jwt-1")
	require.Contains(t, upstream.sentCookies[1], "__cflb=lb-1")
	require.NotContains(t, upstream.sentCookies[1], "oai-did")
}
