package chatgptcookies

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}

func cookieNames(cookies []*http.Cookie) []string {
	names := make([]string, 0, len(cookies))
	for _, c := range cookies {
		names = append(names, c.Name)
	}
	return names
}

func TestJar_KeepsOnlyAllowedNamesOnChatGPTHosts(t *testing.T) {
	jar := NewJar()
	u := mustURL(t, "https://chatgpt.com/backend-api/codex/responses")
	resp := &http.Response{Header: http.Header{"Set-Cookie": []string{
		"__oailb=jwt; Path=/; Max-Age=3600; HttpOnly; Secure; SameSite=Lax",
		"__cflb=lb; Path=/; Secure; HttpOnly",
		"__cf_bm=bm; Domain=chatgpt.com; Path=/; Secure",
		"cf_chl_rc_m=1; Path=/",
		"oai-did=device; Path=/",
		"__Secure-next-auth.session-token=x; Path=/; Secure",
	}}}
	jar.SetCookies(u, resp.Cookies())

	require.ElementsMatch(t, []string{"__oailb", "__cflb", "__cf_bm", "cf_chl_rc_m"}, cookieNames(jar.Cookies(u)))
	// 这四个 cookie 都是 Path=/，所以同主机其它路径（额度面）也取得到；cookiejar 本身按 path 匹配。
	require.Len(t, jar.Cookies(mustURL(t, "https://chatgpt.com/backend-api/wham/usage")), 4)
}

func TestJar_IgnoresNonChatGPTOrPlainHTTP(t *testing.T) {
	jar := NewJar()
	set := func(raw string) {
		u := mustURL(t, raw)
		jar.SetCookies(u, []*http.Cookie{{Name: "__cflb", Value: "x", Path: "/"}})
	}
	set("https://api.openai.com/v1/responses")
	set("http://chatgpt.com/backend-api/codex/responses")
	set("https://evilchatgpt.com/")
	set("https://chatgpt.com.evil.example/")
	set("https://foo.chat.openai.com/")

	for _, raw := range []string{
		"https://api.openai.com/v1/responses",
		"http://chatgpt.com/backend-api/codex/responses",
		"https://chatgpt.com/backend-api/codex/responses",
	} {
		require.Empty(t, jar.Cookies(mustURL(t, raw)), raw)
	}
}

func TestIsChatGPTHost(t *testing.T) {
	for _, host := range []string{"chatgpt.com", "foo.chatgpt.com", "CHATGPT.com", "chat.openai.com", "chatgpt-staging.com", "api.chatgpt-staging.com"} {
		require.True(t, IsChatGPTHost(host), host)
	}
	for _, host := range []string{"evilchatgpt.com", "chatgpt.com.evil.example", "api.openai.com", "foo.chat.openai.com", ""} {
		require.False(t, IsChatGPTHost(host), host)
	}
}

func TestNilJarIsSafe(t *testing.T) {
	var jar *Jar
	u := mustURL(t, "https://chatgpt.com/")
	jar.SetCookies(u, []*http.Cookie{{Name: "__cflb", Value: "x"}})
	require.Nil(t, jar.Cookies(u))
}
