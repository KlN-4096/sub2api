package service

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func routeCookieTestOailb(t *testing.T, host string) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"host": host,
		"iss":  "edge-gateway",
		"aud":  []string{"chatgpt.com"},
		"exp":  1790454095,
	})
	require.NoError(t, err)
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256","kid":"oailb-v1","typ":"JWT"}`))
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

// TestOpenAICodexRoutePairFromSetCookie：上游下发的 Set-Cookie 一行一对，属性段要丢掉，
// 只留这两个名字，且顺序稳定（否则同一组路由对在不同请求里落库成不同字符串，没法比对）。
func TestOpenAICodexRoutePairFromSetCookie(t *testing.T) {
	oailb := routeCookieTestOailb(t, "chat.gateway.unified-101.api.openai.com")
	h := http.Header{}
	// 故意把 __oailb 放前面，验证输出仍按固定顺序
	h.Add("Set-Cookie", "__oailb="+oailb+"; Path=/; Max-Age=3900; HttpOnly")
	h.Add("Set-Cookie", "__cflb=0H28vzvP5FJafnk; Path=/; SameSite=None")
	h.Add("Set-Cookie", "__cf_bm=should-not-appear; Path=/")
	h.Add("Set-Cookie", "other=x")

	pair := openAICodexRoutePairFromSetCookie(h)
	require.Equal(t, "__cflb=0H28vzvP5FJafnk; __oailb="+oailb, pair)
	require.NotContains(t, pair, "__cf_bm", "__cf_bm 与 TLS/IP 绑定，不属于路由对")
	require.NotContains(t, pair, "Path=", "属性段不能进落库串")

	require.Equal(t, "unified-101", openAICodexRouteGateway(pair))
}

// TestOpenAICodexRoutePairFromCookie：我们带出去的 Cookie 是一行多对。
func TestOpenAICodexRoutePairFromCookie(t *testing.T) {
	oailb := routeCookieTestOailb(t, "chat.gateway.unified-88.api.openai.com")
	h := http.Header{}
	h.Set("Cookie", "__cf_bm=zz; __cflb=abc; __oailb="+oailb)

	pair := openAICodexRoutePairFromCookie(h)
	require.Equal(t, "__cflb=abc; __oailb="+oailb, pair)
	require.Equal(t, "unified-88", openAICodexRouteGateway(pair))
}

// TestOpenAICodexRouteGatewayBadInput：解不开的一律返回空，不能让观测列写进半截垃圾。
func TestOpenAICodexRouteGatewayBadInput(t *testing.T) {
	for name, pair := range map[string]string{
		"空串":                 "",
		"没有 oailb":           "__cflb=abc",
		"不是 JWT":             "__oailb=notajwt",
		"载荷不是 base64":        "__oailb=a.!!!.c",
		"载荷不是 JSON":          "__oailb=a." + base64.RawURLEncoding.EncodeToString([]byte("nope")) + ".c",
		"host 里没有 unified 段": "__oailb=" + routeCookieTestOailb(t, "chat.gateway.example.com"),
	} {
		require.Empty(t, openAICodexRouteGateway(pair), name)
	}
}

// TestUsageCodexRoutePtr：空值给 NULL，超长截断且不切多字节。
func TestUsageCodexRoutePtr(t *testing.T) {
	require.Nil(t, usageCodexRoutePairPtr("   "))
	require.Nil(t, usageCodexRouteGatewayPtr("__cflb=abc"))

	oailb := routeCookieTestOailb(t, "chat.gateway.unified-7.api.openai.com")
	pair := "__cflb=abc; __oailb=" + oailb
	got := usageCodexRoutePairPtr(pair)
	require.NotNil(t, got)
	require.Equal(t, pair, *got)

	gw := usageCodexRouteGatewayPtr(pair)
	require.NotNil(t, gw)
	require.Equal(t, "unified-7", *gw)

	long := usageCodexRoutePairPtr("__cflb=" + strings.Repeat("中", 400))
	require.NotNil(t, long)
	require.LessOrEqual(t, len(*long), maxUsageRoutePairLen)
	require.True(t, strings.HasPrefix(*long, "__cflb=中"), "截断不能切坏多字节字符")
}

// TestRoutePairInUsePrefersUpstream：上游新下发的路由对优先于罐里回放的那一组——
// 改派发生在这一发上，落库要记改派后的结果。
func TestRoutePairInUsePrefersUpstream(t *testing.T) {
	s := &OpenAIGatewayService{}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	fresh := routeCookieTestOailb(t, "chat.gateway.unified-165.api.openai.com")
	upstream := http.Header{}
	upstream.Add("Set-Cookie", "__cflb=new; Path=/")
	upstream.Add("Set-Cookie", "__oailb="+fresh+"; Path=/")
	pair, fromPool, poolGateway, poolVersion := s.routePairInUse(account, upstream, OpenAIGatewayPoolApplied{})
	require.Equal(t, "unified-165", openAICodexRouteGateway(pair))
	require.False(t, fromPool, "没接管网关池就不是覆写")
	require.Empty(t, poolGateway)
	require.Empty(t, poolVersion)

	// 上游没下发时回读罐；罐是空的就给空串，不能崩。
	jarPair, fromPool, _, _ := s.routePairInUse(account, http.Header{}, OpenAIGatewayPoolApplied{})
	require.Empty(t, jarPair)
	require.False(t, fromPool)
	nilPair, fromPool, _, _ := s.routePairInUse(nil, http.Header{}, OpenAIGatewayPoolApplied{})
	require.Empty(t, nilPair)
	require.False(t, fromPool)

	// 账号类型不适用时「已覆写」读数保持 NULL，与 FALSE 区分开。
	require.Nil(t, usageCodexRoutePairOverriddenPtr(&Account{ID: 2, Platform: PlatformAnthropic}, true))
	overridden := usageCodexRoutePairOverriddenPtr(account, true)
	require.NotNil(t, overridden)
	require.True(t, *overridden)
}
