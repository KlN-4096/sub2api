//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// 真客户端的 web_search.user_location 来自本机配置（protocol/src/config_types.rs:481
// WebSearchUserLocation：type/country/region/city/timezone，全部可选），未配置时整个
// 字段缺席。客户端在国内、出口在美国时，这个字段会和已经改写好的 environment_context
// 时区自相矛盾——一个自称在上海的用户从拉斯维加斯的 IP 发请求。
const (
	userLocationTestCity    = "Las Vegas"
	userLocationTestRegion  = "Nevada"
	userLocationTestCountry = "US"
)

// userLocationTestClientRaw 是客户端声明的原始位置：与出口完全不同的一处。
const userLocationTestClientRaw = `{"type":"approximate","country":"CN","region":"Shanghai",` +
	`"city":"Shanghai","timezone":"Asia/Shanghai"}`

// wireUserLocationTestAccount 造一个解析结果齐全的双开账号。
func wireUserLocationTestAccount(geo bool) *Account {
	account := wireProfileTestAccount(true)
	account.Extra[codexWireTimezoneResolvedExtraKey] = wireTimezoneTestExit
	account.Extra[codexWireTimezoneResolvedProxyExtraKey] = codexWireTimezoneProxyTag(account)
	if geo {
		account.Extra[codexWireLocationCityExtraKey] = userLocationTestCity
		account.Extra[codexWireLocationRegionExtraKey] = userLocationTestRegion
		account.Extra[codexWireLocationCountryExtraKey] = userLocationTestCountry
	}
	return account
}

// wireUserLocationTestBody 在共享夹具上挂一个带 user_location 的 web_search 工具。
func wireUserLocationTestBody(t *testing.T, toolType, rawLocation string) []byte {
	t.Helper()
	return wireUserLocationTestBodyWithTools(t, wireUserLocationTestTool(t, toolType, rawLocation))
}

func wireUserLocationTestTool(t *testing.T, toolType, rawLocation string) string {
	t.Helper()
	tool := `{"type":` + quoteJSON(toolType) + `}`
	if rawLocation != "" {
		var err error
		tool, err = sjson.SetRaw(tool, "user_location", rawLocation)
		require.NoError(t, err)
	}
	return tool
}

func wireUserLocationTestBodyWithTools(t *testing.T, tools ...string) []byte {
	t.Helper()
	body, err := sjson.SetRawBytes(wireProfileTestBody(t), "tools",
		[]byte("["+strings.Join(tools, ",")+"]"))
	require.NoError(t, err)
	return body
}

func quoteJSON(value string) string {
	raw, _ := sjson.Set("{}", "v", value)
	return gjson.Get(raw, "v").Raw
}

func TestCodexDeviceWireProfileRewritesWebSearchUserLocation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		toolType    string
		passthrough bool
	}{
		{name: "web_search", toolType: "web_search"},
		{name: "web_search_raw", toolType: "web_search", passthrough: true},
		// 上游给该工具发过带日期后缀的变体，前缀相同即按同一类处理。
		{name: "web_search_preview", toolType: "web_search_preview"},
		{name: "web_search_2025_08_26", toolType: "web_search_2025_08_26"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := wireUserLocationTestBody(t, tc.toolType, userLocationTestClientRaw)
			c := newConvTestContext(t, body)
			account := wireUserLocationTestAccount(true)
			account.Extra["openai_passthrough"] = tc.passthrough
			svc, up := wireProfileTestService()

			_, _ = svc.Forward(context.Background(), c, account, body)

			require.NotNil(t, up.lastReq)
			require.True(t, gjson.ValidBytes(up.lastBody), "改写后仍是合法 JSON")
			require.NotContains(t, string(up.lastBody), "Shanghai", "客户端本机位置不得出站")
			loc := gjson.GetBytes(up.lastBody, "tools.0.user_location")
			require.True(t, loc.IsObject(), "客户端发过的字段必须保留")
			require.Equal(t, "approximate", loc.Get("type").String())
			require.Equal(t, userLocationTestCountry, loc.Get("country").String())
			require.Equal(t, userLocationTestRegion, loc.Get("region").String())
			require.Equal(t, userLocationTestCity, loc.Get("city").String())
			require.Equal(t, wireTimezoneTestExit, loc.Get("timezone").String(),
				"位置时区必须与 environment_context 同源")
			require.Equal(t, tc.toolType, gjson.GetBytes(up.lastBody, "tools.0.type").String())
		})
	}
}

func TestCodexWebSearchUserLocationNotSynthesizedWhenClientOmitsIt(t *testing.T) {
	// 真客户端未配置 location 时整个字段缺席；替它补一个会造出比不改写更强的异常信号。
	body := wireUserLocationTestBody(t, "web_search", "")
	c := newConvTestContext(t, body)
	svc, up := wireProfileTestService()

	_, _ = svc.Forward(context.Background(), c, wireUserLocationTestAccount(true), body)

	require.NotNil(t, up.lastReq)
	require.False(t, gjson.GetBytes(up.lastBody, "tools.0.user_location").Exists(),
		"客户端没发就不能补")
	require.Equal(t, "web_search", gjson.GetBytes(up.lastBody, "tools.0.type").String())
}

func TestCodexWebSearchUserLocationDroppedWhenGeoUnavailable(t *testing.T) {
	// 只有手动覆盖时区、没有出口地理：留着城市就变成"上海 + America/Los_Angeles"，
	// 比不改写更矛盾。删掉整个字段落回"未配置 location 的真客户端"这一合法形态。
	body := wireUserLocationTestBody(t, "web_search", userLocationTestClientRaw)
	c := newConvTestContext(t, body)
	// 只缺地理：时区照常来自自动解析，手动覆盖那条守卫由
	// TestCodexWireLocationDisabledByManualTimezoneOverride 单独覆盖。
	account := wireUserLocationTestAccount(false)
	svc, up := wireProfileTestService()

	_, _ = svc.Forward(context.Background(), c, account, body)

	require.NotNil(t, up.lastReq)
	require.NotContains(t, string(up.lastBody), "Shanghai")
	require.False(t, gjson.GetBytes(up.lastBody, "tools.0.user_location").Exists())
}

func TestCodexWebSearchUserLocationUntouchedWhenNotApplicable(t *testing.T) {
	for _, tc := range []struct {
		name        string
		wireProfile bool
		resolved    bool
		toolType    string
	}{
		{name: "non_wire_profile", resolved: true, toolType: "web_search"},
		// 时区都没解析出来时整条功能是 no-op，位置也不该单独动。
		{name: "no_timezone_resolved", wireProfile: true, toolType: "web_search"},
		// 前缀不同的工具不属于 web_search 家族，位置字段不归我们管。
		{name: "other_tool", wireProfile: true, resolved: true, toolType: "websearch_custom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := wireUserLocationTestBody(t, tc.toolType, userLocationTestClientRaw)
			c := newConvTestContext(t, body)
			account := wireProfileTestAccount(tc.wireProfile)
			if tc.resolved {
				account.Extra[codexWireTimezoneResolvedExtraKey] = wireTimezoneTestExit
				account.Extra[codexWireTimezoneResolvedProxyExtraKey] = codexWireTimezoneProxyTag(account)
				account.Extra[codexWireLocationCityExtraKey] = userLocationTestCity
				account.Extra[codexWireLocationRegionExtraKey] = userLocationTestRegion
				account.Extra[codexWireLocationCountryExtraKey] = userLocationTestCountry
			}
			svc, up := wireProfileTestService()

			_, _ = svc.Forward(context.Background(), c, account, body)

			require.NotNil(t, up.lastReq)
			require.Equal(t, "Shanghai",
				gjson.GetBytes(up.lastBody, "tools.0.user_location.city").String(),
				"不适用时原样透传")
		})
	}
}

// 下标不是 0 的工具也必须改到，且不属于 web_search 家族的工具原样不动。
// 在单元层验：Forward 路径会剥掉形状不合法的工具（与本功能无关），下标会跟着位移，
// 掩盖掉这里要证明的"按真实下标定位"。
func TestCodexWebSearchUserLocationRewritesEveryMatchingTool(t *testing.T) {
	body := wireUserLocationTestBodyWithTools(t,
		wireUserLocationTestTool(t, "function", userLocationTestClientRaw),
		wireUserLocationTestTool(t, "web_search", userLocationTestClientRaw),
		wireUserLocationTestTool(t, "web_search_preview", userLocationTestClientRaw),
	)

	out := rewriteCodexWebSearchUserLocationWith(wireUserLocationTestAccount(true), wireTimezoneTestExit, body)

	require.Equal(t, "Shanghai", gjson.GetBytes(out, "tools.0.user_location.city").String(),
		"非 web_search 家族的工具不归我们管")
	for _, index := range []string{"tools.1", "tools.2"} {
		require.Equal(t, userLocationTestCity,
			gjson.GetBytes(out, index+".user_location.city").String(), index)
		require.Equal(t, wireTimezoneTestExit,
			gjson.GetBytes(out, index+".user_location.timezone").String(), index)
	}

	// 端到端补一条：两个 web_search 都要改到，客户端城市不得出站。
	e2e := wireUserLocationTestBodyWithTools(t,
		wireUserLocationTestTool(t, "web_search", userLocationTestClientRaw),
		wireUserLocationTestTool(t, "web_search_preview", userLocationTestClientRaw),
	)
	c := newConvTestContext(t, e2e)
	svc, up := wireProfileTestService()

	_, _ = svc.Forward(context.Background(), c, wireUserLocationTestAccount(true), e2e)

	require.NotNil(t, up.lastReq)
	require.NotContains(t, string(up.lastBody), "Shanghai")
	require.Equal(t, 2, len(gjson.GetBytes(up.lastBody, "tools").Array()))
	for _, index := range []string{"tools.0", "tools.1"} {
		require.Equal(t, userLocationTestCity,
			gjson.GetBytes(up.lastBody, index+".user_location.city").String(), index)
	}
}

// 重复键：gjson 读第一个而上游读最后一个，只改第一个会让客户端原值存活。
//
// 必须走透传：非透传路径会把体反序列化成 map 再编回去，重复键在到达改写之前就被
// Go 收敛成最后一个，测不到这里要证明的东西。透传与 WS 按字节转发，重复键真的会到。
func TestCodexWebSearchUserLocationHandlesDuplicateKeys(t *testing.T) {
	// 三份而不是两份：删一次再 set 一次恰好能盖住两份，覆盖不到"只删第一个"这个缺陷。
	t.Run("重复的 user_location 必须全删", func(t *testing.T) {
		tool := `{"type":"web_search"` +
			strings.Repeat(`,"user_location":`+userLocationTestClientRaw, 3) + `}`
		body := wireUserLocationTestBodyWithTools(t, tool)
		c := newConvTestContext(t, body)
		account := wireUserLocationTestAccount(true)
		account.Extra["openai_passthrough"] = true
		svc, up := wireProfileTestService()

		_, _ = svc.Forward(context.Background(), c, account, body)

		require.NotNil(t, up.lastReq)
		require.NotContains(t, string(up.lastBody), "Shanghai")
		require.Equal(t, 1, strings.Count(string(up.lastBody), `"user_location"`))
	})

	// 假的首个 type 不能把位置藏过去：gjson 读第一个（function），上游读最后一个
	// （web_search）。只看 tool.Get("type") 就会放行。
	t.Run("重复的 type 按命中处理", func(t *testing.T) {
		tool := `{"type":"function","type":"web_search","user_location":` + userLocationTestClientRaw + `}`
		body := wireUserLocationTestBodyWithTools(t, tool)
		require.Equal(t, "function", gjson.GetBytes(body, "tools.0.type").String(),
			"前置：gjson 看到的首个 type 不是 web_search")
		c := newConvTestContext(t, body)
		account := wireUserLocationTestAccount(true)
		account.Extra["openai_passthrough"] = true
		svc, up := wireProfileTestService()

		_, _ = svc.Forward(context.Background(), c, account, body)

		require.NotNil(t, up.lastReq)
		require.Equal(t, "function", gjson.GetBytes(up.lastBody, "tools.0.type").String(),
			"前置：透传路径保留了重复键")
		require.NotContains(t, string(up.lastBody), "Shanghai")
	})
}

// user_location 存在但不是对象：真客户端产不出，改过的客户端能，照样得对齐。
func TestCodexWebSearchUserLocationDropsNonObjectValue(t *testing.T) {
	body := wireUserLocationTestBody(t, "web_search", `"Shanghai"`)
	c := newConvTestContext(t, body)
	svc, up := wireProfileTestService()

	_, _ = svc.Forward(context.Background(), c, wireUserLocationTestAccount(true), body)

	require.NotNil(t, up.lastReq)
	require.NotContains(t, string(up.lastBody), "Shanghai")
	// 断到具体值，否则"整个工具被剥掉"也能让上面那条通过。
	require.Equal(t, userLocationTestCity,
		gjson.GetBytes(up.lastBody, "tools.0.user_location.city").String())
}

// 显式 null 等于"没报位置"（上游两处字段都是 Option，null 反序列化成 None）。
// gjson 的 Exists() 对 null 返回 true，只判 Exists 会把它改写成一个完整的出口位置——
// 等于替一个没报位置的客户端补一个，比不改写更异常。
func TestCodexWebSearchUserLocationLeavesExplicitNull(t *testing.T) {
	t.Run("responses", func(t *testing.T) {
		body := wireUserLocationTestBody(t, "web_search", `null`)

		out := rewriteCodexWebSearchUserLocationWith(wireUserLocationTestAccount(true), wireTimezoneTestExit, body)

		require.Equal(t, string(body), string(out))
		require.Equal(t, gjson.Null, gjson.GetBytes(out, "tools.0.user_location").Type)
	})

	t.Run("alpha_search", func(t *testing.T) {
		body := []byte(`{"query":"weather","settings":{"user_location":null}}`)
		c := newConvTestContext(t, body)

		out := rewriteCodexAlphaSearchUserLocation(c, wireUserLocationTestAccount(true), body)

		require.Equal(t, string(body), string(out))
	})
}

// 出口地理里的 & 不能被转义成 &：真客户端出线走 serde_json，不转义。
func TestCodexWebSearchUserLocationDoesNotEscapeHTML(t *testing.T) {
	body := wireUserLocationTestBody(t, "web_search", userLocationTestClientRaw)
	c := newConvTestContext(t, body)
	account := wireUserLocationTestAccount(true)
	account.Extra[codexWireLocationRegionExtraKey] = "Dadra & Nagar Haveli"
	svc, up := wireProfileTestService()

	_, _ = svc.Forward(context.Background(), c, account, body)

	require.NotNil(t, up.lastReq)
	// encoding/json 默认 EscapeHTML 会把 & 写成 &，那样这条 Contains 就会失败。
	// 真客户端出线走 serde_json::to_string，不转义。
	require.Contains(t, string(up.lastBody), "Dadra & Nagar Haveli")
}

// WS 帧与 HTTP 走同一条规则，删掉 WS 那个挂载点这条必须红。
func TestCodexDeviceWireProfileWSFrameRewritesUserLocation(t *testing.T) {
	account := wireUserLocationTestAccount(true)
	payload := []byte(`{"type":"response.create","model":"gpt-5.5","tools":[` +
		wireUserLocationTestTool(t, "web_search", userLocationTestClientRaw) + `]}`)
	c := newConvTestContext(t, payload)

	out := applyCodexWSFrameWireProfile(c, account, payload, "")

	require.NotContains(t, string(out), "Shanghai")
	require.Equal(t, userLocationTestCity, gjson.GetBytes(out, "tools.0.user_location.city").String())
	require.Equal(t, wireTimezoneTestExit, gjson.GetBytes(out, "tools.0.user_location.timezone").String())
}

// /alpha/search 的体是另一套结构：位置直接挂在 settings 下（codex-api/src/search.rs:231
// SearchSettings.user_location），没有 tools 数组。走 ForwardAlphaSearch 端到端验两条
// 分支：OAuth 走主路径直发 /alpha/search，PAT 走兜底、由
// buildOpenAIAlphaSearchResponsesWebSearchBody 把 settings 里的位置搬进 tools。
// 改写挂在 PAT 判定之前，两条都覆盖得到。
func TestCodexAlphaSearchUserLocationAlignedToExit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pat      bool
		geo      bool
		wantPath string
	}{
		{name: "oauth_main_path", geo: true, wantPath: "settings.user_location"},
		{name: "pat_responses_fallback", pat: true, geo: true, wantPath: "tools.0.user_location"},
		{name: "oauth_no_geo_drops_field", wantPath: "settings.user_location"},
		{name: "pat_no_geo_drops_field", pat: true, wantPath: "tools.0.user_location"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			body, err := sjson.SetRaw(`{"id":"search-session","model":"gpt-5.6-sol",`+
				`"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"weather"}]}],`+
				`"settings":{"external_web_access":true}}`, "settings.user_location", userLocationTestClientRaw)
			require.NoError(t, err)

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search?feature=standalone",
				bytes.NewReader([]byte(body)))
			c.Request.Header.Set("Content-Type", "application/json")
			up := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"output":"ok"}`)),
			}}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up}
			account := wireUserLocationTestAccount(tc.geo)
			if tc.pat {
				account.Credentials["auth_mode"] = "personalaccesstoken"
				require.True(t, account.IsOpenAIPersonalAccessToken(), "前置：走的是 PAT 兜底")
			}

			_, _ = svc.ForwardAlphaSearch(context.Background(), c, account, []byte(body))

			require.NotNil(t, up.lastReq)
			require.NotContains(t, string(up.lastBody), "Shanghai", "客户端本机城市不得出站")
			if !tc.geo {
				require.False(t, gjson.GetBytes(up.lastBody, tc.wantPath).Exists(),
					"拿不到出口地理时整个字段删掉")
				return
			}
			require.Equal(t, userLocationTestCity,
				gjson.GetBytes(up.lastBody, tc.wantPath+".city").String())
			require.Equal(t, wireTimezoneTestExit,
				gjson.GetBytes(up.lastBody, tc.wantPath+".timezone").String())
		})
	}
}

func TestCodexAlphaSearchUserLocationUntouchedWhenNotApplicable(t *testing.T) {
	body, err := sjson.SetRaw(`{"query":"weather"}`, "settings.user_location", userLocationTestClientRaw)
	require.NoError(t, err)
	c := newConvTestContext(t, []byte(body))

	// 没发就不补。
	bare := []byte(`{"query":"weather"}`)
	require.Equal(t, bare, rewriteCodexAlphaSearchUserLocation(c, wireUserLocationTestAccount(true), bare))
	// 非双开账号原样不动。
	require.Equal(t, []byte(body),
		rewriteCodexAlphaSearchUserLocation(c, wireProfileTestAccount(false), []byte(body)))
	// 时区没解析出来时整条 no-op。
	noTZ := wireProfileTestAccount(true)
	require.Equal(t, []byte(body), rewriteCodexAlphaSearchUserLocation(c, noTZ, []byte(body)))
}

// codexWireLocation 的失效判定必须和时区共用一套：换了出口，旧地理也不能再用。
func TestCodexWireLocationFollowsProxyTag(t *testing.T) {
	account := wireUserLocationTestAccount(true)
	city, region, country, ok := codexWireLocation(account)
	require.True(t, ok)
	require.Equal(t, userLocationTestCity, city)
	require.Equal(t, userLocationTestRegion, region)
	require.Equal(t, userLocationTestCountry, country)

	account.Extra[codexWireTimezoneResolvedProxyExtraKey] = "proxy:999|stale"
	_, _, _, ok = codexWireLocation(account)
	require.False(t, ok, "换了出口后旧地理必须失效")

	account.Extra[codexWireTimezoneResolvedProxyExtraKey] = codexWireTimezoneProxyTag(account)
	account.Extra[codexWireLocationRegionExtraKey] = "  "
	_, _, _, ok = codexWireLocation(account)
	require.False(t, ok, "三项缺一即视为不可用")
}

// 手动覆盖时区后，自动解析来的地理必须一起失效——两者来源不同，拼起来就是单个对象
// 内部的矛盾（拉斯维加斯 + Asia/Tokyo）。而 shouldResolveCodexWireTimezone 在覆盖
// 存在时拒绝重解析，地理永远追不上，这个状态是永久的。
func TestCodexWireLocationDisabledByManualTimezoneOverride(t *testing.T) {
	account := wireUserLocationTestAccount(true)
	_, _, _, ok := codexWireLocation(account)
	require.True(t, ok, "前置：自动解析齐全时可用")

	account.Extra[codexWireTimezoneExtraKey] = "Asia/Tokyo"
	_, _, _, ok = codexWireLocation(account)
	require.False(t, ok, "手动覆盖时区后地理必须失效")

	// 非法的手动值会被 codexWireTimezoneName 跳过、落回自动解析结果；这种情况下覆盖
	// 根本没生效，地理与时区仍然同源，不该被误杀。
	account.Extra[codexWireTimezoneExtraKey] = "America/Los_Angelas" // 拼错，LoadLocation 失败
	require.Equal(t, wireTimezoneTestExit, codexWireTimezoneName(account),
		"前置：非法手动值不生效")
	_, _, _, ok = codexWireLocation(account)
	require.True(t, ok, "覆盖没生效时地理必须保留")

	account.Extra[codexWireTimezoneExtraKey] = "Asia/Tokyo"

	// 出站表现：整个 user_location 被删掉，而不是拼出"拉斯维加斯 + Asia/Tokyo"。
	body := wireUserLocationTestBody(t, "web_search", userLocationTestClientRaw)
	c := newConvTestContext(t, body)
	svc, up := wireProfileTestService()

	_, _ = svc.Forward(context.Background(), c, account, body)

	require.NotNil(t, up.lastReq)
	require.Equal(t, "Asia/Tokyo", codexWireTimezoneName(account), "前置：出站时区用的是手动值")
	require.NotContains(t, string(up.lastBody), userLocationTestCity)
	require.NotContains(t, string(up.lastBody), "Shanghai")
	require.False(t, gjson.GetBytes(up.lastBody, "tools.0.user_location").Exists())
}

// 半改写体：多个工具里有一个改不动时，不能留下"第一个已对齐、第二个还是上海"。
func TestCodexWebSearchUserLocationAllOrNothing(t *testing.T) {
	account := wireUserLocationTestAccount(true)

	ok := wireUserLocationTestBodyWithTools(t,
		wireUserLocationTestTool(t, "web_search", userLocationTestClientRaw),
		wireUserLocationTestTool(t, "web_search", userLocationTestClientRaw),
	)
	require.NotContains(t, string(rewriteCodexWebSearchUserLocationWith(account, wireTimezoneTestExit, ok)),
		"Shanghai", "前置：正常情况两个工具都改")

	// 第二个工具的重复键超过删除次数上限，rewriteCodexUserLocationAt 会放弃这一处。
	stuck := `{"type":"web_search"` +
		strings.Repeat(`,"user_location":`+userLocationTestClientRaw, codexUserLocationDeleteAttempts+1) + `}`
	body := wireUserLocationTestBodyWithTools(t,
		wireUserLocationTestTool(t, "web_search", userLocationTestClientRaw), stuck)

	out := rewriteCodexWebSearchUserLocationWith(account, wireTimezoneTestExit, body)

	require.Equal(t, string(body), string(out), "改不动就整条放弃")
	require.Equal(t, "Shanghai", gjson.GetBytes(out, "tools.0.user_location.city").String(),
		"第一个工具必须一起回滚，不能留半改写体")
}
