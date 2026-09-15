//go:build unit

package service

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 真客户端的 environment_context 形态（core/src/context/world_state/environment.rs:296-329
// push_environment_values + :290-291 的 <current_date> / <timezone>）。标签在请求体的 input
// 文本里，出站必须仍是字面量 <，被转义成 < 就说明序列化路径换了编码器。
// 固定创建时刻：历史环境必须按这次创建的时间换算，不能跟着测试运行日期变化。
var wireTimezoneTestCreatedAt = time.Date(2026, 3, 1, 3, 30, 0, 0, time.UTC)

func wireTimezoneTestEnvContext(t *testing.T) string {
	t.Helper()
	return "<environment_context>\n  <cwd>/home/dev/proj</cwd>\n" +
		"  <shell>bash</shell>\n  <current_date>" + wireTimezoneExpectedDate(t, "Asia/Shanghai") +
		"</current_date>\n  <timezone>Asia/Shanghai</timezone>\n</environment_context>"
}

const wireTimezoneTestExit = "America/Los_Angeles"

// wireTimezoneTestBody 保留共享身份夹具，环境项带原版的内容分类与固定创建时间。
func wireTimezoneTestBody(t *testing.T) []byte {
	t.Helper()
	body := wireProfileTestBody(t)
	input := "[" + wireTimezoneTestMessage(wireTimezoneTestEnvContext(t), wireTimezoneTestCreatedAt) + "]"
	body, err := sjson.SetRawBytes(body, "input", []byte(input))
	require.NoError(t, err)
	return body
}

func wireTimezoneTestMessage(text string, createdAt time.Time) string {
	return `{"type":"message","role":"user","content":[{"type":"input_text","text":` +
		strconv.Quote(text) + `}],"internal_chat_message_metadata_passthrough":{` +
		`"content_item_kinds":["environments.environment_context"],"create_time":` +
		strconv.FormatInt(createdAt.Unix(), 10) + `}}`
}

func wireTimezoneExpectedDate(t *testing.T, name string) string {
	t.Helper()
	loc, err := time.LoadLocation(name)
	require.NoError(t, err)
	return wireTimezoneTestCreatedAt.In(loc).Format("2006-01-02")
}

func TestCodexDeviceWireProfileRewritesEnvironmentTimezone(t *testing.T) {
	for _, tc := range []struct {
		name        string
		passthrough bool
		compact     bool
	}{
		{name: "map"},
		{name: "raw", passthrough: true},
		{name: "compact", compact: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := wireTimezoneTestBody(t)
			c := newConvTestContext(t, body)
			if tc.compact {
				c.Request.URL.Path = "/v1/responses/compact"
			}
			account := wireProfileTestAccount(true)
			account.Extra["openai_passthrough"] = tc.passthrough
			account.Extra[codexWireTimezoneExtraKey] = wireTimezoneTestExit
			svc, up := wireProfileTestService()

			_, _ = svc.Forward(context.Background(), c, account, body)

			require.NotNil(t, up.lastReq)
			out := string(up.lastBody)
			require.Contains(t, out, "<timezone>"+wireTimezoneTestExit+"</timezone>")
			require.NotContains(t, out, "Asia/Shanghai")
			require.Contains(t, out,
				"<current_date>"+wireTimezoneExpectedDate(t, wireTimezoneTestExit)+"</current_date>")
			// 只动这两个标签：cwd / shell 与其余请求体原样。
			require.Contains(t, out, "<cwd>/home/dev/proj</cwd>")
			require.Contains(t, out, "<shell>bash</shell>")
			require.True(t, gjson.ValidBytes(up.lastBody), "改写后仍是合法 JSON")
			require.NotEmpty(t, gjson.GetBytes(up.lastBody, "model").String())
		})
	}
}

func TestCodexEnvironmentTimezoneUntouchedWhenNotApplicable(t *testing.T) {
	for _, tc := range []struct {
		name        string
		wireProfile bool
		timezone    string
	}{
		{name: "non_wire_profile", wireProfile: false, timezone: wireTimezoneTestExit},
		{name: "no_timezone_configured", wireProfile: true},
		{name: "invalid_timezone", wireProfile: true, timezone: `Not/A"Zone`},
		{name: "unknown_timezone", wireProfile: true, timezone: "Mars/Olympus"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := wireTimezoneTestBody(t)
			c := newConvTestContext(t, body)
			account := wireProfileTestAccount(tc.wireProfile)
			if tc.timezone != "" {
				account.Extra[codexWireTimezoneExtraKey] = tc.timezone
			}
			svc, up := wireProfileTestService()

			_, _ = svc.Forward(context.Background(), c, account, body)

			require.NotNil(t, up.lastReq)
			out := string(up.lastBody)
			require.Contains(t, out, "<timezone>Asia/Shanghai</timezone>", "不适用时不得改写")
			require.Contains(t, out,
				"<current_date>"+wireTimezoneExpectedDate(t, "Asia/Shanghai")+"</current_date>")
		})
	}
}

func TestCodexWireTimezoneNameResolutionOrder(t *testing.T) {
	account := wireProfileTestAccount(true)
	require.Empty(t, codexWireTimezoneName(account))

	account.Extra[codexWireTimezoneResolvedExtraKey] = "Europe/Berlin"
	account.Extra[codexWireTimezoneResolvedProxyExtraKey] = "none"
	require.Equal(t, "Europe/Berlin", codexWireTimezoneName(account), "无手动值时用自动解析结果")

	account.Extra[codexWireTimezoneExtraKey] = "Asia/Tokyo"
	require.Equal(t, "Asia/Tokyo", codexWireTimezoneName(account), "手动覆盖优先")

	account.Extra[codexWireTimezoneExtraKey] = "../etc/passwd"
	require.Equal(t, "Europe/Berlin", codexWireTimezoneName(account), "非法手动值回落到自动解析结果")

	account.Extra[codexWireTimezoneResolvedExtraKey] = "Mars/Olympus"
	require.Empty(t, codexWireTimezoneName(account), "两个值都非法则不改写")
}

// envBlock 造一个最小的 environment_context 块：改写只在这个区间内发生，
// 用裸标签写夹具会因为"根本没进区间"而假绿。
func envBlock(date, tz string) string {
	return "<environment_context><current_date>" + date + "</current_date><timezone>" +
		tz + "</timezone></environment_context>"
}

func TestShouldResolveCodexWireTimezone(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	fresh := now.Add(-time.Hour).Format(time.RFC3339)
	stale := now.Add(-25 * time.Hour).Format(time.RFC3339)

	resolved := func(at, proxyTag string) *Account {
		a := wireProfileTestAccount(true)
		a.Extra[codexWireTimezoneResolvedExtraKey] = "Europe/Berlin"
		a.Extra[codexWireTimezoneResolvedAtExtraKey] = at
		a.Extra[codexWireTimezoneResolvedProxyExtraKey] = proxyTag
		return a
	}

	t.Run("not_wire_profile", func(t *testing.T) {
		require.False(t, shouldResolveCodexWireTimezone(wireProfileTestAccount(false), wireProfileTestAccount(false), now))
	})
	t.Run("manual_override_never_resolves", func(t *testing.T) {
		a := resolved(stale, "none")
		a.Extra[codexWireTimezoneExtraKey] = "Asia/Tokyo"
		require.False(t, shouldResolveCodexWireTimezone(a, a, now))
	})
	t.Run("never_resolved", func(t *testing.T) {
		require.True(t, shouldResolveCodexWireTimezone(wireProfileTestAccount(true), wireProfileTestAccount(true), now))
	})
	t.Run("fresh", func(t *testing.T) {
		require.False(t, shouldResolveCodexWireTimezone(resolved(fresh, "none"), resolved(fresh, "none"), now))
	})
	t.Run("stale", func(t *testing.T) {
		require.True(t, shouldResolveCodexWireTimezone(resolved(stale, "none"), resolved(stale, "none"), now))
	})
	t.Run("proxy_changed_resolves_immediately", func(t *testing.T) {
		a := resolved(fresh, "proxy:7")
		require.True(t, shouldResolveCodexWireTimezone(a, a, now), "换出口当轮就要重解析")
	})
	t.Run("proxy_tag_matches_current_proxy", func(t *testing.T) {
		id := int64(7)
		proxy := &Proxy{ID: id, Protocol: "socks5", Host: "10.0.0.9", Port: 1080}
		a := resolved(fresh, "")
		a.ProxyID, a.Proxy = &id, proxy
		a.Extra[codexWireTimezoneResolvedProxyExtraKey] = codexWireTimezoneProxyTag(a)
		require.False(t, shouldResolveCodexWireTimezone(a, a, now))

		// 同一条代理行被原地改到别的出口：只比 ID 的话要等 24h TTL 才跟上。
		a.Proxy = &Proxy{ID: id, Protocol: "socks5", Host: "10.0.0.9", Port: 1081}
		require.True(t, shouldResolveCodexWireTimezone(a, a, now), "代理 URL 变了就要重解析")
	})
	// 影子行不带收敛开关，闸门必须看凭证账号——否则请求时会改写、解析时却不解析。
	t.Run("shadow_row_uses_credential_account", func(t *testing.T) {
		shadow := wireProfileTestAccount(true)
		delete(shadow.Extra, codexFingerprintConvergenceExtraKey)
		require.False(t, shouldResolveCodexWireTimezone(shadow, shadow, now), "自比时开关不在，判不出")
		require.True(t, shouldResolveCodexWireTimezone(shadow, wireProfileTestAccount(true), now),
			"凭证账号开了收敛就该解析")
	})
	t.Run("unparsable_timestamp", func(t *testing.T) {
		require.True(t, shouldResolveCodexWireTimezone(resolved("yesterday", "none"), resolved("yesterday", "none"), now))
	})
}

func TestCodexWireTimezoneExtraUpdates(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	id := int64(7)
	account := wireProfileTestAccount(true)
	account.ProxyID = &id
	account.Proxy = &Proxy{ID: id, Protocol: "socks5", Host: "10.0.0.9", Port: 1080}

	updates := codexWireTimezoneExtraUpdates(codexWireTimezoneProxyTag(account), "24.120.102.167", "America/Los_Angeles", now)
	require.Equal(t, map[string]any{
		codexWireTimezoneResolvedExtraKey:      "America/Los_Angeles",
		codexWireTimezoneResolvedAtExtraKey:    "2026-03-01T12:00:00Z",
		codexWireTimezoneResolvedIPExtraKey:    "24.120.102.167",
		codexWireTimezoneResolvedProxyExtraKey: codexWireTimezoneProxyTag(account),
	}, updates)

	require.Nil(t, codexWireTimezoneExtraUpdates(codexWireTimezoneProxyTag(account), "1.2.3.4", "Mars/Olympus", now),
		"上游返回的时区名非法时保留旧值")
	require.Nil(t, codexWireTimezoneExtraUpdates(codexWireTimezoneProxyTag(account), "1.2.3.4", "", now))
}

func TestCodexDeviceWireProfileWSFrameRewritesTimezone(t *testing.T) {
	account := wireProfileTestAccount(true)
	account.Extra[codexWireTimezoneExtraKey] = wireTimezoneTestExit
	payload := []byte(`{"type":"response.create","model":"gpt-5.5","input":[` +
		wireTimezoneTestMessage(wireTimezoneTestEnvContext(t), wireTimezoneTestCreatedAt) + `]}`)
	c := newConvTestContext(t, payload)

	out := string(applyCodexWSFrameWireProfile(c, account, payload, ""))

	require.Contains(t, out, "<timezone>"+wireTimezoneTestExit+"</timezone>")
	require.Contains(t, out, "<current_date>"+wireTimezoneExpectedDate(t, wireTimezoneTestExit)+"</current_date>")
	require.NotContains(t, out, "Asia/Shanghai")
	require.Contains(t, out, "<cwd>/home/dev/proj</cwd>")
	require.True(t, gjson.ValidBytes([]byte(out)))
}

// 区间配对必须是线性的。旧实现"边配对边回头验区间"在客户端可控的体上是 O(n²)：
// N 个开标签放进一个 JSON 字符串、N 个闭标签放进另一个，数量闸门恰好平衡，每个候选区间
// 都要扫到第一个未转义引号才判否，而拒绝时游标只前进一个标签长度。这段跑在同步转发路径
// 与每个 WS 帧上、不看 ctx，网关超时杀不掉；旧实现在这个 1 MB 夹具上要跑几秒。
func TestCodexEnvironmentContextBlocksStaysLinearOnAdversarialBody(t *testing.T) {
	const n = 20000
	var b bytes.Buffer
	b.WriteString(`{"input":[{"text":"`)
	b.WriteString(strings.Repeat("<environment_context>", n))
	b.WriteString(`"},{"text":"`)
	b.WriteString(strings.Repeat("</environment_context>", n))
	b.WriteString(`"}]}`)
	body := b.Bytes()
	require.Greater(t, len(body), 800*1024, "夹具要够大才有区分度")
	marked := []byte(`{"input":[` + wireTimezoneTestMessage(
		strings.Repeat("<environment_context>", n)+strings.Repeat("</environment_context>", n),
		wireTimezoneTestCreatedAt) + `]}`)

	done := make(chan bool, 1)
	go func() {
		done <- bytes.Equal(body, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, body)) &&
			bytes.Equal(marked, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, marked))
	}()
	select {
	case unchanged := <-done:
		require.True(t, unchanged, "跨 JSON 字符串的伪环境不得被改写")
	case <-time.After(3 * time.Second):
		t.Fatal("区间配对退化成二次复杂度：客户端可控的请求体能钉死一个核")
	}
}
