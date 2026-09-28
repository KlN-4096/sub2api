//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// headerValuesFold 忽略 casing 收集同名头：覆写按 wire casing 直接写 map，
// 未知头名原样保持小写（resolveWireCasing），http.Header.Get 按 canonical key 查会漏掉。
func headerValuesFold(h http.Header, name string) []string {
	var out []string
	for key, values := range h {
		if strings.EqualFold(key, name) {
			out = append(out, values...)
		}
	}
	return out
}

// 账号级请求头覆写不得伪造 content-encoding：双开的体是网关压出来的 zstd，
// 静态覆写必然与实际字节不符。OpenAI 平台只有 api_key 账号能开覆写，用它构造。
func TestHeaderOverrideCannotForgeContentEncoding(t *testing.T) {
	account := &Account{
		ID:       4242,
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Credentials: map[string]any{
			"api_key":                    "sk-test",
			credKeyHeaderOverrideEnabled: true,
			credKeyHeaderOverrides: map[string]any{
				"content-encoding": "gzip",
				"x-custom-header":  "kept",
			},
		},
	}
	headers := http.Header{}
	headers.Set("Content-Encoding", "zstd")

	account.ApplyHeaderOverrides(headers)

	require.Equal(t, []string{"zstd"}, headerValuesFold(headers, "content-encoding"),
		"覆写不得改写实际体编码，也不得追加第二个同名头")
	require.Equal(t, []string{"kept"}, headerValuesFold(headers, "x-custom-header"),
		"其它覆写照常生效——证明上一条不是因为覆写整体没开")

	require.Error(t, NormalizeHeaderOverrideCredentials(map[string]any{
		credKeyHeaderOverrideEnabled: true,
		credKeyHeaderOverrides:       map[string]any{"Content-Encoding": "gzip"},
	}), "保存路径直接 400，不是存下来再静默丢弃")
}

// PAT 账号 /alpha/search 的 /responses 兜底此前是「双开的体 + 非双开的头」。
// 线协议投影会去掉旧的 responses=experimental，与常规 /responses 一致。
func TestCodexDeviceWireProfilePATAlphaSearchFallbackAppliesWireProfile(t *testing.T) {
	for name, enabled := range map[string]bool{"wire_profile_on": true, "wire_profile_off": false} {
		t.Run(name, func(t *testing.T) {
			body := []byte(`{"id":"search-session","model":"gpt-5.4","commands":{"search_query":[{"q":"news"}]}}`)
			c := newConvTestContext(t, body)
			c.Request.URL.Path = "/v1/alpha/search"
			account := wireProfileTestAccount(enabled)
			account.Credentials["auth_mode"] = OpenAIAuthModePersonalAccessToken
			account.Credentials["access_token"] = "at-offline-token"
			svc, up := wireProfileTestService()
			up.resp = &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(alphaSearchResponsesSSE("offline result"))),
			}

			_, err := svc.ForwardAlphaSearch(context.Background(), c, account, body)

			require.NoError(t, err)
			require.NotNil(t, up.lastReq)
			require.Equal(t, chatgptCodexURL, up.lastReq.URL.String())
			if enabled {
				require.Empty(t, up.lastReq.Header.Get("OpenAI-Beta"),
					"双开出站不带旧的 responses=experimental")
			} else {
				require.Equal(t, "responses=experimental", up.lastReq.Header.Get("OpenAI-Beta"),
					"非双开维持既有行为")
			}
		})
	}
}
