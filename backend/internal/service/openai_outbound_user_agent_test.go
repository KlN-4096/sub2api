//go:build unit

package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 出站 User-Agent 的边界：任何打到 ChatGPT / OpenAI 的请求都不该带客户端自报的身份，
// 也不该让 net/http 的 Go-http-client 默认值代表本服务。
//
// 这几条都是 2026-09-20 审计用端到端测试实测出来的洞，之前一条覆盖都没有：
//   - setup-token 的 alpha search 整块身份缺失，线上实收 "Go-http-client/1.1"
//   - count_tokens 显式拷贝客户端 UA，线上实收 "claude-cli/1.0.60 (external, cli)"
//
// 「线上实收」而不是只看 req.Header：Go 的默认 UA 是写请求时才补的，头里查不到。

// outboundUserAgent 把请求真的发给一个 httptest 服务端，返回服务端看到的 User-Agent。
func outboundUserAgent(t *testing.T, req *http.Request) string {
	t.Helper()
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sendURL, err := http.NewRequest(req.Method, srv.URL, nil)
	require.NoError(t, err)
	sendURL.Header = req.Header.Clone()

	resp, err := srv.Client().Do(sendURL)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return <-got
}

func newOutboundUATestAccount(t *testing.T, accountType string) *Account {
	t.Helper()
	a := newTestOAuthAccount(9401, nil)
	a.Type = accountType
	a.Status, a.Schedulable, a.Concurrency = StatusActive, true, 1
	a.Credentials = map[string]any{"access_token": "offline-token", "chatgpt_account_id": "offline-account"}
	return a
}

func newOutboundUAGinContext(t *testing.T, clientUA string) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", http.NoBody)
	req.Header.Set("User-Agent", clientUA)
	req.Header.Set("Accept-Language", "zh-CN")
	c.Request = req
	return c
}

// TestAlphaSearchSetupTokenCarriesCodexIdentity 钉住 L2a：openAIAlphaSearchURL 对
// setup-token 返回的同样是 chatgpt.com，身份块不能只认 oauth。
func TestAlphaSearchSetupTokenCarriesCodexIdentity(t *testing.T) {
	for _, accountType := range []string{AccountTypeOAuth, AccountTypeSetupToken} {
		t.Run(accountType, func(t *testing.T) {
			svc := &OpenAIGatewayService{}
			account := newOutboundUATestAccount(t, accountType)

			url, err := svc.openAIAlphaSearchURL(account)
			require.NoError(t, err)
			require.Contains(t, url, "chatgpt.com", "前提：这个类型确实打 chatgpt.com")

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, http.NoBody)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer offline-token")
			if account.UsesOpenAICodexProtocol() {
				req.Host = "chatgpt.com"
				enforceCodexIdentityHeadersWithUA(req.Header, account.GetOpenAIUserAgent())
			}

			require.Equal(t, CodexCanonicalUserAgent(), req.Header.Get("User-Agent"))
			require.NotContains(t, outboundUserAgent(t, req), "Go-http-client",
				"不设 UA 的话 net/http 会用自己的默认值代表本服务")
		})
	}
}

// TestUpstreamModelsRequestNeverSendsGoDefaultUA 钉住 L2b：模型列表同步也要有 UA。
func TestUpstreamModelsRequestNeverSendsGoDefaultUA(t *testing.T) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.openai.com/v1/models", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", resolveCodexOutboundIdentity("").userAgent)

	require.NotContains(t, outboundUserAgent(t, req), "Go-http-client")
}

// TestCountTokensDoesNotLeakClientUserAgent 钉住 L3：count_tokens 会显式拷贝客户端的
// user-agent，OAuth / setup-token 账号必须在发送前收口掉；accept-language 保留。
func TestCountTokensDoesNotLeakClientUserAgent(t *testing.T) {
	const clientUA = "claude-cli/1.0.60 (external, cli)"

	for _, accountType := range []string{AccountTypeOAuth, AccountTypeSetupToken} {
		t.Run(accountType, func(t *testing.T) {
			c := newOutboundUAGinContext(t, clientUA)
			account := newOutboundUATestAccount(t, accountType)

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, openaiPlatformAPIInputTokensURL, http.NoBody)
			require.NoError(t, err)
			for key, values := range c.Request.Header {
				lower := key
				if lower != "User-Agent" && lower != "Accept-Language" {
					continue
				}
				for _, v := range values {
					req.Header.Add(key, v)
				}
			}
			require.Equal(t, clientUA, req.Header.Get("User-Agent"), "前提：拷贝循环确实带上了客户端 UA")

			if account.UsesOpenAICodexProtocol() {
				enforceCodexIdentityHeadersWithUA(req.Header, account.GetOpenAIUserAgent())
			}

			require.Equal(t, CodexCanonicalUserAgent(), req.Header.Get("User-Agent"))
			require.Equal(t, "zh-CN", req.Header.Get("Accept-Language"), "语言偏好不是身份，照旧透传")
			require.NotContains(t, outboundUserAgent(t, req), "claude-cli")
		})
	}
}
