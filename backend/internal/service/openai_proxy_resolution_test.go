package service

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

type configuredProxyRepoStub struct {
	ProxyRepository
	proxy *Proxy
	err   error
	calls int
}

func TestConfiguredProxyResolutionPreservesExplicitRoutes(t *testing.T) {
	proxyID := int64(7)
	proxy := &Proxy{ID: proxyID, Protocol: "http", Host: "127.0.0.1", Port: 8123}
	t.Run("unbound_ignores_stale_relation", func(t *testing.T) {
		value, err := resolveConfiguredProxyURL(context.Background(), nil, nil, proxy)
		require.NoError(t, err)
		require.Empty(t, value)
	})
	t.Run("loaded_and_repository", func(t *testing.T) {
		repo := &configuredProxyRepoStub{proxy: proxy}
		value, err := resolveConfiguredProxyURL(context.Background(), repo, &proxyID, proxy)
		require.NoError(t, err)
		require.Equal(t, proxy.URL(), value)
		require.Zero(t, repo.calls)
		value, err = resolveConfiguredProxyURL(context.Background(), repo, &proxyID, nil)
		require.NoError(t, err)
		require.Equal(t, proxy.URL(), value)
		require.Equal(t, 1, repo.calls)
	})
	t.Run("invalid_url_hides_credentials", func(t *testing.T) {
		invalid := &Proxy{ID: proxyID, Protocol: "ftp", Host: "proxy.invalid", Port: 80, Username: "private-user", Password: "private-password"}
		value, err := resolveConfiguredProxyURL(context.Background(), nil, &proxyID, invalid)
		require.Empty(t, value)
		require.ErrorContains(t, err, "proxy")
		require.NotContains(t, err.Error(), invalid.Password)
	})
	t.Run("mismatched_loaded_binding", func(t *testing.T) {
		other := *proxy
		other.ID++
		value, err := resolveConfiguredProxyURL(context.Background(), nil, &proxyID, &other)
		require.Empty(t, value)
		require.ErrorContains(t, err, "binding")
	})
}

func TestOpenAIProxyBindingGuardsTransportBoundaries(t *testing.T) {
	proxyID := int64(7)
	account := &Account{ID: 1, Platform: PlatformOpenAI, ProxyID: &proxyID}
	request, err := http.NewRequest(http.MethodPost, "https://upstream.invalid/responses", nil)
	require.NoError(t, err)
	// No transport is installed: the missing binding must be rejected before any dial.
	svc := &OpenAIGatewayService{}
	_, err = svc.doOpenAIUpstream(request, "", account)
	require.ErrorContains(t, err, "proxy")
	_, err = (&AccountTestService{}).doOpenAIAccountTestUpstream(request, "", account, false)
	require.ErrorContains(t, err, "proxy")
	pool := newOpenAIWSConnPool(&config.Config{})
	t.Cleanup(pool.Close)
	_, err = pool.Acquire(context.Background(), openAIWSAcquireRequest{Account: account, WSURL: "wss://upstream.invalid/responses"})
	require.ErrorContains(t, err, "proxy")
	require.NoError(t, requireOpenAIProxyBinding(&Account{Platform: PlatformOpenAI}, ""))
	require.NoError(t, requireOpenAIProxyBinding(&Account{Platform: PlatformGrok, ProxyID: &proxyID}, ""))
}

func (r *configuredProxyRepoStub) GetByID(context.Context, int64) (*Proxy, error) {
	r.calls++
	return r.proxy, r.err
}

func TestOpenAIConfiguredProxyFailureStopsBeforeNetwork(t *testing.T) {
	for _, name := range []string{"missing_repository", "missing_proxy", "lookup_error"} {
		t.Run(name, func(t *testing.T) {
			proxyID := int64(7)
			account := &Account{
				ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ProxyID: &proxyID,
				Credentials: map[string]any{"chatgpt_account_id": "offline-account", "refresh_token": "offline-refresh"},
			}
			var proxyRepo ProxyRepository
			if name != "missing_repository" {
				stub := &configuredProxyRepoStub{}
				if name == "lookup_error" {
					stub.err = errors.New("proxy database unavailable")
				}
				proxyRepo = stub
			}
			t.Run("quota", func(t *testing.T) {
				repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{account.ID: account}}
				cache := &stubQuotaTokenCache{tokens: map[string]string{OpenAITokenCacheKey(account): "offline-token"}}
				clientCalls := 0
				svc := NewOpenAIQuotaService(repo, proxyRepo, NewOpenAITokenProvider(repo, cache, nil),
					func(string) (*req.Client, error) {
						clientCalls++
						return nil, errors.New("unexpected outbound client")
					}, nil)
				_, err := svc.QueryUsage(context.Background(), account.ID)
				require.Zero(t, clientCalls, "已配置代理不可用时不得构造默认出口客户端")
				require.ErrorContains(t, err, "proxy")
			})
			t.Run("oauth_refresh", func(t *testing.T) {
				client := &openaiOAuthClientRefreshStub{}
				svc := NewOpenAIOAuthService(proxyRepo, client)
				t.Cleanup(svc.Stop)
				_, err := svc.RefreshAccountToken(context.Background(), account)
				require.Zero(t, client.refreshCalls, "代理解析失败必须早于 token 出站刷新")
				require.ErrorContains(t, err, "proxy")
			})
		})
	}
}
