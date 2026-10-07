package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSConnPoolConfigurationCompatibility(t *testing.T) {
	for _, change := range []struct {
		name string
		edit func(*openAIWSAcquireRequest)
	}{
		{"proxy", func(r *openAIWSAcquireRequest) { r.ProxyURL = "http://127.0.0.1:8002" }},
		{"endpoint", func(r *openAIWSAcquireRequest) { r.WSURL = "wss://other.invalid/responses" }},
		{"user_agent", func(r *openAIWSAcquireRequest) { r.Headers.Set("User-Agent", "client-b") }},
		{"ws_protocol", func(r *openAIWSAcquireRequest) { r.Headers.Set("OpenAI-Beta", openAIWSBetaV1Value) }},
		{"language", func(r *openAIWSAcquireRequest) { r.Headers.Set("Accept-Language", "zh-CN") }},
		{"account", func(r *openAIWSAcquireRequest) { r.Headers.Set("Chatgpt-Account-Id", "account-b") }},
		{"seed", func(r *openAIWSAcquireRequest) {
			r.Account.Extra[codexFingerprintSeedExtraKey] = "22222222-2222-4222-8222-222222222222"
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 2
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 2
			pool := newOpenAIWSConnPool(cfg)
			t.Cleanup(pool.Close)
			pool.setClientDialerForTest(&openAIWSCountingDialer{})
			account := activeCodexFingerprintPoolAccountForTest(9120)
			account.Extra[codexFingerprintModeExtraKey] = "device"
			request := openAIWSAcquireRequest{
				Account: account, WSURL: "wss://upstream.invalid/responses",
				ProxyURL: "http://127.0.0.1:8001",
				Headers:  http.Header{"User-Agent": {"client-a"}, "Chatgpt-Account-Id": {"account-a"}},
			}
			first, err := pool.Acquire(context.Background(), request)
			require.NoError(t, err)
			t.Cleanup(first.Release)
			next := cloneOpenAIWSAcquireRequest(request)
			next.Account = snapshotOAuthRefreshAccount(account)
			next.Account.Extra = shallowCopyMap(account.Extra)
			change.edit(&next)
			second, err := pool.Acquire(context.Background(), next)
			require.NoError(t, err)
			t.Cleanup(second.Release)
			require.NotEqual(t, first.ConnID(), second.ConnID())
			select {
			case <-first.conn.closedCh:
				t.Fatal("configuration change closed an in-flight connection")
			default:
			}
			second.Release()
			first.Release()
			next.PreferredConnID, next.ForcePreferredConn = first.ConnID(), true
			_, err = pool.Acquire(context.Background(), next)
			require.ErrorIs(t, err, errOpenAIWSPreferredConnUnavailable, "old connection must not accept a new request")
			next.PreferredConnID, next.ForcePreferredConn = "", false
			third, err := pool.Acquire(context.Background(), next)
			require.NoError(t, err)
			defer third.Release()
			require.Equal(t, second.ConnID(), third.ConnID())
		})
	}
}

func TestOpenAIQuotaCallKeepsOneConfigurationSnapshot(t *testing.T) {
	const originalUA = "codex-tui/0.153.4 (Windows 10.0.26200; x86_64) WindowsTerminal (codex-tui; 0.153.4)"
	proxyID := int64(7)
	account := &Account{
		ID: 9121, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		ProxyID: &proxyID, Proxy: &Proxy{ID: proxyID, Protocol: "http", Host: "127.0.0.1", Port: 8001},
		Credentials: map[string]any{"access_token": "snapshot-token", "chatgpt_account_id": "snapshot-account"},
		Extra:       map[string]any{codexAccountUserAgentExtraKey: originalUA},
	}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	requests := make(chan http.Header, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"rate_limit":{"allowed":true},"rate_limit_reset_credits":{"available_count":0}}`))
	}))
	defer server.Close()
	redirect := newQuotaRedirectingFactory(server)
	svc := NewOpenAIQuotaService(repo, nil, NewOpenAITokenProvider(repo, nil, nil), func(proxyURL string) (*req.Client, error) {
		require.Equal(t, "http://127.0.0.1:8001", proxyURL)
		// Simulate an administrator update after route/token resolution.
		account.Extra[codexAccountUserAgentExtraKey] = "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466"
		account.Credentials["chatgpt_account_id"] = "new-account"
		return redirect(proxyURL)
	}, nil)
	_, err := svc.QueryUsage(context.Background(), account.ID)
	require.NoError(t, err)
	for range 2 {
		headers := <-requests
		require.Equal(t, resolveCodexOutboundIdentity(originalUA).userAgent, headers.Get("User-Agent"))
		require.Equal(t, "Bearer snapshot-token", headers.Get("Authorization"))
		require.Equal(t, "snapshot-account", headers.Get("Chatgpt-Account-Id"))
	}
}
