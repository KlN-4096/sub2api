package service

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// 生产里的连接池只在 getOpenAIWSConnPool 构造，罐必须在那里接上。
func TestGetOpenAIWSConnPool_WiresCodexCookieStore(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: codexWSWireProfileConfig()}
	pool := svc.getOpenAIWSConnPool()
	require.NotNil(t, pool)
	require.Same(t, &svc.codexCookies, pool.cookies)
}

// 连接池拨号：握手前带上罐里的 cookie，握手响应的 Set-Cookie 收进同一只罐。
func TestOpenAIWSConnPoolDialConn_ReplaysAndStoresCodexCookies(t *testing.T) {
	account := codexCookieTestAccount(31, AccountTypeOAuth)
	const wsURL = "wss://chatgpt.com/backend-api/codex/responses"
	dialer := &codexWSStagedDialer{
		conns:     []openAIWSClientConn{&openAIWSCaptureConn{}},
		handshake: http.Header{"Set-Cookie": []string{"__oailb=from-handshake; Path=/; Secure; HttpOnly"}},
	}
	pool := newOpenAIWSConnPool(codexWSWireProfileConfig())
	pool.setClientDialerForTest(dialer)
	store := &openAICodexCookieStore{}
	store.Store(account, wsURL, http.Header{"Set-Cookie": []string{"__cflb=seeded; Path=/; Secure"}})
	pool.cookies = store

	conn, err := pool.dialConn(context.Background(), openAIWSAcquireRequest{
		Account: account,
		WSURL:   wsURL,
		Headers: http.Header{"Authorization": []string{"Bearer offline"}},
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
	t.Cleanup(conn.close)

	sent := dialer.Headers()
	require.Len(t, sent, 1)
	require.Equal(t, "__cflb=seeded", sent[0].Get("Cookie"), "握手请求必须回放罐里的 cookie")
	require.Equal(t, "Bearer offline", sent[0].Get("Authorization"), "原有握手头不受影响")

	probe := http.Header{}
	store.Attach(account, wsURL, probe)
	require.Contains(t, probe.Get("Cookie"), "__oailb=from-handshake", "握手响应的 Set-Cookie 要收进罐")
	require.Contains(t, probe.Get("Cookie"), "__cflb=seeded")
}

// 透传适配器不经连接池、自己拨号，挂钩点独立：删掉 relay 里的 Attach / Store 任一处，本用例失败。
func TestOpenAIWSPassthrough_ReplaysAndStoresCodexCookies(t *testing.T) {
	account := wireProfileTestAccount(false)
	account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModePassthrough
	cfg := codexWSWireProfileConfig()
	svc := codexWSWireProfileService(cfg)
	svc.accountRepo = &stubQuotaAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	wsURL, err := svc.buildOpenAIResponsesWSURL(account)
	require.NoError(t, err)
	dialer := &codexWSStagedDialer{
		conns:     []openAIWSClientConn{&openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_1")}}},
		handshake: http.Header{"Set-Cookie": []string{"__oailb=from-handshake; Path=/; Secure; HttpOnly"}},
	}
	svc.openaiWSPassthroughDialer = dialer
	svc.codexCookies.Store(account, wsURL, http.Header{"Set-Cookie": []string{"__cflb=seeded; Path=/; Secure"}})

	runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{codexWSTestFrame})

	sent := dialer.Headers()
	require.Len(t, sent, 1)
	require.Equal(t, "__cflb=seeded", sent[0].Get("Cookie"), "透传握手必须回放罐里的 cookie")
	probe := http.Header{}
	svc.codexCookies.Attach(account, wsURL, probe)
	require.Contains(t, probe.Get("Cookie"), "__oailb=from-handshake", "透传握手响应的 Set-Cookie 要收进罐")
}
