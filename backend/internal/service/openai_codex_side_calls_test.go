//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// codexSideCallUpstream 只截获侧信道 GET：嵌入接口即可满足 HTTPUpstream，
// 其余方法在本用例里不会被调用。
type codexSideCallUpstream struct {
	HTTPUpstream
	ch chan *http.Request
	// proxies 记下每次出站用的代理：侧信道必须与推理走同一条出口，否则账号的 token
	// 会从网关自己的 IP 直连上游——这个功能存在的理由就是消除这类关联。
	proxies chan string
}

func (u *codexSideCallUpstream) Do(req *http.Request, proxyURL string, _ int64, _ int) (*http.Response, error) {
	u.proxies <- proxyURL
	u.ch <- req.Clone(context.Background())
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("{}")),
	}, nil
}

func codexSideCallTestService() (*OpenAIGatewayService, *codexSideCallUpstream) {
	up := &codexSideCallUpstream{ch: make(chan *http.Request, 16), proxies: make(chan string, 16)}
	return &OpenAIGatewayService{
		cfg:            &config.Config{},
		httpUpstream:   up,
		codexSideCalls: newCodexSideCallState(),
	}, up
}

// codexSideCallTestRequest 模拟一条已定稿的出站推理请求：身份头都已写好。
func codexSideCallTestRequest(threadID string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	req.Header.Set("authorization", "Bearer offline-token")
	req.Header.Set("chatgpt-account-id", "offline-account")
	req.Header.Set("user-agent", codexCLIUserAgent)
	req.Header.Set("originator", "codex-tui")
	req.Header.Set("version", codexCLIVersion)
	req.Header.Set("x-codex-turn-metadata", `{"session_id":"s"}`)
	if threadID != "" {
		req.Header.Set("thread-id", threadID)
	}
	return req
}

// collectCodexSideCalls 收满 want 条后再确认没有多余请求。
func collectCodexSideCalls(t *testing.T, up *codexSideCallUpstream, want int) map[string]*http.Request {
	t.Helper()
	got := make(map[string]*http.Request, want)
	deadline := time.After(3 * time.Second)
	for len(got) < want {
		select {
		case r := <-up.ch:
			got[r.URL.String()] = r
		case <-deadline:
			t.Fatalf("只收到 %d 条侧信道请求，期望 %d", len(got), want)
		}
	}
	select {
	case extra := <-up.ch:
		t.Fatalf("出现多余的侧信道请求：%s", extra.URL.String())
	case <-time.After(200 * time.Millisecond):
	}
	return got
}

func requireNoCodexSideCall(t *testing.T, up *codexSideCallUpstream) {
	t.Helper()
	select {
	case extra := <-up.ch:
		t.Fatalf("不该发出侧信道请求，却发了 %s", extra.URL.String())
	case <-time.After(200 * time.Millisecond):
	}
}

func TestCodexSideCallsFollowRealClientCadence(t *testing.T) {
	svc, up := codexSideCallTestService()
	proxyID := int64(77)
	account := wireProfileTestAccount(true)
	account.ProxyID = &proxyID
	account.Proxy = &Proxy{ID: proxyID, Protocol: "socks5", Host: "10.0.0.9", Port: 1080}
	c := newConvTestContext(t, wireProfileTestBody(t))

	svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest("thread-a"))
	first := collectCodexSideCalls(t, up, 1)

	settings := first[chatGPTSettingsUserURL]
	require.NotNil(t, settings, "新线程首个请求要查一次 settings/user")
	require.Equal(t, http.MethodGet, settings.Method)
	require.Equal(t, "no-cache, no-store", settings.Header.Get("cache-control"),
		"真客户端这条带 no-cache, no-store（backend-client/src/client.rs:486-498）")
	require.Equal(t, "socks5://10.0.0.9:1080", <-up.proxies,
		"侧信道必须走账号自己的代理，不能从网关 IP 直连")

	require.Equal(t, "Bearer offline-token", settings.Header.Get("authorization"))
	require.Equal(t, "offline-account", settings.Header.Get("chatgpt-account-id"))
	require.Equal(t, codexCLIUserAgent, settings.Header.Get("user-agent"))
	// backend-client 不走推理面的 OpenAI Provider，这两个头真客户端不发。
	require.Empty(t, settings.Header.Get("originator"))
	require.Empty(t, settings.Header.Get("version"))
	require.Empty(t, settings.Header.Get("x-codex-turn-metadata"))
	require.Nil(t, settings.Body)

	// 同一线程的后续请求不再查。
	svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest("thread-a"))
	requireNoCodexSideCall(t, up)

	// 换线程再查一次。
	svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest("thread-b"))
	second := collectCodexSideCalls(t, up, 1)
	require.NotNil(t, second[chatGPTSettingsUserURL])
}

// config/bundle 只在 business/edu/enterprise plan 上由真客户端发起
// （cloud-config/src/service.rs:50-58 + protocol/src/account.rs:67-80），
// 个人 Plus/Pro 账号补发它等于凭空多一个特征，网关一条都不发。
func TestCodexSideCallsNeverFetchConfigBundle(t *testing.T) {
	svc, up := codexSideCallTestService()
	account := wireProfileTestAccount(true)
	c := newConvTestContext(t, wireProfileTestBody(t))

	for _, thread := range []string{"t1", "t2", "t3"} {
		svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest(thread))
	}

	// 逐条读通道而不是 collectCodexSideCalls：那个 helper 按 URL 去重，收不满同名的三条。
	for i := 0; i < 3; i++ {
		select {
		case req := <-up.ch:
			require.Equal(t, chatGPTSettingsUserURL, req.URL.String(), "只允许 settings/user 一个端点")
		case <-time.After(3 * time.Second):
			t.Fatalf("第 %d 条侧信道请求没发出", i+1)
		}
	}
	requireNoCodexSideCall(t, up)
}

func TestCodexSideCallsSkippedWhenNotApplicable(t *testing.T) {
	newRequest := func(mutate func(*http.Request)) *http.Request {
		req := codexSideCallTestRequest("thread-x")
		if mutate != nil {
			mutate(req)
		}
		return req
	}

	for _, tc := range []struct {
		name        string
		wireProfile bool
		disable     bool
		request     *http.Request
	}{
		{name: "not_wire_profile", request: newRequest(nil)},
		{name: "state_not_initialized", wireProfile: true, disable: true, request: newRequest(nil)},
		{
			name: "compact", wireProfile: true,
			request: newRequest(func(r *http.Request) {
				r.URL.Path = "/backend-api/codex/responses/compact"
			}),
		},
		{
			name: "search", wireProfile: true,
			request: newRequest(func(r *http.Request) { r.URL.Path = "/backend-api/codex/alpha/search" }),
		},
		{
			name: "not_post", wireProfile: true,
			request: newRequest(func(r *http.Request) { r.Method = http.MethodGet }),
		},
		{
			name: "missing_authorization", wireProfile: true,
			request: newRequest(func(r *http.Request) { r.Header.Del("authorization") }),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, up := codexSideCallTestService()
			if tc.disable {
				svc.codexSideCalls = nil
			}
			account := wireProfileTestAccount(tc.wireProfile)
			c := newConvTestContext(t, wireProfileTestBody(t))

			svc.scheduleCodexSideCalls(c, account, tc.request)

			requireNoCodexSideCall(t, up)
		})
	}
}

// 没有 thread-id 就没有触发条件：settings/user 是线程级的。
func TestCodexSideCallsWithoutThreadIDSendNothing(t *testing.T) {
	svc, up := codexSideCallTestService()
	account := wireProfileTestAccount(true)
	c := newConvTestContext(t, wireProfileTestBody(t))

	svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest(""))

	requireNoCodexSideCall(t, up)
}

func TestCodexSideCallHeadersCopyOnlyBackendClientSet(t *testing.T) {
	src := http.Header{}
	src.Set("authorization", "Bearer t")
	src.Set("chatgpt-account-id", "a")
	src.Set("user-agent", "ua")
	src.Set("x-openai-fedramp", "true")
	src.Set("originator", "codex-tui")
	src.Set("session-id", "s")
	src.Set("content-type", "application/json")

	out := codexSideCallHeaders(src)

	require.Equal(t, http.Header{
		"Authorization":      []string{"Bearer t"},
		"Chatgpt-Account-Id": []string{"a"},
		"User-Agent":         []string{"ua"},
		"X-Openai-Fedramp":   []string{"true"},
	}, out)
}
