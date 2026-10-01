package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 这组用例一律打 httptest 假池子，**绝不打真实上游**。

const (
	gwpoolTestURL   = "https://chatgpt.com/backend-api/codex/responses"
	gwpoolTestWSURL = "wss://chatgpt.com/backend-api/codex/responses"
)

// gwpoolTestAccount 是开着账号级开关的 oauth 账号。同一份凭据 = 同一个凭证域身份。
func gwpoolTestAccount(id int64) *Account {
	return &Account{
		ID: id, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{"chatgpt_account_id": "acc-a", "chatgpt_user_id": "user-a"},
		Extra:       map[string]any{openAIGatewayPoolExtraKey: true},
	}
}

const gwpoolTestIdentity = "chatgpt:acc-a:user:user-a"

// gwpoolFakePool 是假池子：记 /cookie 的次数与查询串，以及收到的每一条 /touch。
// forceCookie / forceStatus 只在构造后、发第一个请求之前设置。
type gwpoolFakePool struct {
	client      *gwpool.Client
	hits        atomic.Int64
	queries     chan string
	touches     chan map[string]any
	cookie      string
	gateway     string
	validForS   int
	forceCookie string
	forceStatus int
}

func newGwpoolFakePool(t *testing.T, cookie string, validForS int) *gwpoolFakePool {
	t.Helper()
	fake := &gwpoolFakePool{
		queries: make(chan string, 16), touches: make(chan map[string]any, 16),
		cookie: cookie, validForS: validForS,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cookie":
			fake.hits.Add(1)
			fake.queries <- r.URL.RawQuery
			forced := r.URL.Query().Get("force") == "1"
			if forced && fake.forceStatus != 0 {
				w.WriteHeader(fake.forceStatus)
				return
			}
			cookie := fake.cookie
			if forced && fake.forceCookie != "" {
				cookie = fake.forceCookie
			}
			_, _ = io.WriteString(w, `{"gateway":"`+fake.gateway+`","cookie":"`+cookie+
				`","valid_for_s":`+strconv.Itoa(fake.validForS)+`,"verified_full":true,"ttl_is_advisory":true}`)
		case "/touch":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			fake.touches <- body
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	fake.client = gwpool.New(srv.URL, "ck")
	return fake
}

func (f *gwpoolFakePool) nextQuery(t *testing.T) string {
	t.Helper()
	select {
	case q := <-f.queries:
		return q
	case <-time.After(3 * time.Second):
		t.Fatal("池子没被请求")
		return ""
	}
}

func (f *gwpoolFakePool) nextTouch(t *testing.T) map[string]any {
	t.Helper()
	select {
	case body := <-f.touches:
		return body
	case <-time.After(3 * time.Second):
		t.Fatal("没收到触碰回报")
		return nil
	}
}

// gwpoolTestPairCookie 造一张能解出网关名的 pair。
func gwpoolTestPairCookie(t *testing.T, gateway string) string {
	t.Helper()
	return "__cflb=pool-lb; __oailb=" + routeCookieTestOailb(t, "chat.gateway."+gateway+".api.openai.com")
}

// ---------------------------------------------------------------------------
// 开关关闭 ⇒ 行为与接入前完全一致
// ---------------------------------------------------------------------------

// 账号级开关的默认值必须是关：Extra 为空、写成字符串、写成别的键，一律不接管。
func TestUsesGatewayPoolDefaultsOff(t *testing.T) {
	require.False(t, (*Account)(nil).UsesGatewayPool())
	require.False(t, codexCookieTestAccount(1, AccountTypeOAuth).UsesGatewayPool(), "Extra 没这个键就是关")

	strAcct := codexCookieTestAccount(2, AccountTypeOAuth)
	strAcct.Extra = map[string]any{openAIGatewayPoolExtraKey: "true"}
	require.False(t, strAcct.UsesGatewayPool(), `开关只认 bool，字符串 "true" 不算开`)

	apiKeyAcct := &Account{ID: 3, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Extra: map[string]any{openAIGatewayPoolExtraKey: true}}
	require.False(t, apiKeyAcct.UsesGatewayPool(), "范围与 cookie 回放一致：只有本地持 ChatGPT 凭据的账号")

	require.True(t, gwpoolTestAccount(4).UsesGatewayPool())
}

// 全局开关关着（pool 为 nil）时即使账号级开着，出站 cookie 仍是罐回放，一个池子请求都不发。
// 这是「开关关闭时行为完全一致」的主用例：断言的是与
// TestDoOpenAIUpstream_ReplaysCookiesFromPreviousResponse 逐条相同的出站 Cookie。
func TestDoOpenAIUpstreamGatewayPoolDisabledKeepsCookieReplay(t *testing.T) {
	poolHit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		poolHit = true
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	upstream := &cookieRecordingUpstream{setCookie: codexCookieUpstreamResponse()}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	// 全局开关关 ⇒ 构造器给 nil 客户端（这里显式走同一条构造路径）。
	svc.codexCookies.pool = newOpenAIGatewayPoolClient(&config.Config{})
	require.Nil(t, svc.codexCookies.pool)
	acct := gwpoolTestAccount(1)

	for range 2 {
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		resp, err := svc.doOpenAIUpstream(req, "", acct)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}
	require.False(t, poolHit, "全局开关关着不许碰池子")
	require.Len(t, upstream.sentCookies, 2)
	require.Empty(t, upstream.sentCookies[0], "首条请求罐还是空的")
	require.Contains(t, upstream.sentCookies[1], "__oailb=jwt-1")
	require.Contains(t, upstream.sentCookies[1], "__cflb=lb-1")
	require.Contains(t, upstream.sentCookies[1], "__cf_bm=bm-1")
	require.NotContains(t, upstream.sentCookies[1], "oai-did")
}

// 全局开关开着、账号级关着：同样一个池子请求都不发，行为仍是罐回放。
func TestAttachRouteAccountSwitchOffKeepsCookieReplay(t *testing.T) {
	fake := newGwpoolFakePool(t, "__cflb=pool", 9)
	store := &openAICodexCookieStore{pool: fake.client}
	acct := codexCookieTestAccount(7, AccountTypeOAuth) // Extra 为空 = 账号级关
	store.Store(acct, gwpoolTestURL, codexCookieUpstreamResponse())

	headers := http.Header{}
	require.NoError(t, store.AttachRoute(context.Background(), acct, gwpoolTestURL, headers))
	require.Contains(t, headers.Get("Cookie"), "__oailb=jwt-1")
	require.NotContains(t, headers.Get("Cookie"), "pool")
	require.Zero(t, fake.hits.Load(), "账号级开关关着不许碰池子")
}

// AttachRoute 在池子关着时与 Attach 的边界行为逐条相同（nil store / nil headers / 非 ChatGPT 主机 /
// 不适用的账号都不写头、不报错）。
func TestAttachRouteMatchesAttachGuards(t *testing.T) {
	store := &openAICodexCookieStore{}
	store.Store(codexCookieTestAccount(1, AccountTypeOAuth), gwpoolTestURL, codexCookieUpstreamResponse())
	ctx := context.Background()

	var nilStore *openAICodexCookieStore
	require.NoError(t, nilStore.AttachRoute(ctx, codexCookieTestAccount(1, AccountTypeOAuth), gwpoolTestURL, http.Header{}))
	require.NoError(t, store.AttachRoute(ctx, codexCookieTestAccount(1, AccountTypeOAuth), gwpoolTestURL, nil))

	apiHeaders := http.Header{}
	require.NoError(t, store.AttachRoute(ctx, codexCookieTestAccount(1, AccountTypeOAuth), "https://api.openai.com/v1/responses", apiHeaders))
	require.Empty(t, apiHeaders.Get("Cookie"))

	apiKeyHeaders := http.Header{}
	require.NoError(t, store.AttachRoute(ctx, codexCookieTestAccount(2, AccountTypeAPIKey), gwpoolTestURL, apiKeyHeaders))
	require.Empty(t, apiKeyHeaders.Get("Cookie"))
}

// 开关关着时路由对读数仍回读罐（接入前的行为）。
func TestRoutePairInUseFallsBackToJarWhenDisabled(t *testing.T) {
	svc := &OpenAIGatewayService{}
	acct := gwpoolTestAccount(1)
	svc.codexCookies.Store(acct, openAITurnStatePairCookieURL, codexCookieUpstreamResponse())
	require.Equal(t, "__cflb=lb-1; __oailb=jwt-1", svc.routePairInUse(context.Background(), acct, http.Header{}))
}

// ---------------------------------------------------------------------------
// 开关打开
// ---------------------------------------------------------------------------

// 池子接管：出站的 __cflb / __oailb 是池子那张，罐里存的（上游上次下发的）不再出站——这正是
// 「cookie 回放把账号钉死在坏网关上」的修法。而 __cf_bm 这种**本出口自己**的 Cloudflare 令牌
// 必须留着（N4：丢掉会让 CF 重新发挑战）。窗口内复用同一张，池子只被打一次。
func TestDoOpenAIUpstreamGatewayPoolReplacesOnlyRouteCookies(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)

	// 上游每发都下发另一个网关的 pair + 一个本出口的 __cf_bm。
	stuck := http.Header{"Set-Cookie": []string{
		"__cflb=stuck-lb; Path=/",
		"__oailb=" + routeCookieTestOailb(t, "chat.gateway.unified-126.api.openai.com") + "; Path=/",
		"__cf_bm=bm-mine; Domain=chatgpt.com; Path=/; Secure",
	}}
	upstream := &cookieRecordingUpstream{setCookie: stuck}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	svc.codexCookies.pool = fake.client
	acct := gwpoolTestAccount(1)

	for range 3 {
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		resp, err := svc.doOpenAIUpstream(req, "", acct)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}
	require.Len(t, upstream.sentCookies, 3)
	for i, sent := range upstream.sentCookies {
		require.Contains(t, sent, poolCookie, "第 %d 发应当带池子那张 pair", i+1)
		require.NotContains(t, sent, "stuck", "上游下发的路由 pair 不能再被带回去")
	}
	require.NotContains(t, upstream.sentCookies[0], "__cf_bm", "首发罐还是空的")
	require.Contains(t, upstream.sentCookies[1], "__cf_bm=bm-mine", "本出口自己的 CF 令牌要留着")
	require.Contains(t, upstream.sentCookies[2], "__cf_bm=bm-mine")
	require.EqualValues(t, 1, fake.hits.Load(), "满血窗口内复用同一张 pair，不该每发都向池子要")
}

// B2：回报的网关必须是**实际注入的那张**，不是事后从响应 / 罐里重新推导的。
// 这里上游持续下发 unified-126（罐里也是它），池子给的是 142：事后推导会报 126。
// 另外每一次注入都要报（窗口内复用的 150 秒里这个槽位一直在被碰）。
func TestAttachRouteReportsInjectedGatewayOnEveryInjection(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.gateway = "unified-142" // 池子自报网关名这条路；解 __oailb 的兜底由 force 用例覆盖
	svc := &OpenAIGatewayService{httpUpstream: &cookieRecordingUpstream{setCookie: http.Header{"Set-Cookie": []string{
		"__oailb=" + routeCookieTestOailb(t, "chat.gateway.unified-126.api.openai.com") + "; Path=/",
	}}}}
	svc.codexCookies.pool = fake.client
	acct := gwpoolTestAccount(1)

	for range 2 {
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		resp, err := svc.doOpenAIUpstream(req, "", acct)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}

	require.EqualValues(t, 1, fake.hits.Load())
	for i := range 2 {
		touch := fake.nextTouch(t)
		require.Equal(t, "unified-142", touch["gateway"], "第 %d 发的回报必须是注入的那个网关", i+1)
		require.Equal(t, gwpoolTestIdentity, touch["account_id"])
		require.Equal(t, openAIGatewayPoolVerdictUnknown, touch["verdict"])
		require.NotZero(t, touch["ts"])
	}
}

// B3：接管分支也必须过主机过滤。pair 发给第三方主机既是泄漏，又白烧一张池子 pair
// （根本没碰到那个网关，读数却照样上报），而供给只有个位数张。
func TestAttachRouteDoesNotLeakPairToNonChatGPTHost(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	store := &openAICodexCookieStore{pool: fake.client}
	acct := gwpoolTestAccount(1)

	for _, rawURL := range []string{
		"https://api.openai.com/v1/responses",
		"https://bps.openai.com/basispoints/api/responses",
		"https://chatgpt.com.evil.example/backend-api/codex/responses",
	} {
		headers := http.Header{}
		require.NoError(t, store.AttachRoute(context.Background(), acct, rawURL, headers))
		require.Empty(t, headers.Get("Cookie"), "%s 不该拿到 pair", rawURL)
	}
	require.Zero(t, fake.hits.Load(), "非 ChatGPT 主机不许消耗池子槽位")
	select {
	case touch := <-fake.touches:
		t.Fatalf("没碰到网关却上报了触碰: %+v", touch)
	case <-time.After(200 * time.Millisecond):
	}
}

// B1：WS 拨号与网关池互斥。连接复用 60 分钟而满血窗口只有约 150 秒，pair 只在握手挂一次；
// 而且预热（min_idle 默认 4）在无业务请求时就拨连接。闸门必须在取 pair **之前**，
// 所以预热一张 pair 都拿不到。
func TestAttachRouteRefusesWebSocketWhenGatewayPoolEnabled(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	store := &openAICodexCookieStore{pool: fake.client}
	acct := gwpoolTestAccount(1)

	headers := http.Header{"Authorization": []string{"Bearer offline"}}
	err := store.AttachRoute(context.Background(), acct, gwpoolTestWSURL, headers)
	require.ErrorIs(t, err, ErrGatewayPoolWSIncompatible)
	require.Empty(t, headers.Get("Cookie"))
	require.Zero(t, fake.hits.Load(), "WS 路径（含预热）绝不许消耗池子槽位")
}

// S7 + B1：连接池 dialConn 这条路也拒，而且在**拨号之前**拒。
func TestOpenAIWSConnPoolDialConnRefusesGatewayPoolAccount(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{&openAIWSCaptureConn{}}}
	pool := newOpenAIWSConnPool(codexWSWireProfileConfig())
	pool.setClientDialerForTest(dialer)
	pool.cookies = &openAICodexCookieStore{pool: fake.client}

	conn, err := pool.dialConn(context.Background(), openAIWSAcquireRequest{
		Account: gwpoolTestAccount(31),
		WSURL:   gwpoolTestWSURL,
		Headers: http.Header{"Authorization": []string{"Bearer offline"}},
	})
	require.ErrorIs(t, err, ErrGatewayPoolWSIncompatible)
	require.Nil(t, conn)
	require.Empty(t, dialer.Headers(), "拒绝必须发生在拨号之前")
	require.Zero(t, fake.hits.Load())
}

// S7 + B1：透传适配器自己拨号，挂钩点独立，同样要拒。
func TestOpenAIWSPassthroughRefusesGatewayPoolAccount(t *testing.T) {
	account := wireProfileTestAccount(false)
	account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModePassthrough
	account.Extra[openAIGatewayPoolExtraKey] = true
	cfg := codexWSWireProfileConfig()
	svc := codexWSWireProfileService(cfg)
	svc.accountRepo = &stubQuotaAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc.codexCookies.pool = fake.client
	dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{&openAIWSCaptureConn{}}}
	svc.openaiWSPassthroughDialer = dialer

	relayErr := runCodexWSIngressRelayError(t, svc, account, codexWSIngressInbound(), codexWSTestFrame)
	require.ErrorIs(t, relayErr, ErrGatewayPoolWSIncompatible)
	require.Empty(t, dialer.Headers(), "透传握手必须在拨号之前被拒")
	require.Zero(t, fake.hits.Load())
}

// runCodexWSIngressRelayError 是 runCodexWSIngress 的失败侧变体：只发一帧，回传 relay 自己的错误。
// 原版断言每轮都读到终态事件，拒绝路径上根本没有终态。
func runCodexWSIngressRelayError(t *testing.T, svc *OpenAIGatewayService, account *Account, inbound http.Header, frame string) error {
	t.Helper()
	relayErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			relayErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		req := r.Clone(r.Context())
		req.Header = req.Header.Clone()
		for name, values := range inbound {
			req.Header[name] = values
		}
		ginCtx.Request = req
		readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		_, first, readErr := conn.Read(readCtx)
		cancel()
		if readErr != nil {
			relayErrCh <- readErr
			return
		}
		relayErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "offline-token", first, nil)
	}))
	defer server.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	client, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = client.CloseNow() }()
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	require.NoError(t, client.Write(writeCtx, coderws.MessageText, []byte(frame)))
	cancelWrite()
	select {
	case relayErr := <-relayErrCh:
		return relayErr
	case <-time.After(5 * time.Second):
		t.Fatal("websocket ingress did not finish")
		return nil
	}
}

// B1 的管理端闸：两个开关不许同时落库。按**合并后**的最终 extra 判，部分更新不能从另一半绕过。
func TestValidateOpenAIGatewayPoolAccountExtraRejectsWSUpstream(t *testing.T) {
	acct := gwpoolTestAccount(1)
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{openAIGatewayPoolExtraKey: true}))

	for _, wsKey := range []string{
		"openai_oauth_responses_websockets_v2_enabled",
		"responses_websockets_v2_enabled",
		"openai_ws_enabled",
	} {
		err := validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{
			openAIGatewayPoolExtraKey: true, wsKey: true,
		})
		require.Error(t, err, wsKey)
		require.Contains(t, err.Error(), openAIGatewayPoolExtraKey)
	}

	// 只开一个都放行；force_http 的账号永远不拨 WS，不算冲突。
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{"openai_ws_enabled": true}))
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{
		openAIGatewayPoolExtraKey: true, "openai_ws_enabled": true, "openai_ws_force_http": true,
	}))

	require.True(t, touchesOpenAIGatewayPoolExclusivity(map[string]any{"openai_ws_enabled": false}))
	require.True(t, touchesOpenAIGatewayPoolExclusivity(map[string]any{openAIGatewayPoolExtraKey: true}))
	require.False(t, touchesOpenAIGatewayPoolExclusivity(map[string]any{"quota_used": 1}))
}

// 部分更新（重授权 / 单键编辑）也要拦：只写 gwpool 一边，WS 那边已经在库里开着。
func TestUpdateAccountExtraRejectsGatewayPoolWithWSUpstream(t *testing.T) {
	accountID := int64(201)
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{
		accountID: {ID: accountID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Extra: map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}},
	}}
	svc := &adminServiceImpl{accountRepo: repo}

	err := svc.UpdateAccountExtra(context.Background(), accountID, map[string]any{openAIGatewayPoolExtraKey: true})
	require.Error(t, err)
	require.NotContains(t, repo.accounts[accountID].Extra, openAIGatewayPoolExtraKey, "拒绝就不该落库")

	// 关掉 WS 再开就放行。
	repo.accounts[accountID].Extra = map[string]any{}
	require.NoError(t, svc.UpdateAccountExtra(context.Background(), accountID, map[string]any{openAIGatewayPoolExtraKey: true}))
}

// B4：池子这一侧的失败不能被当成「代理持久故障」把真账号停调度 10 分钟。
// 池子的报错字面和真实代理死掉一模一样，所以标记必须先于字符串判定。
func TestClassifyUpstreamTransportErrorIgnoresGatewayPoolFailures(t *testing.T) {
	for name, err := range map[string]error{
		"池子没起":   fmt.Errorf("%w: request failed: Get \"http://127.0.0.1:8099/cookie\": dial tcp 127.0.0.1:8099: connect: connection refused", gwpool.ErrPool),
		"池子域名没了": fmt.Errorf("%w: request failed: dial tcp: lookup gwpool.internal: no such host", gwpool.ErrPool),
		"没有满血槽位": gwpool.ErrNoSlot,
		"WS 互斥":  ErrGatewayPoolWSIncompatible,
	} {
		require.False(t, classifyUpstreamTransportError(err).Persistent, name)
	}
	// 真实代理故障仍然按持久故障处理（没被这条豁免带偏）。
	require.True(t, classifyUpstreamTransportError(errors.New("proxyconnect tcp: dial tcp 10.0.0.1:1080: connect: connection refused")).Persistent)
}

// S2：影子行自己不持凭据，池子侧身份必须按母账号算，否则同一个 (账号 × 网关) 单位会被当成
// 两个槽位排班、各拿一张 pair。
func TestGatewayPoolIdentityFollowsParentForShadowRow(t *testing.T) {
	parent := gwpoolTestAccount(1)
	shadowID := int64(77)
	shadow := &Account{ID: shadowID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		ParentAccountID: &parent.ID, Credentials: map[string]any{},
		Extra: map[string]any{openAIGatewayPoolExtraKey: true}}
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{parent.ID: parent, shadowID: shadow}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.codexCookies.identity = svc.codexCredentialIdentity

	// 没过母账号解析时 namespace 为空、退回 id:77——这正是要避免的分裂。
	require.Equal(t, "id:77", openAIGatewayPoolAccountKey(shadow))

	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), shadow)
	require.NoError(t, err)
	require.Equal(t, gwpoolTestIdentity, identity)

	parentIdentity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), parent)
	require.NoError(t, err)
	require.Equal(t, identity, parentIdentity, "影子行与母行必须是同一个槽位")

	// 解析不出母账号时 fail closed：按影子行自己上报会把一个单位记成两个。
	orphanParent := int64(999)
	orphan := &Account{ID: 78, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		ParentAccountID: &orphanParent, Extra: map[string]any{openAIGatewayPoolExtraKey: true}}
	_, err = svc.codexCookies.gatewayPoolIdentity(context.Background(), orphan)
	require.Error(t, err)
}

// S3：同一个 ChatGPT 账号的多个本地行（克隆行）共用一张 pair 和一个池子槽位。
func TestGatewayPoolSharesOnePairAcrossRowsOfSameAccount(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	store := &openAICodexCookieStore{pool: fake.client}

	for _, id := range []int64{1, 35} { // pro1 与它的克隆行
		headers := http.Header{}
		require.NoError(t, store.AttachRoute(context.Background(), gwpoolTestAccount(id), gwpoolTestURL, headers))
		require.Equal(t, poolCookie, headers.Get("Cookie"))
	}
	require.EqualValues(t, 1, fake.hits.Load(), "同一个凭证域身份只该向池子要一张")
	for range 2 {
		require.Equal(t, gwpoolTestIdentity, fake.nextTouch(t)["account_id"])
	}
}

// S4：共享的那次取用自己的 ctx——第一名的客户端断开不能把同账号其他请求连坐成 502。
func TestGatewayPoolFetchSurvivesCallerCancel(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	// arrived / release 是确定性屏障：取 pair 已经出站（arrived）才取消，取消之后才让池子回话
	// （release）。不用 sleep——那只是赌调度，机器一卡就赌输。
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		arrived <- struct{}{}
		<-release
		_, _ = io.WriteString(w, `{"gateway":"unified-142","cookie":"`+poolCookie+`","valid_for_s":150}`)
	}))
	defer srv.Close()

	store := &openAICodexCookieStore{pool: gwpool.New(srv.URL, "ck")}
	acct := gwpoolTestAccount(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		headers := http.Header{}
		err := store.AttachRoute(ctx, acct, gwpoolTestURL, headers)
		if err == nil && headers.Get("Cookie") != poolCookie {
			err = io.ErrUnexpectedEOF
		}
		done <- err
	}()
	<-arrived // 请求已到池子 ⇒ 取 pair 确实在飞
	cancel()  // 客户端断开
	close(release)
	require.NoError(t, <-done, "取 pair 不该跟随业务 ctx 的取消")
	require.EqualValues(t, 1, hits.Load())
}

// S5：池子是外部 API = 信任边界。别的 cookie 名不许发给 chatgpt.com，否则落库读数
// （routePairOf 的产物）与实际出站不一致，排查时会被骗。
func TestGatewayPoolKeepsOnlyRouteCookieNames(t *testing.T) {
	t.Run("夹带别的名字只留路由对", func(t *testing.T) {
		oailb := routeCookieTestOailb(t, "chat.gateway.unified-142.api.openai.com")
		fake := newGwpoolFakePool(t, "sessionid=steal; __cflb=lb; __oailb="+oailb, 150)
		store := &openAICodexCookieStore{pool: fake.client}
		headers := http.Header{}
		require.NoError(t, store.AttachRoute(context.Background(), gwpoolTestAccount(1), gwpoolTestURL, headers))
		require.Equal(t, "__cflb=lb; __oailb="+oailb, headers.Get("Cookie"))
		require.NotContains(t, headers.Get("Cookie"), "sessionid")
	})
	t.Run("一个路由 cookie 都没有就是畸形响应", func(t *testing.T) {
		fake := newGwpoolFakePool(t, "sessionid=steal", 150)
		store := &openAICodexCookieStore{pool: fake.client}
		headers := http.Header{}
		err := store.AttachRoute(context.Background(), gwpoolTestAccount(1), gwpoolTestURL, headers)
		require.ErrorIs(t, err, gwpool.ErrPool)
		require.Empty(t, headers.Get("Cookie"))
	})
}

// 建议窗口（ttl_is_advisory）到点 ⇒ 换一张**不同的**网关：第一次不带 force，到期后带 force=1。
// 换不换由这边判，池子不操心；不带 force 的话池子可能把烧过的那张原样发回来。
func TestGatewayPoolForcesRotationAfterWindow(t *testing.T) {
	first := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, first, 150)
	fake.forceCookie = "__cflb=pool-lb2; __oailb=" + routeCookieTestOailb(t, "chat.gateway.unified-84.api.openai.com")
	store := &openAICodexCookieStore{pool: fake.client}
	acct := gwpoolTestAccount(1)

	headers := http.Header{}
	require.NoError(t, store.AttachRoute(context.Background(), acct, gwpoolTestURL, headers))
	require.Equal(t, first, headers.Get("Cookie"))
	require.Empty(t, fake.nextQuery(t), "正常路径不带 force")
	require.Equal(t, "unified-142", fake.nextTouch(t)["gateway"])

	// 把窗口拨到过去，模拟 valid_for_s 到点。
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: first, gateway: "unified-142", until: time.Now().Add(-time.Second)})

	rotated := http.Header{}
	require.NoError(t, store.AttachRoute(context.Background(), acct, gwpoolTestURL, rotated))
	require.Equal(t, fake.forceCookie, rotated.Get("Cookie"), "到期后要换成池子新给的那张")
	require.Equal(t, "force=1", fake.nextQuery(t))
	// force 换来的那张同样是一次真实接触，照样回报；网关名这里由 __oailb 解出（池子没自报）。
	require.Equal(t, "unified-84", fake.nextTouch(t)["gateway"])
	require.EqualValues(t, 2, fake.hits.Load())
}

// force=1 收到 503 = 池子真的换不出来了 ⇒ fail-closed，**不回落罐回放、也不复用过期那张**。
// 而且下一发仍然 force：过期那张留在缓存里正是「别再给我这一个」的依据。
func TestGatewayPoolForceNoSlotFailsClosed(t *testing.T) {
	stale := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, stale, 150)
	fake.forceStatus = http.StatusServiceUnavailable
	store := &openAICodexCookieStore{pool: fake.client}
	acct := gwpoolTestAccount(1)
	// 罐里有一张能回放的：也不许用。
	store.Store(acct, gwpoolTestURL, codexCookieUpstreamResponse())
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: stale, gateway: "unified-142", until: time.Now().Add(-time.Second)})

	for range 2 {
		headers := http.Header{}
		err := store.AttachRoute(context.Background(), acct, gwpoolTestURL, headers)
		require.ErrorIs(t, err, gwpool.ErrNoSlot)
		require.Empty(t, headers.Get("Cookie"), "失败时一个 cookie 都不许写出去")
		require.Equal(t, "force=1", fake.nextQuery(t), "过期那张还在缓存里，下一发仍要换")
	}
	require.EqualValues(t, 2, fake.hits.Load(), "一次失败一次请求，不许自动重试")
}

// 池子 503（没有满血槽位）：报错走既有失败路径，**不回落罐回放**，上游一个请求都不发。
func TestDoOpenAIUpstreamGatewayPoolNoSlotFailsClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	upstream := &cookieRecordingUpstream{setCookie: codexCookieUpstreamResponse()}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	svc.codexCookies.pool = gwpool.New(srv.URL, "ck")
	acct := gwpoolTestAccount(1)
	// 罐里有一张能回放的 pair：也不许用。
	svc.codexCookies.Store(acct, gwpoolTestURL, codexCookieUpstreamResponse())

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	resp, err := svc.doOpenAIUpstream(req, "", acct)
	require.ErrorIs(t, err, gwpool.ErrNoSlot)
	require.Nil(t, resp)
	require.Empty(t, upstream.sentCookies, "池子没槽位就不该发上游请求")
	require.Empty(t, req.Header.Get("Cookie"), "失败时不许留下降级的回放 cookie")
}

// 池子接管时路由对读数取池子那张（罐已不参与出站，回读罐会记成别的网关）；
// 上游新下发时仍以新下发的为准——那是改派后的真实落点。
func TestRoutePairInUsePrefersGatewayPoolPair(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	svc := &OpenAIGatewayService{}
	svc.codexCookies.pool = fake.client
	acct := gwpoolTestAccount(1)
	// 罐里存着另一个网关的 pair，它不该再被当成这一发的读数。
	svc.codexCookies.Store(acct, openAITurnStatePairCookieURL, codexCookieUpstreamResponse())

	headers := http.Header{}
	require.NoError(t, svc.codexCookies.AttachRoute(context.Background(), acct, gwpoolTestURL, headers))

	pair := svc.routePairInUse(context.Background(), acct, http.Header{})
	require.Equal(t, poolCookie, pair)
	require.Equal(t, "unified-142", openAICodexRouteGateway(pair))

	fresh := http.Header{"Set-Cookie": []string{
		"__cflb=new-lb; Path=/",
		"__oailb=" + routeCookieTestOailb(t, "chat.gateway.unified-84.api.openai.com") + "; Path=/",
	}}
	require.Equal(t, "unified-84", openAICodexRouteGateway(svc.routePairInUse(context.Background(), acct, fresh)))
}

// 同身份并发只向池子要一张 pair：池子一个网关一周期只出一张，并发各要一张就是白烧供给。
func TestGatewayPoolPairDedupesConcurrentFetches(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	var hits atomic.Int64
	// arrived / release 是确定性屏障：第一发到了池子（arrived）才放后面三个进来，确认它们之后
	// 才让池子回话（release）。所以第一发**必然**还在飞，后三发只能是 singleflight 的跟随者。
	// 不用 sleep：那只是赌调度，而这里要钉的恰恰是「不许各要一张」。
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		arrived <- struct{}{}
		<-release
		_, _ = io.WriteString(w, `{"gateway":"unified-142","cookie":"`+poolCookie+`","valid_for_s":150}`)
	}))
	defer srv.Close()

	store := &openAICodexCookieStore{pool: gwpool.New(srv.URL, "ck")}
	acct := gwpoolTestAccount(1)
	done := make(chan error, 4)
	attach := func() {
		headers := http.Header{}
		err := store.AttachRoute(context.Background(), acct, gwpoolTestURL, headers)
		if err == nil && headers.Get("Cookie") != poolCookie {
			err = io.ErrUnexpectedEOF
		}
		done <- err
	}
	go attach()
	<-arrived // 第一发已出站并卡在池子里
	var joined sync.WaitGroup
	for range 3 {
		joined.Add(1)
		go func() {
			joined.Done() // 已经起跑；下面的 Wait 保证 release 不会抢在前面
			attach()
		}()
	}
	joined.Wait()
	close(release)
	for range 4 {
		require.NoError(t, <-done)
	}
	// 跟随者要么并进第一发那次 flight，要么醒来时读到缓存里那张：两条路都不许再打池子一次。
	require.EqualValues(t, 1, hits.Load())
}

// 客户端构造：全局开关关着或地址没配时返回 nil = 不接管（配置合法性由 config.Validate 启动期拦）。
func TestNewOpenAIGatewayPoolClient(t *testing.T) {
	require.Nil(t, newOpenAIGatewayPoolClient(nil))
	require.Nil(t, newOpenAIGatewayPoolClient(&config.Config{
		Gwpool: config.GwpoolConfig{Enabled: true, ConsumerKey: "ck"},
	}), "缺地址不接管")
	require.NotNil(t, newOpenAIGatewayPoolClient(&config.Config{
		Gwpool: config.GwpoolConfig{Enabled: true, BaseURL: "http://127.0.0.1:8099", ConsumerKey: "ck"},
	}))
}
