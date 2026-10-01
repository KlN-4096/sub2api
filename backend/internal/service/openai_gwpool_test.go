package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// gwpoolTestAccount 是开着账号级开关、但**没配地址**的 oauth 账号。同一份凭据 = 同一个凭证域身份。
// 要能真的取到 pair 得再过 gwpoolFakePool.configure（配置全在账号级，没有实例级那一层）。
func gwpoolTestAccount(id int64) *Account {
	return &Account{
		ID: id, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{"chatgpt_account_id": "acc-a", "chatgpt_user_id": "user-a"},
		Extra:       map[string]any{openAIGatewayPoolExtraKey: true},
	}
}

const (
	gwpoolTestIdentity    = "chatgpt:acc-a:user:user-a"
	gwpoolTestConsumerKey = "ck"
	// gwpoolTestAccountQuery 是每次问池子都会带的那一段：一把 consumer key 能替多个上游账号取票，
	// 槽位必须记在**这一发真正要用的那个上游账号**上（身份串里的 chatgpt_account_id）。
	gwpoolTestAccountQuery = "account=acc-a"
)

// gwpoolFakeGateway 是假池子 /gateways 里的一项，字段名与池子契约一致。
type gwpoolFakeGateway struct {
	Name       string `json:"name"`
	PairReady  bool   `json:"pair_ready"`
	UsedByYou  bool   `json:"used_by_you"`
	LastUsedAt string `json:"last_used_at,omitempty"`
}

// gwpoolFakePool 是假池子：记 /cookie 与 /gateways 的次数、/cookie 的查询串，以及**这两个之外**
// 任何路径被打的次数（/touch 已删，strays 必须恒为 0）。
// forceCookie / forceStatus / listGateways / listStatus 只在构造后、发第一个请求之前设置。
type gwpoolFakePool struct {
	baseURL      string
	hits         atomic.Int64
	strays       atomic.Int64
	strayPaths   chan string
	queries      chan string
	cookie       string
	gateway      string
	validForS    int
	forceCookie  string
	forceStatus  int
	listHits     atomic.Int64
	listQueries  chan string
	listGateways []gwpoolFakeGateway // 空 = 空列表，消费端挑不出来
	listStatus   int                 // 非 0 时 /gateways 直接回这个状态码
	// refuseStatus / refuseCode / refuseRetryAfter 让 /cookie 回结构化拒绝（2026-10-02 契约）。
	refuseStatus     int
	refuseCode       string
	refuseRetryAfter int
	// releaseHits / releaseBodies / releaseStatus 记 POST /release：还票是尽力而为，但
	// 「该还的时候有没有还」是要钉死的行为。
	releaseHits   atomic.Int64
	releaseBodies chan string
	releaseStatus int // 非 0 时 /release 回这个状态码（默认 204）
	// omitVersion 模拟不报 cookie_version 的老池子；onCookie 在 /cookie 被打到时回调
	// （用来复现「取到票之后、发送之前客户端就走了」）。
	omitVersion bool
	onCookie    func()
}

func newGwpoolFakePool(t *testing.T, cookie string, validForS int) *gwpoolFakePool {
	t.Helper()
	fake := &gwpoolFakePool{
		queries: make(chan string, 16), strayPaths: make(chan string, 16),
		listQueries: make(chan string, 16), releaseBodies: make(chan string, 16),
		cookie: cookie, validForS: validForS,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cookie":
			hits := fake.hits.Add(1)
			fake.queries <- r.URL.RawQuery
			forced := r.URL.Query().Get("force") == "1"
			if fake.refuseStatus != 0 {
				if fake.refuseRetryAfter > 0 {
					w.Header().Set("Retry-After", strconv.Itoa(fake.refuseRetryAfter))
				}
				w.WriteHeader(fake.refuseStatus)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
					"code": fake.refuseCode, "message": "refused",
					"retry_after_seconds": fake.refuseRetryAfter,
				}})
				return
			}
			if forced && fake.forceStatus != 0 {
				w.WriteHeader(fake.forceStatus)
				return
			}
			cookie := fake.cookie
			if forced && fake.forceCookie != "" {
				cookie = fake.forceCookie
			}
			gateway := fake.gateway
			if steered := r.URL.Query().Get("gateway"); steered != "" {
				// 点名取票时原样回报点中的那个（真池子也是这样）⇒ 账本记的就是这个名字。
				gateway = steered
			}
			// 每张票一个不同的 cookie_version（真池子也是），exclude_versions 才测得出来。
			version := `,"cookie_version":"tkt-` + strconv.FormatInt(hits, 10) + `"`
			if fake.omitVersion {
				version = ""
			}
			_, _ = io.WriteString(w, `{"gateway":"`+gateway+`","cookie":"`+cookie+
				`","valid_for_s":`+strconv.Itoa(fake.validForS)+`,"verified_full":true,"ttl_is_advisory":true`+
				version+`}`)
			if fake.onCookie != nil {
				fake.onCookie()
			}
		case "/gateways":
			fake.listHits.Add(1)
			select {
			case fake.listQueries <- r.URL.RawQuery:
			default:
			}
			if fake.listStatus != 0 {
				w.WriteHeader(fake.listStatus)
				return
			}
			// 回显 account（真池子的自检字段）：报错给别人时消费端据此弃用这张列表。
			_ = json.NewEncoder(w).Encode(map[string]any{
				"account": r.URL.Query().Get("account"), "live": len(fake.listGateways),
				"target": 6, "gateways": fake.listGateways,
			})
		case "/release":
			fake.releaseHits.Add(1)
			if fake.releaseStatus != 0 {
				w.WriteHeader(fake.releaseStatus)
				return
			}
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<10))
			select {
			case fake.releaseBodies <- strings.TrimSpace(string(body)):
			default:
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			// 池子只有 /cookie、/gateways 和 /release。任何别的路径（历史上的 /touch 就在这里）
			// 都算越界 —— 尤其是任何形式的「回报满血/降智」，那是刻意没有的东西。
			fake.strays.Add(1)
			select {
			case fake.strayPaths <- r.URL.Path:
			default:
			}
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	fake.baseURL = srv.URL
	return fake
}

// configure 把这个假池子的地址与 consumer key 写进账号 extra。
func (f *gwpoolFakePool) configure(accounts ...*Account) {
	for _, acct := range accounts {
		acct.Extra[openAIGatewayPoolBaseURLExtraKey] = f.baseURL
		acct.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = gwpoolTestConsumerKey
	}
}

// account 造一个配好这个假池子的账号。
func (f *gwpoolFakePool) account(id int64) *Account {
	acct := gwpoolTestAccount(id)
	f.configure(acct)
	return acct
}

// nextQuery 取下一次 /cookie 的查询串，**剔掉 min_remaining 与 wait**：这两项每发都带，
// 而 wait 的值按剩余预算算（见 gwpool.CookieRequest.query），钉它等于钉时序。
// 这两项自己由 TestGatewayPoolAsksForUsableLifetimeAndWait 钉。
// 其余项顺序不变：客户端用 url.Values.Encode()，本来就按键名排序。
func (f *gwpoolFakePool) nextQuery(t *testing.T) string {
	t.Helper()
	raw := f.nextRawQuery(t)
	query, err := url.ParseQuery(raw)
	require.NoErrorf(t, err, "查询串解不开: %s", raw)
	query.Del("min_remaining")
	query.Del("wait")
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value := query.Get(key)
		if key == "exclude" || key == "exclude_versions" {
			// 列表项排序后再比：**项内顺序**（最近烧的在前）由
			// TestGatewayPoolExcludesRecentlyBurntGateways 用可控时间戳单独钉——这里两笔记账
			// 相差微秒，Windows 的时钟粒度会把它们记成同一刻。
			items := strings.Split(value, ",")
			slices.Sort(items)
			value = strings.Join(items, ",")
		}
		// 不用 Encode()：它会把逗号转义成 %2C，断言读起来全是噪声。这些值都是简单标识符。
		parts = append(parts, key+"="+value)
	}
	return strings.Join(parts, "&")
}

func (f *gwpoolFakePool) nextRawQuery(t *testing.T) string {
	t.Helper()
	select {
	case q := <-f.queries:
		return q
	case <-time.After(3 * time.Second):
		t.Fatal("池子没被请求")
		return ""
	}
}

// nextRelease 取下一次 POST /release 的请求体。
func (f *gwpoolFakePool) nextRelease(t *testing.T) string {
	t.Helper()
	select {
	case body := <-f.releaseBodies:
		return body
	case <-time.After(3 * time.Second):
		t.Fatal("没有还票")
		return ""
	}
}

// attachRoute 调 AttachRoute 并丢掉还票闭包：这组用例关心的是出站 cookie 与错误。
// 还票本身在 TestGatewayPoolReleases* 里单独测。
func attachRoute(
	ctx context.Context,
	store *openAICodexCookieStore,
	account *Account,
	rawURL string,
	headers http.Header,
) error {
	_, err := store.AttachRoute(ctx, account, rawURL, headers)
	return err
}

// attachRouteApplied 调 AttachRoute 并把 per-request 标记取回来 —— 这里代演转发入口
// （Forward*）挂 sink、返回前 publish 到 OpenAIForwardResult 的那两步。
func attachRouteApplied(
	t *testing.T,
	store *openAICodexCookieStore,
	account *Account,
	rawURL string,
	headers http.Header,
) OpenAIGatewayPoolApplied {
	t.Helper()
	ctx, sink := withOpenAIGatewayPoolSink(context.Background())
	_, err := store.AttachRoute(ctx, account, rawURL, headers)
	require.NoError(t, err)
	result := &OpenAIForwardResult{}
	sink.publish(result)
	return result.GatewayPoolApplied
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

// 账号级开关关着时即使地址与凭据都配好了，出站 cookie 仍是罐回放，一个池子请求都不发。
// 这是「开关关闭时行为完全一致」的主用例：断言的是与
// TestDoOpenAIUpstream_ReplaysCookiesFromPreviousResponse 逐条相同的出站 Cookie。
func TestDoOpenAIUpstreamGatewayPoolDisabledKeepsCookieReplay(t *testing.T) {
	fake := newGwpoolFakePool(t, "__cflb=pool-lb; __oailb=pool-jwt", 150)

	upstream := &cookieRecordingUpstream{setCookie: codexCookieUpstreamResponse()}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	// 地址与 key 都在 extra 里，只是开关关着。
	acct := codexCookieTestAccount(1, AccountTypeOAuth)
	if acct.Extra == nil {
		acct.Extra = map[string]any{}
	}
	fake.configure(acct)
	require.False(t, acct.UsesGatewayPool())

	for range 2 {
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		resp, err := svc.doOpenAIUpstream(req, "", acct)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}
	require.Zero(t, fake.hits.Load(), "账号级开关关着不许碰池子")
	require.Len(t, upstream.sentCookies, 2)
	require.Empty(t, upstream.sentCookies[0], "首条请求罐还是空的")
	require.Contains(t, upstream.sentCookies[1], "__oailb=jwt-1")
	require.Contains(t, upstream.sentCookies[1], "__cflb=lb-1")
	require.Contains(t, upstream.sentCookies[1], "__cf_bm=bm-1")
	require.NotContains(t, upstream.sentCookies[1], "oai-did")
}

// 账号级关着：一个池子请求都不发，行为仍是罐回放。
func TestAttachRouteAccountSwitchOffKeepsCookieReplay(t *testing.T) {
	fake := newGwpoolFakePool(t, "__cflb=pool", 9)
	store := &openAICodexCookieStore{}
	acct := codexCookieTestAccount(7, AccountTypeOAuth)
	acct.Extra = map[string]any{} // 开关缺省 = 账号级关
	fake.configure(acct)          // 地址配着也不许碰
	store.Store(acct, gwpoolTestURL, codexCookieUpstreamResponse())

	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, headers))
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
	require.NoError(t, attachRoute(ctx, nilStore, codexCookieTestAccount(1, AccountTypeOAuth), gwpoolTestURL, http.Header{}))
	require.NoError(t, attachRoute(ctx, store, codexCookieTestAccount(1, AccountTypeOAuth), gwpoolTestURL, nil))

	apiHeaders := http.Header{}
	require.NoError(t, attachRoute(ctx, store, codexCookieTestAccount(1, AccountTypeOAuth), "https://api.openai.com/v1/responses", apiHeaders))
	require.Empty(t, apiHeaders.Get("Cookie"))

	apiKeyHeaders := http.Header{}
	require.NoError(t, attachRoute(ctx, store, codexCookieTestAccount(2, AccountTypeAPIKey), gwpoolTestURL, apiKeyHeaders))
	require.Empty(t, apiKeyHeaders.Get("Cookie"))
}

// 开关关着时路由对读数仍回读罐（接入前的行为）。
func TestRoutePairInUseFallsBackToJarWhenDisabled(t *testing.T) {
	svc := &OpenAIGatewayService{}
	acct := gwpoolTestAccount(1)
	svc.codexCookies.Store(acct, openAITurnStatePairCookieURL, codexCookieUpstreamResponse())
	pair, fromPool, poolGateway, _ := svc.routePairInUse(acct, http.Header{}, OpenAIGatewayPoolApplied{})
	require.Equal(t, "__cflb=lb-1; __oailb=jwt-1", pair)
	require.False(t, fromPool, "没接管就不是覆写")
	require.Empty(t, poolGateway)
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
	acct := fake.account(1)

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

// 停掉 /touch 回报：池子在**交付那一刻**就记了 last_touch，verdict 也是它自己用 state-echo 验的；
// 转发路径上一个判据都不剩，回报只能填 "unknown"，等于把刚验出来的 "full" 覆盖掉，让验过满血的
// 槽位提前被拿去烧。「这张票坏了」由取 pair 时的 force=1 承载。
//
// 顺带钉住另一个隐患：回报曾是每请求一个裸 goroutine，会越过用例边界往进程全局 slog 写。
func TestGatewayPoolNeverReportsTouch(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.gateway = "unified-142"
	svc := &OpenAIGatewayService{httpUpstream: &cookieRecordingUpstream{setCookie: http.Header{"Set-Cookie": []string{
		"__oailb=" + routeCookieTestOailb(t, "chat.gateway.unified-126.api.openai.com") + "; Path=/",
	}}}}
	acct := fake.account(1)

	for range 2 {
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		resp, err := svc.doOpenAIUpstream(req, "", acct)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}

	require.EqualValues(t, 1, fake.hits.Load())
	// 回报曾是每请求一个裸 goroutine：不等它就等于什么都没测。给一个远超回环往返的窗口。
	select {
	case path := <-fake.strayPaths:
		t.Fatalf("向池子打了 %s：/cookie 之外不该有任何请求", path)
	case <-time.After(300 * time.Millisecond):
	}
	require.Zero(t, fake.strays.Load(), "sub2api 不再向池子回报触碰")
}

// B3：接管分支也必须过主机过滤。pair 发给第三方主机既是泄漏，又白烧一张池子 pair
// （根本没碰到那个网关，读数却照样上报），而供给只有个位数张。
func TestAttachRouteDoesNotLeakPairToNonChatGPTHost(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	for _, rawURL := range []string{
		"https://api.openai.com/v1/responses",
		"https://bps.openai.com/basispoints/api/responses",
		"https://chatgpt.com.evil.example/backend-api/codex/responses",
	} {
		headers := http.Header{}
		require.NoError(t, attachRoute(context.Background(), store, acct, rawURL, headers))
		require.Empty(t, headers.Get("Cookie"), "%s 不该拿到 pair", rawURL)
	}
	require.Zero(t, fake.hits.Load(), "非 ChatGPT 主机不许消耗池子槽位")
	require.Zero(t, fake.strays.Load())
}

// B1：WS 拨号与网关池互斥。连接复用 60 分钟而满血窗口只有约 150 秒，pair 只在握手挂一次；
// 而且预热（min_idle 默认 4）在无业务请求时就拨连接。闸门必须在取 pair **之前**，
// 所以预热一张 pair 都拿不到。
func TestAttachRouteRefusesWebSocketWhenGatewayPoolEnabled(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	headers := http.Header{"Authorization": []string{"Bearer offline"}}
	err := attachRoute(context.Background(), store, acct, gwpoolTestWSURL, headers)
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
	pool.cookies = &openAICodexCookieStore{}

	conn, err := pool.dialConn(context.Background(), openAIWSAcquireRequest{
		Account: fake.account(31),
		WSURL:   gwpoolTestWSURL,
		Headers: http.Header{"Authorization": []string{"Bearer offline"}},
	})
	require.ErrorIs(t, err, ErrGatewayPoolWSIncompatible)
	require.Nil(t, conn)
	require.Empty(t, dialer.Headers(), "拒绝必须发生在拨号之前")
	require.Zero(t, fake.hits.Load())
}

// 选路降级（WS 入站侧）：透传模式的账号开了网关池时，这一轮桥到 HTTP/SSE 上游，
// 既不拨 WS（拨了就压在一个会烧完的网关上），也不把请求打成失败。
// 出站的 HTTP 请求必须带着池子那张 pair——降级之后覆写仍然生效，这才是降级的意义。
func TestOpenAIWSIngressBridgesGatewayPoolAccountToHTTP(t *testing.T) {
	account := wireProfileTestAccount(false)
	account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModePassthrough
	account.Extra[openAIGatewayPoolExtraKey] = true
	cfg := codexWSWireProfileConfig()
	svc := codexWSWireProfileService(cfg)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader(
			"data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_gwpool_bridge\",\"output\":[]," +
				"\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\ndata: [DONE]\n\n")),
	}}
	svc.httpUpstream = upstream
	svc.accountRepo = &stubQuotaAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	fake.configure(account)
	dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{&openAIWSCaptureConn{}}}
	svc.openaiWSPassthroughDialer = dialer

	// 读 1 个事件（HTTP 桥把 response.done 原样转出）再正常关闭：会话等不到下一帧 relay 不会返回。
	relayErr := runCodexWSIngressOneTurn(t, svc, account, codexWSIngressInbound(), codexWSTestFrame, 1)
	if relayErr != nil {
		require.Contains(t, relayErr.Error(), "StatusNormalClosure")
	}
	require.Empty(t, dialer.Headers(), "降级之后绝不许拨 WS")
	require.NotNil(t, upstream.lastReq, "这一轮必须从 HTTP 上游出去")
	require.Contains(t, upstream.lastReq.Header.Get("Cookie"), poolCookie, "桥到 HTTP 之后覆写仍要生效")
	require.Equal(t, int64(1), fake.hits.Load())
}

// 复现 + 回归（需求 1）：mode_router_v2 开着时，账号**不用显式开 WS** 也会被判成 WS 上游
// （回落到全局 ingress_mode_default），于是开了网关池的账号每一发都在 AttachRoute 上撞
// ErrGatewayPoolWSIncompatible。选路层的后置过滤器必须把决策改成 http_sse。
func TestGatewayPoolDowngradesWSDecisionToHTTPSSE(t *testing.T) {
	cfg := codexWSWireProfileConfig()
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
	resolver := NewOpenAIWSProtocolResolver(cfg)

	plain := wireProfileTestAccount(false)
	decision := resolver.Resolve(plain)
	require.Equal(t, OpenAIUpstreamTransportResponsesWebsocketV2, decision.Transport,
		"前提：这个账号没开任何 WS 开关也会被判成 WS 上游（复现的起点）")
	require.Equal(t, decision, resolveOpenAIWSDecisionByGatewayPool(decision, plain), "没开网关池不许动决策")

	pooled := wireProfileTestAccount(false)
	pooled.Extra[openAIGatewayPoolExtraKey] = true
	pooledDecision := resolveOpenAIWSDecisionByGatewayPool(resolver.Resolve(pooled), pooled)
	require.Equal(t, OpenAIUpstreamTransportHTTPSSE, pooledDecision.Transport)
	require.Equal(t, "gwpool_takeover", pooledDecision.Reason)

	// 本来就是 HTTP/SSE 的决策原样放过（理由不能被盖成 gwpool_takeover，那会掩盖真实原因）。
	httpDecision := openAIWSHTTPDecision("global_disabled")
	require.Equal(t, httpDecision, resolveOpenAIWSDecisionByGatewayPool(httpDecision, pooled))

	// 纯判据（不打日志的那个）给「只想知道实际传输是什么」的消费方用：粘性
	// previous_response_id 绑定按它判，否则网关池账号会被当成 WSv2、绑定留着，
	// 下一发被钉回这个账号又走 HTTP（HTTP 上游没有 WSv2 的延续态）。
	require.True(t, gatewayPoolTakesOverWSUpstream(resolver.Resolve(pooled), pooled))
	require.False(t, gatewayPoolTakesOverWSUpstream(resolver.Resolve(plain), plain))
	require.False(t, gatewayPoolTakesOverWSUpstream(httpDecision, pooled),
		"本来就走 HTTP 的不算「被池子降级」")
	require.False(t, gatewayPoolTakesOverWSUpstream(resolver.Resolve(pooled), nil))
}

// runCodexWSIngressOneTurn 是 runCodexWSIngress 的单轮变体：发一帧，把 relay 自己的错误回传。
// 原版断言每轮都读到 response.completed——降级路径上客户端收到的是 HTTP 桥原样转出的
// response.done，拒绝路径上更是一个事件都没有。readEvents 是发完帧后要读几个事件：
// 读完就正常关闭客户端，否则会话会一直等下一帧，relay 永不返回。
func runCodexWSIngressOneTurn(
	t *testing.T,
	svc *OpenAIGatewayService,
	account *Account,
	inbound http.Header,
	frame string,
	readEvents int,
) error {
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
	for i := 0; i < readEvents; i++ {
		readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
		_, _, readErr := client.Read(readCtx)
		cancelRead()
		require.NoError(t, readErr, "event %d", i+1)
	}
	if readEvents > 0 {
		_ = client.Close(coderws.StatusNormalClosure, "done")
	}
	select {
	case relayErr := <-relayErrCh:
		return relayErr
	case <-time.After(5 * time.Second):
		t.Fatal("websocket ingress did not finish")
		return nil
	}
}

// 勾上网关池不该让保存失败：WS 上游 2026-10-01 起不再是冲突项（本该走 WS 的请求在选路层降级），
// 但「开关开着就必须配齐地址与凭据」这条跨字段约束照旧。
func TestValidateOpenAIGatewayPoolAccountExtraAllowsWSUpstream(t *testing.T) {
	acct := gwpoolTestAccount(1)
	// 开着池子的合法配置底子（地址与凭据的校验由另一条用例覆盖）。
	poolExtra := func(more map[string]any) map[string]any {
		extra := map[string]any{
			openAIGatewayPoolExtraKey:            true,
			openAIGatewayPoolBaseURLExtraKey:     "http://127.0.0.1:8099",
			OpenAIGatewayPoolConsumerKeyExtraKey: "ck",
		}
		for k, v := range more {
			extra[k] = v
		}
		return extra
	}
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(acct, poolExtra(nil)))

	for _, wsKey := range []string{
		"openai_oauth_responses_websockets_v2_enabled",
		"responses_websockets_v2_enabled",
		"openai_ws_enabled",
	} {
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(acct, poolExtra(map[string]any{wsKey: true})), wsKey)
	}

	// WS 的几个键不再有跨字段约束 ⇒ 不该再为它们白读一次账号。
	require.False(t, touchesOpenAIGatewayPoolConfig(map[string]any{"openai_ws_enabled": false}))
	require.True(t, touchesOpenAIGatewayPoolConfig(map[string]any{openAIGatewayPoolExtraKey: true}))
	require.True(t, touchesOpenAIGatewayPoolConfig(map[string]any{openAIGatewayPoolBaseURLExtraKey: "http://x"}))
	require.False(t, touchesOpenAIGatewayPoolConfig(map[string]any{"quota_used": 1}))
}

// 复现：部分更新（重授权 / 单键编辑）开网关池时，库里已开着 WS 上游曾被整单拒绝。
func TestUpdateAccountExtraAllowsGatewayPoolWithWSUpstream(t *testing.T) {
	accountID := int64(201)
	// 地址与凭据已在库里（只改开关这一个键），所以放行与否只取决于 WS 那一边。
	poolConfig := map[string]any{
		openAIGatewayPoolBaseURLExtraKey:     "http://127.0.0.1:8099",
		OpenAIGatewayPoolConsumerKeyExtraKey: "stored-ck",
	}
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{
		accountID: {ID: accountID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Extra: mergeMap(poolConfig, map[string]any{"openai_oauth_responses_websockets_v2_enabled": true})},
	}}
	svc := &adminServiceImpl{accountRepo: repo}

	require.NoError(t, svc.UpdateAccountExtra(context.Background(), accountID,
		map[string]any{openAIGatewayPoolExtraKey: true}))
	require.Equal(t, true, repo.accounts[accountID].Extra[openAIGatewayPoolExtraKey])

	// 关掉 WS 再开也放行。
	repo.accounts[accountID].Extra = mergeMap(poolConfig, nil)
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
	store := &openAICodexCookieStore{}

	for _, id := range []int64{1, 35} { // pro1 与它的克隆行
		headers := http.Header{}
		require.NoError(t, attachRoute(context.Background(), store, fake.account(id), gwpoolTestURL, headers))
		require.Equal(t, poolCookie, headers.Get("Cookie"))
	}
	require.EqualValues(t, 1, fake.hits.Load(), "同一个凭证域身份只该向池子要一张")
}

// S4：共享的那次取用自己的 ctx——第一名的客户端断开不能把同账号其他请求连坐成 502。
func TestGatewayPoolFetchSurvivesCallerCancel(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	// arrived / release 是确定性屏障：取 pair 已经出站（arrived）才取消，取消之后才让池子回话
	// （release）。不用 sleep——那只是赌调度，机器一卡就赌输。
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 只有 /cookie 走屏障：/gateways（挑网关）回 404 ⇒ 退回裸取，与这条用例无关。
		if r.URL.Path != "/cookie" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hits.Add(1)
		arrived <- struct{}{}
		<-release
		_, _ = io.WriteString(w, `{"gateway":"unified-142","cookie":"`+poolCookie+`","valid_for_s":150}`)
	}))
	defer srv.Close()

	store := &openAICodexCookieStore{}
	acct := gwpoolTestAccount(1)
	acct.Extra[openAIGatewayPoolBaseURLExtraKey] = srv.URL
	acct.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = gwpoolTestConsumerKey
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		headers := http.Header{}
		err := attachRoute(ctx, store, acct, gwpoolTestURL, headers)
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
		store := &openAICodexCookieStore{}
		headers := http.Header{}
		require.NoError(t, attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, headers))
		require.Equal(t, "__cflb=lb; __oailb="+oailb, headers.Get("Cookie"))
		require.NotContains(t, headers.Get("Cookie"), "sessionid")
	})
	t.Run("一个路由 cookie 都没有就是畸形响应", func(t *testing.T) {
		fake := newGwpoolFakePool(t, "sessionid=steal", 150)
		store := &openAICodexCookieStore{}
		headers := http.Header{}
		err := attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, headers)
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
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, headers))
	require.Equal(t, first, headers.Get("Cookie"))
	require.Equal(t, gwpoolTestAccountQuery, fake.nextQuery(t), "正常路径不带 force")

	// 把窗口拨到过去，模拟 valid_for_s 到点。
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: first, gateway: "unified-142", version: "tkt-1", until: time.Now().Add(-time.Second)})

	rotated := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, rotated))
	require.Equal(t, fake.forceCookie, rotated.Get("Cookie"), "到期后要换成池子新给的那张")
	// force 说「换个网关」，exclude_versions 说「这张具体的票不行」，exclude 是本地账本里还在
	// 窗口内的那些 —— 三样一起带，池子才知道换什么、避开什么。
	require.Equal(t, gwpoolTestAccountQuery+"&exclude=unified-142&exclude_versions=tkt-1&force=1",
		fake.nextQuery(t))
	require.EqualValues(t, 2, fake.hits.Load())
}

// force=1 收到 503 = 池子真的换不出来了 ⇒ fail-closed，**不回落罐回放、也不复用过期那张**。
// 而且下一发仍然 force：过期那张留在缓存里正是「别再给我这一个」的依据。
func TestGatewayPoolForceNoSlotFailsClosed(t *testing.T) {
	stale := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, stale, 150)
	fake.forceStatus = http.StatusServiceUnavailable
	store := &openAICodexCookieStore{}
	acct := fake.account(1)
	// 罐里有一张能回放的：也不许用。
	store.Store(acct, gwpoolTestURL, codexCookieUpstreamResponse())
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: stale, gateway: "unified-142", version: "tkt-stale", until: time.Now().Add(-time.Second)})

	for range 2 {
		headers := http.Header{}
		err := attachRoute(context.Background(), store, acct, gwpoolTestURL, headers)
		require.ErrorIs(t, err, gwpool.ErrNoSlot)
		require.Empty(t, headers.Get("Cookie"), "失败时一个 cookie 都不许写出去")
		require.Equal(t, gwpoolTestAccountQuery+"&exclude_versions=tkt-stale&force=1",
			fake.nextQuery(t), "过期那张还在缓存里，下一发仍要换、仍要点名排除它")
	}
	require.EqualValues(t, 2, fake.hits.Load(), "一次失败一次请求，不许自动重试")
}

// ---------------------------------------------------------------------------
// 自己挑网关（GET /gateways → GET /cookie?gateway=）
// ---------------------------------------------------------------------------

// 要票前先列网关，在「有活 pair、池子说你没烧过」的里面点一个名。
func TestGatewayPoolPicksUnburntGateway(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-167")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	fake.listGateways = []gwpoolFakeGateway{
		{Name: "unified-126", PairReady: true, UsedByYou: true}, // 池子记着这个身份烧过
		{Name: "unified-195"},                  // 没有活 pair
		{Name: "unified-167", PairReady: true}, // 唯一候选
	}
	store := &openAICodexCookieStore{}

	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, headers))
	require.Equal(t, poolCookie, headers.Get("Cookie"))
	require.Equal(t, gwpoolTestAccountQuery+"&gateway=unified-167", fake.nextQuery(t), "必须点名，而不是让池子随便给")
	require.Equal(t, gwpoolTestAccountQuery, <-fake.listQueries,
		"列网关也要报上游账号，否则 used_by_you 是上传者的历史")
	require.EqualValues(t, 1, fake.listHits.Load(), "一次取票只列一次网关")
	require.EqualValues(t, 1, fake.hits.Load())
	require.Zero(t, fake.strays.Load())
}

// 多个候选时挑**最久没碰**的；池子缺省 last_used_at（从没碰过）最优。
func TestGatewayPoolPicksLeastRecentlyUsedCandidate(t *testing.T) {
	for name, tc := range map[string]struct {
		gateways []gwpoolFakeGateway
		want     string
	}{
		"都碰过就挑最久远的": {
			gateways: []gwpoolFakeGateway{
				{Name: "unified-167", PairReady: true, LastUsedAt: "2026-10-01T09:00:00Z"},
				{Name: "unified-183", PairReady: true, LastUsedAt: "2026-10-01T06:00:00Z"},
				{Name: "unified-165", PairReady: true, LastUsedAt: "2026-10-01T11:00:00Z"},
			},
			want: gwpoolTestAccountQuery + "&gateway=unified-183",
		},
		"从没碰过的优先": {
			gateways: []gwpoolFakeGateway{
				{Name: "unified-167", PairReady: true, LastUsedAt: "2026-10-01T06:00:00Z"},
				{Name: "unified-183", PairReady: true},
			},
			want: gwpoolTestAccountQuery + "&gateway=unified-183",
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			fake.listGateways = tc.gateways
			store := &openAICodexCookieStore{}
			require.NoError(t, attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, http.Header{}))
			require.Equal(t, tc.want, fake.nextQuery(t))
		})
	}
}

// 列表拿不到（池子还没加这个端点 / 临时挂了）⇒ 退回裸取，由池子自己挑。
// **绝不能因为列不出来就让这一发失败**：接 /gateways 之前的行为永远是兜底。
func TestGatewayPoolFallsBackToBareTakeWhenListUnavailable(t *testing.T) {
	for name, status := range map[string]int{
		"池子没这个端点": http.StatusNotFound,
		"列表挂了":    http.StatusInternalServerError,
		"列表没槽位":   http.StatusServiceUnavailable,
	} {
		t.Run(name, func(t *testing.T) {
			poolCookie := gwpoolTestPairCookie(t, "unified-142")
			fake := newGwpoolFakePool(t, poolCookie, 150)
			fake.listStatus = status
			store := &openAICodexCookieStore{}

			headers := http.Header{}
			require.NoError(t, attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, headers))
			require.Equal(t, poolCookie, headers.Get("Cookie"), "列不出来也必须照常拿到票")
			require.Equal(t, gwpoolTestAccountQuery, fake.nextQuery(t), "退回裸取：不带 gateway")
			require.EqualValues(t, 1, fake.listHits.Load())
			require.EqualValues(t, 1, fake.hits.Load())
		})
	}
}

// 一个候选都挑不出来（池子侧全烧过 / 没活 pair，加上本地账本排掉的那个）⇒ 同样裸取。
func TestGatewayPoolFallsBackToBareTakeWhenEveryCandidateIsBurnt(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	fake.listGateways = []gwpoolFakeGateway{
		{Name: "unified-126", PairReady: true, UsedByYou: true},
		{Name: "unified-195"},
		// 池子还认为这个可用，但本地账本里 4 小时内碰过（下面种进去）。
		{Name: "unified-167", PairReady: true},
	}
	store := &openAICodexCookieStore{}
	store.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-167")

	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, headers))
	require.Equal(t, poolCookie, headers.Get("Cookie"), "挑不出来也要拿到票，落点交给池子")
	require.Equal(t, gwpoolTestAccountQuery+"&exclude=unified-167", fake.nextQuery(t),
		"挑不出来就不许点名，但账本要带成 exclude —— 裸取恰恰是最需要它的时候")
}

// 点名的那个在「列表」与「取票」之间被别人租走（503）⇒ 退回裸取一次。
// 这一发什么都没交付、没烧任何槽位，不退回去就等于自己挑网关反而把能成的请求打成失败。
func TestGatewayPoolRetriesBareWhenSteeredTakeHasNoSlot(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	var steered, bare atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/gateways":
			_, _ = io.WriteString(w, `{"gateways":[{"name":"unified-167","pair_ready":true}]}`)
		case r.URL.Query().Get("gateway") != "":
			steered.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			bare.Add(1)
			_, _ = io.WriteString(w, `{"gateway":"unified-142","cookie":"`+poolCookie+`","valid_for_s":150}`)
		}
	}))
	defer srv.Close()

	store := &openAICodexCookieStore{}
	acct := gwpoolTestAccount(1)
	acct.Extra[openAIGatewayPoolBaseURLExtraKey] = srv.URL
	acct.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = gwpoolTestConsumerKey

	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, headers))
	require.Equal(t, poolCookie, headers.Get("Cookie"))
	require.EqualValues(t, 1, steered.Load())
	require.EqualValues(t, 1, bare.Load(), "点名失败只退回裸取一次，不许自动重试点名")
}

// 本地账本的键是**上游账号**而不是本地账号行 ID：同一份 Codex 凭据挂在多个 sub2api 账号行上时，
// 烧的是同一个 (上游账号 × 网关) 单位，按行记会让每一行都以为自己还有满血窗口。
// 粒度到 chatgpt_account_id 为止——同账号下不同 chatgpt_user_id 的两份凭据也共用一本账。
func TestGatewayPoolLedgerIsKeyedByCredentialIdentityNotRowID(t *testing.T) {
	first := gwpoolTestPairCookie(t, "unified-167")
	second := gwpoolTestPairCookie(t, "unified-183")
	fake := newGwpoolFakePool(t, first, 150)
	fake.forceCookie = second
	// 池子那本账里三个都「没烧过」⇒ 之后排掉谁都只可能是本地账本。
	// last_used_at 决定挑选顺序：167（从没碰过）→ 183（09-30）→ 165（10-01）。
	fake.listGateways = []gwpoolFakeGateway{
		{Name: "unified-167", PairReady: true},
		{Name: "unified-183", PairReady: true, LastUsedAt: "2026-09-30T00:00:00Z"},
		{Name: "unified-165", PairReady: true, LastUsedAt: "2026-10-01T00:00:00Z"},
	}
	store := &openAICodexCookieStore{}

	// 行 1 取票：167 从没碰过 ⇒ 挑它，并记进账本。
	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, headers))
	require.Equal(t, gwpoolTestAccountQuery+"&gateway=unified-167", fake.nextQuery(t))
	require.True(t, store.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-167", time.Hour))

	// 把缓存那张拨到过期，逼下一发重新取票（否则同身份直接复用窗口内那张）。
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: first, gateway: "unified-167", until: time.Now().Add(-time.Second)})

	// 行 35 是同一份凭据的另一个本地行：账本必须共用 ⇒ 167 已经烧过，只能挑 183。
	rotated := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, fake.account(35), gwpoolTestURL, rotated))
	require.Equal(t, second, rotated.Get("Cookie"))
	require.Equal(t, gwpoolTestAccountQuery+"&exclude=unified-167&force=1&gateway=unified-183",
		fake.nextQuery(t), "另一个本地行不许再去碰 167：点名避开它，exclude 也要带上它")

	// 同一个 chatgpt_account_id、另一个 chatgpt_user_id（例如工作区里的第二个人）：对 OpenAI
	// 是同一个账号，烧的是同一个窗口 ⇒ 必须共用这本账，167 和 183 照样被排掉，只剩 165。
	sameAccountOtherUser := fake.account(36)
	sameAccountOtherUser.Credentials = map[string]any{
		"chatgpt_account_id": "acc-a", "chatgpt_user_id": "user-b"}
	require.NotEqual(t, gwpoolTestIdentity, openAIGatewayPoolAccountKey(sameAccountOtherUser),
		"前提：它是另一个凭证域身份，所以共用账本只能来自账号粒度的收敛")
	require.NoError(t, attachRoute(context.Background(), store, sameAccountOtherUser, gwpoolTestURL, http.Header{}))
	require.Equal(t, gwpoolTestAccountQuery+"&exclude=unified-167,unified-183&gateway=unified-165", fake.nextQuery(t),
		"同账号另一个 user 不许再去碰 167 / 183")

	// 另一个上游账号是另一本账，不受影响，照样挑 167。
	other := fake.account(37)
	other.Credentials = map[string]any{"chatgpt_account_id": "acc-b", "chatgpt_user_id": "user-b"}
	require.NoError(t, attachRoute(context.Background(), store, other, gwpoolTestURL, http.Header{}))
	require.Equal(t, "account=acc-b&gateway=unified-167", fake.nextQuery(t),
		"另一个上游账号：报自己的 account，且不受别人那本账影响")
}

// 账本的保留窗口默认 4 小时（(账号 × 网关) 的再生周期估算值），可按账号配；
// 配坏了（0 / 负数 / 非数字）回默认，不该因为一个旋钮让账号挑不出网关。
func TestGatewayPoolLedgerWindowIsConfigurable(t *testing.T) {
	store := &openAICodexCookieStore{}
	acct := gwpoolTestAccount(1)
	require.Equal(t, 4*time.Hour, acct.gatewayPoolGatewayWindow())

	store.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-167")
	require.True(t, store.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-167", acct.gatewayPoolGatewayWindow()))
	require.False(t, store.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-183", acct.gatewayPoolGatewayWindow()))
	require.False(t, store.gatewayPoolUsedRecently("chatgpt:acc-b", "unified-167", acct.gatewayPoolGatewayWindow()))

	// 出了窗口就不再排除它。
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "unified-167"), time.Now().Add(-5*time.Hour))
	require.False(t, store.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-167", acct.gatewayPoolGatewayWindow()))

	acct.Extra[openAIGatewayPoolGatewayWindowExtraKey] = 60
	require.Equal(t, time.Minute, acct.gatewayPoolGatewayWindow())
	for _, bad := range []any{0, -1, "nonsense", nil} {
		acct.Extra[openAIGatewayPoolGatewayWindowExtraKey] = bad
		require.Equal(t, 4*time.Hour, acct.gatewayPoolGatewayWindow(), "%v", bad)
	}
}

// 池子 503（没有满血槽位）：报错走既有失败路径，**不回落罐回放**，上游一个请求都不发。
func TestDoOpenAIUpstreamGatewayPoolNoSlotFailsClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	upstream := &cookieRecordingUpstream{setCookie: codexCookieUpstreamResponse()}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := gwpoolTestAccount(1)
	acct.Extra[openAIGatewayPoolBaseURLExtraKey] = srv.URL
	acct.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = gwpoolTestConsumerKey
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
	acct := fake.account(1)
	// 罐里存着另一个网关的 pair，它不该再被当成这一发的读数。
	svc.codexCookies.Store(acct, openAITurnStatePairCookieURL, codexCookieUpstreamResponse())

	headers := http.Header{}
	applied := attachRouteApplied(t, &svc.codexCookies, acct, gwpoolTestURL, headers)

	pair, fromPool, poolGateway, poolVersion := svc.routePairInUse(acct, http.Header{}, applied)
	require.Equal(t, poolCookie, pair)
	require.Equal(t, "unified-142", openAICodexRouteGateway(pair))
	require.True(t, fromPool, "使用记录要标出这一发被池子覆写过")
	require.Equal(t, "unified-142", poolGateway, "注入生效 = 池子给的网关与落点一致")
	require.Equal(t, "tkt-1", poolVersion, "票号落库才能和池子的交付日志对上账")

	fresh := http.Header{"Set-Cookie": []string{
		"__cflb=new-lb; Path=/",
		"__oailb=" + routeCookieTestOailb(t, "chat.gateway.unified-84.api.openai.com") + "; Path=/",
	}}
	// 上游改派：落点记新的那个，「已覆写」仍为真（出站带的确实是池子那张），而池子给的网关
	// 仍是 142 ⇒ 两者不一致就是「注入被拒」的判据，页面据此分辨。
	freshPair, fromPool, poolGateway, poolVersion := svc.routePairInUse(acct, fresh, applied)
	require.Equal(t, "unified-84", openAICodexRouteGateway(freshPair))
	require.True(t, fromPool)
	require.Equal(t, "unified-142", poolGateway)
	require.Equal(t, "tkt-1", poolVersion, "改派也要记票号：对账时正是要看这张票发生了什么")
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 只有 /cookie 走屏障：/gateways（挑网关）回 404 ⇒ 退回裸取，与这条用例无关。
		if r.URL.Path != "/cookie" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hits.Add(1)
		arrived <- struct{}{}
		<-release
		_, _ = io.WriteString(w, `{"gateway":"unified-142","cookie":"`+poolCookie+`","valid_for_s":150}`)
	}))
	defer srv.Close()

	store := &openAICodexCookieStore{}
	acct := gwpoolTestAccount(1)
	acct.Extra[openAIGatewayPoolBaseURLExtraKey] = srv.URL
	acct.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = gwpoolTestConsumerKey
	done := make(chan error, 4)
	attach := func() {
		headers := http.Header{}
		err := attachRoute(context.Background(), store, acct, gwpoolTestURL, headers)
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

// 客户端按账号取，且按 (base_url, consumer key) 缓存：gwpool.New 每次都自带一个 http.Transport，
// 每请求新建等于每请求一个独立连接池，连接永不复用、fd 一路涨。
func TestGatewayPoolClientCachedPerBaseURLAndKey(t *testing.T) {
	store := &openAICodexCookieStore{}

	a := gwpoolTestAccount(1)
	a.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:8099"
	a.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "ck-one"
	first, err := store.poolClient(a)
	require.NoError(t, err)
	require.NotNil(t, first)

	// 同一份配置的另一个本地行 ⇒ 同一个客户端实例。
	b := gwpoolTestAccount(35)
	b.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:8099"
	b.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "ck-one"
	again, err := store.poolClient(b)
	require.NoError(t, err)
	require.Same(t, first, again, "同一份配置必须复用客户端，否则连接池泄漏")

	// key 换了就是另一个池子身份（池子按账号发 key）⇒ 必须是另一个客户端。
	c := gwpoolTestAccount(36)
	c.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:8099"
	c.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "ck-two"
	other, err := store.poolClient(c)
	require.NoError(t, err)
	require.NotSame(t, first, other, "换了 consumer key 不能复用上一个身份的客户端")
}

// 开关开着但地址缺失 / 解不开 ⇒ fail closed，**不退回罐回放**（那正是要修掉的「钉死在坏网关」），
// 而且错误要包着 gwpool.ErrPool：配错是池子侧问题，不能把真账号当成上游故障停调度 10 分钟。
func TestGatewayPoolEnabledWithoutBaseURLFailsClosed(t *testing.T) {
	store := &openAICodexCookieStore{}
	acct := gwpoolTestAccount(1) // 开关开着、没配地址
	// 罐里有一张能回放的：绝不许退回去用。
	store.Store(acct, gwpoolTestURL, codexCookieUpstreamResponse())

	headers := http.Header{}
	err := attachRoute(context.Background(), store, acct, gwpoolTestURL, headers)
	require.ErrorIs(t, err, gwpool.ErrPool)
	require.Empty(t, headers.Get("Cookie"), "失败时一个 cookie 都不许写出去")
	require.False(t, classifyUpstreamTransportError(err).Persistent, "配错不该停调度真账号")

	acct.Extra[openAIGatewayPoolBaseURLExtraKey] = "://nonsense"
	_, err = store.poolClient(acct)
	require.ErrorIs(t, err, gwpool.ErrPool)
}

// 账号级配置的写入闸：开关开着就必须配齐地址与凭据（配置已不在实例级，没有启动期可拦）。
func TestValidateOpenAIGatewayPoolAccountExtraRequiresBaseURLAndKey(t *testing.T) {
	acct := gwpoolTestAccount(1)

	err := validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{openAIGatewayPoolExtraKey: true})
	require.Error(t, err, "缺地址不许落库")
	require.Contains(t, err.Error(), openAIGatewayPoolBaseURLExtraKey)

	for _, bad := range []any{"", "nonsense", "/relative", "ftp://host", "http://host#frag", 42} {
		err := validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{
			openAIGatewayPoolExtraKey:            true,
			openAIGatewayPoolBaseURLExtraKey:     bad,
			OpenAIGatewayPoolConsumerKeyExtraKey: "ck",
		})
		require.Error(t, err, "base_url=%v", bad)
	}

	err = validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{
		openAIGatewayPoolExtraKey:        true,
		openAIGatewayPoolBaseURLExtraKey: "http://127.0.0.1:8099",
	})
	require.Error(t, err, "缺 consumer key 不许落库")
	require.Contains(t, err.Error(), OpenAIGatewayPoolConsumerKeyExtraKey)

	// 配齐了就放行；开关关着时这两项一律不管（留着旧配置不该挡住保存）。
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{
		openAIGatewayPoolExtraKey:            true,
		openAIGatewayPoolBaseURLExtraKey:     "http://127.0.0.1:8099",
		OpenAIGatewayPoolConsumerKeyExtraKey: "ck",
	}))
	require.NoError(t, validateOpenAIGatewayPoolAccountExtra(acct, map[string]any{
		openAIGatewayPoolBaseURLExtraKey: "nonsense",
	}))

	// 新增的两个配置键也要能触发「加载账号做合并校验」，否则只改地址的部分更新会绕过去。
	require.True(t, touchesOpenAIGatewayPoolConfig(map[string]any{openAIGatewayPoolBaseURLExtraKey: "x"}))
	require.True(t, touchesOpenAIGatewayPoolConfig(map[string]any{OpenAIGatewayPoolConsumerKeyExtraKey: "x"}))
}

// consumer key 是凭据：页面不回显原值，所以只有**非空字符串**才算改动，其余一律续用库里那份。
// 没有这一道，每次保存账号都会把凭据抹掉。
func TestMergeOpenAIGatewayPoolConsumerKeyPreservesStoredValue(t *testing.T) {
	existing := map[string]any{OpenAIGatewayPoolConsumerKeyExtraKey: "stored-ck"}

	for name, submitted := range map[string]any{
		"脱敏回显的 bool": true,
		"空串":         "",
		"只有空白":       "   ",
		"类型不对":       42,
	} {
		incoming := map[string]any{OpenAIGatewayPoolConsumerKeyExtraKey: submitted}
		mergeOpenAIGatewayPoolConsumerKey(existing, incoming)
		require.Equal(t, "stored-ck", incoming[OpenAIGatewayPoolConsumerKeyExtraKey], name)
	}

	// 完全没提也续上（全量 extra 更新里缺省 = 被脱敏掉了，不是「要删」）。
	incoming := map[string]any{}
	mergeOpenAIGatewayPoolConsumerKey(existing, incoming)
	require.Equal(t, "stored-ck", incoming[OpenAIGatewayPoolConsumerKeyExtraKey])

	// 真的改了就按新值走。
	incoming = map[string]any{OpenAIGatewayPoolConsumerKeyExtraKey: "fresh-ck"}
	mergeOpenAIGatewayPoolConsumerKey(existing, incoming)
	require.Equal(t, "fresh-ck", incoming[OpenAIGatewayPoolConsumerKeyExtraKey])

	// 部分更新 / 批量传 nil existing：只剔掉，绝不回填——批量共用一份 extra，回填会把
	// 一个账号的凭据写进其它账号。
	incoming = map[string]any{OpenAIGatewayPoolConsumerKeyExtraKey: true, "other": 1}
	mergeOpenAIGatewayPoolConsumerKey(nil, incoming)
	require.NotContains(t, incoming, OpenAIGatewayPoolConsumerKeyExtraKey)
	require.Equal(t, 1, incoming["other"])
}

// 全量保存一次账号后凭据必须还在库里（走真实的 UpdateAccount，不是直接调 merge）。
func TestUpdateAccountKeepsGatewayPoolConsumerKey(t *testing.T) {
	accountID := int64(202)
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{
		accountID: {ID: accountID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Credentials: map[string]any{"chatgpt_account_id": "acc-a"},
			Extra: map[string]any{
				openAIGatewayPoolExtraKey:            true,
				openAIGatewayPoolBaseURLExtraKey:     "http://127.0.0.1:8099",
				OpenAIGatewayPoolConsumerKeyExtraKey: "stored-ck",
			}},
	}}
	svc := &adminServiceImpl{accountRepo: repo}

	// 页面提交的是脱敏后的形态：key 的位置上是个 bool。
	_, err := svc.UpdateAccount(context.Background(), accountID, &UpdateAccountInput{Extra: map[string]any{
		openAIGatewayPoolExtraKey:            true,
		openAIGatewayPoolBaseURLExtraKey:     "http://127.0.0.1:8099",
		OpenAIGatewayPoolConsumerKeyExtraKey: true,
	}})
	require.NoError(t, err)
	require.Equal(t, "stored-ck", repo.accounts[accountID].Extra[OpenAIGatewayPoolConsumerKeyExtraKey],
		"保存账号不许把凭据抹掉")

	// 真的填了新 key 就改掉。
	_, err = svc.UpdateAccount(context.Background(), accountID, &UpdateAccountInput{Extra: map[string]any{
		openAIGatewayPoolExtraKey:            true,
		openAIGatewayPoolBaseURLExtraKey:     "http://127.0.0.1:8099",
		OpenAIGatewayPoolConsumerKeyExtraKey: "fresh-ck",
	}})
	require.NoError(t, err)
	require.Equal(t, "fresh-ck", repo.accounts[accountID].Extra[OpenAIGatewayPoolConsumerKeyExtraKey])
}

// ---------------------------------------------------------------------------
// 覆写范围与四个旋钮（都在账号 extra 上）
// ---------------------------------------------------------------------------

// 路径常量不能和真实出口漂开：漂了就会把推理面判成非推理面，整个覆写静默失效。
func TestGatewayPoolInferencePathMatchesCodexURL(t *testing.T) {
	require.True(t, strings.HasSuffix(chatgptCodexURL, openAIGatewayPoolInferencePath),
		"chatgptCodexURL=%s", chatgptCodexURL)
}

// 默认只给 Codex 推理面覆写：侧信道（装饰性 GET）、/codex/alpha/search 这些也打在 chatgpt.com
// 上，给它们取票等于白烧一个 (上游账号 × 网关) 单位。开 openai_gwpool_all_models 回到
// 「所有 chatgpt.com 请求都覆写」。
func TestAttachRouteOnlyOverridesInferenceFaceByDefault(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	store := &openAICodexCookieStore{}
	acct := fake.account(1)
	// 罐里存着上游上次下发的那组：非推理面要原样落回它，而不是不带 cookie。
	store.Store(acct, gwpoolTestURL, codexCookieUpstreamResponse())

	for _, rawURL := range []string{
		"https://chatgpt.com/backend-api/wham/settings/user",
		"https://chatgpt.com/backend-api/codex/alpha/search",
		"https://chatgpt.com/backend-api/codex/realtime/calls",
	} {
		headers := http.Header{}
		require.NoError(t, attachRoute(context.Background(), store, acct, rawURL, headers))
		require.Contains(t, headers.Get("Cookie"), "__oailb=jwt-1", rawURL)
		require.NotContains(t, headers.Get("Cookie"), poolCookie, rawURL)
	}
	require.Zero(t, fake.hits.Load(), "非推理面一张票都不许取")

	// 推理面照常覆写。
	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, headers))
	require.Contains(t, headers.Get("Cookie"), poolCookie)
	require.Equal(t, int64(1), fake.hits.Load())

	// 开关打开：非推理面也覆写（接这个开关之前的行为）。
	acct.Extra[openAIGatewayPoolAllModelsExtraKey] = true
	headers = http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct,
		"https://chatgpt.com/backend-api/codex/alpha/search", headers))
	require.Contains(t, headers.Get("Cookie"), poolCookie)
	require.Equal(t, int64(1), fake.hits.Load(), "窗口内复用同一张，不该再取一次")
}

// 四个旋钮的默认值必须等于接它们之前的写死值；配坏了（0 / 负数 / 非数字）也回默认，
// 不许让账号取不到票。
func TestGatewayPoolAccountKnobDefaults(t *testing.T) {
	bare := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	require.Equal(t, openAIGatewayPoolFetchTimeout, bare.gatewayPoolFetchTimeout())
	require.Equal(t, openAIGatewayPoolListTimeout, bare.gatewayPoolListTimeout())
	require.Equal(t, openAIGatewayPoolGatewayWindow, bare.gatewayPoolGatewayWindow())
	require.True(t, bare.gatewayPoolSteering(), "自己挑落点缺省即开")
	require.False(t, bare.gatewayPoolAllModels())

	// 超大值也算配坏：time.Duration 是纳秒级 int64，秒数到 1e10 就乘溢出成**负数** ⇒
	// 本地账本整体静默失效（gatewayPoolUsedRecently 恒 false），与「窗口越大越严」正好相反；
	// 同量级的取票超时则让这个账号全量取不到票。
	for name, bad := range map[string]any{
		"零": 0, "负数": -5, "非数字": "x", "布尔": true,
		"溢出": 10_000_000_000, "刚超上限": openAIGatewayPoolMaxSeconds + 1,
	} {
		acct := &Account{ID: 1, Extra: map[string]any{
			openAIGatewayPoolFetchTimeoutExtraKey:  bad,
			openAIGatewayPoolListTimeoutExtraKey:   bad,
			openAIGatewayPoolGatewayWindowExtraKey: bad,
		}}
		require.Equal(t, openAIGatewayPoolFetchTimeout, acct.gatewayPoolFetchTimeout(), name)
		require.Equal(t, openAIGatewayPoolListTimeout, acct.gatewayPoolListTimeout(), name)
		require.Equal(t, openAIGatewayPoolGatewayWindow, acct.gatewayPoolGatewayWindow(), name)
	}

	tuned := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
		openAIGatewayPoolFetchTimeoutExtraKey:  20,
		openAIGatewayPoolListTimeoutExtraKey:   float64(5), // jsonb 解出来是 float64
		openAIGatewayPoolGatewayWindowExtraKey: 7200,
		openAIGatewayPoolSteeringExtraKey:      false,
		openAIGatewayPoolAllModelsExtraKey:     true,
	}}
	require.Equal(t, 20*time.Second, tuned.gatewayPoolFetchTimeout())
	require.Equal(t, 5*time.Second, tuned.gatewayPoolListTimeout())
	require.Equal(t, 2*time.Hour, tuned.gatewayPoolGatewayWindow())
	// 上限本身要收：1 天是合法配置。
	atCap := &Account{ID: 1, Extra: map[string]any{
		openAIGatewayPoolGatewayWindowExtraKey: openAIGatewayPoolMaxSeconds,
	}}
	require.Equal(t, 24*time.Hour, atCap.gatewayPoolGatewayWindow())
	require.False(t, tuned.gatewayPoolSteering())
	require.True(t, tuned.gatewayPoolAllModels())

	// 只有显式 false 才关掉「自己挑落点」：写错类型不能把它关掉（那会静默改变调度行为）。
	require.True(t, (&Account{Extra: map[string]any{openAIGatewayPoolSteeringExtraKey: "false"}}).gatewayPoolSteering())
}

// steering 关掉 ⇒ 一次 /gateways 都不发，由池子自己挑；票照样取到。
func TestGatewayPoolSteeringOffSkipsGatewayListing(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-84", PairReady: true}}
	store := &openAICodexCookieStore{}
	acct := fake.account(1)
	acct.Extra[openAIGatewayPoolSteeringExtraKey] = false

	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, headers))
	require.Equal(t, int64(1), fake.hits.Load())
	require.Zero(t, fake.listHits.Load(), "关掉自己挑落点就不该列网关")
	require.NotContains(t, fake.nextQuery(t), "gateway=", "不许点名")
}

// 池子侧失败的报错要能看懂（双语单串，转发面没有 i18n 协商通道），同时必须保住两件事：
// errors.Is 的标记（否则传输错误分类器会把真账号按「代理持久故障」停调度 10 分钟），
// 以及原始诊断信息。
func TestGatewayPoolClientErrorIsBilingualAndKeepsMarker(t *testing.T) {
	noSlot := gatewayPoolClientError(gwpool.ErrNoSlot)
	require.ErrorIs(t, noSlot, gwpool.ErrNoSlot)
	require.ErrorIs(t, noSlot, gwpool.ErrPool)
	require.Contains(t, noSlot.Error(), "网关池当前没有满血槽位")
	require.Contains(t, noSlot.Error(), "no full-strength slot")
	require.False(t, classifyUpstreamTransportError(noSlot).Persistent)

	misconfigured := gatewayPoolClientError(fmt.Errorf("%w: cookie request returned HTTP 401", gwpool.ErrPool))
	require.ErrorIs(t, misconfigured, gwpool.ErrPool)
	require.NotErrorIs(t, misconfigured, gwpool.ErrNoSlot)
	require.Contains(t, misconfigured.Error(), "网关池不可用或配置有误")
	require.Contains(t, misconfigured.Error(), "HTTP 401", "原始诊断信息不能被吃掉")
	require.Nil(t, gatewayPoolClientError(nil))

	// 缺地址这类配置错误走的是同一条路（poolClient 的错误经 gatewayPoolPair 抛上来）。
	store := &openAICodexCookieStore{}
	acct := gwpoolTestAccount(1) // 开着开关但没配地址
	acct.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "ck-secret-value"
	err := attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{})
	require.ErrorIs(t, err, gwpool.ErrPool)
	require.Contains(t, err.Error(), "网关池不可用或配置有误")
	require.NotContains(t, err.Error(), "ck-secret-value", "凭据一个字都不许进报错")
}

// 客户端按 (base_url, consumer key, 单次调用上限) 缓存。上限必须进键：客户端自带 http.Client，
// 不进键的话改了超时拿回来的还是旧那个（而 http.Client.Timeout 与 ctx 取较小者 ⇒ 账号配的
// 取票超时会被旧客户端悄悄截短）。
func TestPoolClientCacheKeyFollowsCallTimeout(t *testing.T) {
	store := &openAICodexCookieStore{}
	acct := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
		openAIGatewayPoolExtraKey:            true,
		openAIGatewayPoolBaseURLExtraKey:     "http://127.0.0.1:8099",
		OpenAIGatewayPoolConsumerKeyExtraKey: "ck",
	}}

	first, err := store.poolClient(acct)
	require.NoError(t, err)
	again, err := store.poolClient(acct)
	require.NoError(t, err)
	require.Same(t, first, again, "同一份配置必须复用同一个客户端（每请求新建 = 每请求一个连接池）")

	acct.Extra[openAIGatewayPoolFetchTimeoutExtraKey] = 20
	tuned, err := store.poolClient(acct)
	require.NoError(t, err)
	require.NotSame(t, first, tuned, "改了取票超时必须换一个客户端")

	// 客户端的上限取两个 ctx deadline 里更大的那个 ⇒ (fetch 20, list 2) 与 (fetch 2, list 20)
	// 落在同一个缓存键上。这条同时钉住「上限 = max(fetch, list)」。
	acct.Extra[openAIGatewayPoolFetchTimeoutExtraKey] = 2
	acct.Extra[openAIGatewayPoolListTimeoutExtraKey] = 20
	swapped, err := store.poolClient(acct)
	require.NoError(t, err)
	require.Same(t, tuned, swapped)
}
