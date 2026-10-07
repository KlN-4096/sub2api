package service

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

// 2026-10-02 池子新协议的消费侧：exclude（本地账本）/ 按错误码退避 / min_remaining / wait /
// cookie_version + exclude_versions / POST /release。一律打 httptest 假池子，**绝不打真实上游**。

// gwpoolBackoffLeft 只要剩余时长那一半，给 require.Zero / require.LessOrEqual 当表达式用。
func gwpoolBackoffLeft(store *openAICodexCookieStore) time.Duration {
	left, _ := store.gatewayPoolBackoffFor(gwpoolTestIdentity)
	return left
}

func TestGatewayPoolProbeModelOverrideIsExplicitAndWhitelisted(t *testing.T) {
	account := gwpoolTestAccount(1)
	require.Equal(t, "gpt-6-luna", account.gatewayPoolProbeModel("gpt-6-astra"))
	account.Extra[openAIGatewayPoolProbeModelExtraKey] = "gpt-6-luna"
	require.Equal(t, "gpt-6-luna", account.gatewayPoolProbeModel("gpt-6-astra"))
	account.Extra[openAIGatewayPoolProbeModelExtraKey] = "gpt-99-test"
	require.Equal(t, "gpt-6-luna", account.gatewayPoolProbeModel("gpt-6-astra"),
		"invalid persisted values fall back to the default probe model")
	require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, map[string]any{
		openAIGatewayPoolExtraKey:            true,
		openAIGatewayPoolBaseURLExtraKey:     "https://pool.example.test",
		OpenAIGatewayPoolConsumerKeyExtraKey: "key",
		openAIGatewayPoolProbeModelExtraKey:  "gpt-99-test",
	}))
	account.Extra[openAIGatewayPoolProbeModelExtraKey] = gatewayPoolProbeModelBusiness
	require.Equal(t, "gpt-6-astra", account.gatewayPoolProbeModel("gpt-6-astra"))
	for _, model := range []string{gatewayPoolProbeModelAstra, gatewayPoolProbeModelSol, gatewayPoolProbeModelLuna, gatewayPoolProbeModelBusiness} {
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, map[string]any{
			openAIGatewayPoolExtraKey: true, openAIGatewayPoolBaseURLExtraKey: "https://pool.example.test",
			OpenAIGatewayPoolConsumerKeyExtraKey: "key", openAIGatewayPoolProbeModelExtraKey: model,
		}))
	}
}

// ---------------------------------------------------------------------------
// 1. 本地账本当 exclude 带上去
// ---------------------------------------------------------------------------

// 账本里还在窗口内的网关要全部带成 exclude，**最近烧的在前**（超 64 项时裁尾），
// 过了窗口的不带，别的身份的不带。
func TestGatewayPoolExcludesRecentlyBurntGateways(t *testing.T) {
	store := &openAICodexCookieStore{}
	now := time.Now()
	// 时间戳自己写进账本：两笔真实取票相差微秒，Windows 的时钟粒度会把它们记成同一刻，
	// 钉顺序就只能钉在可控的时间上。
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "unified-old"), now.Add(-5*time.Hour))
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "unified-1"), now.Add(-3*time.Minute))
	store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "unified-2"), now.Add(-time.Minute))
	store.poolUsed.Store(gatewayPoolLedgerKey("chatgpt:acc-b", "unified-other"), now)

	burnt := store.gatewayPoolBurnedGateways(gwpoolTestIdentity, openAIGatewayPoolGatewayWindow, gwpool.MaxExcludeItems)
	require.Equal(t, []string{"unified-2", "unified-1"}, burnt,
		"窗口内的两个、最近烧的在前；5 小时前那个出了 4 小时窗口，别的身份的不算")

	// 收敛到上游账号粒度：同一个 chatgpt_account_id 下的另一个 user 看到同一本账
	// （降智的作用单位是 (上游账号 × 网关)）。
	require.Equal(t, burnt, store.gatewayPoolBurnedGateways("chatgpt:acc-a:user:user-b",
		openAIGatewayPoolGatewayWindow, gwpool.MaxExcludeItems))

	// 上限：裁到 limit 项，裁掉的是最久的那些。
	for i := range 80 {
		store.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "unified-bulk-"+strconv.Itoa(i)),
			now.Add(-time.Duration(i)*time.Second))
	}
	capped := store.gatewayPoolBurnedGateways(gwpoolTestIdentity, openAIGatewayPoolGatewayWindow, gwpool.MaxExcludeItems)
	require.Len(t, capped, gwpool.MaxExcludeItems)
	require.Equal(t, "unified-bulk-0", capped[0], "最近烧的必须留在表里")
	require.NotContains(t, capped, "unified-bulk-79", "裁掉的是最久的那些")

	// 没有账本 / 没有身份时不带这个参数，别给池子发一个空 exclude。
	require.Empty(t, store.gatewayPoolBurnedGateways("", openAIGatewayPoolGatewayWindow, gwpool.MaxExcludeItems))
	require.Empty(t, (&openAICodexCookieStore{}).gatewayPoolBurnedGateways(gwpoolTestIdentity,
		openAIGatewayPoolGatewayWindow, gwpool.MaxExcludeItems))
}

// ---------------------------------------------------------------------------
// 2. 按错误码分流
// ---------------------------------------------------------------------------

// all_cooling / no_exit / upstream_rejected 要按身份退避：退避期内**一个池子请求
// 都不发**（原先一视同仁 ⇒ 客户端的重试环每轮都在池子侧触发一次发现铸票）。
//
// rate_limited **不在这一组**：它讲的是池子的铸票预算，不是「有没有票给我」，
// 见 TestGatewayPoolRateLimitedNeverBlocksBusinessRequests。
func TestGatewayPoolBacksOffByErrorCode(t *testing.T) {
	cases := []struct {
		name        string
		code        string
		retryAfter  int
		wantBackoff time.Duration
	}{
		// all_cooling 不在这一组，它有自己的封顶，见 TestGatewayPoolCapsAllCoolingBackoff。
		{"冷却", gwpool.CodeAllCooling, 0, openAIGatewayPoolDefaultBackoff},
		{"出口全熔断", gwpool.CodeNoExit, 0, openAIGatewayPoolDefaultBackoff},
		{"上游否决池子的号", gwpool.CodeUpstreamRejected, 0, openAIGatewayPoolDefaultBackoff},
		// consumer key 配错 / 被吊销：重试一万次也是这个结果，池子按契约给 300s。
		{"池子不受理我们的 key", gwpool.CodeConsumerRejected, 300, 5 * time.Minute},
		// 出站参数不合法 = 我们这边的 bug/配置错。池子刻意不给 retry_after ⇒ 时长必须本地兜，
		// 不然这个码就退化成一个纯热循环。
		{"我们的请求不合法", gwpool.CodeBadRequest, 0, openAIGatewayPoolBadRequestBackoff},
		{"退避时长钳上限", gwpool.CodeNoExit, int((10 * gwpool.MaxRetryAfter).Seconds()), gwpool.MaxRetryAfter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			fake.refuseStatus = http.StatusServiceUnavailable
			fake.refuseCode = tc.code
			fake.refuseRetryAfter = tc.retryAfter
			store := &openAICodexCookieStore{}
			acct := fake.account(1)

			err := attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{})
			require.ErrorIs(t, err, gwpool.ErrNoSlot, "拒票一律 fail-closed，绝不回落降智路由")
			require.EqualValues(t, 1, fake.hits.Load())

			// 退避记在身份上，所以**下一发请求**也不许去敲池子。
			err = attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{})
			require.ErrorIs(t, err, gwpool.ErrNoSlot)
			require.EqualValues(t, 1, fake.hits.Load(), "退避期内一个池子请求都不许发")

			remaining, _ := store.gatewayPoolBackoffFor(gwpoolTestIdentity)
			require.Positive(t, remaining)
			require.LessOrEqual(t, remaining, tc.wantBackoff)
			require.Greater(t, remaining, tc.wantBackoff-10*time.Second)
		})
	}
}

// rate_limited 管的是池子的**铸票**预算（按「铸票账号 × 区域」6 发/小时，铸票与 state-echo
// 共用），不是「池子有没有票给我」：池子只在一张活票都没有时才铸，而别的账号铸出的票、
// 别的区域的票、续期续活的票在它说的那一小时里全都是可交付的。
//
// 采信那个 retry_after 就是拿池子内部某个号的配额停掉**我们所有业务请求**，最长一小时，
// 期间每一发聊天直接 502。所以这个码一秒都不许退避——下一发请求照样去问。
func TestGatewayPoolRateLimitedNeverBlocksBusinessRequests(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.refuseStatus = http.StatusServiceUnavailable
	fake.refuseCode = gwpool.CodeRateLimited
	fake.refuseRetryAfter = int(time.Hour.Seconds()) // 池子给的是满格一小时
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	err := attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{})
	require.ErrorIs(t, err, gwpool.ErrNoSlot)
	require.EqualValues(t, 1, fake.hits.Load())

	require.Zero(t, gwpoolBackoffLeft(store),
		"rate_limited 不许记退避：它说的是池子铸不动，不是池子没票")

	// 池子补上票之后，**下一发业务请求立刻就能拿到**——退避会把这一段整个吃掉。
	fake.refuseCode, fake.refuseStatus, fake.refuseRetryAfter = "", 0, 0
	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, headers))
	require.EqualValues(t, 2, fake.hits.Load(), "第二发必须真的去问了池子")
	require.Contains(t, headers.Get("Cookie"), "__cflb=")
}

// all_cooling 的 retry_after **只对「刚才那批候选网关」成立**，不能当成全局静默期：
// 池子算它用的是 `slot_cooldown`（管理页可配，默认 4h）减去那一行在那批网关上的已歇时长，
// 而候选集随时会变——新账号上传、发现新网关、保温补票。
//
// 2026-10-02 现场：池子回了 all_cooling + retry_after≈3h47m，sub2api 据此静默到 06:28；
// 22 分钟后池子已经有 3 个这个号从没碰过、且带着活票的网关，它却连问都不问，
// 期间每一发业务请求都因「没有满血槽位」直接 502（设计上绝不回落降智路由）。
func TestGatewayPoolCapsAllCoolingBackoff(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.refuseStatus = http.StatusServiceUnavailable
	fake.refuseCode = gwpool.CodeAllCooling
	fake.refuseRetryAfter = int((4 * time.Hour).Seconds())
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	require.ErrorIs(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}),
		gwpool.ErrNoSlot)

	// 期望值写成独立字面量，不引用生产常量：引用它的话把常量改成 4h 这条用例照样绿。
	require.LessOrEqual(t, gwpoolBackoffLeft(store), time.Minute,
		"all_cooling 的 retry_after 只对当时那批网关成立，不能拿来静默数小时")
}

// 反向：别为了封顶把「池子说更短」也盖掉——那会让一次运气不好变成整整一分钟不干活。
func TestGatewayPoolHonorsShortAllCoolingRetryAfter(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.refuseStatus = http.StatusServiceUnavailable
	fake.refuseCode = gwpool.CodeAllCooling
	fake.refuseRetryAfter = 12
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	require.ErrorIs(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}),
		gwpool.ErrNoSlot)

	remaining, _ := store.gatewayPoolBackoffFor(gwpoolTestIdentity)
	require.Positive(t, remaining)
	require.LessOrEqual(t, remaining, 12*time.Second)
	require.Greater(t, remaining, 2*time.Second)
}

// 可等待 / 一次性的码不退避：no_live_pair、no_gateway 这些交给 wait 与既有的一次裸取重试，
// 停掉它们等于把一次运气不好变成一分钟不干活。集合外的码同理（不照抄没约定过的分流依据）。
func TestGatewayPoolDoesNotBackOffOnWaitableCodes(t *testing.T) {
	for _, code := range []string{
		gwpool.CodeNoLivePair, gwpool.CodeNoGateway, gwpool.CodeMintFailed,
		gwpool.CodePublicClosed, gwpool.CodeDegraded, "something_new",
	} {
		t.Run(code, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			fake.refuseStatus = http.StatusServiceUnavailable
			fake.refuseCode = code
			store := &openAICodexCookieStore{}
			acct := fake.account(1)

			for range 2 {
				require.ErrorIs(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}),
					gwpool.ErrNoSlot)
			}
			require.EqualValues(t, 2, fake.hits.Load(), "不退避 ⇒ 下一发照常取票")
			require.Zero(t, gwpoolBackoffLeft(store))
		})
	}
}

// no_exit = 池子的出口全熔断 ⇒ 点名失败后**不做**那一次裸取重试：换网关只是把同一个错重复一遍。
// 对照组 no_live_pair 保留重试（点名的那个可能刚被别人租走，裸取还有戏）。
func TestGatewayPoolBareRetryDependsOnErrorCode(t *testing.T) {
	cases := []struct {
		code      string
		wantHits  int64
		wantRetry string
	}{
		{gwpool.CodeNoExit, 1, "池子整体故障，不重试"},
		{gwpool.CodeNoLivePair, 2, "可能只是这个网关被租走了，退回裸取一次"},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-167"), 150)
			fake.listGateways = []gwpoolFakeGateway{{Name: "unified-167", PairReady: true}}
			fake.refuseStatus = http.StatusServiceUnavailable
			fake.refuseCode = tc.code
			store := &openAICodexCookieStore{}

			require.ErrorIs(t, attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, http.Header{}),
				gwpool.ErrNoSlot)
			require.EqualValues(t, tc.wantHits, fake.hits.Load(), tc.wantRetry)
			require.Contains(t, fake.nextQuery(t), "gateway=unified-167", "第一次是点名")
			if tc.wantHits > 1 {
				require.NotContains(t, fake.nextQuery(t), "gateway=", "第二次是裸取")
			}
		})
	}
}

// 取到票就把退避清掉，不留一个过期的到点值。
func TestGatewayPoolClearsBackoffAfterSuccess(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	store := &openAICodexCookieStore{}
	acct := fake.account(1)
	store.poolBackoff.Store(gatewayPoolLedgerIdentity(gwpoolTestIdentity), time.Now().Add(-time.Second))

	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}))
	_, present := store.poolBackoff.Load(gatewayPoolLedgerIdentity(gwpoolTestIdentity))
	require.False(t, present, "过期的退避记录取票成功后就该删掉")
}

// ---------------------------------------------------------------------------
// 3 / 5. min_remaining 与 wait
// ---------------------------------------------------------------------------

// 每发都要带 min_remaining（够缓存复用，不是够这一发用）与 wait，且 **wait 必须严格小于取票超时**
// ——池子还在等、这边先断等于白等一场。
func TestGatewayPoolAsksForUsableLifetimeAndWait(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}))
	query, err := url.ParseQuery(fake.nextRawQuery(t))
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(int(openAIGatewayPoolMinRemaining.Seconds())), query.Get("min_remaining"))

	wait, err := strconv.Atoi(query.Get("wait"))
	require.NoError(t, err, "wait 必须带，不然池子不会等现铸")
	require.Positive(t, wait)
	require.Less(t, float64(wait), acct.gatewayPoolFetchTimeout().Seconds(),
		"wait 必须小于取票超时，否则池子还在等、这边先超时")

	// 取票超时配小 ⇒ wait 跟着变小甚至不带（钳位是代码而不是注释）。
	acct.Extra[openAIGatewayPoolFetchTimeoutExtraKey] = 1
	store = &openAICodexCookieStore{} // 独立观察等待参数，不绕过上一发刚开始的本地冷却。
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}))
	tight, err := url.ParseQuery(fake.nextRawQuery(t))
	require.NoError(t, err)
	require.Empty(t, tight.Get("wait"), "1s 的取票预算塞不下任何等待")
}

// ---------------------------------------------------------------------------
// 4. cookie_version 落库 + 换票时点名排除
// ---------------------------------------------------------------------------

// 池子报的 cookie_version 要记进缓存，换票时作为 exclude_versions 上送（比 force 精确：
// force 只说「换个网关」）。
func TestGatewayPoolExcludesStaleTicketVersion(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.forceCookie = gwpoolTestPairCookie(t, "unified-84")
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}))
	first, state := store.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.Equal(t, "tkt-1", first.version, "票号要进缓存，不然换票时说不出「哪一张不行」")
	_ = fake.nextQuery(t)

	// 窗口到点：手里这张转 stale ⇒ 下一次取票带上它的票号。
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: first.cookie, gateway: first.gateway, version: first.version,
		until: time.Now().Add(-time.Second)})
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}))
	require.Contains(t, fake.nextQuery(t), "exclude_versions=tkt-1")

	second, _ := store.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, "tkt-2", second.version, "换来的那张要换上新票号")
}

// ---------------------------------------------------------------------------
// 6. 取了票但一个字节都没发出去 ⇒ 还票
// ---------------------------------------------------------------------------

// gwpoolRunOnce 跑**预热下面那一层**。
//
// 这一组用例关闭质量防护，只验取票/注入/
// 还票协议：走上层的话每个用例都要先把判据那两发也配出来，而且它们的请求体里没有 model ⇒
// 预热会先 fail closed（errOpenAIGatewayPoolWarmNoModel），一张票都取不到，测不到任何东西。
//
// 靶子选这一层是对的，不是绕过：还票判据本身就住在 doOpenAIUpstreamOnce 里
// （gatewayPoolReleasesUnsent 的调用点）。预热与转发的组合由 openai_gwpool_warm_test.go 盯。
func gwpoolRunOnce(svc *OpenAIGatewayService, req *http.Request, acct *Account) (*http.Response, error) {
	transportOnly := *acct
	transportOnly.Extra = make(map[string]any, len(acct.Extra)+1)
	for key, value := range acct.Extra {
		transportOnly.Extra[key] = value
	}
	transportOnly.Extra[openAIGatewayPoolGuardEnabledExtraKey] = false
	resp, _, err := svc.doOpenAIUpstreamOnce(req, "", &transportOnly)
	return resp, err
}

// gwpoolErrorUpstream 让 doOpenAIUpstream 在「已经取到票」之后失败，失败形态由 err 给定。
//
// 为什么要能给任意形态：doOpenAIUpstreamRoundTrip 有**两条出口**（先插件
// RoundTripOpenAIOAuth，没命中才 httpUpstream.Do），两条出口的错误汇到同一个判据
// （gatewayPoolReleasesUnsent）。插件那条返回的是 *PluginTransportError（带 RequestSent，
// 而且**没有 Unwrap**）或裸 ctx.Err()，用一个「只会回裸 error / *url.Error」的假上游
// 就永远穿不到那两种形态——2026-10-02 的审查正是在这里抓到一个错边的判据。
// 插件进程没法在单测里立起来（PluginManager 是具体类型 + 子进程），所以按**错误形态**覆盖。
type gwpoolErrorUpstream struct {
	err   error
	calls int
}

func gwpoolBareError() error {
	// httpUpstream.Do 进 http.Client 之前那几道（主机校验、取客户端/并发槽）返回的就是裸 error。
	return errors.New("request host is not allowed")
}

func gwpoolTransportError() error {
	// client.Do 的失败一律被 net/http 包成 *url.Error。
	return &url.Error{Op: "Post", URL: gwpoolTestURL, Err: errors.New("dial tcp: connection refused")}
}

func (u *gwpoolErrorUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	u.calls++
	if u.err != nil {
		return nil, u.err
	}
	return nil, gwpoolTransportError()
}

func (u *gwpoolErrorUpstream) DoWithTLS(
	req *http.Request, proxyURL string, id int64, c int, _ *tlsfingerprint.Profile,
) (*http.Response, error) {
	return u.Do(req, proxyURL, id, c)
}

// 这一发在进 http.Client 之前就失败（裸 error）⇒ 槽位还给池子，并把缓存里那张删掉
// （槽位都还了还继续拿它出站等于对池子说谎）。
func TestGatewayPoolReleasesTicketWhenNothingWasSent(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolErrorUpstream{err: gwpoolBareError()}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := fake.account(1)

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	_, err = gwpoolRunOnce(svc, req, acct)
	require.Error(t, err)

	require.Equal(t, `{"cookie_version":"tkt-1"}`, fake.nextRelease(t))
	require.EqualValues(t, 1, fake.releaseHits.Load())
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairNone, state, "还掉的票不许留在缓存里继续出站")
}

// 同上，但票是**临期票**：取票后第一发会先抢走续期名额（原地改掉缓存里那张的 renewPending），
// 所以还票时按整个结构体做 CompareAndDelete 会对不上 ⇒ 票还了、本地还留着它继续出站。
// 认票只能按票号。
func TestGatewayPoolReleasesNearExpiryTicketFromCacheToo(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.pairRemainingS = 600
	svc := &OpenAIGatewayService{httpUpstream: &gwpoolErrorUpstream{err: gwpoolBareError()}}
	acct := fake.account(1)

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	_, err = gwpoolRunOnce(svc, req, acct)
	require.Error(t, err)

	require.Equal(t, `{"cookie_version":"tkt-1"}`, fake.nextRelease(t))
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairNone, state, "还掉的票不许留在缓存里继续出站")
}

// 客户端在共享取票完成前离开：调用者及时退出，独立取票留缓存供下一发使用，不发送业务。
func TestGatewayPoolKeepsPendingTicketWhenClientVanishedBeforeSend(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolErrorUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := fake.account(1)

	// 池子收到 /cookie 时取消调用者，响应此时尚未回到 AttachRoute。
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	fake.onCookie = cancel
	defer cancel()

	_, err = gwpoolRunOnce(svc, req, acct)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, upstream.calls, "客户端已经走了就不该再打上游")
	require.Eventually(t, func() bool {
		_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
		return state == openAIGatewayPoolPairLive
	}, time.Second, time.Millisecond)
	require.Zero(t, fake.releaseHits.Load(), "共享取票可能已被其它调用者接用，不异步还票")
}

// 对照组一：请求真发出去了（拿到响应）⇒ 绝不还票。满血窗口是**首次接触**就烧掉的。
// 对照组二：传输错误（*url.Error）⇒ 连上没连上分不出来，宁可不还 —— 错还会让池子把烧过的
// 槽位当新鲜的再发给别人。
func TestGatewayPoolKeepsTicketWhenRequestMayHaveLanded(t *testing.T) {
	t.Run("拿到响应", func(t *testing.T) {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
		svc := &OpenAIGatewayService{httpUpstream: &cookieRecordingUpstream{}}
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		resp, err := gwpoolRunOnce(svc, req, fake.account(1))
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Zero(t, fake.releaseHits.Load())
	})
	t.Run("传输错误", func(t *testing.T) {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
		svc := &OpenAIGatewayService{httpUpstream: &gwpoolErrorUpstream{}}
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		_, err = gwpoolRunOnce(svc, req, fake.account(1))
		require.Error(t, err)
		require.Zero(t, fake.releaseHits.Load(), "可能已经碰到网关了，不许谎报没用过")
	})
}

// 缓存复用的那张不是「我取的」，不许由我还：还了之后别的请求还在拿它出站。
func TestGatewayPoolDoesNotReleaseReusedTicket(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{httpUpstream: &gwpoolErrorUpstream{err: gwpoolBareError()}}
	acct := fake.account(1)

	// 先把一张活票塞进缓存：这一发是复用它，不是自己取的 ⇒ 失败时不许还、也不许删。
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: gwpoolTestPairCookie(t, "unified-9"), gateway: "unified-9",
		version: "tkt-reused", until: time.Now().Add(time.Minute)})
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	_, err = gwpoolRunOnce(svc, req, acct)
	require.Error(t, err)

	require.Zero(t, fake.releaseHits.Load(), "复用的票不是我取的，不许我还")
	reused, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state, "也不许把它从缓存里删掉")
	require.Equal(t, "tkt-reused", reused.version)
}

// 还票失败（池子回 409 / 打不通）绝不影响主流程：请求的错误原样返回，不多出别的错误。
func TestGatewayPoolReleaseFailureIsSwallowed(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.releaseStatus = http.StatusConflict
	svc := &OpenAIGatewayService{httpUpstream: &gwpoolErrorUpstream{err: gwpoolBareError()}}

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	_, err = gwpoolRunOnce(svc, req, fake.account(1))
	require.EqualError(t, err, gwpoolBareError().Error(), "还票的失败不许冒泡")
	require.EqualValues(t, 1, fake.releaseHits.Load())
}

// 池子没报 cookie_version（老池子）⇒ 还不了，但主流程照常，一个 /release 都不发。
func TestGatewayPoolSkipsReleaseWithoutTicketVersion(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.omitVersion = true
	svc := &OpenAIGatewayService{httpUpstream: &gwpoolErrorUpstream{err: gwpoolBareError()}}

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	_, err = gwpoolRunOnce(svc, req, fake.account(1))
	require.Error(t, err)
	require.Zero(t, fake.releaseHits.Load())
	require.Zero(t, fake.strays.Load())
}

// ---------------------------------------------------------------------------
// 票的可用期判据（2026-10-02 协调方点名核查）
// ---------------------------------------------------------------------------

// 缓存的到点必须来自池子的 valid_for_s（满血窗口，最多 150s），**不是**账号级的
// openai_gwpool_gateway_window_s（默认 4h —— 那是本地账本窗口，管「别重用烧过的网关」，
// 不是票的可用期）。认错了的后果是窗口之后每一发都在拿降智的路由跑业务。
func TestGatewayPoolPairExpiresByValidForNotLedgerWindow(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	store := &openAICodexCookieStore{}
	acct := fake.account(1)
	// 把账本窗口配成和默认值不同的另一个大数：到点若按它算就会被这条用例抓到。
	acct.Extra[openAIGatewayPoolGatewayWindowExtraKey] = 7200

	before := time.Now()
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, http.Header{}))
	pair, state := store.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.WithinDuration(t, before.Add(150*time.Second), pair.until, 2*time.Second,
		"到点 = 取票时刻 + valid_for_s")
	require.Less(t, pair.until.Sub(before), 10*time.Minute,
		"绝不能按账本窗口（这里 2h、默认 4h）算：那会让一张票在满血窗口之后继续出站")
}

// 复现口径：一张票过了 valid_for_s 之后，下一发**重新取票**（带 force=1 换网关），
// 而不是复用那张已经烧掉的。
//
// valid_for_s 用 1 秒而不是 150 秒：判据是「现在过了 until 没有」（cachedPoolPair），
// 与具体秒数无关，而单个后端测试要留在 60s 预算内。上面那条用例钉的正是「until 来自
// valid_for_s」，两条合起来覆盖「151 秒后不复用」。
func TestGatewayPoolRefetchesAfterValidForElapses(t *testing.T) {
	first := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, first, 1)
	fake.forceCookie = gwpoolTestPairCookie(t, "unified-84")
	store := &openAICodexCookieStore{}
	acct := fake.account(1)

	fresh := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, fresh))
	require.Equal(t, first, fresh.Get("Cookie"))
	require.EqualValues(t, 1, fake.hits.Load())
	_ = fake.nextQuery(t)

	// 窗口内复用：不许再敲池子。
	reused := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, reused))
	require.Equal(t, first, reused.Get("Cookie"))
	require.EqualValues(t, 1, fake.hits.Load(), "窗口内复用同一张，不许每发都取票")

	time.Sleep(1100 * time.Millisecond) // 过 valid_for_s

	rotated := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, rotated))
	require.EqualValues(t, 2, fake.hits.Load(), "过了满血窗口必须重新取票")
	require.Equal(t, fake.forceCookie, rotated.Get("Cookie"), "出站换成新那张，不许继续带烧过的")
	require.Contains(t, fake.nextQuery(t), "force=1", "换票要点明「换一个不同的网关」")
}

// 还票判据必须覆盖 doOpenAIUpstreamRoundTrip 的**两条出口**的全部错误形态。
// 插件那条的 *PluginTransportError 没有 Unwrap ⇒ 只看 *url.Error 的判据会把「已经发出去的」
// 也还回池子，而错还是**害别人**：池子会把一个其实烧过的槽位当新鲜的再发给下一个消费者。
func TestGatewayPoolReleaseJudgementCoversBothRoundTripExits(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantRelease bool
	}{
		{"插件说已经发出去了", &PluginTransportError{Code: "PLUGIN_X", RequestSent: true}, false},
		{"插件说没发出去", &PluginTransportError{Code: "PLUGIN_X", RequestSent: false}, true},
		{"插件 RPC 在 ctx 死后返回裸 ctx 错误", context.Canceled, false},
		{"插件 RPC 超时", context.DeadlineExceeded, false},
		{"传输跑过了（*url.Error）", gwpoolTransportError(), false},
		{"还没进 http.Client（裸 error）", gwpoolBareError(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			svc := &OpenAIGatewayService{httpUpstream: &gwpoolErrorUpstream{err: tc.err}}
			// ctx 是活的：发送前那道显式检查不许抢掉这里要测的判据。
			req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
			require.NoError(t, err)
			_, err = gwpoolRunOnce(svc, req, fake.account(1))
			require.Error(t, err)

			if tc.wantRelease {
				require.Equal(t, `{"cookie_version":"tkt-1"}`, fake.nextRelease(t))
				return
			}
			require.Zero(t, fake.releaseHits.Load(), "可能已经发出去了，不许谎报没用过")
		})
	}
}

// ---------------------------------------------------------------------------
// 「已覆写」必须是 per-request 的事实，不是回读缓存（2026-10-02 审查 F1）
// ---------------------------------------------------------------------------

// 回读缓存的那一版会说假话：它只查「接管开着 + 缓存里有票」，手上没有 URL ⇒ 账号取过一张票之后
// （stale 票永不删 ⇒ 永久），每一发**非推理面**且记用量的请求都会落库成「已覆写 + 池子的网关」，
// 而出站带的是罐里那组 —— 正好把这张卡片唯一要回答的问题反过来答了。
func TestGatewayPoolAppliedMarkerIsPerRequest(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	svc := &OpenAIGatewayService{}
	store := &svc.codexCookies
	acct := fake.account(1)
	// 罐里有一组（上游上次下发的）：非推理面的请求出站带的就是它。
	store.Store(acct, gwpoolTestURL, codexCookieUpstreamResponse())

	// 1) 推理面且真注入 ⇒ TRUE + 池子的网关与票号。
	inference := http.Header{}
	applied := attachRouteApplied(t, store, acct, gwpoolTestURL, inference)
	// 罐里那个 __cf_bm 是本出口自己的 CF 令牌，刻意留着；路由对那两项换成池子的。
	require.Contains(t, inference.Get("Cookie"), poolCookie)
	pair, fromPool, poolGateway, poolVersion := svc.routePairInUse(acct, http.Header{}, applied)
	require.True(t, fromPool)
	require.Equal(t, poolCookie, pair)
	require.Equal(t, "unified-142", poolGateway)
	require.Equal(t, "tkt-1", poolVersion)
	require.NotNil(t, usageCodexRoutePairOverriddenPtr(acct, fromPool))
	require.True(t, *usageCodexRoutePairOverriddenPtr(acct, fromPool))

	// 2) 非推理面（缓存里此刻**有**一张活票）⇒ FALSE + 网关票号都为 NULL。
	//    可达调用点：/codex/alpha/search、/codex/images/*，它们都记用量。
	for _, rawURL := range []string{
		"https://chatgpt.com/backend-api/codex/alpha/search",
		"https://chatgpt.com/backend-api/codex/images/generations",
		"https://chatgpt.com/backend-api/wham/settings",
	} {
		t.Run(rawURL, func(t *testing.T) {
			decorative := http.Header{}
			sideApplied := attachRouteApplied(t, store, acct, rawURL, decorative)
			require.Equal(t, OpenAIGatewayPoolApplied{}, sideApplied, "这一发没注入，标记必须是零值")
			require.NotContains(t, decorative.Get("Cookie"), "pool-lb", "非推理面走罐回放")

			_, fromPool, gateway, version := svc.routePairInUse(acct, http.Header{}, sideApplied)
			require.False(t, fromPool, "没注入就不许落「已覆写」")
			require.Empty(t, gateway)
			require.Empty(t, version)
			require.Nil(t, usageCodexRoutePairPoolGatewayPtr(gateway), "网关列要保持 NULL")
			require.Nil(t, usageCodexRoutePairPoolVersionPtr(version))
			overridden := usageCodexRoutePairOverriddenPtr(acct, fromPool)
			require.NotNil(t, overridden, "FALSE 与 NULL 要分开：账号类型适用就落 FALSE")
			require.False(t, *overridden)
		})
	}

	// 3) 接管开着但取不到票（池子 503）⇒ 这一发失败，标记仍是零值。
	refusing := newGwpoolFakePool(t, poolCookie, 150)
	refusing.refuseStatus = http.StatusServiceUnavailable
	refusing.refuseCode = gwpool.CodeNoLivePair
	noSlotStore := &openAICodexCookieStore{}
	noSlotAcct := refusing.account(2)
	ctx, sink := withOpenAIGatewayPoolSink(context.Background(), nil)
	_, err := noSlotStore.AttachRoute(ctx, noSlotAcct, gwpoolTestURL, http.Header{})
	require.ErrorIs(t, err, gwpool.ErrNoSlot)
	result := &OpenAIForwardResult{}
	sink.publish(result)
	require.Equal(t, OpenAIGatewayPoolApplied{}, result.GatewayPoolApplied)
	_, fromPool, _, _ = svc.routePairInUse(noSlotAcct, http.Header{}, result.GatewayPoolApplied)
	require.False(t, fromPool)
}

// 故障转移在同一个 ctx 里换号重试 ⇒ 标记带账号 id，用量侧校验不上就不算覆写。
// 不带这一道，前一个号注入过的读数会落到后一个号的用量行上。
func TestGatewayPoolAppliedMarkerIsScopedToAccount(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := fake.account(1)
	applied := attachRouteApplied(t, &svc.codexCookies, acct, gwpoolTestURL, http.Header{})
	require.True(t, applied.Cookie != "")

	failedOver := codexCookieTestAccount(99, AccountTypeOAuth)
	_, fromPool, gateway, version := svc.routePairInUse(failedOver, http.Header{}, applied)
	require.False(t, fromPool, "换号重试之后，前一个号的注入读数不许记到这一行上")
	require.Empty(t, gateway)
	require.Empty(t, version)
}

// ---------------------------------------------------------------------------
// pair 续期回传（2026-10-02）
// ---------------------------------------------------------------------------
//
// 实测前提（docs/conventions/codex-full-strength-tickets.md）：两件齐发时上游什么都不回；
// 重启后内存账本是空的，但落库的落点记录还在 ⇒ exclude 必须从它补回来。
//
// 现场（2026-10-02）：池子对同一个号说「45 个候选网关都还在 4h 冷却里」，而我们这一发的
// gwpool_pair_taken 打的是 excluded=6 —— 那 6 是重启之后重新数起来的，于是烧过的落点被
// 原样发回来，而业务请求落上去就是降智。
func TestGatewayPoolSeedsExcludeFromThePersistedLandingRecord(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	store := &openAICodexCookieStore{} // 全新进程：poolUsed 是空的
	acct := fake.account(1)
	acct.Extra[openAIGatewayHistoryExtraKey] = map[string]any{
		"seen": map[string]any{
			"unified-167": map[string]any{"at": time.Now().Add(-10 * time.Minute).Format(time.RFC3339Nano)},
			// 出了本地初始窗口（默认 1 小时）⇒ 不该补进来。
			"unified-84": map[string]any{"at": time.Now().Add(-5 * time.Hour).Format(time.RFC3339Nano)},
		},
	}

	headers := http.Header{}
	require.NoError(t, attachRoute(context.Background(), store, acct, gwpoolTestURL, headers))
	require.Equal(t, gwpoolTestCookieQuery+"&exclude=unified-167", fake.nextQuery(t),
		"窗口内那条要补回 exclude；出了窗口的那条不许补")
	require.True(t, store.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-167", time.Hour+time.Minute))
}

// 补回来的时间**只许往后对齐**：这个进程自己刚取的票比落库那条新，不许被旧读数盖回去
// （盖回去会让「刚烧过」看起来像「一小时前烧的」，而挑落点正是按这个时间排序轮转的）。
func TestGatewayPoolHydrateNeverRewindsAFresherLocalEntry(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	store := &openAICodexCookieStore{}
	store.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-167") // 刚刚
	acct := fake.account(1)
	acct.Extra[openAIGatewayHistoryExtraKey] = map[string]any{
		"seen": map[string]any{
			"unified-167": map[string]any{"at": time.Now().Add(-3 * time.Hour).Format(time.RFC3339Nano)},
		},
	}

	store.gatewayPoolHydrateUsed(acct, gwpoolTestIdentity)
	at, used := store.gatewayPoolUsedAt(gwpoolTestIdentity, "unified-167", openAIGatewayPoolGatewayWindow)
	require.True(t, used)
	require.WithinDuration(t, time.Now(), at, time.Minute, "内存里那条更新，不许被落库的旧读数盖掉")
}
