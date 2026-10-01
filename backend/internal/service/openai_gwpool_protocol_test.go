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

// all_cooling / rate_limited / no_exit / upstream_rejected 要按身份退避：退避期内**一个池子请求
// 都不发**（原先一视同仁 ⇒ 客户端的重试环每轮都在池子侧触发一次发现铸票）。
func TestGatewayPoolBacksOffByErrorCode(t *testing.T) {
	cases := []struct {
		name        string
		code        string
		retryAfter  int
		wantBackoff time.Duration
	}{
		{"冷却按池子给的时长", gwpool.CodeAllCooling, 900, 15 * time.Minute},
		{"限流", gwpool.CodeRateLimited, 0, openAIGatewayPoolDefaultBackoff},
		{"出口全熔断", gwpool.CodeNoExit, 0, openAIGatewayPoolDefaultBackoff},
		{"上游否决池子的号", gwpool.CodeUpstreamRejected, 0, openAIGatewayPoolDefaultBackoff},
		// consumer key 配错 / 被吊销：重试一万次也是这个结果，池子按契约给 300s。
		{"池子不受理我们的 key", gwpool.CodeConsumerRejected, 300, 5 * time.Minute},
		// 出站参数不合法 = 我们这边的 bug/配置错。池子刻意不给 retry_after ⇒ 时长必须本地兜，
		// 不然这个码就退化成一个纯热循环。
		{"我们的请求不合法", gwpool.CodeBadRequest, 0, openAIGatewayPoolBadRequestBackoff},
		{"退避时长钳上限", gwpool.CodeAllCooling, int((10 * gwpool.MaxRetryAfter).Seconds()), gwpool.MaxRetryAfter},
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

			remaining := store.gatewayPoolBackoffFor(gwpoolTestIdentity)
			require.Positive(t, remaining)
			require.LessOrEqual(t, remaining, tc.wantBackoff)
			require.Greater(t, remaining, tc.wantBackoff-10*time.Second)
		})
	}
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
			require.Zero(t, store.gatewayPoolBackoffFor(gwpoolTestIdentity))
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
	store.poolPairs.Delete(gwpoolTestIdentity)
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
	_, err = svc.doOpenAIUpstream(req, "", acct)
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
	_, err = svc.doOpenAIUpstream(req, "", acct)
	require.Error(t, err)

	require.Equal(t, `{"cookie_version":"tkt-1"}`, fake.nextRelease(t))
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairNone, state, "还掉的票不许留在缓存里继续出站")
}

// 客户端在取票之后、发送之前就走了 ⇒ 同样还票，而且**一个上游请求都不发**。
func TestGatewayPoolReleasesTicketWhenClientVanishedBeforeSend(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolErrorUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := fake.account(1)

	// ctx 在 AttachRoute 取到票之后被取消：用一个「取完票就自己取消」的 ctx 复现。
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	fake.onCookie = cancel
	defer cancel()

	_, err = svc.doOpenAIUpstream(req, "", acct)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, upstream.calls, "客户端已经走了就不该再打上游")
	require.Equal(t, `{"cookie_version":"tkt-1"}`, fake.nextRelease(t))
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
		resp, err := svc.doOpenAIUpstream(req, "", fake.account(1))
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Zero(t, fake.releaseHits.Load())
	})
	t.Run("传输错误", func(t *testing.T) {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
		svc := &OpenAIGatewayService{httpUpstream: &gwpoolErrorUpstream{}}
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
		require.NoError(t, err)
		_, err = svc.doOpenAIUpstream(req, "", fake.account(1))
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
	_, err = svc.doOpenAIUpstream(req, "", acct)
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
	_, err = svc.doOpenAIUpstream(req, "", fake.account(1))
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
	_, err = svc.doOpenAIUpstream(req, "", fake.account(1))
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
			_, err = svc.doOpenAIUpstream(req, "", fake.account(1))
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
	ctx, sink := withOpenAIGatewayPoolSink(context.Background())
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
// 只送 __cflb 时上游会补发一张新 __cflb（寿命重新拉满 3600s）。所以续期必须摘掉 __oailb，
// 而摘掉就偏离真实 Codex 客户端的报文形状 ⇒ 两道闸把它压到最小：只有**临期票**、
// 只有**取票后的第一发**。

// gwpoolDoUpstream 代演转发入口：给请求 ctx 挂 sink（续期只在挂了 sink 的路径上发生），
// 跑一发真实的 doOpenAIUpstream，把 per-request 标记取回来。
func gwpoolDoUpstream(t *testing.T, svc *OpenAIGatewayService, acct *Account) OpenAIGatewayPoolApplied {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	ctx, sink := withOpenAIGatewayPoolSink(req.Context())
	resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", acct)
	require.NoError(t, err)
	_ = resp.Body.Close()
	result := &OpenAIForwardResult{}
	sink.publish(result)
	return result.GatewayPoolApplied
}

// gwpoolRenewedPair 造一组「上游补发的新两件」：落点仍是同一个网关（__cflb 钉死了路由），
// 只有串变了 —— 续期那一发的常态就是这样。
func gwpoolRenewedPair(t *testing.T, gateway string) (cflb, oailb string) {
	t.Helper()
	// 只改签名段：解出来的落点还是同一个网关，但整串和交付时那张不同 ⇒ 能断言「换成新的了」。
	return "__cflb=renewed-lb", "__oailb=" + routeCookieTestOailb(t, "chat.gateway."+gateway+".api.openai.com") + "x"
}

// 核心口径：临期票 ⇒ 第一发摘 __oailb + 回传；第二发两件齐发（且是**新的**两件）+ 不回传。
// 反向：不翻标记 ⇒ 第二发也会摘，两条断言一起红。
func TestGatewayPoolRenewsOnlyOnFirstRequestAfterTaking(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	fake.pairRemainingS = 600 // 临期（< 12 分钟）⇒ 该续
	cflb, oailb := gwpoolRenewedPair(t, "unified-142")
	upstream := &cookieRecordingUpstream{setCookie: http.Header{"Set-Cookie": []string{
		cflb + "; Path=/", oailb + "; Path=/",
	}}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	store := &svc.codexCookies
	acct := fake.account(1)

	before := time.Now()
	applied := gwpoolDoUpstream(t, svc, acct)
	require.True(t, applied.Renewing, "取票后的第一发该拿到续期名额")
	require.Contains(t, upstream.sentCookies[0], "__cflb=pool-lb", "路由仍由池子那张 __cflb 钉住")
	require.NotContains(t, upstream.sentCookies[0], "__oailb=", "续期那一发必须摘掉 __oailb")
	// 回传的只有「哪张票 + 新的一串 cookie」两个字段：任何满血/降智/质量字段都是越界。
	require.JSONEq(t, `{"cookie_version":"tkt-1","cookie":"`+cflb+"; "+oailb+`"}`, fake.nextRenew(t))

	// 缓存里换成新的两件（下一发才能继续钉在这条路由上），**until 不跟着延长**。
	require.Eventually(t, func() bool {
		cached, state := store.cachedPoolPair(gwpoolTestIdentity)
		return state == openAIGatewayPoolPairLive && cached.cookie == cflb+"; "+oailb
	}, 2*time.Second, 5*time.Millisecond, "续完之后缓存里应当是新的两件")
	cached, _ := store.cachedPoolPair(gwpoolTestIdentity)
	require.WithinDuration(t, before.Add(150*time.Second), cached.until, 2*time.Second,
		"续期延长的是路由寿命，不是满血窗口：到点仍只来自交付时的 valid_for_s")
	require.Equal(t, gwpoolCookieVersion(cflb+"; "+oailb), cached.version,
		"池子换了票号就跟着换（还票/exclude 要用它）；票号 = sha256(cookie)[:12]，消费端复算过")
	require.False(t, cached.renewPending, "名额已经用掉了")

	// 第二发：两件齐发（和真实客户端一致）、不再回传。
	second := gwpoolDoUpstream(t, svc, acct)
	require.False(t, second.Renewing, "同一张票只续一次")
	require.Contains(t, upstream.sentCookies[1], cflb)
	require.Contains(t, upstream.sentCookies[1], oailb)
	require.EqualValues(t, 1, fake.renewHits.Load(), "第二发不许再回传")
	require.EqualValues(t, 1, fake.hits.Load(), "满血窗口内不该再取票")
}

// 12 分钟闸：常态下（池子刚铸的票剩 50 多分钟）一发都不该续 —— 续一次就把 __cflb 拉满 3600s，
// 还剩那么久的票续了没有任何增量，而每一发摘 __oailb 都在偏离真实客户端的报文形状。
// 字段缺失（老池子）同样按不续处理：少续一张只是回到今天的行为，错续是白改报文形状。
func TestGatewayPoolRenewsOnlyNearExpiryPairs(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remaining int
	}{
		{name: "剩 50 分钟", remaining: 3000},
		{name: "池子没报这个字段", remaining: 0},
		// 阈值本身不算临期（12 分钟整 = 720s）。
		{name: "正好等于阈值", remaining: int(openAIGatewayPoolRenewBelow.Seconds())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			poolCookie := gwpoolTestPairCookie(t, "unified-142")
			fake := newGwpoolFakePool(t, poolCookie, 150)
			fake.pairRemainingS = tc.remaining
			cflb, oailb := gwpoolRenewedPair(t, "unified-142")
			upstream := &cookieRecordingUpstream{setCookie: http.Header{"Set-Cookie": []string{
				cflb + "; Path=/", oailb + "; Path=/",
			}}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			acct := fake.account(1)

			applied := gwpoolDoUpstream(t, svc, acct)
			require.False(t, applied.Renewing)
			require.Equal(t, poolCookie, upstream.sentCookies[0], "两件照旧齐发")
			require.Never(t, func() bool { return fake.renewHits.Load() > 0 },
				300*time.Millisecond, 20*time.Millisecond, "不临期 ⇒ 一发都不许回传")
			// 没续 ⇒ 缓存里仍是交付时那张（上游下发的那组不许被当成续期结果吃进去）。
			cached, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
			require.Equal(t, poolCookie, cached.cookie)
		})
	}
}

// 标记跟着缓存里那张 pair 存 ⇒ 取到新票就重新置位，新票的第一发又续一次。
// 用全局状态实现的话这一条会红（第一张票用掉之后就再没人续了）。
func TestGatewayPoolRearmsRenewForEveryNewTicket(t *testing.T) {
	first := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, first, 1) // valid_for_s=1 ⇒ 下一发重新取票
	fake.forceCookie = gwpoolTestPairCookie(t, "unified-84")
	fake.pairRemainingS = 600
	cflb, oailb := gwpoolRenewedPair(t, "unified-142")
	upstream := &cookieRecordingUpstream{setCookie: http.Header{"Set-Cookie": []string{
		cflb + "; Path=/", oailb + "; Path=/",
	}}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := fake.account(1)

	require.True(t, gwpoolDoUpstream(t, svc, acct).Renewing, "第一张票的第一发")
	require.Contains(t, fake.nextRenew(t), `"cookie_version":"tkt-1"`)

	time.Sleep(1100 * time.Millisecond) // 过 valid_for_s ⇒ 换票

	// 第二张票落在另一个网关上 ⇒ 上游补发的 __oailb 也指向那个网关（落点闸要的就是这个一致性，
	// 见 gatewayPoolRenew：解出来的落点和交付时说的不一样就不续）。
	cflb84, oailb84 := gwpoolRenewedPair(t, "unified-84")
	upstream.setCookie = http.Header{"Set-Cookie": []string{cflb84 + "; Path=/", oailb84 + "; Path=/"}}

	require.True(t, gwpoolDoUpstream(t, svc, acct).Renewing, "新票要重新置位续期名额")
	require.NotContains(t, upstream.sentCookies[1], "__oailb=", "新票的第一发同样摘掉 __oailb")
	require.Contains(t, fake.nextRenew(t), `"cookie_version":"tkt-2"`, "续的是新票")
	require.EqualValues(t, 2, fake.renewHits.Load())
}

// 上游没补一组**完整的**新两件就什么都不做：缺 __cflb = 这一发没续到（回传旧的会让池子以为
// 续上了）；缺 __oailb = 凑不出一张完整的票，而「新 __cflb + 旧 __oailb」混着用是刻意不做的。
// 两种情况下手里那张都还能用 ⇒ 缓存也不动，后续请求照旧带它。
func TestGatewayPoolDoesNotRenewWithoutCompleteFreshPair(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	cflb, oailb := gwpoolRenewedPair(t, "unified-142")
	for _, tc := range []struct {
		name   string
		header http.Header
	}{
		{name: "上游什么都没下发", header: http.Header{}},
		{name: "只补了新 __cflb", header: http.Header{"Set-Cookie": []string{cflb + "; Path=/"}}},
		{name: "只补了新 __oailb", header: http.Header{"Set-Cookie": []string{oailb + "; Path=/"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, poolCookie, 150)
			fake.pairRemainingS = 600
			upstream := &cookieRecordingUpstream{setCookie: tc.header}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			acct := fake.account(1)

			require.True(t, gwpoolDoUpstream(t, svc, acct).Renewing, "名额照样消耗掉（这一发已经摘了）")
			require.Never(t, func() bool { return fake.renewHits.Load() > 0 },
				300*time.Millisecond, 20*time.Millisecond, "没有完整的新两件 ⇒ 不回传")

			cached, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
			require.Equal(t, poolCookie, cached.cookie, "缓存不动：手里那张并没死")
			gwpoolDoUpstream(t, svc, acct)
			require.Equal(t, poolCookie, upstream.sentCookies[1], "第二发带的还是池子交付的那张，两件齐发")
		})
	}
}

// 回传失败（池子 500）：业务请求不受影响，本地**照样**换成新的两件（新 __cflb 是上游刚发的、
// 比手里那张新，而混着送是刻意不做的），只有票号不换 —— 池子没收下，它那边还是旧票号。
func TestGatewayPoolRenewFailureStillSwapsLocalPair(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	fake.pairRemainingS = 600
	fake.renewStatus = http.StatusInternalServerError
	cflb, oailb := gwpoolRenewedPair(t, "unified-142")
	upstream := &cookieRecordingUpstream{setCookie: http.Header{"Set-Cookie": []string{
		cflb + "; Path=/", oailb + "; Path=/",
	}}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := fake.account(1)

	require.True(t, gwpoolDoUpstream(t, svc, acct).Renewing)
	_ = fake.nextRenew(t)
	require.Eventually(t, func() bool {
		cached, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
		return cached.cookie == cflb+"; "+oailb
	}, 2*time.Second, 5*time.Millisecond, "池子收不收，本地都换成新的两件")
	cached, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, "tkt-1", cached.version, "池子没收下 ⇒ 票号不换")

	second := gwpoolDoUpstream(t, svc, acct)
	require.False(t, second.Renewing)
	require.Contains(t, upstream.sentCookies[1], cflb)
	require.Contains(t, upstream.sentCookies[1], oailb)
	require.EqualValues(t, 1, fake.renewHits.Load(), "失败不重试：重试不会让上游再发一张新的")
}

// 落点闸：`__cflb` 被上游无视时（亲和目标被摘 / 票已到点）它按地理重新分配 ⇒ 新 __oailb 里是
// 另一个网关。这时候**三件事**都要对：不回传（池子的完整性闸必然拒）、把缓存里那张删掉
// （留着就是「gateway=142 而 cookie 落 126」的自相矛盾状态，窗口内后续每一发都按 126 出站）、
// 把**真实落点**记进本地 4h 账本（实烧的是 126，不记的话这个号以后会要一张 126 的票、
// 以为自己没碰过，拿到的是降智票）。
func TestGatewayPoolRerouteSkipsRenewAndBooksRealGateway(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	fake.pairRemainingS = 600
	// 上游无视了 __cflb：新 __oailb 指向 126。
	stray := "__oailb=" + routeCookieTestOailb(t, "chat.gateway.unified-126.api.openai.com")
	svc := &OpenAIGatewayService{httpUpstream: &cookieRecordingUpstream{setCookie: http.Header{
		"Set-Cookie": []string{"__cflb=stray-lb; Path=/", stray + "; Path=/"},
	}}}
	acct := fake.account(1)

	require.True(t, gwpoolDoUpstream(t, svc, acct).Renewing, "名额照样消耗掉（这一发已经摘了）")

	require.Never(t, func() bool { return fake.renewHits.Load() > 0 },
		300*time.Millisecond, 20*time.Millisecond, "落点不对 ⇒ 不回传（池子必然拒）")
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairNone, state, "被改派的票不许留在缓存里继续出站")
	require.Contains(t, svc.codexCookies.gatewayPoolBurnedGateways(gwpoolTestIdentity, time.Hour, 8),
		"unified-126", "真实落点必须进本地账本：那个槽位是实烧的")
}

// 续期那一发摘掉了 __oailb ⇒ 上游会补发一组 ⇒ 用量侧**有** Set-Cookie 了。那一发的用量行要记
// **上游新下发的那组**（= 真实路由），不是 applied.Cookie（池子交付时那组）。
func TestRoutePairInUseRecordsFreshPairOnRenewal(t *testing.T) {
	svc := &OpenAIGatewayService{}
	acct := codexCookieTestAccount(1, AccountTypeOAuth)
	applied := OpenAIGatewayPoolApplied{
		AccountID: acct.ID, Cookie: gwpoolTestPairCookie(t, "unified-142"),
		Gateway: "unified-142", Version: "tkt-1",
	}

	// 1) 没有 Set-Cookie（两件齐发时的常态）⇒ 记池子那组，网关/票号原样传下去。
	pair, fromPool, promised, version := svc.routePairInUse(acct, http.Header{}, applied)
	require.True(t, fromPool)
	require.Equal(t, applied.Cookie, pair)
	require.Equal(t, "unified-142", promised)
	require.Equal(t, "tkt-1", version)

	// 2) 续期那一发：记的是上游新下发的那组（落点仍是 142 ⇒ 徽标仍是「已覆写」）。
	cflb, oailb := gwpoolRenewedPair(t, "unified-142")
	pair, fromPool, promised, _ = svc.routePairInUse(acct,
		http.Header{"Set-Cookie": []string{cflb + "; Path=/", oailb + "; Path=/"}}, applied)
	require.True(t, fromPool)
	require.Equal(t, cflb+"; "+oailb, pair, "用量行要记上游新下发的那组，不是交付时那组")
	require.Equal(t, promised, openAICodexRouteGateway(pair), "落点没变 ⇒ 不是被改派")

	// 3) 真漂移：新 __oailb 指向别的网关 ⇒ 落点 != 交付的网关 ⇒ 被改派。
	strayOailb := "__oailb=" + routeCookieTestOailb(t, "chat.gateway.unified-126.api.openai.com")
	pair, _, promised, _ = svc.routePairInUse(acct,
		http.Header{"Set-Cookie": []string{"__cflb=stray; Path=/", strayOailb + "; Path=/"}}, applied)
	require.Equal(t, "__cflb=stray; "+strayOailb, pair)
	require.Equal(t, "unified-126", openAICodexRouteGateway(pair))
	require.NotEqual(t, promised, openAICodexRouteGateway(pair))
}
