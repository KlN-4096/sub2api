package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

func TestGatewayPoolProbeModelOnlyOverridesProbeRequests(t *testing.T) {
	for _, selected := range []string{"", "gpt-6-astra", "gpt-6-sol", "gpt-6-luna", "business"} {
		t.Run("selection="+selected, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			account.Credentials["access_token"] = "offline-test-token"
			if selected != "" {
				account.Extra[openAIGatewayPoolProbeModelExtraKey] = selected
			}
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
				{status: http.StatusOK, minted: "probe-state"},
				{status: http.StatusOK},
				{status: http.StatusOK},
			}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
			require.NoError(t, err)
			req.Header.Set(openAICodexTurnStateHeader, "business-state")
			ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)
			resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", account)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Len(t, upstream.sentBodies, 3, "only selected-model mint/echo and the unchanged business request")
			want := selected
			if want == "" {
				want = "gpt-6-luna"
			}
			if want == "business" {
				want = "gpt-6-astra"
			}
			require.Equal(t, want, gjson.Get(upstream.sentBodies[0], "model").String())
			require.Equal(t, want, gjson.Get(upstream.sentBodies[1], "model").String())
			require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[2])
			require.Equal(t, []string{"", "probe-state", "business-state"}, upstream.sentState,
				"probe state never leaks into the business model")
		})
	}
}

// 预热（openai_gwpool_warm.go）。一律打假池子 + 假 shooter，**绝不打真实上游**。

// gwpoolWarmShot 记一发垫话送出去的 cookie 与 state，以及假上游的回应。
type gwpoolWarmShot struct {
	cookie string
	state  string
}

// gwpoolWarmReply 是假上游对某一发垫话的回应：状态码 + 它下发的 state（空 = 不下发）。
type gwpoolWarmReply struct {
	status int
	minted string
	err    error
}

// gwpoolWarmShooter 按顺序给出每一发的回应；replies 用完之后按「满血」回
// （A 下发一张票、B 不回新的），这样默认形态是最省口舌的那条路。
type gwpoolWarmShooter struct {
	replies []gwpoolWarmReply
	shots   []gwpoolWarmShot
}

func (g *gwpoolWarmShooter) shoot(_ context.Context, cookie, state string) (int, string, error) {
	g.shots = append(g.shots, gwpoolWarmShot{cookie: cookie, state: state})
	if n := len(g.shots) - 1; n < len(g.replies) {
		r := g.replies[n]
		return r.status, r.minted, r.err
	}
	if state == "" {
		return http.StatusOK, gwpoolEchoFreshTicket, nil // A：下发一张
	}
	return http.StatusOK, "", nil // B：不回新的 = 满血
}

// gwpoolWarmModel 是这组用例里的本轮模型（= gwpoolEchoBody1 里那个）。
const gwpoolWarmModel = "gpt-6-astra"

// gwpoolWarmRun 跑一次预热。请求形态照业务请求（有 GetBody、带 model），sink 挂上。
func gwpoolWarmRun(t *testing.T, svc *OpenAIGatewayService, acct *Account, shooter *gwpoolWarmShooter) error {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), ginCtx)
	return svc.gatewayPoolWarmUpWith(req.WithContext(ctx), acct,
		gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot)
}

// gwpoolWarmAccount 是配好假池子的账号（预热无条件跑，没有档位可开）。
func gwpoolWarmAccount(fake *gwpoolFakePool) *Account {
	return fake.account(1)
}

// 手里那张票**验过满血**而且还 Live ⇒ 一发垫话都不打。
//
// 这条是整个 queue 档的成本前提：窗口内连打多发都满血（2026-10-02 实测），所以判据只在
// 换票那一刻跑一次，摊到整个窗口里接近零。少了它，每一发业务请求前都要白烧两发。
func TestWarmUpSpendsNothingOnAVerifiedLivePair(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := gwpoolWarmAccount(fake)
	shooter := &gwpoolWarmShooter{}

	// 第一发：缓存里什么都没有 ⇒ 取票 + 验一次（假上游默认不下发新 state = 满血）。
	require.NoError(t, gwpoolWarmRun(t, svc, acct, shooter))
	require.Len(t, shooter.shots, 2, "一组判据恰好两发")
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity))

	// 第二发走**生产那条快路**（gatewayPoolWarmUp，不是注入 shooter 的那个）：票验过 + 还 Live
	// ⇒ 一发都不打、也不再问池子。
	//
	// 压缩请求从 sink 读取模型；快路必须匹配验证模型，不能沿用其它模型的判据。
	before := fake.hits.Load()
	acct.Extra[openAIGatewayPoolProbeModelExtraKey] = gwpoolWarmModel
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("\x28\xb5\x2f\xfd not json"))
	require.NoError(t, err)
	ctx, sink := withOpenAIGatewayPoolSink(req.Context(), nil)
	sink.noteModel(gwpoolWarmModel)
	require.NoError(t, svc.gatewayPoolWarmUp(req.WithContext(ctx), "", acct),
		"同模型验过 + Live ⇒ 快路直接放行")
	require.Empty(t, upstream.sentBodies, "窗口内不许再验")
	require.Equal(t, before, fake.hits.Load(), "窗口内不许再取票")
}

// Live **不等于**验过：取票那一刻写的 `until = now + valid_for_s` 让票天生是 Live 的，
// 所以快路必须同时判「验过」—— 只判 Live 的话有三条路会拿一张从没验过的票直接放行业务请求
// （并发取票的跟随者、克隆行共用身份、以及 retry/off 档留下的票），queue 档的承诺当场破掉。
func TestWarmUpStillProbesALivePairThatWasNeverVerified(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := gwpoolWarmAccount(fake)

	// 别人（并发取票的跟随者 / 克隆行 / retry 与 off 档）留下的一张 Live 票，没人验过。
	require.NoError(t, attachRoute(context.Background(), &svc.codexCookies, acct, gwpoolTestURL, http.Header{}))
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.False(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity),
		"Live ≠ 验过 —— 快路判的是这个布尔值，它为真就一发不打直接放行")

	// 预热照样跑一组判据（而不是看见 Live 就放行）。
	shooter := &gwpoolWarmShooter{}
	require.NoError(t, gwpoolWarmRun(t, svc, acct, shooter))
	require.Len(t, shooter.shots, 2, "没验过的 Live 票必须验")
	require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity))
	require.Equal(t, int64(1), fake.hits.Load(), "复用手上那张，不许为了验再取一张")
}

// 判据本体：A 只带 cookie 拿一张 state，B 带 cookie + 那张 state。
func TestWarmUpRunsStateEchoWithTheSamePair(t *testing.T) {
	cookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, cookie, 150)
	svc := &OpenAIGatewayService{}
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{
		{status: http.StatusOK, minted: gwpoolEchoFreshTicket}, // A 下发一张
		{status: http.StatusOK},                                // B 不回新的 = 满血
	}}

	require.NoError(t, gwpoolWarmRun(t, svc, gwpoolWarmAccount(fake), shooter))
	require.Len(t, shooter.shots, 2)
	require.Empty(t, shooter.shots[0].state, "A 不许带 state —— 带了就不是取票而是回声")
	require.Equal(t, gwpoolEchoFreshTicket, shooter.shots[1].state, "B 必须回送 A 拿到的那一张")
	for i, shot := range shooter.shots {
		require.Equal(t, cookie, shot.cookie, "两发必须用同一张 pair（第 %d 发）", i+1)
		require.Equal(t, "unified-142", openAICodexRouteGateway(shot.cookie),
			"绝不许摘掉 __oailb：摘了就漂回自己那个大区的网关")
	}
}

// 判成降智 ⇒ 标 Stale + 记账本 ⇒ 下一张票换网关；换到满血的那张才放业务请求进去。
func TestWarmUpRotatesPastADegradedGatewayAndServesTheFullOne(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.forceCookie = gwpoolTestPairCookie(t, "unified-84") // force=1 取到的那张落在另一个网关
	svc := &OpenAIGatewayService{}
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{
		{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
		{status: http.StatusOK, minted: "fake-reminted-ticket"}, // 回了一张**不同的** = 降智
		{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
		{status: http.StatusOK}, // 不回新的 = 满血
	}}

	require.NoError(t, gwpoolWarmRun(t, svc, gwpoolWarmAccount(fake), shooter))
	require.Len(t, shooter.shots, 4, "两张票各两发")
	require.Equal(t, "unified-142", openAICodexRouteGateway(shooter.shots[0].cookie))
	require.Equal(t, "unified-84", openAICodexRouteGateway(shooter.shots[2].cookie),
		"第二张票必须落在另一个网关上")

	// 换票必须带 force=1 + 点名排除被判死那一张，否则池子可能把同一个落点再发回来。
	require.NotContains(t, fake.nextQuery(t), "force=1", "第一次取票是常规取票")
	forced := fake.nextQuery(t)
	require.Contains(t, forced, "force=1")
	require.Contains(t, forced, "exclude_versions=tkt-1")

	// 缓存里留下的是满血那张 ⇒ 紧接着业务请求那一发原样复用它。
	pair, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.Equal(t, "unified-84", pair.gateway)

	// 两个落点都进账本（都真的被碰过了）：下一个窗口挑票时都要排掉。
	burned := svc.codexCookies.gatewayPoolBurnedGateways(gwpoolTestIdentity, openAIGatewayPoolGatewayWindow, 8)
	require.ElementsMatch(t, []string{"unified-142", "unified-84"}, burned)
}

// 试满了也没验出满血 ⇒ **按失败处理，绝不降级放行**（queue 档唯一的承诺）。
func TestWarmUpFailsClosedWhenNoTicketVerifiesFull(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.cookieForHit = func(hit int64) string {
		return gwpoolTestPairCookie(t, fmt.Sprintf("unified-%d", 140+hit))
	}
	svc := &OpenAIGatewayService{}
	shooter := &gwpoolWarmShooter{}
	// 每一发 B 都回一张不同的新票 ⇒ 恒判降智。
	for i := 0; i < gatewayPoolWarmMaxTickets; i++ {
		fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{Name: fmt.Sprintf("unified-%d", 141+i), PairReady: true})
		shooter.replies = append(shooter.replies,
			gwpoolWarmReply{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
			gwpoolWarmReply{status: http.StatusOK, minted: "fake-reminted-ticket"})
	}

	err := gwpoolWarmRun(t, svc, gwpoolWarmAccount(fake), shooter)
	require.ErrorIs(t, err, errOpenAIGatewayPoolWarmExhausted)
	require.NotErrorIs(t, err, errOpenAIGatewayPoolRouteDegraded,
		"和「真判到降智」刻意分开：业务请求一个字节都没出去、被标记的是好几个网关，"+
			"而「稍后重试即可」在这里是最坏的建议（CLI 对 503 自动重发，每次再烧几张票）")
	require.ErrorIs(t, err, gwpool.ErrPool,
		"必须包着 ErrPool：classifyUpstreamTransportError 据此豁免「按代理持久故障停调度」")
	require.Equal(t, gatewayPoolWarmExhaustedClientMsg, gatewayPoolClientMessage(err))
	require.Len(t, shooter.shots, 2*gatewayPoolWarmMaxTickets, "上限是硬的：不许无限试下去")
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.NotEqual(t, openAIGatewayPoolPairLive, state, "不许留一张没验过的票给业务请求")
}

// 非 200 / 传输失败不下质量结论；严格模式阻止业务，不能把无法判断标成降智。
func TestWarmUpBlocksInconclusiveShotsWithoutCallingThemDegraded(t *testing.T) {
	for name, tc := range map[string]struct {
		replies []gwpoolWarmReply
		sent    bool // 这张票有没有确证送达上游（拿到过状态码）
	}{
		"A 被限流": {[]gwpoolWarmReply{{status: http.StatusTooManyRequests}}, true},
		"B 被拒":  {[]gwpoolWarmReply{{status: http.StatusOK, minted: gwpoolEchoFreshTicket}, {status: http.StatusForbidden}}, true},
		"传输失败":  {[]gwpoolWarmReply{{err: errors.New("dial tcp: i/o timeout")}}, false},
	} {
		replies, sent := tc.replies, tc.sent
		t.Run(name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			svc := &OpenAIGatewayService{}
			shooter := &gwpoolWarmShooter{replies: replies}

			account := gwpoolWarmAccount(fake)
			account.Extra[openAIGatewayPoolRecoveryExtraKey] = 0
			err := gwpoolWarmRun(t, svc, account, shooter)
			require.Error(t, err, "严格模式下无法判断必须停止业务出站")
			require.NotErrorIs(t, err, errOpenAIGatewayPoolRouteDegraded, "无法判断不能冒充明确降级")
			require.LessOrEqual(t, len(shooter.shots), 2, "本轮就停，不许换票再试")
			// **不标验过**：业务请求那一发的内嵌判据还要再判一次（退化成 retry 档）。
			require.False(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity),
				"没验出满血就不许标成验过，否则下一发连内嵌判据都跳了")
			// 还不还槽位按「有没有确证送达上游」判，**不能指望业务请求那条路去还**：
			// 那边的 gatewayPoolPair 命中「缓存里还 Live」的早返回 ⇒ 这次调用没向池子取票 ⇒
			// release 恒为 nil ⇒ gatewayPoolReleasesUnsent 在 queue 档上是个空操作。
			_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
			if sent {
				require.Zero(t, fake.releaseHits.Load(), "发出去过的票不许还回共享池")
				// 票也不标坏：确证只是「这一发没下结论」，判坏它会白扔一个落点，
				// 而供给是个位数张/小时。业务请求照常复用它。
				require.Equal(t, openAIGatewayPoolPairLive, state, "没下结论不许判坏这张票")
			} else {
				require.Equal(t, int64(1), fake.releaseHits.Load(), "一个字节都没出去的票要还回池子")
				// 还回去了就不许再留在缓存里：业务请求复用一张已经还给别人的票，
				// 等于两个消费者同时用同一个落点。
				require.NotEqual(t, openAIGatewayPoolPairLive, state, "还掉的票不许还留着 Live")
			}
		})
	}
}

// 预算在**一次 attempt 内部**耗尽 ⇒ 失败关闭，绝不把没验过的票留给业务请求。
//
// 预算挂在 ctx 上，而取票那一步刻意不吃它（WithoutCancel + 自己的 gatewayPoolFetchTimeout）
// ⇒ 循环顶那条检查只挡得住「整轮已超」。池子在冷却时取票能吃满 25 秒，三轮就过线；那时两发
// 垫话在一个已死的 ctx 上立刻报错，看起来和「上游回了个 429」一模一样 —— 按「不下结论就放行」
// 处理的话，手里那张**刚 force 取回来、一发判据都没跑过**的票会被业务请求原样复用。
func TestWarmUpFailsClosedWhenTheBudgetDiesMidAttempt(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := gwpoolWarmAccount(fake)

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)
	// 预算在第一发垫话期间到点：shooter 自己把 ctx 等死，再照传输失败返回（真实形态 ——
	// gatewayPoolWarmShot 的 WithTimeout 挂在一个已死的父 ctx 上，当场就报 DeadlineExceeded）。
	budget, cancel := context.WithCancel(ctx)
	shoot := func(_ context.Context, _, _ string) (int, string, error) {
		cancel()
		return 0, "", context.DeadlineExceeded
	}

	err = svc.gatewayPoolWarmUpWith(req.WithContext(budget), acct,
		gwpoolTestIdentity, gwpoolWarmModel, shoot)
	require.ErrorIs(t, err, context.Canceled, "a cancelled business waiter exits without releasing unverified business")
	require.False(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity),
		"一发判据都没跑完，不许标成验过")
	require.Equal(t, int64(1), fake.hits.Load(), "过线之后不许再取票")
}

// 垫话必须和它要放行的那一发业务请求走**同一条传输层**。
//
// 装了接管 OAuth 出站的插件时，业务请求走插件、裸打 httpUpstream 走的是另一条路（TLS 指纹
// 都不同）—— 那就成了「在 A 上量、给 B 放行」的空闸。把 gatewayPoolWarmShot 里那行
// doOpenAIUpstreamRoundTrip 换成 httpUpstream.Do，整包 209 秒全绿、零失败（2026-10-02 变异
// 实测）⇒ 唯一救它的那一行当时没有任何测试钉住。这条就是那个钉子：路由在场但 runtime 为 nil
// ⇒ RoundTripOpenAIOAuth 回 handled=true + 「插件不可用」⇒ 垫话必须拿到这个错误，
// 而假 httpUpstream 一个字节都不该收到。
func TestWarmShotGoesThroughTheSameTransportAsTheBusinessRequest(t *testing.T) {
	manager := &PluginManager{}
	manager.route.Store(&pluginRoute{pluginID: 1, rolloutPercent: 100, unavailable: "测试不可用"})
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{pluginManager: manager, httpUpstream: upstream}
	acct := &Account{
		ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1,
		Credentials: map[string]any{"access_token": "fake-token"},
	}

	status, state, err := svc.gatewayPoolWarmShot(
		context.Background(), acct, "", gwpoolTestPairCookie(t, "unified-142"), gwpoolWarmModel, "")
	require.Error(t, err, "插件接管着 OAuth 出站，垫话不许绕过它")
	require.Contains(t, err.Error(), "插件不可用")
	require.Zero(t, status)
	require.Empty(t, state)
	require.Empty(t, upstream.sentBodies, "一个字节都不许从另一条传输层出去")
}

// 判据三格：不回新 state / 回下同一张 ⇒ 满血；回一张不同的 ⇒ 降智；其余 ⇒ 不下结论。
func TestWarmProbeVerdicts(t *testing.T) {
	const got = "fake-fresh-ticket"
	for name, tc := range map[string]struct {
		replies          []gwpoolWarmReply
		full, conclusive bool
	}{
		"不回新 state = 满血": {[]gwpoolWarmReply{{status: 200, minted: got}, {status: 200}}, true, true},
		"回下同一张 = 满血":     {[]gwpoolWarmReply{{status: 200, minted: got}, {status: 200, minted: got}}, true, true},
		"回一张不同的 = 降智":    {[]gwpoolWarmReply{{status: 200, minted: got}, {status: 200, minted: "other"}}, false, true},
		"A 非 200":        {[]gwpoolWarmReply{{status: 429, minted: got}}, false, false},
		"A 没回 state":     {[]gwpoolWarmReply{{status: 200}}, false, false},
		"B 非 200":        {[]gwpoolWarmReply{{status: 200, minted: got}, {status: 500, minted: "other"}}, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			shooter := &gwpoolWarmShooter{replies: tc.replies}
			full, conclusive, sent, err := gatewayPoolWarmProbe(
				context.Background(), 1, "unified-142", 1, "ck", shooter.shoot)
			if tc.conclusive {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "retain the failure category for bounded preflight waiting")
			}
			require.Equal(t, tc.conclusive, conclusive)
			require.Equal(t, tc.full, full)
			require.True(t, sent, "拿到过状态码就是确证送达")
		})
	}
}

// 纯传输失败（一个状态码都没拿到）⇒ sent=false ⇒ 槽位要还回池子。
//
// 这条和上面那张表分开：那张表每一格都拿到过状态码。sent 的全部用处就是把「确证烧了」和
// 「一个字节都没出去」分开 —— 判错了要么白扔一个落点（供给个位数张/小时），要么让池子把一张
// 烧过的票当新鲜的再发给别人。
func TestWarmProbeReportsWhetherTheTicketReachedUpstream(t *testing.T) {
	dial := errors.New("dial tcp: i/o timeout")

	// shot A 就拨不通 ⇒ 没送达。
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{{err: dial}}}
	_, conclusive, sent, err := gatewayPoolWarmProbe(
		context.Background(), 1, "unified-142", 1, "ck", shooter.shoot)
	require.ErrorIs(t, err, dial)
	require.False(t, conclusive)
	require.False(t, sent, "一个状态码都没拿到 = 没送达")

	// shot A 已经 200 过、shot B 才拨不通 ⇒ **算送达**：落点已经被碰了，不许还。
	shooter = &gwpoolWarmShooter{replies: []gwpoolWarmReply{
		{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
		{err: dial},
	}}
	_, conclusive, sent, err = gatewayPoolWarmProbe(
		context.Background(), 1, "unified-142", 1, "ck", shooter.shoot)
	require.ErrorIs(t, err, dial)
	require.False(t, conclusive)
	require.True(t, sent, "A 已经打到上游了，窗口真的烧了")
}

// 垫话的模型必须和业务请求一致（state 绑在 账号 × 模型 × 这张票 上）；读不出来就不预热。
func TestGatewayPoolWarmModelReadsTheRequestBody(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	require.Equal(t, "gpt-6-astra", gatewayPoolWarmModel(req))

	// 读不出来的三种：没有 GetBody（body 读一遍就没了）、不是 JSON、没有 model 字段。
	noGetBody, err := http.NewRequest(http.MethodPost, gwpoolTestURL, struct{ *strings.Reader }{strings.NewReader("x")})
	require.NoError(t, err)
	require.Nil(t, noGetBody.GetBody)
	require.Empty(t, gatewayPoolWarmModel(noGetBody))

	for _, body := range []string{"not json", `{}`, `{"model":"   "}`} {
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(body))
		require.NoError(t, err)
		require.Emptyf(t, gatewayPoolWarmModel(req), "body=%s", body)
	}
}

// 预热是**无条件**的：三个删掉的档位键全配成最松那组值，照样跑预热、照样 fail closed。
//
// 2026-10-03 删掉档位之前，这条测的是「别的档一发垫话都不打」。现在反过来钉：没有任何配置
// 能让业务请求绕过预热。读不出 model 这条是最锋利的探针 —— 它是预热**自己**的失败形态
// （errOpenAIGatewayPoolWarmNoModel），别的层产不出来，所以看到它就等于看到预热跑了；
// 而且这一发连票都没取、上游一个请求都没打，断言不依赖任何假出站链。
func TestWarmUpRunsWithoutAnyModeConfigured(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolErrorUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := fake.account(1)
	acct.Extra["openai_gwpool_guard"] = "off"
	acct.Extra["openai_gwpool_state_echo"] = false
	acct.Extra["openai_gwpool_degraded_retries"] = 0
	acct.Extra[openAIGatewayPoolProbeModelExtraKey] = gatewayPoolProbeModelBusiness

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("{}"))
	require.NoError(t, err)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), ginCtx)
	resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", acct)
	require.ErrorIs(t, err, errOpenAIGatewayPoolWarmNoModel)
	require.Nil(t, resp)
	require.Zero(t, upstream.calls, "预热拦下来的请求一个字节都不许出去")
	require.Zero(t, fake.hits.Load(), "也不该向池子取票")
}

// 同一身份的并发预热只跑**一遍**判据。
//
// 取票本身已被 gatewayPoolPair 的 singleflight 收成一次 ⇒ 并发请求手里是同一张票、同一个
// (上游账号 × 网关) 单元、同一个满血窗口 ⇒ 结论必然相同。各自打一遍纯属白烧配额，
// 而池子的供给只有个位数张/小时。
func TestWarmUpCollapsesConcurrentProbesOnTheSameTicket(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := gwpoolWarmAccount(fake)

	var mu sync.Mutex
	shots := 0
	release := make(chan struct{})
	shoot := func(_ context.Context, _, state string) (int, string, error) {
		mu.Lock()
		shots++
		mu.Unlock()
		if state == "" {
			<-release // 卡住 A 发，让后面那几路一定撞进 singleflight
			return http.StatusOK, gwpoolEchoFreshTicket, nil
		}
		return http.StatusOK, "", nil
	}

	const n = 6
	errs := make(chan error, n)
	for range n {
		go func() {
			// goroutine 里**不许用 require**：testify 的 FailNow 是 runtime.Goexit，
			// 在非测试 goroutine 里调用就再也不往 errs 写 ⇒ 下面的接收永久阻塞，
			// 整个包挂到 go test 的总超时。错误一律送回主 goroutine 判。
			req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
			if err != nil {
				errs <- err
				return
			}
			ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)
			errs <- svc.gatewayPoolWarmUpWith(req.WithContext(ctx), acct,
				gwpoolTestIdentity, gwpoolWarmModel, shoot)
		}()
	}
	// 等所有人都进到 A 发（或在 singleflight 里排队）再放行。
	// defer 关闭：Eventually 超时时也要放行，否则 6 个 goroutine 全卡在 <-release 上泄漏。
	closed := false
	defer func() {
		if !closed {
			close(release)
		}
	}()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return shots == 1
	}, 5*time.Second, 5*time.Millisecond, "第一发之外不该有别的发出去")
	close(release)
	closed = true
	for range n {
		require.NoError(t, <-errs)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, shots, "%d 路并发只该打一组判据（两发）", n)
	require.Equal(t, int64(1), fake.hits.Load(), "取票也只该问池子一次")
}

// 代理绑定这道闸必须在垫话**之前**自己查一遍。
//
// 它原本只在 doOpenAIUpstreamOnce 里（requireOpenAIProxyBinding，"last-mile guard"），而预热跑在
// 那之前 ⇒ 代理行没预加载 / 被删 / URL() 为空时（ProxyID 非 nil 而 proxyURL 为空），垫话会带着
// 这个账号的 Bearer 从服务器真实出口直连出去 —— 而出口 IP 就是账号身份的一部分。
func TestWarmUpRefusesToProbeWithoutTheBoundProxy(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := gwpoolWarmAccount(fake)
	proxyID := int64(9)
	acct.ProxyID = &proxyID // 绑了代理，但解析出来的 URL 是空的（代理行没加载 / 被删）

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)

	err = svc.gatewayPoolWarmUp(req.WithContext(ctx), "", acct)
	require.Error(t, err, "没有可用代理时一发垫话都不许出去")
	require.Contains(t, err.Error(), "proxy 9 is unavailable")
	require.Empty(t, upstream.sentBodies, "一个字节都不许从服务器真实出口出去")
	require.Zero(t, fake.hits.Load(), "连票都不该取")
}

// 读不出本轮模型 ⇒ **fail closed**，不许静默退回 retry 档。
//
// 判据的 turn-state 绑在 (账号 × 模型 × 这张票) 上，不知道模型就没法验。悄悄降级会把运营方选的
// 「绝不把降智交给客户端」抹掉，而唯一线索是一条日志。已知触发条件：双开账号（出站体被 zstd
// 压过，裸解 JSON 必然失败）× 0.156 之前的客户端（turn-metadata 头里也不补 model）。
func TestWarmUpFailsClosedWhenTheModelIsUnreadable(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}

	// 压过的体：既不是合法 JSON，头里也没有 turn-metadata。
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("\x28\xb5\x2f\xfd not json"))
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)

	account := gwpoolWarmAccount(fake)
	account.Extra[openAIGatewayPoolProbeModelExtraKey] = gatewayPoolProbeModelBusiness
	err = svc.gatewayPoolWarmUp(req.WithContext(ctx), "", account)
	require.ErrorIs(t, err, errOpenAIGatewayPoolWarmNoModel)
	require.ErrorIs(t, err, gwpool.ErrPool, "必须包着 ErrPool，否则这条会被当成账号故障停调度")
	require.Equal(t, gatewayPoolWarmNoModelClientMsg, gatewayPoolClientMessage(err))
	require.Empty(t, upstream.sentBodies, "不知道模型就一发都不许打")
	require.Zero(t, fake.hits.Load())
}

// 模型的三个来源按可靠度排序：本次请求的网关池 sink（buildUpstreamRequest 在**压缩之前**从明文
// 体里记的）→ turn-metadata 头 → 明文请求体。顺序是承重的：双开账号的出站体是 zstd，裸解必然
// 失败，而 turn-metadata 的 model 只在入站本来就带这个字段时才对齐（0.156 之前的客户端、桥接口
// 过来的请求都没有）—— 只有 sink 那一条是所有经转发入口的出站请求都过的。
func TestGatewayPoolWarmModelPrefersTheSinkOverTheHeaderAndBody(t *testing.T) {
	// 三个来源都在、三个都不一样 ⇒ 取 sink 那个。
	all, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(`{"model":"gpt-5.6-sol"}`))
	require.NoError(t, err)
	all.Header.Set(openAIWSTurnMetadataHeader, `{"model":"gpt-6-luna"}`)
	ctx, sink := withOpenAIGatewayPoolSink(all.Context(), nil)
	sink.noteModel("gpt-6-astra")
	require.Equal(t, "gpt-6-astra", gatewayPoolWarmModel(all.WithContext(ctx)))

	// 没有 sink 读数 ⇒ 读头。主转发入口上 sink **一定**有 model（buildUpstreamRequest 必然在
	// doOpenAIUpstream 之前跑完），所以来源 #1 是常态；后两个来源兜的是桥接口、猎手那类
	// 不经 buildUpstreamRequest 的调用方。双开账号：体是 zstd 字节，头里有本轮 model。
	compressed, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader("\x28\xb5\x2f\xfd zstd"))
	require.NoError(t, err)
	compressed.Header.Set(openAIWSTurnMetadataHeader, `{"model":"gpt-6-astra","turn_started_at_unix_ms":1}`)
	require.Equal(t, "gpt-6-astra", gatewayPoolWarmModel(compressed))

	// 头优先于体：两边不一致时按头（头是出站定稿后同步的那一份）。
	both, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(`{"model":"gpt-5.6-sol"}`))
	require.NoError(t, err)
	both.Header.Set(openAIWSTurnMetadataHeader, `{"model":"gpt-6-astra"}`)
	require.Equal(t, "gpt-6-astra", gatewayPoolWarmModel(both))

	// 头畸形 / 没有 model 字段 ⇒ 回落读体，不许返回空串。
	for _, raw := range []string{"", "not json", "{}", `{"model":"  "}`} {
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
		require.NoError(t, err)
		req.Header.Set(openAIWSTurnMetadataHeader, raw)
		require.Equalf(t, "gpt-6-astra", gatewayPoolWarmModel(req), "header=%q", raw)
	}

	// `{"model":7}`：gjson 会把 JSON 数字强转成 "7"，所以这一格**不会**回落读体。
	// 钉下来是因为模型名永远是字符串，真出现数字说明上游契约变了，不该被静默当成模型名。
	numeric, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	numeric.Header.Set(openAIWSTurnMetadataHeader, `{"model":7}`)
	require.Equal(t, "7", gatewayPoolWarmModel(numeric),
		"gjson 把数字强转成字符串 —— 这一格是已知读数，不是设计意图")
}

// 非推理面（侧信道 GET、/codex/alpha/search）一发垫话都不打，也不打日志。
func TestWarmUpIgnoresNonInferenceRequests(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}

	// 侧信道：GET、没有 body ⇒ 从前会在这里打一条「读不出模型」的 Warn，每发一条。
	req, err := http.NewRequest(http.MethodGet, "https://chatgpt.com/backend-api/codex/alpha/search", nil)
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)

	require.NoError(t, svc.gatewayPoolWarmUp(req.WithContext(ctx), "", gwpoolWarmAccount(fake)))
	require.Empty(t, upstream.sentBodies)
	require.Zero(t, fake.hits.Load())
}

func TestWarmUpBudgetsAreIndependentBetweenBusinessRequests(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	svc := &OpenAIGatewayService{}
	first := svc.gatewayPoolWaitContext(context.Background(), account)
	second := svc.gatewayPoolWaitContext(context.Background(), account)
	state := gatewayPoolWaitFrom(first)
	state.mu.Lock()
	state.deadline = time.Now().Add(-time.Second)
	state.mu.Unlock()
	require.ErrorIs(t, gatewayPoolPreparationDeadlineError(first), errOpenAIGatewayPoolWarmExhausted)
	require.NoError(t, gatewayPoolPreparationDeadlineError(second))
}

func TestWarmUpShorterCallerDeadlineDoesNotStartBusiness(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	stopped := make(chan struct{})
	err := svc.gatewayPoolWarmUpWith(request, fake.account(1), gwpoolTestIdentity, gwpoolWarmModel,
		func(ctx context.Context, _, _ string) (int, string, error) {
			<-ctx.Done()
			close(stopped)
			return 0, "", ctx.Err()
		})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("the last waiter did not cancel its probe")
	}
	require.False(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity))
}

// 试票上限是账号旋钮：缺省 5，越界回缺省，封顶 8。
//
// 它是**供给闸**：每张票烧掉一个 (上游账号 × 网关) 单位，而那个单位的再生预算约 25 张/小时
// （已知网关数 ÷ 4 小时冷却）。做成旋钮是因为供给在涨，合适的值跟着它走。
func TestWarmTicketsIsAnAccountKnobWithACeiling(t *testing.T) {
	tickets := func(raw any) int {
		return (&Account{Extra: map[string]any{openAIGatewayPoolWarmTicketsExtraKey: raw}}).gatewayPoolWarmTickets()
	}
	require.Equal(t, gatewayPoolWarmMaxTickets, (&Account{}).gatewayPoolWarmTickets(), "缺省 = 5")
	require.Equal(t, gatewayPoolWarmMaxTickets, (*Account)(nil).gatewayPoolWarmTickets())
	require.Equal(t, 1, tickets(1))
	require.Equal(t, 3, tickets(3.0), "extra 是 JSONB，从库里读回来是 float64")
	require.Equal(t, 3, tickets("3"), "getExtraInt 认数字字符串，和别的秒旋钮同口径")
	require.Equal(t, gatewayPoolWarmMaxTicketsCeiling, tickets(gatewayPoolWarmMaxTicketsCeiling))
	for _, raw := range []any{0, -1, 99, "nope", true, nil} {
		require.Equalf(t, gatewayPoolWarmMaxTickets, tickets(raw), "越界/畸形值回缺省：%v", raw)
	}
}

// Legacy saved limits cannot truncate the shared candidate queue.
func TestWarmUpIgnoresTheLegacyTicketCount(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.cookieForHit = func(hit int64) string {
		return gwpoolTestPairCookie(t, fmt.Sprintf("unified-%d", 140+hit))
	}
	svc := &OpenAIGatewayService{}
	acct := gwpoolWarmAccount(fake)
	acct.Extra[openAIGatewayPoolWarmTicketsExtraKey] = 2
	// 每一轮都判降智：A 下发一张、B 回一张**不同的**。
	replies := make([]gwpoolWarmReply, 0, 8)
	for i := 0; i < 4; i++ {
		replies = append(replies,
			gwpoolWarmReply{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
			gwpoolWarmReply{status: http.StatusOK, minted: gwpoolEchoFreshTicket + "-b"})
	}
	shooter := &gwpoolWarmShooter{replies: replies}

	require.NoError(t, gwpoolWarmRun(t, svc, acct, shooter))
	require.Len(t, shooter.shots, 10, "continue after four rejected candidates to the fifth full one")
}
