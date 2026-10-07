package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

// state-echo 降智判据（openai_gwpool_state_echo.go）。这组用例一律打 httptest 假池子 + 假上游，
// **绝不打真实上游**。
//
// 票的字面量全是明显的占位串（"fake-live-ticket" 之类）：判据只做字符串比较，真实 blob 一个字都
// 不该出现在测试里。

// gwpoolEchoReply 是假上游对某一发的回应：状态码 + 它在响应头里下发的 turn-state（空 = 不下发）。
type gwpoolEchoReply struct {
	status    int
	minted    string
	requestID string
}

// gwpoolEchoBody 记下响应体有没有被关掉：判到降智时必须当场关闭（让 HTTP/2 发 RST_STREAM，
// 上游立刻停止生成），而且**一个字节都不读**。
type gwpoolEchoBody struct {
	closed bool
	reads  int
}

func (b *gwpoolEchoBody) Read([]byte) (int, error) { b.reads++; return 0, io.EOF }
func (b *gwpoolEchoBody) Close() error             { b.closed = true; return nil }

// gwpoolEchoUpstream 按顺序给出每一发的回应，并记下每一发实际送出去的 turn-state、Cookie 与
// 请求体 —— 换票重发必须把同一个请求体一字不差地再发一遍。
type gwpoolEchoUpstream struct {
	replies     []gwpoolEchoReply
	sentState   []string
	sentCookies []string
	sentBodies  []string
	bodies      []*gwpoolEchoBody
	beforeReply func(*http.Request, int) error
}

func (u *gwpoolEchoUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.sentState = append(u.sentState, req.Header.Get(openAICodexTurnStateHeader))
	u.sentCookies = append(u.sentCookies, req.Header.Get("Cookie"))
	sent := ""
	if req.Body != nil {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		sent = string(raw)
	}
	u.sentBodies = append(u.sentBodies, sent)
	if u.beforeReply != nil {
		if err := u.beforeReply(req, len(u.sentBodies)); err != nil {
			return nil, err
		}
	}

	reply := gwpoolEchoReply{status: http.StatusOK}
	if n := len(u.sentBodies) - 1; n < len(u.replies) {
		reply = u.replies[n]
	}
	body := &gwpoolEchoBody{}
	u.bodies = append(u.bodies, body)
	resp := &http.Response{StatusCode: reply.status, Header: http.Header{}, Body: body}
	if reply.minted != "" {
		resp.Header.Set(openAICodexTurnStateHeader, reply.minted)
	}
	if reply.requestID != "" {
		resp.Header.Set("x-request-id", reply.requestID)
	}
	return resp, nil
}

func (u *gwpoolEchoUpstream) DoWithTLS(
	req *http.Request, proxyURL string, id int64, c int, _ *tlsfingerprint.Profile,
) (*http.Response, error) {
	return u.Do(req, proxyURL, id, c)
}

const (
	gwpoolEchoLiveTicket  = "fake-live-ticket"
	gwpoolEchoFreshTicket = "fake-fresh-ticket"
	gwpoolEchoBody1       = `{"model":"gpt-6-astra","input":"x"}`
)

// gwpoolEchoRun 跑一发 doOpenAIUpstream：挂 sink（判据与注入标记都挂在它上面）、按真客户端形态
// 带上客户端回带的那张票。sent 为空 = 这一发没送票。
func gwpoolEchoRun(t *testing.T, svc *OpenAIGatewayService, acct *Account, sent string) (*gin.Context, *http.Response, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	require.NotNil(t, req.GetBody, "bytes/strings reader 造出来的请求必须有 GetBody，否则重放做不了")
	if sent != "" {
		req.Header.Set(openAICodexTurnStateHeader, sent)
	}
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), ginCtx)
	gwpoolEchoSeedVerified(t, svc, acct)
	resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", acct)
	return ginCtx, resp, err
}

// gwpoolEchoAccount 造一个配好假池子的账号。
func gwpoolEchoAccount(fake *gwpoolFakePool) *Account {
	return fake.account(1)
}

// gwpoolEchoSeedVerified 先按正常路径取一张票，再把它标成「已验满血」。
//
// 降智防护的档位 2026-10-03 删了 ⇒ doOpenAIUpstream 一律先跑预热。而这个文件验的是**业务响应**
// 上的判据，不是预热：不先塞这一张的话，每个用例都要把两发垫话的回应也排进 replies 里，
// sentBodies 的下标全要跟着挪，而那些断言本来就是在数业务请求。
//
// 「已验满血 + 还 Live」正是预热的快路条件（gatewayPoolWarmUp 的早返回），一发垫话都不打。
// 刻意走真实取票而不是手搓一个 pair：网关名、票号、cookie 都得和假池子发的那张对得上。
func gwpoolEchoSeedVerified(t *testing.T, svc *OpenAIGatewayService, acct *Account) {
	t.Helper()
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
	identity, err := svc.codexCookies.gatewayPoolIdentity(ctx, acct)
	if err != nil {
		return // 解不出身份 ⇒ 预热那条路自己也会在这里返回，没什么要塞的
	}
	if _, state := svc.codexCookies.cachedPoolPair(identity); state != openAIGatewayPoolPairLive {
		// release 刻意丢掉：这张票要留在缓存里给紧接着那一发业务请求用，还回去就白取了。
		if _, err := svc.codexCookies.AttachRoute(ctx, acct, gwpoolTestURL, http.Header{}); err != nil {
			return // 本来就不接管（没配池子）⇒ 预热也不会跑
		}
	}
	pair, _ := svc.codexCookies.cachedPoolPair(identity)
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, pair.version)
}

// ---------------------------------------------------------------------------
// 判据三格 + 非 200
// ---------------------------------------------------------------------------

// 送了票 + 上游回了一张**不同的**新票 ⇒ 截断这一发，并把当前 pair 标 Stale（下一发换网关）。
// retries=0 = 只截断那一档。
func TestStateEchoDegradedTruncatesAndMarksPairStale(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	// 一次业务刷新后连续三次确认均刷新，不交付原业务。
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
		{status: http.StatusOK, minted: "confirm-2"},
		{status: http.StatusOK, minted: "confirm-3"},
		{status: http.StatusOK, minted: "confirm-4"},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := gwpoolEchoAccount(fake)

	gwpoolEchoSeedVerified(t, svc, acct)
	mark, ok := svc.codexCookies.gatewayPoolVerifiedMarkOf(gwpoolTestIdentity)
	require.True(t, ok)
	mark.at = time.Now().Add(-75 * time.Second)
	svc.codexCookies.poolVerified.Store(gwpoolTestIdentity, mark)

	ginCtx, resp, err := gwpoolEchoRun(t, svc, acct, gwpoolEchoLiveTicket)
	require.Nil(t, resp, "截断不许把降智的响应交给调用方")
	require.Error(t, err)
	require.ErrorIs(t, err, gwpool.ErrPool,
		"必须包着 ErrPool：classifyUpstreamTransportError 据此豁免「按代理持久故障停调度 10 分钟」")
	require.ErrorIs(t, err, errOpenAIGatewayPoolRouteDegraded)

	require.Len(t, upstream.sentBodies, 4, "一次业务 + 三次最小确认，不重发业务")
	require.True(t, upstream.bodies[0].closed)
	require.Zero(t, upstream.bodies[0].reads)
	last := len(upstream.bodies) - 1
	require.True(t, upstream.bodies[last].closed, "丢弃的响应体必须当场关掉")
	require.Zero(t, upstream.bodies[last].reads, "判据只读响应头，一个字节的响应体都不许读")

	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairStale, state,
		"标 Stale 而不是删：删掉之后下一发不带 force / exclude_versions，池子会把烧过的那张原样发回来")
	cached, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, "tkt-1", cached.version, "票号要留着，它就是 exclude_versions 的内容")

	// 本地账本按上游账号收敛记一笔（不按 sub2api 的账号行）。
	require.True(t, svc.codexCookies.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-142", time.Hour))

	// 丢弃的那一发留了一条可审计读数，取走即清。
	discarded := takeDiscardedOpenAIGatewayPoolAttempts(ginCtx)
	require.Len(t, discarded, 1)
	require.Equal(t, "unified-142", discarded[0].Applied.Gateway)
	require.InDelta(t, 75000, discarded[0].Applied.FullHeldMs, 3000,
		"丢弃行必须携带这一张票刚量出的时长，不能发布计算前的旧快照")
	require.Empty(t, takeDiscardedOpenAIGatewayPoolAttempts(ginCtx), "读数取走即清，不许落重复行")
}

func TestGatewayPoolSinkPublishesAttemptTimingWithoutLeakingToNextGateway(t *testing.T) {
	sink := &openAIGatewayPoolSink{}
	sink.notePoolCounts(50, 12)
	sink.mark(OpenAIGatewayPoolApplied{Gateway: "unified-142", Version: "first"})
	firstAttempt := sink.snapshot()
	sink.noteFullHeld(firstAttempt, 75*time.Second)
	var first OpenAIForwardResult
	sink.publish(&first)
	require.EqualValues(t, 75000, first.GatewayPoolApplied.FullHeldMs)
	require.Equal(t, 50, first.GatewayPoolApplied.PoolLive)
	require.Equal(t, 12, first.GatewayPoolApplied.PoolFree)

	sink.mark(OpenAIGatewayPoolApplied{Gateway: "unified-143", Version: "second"})
	sink.noteFullHeld(firstAttempt, 90*time.Second)
	var second OpenAIForwardResult
	sink.publish(&second)
	require.Zero(t, second.GatewayPoolApplied.FullHeldMs,
		"换网关后不许把上一发的满血时长归到新网关")
	require.Zero(t, sink.snapshot().FullHeldMs)
}

// 送了票 + 上游不回新票 ⇒ **满血**，原样透传，什么都不碰。
func TestStateEchoFullStrengthPassesThrough(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := gwpoolEchoAccount(fake)

	ginCtx, resp, err := gwpoolEchoRun(t, svc, acct, gwpoolEchoLiveTicket)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.False(t, upstream.bodies[0].closed, "满血的响应要原样交给调用方，不许替它关掉")
	_ = resp.Body.Close()

	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.Empty(t, takeDiscardedOpenAIGatewayPoolAttempts(ginCtx))
	require.EqualValues(t, 1, fake.hits.Load(), "满血 ⇒ 不换票")
}

// 上游回的那张**和送出去的一样** ⇒ 同样算满血（文档第一节的判据原文）。
func TestStateEchoEchoedSameTicketIsFullStrength(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: gwpoolEchoLiveTicket},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}

	_, resp, err := gwpoolEchoRun(t, svc, gwpoolEchoAccount(fake), gwpoolEchoLiveTicket)
	require.NoError(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()
	require.Len(t, upstream.sentBodies, 1)
}

// 没送票 + 上游回了一张新票 ⇒ **不管**。不带票的请求 87% 都会拿到新铸的一张，那不是回声。
func TestStateEchoWithoutSentTicketIsNotAVerdict(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}

	ginCtx, resp, err := gwpoolEchoRun(t, svc, gwpoolEchoAccount(fake), "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()
	require.Len(t, upstream.sentBodies, 1, "判不出东西就不该换票重发")
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.Empty(t, takeDiscardedOpenAIGatewayPoolAttempts(ginCtx))
}

// 纪律 1：**非 200 带回新票一律不下结论**。作者原版把 429/403 判成满血（响应头里本来就没票），
// 反向这里同样不许判成降智 —— 那是限流/故障，不是路由质量。
func TestStateEchoIgnoresNon200EvenWithFreshTicket(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusBadGateway, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
				{status: status, minted: gwpoolEchoFreshTicket},
			}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}

			ginCtx, resp, err := gwpoolEchoRun(t, svc, gwpoolEchoAccount(fake), gwpoolEchoLiveTicket)
			require.NoError(t, err, "非 200 要原样交回去，由既有错误路径处理")
			require.NotNil(t, resp)
			require.Equal(t, status, resp.StatusCode)
			_ = resp.Body.Close()

			require.Len(t, upstream.sentBodies, 1, "不下结论 ⇒ 不换票、不重发")
			_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
			require.Equal(t, openAIGatewayPoolPairLive, state, "不许把限流当成坏路由")
			require.Empty(t, takeDiscardedOpenAIGatewayPoolAttempts(ginCtx))
		})
	}
}

// ---------------------------------------------------------------------------
// 没有档位了
// ---------------------------------------------------------------------------

// 降智防护**没有开关**：三个老键一起配成最松的那组值，判据照样跑、照样截断。
//
// 这一条钉的是 2026-10-03 那次删档不会被悄悄复活：三个键里任何一个被重新接回读路径，
// 这个用例就红。它们在存量库里是真实存在的值（页面写得出 guard，更早的页面写得出另两个），
// 所以「读到了就关掉防护」是一个**能在现网发生**的回归，不是假想。
func TestGatewayPoolGuardHasNoModesLeft(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	// 旧键不绕过单轮三次确认。
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
		{status: http.StatusOK, minted: "confirm-2"},
		{status: http.StatusOK, minted: "confirm-3"},
		{status: http.StatusOK, minted: "confirm-4"},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	acct := fake.account(1)
	// 删掉的那三个键，全配成「别判、别截断」。
	acct.Extra["openai_gwpool_guard"] = "off"
	acct.Extra["openai_gwpool_state_echo"] = false
	acct.Extra["openai_gwpool_degraded_retries"] = 0

	_, resp, err := gwpoolEchoRun(t, svc, acct, gwpoolEchoLiveTicket)
	require.Nil(t, resp, "死键不许把降智的响应放出去")
	require.ErrorIs(t, err, errOpenAIGatewayPoolRouteDegraded)
	_, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairStale, state, "判死之后照样标 Stale")
}

// 这一发没注入池子那张 pair（非推理面端点）⇒ 判据根本不跑：
// 没有「当前网关」可换，判出来也没有动作可做。
func TestStateEchoSkipsRequestsWithoutAnInjectedPair(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}

	req, err := http.NewRequest(http.MethodPost,
		"https://chatgpt.com/backend-api/codex/alpha/search", strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	req.Header.Set(openAICodexTurnStateHeader, gwpoolEchoLiveTicket)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), ginCtx)

	resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", fake.account(1))
	require.NoError(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()
	require.Zero(t, fake.hits.Load(), "非推理面本来就不取票")
	require.Len(t, upstream.sentBodies, 1)
	require.Empty(t, takeDiscardedOpenAIGatewayPoolAttempts(ginCtx))
}

// 判据绝不接进账号熔断：错误包着 gwpool.ErrPool ⇒ 传输错误分类器不把它当代理持久故障。
// 一次误判（判据有假阳性）不该停掉一个真账号。
func TestStateEchoErrorIsExemptFromAccountEviction(t *testing.T) {
	require.False(t, classifyUpstreamTransportError(errOpenAIGatewayPoolRouteDegraded).Persistent)
	require.True(t, errors.Is(errOpenAIGatewayPoolRouteDegraded, gwpool.ErrPool))
	require.NotContains(t, errOpenAIGatewayPoolRouteDegraded.Error(), gwpoolEchoLiveTicket,
		"错误文案里不许出现票本体")
}

// 判降智不许触发换账号 failover。
//
// 真正的放大系数在 handler 层：换账号上限默认 10，内层 degraded_retries 封顶 1 ⇒
// 不设 Stop 的话一次客户端请求最坏 2×(1+10)=22 发真实上游，各烧一张 pair 和一个
// (上游账号 × 网关) 单位。而降智是**路由**问题，换账号换不出满血路由。
func TestDegradedRouteDoesNotFanOutAcrossAccounts(t *testing.T) {
	degraded := &UpstreamFailoverError{NextAccountAction: NextAccountStop}
	require.False(t, degraded.ShouldRetryNextAccount(),
		"判降智还换账号 ⇒ 放大系数被账号数乘一遍")

	// 对照：普通传输层失败照常换账号，别把这条闸误伤成「所有 502 都不换号」。
	plain := &UpstreamFailoverError{StatusCode: 502}
	require.True(t, plain.ShouldRetryNextAccount())
}

// 标 Stale 认两件事：票号对得上，或者**落点**对得上。
func TestGatewayPoolMarkStaleOnlyTouchesTheNamedTicket(t *testing.T) {
	store := &openAICodexCookieStore{}
	live := openAIGatewayPoolPair{
		cookie: "__cflb=a", gateway: "unified-142", version: "tkt-2",
		until: time.Now().Add(time.Minute),
	}
	store.poolPairs.Store(gwpoolTestIdentity, live)

	// 票号和落点都对不上 ⇒ 缓存里确实是另一张票了，不许动。
	store.gatewayPoolMarkStale(gwpoolTestIdentity, "tkt-1", "unified-999")
	_, state := store.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state, "票号和落点都对不上 ⇒ 不许动")

	store.gatewayPoolMarkStale(gwpoolTestIdentity, "tkt-2", "unified-142")
	cached, state := store.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairStale, state)
	require.Equal(t, "tkt-2", cached.version, "票号要留着做 exclude_versions")

	store.gatewayPoolMarkStale("", "tkt-2", "unified-142") // 空身份：静默返回，不 panic
	require.NotPanics(t, func() { (*openAICodexCookieStore)(nil).gatewayPoolMarkStale("x", "y", "z") })
}

// 票号在手上这一份之后被换掉时，标 Stale 不能静默失效。
//
// 时序：请求 B 读走缓存里那张 pair（tkt-1）之后被判降智，而此时缓存里已经换成了 tkt-2
// （并发重新取票）。只比票号的话 B 这一标就白标了——`until` 不清零、cachedPoolPair 继续判
// Live ⇒ 刚被判死的那条路由在窗口剩余时间里每一发都照走。所以落点对得上也要认。
func TestGatewayPoolMarkStaleFallsBackToGatewayWhenTicketChanged(t *testing.T) {
	store := &openAICodexCookieStore{}
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: "__cflb=a", gateway: "unified-142", version: "tkt-2", // 缓存里已经换成 tkt-2
		until: time.Now().Add(time.Minute),
	})

	// B 手上还是换票前那张 tkt-1，但判死的是 unified-142 这个落点。
	store.gatewayPoolMarkStale(gwpoolTestIdentity, "tkt-1", "unified-142")

	cached, state := store.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairStale, state,
		"换过票号就标不上了 ⇒ 被判死的路由会继续出站")
	require.Equal(t, "tkt-2", cached.version, "要排掉的是缓存里现持的那张票号")
}

// 半程 state-echo 的票龄梯子：越老越信一次刷新。
func TestEchoStrikeLadderByTicketAge(t *testing.T) {
	for _, tc := range []struct {
		age  time.Duration
		want int
	}{
		{0, 3},
		{89 * time.Second, 3},
		{90 * time.Second, 2},
		{139 * time.Second, 2},
		{140 * time.Second, 1},
		{3 * time.Minute, 1},
		{time.Hour, 1},
	} {
		require.Equal(t, tc.want, gatewayPoolEchoStrikes(tc.age), "票龄 %s", tc.age)
	}
	// 上界开口：满血窗口实测是「约 183 秒」，写死 140–180s 的话活得更久的票在 180 秒
	// 之后会掉进一个没定义的格子。开口之后它继续落在最严那一档。
	require.Equal(t, 1, gatewayPoolEchoStrikes(10*time.Minute))
	// 缓存里万一有一张没带 since 的票（零值 ⇒ 票龄巨大）⇒ 落最严那档 = 老行为。
	require.Equal(t, 1, gatewayPoolEchoStrikes(time.Since(time.Time{})))
}

// 丢弃读数的入口在没有 gin 上下文时静默退化（裸结构体单测、WS 之类没挂 sink 的路径）。
func TestDiscardedAttemptsWithoutSinkAreEmpty(t *testing.T) {
	require.Empty(t, takeDiscardedOpenAIGatewayPoolAttempts(nil))
	bare, _ := gin.CreateTestContext(httptest.NewRecorder())
	require.Empty(t, takeDiscardedOpenAIGatewayPoolAttempts(bare))
	_, sink := withOpenAIGatewayPoolSink(context.Background(), nil)
	require.NotNil(t, sink)
	sink.noteDiscarded(OpenAIGatewayPoolDiscardedAttempt{})
	require.Len(t, sink.discarded, 1)
}

// 池子的可交付网关数要搭 Applied 这班车到用量侧，而且**不能被 mark() 覆盖**。
//
// 取清单发生在注入之前（gatewayPoolPick → AttachRoute），所以它只能存在 sink 上、由
// snapshot() 在读的时候合进来。写进 applied 的那种写法会被后来的 mark 整体盖掉，
// 现象是卡片上的分母恒为 0 —— 和「这个号没开 steering」长得一模一样，查不出来。
func TestPoolLiveSurvivesMark(t *testing.T) {
	_, sink := withOpenAIGatewayPoolSink(context.Background(), nil)
	sink.notePoolCounts(62, 5)
	sink.mark(OpenAIGatewayPoolApplied{Gateway: "unified-142", Version: "tkt-1"})
	snap := sink.snapshot()
	require.Equal(t, 62, snap.PoolLive, "mark 把池子清单读数盖掉了")
	require.Equal(t, 5, snap.PoolFree)
	require.Equal(t, "unified-142", snap.Gateway)

	// live<=0 整对不覆盖：列表打不开的那一发该留着上一次问到的，报 0 会说成「池子是空的」。
	sink.notePoolCounts(0, 9)
	require.Equal(t, 62, sink.snapshot().PoolLive)
	require.Equal(t, 5, sink.snapshot().PoolFree, "free 跟着一个没落地的 live 被改掉了")
	// 可交付的全烧过了：free=0 是真的 0，和 live 一起落地。
	sink.notePoolCounts(41, 0)
	require.Equal(t, 41, sink.snapshot().PoolLive)
	require.Zero(t, sink.snapshot().PoolFree)

	// 没挂 sink 的路径静默退化，不 panic。
	require.NotPanics(t, func() { (*openAIGatewayPoolSink)(nil).notePoolCounts(9, 1) })
	require.Zero(t, (*openAIGatewayPoolSink)(nil).snapshot().PoolLive)
}

// 判据的**两个方向**都要留下读数（2026-10-02 加的 Applied.Verdict ⇒ 账号卡片的状态色）。
//
// 账号卡片上「验过是满血」和「没验过」是两回事，而满血那条读数只有这里产出 —— 判据判满血时
// 什么动作都不做，不记的话卡片永远只有「降智」和「空白」两种颜色。
func TestStateEchoRecordsBothVerdictsOnTheAppliedSnapshot(t *testing.T) {
	t.Run("满血", func(t *testing.T) {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
		upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: http.StatusOK}}}
		svc := &OpenAIGatewayService{httpUpstream: upstream}

		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
		require.NoError(t, err)
		req.Header.Set(openAICodexTurnStateHeader, gwpoolEchoLiveTicket)
		ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx, sink := withOpenAIGatewayPoolSink(req.Context(), ginCtx)
		gwpoolEchoSeedVerified(t, svc, fake.account(1))
		resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", fake.account(1))
		require.NoError(t, err)
		require.NotNil(t, resp)
		_ = resp.Body.Close()

		applied := sink.snapshot()
		require.Equal(t, "unified-142", applied.Gateway)
		require.Equal(t, openAIGatewayVerdictFull, applied.Verdict)
	})

	t.Run("降智那一发自己带着降智读数", func(t *testing.T) {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
		// 原业务刷新后，三发确认均刷新才落降级读数。
		upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
			{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
			{status: http.StatusOK, minted: "confirm-2"},
			{status: http.StatusOK, minted: "confirm-3"},
			{status: http.StatusOK, minted: "confirm-4"},
		}}
		svc := &OpenAIGatewayService{httpUpstream: upstream}
		acct := gwpoolEchoAccount(fake)

		ginCtx, resp, err := gwpoolEchoRun(t, svc, acct, gwpoolEchoLiveTicket)
		require.ErrorIs(t, err, errOpenAIGatewayPoolRouteDegraded, "连着三发被刷新就该判死")
		require.Nil(t, resp)

		// 丢弃行按它自己那一刻的快照落库：读数和落点绑在同一份 Applied 上，而一次客户端
		// 请求里可能先后落在好几个落点上（预热一张张试、换号重试），各读各的。
		discarded := takeDiscardedOpenAIGatewayPoolAttempts(ginCtx)
		require.Len(t, discarded, 1)
		require.Equal(t, "unified-142", discarded[0].Applied.Gateway)
		require.Equal(t, openAIGatewayVerdictDegraded, discarded[0].Applied.Verdict)
	})

	t.Run("没下结论就不许留读数", func(t *testing.T) {
		fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
		// 没送票 ⇒ 没有回声 ⇒ 判不出来（纪律：上游对不带票的请求本来就会铸一张新的）。
		upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
			{status: http.StatusOK, minted: gwpoolEchoFreshTicket},
		}}
		svc := &OpenAIGatewayService{httpUpstream: upstream}

		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
		require.NoError(t, err)
		ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx, sink := withOpenAIGatewayPoolSink(req.Context(), ginCtx)
		gwpoolEchoSeedVerified(t, svc, fake.account(1))
		resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", fake.account(1))
		require.NoError(t, err)
		require.NotNil(t, resp)
		_ = resp.Body.Close()
		require.Empty(t, sink.snapshot().Verdict, "判不出来不许写成满血")
	})
}

// noteVerdict 落在别的落点上时不许改读数：换票重试那一圈里 mark 已经指向下一张票了。
func TestSinkVerdictOnlyApplToTheMarkedGateway(t *testing.T) {
	_, sink := withOpenAIGatewayPoolSink(context.Background(), nil)
	sink.mark(OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "unified-84", Cookie: "c", Version: "tkt-2"})

	sink.noteVerdict("unified-142", openAIGatewayVerdictDegraded) // 上一张票的结论，晚到了
	require.Empty(t, sink.snapshot().Verdict, "落点对不上就不许写")

	sink.noteVerdict("", openAIGatewayVerdictFull)
	require.Empty(t, sink.snapshot().Verdict)

	sink.noteVerdict("unified-84", openAIGatewayVerdictFull)
	require.Equal(t, openAIGatewayVerdictFull, sink.snapshot().Verdict)
	require.NotPanics(t, func() { (*openAIGatewayPoolSink)(nil).noteVerdict("x", "full") })
}
