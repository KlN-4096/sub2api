package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 后台预热（openai_gwpool_prewarm.go）。一律打假池子 + 假 shooter，**绝不打真实上游**。

// gwpoolSeedWindow 塞够 n 个满血时长样本，全是同一个值 ⇒ p95 恰好是它。
func gwpoolSeedWindow(store *openAICodexCookieStore, n int, d time.Duration) {
	for range n {
		store.poolFullWindow.observe(d)
	}
}

// gwpoolSeedVerifiedAge 把缓存里放一张 Live 的票，并把「验过满血」那一笔的时刻往前挪 age。
func gwpoolSeedVerifiedAge(store *openAICodexCookieStore, version string, age time.Duration) openAIGatewayPoolPair {
	now := time.Now()
	pair := openAIGatewayPoolPair{
		cookie:  "__cflb=seed; __oailb=seed",
		gateway: "unified-1",
		version: version,
		until:   now.Add(time.Hour),
		since:   now.Add(-age),
	}
	store.poolPairs.Store(gwpoolTestIdentity, pair)
	store.poolVerified.Store(gwpoolTestIdentity,
		gatewayPoolVerifiedMark{version: version, at: now.Add(-age)})
	return pair
}

// 交付租约只有 150s；不能等 p95=200s 的窗口快结束才准备，也不必等历史样本攒齐。
func TestPrewarmUsesTheLiveLeaseBeforeTheHistoricalWindow(t *testing.T) {
	for _, samples := range []int{0, gatewayPoolPrewarmMinSamples} {
		store := &openAICodexCookieStore{}
		gwpoolSeedWindow(store, samples, 200*time.Second)
		pair := gwpoolSeedVerifiedAge(store, "tkt-1", 120*time.Second)
		pair.until = time.Now().Add(30 * time.Second)
		store.poolPairs.Store(gwpoolTestIdentity, pair)
		_, due := store.gatewayPoolPrewarmDue(gwpoolTestIdentity, gwpoolTestAccount(1))
		require.True(t, due, "剩 30s 的 150s 租约应提前准备，历史样本=%d", samples)
	}
}

// 历史窗口更短时，用验证耗时、取票/清单等待和余量决定提前量。
func TestPrewarmUsesHistoryAndVerificationDuration(t *testing.T) {
	const window = 200 * time.Second
	for _, tc := range []struct {
		name     string
		age      time.Duration
		duration time.Duration
		due      bool
	}{
		{"还没到点", 150 * time.Second, 0, false},
		{"正常验证到点", 155 * time.Second, 0, true},
		{"慢验证提前准备", 130 * time.Second, 45 * time.Second, true},
		{"早就过点", window + time.Minute, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &openAICodexCookieStore{}
			gwpoolSeedWindow(store, gatewayPoolPrewarmMinSamples, window)
			gwpoolSeedVerifiedAge(store, "tkt-1", tc.age)
			store.poolWarmDuration.Store(gwpoolTestIdentity, tc.duration)
			_, due := store.gatewayPoolPrewarmDue(gwpoolTestIdentity, gwpoolTestAccount(1))
			require.Equal(t, tc.due, due)
		})
	}
}

// 没验过满血的票不算到点：窗口起点不知道，拿「取票到现在」当票龄会让预热乱开。
func TestPrewarmNeedsAVerifiedPair(t *testing.T) {
	store := &openAICodexCookieStore{}
	gwpoolSeedWindow(store, gatewayPoolPrewarmMinSamples, 200*time.Second)
	now := time.Now()
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		version: "tkt-1", until: now.Add(time.Hour), since: now.Add(-time.Hour),
	})
	_, due := store.gatewayPoolPrewarmDue(gwpoolTestIdentity, gwpoolTestAccount(1))
	require.False(t, due)
}

// **缺省即关**：到点了也不许自己花票，除非显式开了那个键。
//
// 和「续期」同口径（gatewayPoolRenew）：它在没有客户端等着的时候烧票，而供给是这个功能最紧的
// 那根绳子 —— 供给见底的账号开它只会更快打到 all_cooling。
func TestPrewarmIsOffUntilTheSwitchIsOn(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := fake.account(1)
	require.False(t, acct.gatewayPoolPrewarmEnabled(), "缺省即关")

	gwpoolSeedWindow(&svc.codexCookies, gatewayPoolPrewarmMinSamples, 200*time.Second)
	gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-1", time.Hour)
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), ginCtx)
	req = req.WithContext(ctx)

	// **直接打会花票那个入口**：闸判在调用方的话，少一处判断就等于关不住。
	svc.gatewayPoolPrewarm(req, "", acct, gwpoolTestIdentity, gwpoolWarmModel)
	require.Zero(t, fake.hits.Load(), "关着的时候一张票都不许取")
	_, running := svc.codexCookies.poolPrewarm.Load(gwpoolTestIdentity)
	require.False(t, running, "关着的时候连占位都不许留，否则开了也再也跑不起来")

	acct.Extra[openAIGatewayPoolPrewarmExtraKey] = true
	require.True(t, acct.gatewayPoolPrewarmEnabled())
}

// 验出满血的候选票换进缓存，**而手里那张在整个预热期间一直是能用的那张**。
//
// 后一半是这个功能的全部意义：先标 Stale 再取票的那种实现会让紧接着的业务请求落在一张没验过
// 的票上，而那正是预热要消灭的那一格。
func TestPrewarmSwapsInTheVerifiedPairWithoutDisturbingTheLiveOne(t *testing.T) {
	poolCookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, poolCookie, 150)
	svc := &OpenAIGatewayService{}
	acct := fake.account(1)
	current := gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-old", 190*time.Second)

	shooter := &gwpoolWarmShooter{} // 默认形态 = 满血
	svc.gatewayPoolPrewarmRound(context.Background(), acct, gwpoolTestIdentity,
		current, 190*time.Second, shooter.shoot)

	pair, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.Equal(t, poolCookie, pair.cookie, "换上来的该是池子新发的那张")
	require.NotEqual(t, current.version, pair.version)
	require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity),
		"换上去的同时要标成验过，否则下一发业务请求会白跑一轮前台预热")
	// 两发垫话都必须打在**候选票**上，不是手里那张 —— 验的是新落点。
	require.Len(t, shooter.shots, 2)
	for i, shot := range shooter.shots {
		require.Equal(t, poolCookie, shot.cookie, "第 %d 发垫话送错了票", i+1)
	}
}

// 候选票判成降智就丢掉，**手里那张一个字都不动**（它还在服务业务请求）。
func TestPrewarmKeepsTheLivePairWhenEveryCandidateIsDegraded(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := fake.account(1)
	acct.Extra[openAIGatewayPoolWarmTicketsExtraKey] = 2
	current := gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-old", 190*time.Second)

	// 每一轮：A 下发一张，B 回**一张不同的** ⇒ 降智。
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{
		{status: http.StatusOK, minted: "mint-1"},
		{status: http.StatusOK, minted: "mint-2"},
		{status: http.StatusOK, minted: "mint-3"},
		{status: http.StatusOK, minted: "mint-4"},
	}}
	svc.gatewayPoolPrewarmRound(context.Background(), acct, gwpoolTestIdentity,
		current, 190*time.Second, shooter.shoot)

	pair, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, openAIGatewayPoolPairLive, state, "一轮没验出满血不许动手里那张")
	require.Equal(t, current.version, pair.version)
	require.True(t, svc.codexCookies.gatewayPoolVerifiedFull(gwpoolTestIdentity))
	require.Len(t, shooter.shots, 2, "后台只试一个候选，不能沿用前台的张数上限")
	require.EqualValues(t, 1, fake.hits.Load(), "判降级后连第二张票都不该取")
}

func TestPrewarmDoesNotInstallAnExpiredOrCancelledCandidate(t *testing.T) {
	for _, reason := range []string{"expired", "too_short", "cancelled"} {
		t.Run(reason, func(t *testing.T) {
			svc := &OpenAIGatewayService{}
			current := gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-old", time.Minute)
			candidate := current
			candidate.version = "tkt-new"
			switch reason {
			case "expired":
				candidate.until = time.Now().Add(-time.Second)
			case "too_short":
				candidate.until = time.Now().Add(30 * time.Second)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if reason == "cancelled" {
				cancel()
			}
			svc.installPrewarmedPair(ctx, gwpoolTestAccount(1), gwpoolTestIdentity,
				current, candidate, 1)
			got, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
			require.Equal(t, current, got)
		})
	}
}

// 预热期间手里那张被业务请求换掉了 ⇒ 候选票作废，**绝不硬塞**。
//
// 硬塞会把一张已经验过满血的票顶掉，而它可能正在服务请求。
func TestPrewarmDropsTheCandidateWhenTheCachedPairMovedOn(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := fake.account(1)
	current := gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-old", 190*time.Second)
	var moved openAIGatewayPoolPair
	shooter := &gwpoolWarmShooter{}
	svc.gatewayPoolPrewarmRound(context.Background(), acct, gwpoolTestIdentity,
		current, 190*time.Second, func(ctx context.Context, cookie, state string) (int, string, error) {
			result, minted, err := shooter.shoot(ctx, cookie, state)
			if len(shooter.shots) == 2 {
				moved = gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-newer", time.Second)
			}
			return result, minted, err
		})

	pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Len(t, shooter.shots, 2, "确实在验证中换票，而不是验证前提前返回")
	require.Equal(t, moved.version, pair.version, "别人换上去的那张不许被顶掉")
}

// 同一身份同时只许一轮：并发两轮就是双倍烧票换同一个窗口。
func TestPrewarmRunsOneRoundPerIdentity(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := fake.account(1)
	acct.Extra[openAIGatewayPoolPrewarmExtraKey] = true
	gwpoolSeedWindow(&svc.codexCookies, gatewayPoolPrewarmMinSamples, 200*time.Second)
	gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-old", 190*time.Second)
	// 占位已经被另一轮拿着 ⇒ 这一发什么都不该做。
	svc.codexCookies.poolPrewarm.Store(gwpoolTestIdentity, gatewayPoolPrewarmMark{running: true})

	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), ginCtx)
	svc.gatewayPoolPrewarm(req.WithContext(ctx), "", acct, gwpoolTestIdentity, gwpoolWarmModel)

	require.Zero(t, fake.hits.Load(), "已经有一轮在跑就不许再取票")
}

func TestPrewarmAttemptIsSharedAndRetainedForTheCurrentWindow(t *testing.T) {
	store := &openAICodexCookieStore{}
	current := gwpoolSeedVerifiedAge(store, "tkt-old", time.Minute)
	var wg sync.WaitGroup
	winners := make(chan bool, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			winners <- store.gatewayPoolBeginPrewarm(gwpoolTestIdentity, current)
		}()
	}
	wg.Wait()
	close(winners)
	count := 0
	for won := range winners {
		if won {
			count++
		}
	}
	require.Equal(t, 1, count)
	store.gatewayPoolFinishPrewarm(gwpoolTestIdentity, current)
	require.False(t, store.gatewayPoolBeginPrewarm(gwpoolTestIdentity, current), "结束后不重开同一窗口")
	renewed := current
	renewed.version = "tkt-renewed"
	renewed.echoMisses++
	require.False(t, store.gatewayPoolBeginPrewarm(gwpoolTestIdentity, renewed), "续票/响应计数不是新窗口")
	next := current
	next.since = time.Now()
	require.True(t, store.gatewayPoolBeginPrewarm(gwpoolTestIdentity, next))
	store.gatewayPoolFinishPrewarm(gwpoolTestIdentity, current)
	require.False(t, store.gatewayPoolBeginPrewarm(gwpoolTestIdentity, current), "旧轮结束不能解开新轮的锁")
}

func TestPrewarmFailedRoundDoesNotRetryOnTheNextBusinessRequest(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.forceStatus = http.StatusServiceUnavailable
	svc := &OpenAIGatewayService{}
	acct := fake.account(1)
	acct.Extra[openAIGatewayPoolPrewarmExtraKey] = true
	current := gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-old", time.Minute)
	current.until = time.Now().Add(30 * time.Second)
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, current)
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	startAndWait := func() {
		svc.gatewayPoolPrewarm(req, "", acct, gwpoolTestIdentity, gwpoolWarmModel)
		require.Eventually(t, func() bool {
			value, ok := svc.codexCookies.poolPrewarm.Load(gwpoolTestIdentity)
			mark, valid := value.(gatewayPoolPrewarmMark)
			return ok && valid && !mark.running
		}, time.Second, time.Millisecond)
	}
	startAndWait()
	require.EqualValues(t, 1, fake.hits.Load())
	svc.codexCookies.poolBackoff.Delete(gatewayPoolLedgerIdentity(gwpoolTestIdentity))
	startAndWait()
	require.EqualValues(t, 1, fake.hits.Load(), "不依赖池子退避挡住本窗口的第二轮")
	require.Empty(t, svc.codexCookies.poolCooldown, "缺票不能学习冷却")
}

func TestPrewarmInconclusiveDoesNotTrainCooldown(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	svc := &OpenAIGatewayService{}
	acct := fake.account(1)
	current := gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-old", time.Minute)
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{{status: http.StatusTooManyRequests}}}
	svc.gatewayPoolPrewarmRound(context.Background(), acct, gwpoolTestIdentity, current, time.Minute, shooter.shoot)
	got, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, current, got)
	state := svc.codexCookies.poolCooldown[gatewayPoolLedgerKey(gwpoolTestIdentity, "unified-142")]
	require.Equal(t, 3600, state.WindowSeconds)
	require.Empty(t, state.Outcome)
	require.Empty(t, state.Successes)
	require.Len(t, shooter.shots, 1)
	require.EqualValues(t, 1, fake.hits.Load())
}

func TestPrewarmRejectsAShortCandidateWithoutProbing(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 65)
	svc := &OpenAIGatewayService{}
	current := gwpoolSeedVerifiedAge(&svc.codexCookies, "tkt-old", time.Minute)
	shooter := &gwpoolWarmShooter{}
	svc.gatewayPoolPrewarmRound(context.Background(), fake.account(1), gwpoolTestIdentity, current, time.Minute, shooter.shoot)
	got, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, current, got)
	require.Empty(t, shooter.shots)
	require.False(t, svc.codexCookies.gatewayPoolUsedRecently(gwpoolTestIdentity, "unified-142", time.Hour),
		"验证前丢弃的短票不能启动本地冷却")
	require.Empty(t, svc.codexCookies.poolCooldown)
	require.Eventually(t, func() bool { return fake.releaseHits.Load() == 1 }, time.Second, time.Millisecond)
	require.Contains(t, <-fake.queries, "min_remaining=75", "取票应预留验证耗时和交付余量")
}

// 满血时长的样本**只认验过满血的票**。
//
// 没验过的票不知道窗口什么时候开的，把它的「取票到判死」当成满血时长会把 p95 拉低，
// 于是预热越来越早、越来越费票。
func TestFullWindowSamplesOnlyCountVerifiedPairs(t *testing.T) {
	store := &openAICodexCookieStore{}
	now := time.Now()
	// 没验过的那张：标 Stale 不该留下样本。
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		version: "tkt-unverified", gateway: "unified-1",
		until: now.Add(time.Hour), since: now.Add(-time.Minute),
	})
	store.gatewayPoolMarkStale(gwpoolTestIdentity, "tkt-unverified", "unified-1")
	require.Zero(t, store.poolFullWindow.total)

	// 验过的那张：留下一个约等于「验过之后过了多久」的样本。
	gwpoolSeedVerifiedAge(store, "tkt-verified", 3*time.Minute)
	store.gatewayPoolMarkStale(gwpoolTestIdentity, "tkt-verified", "unified-1")
	require.Equal(t, 1, store.poolFullWindow.total)
	_, ok := store.poolFullWindow.p95()
	require.False(t, ok, "一个样本还不够开预热")
	require.InDelta(t, (3 * time.Minute).Seconds(),
		store.poolFullWindow.samples[0].Seconds(), 1, "样本该是「判出满血到判死」那一段")
}

// 同一张票被重复标「验过满血」不许把窗口起点往后推。
//
// 推了的话每标一次都把「这张票还能满血多久」重算一遍 ⇒ 满血时长的样本被系统性拉长 ⇒
// 后台预热越来越晚，最后退化成没有预热。并发预热各标一次、前台验完复查那一下，都会走到这里。
func TestVerifiedMarkKeepsTheWindowStartForTheSameTicket(t *testing.T) {
	store := &openAICodexCookieStore{}
	gwpoolSeedVerifiedAge(store, "tkt-1", 2*time.Minute)
	before, ok := store.gatewayPoolVerifiedMarkOf(gwpoolTestIdentity)
	require.True(t, ok)

	store.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "tkt-1")
	after, _ := store.gatewayPoolVerifiedMarkOf(gwpoolTestIdentity)
	require.Equal(t, before.at, after.at, "同一张票重复标不许推进窗口起点")

	store.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "tkt-2")
	rolled, _ := store.gatewayPoolVerifiedMarkOf(gwpoolTestIdentity)
	require.True(t, rolled.at.After(before.at), "换了票就是一个新窗口")
}

// 样本池是环形的：超过容量之后只留最近那些，p95 跟着上游行为走。
func TestFullWindowKeepsOnlyTheRecentSamples(t *testing.T) {
	store := &openAICodexCookieStore{}
	gwpoolSeedWindow(store, gatewayPoolPrewarmSampleCap, 100*time.Second)
	window, ok := store.poolFullWindow.p95()
	require.True(t, ok)
	require.Equal(t, 100*time.Second, window)

	// 再灌满一池新值：老的全被挤掉。
	gwpoolSeedWindow(store, gatewayPoolPrewarmSampleCap, 300*time.Second)
	window, _ = store.poolFullWindow.p95()
	require.Equal(t, 300*time.Second, window)
	require.Len(t, store.poolFullWindow.samples, gatewayPoolPrewarmSampleCap, "池子不许长胖")
}

// 并发 observe / p95 不许炸（样本池挂在进程级，转发面每一发都可能读它）。
func TestFullWindowIsConcurrencySafe(t *testing.T) {
	store := &openAICodexCookieStore{}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 64 {
				store.poolFullWindow.observe(time.Duration(i*64+j+1) * time.Millisecond)
				_, _ = store.poolFullWindow.p95()
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, 16*64, store.poolFullWindow.total)
}
