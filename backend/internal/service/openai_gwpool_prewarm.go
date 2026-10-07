package service

// 提前准备下一个网关：业务触发、当前窗口最多一个候选，不运行空闲账号的定时探测。
// 按缓存租约、历史窗口和验证耗时决定开始时间。候选不进业务缓存，直到验证通过且寿命足够；
// 缺票、失败或异常均结束本轮，同一窗口不再重开。前台验证仍是兜底，不承诺无等待切换。

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	gatewayPoolPrewarmMargin = 5 * time.Second
	// 历史仅作补充，样本不足仍可按真实缓存租约触发。
	gatewayPoolPrewarmMinSamples = 100
	// gatewayPoolPrewarmSampleCap 是环形样本池的容量。
	//
	// 只留最近这些：满血窗口的时长是上游的行为，会变（现场测过 150s 和 ~200s 两种口径）。
	// 留太久的样本会让 p95 被几小时前的上游行为钉住。
	gatewayPoolPrewarmSampleCap = 512
	gatewayPoolPrewarmBudget    = 90 * time.Second
)

// gatewayPoolVerifiedMark 是「这个身份手上那张票验过满血」的那一笔，连**判出来的时刻**一起记。
//
// 时刻是承重的，不是顺手记的：满血窗口的时长 = 从判出满血到判出降智之间那段，而
// (消费账号 × 网关) 的窗口是**首次接触**就开始烧的 ⇒ 判出满血那一刻最接近窗口起点。
// 没有它就算不出样本，也就没有 p95，预热永远不会开始。
type gatewayPoolVerifiedMark struct {
	version string
	at      time.Time
}

// gatewayPoolFullWindow 是满血时长的环形样本池。
//
// 为什么全局一份而不是按身份分：满血窗口的时长是**上游**的行为（现场测到的 170–200s 对所有
// 账号是同一个量级），不是某个账号的属性。按身份分的话每个身份都要独立攒 100 个样本 ——
// 一个身份一小时也就换十几次票，那等于永远攒不满。
//
// 用 sort 而不是维护一个有序结构：p95 只在业务请求的预热快路上算（每发一次），512 个元素的
// 复制 + 排序是微秒级，而换来的是一个一眼看得懂的实现。
// ponytail: 每次取 p95 都复制+排序，样本池再大一个数量级就该换成分桶直方图。
type gatewayPoolFullWindow struct {
	mu      sync.Mutex
	samples []time.Duration
	next    int
	total   int
}

// observe 记一个满血时长样本。非正数丢掉（时钟回拨、同一刻判两次）。
func (w *gatewayPoolFullWindow) observe(d time.Duration) {
	if w == nil || d <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.samples) < gatewayPoolPrewarmSampleCap {
		w.samples = append(w.samples, d)
	} else {
		w.samples[w.next] = d
		w.next = (w.next + 1) % gatewayPoolPrewarmSampleCap
	}
	w.total++
}

// p95 报告样本的 95 分位，以及是否足够作为历史参考。
func (w *gatewayPoolFullWindow) p95() (time.Duration, bool) {
	if w == nil {
		return 0, false
	}
	w.mu.Lock()
	if w.total < gatewayPoolPrewarmMinSamples {
		w.mu.Unlock()
		return 0, false
	}
	got := make([]time.Duration, len(w.samples))
	copy(got, w.samples)
	w.mu.Unlock()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	// 向下取整的序号：len=100 时取第 95 个（下标 94），len=1 时取下标 0。
	idx := len(got) * 95 / 100
	if idx >= len(got) {
		idx = len(got) - 1
	}
	return got[idx], true
}

// gatewayPoolNoteFullWindow 把「这张票的满血窗口到此结束」记成一个样本。
//
// 调用点只有一个：gatewayPoolMarkStale（判到降智、标 Stale 的那一刻）。那是两条降智路径
// （业务响应上的 state-echo、预热的垫话判据）共同的漏斗。
//
// 只有**验过满血**的那张票才产样本：没验过的票不知道窗口什么时候开的，把它的「取票到判死」
// 当成满血时长会把 p95 拉低，于是预热越来越早、越来越费票。
// 返回值是测到的时长，0 = 这张票没验过满血（没有窗口起点）。调用方拿它往账号账本里记一笔
// 「这个 (账号 × 网关) 的满血持续了多久」—— 卡片上那一段只能用这个数，**不能**拿账本里的
// 「判满血的时刻」和「判降智的时刻」相减：后者要等下一次真的打到这个网关才会写，中间的空闲
// 全算进去，实测能得出 22655 秒（2026-10-03 现网）。这里两头都在同一张票的生命里，有界。
func (s *openAICodexCookieStore) gatewayPoolNoteFullWindow(identity, version string) time.Duration {
	if s == nil || identity == "" || version == "" {
		return 0
	}
	mark, ok := s.gatewayPoolVerifiedMarkOf(identity)
	if !ok || mark.version != version || mark.at.IsZero() {
		return 0
	}
	held := time.Since(mark.at)
	s.poolFullWindow.observe(held)
	return held
}

func (s *openAICodexCookieStore) gatewayPoolVerificationTime(identity string) time.Duration {
	if value, ok := s.poolWarmDuration.Load(identity); ok {
		if d, ok := value.(time.Duration); ok && d > gatewayPoolWarmMinBudget {
			if d > 2*gatewayPoolWarmShotTimeout {
				return 2 * gatewayPoolWarmShotTimeout
			}
			return d
		}
	}
	return gatewayPoolWarmMinBudget
}

// 租约与历史窗口取较早者；准备时间含列清单、取票、最近验证耗时与少量余量。
func (s *openAICodexCookieStore) gatewayPoolPrewarmDue(identity string, account *Account) (time.Duration, bool) {
	if s == nil || account == nil {
		return 0, false
	}
	current, state := s.cachedPoolPair(identity)
	if state != openAIGatewayPoolPairLive || current.version == "" {
		return 0, false
	}
	mark, ok := s.gatewayPoolVerifiedMarkOf(identity)
	if !ok || mark.at.IsZero() || mark.version != current.version {
		return 0, false
	}
	deadline := current.until
	if window, ok := s.poolFullWindow.p95(); ok && mark.at.Add(window).Before(deadline) {
		deadline = mark.at.Add(window)
	}
	lead := s.gatewayPoolVerificationTime(identity) + account.gatewayPoolFetchTimeout() +
		account.gatewayPoolListTimeout() + gatewayPoolPrewarmMargin
	if lead > gatewayPoolPrewarmBudget {
		lead = gatewayPoolPrewarmBudget
	}
	return time.Since(mark.at), time.Until(deadline) <= lead
}

type gatewayPoolPrewarmMark struct {
	window  time.Time // since 不随票号续期或业务响应计数变化，才是同一个当前窗口。
	running bool
}

func (s *openAICodexCookieStore) gatewayPoolBeginPrewarm(identity string, current openAIGatewayPoolPair) bool {
	if identity == "" || current.since.IsZero() {
		return false
	}
	mark := gatewayPoolPrewarmMark{window: current.since, running: true}
	for {
		prev, loaded := s.poolPrewarm.LoadOrStore(identity, mark)
		if !loaded {
			return true
		}
		old, ok := prev.(gatewayPoolPrewarmMark)
		if !ok || old.running || old.window.Equal(current.since) {
			return false
		}
		if s.poolPrewarm.CompareAndSwap(identity, old, mark) {
			return true
		}
	}
}

func (s *openAICodexCookieStore) gatewayPoolFinishPrewarm(identity string, current openAIGatewayPoolPair) {
	s.poolPrewarm.CompareAndSwap(identity,
		gatewayPoolPrewarmMark{window: current.since, running: true},
		gatewayPoolPrewarmMark{window: current.since})
}

// gatewayPoolPrewarm 在手里那张票快到点时，后台另取一张候选票验满血，验出来才换上去。
//
// 立刻返回：真正的活在一个脱离请求的 goroutine 里跑（它要活过这一发客户端请求）。
// 没到点、该窗口已试过或已有一轮在跑，都不做任何请求。
//
// model 必须由调用方在**请求还活着的时候**读出来传进来（gatewayPoolWarmModel 要读请求体）：
// state 绑在 (账号 × 模型 × 这张 cflb/oailb 对) 上，拿别的模型去验等于验了另一件事。
func (s *OpenAIGatewayService) gatewayPoolPrewarm(
	request *http.Request, proxyURL string, account *Account, identity, model string,
) {
	// 开关判在这里而不是只判在调用方：这是唯一会自己花票的入口，闸就该和动作在一起。
	// 调用方那一处 gatewayPoolPrewarmEnabled 只是为了省掉「读 model 要解一遍请求体」。
	if s == nil || request == nil || model == "" || !account.gatewayPoolPrewarmEnabled() {
		return
	}
	age, due := s.codexCookies.gatewayPoolPrewarmDue(identity, account)
	if !due {
		return
	}
	current, state := s.codexCookies.cachedPoolPair(identity)
	if state != openAIGatewayPoolPairLive || !s.codexCookies.gatewayPoolVerifiedFull(identity) {
		return // 手里那张已经不 Live 了 ⇒ 下一发业务请求自己会走前台预热，不用这条路
	}
	if !s.codexCookies.gatewayPoolBeginPrewarm(identity, current) {
		return
	}
	// 活过触发它的业务请求，但不活过当前租约，避免无效的后台长跑。
	deadline := time.Now().Add(gatewayPoolPrewarmBudget)
	if current.until.Before(deadline) {
		deadline = current.until
	}
	ctx, cancel := context.WithDeadline(context.WithoutCancel(request.Context()), deadline)
	ctx = context.WithValue(ctx, gatewayPoolProbeModelKey{}, model)
	go func() {
		defer cancel()
		defer s.codexCookies.gatewayPoolFinishPrewarm(identity, current)
		s.gatewayPoolPrewarmRound(ctx, account, identity, current, age,
			func(ctx context.Context, cookie, state string) (int, string, error) {
				return s.gatewayPoolWarmShot(ctx, account, proxyURL, cookie, model, state)
			})
	}()
}

// gatewayPoolPrewarmRound 只取一个候选，任何失败都结束，不沿用前台张数配置。
//
// shoot 抽成参数的理由和前台那条路一样（见 gatewayPoolWarmShooter）：让这一轮在**不碰真实
// 出站构造链**的情况下被测到。生产路径只有一个实现。
func (s *OpenAIGatewayService) gatewayPoolPrewarmRound(
	ctx context.Context,
	account *Account,
	identity string,
	current openAIGatewayPoolPair,
	age time.Duration,
	shoot gatewayPoolWarmShooter,
) {
	ctx = context.WithValue(ctx, gatewayPoolProbeSourceKey{}, "background")
	if ctx.Err() != nil {
		return
	}
	if cached, state := s.codexCookies.cachedPoolPair(identity); state != openAIGatewayPoolPairLive || cached != current {
		return
	}
	pool, err := s.codexCookies.poolClient(account)
	if err != nil {
		slog.Warn("gwpool_prewarm_no_pool", "account_id", account.ID,
			"error", gatewayPoolWarmErrorText(err))
		return
	}
	slog.Info("gwpool_prewarm_start", "account_id", account.ID,
		"gateway", current.gateway, "age_s", int(age.Seconds()), "tickets", 1)
	candidate, err := s.codexCookies.gatewayPoolTakeFresh(
		ctx, pool, account, identity, true, []string{current.version},
		openAIGatewayPoolMinRemaining+s.codexCookies.gatewayPoolVerificationTime(identity))
	if err != nil {
		slog.Info("gwpool_prewarm_no_ticket", "account_id", account.ID,
			"error", gatewayPoolWarmErrorText(err))
		return
	}
	applied := OpenAIGatewayPoolApplied{
		AccountID: account.ID, Cookie: candidate.cookie, Gateway: candidate.gateway,
		Region: candidate.region, Version: candidate.version,
		RoundID: candidate.roundID,
	}
	probeCtx, cancel := context.WithDeadline(ctx, candidate.until.Add(-openAIGatewayPoolMinRemaining))
	defer cancel()
	full, conclusive, _, firstSent, perr := s.codexCookies.gatewayPoolWarmVerdict(
		probeCtx, account, identity, applied, candidate.cookie, 1, shoot)
	candidate.firstSent = firstSent
	if probeCtx.Err() != nil {
		conclusive, perr = false, probeCtx.Err()
	}
	switch {
	case !conclusive:
		slog.Warn("gwpool_prewarm_inconclusive", "account_id", account.ID,
			"gateway", candidate.gateway, "error", gatewayPoolWarmErrorText(perr))
		s.notePrewarmVerdict(ctx, account, applied, "", false)
	case full:
		installed := s.installPrewarmedPair(ctx, account, identity, current, candidate, 1)
		// 已验证但被业务换票抢先的候选也要记结论，只是不推进当前网关。
		s.notePrewarmVerdict(ctx, account, applied, openAIGatewayVerdictFull, installed)
	default:
		slog.Info("gwpool_prewarm_degraded", "account_id", account.ID, "gateway", candidate.gateway)
		s.notePrewarmVerdict(ctx, account, applied, openAIGatewayVerdictDegraded, false)
	}
}

// installPrewarmedPair 把验出满血的候选票换到缓存里。
//
// CAS 而不是 Store：只在缓存里**还是当初那一张**时才换。期间业务请求换过票（手里那张被判死、
// 前台预热另取了一张并验过）的话，这一轮的候选票就作废 —— 硬塞进去会把一张**已经验过满血的**
// 票顶掉，而它可能正在服务请求。候选票作废不是泄漏：槽位已经烧了，账本也记过了。
func (s *OpenAIGatewayService) installPrewarmedPair(
	ctx context.Context,
	account *Account,
	identity string,
	current, candidate openAIGatewayPoolPair,
	attempt int,
) bool {
	if ctx.Err() != nil || candidate.version == "" || time.Until(candidate.until) < openAIGatewayPoolMinRemaining {
		slog.Info("gwpool_prewarm_expired", "account_id", account.ID, "gateway", candidate.gateway)
		return false
	}
	if !s.codexCookies.poolPairs.CompareAndSwap(identity, current, candidate) {
		slog.Info("gwpool_prewarm_superseded", "account_id", account.ID,
			"gateway", candidate.gateway, "attempt", attempt,
			"reason", "the cached pair changed while this round was verifying")
		return false
	}
	// 顺序是承重的：先换票再标验过。反过来的话中间那一瞬「缓存里是旧票、验过的是新票号」⇒
	// gatewayPoolVerifiedFull 比对票号失败 ⇒ 并发的业务请求会以为手里这张没验过，白跑一轮前台预热。
	s.codexCookies.gatewayPoolMarkVerifiedFull(identity, candidate.version)
	slog.Info("gwpool_prewarm_ready", "account_id", account.ID,
		"gateway", candidate.gateway, "region", candidate.region, "tickets", attempt)
	return true
}

// notePrewarmVerdict 把这一轮的判定写进账号的落点记录。
//
// 证据落账用独立短预算，后台验证刚好超时也不能丢掉已经启动的冷却记录。
func (s *OpenAIGatewayService) notePrewarmVerdict(
	ctx context.Context,
	account *Account,
	applied OpenAIGatewayPoolApplied,
	verdict string,
	advanceCurrent bool,
) {
	noteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer cancel()
	s.noteGatewayPoolCooldownVerdict(noteCtx, account, applied, verdict)
	s.noteOpenAIGatewayUse(noteCtx, account, applied.Gateway, applied.Region, verdict, advanceCurrent,
		applied.PoolLive, applied.PoolFree, applied.FullHeldMs)
}
