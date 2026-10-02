package service

// 后台预热：在手里那张票的满血窗口**到点之前**，另取一张候选票在后台验满血，验出来才换上去。
//
// 要它解决的是 queue 那条路唯一剩下的疼点：预热跑在客户端的请求里，客户端在那整段时间里一个
// 字节都收不到（平均约 6 发垫话，最坏约两分钟）。把这件事挪到两次客户端请求**之间**，客户端
// 就只在「刚好撞上换票」时才等。
//
// 三条设计约束，每条都对应一个真实的坑：
//
//  1. **候选票绝不进缓存，除非验出满血。** 走 gatewayPoolPair 取票会把新票写进 (身份 → pair)
//     缓存，于是紧接着那一发业务请求会落在一张**没验过**的票上 —— 正是预热要消灭的那一格。
//     所以这里走 gatewayPoolTakeFresh（不碰缓存），验出满血才 CAS 换上去。
//     手里那张在整个预热期间照常服务业务请求，一秒空档都没有。
//
//  2. **什么时候开始，由实测的满血时长决定，不是写死的常数。** 用户拍的规则：攒满 100 个满血
//     时长样本之后算 p95，p95 − 15s 就是开始预热的票龄；0–100 发不预热。早了白烧供给（手里那张
//     还好着），晚了客户端还是要等。
//
//  3. **同一身份同时只有一轮。** 预热是「花票换时间」，并发两轮就是双倍烧票换同一个窗口。
//
// 刻意不做的事：不起常驻 goroutine / 定时器。触发点挂在业务请求的预热快路上
// （gatewayPoolWarmUp 判出「手里那张还验过满血」的那一刻）—— 没有请求的身份不需要热票，
// 而起了常驻循环就会给闲着的账号也烧供给。

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// gatewayPoolPrewarmLead 是提前量：票龄到 p95 − 这个数就开始预热（用户拍板 15 秒）。
	gatewayPoolPrewarmLead = 15 * time.Second
	// gatewayPoolPrewarmMinSamples 是开始预热所需的样本数（用户拍板 100）。
	//
	// 攒够之前**一发都不预热**：样本不足时 p95 由个别极值决定，而猜错的两个方向都要花钱 ——
	// 猜早了在手里那张还满血时就换票（白烧一个 (上游账号 × 网关) 单位），猜晚了等于没预热。
	gatewayPoolPrewarmMinSamples = 100
	// gatewayPoolPrewarmSampleCap 是环形样本池的容量。
	//
	// 只留最近这些：满血窗口的时长是上游的行为，会变（现场测过 150s 和 ~200s 两种口径）。
	// 留太久的样本会让 p95 被几小时前的上游行为钉住。
	gatewayPoolPrewarmSampleCap = 512
	// gatewayPoolPrewarmBudget 是一轮后台预热自己的墙上时间上限。
	//
	// 比客户端那条路的 gatewayPoolWarmBudget(90s) 宽：这一轮没有客户端在等，可以把票试满。
	// 但仍然要有上限 —— 它持有「这个身份正在预热」的占位，卡住就等于永远不再预热。
	gatewayPoolPrewarmBudget = 4 * time.Minute
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

// p95 报告样本的 95 分位，以及「样本够不够」。不够时第二个返回值为 false，调用方不许预热。
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
func (s *openAICodexCookieStore) gatewayPoolNoteFullWindow(identity, version string) {
	if s == nil || identity == "" || version == "" {
		return
	}
	mark, ok := s.gatewayPoolVerifiedMarkOf(identity)
	if !ok || mark.version != version || mark.at.IsZero() {
		return
	}
	s.poolFullWindow.observe(time.Since(mark.at))
}

// gatewayPoolPrewarmDue 报告「手里这张票该开始预热换下一张了吗」。
//
// 不够样本、票没验过满血、或者票龄还没到 p95 − 提前量，都回 false。
func (s *openAICodexCookieStore) gatewayPoolPrewarmDue(identity string) (time.Duration, bool) {
	if s == nil {
		return 0, false
	}
	window, ok := s.poolFullWindow.p95()
	if !ok {
		return 0, false
	}
	mark, ok := s.gatewayPoolVerifiedMarkOf(identity)
	if !ok || mark.at.IsZero() {
		return 0, false
	}
	// 提前量比 p95 还长（上游窗口短到 15 秒以内）⇒ 一验出满血就该预热，lead 失去意义但不出错。
	if age := time.Since(mark.at); age >= window-gatewayPoolPrewarmLead {
		return age, true
	}
	return 0, false
}

// gatewayPoolPrewarm 在手里那张票快到点时，后台另取一张候选票验满血，验出来才换上去。
//
// 立刻返回：真正的活在一个脱离请求的 goroutine 里跑（它要活过这一发客户端请求）。
// 没到点、样本不够、已经有一轮在跑，都是直接返回、什么都不做。
//
// model 必须由调用方在**请求还活着的时候**读出来传进来（gatewayPoolWarmModel 要读请求体）：
// state 绑在 (账号 × 模型 × 这张 cflb/oailb 对) 上，拿别的模型去验等于验了另一件事。
func (s *OpenAIGatewayService) gatewayPoolPrewarm(
	request *http.Request, proxyURL string, account *Account, identity, model string,
) {
	// 开关判在这里而不是只判在调用方：这是唯一会自己花票的入口，闸就该和动作在一起。
	// 调用方那一处 gatewayPoolPrewarmEnabled 只是为了省掉「读 model 要解一遍请求体」。
	if s == nil || model == "" || !account.gatewayPoolPrewarmEnabled() {
		return
	}
	age, due := s.codexCookies.gatewayPoolPrewarmDue(identity)
	if !due {
		return
	}
	current, state := s.codexCookies.cachedPoolPair(identity)
	if state != openAIGatewayPoolPairLive {
		return // 手里那张已经不 Live 了 ⇒ 下一发业务请求自己会走前台预热，不用这条路
	}
	// 同一身份同时只许一轮：LoadOrStore 做占位，拿不到就说明已经有人在跑。
	if _, running := s.codexCookies.poolPrewarm.LoadOrStore(identity, struct{}{}); running {
		return
	}
	// ctx 刻意脱离这一发请求：预热要活过它（客户端的响应早就写完了）。自己带预算上限。
	ctx, cancel := context.WithTimeout(
		context.WithoutCancel(request.Context()), gatewayPoolPrewarmBudget)
	go func() {
		defer cancel()
		defer s.codexCookies.poolPrewarm.Delete(identity)
		s.gatewayPoolPrewarmRound(ctx, account, identity, current, age,
			func(ctx context.Context, cookie, state string) (int, string, error) {
				return s.gatewayPoolWarmShot(ctx, account, proxyURL, cookie, model, state)
			})
	}()
}

// gatewayPoolPrewarmRound 跑一轮后台预热：一张张试，验出满血的第一张换上去。
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
	pool, err := s.codexCookies.poolClient(account)
	if err != nil {
		slog.Warn("gwpool_prewarm_no_pool", "account_id", account.ID,
			"error", gatewayPoolWarmErrorText(err))
		return
	}
	tickets := account.gatewayPoolWarmTickets()
	slog.Info("gwpool_prewarm_start", "account_id", account.ID,
		"gateway", current.gateway, "age_s", int(age.Seconds()), "tickets", tickets)
	// burned 只为那条终态日志能一行答完「这一轮试了哪几个网关」。
	burned := make([]string, 0, tickets)
	for attempt := 1; attempt <= tickets; attempt++ {
		if ctx.Err() != nil {
			break
		}
		// force + exclude_versions：要一张**不同的**网关，而且绝不要手里这一张。
		candidate, err := s.codexCookies.gatewayPoolTakeFresh(
			ctx, pool, account, identity, true, []string{current.version})
		if err != nil {
			// 供给见底是常态（池子一个网关一周期只出一张），不是故障：手里那张还在服务，
			// 下一发业务请求到点时会再触发一轮。所以只记一条，不退避、不重试。
			slog.Info("gwpool_prewarm_no_ticket", "account_id", account.ID,
				"attempt", attempt, "error", gatewayPoolWarmErrorText(err))
			return
		}
		applied := OpenAIGatewayPoolApplied{
			AccountID: account.ID,
			Cookie:    candidate.cookie,
			Gateway:   candidate.gateway,
			Region:    candidate.region,
			Version:   candidate.version,
		}
		full, conclusive, _, perr := s.codexCookies.gatewayPoolWarmVerdict(
			ctx, account, identity, applied, candidate.cookie, attempt, shoot)
		switch {
		case !conclusive:
			// 下不了结论（非 200、限流、传输失败）⇒ 不把这个落点判死，也不继续试。
			// **刻意不像前台那条路那样「放行」**：这里没有业务请求要放行，而继续试下去是在
			// 上游正不正常都不知道的时候接着烧票。手里那张还在服务，下一发再来。
			slog.Warn("gwpool_prewarm_inconclusive", "account_id", account.ID,
				"gateway", candidate.gateway, "attempt", attempt,
				"error", gatewayPoolWarmErrorText(perr))
			// 这张候选票确证碰过上游（或连都没连上），窗口按已烧算：本地账本在
			// gatewayPoolTakeFresh 里已经记过，这里只补一笔没有判定的接触进落点卡。
			s.notePrewarmVerdict(ctx, account, applied, "", false)
			return
		case full:
			s.installPrewarmedPair(ctx, account, identity, current, candidate, applied, attempt)
			return
		default:
			// 判成降智：候选票丢掉（**不还给池子** —— 它真的打出去了，窗口真的烧了），
			// 记进落点卡但不推进 `Current`（没有业务请求落在它上面）。
			burned = append(burned, candidate.gateway)
			slog.Info("gwpool_prewarm_degraded", "account_id", account.ID,
				"gateway", candidate.gateway, "attempt", attempt)
			s.notePrewarmVerdict(ctx, account, applied, openAIGatewayVerdictDegraded, false)
		}
	}
	// 一轮都没验出满血。**不是错误**：手里那张还在服务业务请求，客户端什么都没感觉到。
	// 下一发业务请求到点时会再触发一轮 —— 真的到那时手里那张已经降智了，前台预热会接手。
	slog.Info("gwpool_prewarm_exhausted", "account_id", account.ID,
		"tickets", len(burned), "gateways", strings.Join(burned, ","),
		"budget_exhausted", ctx.Err() != nil)
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
	applied OpenAIGatewayPoolApplied,
	attempt int,
) {
	if !s.codexCookies.poolPairs.CompareAndSwap(identity, current, candidate) {
		slog.Info("gwpool_prewarm_superseded", "account_id", account.ID,
			"gateway", candidate.gateway, "attempt", attempt,
			"reason", "the cached pair changed while this round was verifying")
		return
	}
	// 顺序是承重的：先换票再标验过。反过来的话中间那一瞬「缓存里是旧票、验过的是新票号」⇒
	// gatewayPoolVerifiedFull 比对票号失败 ⇒ 并发的业务请求会以为手里这张没验过，白跑一轮前台预热。
	s.codexCookies.gatewayPoolMarkVerifiedFull(identity, candidate.version)
	slog.Info("gwpool_prewarm_ready", "account_id", account.ID,
		"gateway", candidate.gateway, "region", candidate.region, "tickets", attempt)
	// 满血这条连 `Current` 一起推进：它就是接下来的业务请求要落的那个网关。
	s.notePrewarmVerdict(ctx, account, applied, openAIGatewayVerdictFull, true)
}

// notePrewarmVerdict 把这一轮的判定写进账号的落点记录。
//
// 和前台那条路（noteWarmVerdict）分开只因为 ctx 的来路不同：这里的 ctx 已经脱离了请求、
// 自带预算，不需要再 WithoutCancel 一次；写库那一下仍然要有自己的小超时，别让一个慢库把
// 「这个身份正在预热」的占位挂住几分钟。
func (s *OpenAIGatewayService) notePrewarmVerdict(
	ctx context.Context,
	account *Account,
	applied OpenAIGatewayPoolApplied,
	verdict string,
	advanceCurrent bool,
) {
	noteCtx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
	defer cancel()
	s.noteOpenAIGatewayUse(noteCtx, account, applied.Gateway, applied.Region, verdict, advanceCurrent,
		applied.PoolLive)
}
