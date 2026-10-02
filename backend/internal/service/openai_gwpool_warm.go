package service

// queue 档的预热：业务请求只落在**已验满血**的槽上（openAIGatewayPoolGuardExtraKey）。
//
// 为什么要它：另外两档都是**事后**判。cut 判到降智就截断、让客户端自己重发，而判据对首轮请求
// 结构性失效（客户端没送 turn-state ⇒ 没有回声 ⇒ 判不出来，见 gatewayPoolRouteDegraded）⇒
// 那种请求的降智它一发都拦不住。queue 档把判据挪到业务请求**之前**，自己铸一张 state 当回声
// 基准（shot A），拿便宜的垫话去试网关，验出满血才放业务请求进去。
//
// 成本是算过的：
//   - **窗口内连打多发都满血**（2026-10-02 实测，窗口 ≥200s、窗口内 7/7）⇒ 验过一次就覆盖
//     整个窗口里的所有请求，判据成本摊薄到接近零。所以手里那张还 Live 的时候一发都不打。
//   - 判据只要 2 发，只读响应头、不等模型吐完 ⇒ 每发 3–6s。
//   - state-echo 口径的命中率约 33% ⇒ 平均 3 张票 ≈ 6 发垫话换一个窗口。
//
// **这几发垫话不计费给任何 API Key**：它们不是客户端的请求，记到谁头上都是错的（判定点也在
// 「响应头到手、响应体一个字节没读」的时刻，连 token 读数都观测不到，同 openai_gateway_usage.go
// 里丢弃行那段论证）。但它们确实在烧上游账号的配额，所以每一发都打一条 gwpool_warm_probe —— 那
// 是事后唯一能回答「这个号的配额花在哪了」的东西。
//
// 判据本体与纪律一个字不改，见 openai_gwpool_state_echo.go 的文件头。

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

const (
	// gatewayPoolWarmMaxTickets 是一次预热最多试几张票的**默认值**，账号可覆盖
	// （openAIGatewayPoolWarmTicketsExtraKey）。
	//
	// 实测命中率约 29%（14 ready / 48 结论，2026-10-02 线路机）⇒ 累计命中率 4 张约 75%、
	// 5 张约 82%。往上加的边际收益掉得很快，而代价是线性的：每张票 2 发上游请求 + 烧掉一个
	// (上游账号 × 网关) 单位。
	//
	// 这个数是**供给闸**，调它之前先看这笔账：池子的冷却是 (消费账号 × 网关) 4 小时，已知
	// 99 个网关 ⇒ 一个消费账号的票预算 ≈ 99 ÷ 4h ≈ 25 张/小时。现场 16:10–17:12 这一小时
	// 烧了 40 张，超支 1.6 倍 ⇒ 池子对这个号报 all_cooling ⇒ 退避 60 秒 ⇒ 退避期里每一发
	// 业务请求都是 0.2 秒的 503 ⇒ Codex CLI 疯狂重发（五分钟 200 发）⇒ 用户看到的是「卡死」。
	// 所以做成旋钮而不是常数：供给（托管账号数 × 区域数）在涨，合适的值跟着它走。
	//
	// **按网关名算是对的**，别被「真实单位是 (消费账号 × 大区)」那个说法带走（2026-10-03 否了）：
	// 那个推断来自续期路径的 51 发实测（落点漂移 44 次），而续期按构造**必须摘掉 `__oailb`**，
	// 正好是唯一会漂的那条路。交付路径两件齐送 ⇒ 上游一个 cookie 都不回 ⇒ 钉住票上那个网关，
	// 对消费号是一个全新的单元（docs/conventions/codex-full-strength-tickets.md 的 C/D/F 三发）。
	gatewayPoolWarmMaxTickets = 5
	// gatewayPoolWarmMaxTicketsCeiling 是那个旋钮的硬上限。
	//
	// 封顶而不是任配：一轮预热最坏要花 N × 2 × gatewayPoolWarmShotTimeout 的墙上时间，而
	// 客户端在整段时间里一个字节都收不到；配到两位数等于把「首输出超时」变成常态。
	// 超了回默认值（同 gatewayPoolSeconds 的口径：填出这种数一定是打错了）。
	gatewayPoolWarmMaxTicketsCeiling = 8
	// gatewayPoolWarmBudget 是一次预热最多占用客户端多少墙上时间。
	// 5 张票 × 2 发 × 6s ≈ 60s，留一点余量；超了就停，别让客户端无限等。张数可按账号调（见上）。
	gatewayPoolWarmBudget = 90 * time.Second
	// gatewayPoolWarmMinBudget 是「还值得预热吗」的下限：一组判据两发、每发 3–6s，
	// 不到这个数就连一张票都验不完，白烧配额还要把业务请求的首输出预算拖进去。
	gatewayPoolWarmMinBudget = 15 * time.Second
	// gatewayPoolWarmShotTimeout 掐掉流：判据只要响应头，不等模型吐完。
	gatewayPoolWarmShotTimeout = 35 * time.Second
	// gatewayPoolWarmNoteTimeout 兜住写落点卡那一次 UpdateExtra。它在**业务请求出门之前**，
	// 不能没有上限（见 noteWarmVerdict）。
	gatewayPoolWarmNoteTimeout = 3 * time.Second
	// gatewayPoolWarmProbeText / Effort 是垫话。effort 是死变量（对降智没影响，2026-09 实测），
	// 取最低档只为省钱省时间。
	gatewayPoolWarmProbeText   = "hi"
	gatewayPoolWarmProbeEffort = "low"
)

// gatewayPoolWarmShooter 打一发垫话、只读响应头，返回状态码与上游下发的 state。
//
// 抽成函数类型只为一件事：让预热循环与判据在**不碰真实出站构造链**（身份暂存 → OAuth 变换 →
// 取 token → buildUpstreamRequest）的情况下被测到。生产路径只有一个实现。
type gatewayPoolWarmShooter func(ctx context.Context, cookie, state string) (int, string, error)

// gatewayPoolWarmUp 在业务请求之前把手里那张票验成满血，验不出来就报错。
//
// 返回 nil 的三种情形：手里那张还在满血窗口里（一发都不打）、这一发池子根本不接管（没东西可
// 验）、以及真的验出了一张满血的。非 nil 一律是「别放这发业务请求出去」。
func (s *OpenAIGatewayService) gatewayPoolWarmUp(request *http.Request, proxyURL string, account *Account) error {
	if s == nil || request == nil || request.URL == nil || !s.codexCookies.gatewayPoolTakeover(account) {
		return nil
	}
	// 只有推理面才有落点可验。先判路径再做别的：侧信道（装饰性 GET、/codex/alpha/search）
	// 也走这条咽喉，不先排掉的话它们每一发都会在下面打一条「读不出模型」的日志。
	if request.URL.Path != openAIGatewayPoolInferencePath {
		return nil
	}
	// **代理绑定这道闸必须在垫话之前自己查一遍**：它原本在 doOpenAIUpstreamOnce 里
	// （requireOpenAIProxyBinding，"last-mile guard"），而预热跑在那之前 ⇒ 代理行没预加载 /
	// 被删 / URL() 为空时（ProxyID 非 nil 而 proxyURL 为空），垫话会带着这个账号的 Bearer
	// 从服务器真实出口直连出去，而出口 IP 就是账号身份的一部分。
	if err := requireOpenAIProxyBinding(account, proxyURL); err != nil {
		return err
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(request.Context(), account)
	if err != nil {
		return err
	}
	// 手上那张**验过满血**而且还 Live ⇒ 窗口还开着 ⇒ 窗口内连打都满血，一发垫话都不打
	// （这条省掉了绝大多数成本）。
	//
	// 必须同时判「验过」和 Live，**不能只判 Live**：Live 的唯一含义是取票那一刻写的
	// `until = now + valid_for_s`（见 poolVerified 的注释列的两条路）。
	if s.codexCookies.gatewayPoolVerifiedFull(identity) {
		// 快路上顺手看一眼「这张票是不是快到点了」：是就在后台换下一张，让客户端下一次请求
		// 不用在这里等（openai_gwpool_prewarm.go）。到点判据很便宜（两次 map 读），而读 model
		// 要解请求体 —— 所以先问到点、再读 model。
		if account.gatewayPoolPrewarmEnabled() {
			if _, due := s.codexCookies.gatewayPoolPrewarmDue(identity); due {
				s.gatewayPoolPrewarm(request, proxyURL, account, identity, gatewayPoolWarmModel(request))
			}
		}
		return nil
	}
	// 模型必须和业务请求一致：state 绑在 (账号 × 模型 × 这张 cflb/oailb 对) 上，拿别的模型去
	// 验等于验了另一件事。读在 Live 快路之后：绝大多数请求走快路，不该为它们解一遍体。
	model := gatewayPoolWarmModel(request)
	if model == "" {
		// **fail closed，不是静默退回 retry**：运营方选这一档要的就是「绝不把降智交给客户端」，
		// 悄悄降级成「先放行再重发一次」是把他的选择抹掉，而唯一线索是一条日志。
		slog.Warn("gwpool_warm_no_model", "account_id", account.ID,
			"reason", "model unreadable from the pool sink, the turn-metadata header and the request body")
		return errOpenAIGatewayPoolWarmNoModel
	}
	return s.gatewayPoolWarmUpWith(request, account, identity, model,
		func(ctx context.Context, cookie, state string) (int, string, error) {
			return s.gatewayPoolWarmShot(ctx, account, proxyURL, cookie, model, state)
		})
}

func (s *OpenAIGatewayService) gatewayPoolWarmUpWith(
	request *http.Request,
	account *Account,
	identity, model string,
	shoot gatewayPoolWarmShooter,
) error {
	// 预算必须是一个**带截止时间的 ctx**，不能只在循环顶上判时间：一次 attempt 内部就能花掉
	// 两发垫话各 35s，只判循环顶的话 5 张票最坏能让客户端等六分钟 —— 而这一档对运营方承诺的
	// 是 90 秒。挂成 ctx 之后预算一到，排在后面的垫话立刻失败而不是各自再跑满 35s。
	//
	// **仍然会超一点**：取票那一步（gatewayPoolPair）刻意用 WithoutCancel + 自己的
	// gatewayPoolFetchTimeout，不吃这个 ctx 的截止时间（它要让排在后面的同账号请求别被第一名
	// 的断开连坐）⇒ 最坏会被一次取票超时拖过线。文案照这个实情写。
	budget, ok := gatewayPoolWarmBudgetFor(request.Context())
	if !ok {
		// 首输出守卫的额度已经不够验一张票 ⇒ **不预热，放行给业务请求**（退化成 cut 档）。
		// 不这样做的话预热会把守卫那点额度吃光，业务请求带着一个已经过期的 ctx 出门，守卫当场
		// 开火、报成 newOpenAIFirstOutputTimeoutError —— 那是个**按代理归因**的
		// UpstreamFailoverError，恰好是降智路径刻意不产出的那一类（它包 gwpool.ErrPool 就是
		// 为了让路由问题永远不去停一个真账号）。运营方会看到「首输出超时」挂在账号/代理上。
		slog.Warn("gwpool_warm_no_budget", "account_id", account.ID,
			"reason", "the first-output guard's remaining deadline is too short to verify a pair")
		return nil
	}
	ctx, cancel := context.WithTimeout(request.Context(), budget)
	defer cancel()
	rawURL := request.URL.String()
	tickets := account.gatewayPoolWarmTickets()
	// burned 是这一轮判死的落点名，只为放弃时那条终态日志能一行答完「试了哪几个网关」。
	burned := make([]string, 0, tickets)
attempts:
	for attempt := 1; attempt <= tickets; attempt++ {
		if ctx.Err() != nil {
			break
		}
		// 借 AttachRoute 取票：取票、本地账本筛选、exclude、force 换网关那一整套都在它里面，
		// 这里不另写一份选票逻辑。头是个丢弃用的容器，只为把 Cookie 取出来。
		headers := http.Header{}
		release, err := s.codexCookies.AttachRoute(ctx, account, rawURL, headers)
		if err != nil {
			return err
		}
		applied := openAIGatewayPoolSinkFrom(ctx).snapshot()
		cookie := headers.Get("Cookie")
		if cookie == "" || applied.Gateway == "" {
			// 没接管（罐回放）⇒ 没有落点可验。票也没取，没什么要还的。
			gatewayPoolReleaseUnsent(release)
			return nil
		}
		full, conclusive, sent, perr := s.codexCookies.gatewayPoolWarmVerdict(
			ctx, account, identity, applied, cookie, attempt, shoot)
		// 一个字节都没出去过的票还回池子（纯拨号失败那一格）。**不能指望业务请求那条路去还**：
		// 那边的 gatewayPoolPair 命中「缓存里还 Live」的早返回 ⇒ 这次调用没向池子取票 ⇒
		// fresh=false ⇒ release 恒为 nil ⇒ gatewayPoolReleasesUnsent 在 queue 档上是个空操作。
		// 拿到过状态码的一律不还：窗口真的烧了，还回去等于让池子把它当新鲜的再发给别人。
		if !conclusive && !sent {
			gatewayPoolReleaseUnsent(release)
		} else if !conclusive {
			// 下不了结论但**票确证打出去了** ⇒ 这个落点的窗口真的烧了。记一笔没有判定的接触，
			// 否则它只活在进程内存的 poolUsed 里、重启就没了，而落点记录才是 exclude 重启后
			// 的唯一来源（gatewayPoolHydrateUsed）。verdict 留空 = 不覆盖上一次判出来的结论。
			s.noteWarmVerdict(request, account, applied, "", false)
		}
		switch {
		case !conclusive && ctx.Err() != nil:
			// **预算在这一轮内部耗尽**，不是上游给了读数 ⇒ 和循环顶那条 break 同一口径：失败关闭。
			//
			// 这一格必须和下面那格分开，否则 queue 档在这里 fail-open：预算是挂在 ctx 上的，而
			// 取票那一步刻意不吃它（WithoutCancel + 自己的 gatewayPoolFetchTimeout，默认 25s）
			// ⇒ 循环顶只挡得住「整轮已超」，挡不住「某一轮内部超」。池子在冷却时取票能吃满 25s，
			// 三轮就过线；那时两发垫话在一个已死的 ctx 上立刻报错 → !conclusive → 放行，而手里
			// 那张是**刚 force 取回来、一发判据都没跑过**的票（这条路不标 Stale 也不标验过）
			// ⇒ 业务请求的 AttachRoute 读到 Live 原样复用它 ⇒ 「只用验过满血的槽」当场破掉，
			// 连 gatewayPoolWarmTickets 那个「最多烧几张」的上限也一起突破。
			slog.Warn("gwpool_warm_inconclusive", "account_id", account.ID,
				"gateway", applied.Gateway, "attempt", attempt, "budget_exhausted", true,
				"error", gatewayPoolWarmErrorText(perr))
			break attempts
		case !conclusive:
			// 上游真的给了读数但下不了结论（非 200、没回 state），或者真的传输失败
			// ⇒ **放行给业务请求**，不在这里把它判死。
			//
			// 理由不是宽容，是别把读数吞掉：非 200 的典型成因是限流/故障，而限流登记、瞬时熔断、
			// 故障转移全挂在业务响应那条路上。在这里返回错误的话，一个 5h 额度打满的 queue 账号
			// 永远不会被标限流 ⇒ 调度器继续选它 ⇒ 每一发客户端请求都换成「一次交付 + 一发真实
			// 429」，而供给是个位数张/小时。
			//
			// 放行**不会**把降智交给客户端：业务请求上的判据还在（queue 档 judges()=true），
			// 这一发退化成 cut 档的行为 —— 判到降智就截断回干净错误，让客户端自己重发。
			slog.Warn("gwpool_warm_inconclusive", "account_id", account.ID,
				"gateway", applied.Gateway, "attempt", attempt, "budget_exhausted", false,
				"error", gatewayPoolWarmErrorText(perr))
			return nil
		case full:
			s.codexCookies.gatewayPoolMarkVerifiedFull(identity, applied.Version)
			// **验完要复查这张票还在不在交付窗口里**：池子保证的剩余寿命下限是
			// openAIGatewayPoolMinRemaining(60s)，而两发慢垫话最坏 2×gatewayPoolWarmShotTimeout
			// (70s) 能活过它。过掉了的话 cachedPoolPair 已经是 Stale ⇒ 紧接着业务请求的
			// AttachRoute 会带 force=1 **另取一张从没验过的票**把客户端的 prompt 发出去，
			// 而日志是 gwpool_warm_ready —— 既破了承诺又报了假成功。换下一张重验。
			if !s.codexCookies.gatewayPoolVerifiedFull(identity) {
				slog.Warn("gwpool_warm_pair_expired_while_verifying", "account_id", account.ID,
					"gateway", applied.Gateway, "attempt", attempt)
				continue
			}
			slog.Info("gwpool_warm_ready", "account_id", account.ID,
				"gateway", applied.Gateway, "region", applied.Region, "tickets", attempt)
			// 满血这条连 `Current` 一起推进：它就是业务请求马上要落的那个网关。
			s.noteWarmVerdict(request, account, applied, openAIGatewayVerdictFull, true)
			// 票留在缓存里（Live + 已验）⇒ 紧接着业务请求那一发的 AttachRoute 会原样复用它。
			return nil
		default:
			// 判成降智：标 Stale ⇒ 下一圈的 AttachRoute 天然带 force=1 + exclude_versions 换网关。
			// 本地账本不用再记一笔 —— 取票那一刻 gatewayPoolPair 已经记过了（它按实际拿到的落点记）。
			// **刻意不还槽位**：这一发真的打出去了，窗口真的烧了，还回去等于让池子再发一次。
			s.codexCookies.gatewayPoolMarkStale(identity, applied.Version, applied.Gateway)
			slog.Info("gwpool_warm_degraded", "account_id", account.ID,
				"gateway", applied.Gateway, "attempt", attempt)
			burned = append(burned, applied.Gateway)
			// 判死的落点也记进卡里，但**不推进 `Current`**：没有业务请求会落上去，推进了会把
			// 「当前网关」写成最后一个被判死的落点。不记的话这一档每轮真烧 4 个 (账号 × 网关)
			// + 8 发上游配额，而事后在页面上一条痕迹都没有 —— 失败路径不落用量行（RecordUsage
			// 只在转发成功时跑），本地账本 poolUsed 重启即失，只剩 slog。
			s.noteWarmVerdict(request, account, applied, openAIGatewayVerdictDegraded, false)
		}
	}
	// 试满了 / 预算用尽，都没验出满血：按失败处理，**绝不降级放行**。
	//
	// 错误和「真判到降智」刻意分开：这一发的业务请求一个字节都没出去过、被标记的是**好几个**
	// 网关而不是「当前网关」，而且「稍后重试即可」在这里是最坏的建议 —— Codex CLI 对 503 会
	// 自动重发，每一次重发都可能再烧几张票，而供给是个位数张/小时。
	//
	// 这条 Warn 是放弃那一刻**唯一**的终态读数：以前这两条路一条打 budget_exhausted、一条
	// 什么都不打，ops 只能靠「数了 4 条 degraded 又没见到 ready」反推。
	slog.Warn("gwpool_warm_exhausted", "account_id", account.ID,
		"tickets", len(burned), "gateways", strings.Join(burned, ","),
		"budget_exhausted", ctx.Err() != nil)
	return errOpenAIGatewayPoolWarmExhausted
}

// gatewayPoolWarmBudgetFor 算这次预热能花多少墙上时间，并和**首输出守卫的截止时间**对账。
//
// 守卫的截止时间是 `startTime + openai_first_output_timeout_seconds` 的**绝对时刻**
// （openai_gateway_forward.go 的 newOpenAIFirstOutputHeaderGuard），而预热花掉的是同一段墙上
// 时间 —— 两者不对账的话，预热跑完业务请求就带着一个已经过期的 ctx 出门。
//
// 留**一半**给业务请求自己的首输出：没有更有依据的分法（守卫那个值是运营方按模型吐字速度配的，
// 和判据成本无关），一半是能说清楚的那个取舍。剩下不够验一张票就返回 false。
// 没有截止时间（守卫没开，缺省就是没开）时原样给满额。
//
// **预算按账号各发一份，故障转移换号时不累计**（2026-10-02 用户拍板）：每个账号碰过的票不一样，
// A 号烧光自己的额度不代表 B 号没有满血落点可试，共享一份会让排在后面的号拿不到公平的机会。
// 代价是客户端的零输出时间按换号次数叠加 —— 这一侧的刹车改成「每轮更便宜」（试票上限可配，
// 见 gatewayPoolWarmTickets）和「失败带 Retry-After」（gatewayPoolRetryAfter），而不是砍预算。
func gatewayPoolWarmBudgetFor(ctx context.Context) (time.Duration, bool) {
	budget := gatewayPoolWarmBudget
	if deadline, ok := ctx.Deadline(); ok {
		if half := time.Until(deadline) / 2; half < budget {
			budget = half
		}
	}
	if budget < gatewayPoolWarmMinBudget {
		return 0, false
	}
	return budget, true
}

// noteWarmVerdict 把预热判出来的结论写进账号的落点记录（openai_gwpool_gateway_history.go）。
//
// 不写的话这一档恰好是新状态色最看不见的一档：落点卡上的判定本来只由**业务请求**上的判据产出
// （gatewayPoolRouteDegraded），而 queue 档的全部判据都发生在业务请求之前 —— 开了它的账号卡上
// 会永远只有「碰过但没判据」那一色，运营方会以为它没在工作。
//
// advanceCurrent 决定要不要连 `Current`（卡片第一行「当前大区 · 当前网关」）一起推进：
// 只有满血那条该推 —— 它就是业务请求马上要落的落点。判死的那些只进 Seen。
//
// 脱离请求取消、但**自己带超时**：这条读数要在客户端刚好断开的那一瞬也写得进去（同
// dropDegradedGatewayPoolRoute 的取舍），可 context.WithoutCancel 连 deadline 一起剥掉，而
// 这一发卡在**业务请求还没出门之前**（原来的调用点在 RecordUsage 里，响应已经收尾、卡住只是
// 读数晚到）。库一慢，queue 档本来就零字节等着的客户端会再多等一个没有上限的写库。
// UpdateExtra 自述会连带 GetByID + Redis 写，所以这个超时不能省。
func (s *OpenAIGatewayService) noteWarmVerdict(
	request *http.Request,
	account *Account,
	applied OpenAIGatewayPoolApplied,
	verdict string,
	advanceCurrent bool,
) {
	ctx, cancel := context.WithTimeout(
		context.WithoutCancel(request.Context()), gatewayPoolWarmNoteTimeout)
	defer cancel()
	s.noteOpenAIGatewayUse(ctx, account, applied.Gateway, applied.Region, verdict, advanceCurrent)
}

// gatewayPoolWarmVerdict 跑一组判据，并把**同一张票上的并发预热收口成一次**（见 poolWarm）。
//
// 键两段都是承重的：
//   - 身份 —— 一张票能同时服务多个消费账号（2026-10-01 实测），而降智的单位是 (消费账号 × 网关)，
//     只按票号收口会把别的账号的结论拿来当自己的
//   - 票号 —— 换票就是换落点，结论不能继承
//
// **模型刻意不进键**：判据问的是「这个 (消费账号 × 网关) 还满不满血」，那是个与模型无关的两态
// （docs/conventions/codex-full-strength-tickets.md）。模型只约束**一组判据内部**要自洽
// （shot A 铸出来的 state 绑在那个模型上，shot B 必须用同一个去 echo），而那是在
// gatewayPoolWarmProbe 内部保证的 —— 两发用的是同一个 shoot 闭包。
// 把模型放进键会让同账号两个模型并发时各打一组判据，白烧一个落点换一个必然相同的结论；
// 而且 poolVerified 的快路（键只有身份）比这里先命中，放进去那一段在真实时序下基本是死的。
//
// **不脱离取消**（和 gatewayPoolPair 里那次 /cookie 刻意相反）：那边脱离是因为 /cookie 便宜、
// 共享、且不该被第一名的断开杀掉；这边两发垫话要吃调用方的预算（gatewayPoolWarmBudget）和
// 首输出守卫的取消 —— 脱离了就等于「客户端这一发已经注定超时，垫话还在跑并占着并发槽」。
// 代价是跟随者可能被领头者的取消连坐成「判不出来」，而那条路现在是放行给业务请求，不是失败。
//
// 池子没报票号（version 为空）时不收口：那时没有稳定的键，宁可各打一遍也不许把两张不同的票
// 的结论混成一个。
func (s *openAICodexCookieStore) gatewayPoolWarmVerdict(
	ctx context.Context,
	account *Account,
	identity string,
	applied OpenAIGatewayPoolApplied,
	cookie string,
	attempt int,
	shoot gatewayPoolWarmShooter,
) (full, conclusive, sent bool, err error) {
	if applied.Version == "" {
		return gatewayPoolWarmProbe(ctx, account.ID, applied.Gateway, attempt, cookie, shoot)
	}
	type verdict struct{ full, conclusive, sent bool }
	key := gatewayPoolLedgerIdentity(identity) + "\x00" + applied.Version
	got, err, _ := s.poolWarm.Do(key, func() (any, error) {
		full, conclusive, sent, perr := gatewayPoolWarmProbe(ctx, account.ID, applied.Gateway, attempt, cookie, shoot)
		return verdict{full: full, conclusive: conclusive, sent: sent}, perr
	})
	v, _ := got.(verdict)
	return v.full, v.conclusive, v.sent, err
}

// gatewayPoolVerifiedFull 报告这个身份手上那张票验过满血、而且还在交付窗口里。
func (s *openAICodexCookieStore) gatewayPoolVerifiedFull(identity string) bool {
	pair, state := s.cachedPoolPair(identity)
	if state != openAIGatewayPoolPairLive || pair.version == "" {
		return false
	}
	mark, ok := s.gatewayPoolVerifiedMarkOf(identity)
	return ok && mark.version == pair.version
}

// gatewayPoolVerifiedMarkOf 读那一笔「验过满血」的记录（票号 + 判出来的时刻）。
func (s *openAICodexCookieStore) gatewayPoolVerifiedMarkOf(identity string) (gatewayPoolVerifiedMark, bool) {
	if s == nil || identity == "" {
		return gatewayPoolVerifiedMark{}, false
	}
	value, loaded := s.poolVerified.Load(identity)
	if !loaded {
		return gatewayPoolVerifiedMark{}, false
	}
	mark, ok := value.(gatewayPoolVerifiedMark)
	return mark, ok
}

// gatewayPoolMarkVerifiedFull 记「这个身份手上这张票验过满血」。票号为空（池子没报）时不记：
// 那就认不出换没换票，宁可下一发再验一遍。
//
// 时刻只在**票号变了**的时候推进：同一张票被重复标（并发预热各标一次、前台验完复查那一下）
// 不许把窗口起点往后推 —— 推了就等于每标一次都把「这张票还能满血多久」重算一遍，
// 满血时长的样本会被系统性拉长，后台预热跟着越来越晚。
func (s *openAICodexCookieStore) gatewayPoolMarkVerifiedFull(identity, version string) {
	if s == nil || identity == "" || version == "" {
		return
	}
	if mark, ok := s.gatewayPoolVerifiedMarkOf(identity); ok && mark.version == version {
		return
	}
	s.poolVerified.Store(identity, gatewayPoolVerifiedMark{version: version, at: time.Now()})
}

// gatewayPoolWarmProbe 跑一组 state-echo：A 只带 cookie 拿一张 state，B 带 cookie + 那张 state
// 看上游还不还新的。
//
// conclusive=false 表示**没下结论**（传输失败、非 200、A 没回 state），调用方不许把它当降智 ——
// 判据纪律 1：非 200 一律不下结论。A 不可省：state 绑在这张票上，换一张就得重新取。
//
// sent 报告这张票**有没有确证送达上游**（至少拿到过一个状态码）。只有它为假（纯拨号/传输失败）
// 时才允许把槽位还回池子：拿到过状态码就意味着窗口真的烧了，还回去会让池子把它当新鲜的再发给
// 别人。注意 shot B 传输失败时 sent 仍为真 —— A 已经 200 过，落点已经被碰了。
//
// gateway / attempt 只进日志：没有它们的话，「烧了 8 发垫话却没放行」这个问题要靠时间戳把
// gwpool_warm_probe 和 gwpool_warm_degraded 交错着拼，而同账号两路并发预热（poolWarm 只在
// 票号相同时才收口）一上来就拼不出来了。
func gatewayPoolWarmProbe(
	ctx context.Context,
	accountID int64,
	gateway string,
	attempt int,
	cookie string,
	shoot gatewayPoolWarmShooter,
) (full, conclusive, sent bool, err error) {
	status, ticket, err := shoot(ctx, cookie, "")
	slog.Info("gwpool_warm_probe", "account_id", accountID, "gateway", gateway,
		"attempt", attempt, "shot", "a",
		"status", status, "got_state", ticket != "", "error", gatewayPoolWarmErrorText(err))
	sent = err == nil // 拿到了状态码 = 这一发确证到过上游
	if err != nil || status != http.StatusOK || ticket == "" {
		return false, false, sent, err
	}
	status, fresh, err := shoot(ctx, cookie, ticket)
	slog.Info("gwpool_warm_probe", "account_id", accountID, "gateway", gateway,
		"attempt", attempt, "shot", "b",
		"status", status, "got_state", fresh != "", "error", gatewayPoolWarmErrorText(err))
	if err != nil || status != http.StatusOK {
		return false, false, true, err
	}
	// 不回新 state、或回下送出去的同一张 ⇒ 票被接受 ⇒ 满血。回了一张**不同的** ⇒ 被重铸 ⇒ 降智。
	return fresh == "" || fresh == ticket, true, true, nil
}

// gatewayPoolWarmShot 打一发垫话，只读响应头。state 非空时带上它（shot B）。
func (s *OpenAIGatewayService) gatewayPoolWarmShot(
	ctx context.Context,
	account *Account,
	proxyURL, cookie, model, state string,
) (int, string, error) {
	shotCtx, cancel := context.WithTimeout(ctx, gatewayPoolWarmShotTimeout)
	defer cancel()
	_, req, err := s.buildOpenAITurnStateProbe(shotCtx, account, model, gatewayPoolWarmProbeEffort, gatewayPoolWarmProbeText)
	if err != nil {
		return 0, "", err
	}
	// 池子那张 pair 整串替上去。**绝不摘 __oailb**：摘掉就漂回这个号在自己那个大区的网关
	// （2026-10-02 实测 9/9），验的就不是手上这个落点了。
	req.Header.Set("Cookie", cookie)
	if state != "" {
		req.Header.Set(openAICodexTurnStateHeader, state)
	}
	// 不设 req.Close：垫话和紧接着的业务请求要走**同一条**连接/同一个出口，关掉就换出口了。
	//
	// **走 doOpenAIUpstreamRoundTrip 而不是直接 httpUpstream.Do**：装了接管 OAuth 出站的插件时，
	// 业务请求走插件、裸打 httpUpstream 走的是另一条传输层（TLS 指纹都不同，见
	// buildOpenAITurnStateProbe 的注释）—— 那就成了「在 A 上量、给 B 放行」的空闸。
	// 判据必须和它要放行的那一发走同一条路。
	resp, err := s.doOpenAIUpstreamRoundTrip(req, proxyURL, account)
	if err != nil {
		return 0, "", err
	}
	// 头到手即断：提前 Close 让 HTTP/2 发 RST_STREAM，上游立刻停止生成（同
	// dropDegradedGatewayPoolRoute 的手法）。Drain 一点点是为了让非 200 的错误体不卡在内核缓冲。
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
		_ = resp.Body.Close()
	}()
	return resp.StatusCode, extractOpenAICodexTurnState(resp.Header), nil
}

// gatewayPoolWarmModel 读这一发出站请求的 model，按可靠度排三个来源。
//
//  1. **本次请求的网关池 sink** —— 由 buildUpstreamRequest 在压缩之前从明文体里记的
//     （openai_gateway_forward.go）。这是唯一恒定可靠的那个：所有经转发入口的出站请求都过它。
//  2. turn-metadata 头 —— 双开账号的头里有本轮 model。注意它**只在入站本来就带这个字段时才
//     对齐**（alignCodexTurnMetadataJSON 的 `if current, ok := metadata[name]; ok`），所以
//     0.156 之前的客户端、桥接口过来的请求都没有。
//  3. 明文请求体 —— 非双开账号的出站体是明文 JSON。双开账号这里是 zstd，必然解不出
//     （compressCodexRequestBody；仓库自己的测试断言「明文不得直接上线」）。
//
// 为什么不读 gin 上下文：模型名在那儿（OpsUpstreamModelKey），而这一层只拿到 *http.Request。
// 三个都读不出来返回空串，调用方 fail closed。
func gatewayPoolWarmModel(request *http.Request) string {
	if model := openAIGatewayPoolSinkFrom(request.Context()).modelOf(); model != "" {
		return model
	}
	if model := strings.TrimSpace(gjson.Get(request.Header.Get(openAIWSTurnMetadataHeader), "model").String()); model != "" {
		return model
	}
	if request.GetBody == nil {
		return ""
	}
	body, err := request.GetBody()
	if err != nil {
		return ""
	}
	defer func() { _ = body.Close() }()
	var payload struct {
		Model string `json:"model"`
	}
	if err := json.NewDecoder(body).Decode(&payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.Model)
}

// gatewayPoolWarmErrorText 把错误脱敏成一行日志值（空错误给空串）。
//
// 能进来的错误闭集很窄：构造链的错误、拨号/传输层的 *url.Error（URL 里没有凭据，口令由
// proxyurl.Parse 的 Redacted() 挡住），以及 ctx 超时 —— 响应体一个字节都没进过错误
// （gatewayPoolWarmShot 把它整个丢给 io.Discard）。所以这里不需要比
// sanitizeUpstreamErrorMessage 更强的脱敏；反过来也别把这个函数挪到能看到响应体的地方。
func gatewayPoolWarmErrorText(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return sanitizeUpstreamErrorMessage(err.Error())
}
