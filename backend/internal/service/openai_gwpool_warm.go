package service

// 网关质量防护默认开启，openai_gwpool_guard_enabled=false 时跳过。
// 开启时业务只能使用已验证且仍在租约内的票；预算不足或结果未知均拒绝，不把未知记成降级。
// 一组验证先取 state，再带回同一张 state 读响应头；验过的 Live 票复用，不每次重验。
// 这些额外请求消耗上游配额，但没有实测 token，不能计费给客户端 API Key。
// 请求尝试、结果、耗时落 openai_gwpool_metrics，详细过程仍记 gwpool_warm_probe；
// 历史首次可见日志不是账号真实首次接触，不能当成首次满血率。
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
	// Retained only for validating legacy saved settings. Preparation no longer
	// reads these limits; the candidate queue and each caller's deadline govern it.
	gatewayPoolWarmMaxTickets        = 5
	gatewayPoolWarmMaxTicketsCeiling = 8
	// Hard limit for one ticket's A/B (also used by post-response confirmation),
	// not the lifetime of the shared candidate queue.
	gatewayPoolWarmBudget = 90 * time.Second
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
	if s == nil || request == nil || request.URL == nil || !s.codexCookies.gatewayPoolTakeover(account) ||
		!account.gatewayPoolGuardEnabled() {
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
	if !s.codexCookies.gatewayPoolEarlyModelMatches(identity, gatewayPoolProbeModelLuna) {
		return errOpenAIGatewayPoolWarmUnverified
	}
	// A locally verified current ticket is shared across models. Live means
	// neither rejected nor past its route credential deadline; no reference TTL.
	model := account.gatewayPoolProbeModel(gatewayPoolWarmModel(request))
	if pair, state := s.codexCookies.cachedPoolPair(identity); state == openAIGatewayPoolPairLive && pair.early != nil {
		model = pair.early.model
	}
	if s.codexCookies.gatewayPoolVerifiedFullFor(identity, model) {
		return nil
	}
	// Luna is the default. Explicit "business" follows the request model.
	// Both probe requests use one model; no cross-model state injection.
	// Existing verified windows remain valid until their normal end.
	if model == "" {
		// **fail closed，不是静默退回 retry**：运营方选这一档要的就是「绝不把降智交给客户端」，
		// 悄悄降级成「先放行再重发一次」是把他的选择抹掉，而唯一线索是一条日志。
		slog.Warn("gwpool_warm_no_model", "account_id", account.ID,
			"reason", "model unreadable from the pool sink, the turn-metadata header and the request body")
		return errOpenAIGatewayPoolWarmNoModel
	}
	if account.gatewayPoolEarlyEnabled() {
		ctx := context.WithValue(request.Context(), gatewayPoolEarlyIntentKey{}, &gatewayPoolEarlyIntent{
			ctx: request.Context(), model: gatewayPoolProbeModelLuna,
		})
		request = request.WithContext(ctx)
	}
	return s.gatewayPoolWarmUpWith(request, account, identity, model,
		func(ctx context.Context, cookie, state string) (int, string, error) {
			probeModel, _ := ctx.Value(gatewayPoolProbeModelKey{}).(string)
			return s.gatewayPoolWarmShot(ctx, account, proxyURL, cookie, probeModel, state)
		})
}

func (s *OpenAIGatewayService) gatewayPoolPrepare(
	request *http.Request,
	account *Account,
	identity, model string,
	shoot gatewayPoolWarmShooter,
) (resultErr error) {
	ctx := context.WithValue(request.Context(), gatewayPoolProbeModelKey{}, model)
	progress := s.startGatewayPoolProgress(ctx, account, identity)
	s.codexCookies.poolPrepareProgress.Store(identity, progress)
	phase := "unknown"
	defer func() {
		if request.Context().Err() != nil {
			phase = "cancelled"
		} else if errors.Is(resultErr, errOpenAIGatewayPoolWarmExhausted) {
			phase = "exhausted"
		}
		s.codexCookies.poolProgress.update(progress, phase, 0, "", false, true)
	}()
	// There is no fixed 5/8-ticket loop or leader-owned 90s deadline.
	// Every network operation is bounded; only live waiters keep this worker alive.
	rawURL := request.URL.String()
	recoveries := 0
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		fresh, err := s.freshGatewayPoolPreparationAccount(ctx, account)
		if err != nil {
			return err
		}
		if !gatewayPoolWaitAccountMatches(fresh, account) {
			return errOpenAIGatewayPoolWarmUnverified
		}
		if allowed, err := s.gatewayPoolResumeAllowed(ctx, fresh, false); err != nil || !allowed {
			return gatewayPoolRestError()
		}
		s.codexCookies.poolProgress.update(progress, "fetching", 0, "", false, false)
		// 借 AttachRoute 取票：取票、本地账本筛选、exclude、force 换网关那一整套都在它里面，
		// 这里不另写一份选票逻辑。头是个丢弃用的容器，只为把 Cookie 取出来。
		headers := http.Header{}
		release, err := s.attachGatewayPoolRouteWithWait(ctx, account, rawURL, headers)
		if err != nil {
			if errors.Is(err, errGatewayPoolGenerationChanged) {
				continue // a manual clear invalidates old preparation, not the caller's deadline
			}
			if ctx.Err() == nil && recoveries < fresh.gatewayPoolPreparationRecoveries() &&
				gatewayPoolRetryablePreparationError(err) {
				recoveries++
				retry, waitErr := s.waitGatewayPoolRetry(ctx, account, gatewayPoolVerificationRetryGap)
				if waitErr != nil {
					return waitErr
				}
				if retry {
					continue
				}
			}
			return err
		}
		applied := openAIGatewayPoolSinkFrom(ctx).snapshot()
		cookie := headers.Get("Cookie")
		if cookie == "" || applied.Gateway == "" {
			// 没接管（罐回放）⇒ 没有落点可验。票也没取，没什么要还的。
			gatewayPoolReleaseUnsent(release)
			return nil
		}
		probeCtx, probeCancel := context.WithTimeout(ctx, gatewayPoolWarmBudget)
		early := applied.early
		if early != nil {
			intent, _ := ctx.Value(gatewayPoolEarlyIntentKey{}).(*gatewayPoolEarlyIntent)
			if intent == nil || intent.model != early.model {
				probeCancel()
				return errOpenAIGatewayPoolWarmUnverified
			}
			probeCtx = context.WithValue(probeCtx, gatewayPoolProbeModelKey{}, early.model)
		}
		ticketKey := applied.Version
		if ticketKey == "" {
			ticketKey = gatewayPoolUsageDigest(cookie)
		}
		onTried := func() { s.codexCookies.poolProgress.tried(progress, ticketKey) }
		probeCtx = context.WithValue(probeCtx, gatewayPoolProgressTriedKey{}, onTried)
		s.codexCookies.poolProgress.update(progress, "verifying", 0, applied.Gateway, false, false)
		full, conclusive, sent, firstSent, perr := s.codexCookies.gatewayPoolWarmVerdict(
			probeCtx, account, identity, applied, cookie, attempt, shoot)
		probeCancel()
		if !firstSent.IsZero() {
			onTried()
		}
		if !firstSent.IsZero() {
			// 同凭证的克隆行可能跟随同一次单飞；把领头者实际出站时刻带回本行缓存。
			s.codexCookies.gatewayPoolMarkSent(identity, applied.Version, firstSent)
		}
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
			// A cancelled/expired verification is unknown, never a full or
			// degraded verdict. In particular it must not release business.
			slog.Warn("gwpool_warm_inconclusive", "account_id", account.ID,
				"gateway", applied.Gateway, "attempt", attempt, "budget_exhausted", true,
				"error", gatewayPoolWarmErrorText(perr))
			return ctx.Err()
		case !conclusive:
			// 无法判断也拒绝业务，但不把它记成降级；HTTP限流/认证状态由 WarmShot 登记。
			slog.Warn("gwpool_warm_inconclusive", "account_id", account.ID,
				"gateway", applied.Gateway, "attempt", attempt, "budget_exhausted", false,
				"error", gatewayPoolWarmErrorText(perr))
			if early == nil && recoveries < fresh.gatewayPoolPreparationRecoveries() && gatewayPoolRetryableProbeError(perr) {
				recoveries++
				s.codexCookies.poolProgress.update(progress, "waiting", 0, applied.Gateway, false, false)
				retry, waitErr := s.waitGatewayPoolRetry(ctx, account, gatewayPoolVerificationRetryGap)
				if waitErr != nil {
					return waitErr
				}
				if retry {
					continue
				}
			}
			return errOpenAIGatewayPoolWarmUnverified
		case full:
			probeModel, _ := probeCtx.Value(gatewayPoolProbeModelKey{}).(string)
			// Recheck the actual current ticket: a concurrent replacement,
			// confirmed rejection or credential expiry still needs a new preflight.
			if !s.codexCookies.gatewayPoolVerifiedFullFor(identity, probeModel) {
				slog.Warn("gwpool_warm_pair_expired_while_verifying", "account_id", account.ID,
					"gateway", applied.Gateway, "attempt", attempt)
				continue
			}
			slog.Info("gwpool_warm_ready", "account_id", account.ID,
				"gateway", applied.Gateway, "region", applied.Region, "tickets", attempt)
			// 满血这条连 `Current` 一起推进：它就是业务请求马上要落的那个网关。
			s.noteWarmVerdict(request, account, applied, openAIGatewayVerdictFull, true)
			// 票留在缓存里（Live + 已验）⇒ 紧接着业务请求那一发的 AttachRoute 会原样复用它。
			phase = "ready"
			return nil
		default:
			// 判成降智：标 Stale ⇒ 下一圈的 AttachRoute 天然带 force=1 + exclude_versions 换网关。
			// 本地账本不用再记一笔 —— 取票那一刻 gatewayPoolPair 已经记过了（它按实际拿到的落点记）。
			// **刻意不还槽位**：这一发真的打出去了，窗口真的烧了，还回去等于让池子再发一次。
			held := s.codexCookies.gatewayPoolMarkStale(identity, applied.Version, applied.Gateway)
			applied.FullHeldMs = held.Milliseconds()
			slog.Info("gwpool_warm_degraded", "account_id", account.ID,
				"gateway", applied.Gateway, "attempt", attempt)
			s.codexCookies.poolProgress.update(progress, "fetching", 0, applied.Gateway, true, false)
			// 判死的落点也记进卡里，但**不推进 `Current`**：没有业务请求会落上去，推进了会把
			// 「当前网关」写成最后一个被判死的落点。不记的话这一档每轮真烧 4 个 (账号 × 网关)
			// + 8 发上游配额，而事后在页面上一条痕迹都没有 —— 失败路径不落用量行（RecordUsage
			// 只在转发成功时跑），本地账本 poolUsed 重启即失，只剩 slog。
			s.noteWarmVerdict(request, account, applied, openAIGatewayVerdictDegraded, false)
			if early != nil {
				return errGatewayPoolWarmAttemptsFinished
			}
			if applied.PoolLive > 0 && s.gatewayPoolNoRemainingRoutes(ctx, account) {
				return errOpenAIGatewayPoolWarmExhausted
			}
		}
	}
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
	s.noteGatewayPoolCooldownVerdict(ctx, account, applied, verdict)
	s.noteOpenAIGatewayUse(context.WithValue(ctx, gatewayPoolObservationEpochKey{}, applied.cooldownResetAt), account, applied.Gateway, applied.Region, verdict, advanceCurrent,
		applied.PoolLive, applied.PoolFree, applied.FullHeldMs, applied.LedgerTag)
}

// gatewayPoolWarmVerdict 跑一组判据，并把**同一张票上的并发预热收口成一次**（见 poolWarm）。
//
// 键两段都是承重的：
//   - 身份 —— 一张票能同时服务多个消费账号（2026-10-01 实测），而降智的单位是 (消费账号 × 网关)，
//     只按票号收口会把别的账号的结论拿来当自己的
//   - 票号 —— 换票就是换落点，结论不能继承
//
// Eligibility is shared across models; only the model actually probed is recorded.
// 共享工作有独立硬超时，各调用方仍受自己的预算约束。最后一个等待者退出才取消
// 共享工作，避免领头断开连坐其它请求，也避免无人等待时继续消耗探针。
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
) (full, conclusive, sent bool, firstSent time.Time, err error) {
	finish := s.gatewayPoolInventoryOperation(identity)
	defer finish()
	type verdict = gatewayPoolEarlyVerdict
	probe := func(work context.Context) (verdict, error) {
		model, _ := work.Value(gatewayPoolProbeModelKey{}).(string)
		if pair, state := s.cachedPoolPair(identity); state == openAIGatewayPoolPairLive &&
			pair.version == applied.Version && s.gatewayPoolVerifiedFullFor(identity, model) {
			return verdict{full: true, conclusive: true}, nil
		}
		start := time.Now()
		trace := &gatewayPoolProbeTrace{onFirst: func(at time.Time) {
			s.gatewayPoolMarkSent(identity, applied.Version, at)
			if onTried, ok := work.Value(gatewayPoolProgressTriedKey{}).(func()); ok {
				onTried()
			}
			if s.poolUsageAttempt != nil && gatewayPoolProbeSource(work) == "foreground" {
				s.poolUsageAttempt(work, account, identity, model, applied, at, false)
			}
		}}
		probeCtx := context.WithValue(work, gatewayPoolProbeTraceKey{}, trace)
		steps := make([]gatewayPoolProbeStep, 0, 2)
		wrapped := func(ctx context.Context, cookie, state string) (int, string, error) {
			at := time.Now()
			before := trace.shots
			trace.actualGateway = ""
			status, minted, err := shoot(ctx, cookie, state)
			// 离线 shooter 或其它实现未显式打点时，有响应就能确认出站；
			// 纯构造失败/未发出的错误不凭空算触碰或探测次数。
			if status > 0 && trace.shots == before {
				trace.markSent(at)
			}
			s.poolRounds.touch(identity, trace.lastSent)
			shot := "a"
			if len(steps) > 0 {
				shot = "b"
			}
			steps = append(steps, gatewayPoolProbeStep{Shot: shot, Sent: trace.shots > before, Status: status,
				GotState: minted != "", EchoAccepted: state != "" && status == http.StatusOK && (minted == "" || minted == state),
				ActualGateway: trace.actualGateway, DurationMS: time.Since(at).Milliseconds()})
			return status, minted, err
		}
		full, conclusive, sent, err := gatewayPoolWarmProbe(probeCtx, account.ID, applied.Gateway, attempt, cookie, wrapped)
		if full && conclusive {
			s.gatewayPoolMarkVerifiedFull(identity, applied.Version, model)
		} else if conclusive {
			applied.FullHeldMs = s.gatewayPoolMarkStale(identity, applied.Version, applied.Gateway).Milliseconds()
		}
		// Ambiguous transmission forbids returning a ticket but is not a measured contact.
		sent = sent || trace.shots > 0 || trace.mayHaveSent
		if s.poolProbeObserved != nil {
			model, _ := work.Value(gatewayPoolProbeModelKey{}).(string)
			s.poolProbeObserved(work, account, gatewayPoolProbeObservation{
				Source: gatewayPoolProbeSource(work), Shots: trace.shots, Full: full, Conclusive: conclusive,
				DurationMS: time.Since(start).Milliseconds(),
				Applied:    applied, Identity: identity, Model: model, FirstSent: trace.firstSent, LastSent: trace.lastSent, Steps: steps,
			})
		}
		return verdict{full: full, conclusive: conclusive, sent: sent, firstSent: trace.firstSent}, err
	}
	model, _ := ctx.Value(gatewayPoolProbeModelKey{}).(string)
	if early := applied.early; early != nil {
		if model != early.model || ctx.Err() != nil {
			return false, false, false, time.Time{}, errOpenAIGatewayPoolWarmUnverified
		}
		originalProbe := probe
		probe = func(work context.Context) (verdict, error) {
			early.once.Do(func() {
				started := time.Now()
				v, err := originalProbe(work)
				if !v.conclusive {
					// A one-shot result is immutable. Retire only this exact early
					// ticket; unknown is not a quality failure or cooldown sample.
					s.gatewayPoolMarkStaleMatched(identity, applied.Version, applied.Gateway, true)
				}
				v.err = err
				early.result = v
				close(early.done)
				slog.Info("gwpool_early_result", "account_id", account.ID, "gateway", applied.Gateway,
					"model", early.model, "full", v.full, "conclusive", v.conclusive,
					"sent", !v.firstSent.IsZero(), "may_have_sent", v.sent, "first_sent_at", v.firstSent,
					"reserved_at", early.reservedAt, "duration_ms", time.Since(started).Milliseconds())
			})
			return early.result, early.result.err
		}
	}
	if applied.Version == "" {
		v, err := probe(ctx)
		return v.full, v.conclusive, v.sent, v.firstSent, err
	}
	key := gatewayPoolLedgerIdentity(identity) + "\x00" + applied.Version
	timeout := gatewayPoolWarmBudget
	if pair, state := s.cachedPoolPair(identity); pair.version == applied.Version {
		if state != openAIGatewayPoolPairLive {
			return false, false, false, time.Time{}, errOpenAIGatewayPoolWarmUnverified
		}
	}
	v, err := s.poolWarm.do(ctx, key, timeout, probe, func() func() {
		finish := s.gatewayPoolInventoryOperation(identity)
		return func() {
			finish()
			if s.poolUsageFinished != nil {
				s.poolUsageFinished(context.WithoutCancel(ctx), account)
			}
		}
	})
	return v.full, v.conclusive, v.sent, v.firstSent, err
}

// gatewayPoolVerifiedFull reports local proof on the current usable route.
func (s *openAICodexCookieStore) gatewayPoolVerifiedFull(identity string) bool {
	pair, state := s.cachedPoolPair(identity)
	if state != openAIGatewayPoolPairLive || pair.version == "" {
		return false
	}
	mark, ok := s.gatewayPoolVerifiedMarkOf(identity)
	return ok && mark.version == pair.version
}

func (s *openAICodexCookieStore) gatewayPoolVerifiedFullFor(identity, model string) bool {
	return s.gatewayPoolVerifiedVersionFor(identity, model) != ""
}

func (s *openAICodexCookieStore) gatewayPoolVerifiedVersionFor(identity, _ string) string {
	pair, live := s.cachedPoolPair(identity)
	mark, ok := s.gatewayPoolVerifiedMarkOf(identity)
	if live != openAIGatewayPoolPairLive || pair.version == "" || !ok || mark.version != pair.version {
		return ""
	}
	return pair.version
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
func (s *openAICodexCookieStore) gatewayPoolMarkVerifiedFull(identity, version string, models ...string) {
	if s == nil || identity == "" || version == "" {
		return
	}
	model := ""
	if len(models) > 0 {
		model = models[0]
	}
	for {
		pair, state := s.cachedPoolPair(identity)
		if state != openAIGatewayPoolPairLive || pair.version != version {
			return
		}
		prev, loaded := s.poolVerified.Load(identity)
		old, _ := prev.(gatewayPoolVerifiedMark)
		proofs := map[string]time.Time{}
		next := gatewayPoolVerifiedMark{version: version, at: time.Now(), models: &proofs}
		if old.version == version {
			next.at = old.at
			if old.models != nil {
				for name, at := range *old.models {
					proofs[name] = at
				}
			}
			if _, exists := proofs[model]; exists || model == "" {
				return
			}
		}
		if model != "" {
			proofs[model] = time.Now()
		}
		if !loaded {
			if _, raced := s.poolVerified.LoadOrStore(identity, next); raced {
				continue
			}
		} else if !s.poolVerified.CompareAndSwap(identity, prev, next) {
			continue
		}
		if current, live := s.cachedPoolPair(identity); live != openAIGatewayPoolPairLive || current.version != version {
			s.poolVerified.CompareAndDelete(identity, next)
		}
		return
	}
}

// gatewayPoolWarmProbe 跑一组 state-echo：A 只带 cookie 拿一张 state，B 带 cookie + 那张 state
// 看上游还不还新的。
//
// conclusive=false 表示**没下结论**（传输失败、非 200、A 没回 state），调用方不许把它当降智 ——
// 判据纪律 1：非 200 一律不下结论。这里保留当前票的A/B初验；不是说state不能跨票复用。
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
	sent = status > 0 // 拿到了状态码 = 这一发确证到过上游
	if err != nil {
		return false, false, sent, err
	}
	if status != http.StatusOK {
		return false, false, sent, &gatewayPoolProbeHTTPError{status: status}
	}
	if ticket == "" {
		return false, false, sent, errGatewayPoolProbeMissingState
	}
	status, fresh, err := shoot(ctx, cookie, ticket)
	slog.Info("gwpool_warm_probe", "account_id", accountID, "gateway", gateway,
		"attempt", attempt, "shot", "b",
		"status", status, "got_state", fresh != "", "error", gatewayPoolWarmErrorText(err))
	if err != nil {
		return false, false, true, err
	}
	if status != http.StatusOK {
		return false, false, true, &gatewayPoolProbeHTTPError{status: status}
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
	kind := "warm_probe_a"
	if state != "" {
		kind = "warm_probe_b"
	}
	req = req.WithContext(withOpenAIRecordingKind(req.Context(), kind))
	resp, sentAt, err := s.gatewayPoolObservedRoundTrip(req, proxyURL, account, true, func(at time.Time) {
		if trace, ok := ctx.Value(gatewayPoolProbeTraceKey{}).(*gatewayPoolProbeTrace); ok && trace.onFirst != nil {
			trace.onFirst(at)
		}
	})
	if trace, ok := ctx.Value(gatewayPoolProbeTraceKey{}).(*gatewayPoolProbeTrace); ok {
		trace.mayHaveSent = trace.mayHaveSent || !gatewayPoolReleasesUnsent(resp, err)
		if !sentAt.IsZero() {
			trace.markSent(sentAt)
		}
	}
	if err != nil {
		return 0, "", err
	}
	// 头到手即断：提前 Close 让 HTTP/2 发 RST_STREAM，上游立刻停止生成（同
	// dropDegradedGatewayPoolRoute 的手法）。Drain 一点点是为了让非 200 的错误体不卡在内核缓冲。
	if trace, ok := ctx.Value(gatewayPoolProbeTraceKey{}).(*gatewayPoolProbeTrace); ok {
		for _, cookie := range resp.Cookies() {
			if cookie.Name == "__oailb" {
				trace.actualGateway = openAICodexRouteGateway("__oailb=" + cookie.Value)
			}
		}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode >= http.StatusBadRequest {
		// 严格模式不会再用业务请求“补取”错误响应，必须在探测处沿用账号错误登记。
		// 不向日志或客户端传递响应原文；成功响应依旧只读头。
		const probeErrorBodyLimit = 16 << 10
		body, _ := io.ReadAll(io.LimitReader(resp.Body, probeErrorBodyLimit))
		s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, body, model)
	}
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
