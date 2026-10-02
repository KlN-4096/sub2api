package service

// state-echo 判据：在业务请求上就地判这一发的上游路由有没有降智。
//
// 判据本体（docs/conventions/codex-full-strength-tickets.md 第一节，2026-10-01 实证，与人工
// 「糖果题」10/10 一致）：带着一张**活的** turn-state 再打一发，只读响应头 ——
//
//	送了票 + 上游不回新票（或回的和送出去的那张一样）  ⇒ 满血
//	送了票 + 上游回了一张**不同的**新票              ⇒ 降智
//	没送票                                          ⇒ 判不出来（上游对没带票的请求必然铸一张新的，
//	                                                  那不是回声）
//
// 两条判据纪律，都是实测踩出来的，不许省：
//
//  1. **HTTP 必须是 200 才下结论。** 作者原版（relay/index.js:832 verifyStateOnce）把 429/403 走
//     「正常响应」分支，响应头里自然没有 turn-state ⇒ 判成满血，12 发里撞出 2 发假「满血」。
//     反向同理：429/5xx 带回一张新票**不算**降智证据 —— 那是限流/故障，不是路由质量。
//  2. **有假阴性、无假阳性。** 满血号偶尔也会下发新票（用户 2026-10-01 确认）⇒ 判「满血」可信，
//     判「降智」可能偏严。所以这个判据会偶尔白换一次网关、白烧一个槽位，而供给是个位数张/小时。
//     正因为会误判，**必须有开关**（openAIGatewayPoolStateEchoExtraKey），运营方得能停掉它。
//
// 判到降智之后做两件事，都刻意不碰别的子系统：
//
//   - 把当前 (身份 → pair) 缓存项标成 **Stale**（gatewayPoolMarkStale）⇒ 下一次取票天然带
//     force=1 + exclude_versions，换一个网关。复用 openai_gwpool.go 已有的三态，不另造机制。
//   - 按本地账本记一笔「这个上游账号碰过这个网关」（按 chatgpt:<account_id> 收敛，不按 sub2api
//     的账号行 —— 同一份凭据挂在克隆行上时按行记会让每行都以为自己还有窗口）。
//
// **刻意不做的三件事**：
//   - 不回报给网关池。池子那条回报路径（/touch）是刻意删掉的，理由见 openai_gwpool.go 文件头。
//   - 不接进账号熔断 / 选号。降智是**路由**质量，不是账号故障；接进去会让一次误判停掉一个账号。
//     实现上由 errOpenAIGatewayPoolRouteDegraded 包着 gwpool.ErrPool 保证
//     （classifyUpstreamTransportError 对 ErrPool 豁免停调度），而 (账号 × 模型) 那套瞬时熔断
//     只在拿到**上游状态码**的路径上记（openai_account_runtime_block_fastpath.go，且只对
//     apikey / cpr 类型），传输层错误走不到它。
//   - 不改 routePairOf 的信任边界筛选。

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	// openAIGatewayPoolGuardExtraKey 是账号级的降智防护档位，把原来那两个键
	// （openai_gwpool_state_echo + openai_gwpool_degraded_retries）合成一条梯子。
	//
	// 为什么合：两键四个组合里有一个是死的 —— state_echo=false 时 retries 一点意义都没有，
	// 而页面上仍然是两个控件、配得出那个状态。按「守得多严」排成一个枚举之后，非法状态不存在。
	//
	//	off    不判。降智原样交给客户端
	//	cut    判；判到降智就截断回一个干净的错误 + 标 Stale，客户端自己重发
	//	retry  判；判到降智就当场换票把同一个请求重发一遍，客户端无感（**默认**）
	//	queue  业务请求只落在**验过满血**的槽上：取到的新票先用便宜的垫话跑一遍判据
	//	       （openai_gwpool_warm.go），没验出满血就换下一张，验出来了才放业务请求进去。
	//	       判据仍然留着当兜底网，但**不重发**（档位等价于 cut，见 retries()）。
	openAIGatewayPoolGuardExtraKey = "openai_gwpool_guard"
	// openAIGatewayPoolStateEchoExtraKey / openAIGatewayPoolDegradedRetriesExtraKey 是合并前的
	// 两个老键，**只保留读兼容**：库里已经有运营方配好的行，静默回默认会悄悄改掉它们的行为。
	// 新键存在时老键一概不看 —— 但写入侧**刻意把老键同步写一份**（不是删掉，见
	// EditAccountModal.vue 的保存路径）：回滚到只认老键的后端时行为才不变。所以「行里还留着
	// 老键」是常态，gatewayPoolGuard 的存在性判断承重就在这里。
	openAIGatewayPoolStateEchoExtraKey       = "openai_gwpool_state_echo"
	openAIGatewayPoolDegradedRetriesExtraKey = "openai_gwpool_degraded_retries"
	// openAIGatewayPoolDegradedRetriesMax 是换票重发的硬上限。
	//
	// 刻意**不做成可配的数**：判据有假阴性（见文件头纪律 2）会偶尔把满血判成降智，放大系数必须
	// 封顶，不然一个整体降智的池子会把一次客户端请求放大成 N 发真实上游请求 —— 而每一发都烧掉
	// 一个 (上游账号 × 网关) 单位。重试那一发若又判降智，直接走错误路径，绝不再试。
	openAIGatewayPoolDegradedRetriesMax = 1
)

// gatewayPoolGuardMode 是上面那条梯子的一档。
type gatewayPoolGuardMode string

const (
	gatewayPoolGuardOff   gatewayPoolGuardMode = "off"
	gatewayPoolGuardCut   gatewayPoolGuardMode = "cut"
	gatewayPoolGuardRetry gatewayPoolGuardMode = "retry"
	gatewayPoolGuardQueue gatewayPoolGuardMode = "queue"
)

// gatewayPoolGuard 报告这个账号的降智防护档位。缺省 = retry。
//
// 新键**在场就只看它**：认得的值照用，认不出的值（写了错别字、大小写、写成数字）直接回默认档
// retry，**不落回老键**。回落的那一版有个 fail-open 角：库里存量行可能还带着
// `openai_gwpool_state_echo: false`，于是 `guard: "Queue"` 这种手滑会被读成 off —— 运营方点了
// 最严的那档，拿到的是最松的。前端 select 造不出这种值，但管理 API 和批量导入造得出。
func (a *Account) gatewayPoolGuard() gatewayPoolGuardMode {
	if a == nil || a.Extra == nil {
		return gatewayPoolGuardRetry
	}
	// 按**存在性**分支，不是按类型断言：extra 是 JSONB，这个键可以是 number / bool / null。
	// 断言 `.(string)` 的那一版对它们全部失败 ⇒ 掉进老键 ⇒ 行里若还留着
	// `openai_gwpool_state_echo: false`（前端刻意双写，见 EditAccountModal.vue），
	// 一发 `{"openai_gwpool_guard": null}`（「清掉这个键恢复默认」的常见写法）就被读成 **off**：
	// 判据一发不跑、降智原样交给客户端，而前端显示的也是 off，没人会发现它本该是 retry。
	if raw, present := a.Extra[openAIGatewayPoolGuardExtraKey]; present {
		text, _ := raw.(string) // 非字符串 ⇒ "" ⇒ 落 default
		switch mode := gatewayPoolGuardMode(strings.TrimSpace(text)); mode {
		case gatewayPoolGuardOff, gatewayPoolGuardCut, gatewayPoolGuardRetry, gatewayPoolGuardQueue:
			return mode
		default:
			// 静默回默认档 = 运营方以为配上了、其实没有，而唯一的线索是行为。打一条出来。
			slog.Warn("gwpool_guard_unrecognized", "account_id", a.ID,
				"value", fmt.Sprintf("%v", raw), "fallback", string(gatewayPoolGuardRetry))
			return gatewayPoolGuardRetry
		}
	}
	return gatewayPoolGuardLegacy(a.Extra)
}

// gatewayPoolGuardLegacy 把合并前那两个键映射成档位。
//
// 老键的口径原样照搬，一个字都不改：state_echo **缺省即开**（只有显式 false 才关，所以读法是
// 「断言 bool 成功且为 false」，getExtraBool 在这里用不了 —— 它把「没配」和「配了 false」读成
// 同一个值）；retries **只有显式写成数字 0 才是「只截断」**，其余一切（缺省、非数字、越界、
// 负数）都是默认档。extra 是 JSONB：从库里读回来是 float64，从请求体/Go 字面量过来是 int。
func gatewayPoolGuardLegacy(extra map[string]any) gatewayPoolGuardMode {
	if enabled, ok := extra[openAIGatewayPoolStateEchoExtraKey].(bool); ok && !enabled {
		return gatewayPoolGuardOff
	}
	switch raw := extra[openAIGatewayPoolDegradedRetriesExtraKey].(type) {
	case float64:
		if raw == 0 {
			return gatewayPoolGuardCut
		}
	case int:
		if raw == 0 {
			return gatewayPoolGuardCut
		}
	case int64:
		if raw == 0 {
			return gatewayPoolGuardCut
		}
	}
	return gatewayPoolGuardRetry
}

// judges 报告这一档要不要跑 state-echo 判据。
func (m gatewayPoolGuardMode) judges() bool { return m != gatewayPoolGuardOff }

// retries 报告判到降智之后换票重发几次。cut 是 0，其余都是上限（off 档判据根本不跑，取值无关）。
// retries 报告判到降智后换票重发几次。
//
// **queue 和 cut 一样是 0**，这一条是承重的：换票重发那一发走的是 gatewayPoolReplayRequest →
// doOpenAIUpstreamOnce → AttachRoute，force 取一张全新的票，然后**一发判据都不跑**就把用户的
// prompt 打出去 —— 那恰好是 queue 档存在的理由要消灭的东西（「不让用户的业务请求降智，把降智
// 拦在外边」）。queue 档判到降智就截断，让下一发客户端请求重新走预热。
func (m gatewayPoolGuardMode) retries() int {
	if m == gatewayPoolGuardCut || m == gatewayPoolGuardQueue {
		return 0
	}
	return openAIGatewayPoolDegradedRetriesMax
}

// gatewayPoolDegradedClientMsg 原样进网关客户端的错误响应与 Ops 错误日志（转发面没有 i18n 协商
// 通道，与 gatewayPoolNoSlotClientMsg 同口径）。只说类别：票本体、cookie 本体一个字都不许出现。
const gatewayPoolDegradedClientMsg = "这一发的上游路由已降智（带着活 turn-state 又收到一张新的），" +
	"当前网关已标记为要换，本次按失败处理（绝不把降智结果交给客户端），稍后重试即可" +
	" / This request's upstream route is degraded (a live turn-state came back with a fresh one), " +
	"the current gateway is marked for rotation and this request fails instead of serving a " +
	"degraded answer; retry shortly"

// errOpenAIGatewayPoolRouteDegraded 走的是**和池子 503 完全同一条**失败路径。
//
// 包着 gwpool.ErrPool 是承重的，不是装饰：classifyUpstreamTransportError 对 ErrPool 豁免
// 「按代理持久故障停调度 10 分钟 + 发告警」。降智是路由质量问题，把它当成这个账号的网络故障
// 会让一次误判（判据有假阴性，见文件头纪律 2）停掉一个真账号。
var errOpenAIGatewayPoolRouteDegraded = fmt.Errorf("%s: %w", gatewayPoolDegradedClientMsg, gwpool.ErrPool)

// gatewayPoolWarmNoModelClientMsg 是 queue 档读不出本轮模型时那条。
//
// **fail closed 而不是静默退回 retry**：判据的 turn-state 绑在 (账号 × 模型 × 这张票) 上，
// 不知道模型就没法验。悄悄降级会把运营方选的「绝不把降智交给客户端」抹掉，而唯一线索是一条日志。
// 已知触发条件只有一种：双开（codex_fingerprint_mode=device，出站体被 zstd 压过）× 0.156 之前的
// 客户端（turn-metadata 头里不补 model）。文案要直接说出该怎么办。
const gatewayPoolWarmNoModelClientMsg = "读不出本轮模型，无法在请求前验满血（降智防护为「只用验过满血的槽」档）。" +
	"已知成因：账号开了 device 指纹收敛（出站请求体被压缩）而客户端是 Codex 0.156 之前的版本。" +
	"换档到「换票重试」或升级客户端" +
	" / Cannot read this turn's model, so the route cannot be verified before the request " +
	"(degradation guard is set to verified-full slots only). Known cause: this account runs the device " +
	"fingerprint profile (compressed outbound body) with a pre-0.156 Codex client. Switch the guard to " +
	"retry, or upgrade the client"

var errOpenAIGatewayPoolWarmNoModel = fmt.Errorf("%s: %w", gatewayPoolWarmNoModelClientMsg, gwpool.ErrPool)

// gatewayPoolWarmExhaustedClientMsg 是 queue 档取满上限张票、一张都没验出满血时那条。
//
// **刻意不复用 gatewayPoolDegradedClientMsg**：那条说的三件事在这里逐条都不对 ——
// 这一发的业务请求一个字节都没出去过、被标记要换的是**好几个**网关而不是「当前网关」，
// 而「稍后重试即可」在这里是最坏的建议：Codex CLI 对 503 会自动重发，每一次重发都可能再烧
// 几张票，而池子的供给是个位数张/小时。文案必须把「别立刻重发」说出来。
const gatewayPoolWarmExhaustedClientMsg = "连取几张路由票都没验出满血，本次按失败处理" +
	"（降智防护为「只用验过满血的槽」档，绝不把降智结果交给客户端）。这一档每次失败都会花掉几张票，" +
	"而供给有限 —— 请隔一会儿再发，不要立刻重试" +
	" / Several route pairs were taken and none verified full strength, so this request fails " +
	"(the degradation guard is set to verified-full slots only and never serves a degraded answer). " +
	"Each failure in this mode spends several pairs from a limited supply: wait a while before sending " +
	"again rather than retrying immediately"

var errOpenAIGatewayPoolWarmExhausted = fmt.Errorf("%s: %w", gatewayPoolWarmExhaustedClientMsg, gwpool.ErrPool)

// gatewayPoolRouteDegraded 跑 state-echo 判据。只读**响应头**，一个字节的响应体都不碰。
//
// 第一道闸是「这一发到底有没有注入池子那张 pair」（per-request 标记，不是回读 pair 缓存）：
// 没注入就没有「当前网关」可换，判出来也没有动作可做 —— 非推理面的请求
// 落回罐回放，回读缓存会把它们也算进来。账号必须对得上：故障转移在同一个 ctx 里换号重试，
// 标记留的是前一个号的（与 routePairInUse / gatewayPoolRenew 同一条校验）。
func (s *OpenAIGatewayService) gatewayPoolRouteDegraded(
	request *http.Request,
	resp *http.Response,
	account *Account,
) bool {
	if s == nil || request == nil || resp == nil || !account.gatewayPoolGuard().judges() {
		return false
	}
	applied := openAIGatewayPoolSinkFrom(request.Context()).snapshot()
	if applied.Cookie == "" || applied.AccountID != account.ID {
		return false
	}
	// 纪律 1：非 200 一律不下结论。429/5xx 带回新票是限流/故障的副产物，不是路由质量读数。
	if resp.StatusCode != http.StatusOK {
		return false
	}
	sent := strings.TrimSpace(request.Header.Get(openAICodexTurnStateHeader))
	if sent == "" {
		// 没送票 ⇒ 没有回声。上游对不带票的请求 87% 会铸一张新的（openai_codex_turn_state_auto.go
		// 的实测），拿它当降智证据等于把绝大多数首轮请求判死。
		return false
	}
	fresh := extractOpenAICodexTurnState(resp.Header)
	degraded := fresh != "" && fresh != sent
	// 两个方向都记：走到这里就是一个**有结论**的读数，而账号卡片上「这个落点验过是满血」
	// 和「没验过」是两回事（openai_gwpool_gateway_history.go）。满血那条只有这里产出。
	verdict := openAIGatewayVerdictFull
	if degraded {
		verdict = openAIGatewayVerdictDegraded
	}
	openAIGatewayPoolSinkFrom(request.Context()).noteVerdict(applied.Gateway, verdict)
	return degraded
}

// gatewayPoolMarkStale 把缓存里那张票的满血窗口按「已到点」处理。
//
// 不是删（gatewayPoolDropPair）：删掉之后下一发会走不带 force / 不带 exclude_versions 的
// /cookie，池子可能原样把这张烧过的再发回来。标 Stale 保留票号 ⇒ cachedPoolPair 读成
// openAIGatewayPoolPairStale ⇒ 取票时 force=1 + exclude_versions=<这张> ⇒ 必定换一个网关。
//
// 票号对不上就什么都不做：那说明缓存里已经是另一张票了（并发换过、还过）。
func (s *openAICodexCookieStore) gatewayPoolMarkStale(identity, version, gateway string) {
	if s == nil || identity == "" {
		return
	}
	value, ok := s.poolPairs.Load(identity)
	if !ok {
		return
	}
	cached, isPair := value.(openAIGatewayPoolPair)
	if !isPair {
		return
	}
	// 票号对不上时**再按落点比一次**，别直接放弃。
	//
	// 并发 + 临期票的交叠窗口里票号会变：另一路请求抢到续期名额、goroutine 回来后
	// gatewayPoolSwapPair 把缓存里的票号从 v1 换成 v2，而本路判降智时手上还是 v1 ⇒
	// 只比票号的话这里静默什么都不做，`until` 不清零、cachedPoolPair 继续判 Live ⇒
	// 刚被判死的那条路由在窗口剩余时间里每一发都照走，force=1 也发不出去。
	//
	// 被判死的是**落点**不是票号：同一个网关换没换票都该换走，所以落点一致就照标。
	if cached.version != version && (gateway == "" || cached.gateway != gateway) {
		return
	}
	next := cached
	next.until = time.Time{} // 零值早于任何时刻 ⇒ cachedPoolPair 判 Stale。
	s.poolPairs.CompareAndSwap(identity, cached, next)
}

// gatewayPoolReplayRequest 复制一份可重放的上游请求，不可重放时返回 nil。
//
// GetBody 是 net/http 自己为可重放 body（*bytes.Reader / *strings.Reader / *bytes.Buffer）设的
// 工厂，重定向与 HTTP/2 的内建重试用的就是它。本仓库的 Codex 出站请求都是
// http.NewRequestWithContext(..., bytes.NewReader(wireBody))（openai_gateway_passthrough.go:659、
// openai_gateway_forward.go:1495）⇒ 恒有 GetBody。
//
// 它为 nil 说明那条路径的 body 读一遍就没了，这时**绝不重放**：半截的请求体会被上游当成一个
// 合法请求处理，静默产生一次错误的推理轮次。按 nil 退回「只截断」。
func gatewayPoolReplayRequest(request *http.Request) *http.Request {
	if request == nil || request.GetBody == nil {
		return nil
	}
	body, err := request.GetBody()
	if err != nil {
		return nil
	}
	replay := request.Clone(request.Context())
	replay.Body = body
	return replay
}

// dropDegradedGatewayPoolRoute 丢掉这一发降智的响应，并把当前网关标成要换。
//
// 调用点在**读到响应头之后、往下游写第一个字节之前**：doOpenAIUpstream 是传输层，调用方要等它
// 返回才开始解析响应、写下游。所以这里关掉响应体就是干净的截断，不会留一个半截的 SSE 流。
// 不读响应体还有一个副作用是想要的：提前 Close 让 HTTP/2 发 RST_STREAM，上游立刻停止生成
// （与猎手「头到手即断」同一手法，openai_turn_state_hunter.go）。
//
// 这条日志是**唯一**能事后回答「我的请求为什么被截断了」的东西。按明确约定：
// 票本体、cookie 本体都不进日志，票只记 sha256 前缀指纹（openAICodexTurnStateKey，与溯源表同一个
// 键函数），池子的票号同样不打 —— 它是这张票的身份，和 cookie 本体一样对待。
func (s *OpenAIGatewayService) dropDegradedGatewayPoolRoute(
	request *http.Request,
	resp *http.Response,
	account *Account,
	retrying bool,
) {
	// resp 非 nil 由调用约定保证（判成降智的前提就是拿到了响应）；Body 仍要判，合成响应可以没有。
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	applied := openAIGatewayPoolSinkFrom(request.Context()).snapshot()
	// 身份解析对影子行要读一次库：用脱离取消的 ctx，否则客户端刚好断开的那一瞬连标 Stale 都做不了，
	// 票会留在缓存里被下一发继续拿出去（与 gatewayPoolRenew 同一处取舍）。
	detached := context.WithoutCancel(request.Context())
	if identity, err := s.codexCookies.gatewayPoolIdentity(detached, account); err == nil {
		s.codexCookies.gatewayPoolMarkStale(identity, applied.Version, applied.Gateway)
		// 账本记的是**实际交付的那个网关**：标 Stale 只让下一发换票，账本才是「这个上游账号
		// 4 小时内别再点这个落点」的依据。
		s.codexCookies.gatewayPoolMarkUsed(identity, applied.Gateway)
	}
	sent := strings.TrimSpace(request.Header.Get(openAICodexTurnStateHeader))
	// **不打 account_key**：它是 chatgpt:<上游 account_id>[:user:<user_id>]，含上游账号/用户
	// UUID。本文件以外的 7 处 gwpool 日志一律只打 account_id，openai_gwpool.go 的注释也明写
	// 「身份不进报错」。日志会外发到面板、聚合、工单附件，口径必须一致。
	slog.Warn("gwpool_route_degraded",
		"account_id", account.ID,
		"gateway", applied.Gateway,
		"status", resp.StatusCode,
		"sent_state_fingerprint", openAICodexTurnStateKey(sent),
		"retrying", retrying,
		"reason", "a live turn-state came back with a different one (state-echo): this route is degraded")
	// 丢弃的这一发是一次真实上游请求：把读数留给用量侧落一条可审计的记录
	// （openai_gateway_usage.go 的 RecordGatewayPoolDiscardedUsageLogs）。
	openAIGatewayPoolSinkFrom(request.Context()).noteDiscarded(OpenAIGatewayPoolDiscardedAttempt{
		Applied: applied,
		Retried: retrying,
	})
}
