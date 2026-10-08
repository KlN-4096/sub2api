package service

// 业务 state 被刷新后，以最新 state 连续确认最多 3/1 发。
// 业务没带 state 时，收到新值也按同一票龄预算回带确认；两边都没有 state 则放行、不造质量结论。
// 仅 HTTP 200 可下结论；任一确认接受即保留原业务响应，全刷新才丢票。
// 此处沿用项目 state-echo 启发式，不把它宣称为模型能力证明。
// 错误/取消/票关联变化为未知，严格防护阻止交付但不计降级。

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

// 池业务始终验证，旧 guard/state_echo/retries 键不再读取。
// 错误响应只说类别，不携带 state、cookie 或上游身份。
const gatewayPoolDegradedClientMsg = "网关连续回声确认均刷新，当前网关已标记为要换，本次不交付业务结果" +
	" / All gateway echo confirmations refreshed; the route is marked for rotation and the business response is withheld"

// errOpenAIGatewayPoolRouteDegraded 走的是**和池子 503 完全同一条**失败路径。
//
// 包着 gwpool.ErrPool 是承重的，不是装饰：classifyUpstreamTransportError 对 ErrPool 豁免
// 「按代理持久故障停调度 10 分钟 + 发告警」。降智是路由质量问题，把它当成这个账号的网络故障
// 会让一次误判（判据有假阴性，见文件头纪律 2）停掉一个真账号。
var errOpenAIGatewayPoolRouteDegraded = fmt.Errorf("%s: %w", gatewayPoolDegradedClientMsg, gwpool.ErrPool)

const gatewayPoolWarmUnverifiedClientMsg = "无法完成网关质量验证（预算不足、上游异常或路由已更换），严格防护已阻止业务请求，请稍后重试" +
	" / Gateway verification could not complete (insufficient budget, upstream error or a changed route); strict protection blocked the business request. Retry later."

var errOpenAIGatewayPoolWarmUnverified = fmt.Errorf("%s: %w", gatewayPoolWarmUnverifiedClientMsg, gwpool.ErrPool)

const gatewayPoolWarmExhaustedClientMsg = "当前候选已试尽，未获得可用的验满路由；本次不发送业务请求，请等待冷却恢复" +
	" / Eligible gateway candidates are exhausted without a usable verified route; no business request was sent. Wait for cooldown recovery."

var errOpenAIGatewayPoolWarmExhausted = fmt.Errorf("%s: %w", gatewayPoolWarmExhaustedClientMsg, gwpool.ErrPool)

// gatewayPoolRouteDegraded 跑 state-echo 判据。只读**响应头**，一个字节的响应体都不碰。
//
// 第一道闸是「这一发到底有没有注入池子那张 pair」（per-request 标记，不是回读 pair 缓存）：
// 没注入就没有「当前网关」可换，判出来也没有动作可做 —— 非推理面的请求
// 落回罐回放，回读缓存会把它们也算进来。账号必须对得上：故障转移在同一个 ctx 里换号重试，
// 标记留的是前一个号的（与 routePairInUse 同一条校验）。
func (s *OpenAIGatewayService) gatewayPoolRouteDegraded(
	request *http.Request,
	resp *http.Response,
	account *Account,
	proxyURL, identity string,
	sentAt time.Time,
) (bool, error) {
	if s == nil || request == nil || resp == nil || account == nil || !account.UsesGatewayPool() {
		return false, nil
	}
	applied := openAIGatewayPoolSinkFrom(request.Context()).snapshot()
	if applied.Cookie == "" || applied.AccountID != account.ID {
		return false, nil
	}
	// 纪律 1：非 200 一律不下结论。429/5xx 带回新票是限流/故障的副产物，不是路由质量读数。
	if resp.StatusCode != http.StatusOK {
		return false, nil
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "__oailb" {
			actual := openAICodexRouteGateway("__oailb=" + cookie.Value)
			if actual != "" && actual != applied.Gateway {
				return false, retryGatewayPoolAttempt(errOpenAIGatewayPoolWarmUnverified)
			}
		}
	}
	sent := strings.TrimSpace(request.Header.Get(openAICodexTurnStateHeader))
	fresh := extractOpenAICodexTurnState(resp.Header)
	if sent == "" && fresh == "" {
		// User-selected policy: no echo evidence permits pass-through, but
		// is neither a new full-strength observation nor a degraded one.
		return false, nil
	}
	refreshed := fresh != "" && fresh != sent
	if refreshed {
		resumeUsage := pauseGatewayPoolActiveUse(request.Context())
		// Freeze this business contact before confirmations update the same
		// round. Confirmation traffic is not a new initial-quality sample.
		s.noteGatewayPoolBusinessContact(request, account, identity, applied, sentAt, resp)
		full, conclusive, err := s.gatewayPoolConfirmResponse(request, account, proxyURL, identity, fresh, applied, sentAt)
		if err != nil || !conclusive {
			if request.Context().Err() != nil {
				return false, request.Context().Err()
			}
			var retryError *gatewayPoolAttemptRetryError
			if errors.As(err, &retryError) {
				return false, err
			}
			if request.Context().Err() == nil && gatewayPoolRetryableProbeError(err) {
				return false, retryGatewayPoolAttempt(err)
			}
			return false, errOpenAIGatewayPoolWarmUnverified
		}
		refreshed = !full
		if full {
			resumeUsage()
		}
	}
	if !refreshed {
		openAIGatewayPoolSinkFrom(request.Context()).noteVerdict(applied.Gateway, openAIGatewayVerdictFull)
		// The initial A/B already trained this ticket's successful cooldown.
		// Do not add another learning event before the final delivery check.
	}
	return refreshed, nil
}

// 轮开始时冻结确认次数：首次实际出站以来 <90s:3，>=90s:1。
// 老票的「最多两发」包括已经发出的原业务，因此这里只再给一次确认。
const gatewayPoolEchoYoungAge = 90 * time.Second

// 不含原业务那一发，不跨业务累计，确认时每发跟随最新 state。
func gatewayPoolEchoStrikes(age time.Duration) int {
	switch {
	case age < gatewayPoolEchoYoungAge:
		return 3
	default:
		return 1
	}
}

// gatewayPoolMarkStale 把缓存里那张票的满血窗口按「已到点」处理。
//
// 不是直接删缓存：删掉之后下一发会走不带 force / 不带 exclude_versions 的
// /cookie，池子可能原样把这张烧过的再发回来。标 Stale 保留票号 ⇒ cachedPoolPair 读成
// openAIGatewayPoolPairStale ⇒ 取票时 force=1 + exclude_versions=<这张> ⇒ 必定换一个网关。
//
// 票号对不上就什么都不做：那说明缓存里已经是另一张票了（并发换过、还过）。
// 返回值是这张票的满血时长，0 = 没验过满血 / 没标上（见 gatewayPoolNoteFullWindow）。
func (s *openAICodexCookieStore) gatewayPoolMarkStale(identity, version string) time.Duration {
	held, _ := s.gatewayPoolMarkStaleMatched(identity, version)
	return held
}

func (s *openAICodexCookieStore) gatewayPoolMarkStaleMatched(identity, version string) (time.Duration, bool) {
	if s == nil || identity == "" {
		return 0, false
	}
	value, ok := s.poolPairs.Load(identity)
	if !ok {
		return 0, false
	}
	cached, isPair := value.(openAIGatewayPoolPair)
	if !isPair {
		return 0, false
	}
	if cached.invalidated {
		return 0, false
	}
	// A late verdict cannot invalidate a replacement, even on the same gateway.
	if version == "" || cached.version != version {
		return 0, false
	}
	next := cached
	next.invalidated = true
	next.invalidatedAt = time.Now().UTC()
	if !s.poolPairs.CompareAndSwap(identity, cached, next) {
		return 0, false
	}
	s.invalidateGatewayPoolActiveUse(identity, cached.gateway, cached.version, next.invalidatedAt)
	// CAS 成功后才结束这张票的统计窗口，避免重复记时。
	return s.gatewayPoolNoteFullWindow(identity, cached.version), true
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
) bool {
	// resp 非 nil 由调用约定保证（判成降智的前提就是拿到了响应）；Body 仍要判，合成响应可以没有。
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	applied := openAIGatewayPoolSinkFrom(request.Context()).snapshot()
	// 身份解析对影子行要读一次库：用脱离取消的 ctx，否则客户端刚好断开的那一瞬连标 Stale 都做不了，
	// 票会留在缓存里被下一发继续拿出去。
	detached := context.WithoutCancel(request.Context())
	if identity, err := s.codexCookies.gatewayPoolIdentity(detached, account); err == nil {
		// 标 Stale 顺手量到这张票的满血时长 ⇒ 记进 sink，由用量侧落到这个网关的账本上。
		if request.Context().Err() != nil {
			return false
		}
		held, marked := s.codexCookies.gatewayPoolMarkStaleMatched(identity, applied.Version)
		if !marked {
			return false
		}
		openAIGatewayPoolSinkFrom(request.Context()).noteVerdict(applied.Gateway, openAIGatewayVerdictDegraded)
		applied.Verdict = openAIGatewayVerdictDegraded
		applied.FullHeldMs = held.Milliseconds()
		s.finishGatewayPoolContact(detached, account, identity, applied, held)
		openAIGatewayPoolSinkFrom(detached).noteFullHeld(applied, held)
		// 账本记的是**实际交付的那个网关**：标 Stale 只让下一发换票，账本才是「这个上游账号
		// 本地冷却内别再点这个落点」的依据。
		s.codexCookies.gatewayPoolMarkUsed(identity, applied.Gateway, applied.cooldownResetAt)
		s.noteGatewayPoolCooldownVerdict(detached, account, applied, openAIGatewayVerdictDegraded)
	} else {
		return false
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
		"reason", "all state-echo confirmations refreshed: rotate this route")
	// 丢弃的这一发是一次真实上游请求：把读数留给用量侧落一条可审计的记录
	// （openai_gateway_usage.go 的 RecordGatewayPoolDiscardedUsageLogs）。
	openAIGatewayPoolSinkFrom(request.Context()).noteDiscarded(OpenAIGatewayPoolDiscardedAttempt{
		Applied: applied,
	})
	return true
}
