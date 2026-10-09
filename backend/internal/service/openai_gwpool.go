package service

// 网关池只提供路由 pair 和候选参考；严格 Luna 验证、本地成员级 CD、业务
// 失效判定及观测反馈由消费侧维护。活票跨请求复用；需要换票时一次只领一张，
// 无备用库存、远端退票或空闲预探测。独占新票确证未发送时只做本地 CAS 清理。
//
// 配置来自账号 extra；接管仅限 ChatGPT Codex 推理面，不回落到未验证的路由。
// 池账号 WS 入站使用 HTTP 桥逐轮准备，绕过选路层的原生 WS 在取票前拒绝。

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/chatgptcookies"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	// openAIGatewayPoolExtraKey 是账号级开关键，默认缺省即关。写成字符串 "true" 时
	// getExtraBool 返回 false、开关静默失效，只能是 bool（与 extra 里其它账号特性开关同口径）。
	openAIGatewayPoolExtraKey = "openai_gwpool"
	// openAIGatewayPoolBaseURLExtraKey 是该账号要问的池子地址，形如 http://127.0.0.1:8099。
	// 合法性在管理端写入时按 config.ValidateAbsoluteHTTPURL 校验。
	openAIGatewayPoolBaseURLExtraKey = "openai_gwpool_base_url"
	// OpenAIGatewayPoolConsumerKeyExtraKey 是池子发给**这个账号**的消费端凭据。
	//
	// 与 access_token 同级：不进日志、不进任何 API 响应（dto.redactAccountManagedExtra 把它
	// 脱敏成 bool），管理端写入按「只有非空字符串才算改动」合并（见 admin_account.go）。
	OpenAIGatewayPoolConsumerKeyExtraKey = "openai_gwpool_consumer_key"
	// openAIGatewayPoolInferencePath 是 Codex 推理面的路径。chatgptCodexURL（HTTP 出口）与
	// buildOpenAIResponsesWSURL（WS 握手）用的是同一条；那两个是整串 URL，这里只能另抄一份路径，
	// 由 TestGatewayPoolInferencePathMatchesCodexURL 钉住不漂。
	openAIGatewayPoolInferencePath = "/backend-api/codex/responses"
	// openAIGatewayPoolFetchTimeoutExtraKey / openAIGatewayPoolListTimeoutExtraKey 是两个超时的
	// 账号级覆盖（秒）。缺省 / 非正数走下面的默认值——配坏了不该让账号取不到票。
	openAIGatewayPoolFetchTimeoutExtraKey = "openai_gwpool_fetch_timeout_s"
	openAIGatewayPoolListTimeoutExtraKey  = "openai_gwpool_list_timeout_s"
	// Each pool fetch is bounded independently of the logical candidate search.
	// A shared fetch is cancelled when its final waiter leaves.
	openAIGatewayPoolFetchTimeout = 10 * time.Second
	// A directory failure is unknown inventory, never permission for bare fetch.
	openAIGatewayPoolListTimeout = 2 * time.Second
	// openAIGatewayPoolGatewayWindowExtraKey 是本地账本的保留窗口（秒）。缺省 / 非正数走默认值。
	//
	// 没进 openAIGatewayPoolConfigExtraKeys：它没有跨字段约束，写什么都不会让账号配到一个
	// 必然失败的状态（最坏只是挑落点变松或变严）。同理另外几个旋钮也不进。
	openAIGatewayPoolGatewayWindowExtraKey = "openai_gwpool_gateway_window_s"
	// Lowest delivery floor supported by the external pool protocol. Omitting
	// it asks the server for its stricter 300s default; values below 30 clamp
	// to 30 there. Returned remaining/valid_for never impose a local deadline.
	openAIGatewayPoolMinRemaining = 30 * time.Second
	// openAIGatewayPoolDefaultBackoff 是池子说了「该退避」但没给 retry_after_seconds 时的时长。
	//
	// 1 分钟只为掐死重试环：没有退避时，客户端那种「失败就重发」的循环每轮都会在池子侧触发一次
	// 发现铸票（池子的闸是每个号 30 分钟一次），N 个账号就是 30 分钟内 N 发白烧的上游请求。
	// 池子真知道要多久（all_cooling 的冷却是小时级）时它会给 retry_after，那个值优先。
	openAIGatewayPoolDefaultBackoff = time.Minute
	// openAIGatewayPoolBadRequestBackoff 是 bad_request 的退避时长。
	//
	// 这个码**只能本地兜**：池子刻意不给 retry_after_seconds（重试不会变好）。给 300s 而不是
	// 上面那 1 分钟，因为它和别的码性质不同——它说明出站请求本身不合法，那是**我们这边的 bug
	// 或账号配置错**，不是池子的状态问题。不退避就是一个纯热循环（每发推理请求敲一次池子）；
	// 退 300s 不会损失任何本来会成功的请求（请求不改，下一发同样不合法），而且能把这个 bug
	// 变响，不至于静默刷日志。
	openAIGatewayPoolBadRequestBackoff = 5 * time.Minute
	// openAIGatewayPoolMaxSeconds 是所有「秒」旋钮的上限（1 天）。超了回默认值，
	// 见 gatewayPoolSeconds。
	openAIGatewayPoolMaxSeconds = 86400
	// 初始冷却 1 小时；明确失败按标准阶梯退避至24h。它是本地尝试策略，
	// 不是上游恢复承诺；每账号×网关的学习与固定状态见 openai_gwpool_cooldown.go。
	openAIGatewayPoolGatewayWindow = time.Hour
)

// ErrGatewayPoolWSIncompatible 是**兜底**闸：选路层（resolveOpenAIWSDecisionByGatewayPool /
// WS 入站的 forceHTTPBridge）已经把这个账号的 WS 请求改走 HTTP/SSE，所以正常路径走不到这里。
// 留着是防新增的拨号路径绕过选路层，并保证那种情况下**预热拨号也拿不到 pair**（检查在取 pair 之前）。
var ErrGatewayPoolWSIncompatible = errors.New(
	"openai gateway pool is enabled on this account: the WebSocket upstream reuses one connection for " +
		"up to 60 minutes while a full-strength route pair lasts ~150s, so the two cannot be combined")

// gatewayPoolBaseURL / gatewayPoolConsumerKey 读账号级配置。非字符串值读成空串 ⇒ 开关开着时
// 直接被 poolClient 判成配错而 fail closed，不会静默退回罐回放。
func (a *Account) gatewayPoolBaseURL() string {
	return strings.TrimSpace(a.getExtraString(openAIGatewayPoolBaseURLExtraKey))
}

func (a *Account) gatewayPoolConsumerKey() string {
	return strings.TrimSpace(a.getExtraString(OpenAIGatewayPoolConsumerKeyExtraKey))
}

// gatewayPoolGatewayWindow 读初始冷却档位。1~24h 的显式配置保留，缺省/非法值回到 1h。
func (a *Account) gatewayPoolGatewayWindow() time.Duration {
	return time.Duration(gatewayPoolCooldownBase(
		a.gatewayPoolSeconds(openAIGatewayPoolGatewayWindowExtraKey, openAIGatewayPoolGatewayWindow))) * time.Second
}

// Missing overrides use the default per-operation timeouts.
func (a *Account) gatewayPoolFetchTimeout() time.Duration {
	return a.gatewayPoolSeconds(openAIGatewayPoolFetchTimeoutExtraKey, openAIGatewayPoolFetchTimeout)
}

func (a *Account) gatewayPoolListTimeout() time.Duration {
	return a.gatewayPoolSeconds(openAIGatewayPoolListTimeoutExtraKey, openAIGatewayPoolListTimeout)
}

// gatewayPoolSeconds 读一个「秒」旋钮：非正数 / 非数字 / 缺省 / **超过上限**一律取 fallback。
//
// 上限不是洁癖：time.Duration 是纳秒级 int64，秒数到 1e10 就乘溢出成**负数** ⇒
// gatewayPoolUsedRecently 恒 false、gatewayPoolBurnedGateways 恒跳过（本地账本整体静默失效，
// 与「窗口越大越严」的直觉正好相反），同量级的 fetch_timeout_s 则让这个账号全量取不到票。
// 回默认值而不是钳到上限：填出这种数一定是打错了，按默认值跑比按 24 小时跑更接近本意。
func (a *Account) gatewayPoolSeconds(key string, fallback time.Duration) time.Duration {
	if seconds := a.getExtraInt(key); seconds > 0 && seconds <= openAIGatewayPoolMaxSeconds {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

const gatewayPoolProbeModelLuna = "gpt-6-luna"

// poolClient 取该账号的池子客户端，按 (base_url, consumer key) 缓存。
//
// 必须缓存：gwpool.New 每次都自带一个 *http.Transport，每请求新建等于每请求一个独立连接池，
// 连接永不复用、fd 一路涨。
//
// 开关开着但地址缺失/解不开 ⇒ 返回包着 gwpool.ErrPool 的错误让这一发失败（配错是池子侧的问题，
// ErrPool 保证它不会被 classifyUpstreamTransportError 当成上游故障把真账号停调度 10 分钟）。
// 刻意不退回罐回放：那正是要修掉的「把账号钉死在坏网关上」。
func (s *openAICodexCookieStore) poolClient(account *Account) (*gwpool.Client, error) {
	baseURL := account.gatewayPoolBaseURL()
	if baseURL == "" {
		return nil, fmt.Errorf("%w: account %d enables %s without %s",
			gwpool.ErrPool, account.ID, openAIGatewayPoolExtraKey, openAIGatewayPoolBaseURLExtraKey)
	}
	consumerKey := account.gatewayPoolConsumerKey()
	// 客户端的单次调用上限必须盖得住两个 ctx deadline 里更大的那个：http.Client.Timeout 与 ctx
	// 取较小者，给小了就把账号配的「取票超时」悄悄截短（2026-10-02 前写死 5s 正是这个毛病）。
	// 真正的分别限时还是由各自的 ctx 做（列网关 2s、取票 8s）。
	// 不用内建 max：这个包的测试里有个 func max(a, b int) int 盖住了它。
	timeout := account.gatewayPoolFetchTimeout()
	if listTimeout := account.gatewayPoolListTimeout(); listTimeout > timeout {
		timeout = listTimeout
	}
	// \x00 当分隔符：base_url 过了 URL 校验，不可能含 NUL，拼不出歧义键。
	// 超时必须进键：客户端自带 http.Client，不进键的话改了超时拿回来的还是旧的那个。
	cacheKey := baseURL + "\x00" + consumerKey + "\x00" + timeout.String()
	if cached, ok := s.poolClients.Load(cacheKey); ok {
		if client, ok := cached.(*gwpool.Client); ok {
			return client, nil
		}
	}
	client := gwpool.New(baseURL, consumerKey, timeout)
	if client == nil {
		return nil, fmt.Errorf("%w: account %d has an unusable %s",
			gwpool.ErrPool, account.ID, openAIGatewayPoolBaseURLExtraKey)
	}
	actual, _ := s.poolClients.LoadOrStore(cacheKey, client)
	if client, ok := actual.(*gwpool.Client); ok {
		return client, nil
	}
	return nil, fmt.Errorf("%w: account %d gateway pool client cache is corrupt", gwpool.ErrPool, account.ID)
}

// UsesGatewayPool 报告这个账号的 Codex 路由 cookie 由网关池下发。
//
// 范围与 cookie 回放完全一致（openAICodexCookiesApply / IsOpenAIOAuthLike）：只有本地持有
// ChatGPT 凭据的账号才会往 chatgpt.com 发推理请求，cpr 是原样中继、API Key 账号的上游不是它。
func (a *Account) UsesGatewayPool() bool {
	return a != nil && a.IsOpenAIOAuthLike() && a.getExtraBool(openAIGatewayPoolExtraKey)
}

// openAIGatewayPoolConfigExtraKeys 是有**跨字段约束**的网关池 extra 键：开关开着就必须配齐
// 地址与凭据。部分更新（UpdateAccountExtra / 批量）只在碰到它们时才去加载账号做合并校验。
//
// WS 的那几个开关 2026-10-02 从这份清单里撤了：两者不再互斥（本该走 WS 的请求在选路层降级成
// HTTP/SSE），留着只会让每次改 WS 开关都白读一次账号。
var openAIGatewayPoolConfigExtraKeys = []string{
	openAIGatewayPoolExtraKey,
	openAIGatewayPoolResumeGatewaysExtraKey,
	openAIGatewayPoolGatewayWindowExtraKey,
	openAIGatewayPoolCooldownResetHoursKey,
	openAIGatewayPoolRecoveryExtraKey,
	openAIGatewayPoolFetchTimeoutExtraKey,
	openAIGatewayPoolListTimeoutExtraKey,
	openAIGatewayPoolProbeTimeoutExtraKey,
	openAIGatewayPoolBaseURLExtraKey,
	OpenAIGatewayPoolConsumerKeyExtraKey,
	OpenAIGatewayPoolContinuousWaitKey,
}

// touchesOpenAIGatewayPoolConfig 报告这份 extra 更新碰到了有跨字段约束的网关池配置。
func touchesOpenAIGatewayPoolConfig(extra map[string]any) bool {
	for _, key := range openAIGatewayPoolConfigExtraKeys {
		if _, ok := extra[key]; ok {
			return true
		}
	}
	return false
}

// validateOpenAIGatewayPoolAccountExtra 校验账号级网关池配置：开关开着就必须配齐地址与凭据。
// extra 必须是**合并后**的最终形态，否则部分更新会从另一半绕过去。
//
// 错误带稳定的 reason code（GWPOOL_*），文案由前端按 i18n 命名空间
// admin.accounts.openai.gwpoolErrors.<CODE> 渲染（与 ops_user_error.go 同口径：后端给码，前端做
// i18n）。这里的英文串只是没有映射时的兜底。
//
// WS 上游不再是冲突项（2026-10-02）：勾上网关池不该让保存失败，本该走 WS 的请求在选路层
// 自动改走 HTTP/SSE（resolveOpenAIWSDecisionByGatewayPool）。
func validateOpenAIGatewayPoolAccountExtra(account *Account, extra map[string]any) error {
	if account == nil {
		return nil
	}
	for _, key := range []string{openAIGatewayPoolExtraKey, OpenAIGatewayPoolContinuousWaitKey} {
		if raw := extra[key]; raw != nil {
			if _, ok := raw.(bool); !ok {
				return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_SETTING_INVALID", "%s must be boolean", key)
			}
		}
	}
	for key, limit := range map[string]int{
		openAIGatewayPoolResumeGatewaysExtraKey: gatewayPoolResumeGatewaysMax,
		openAIGatewayPoolFetchTimeoutExtraKey:   openAIGatewayPoolMaxSeconds,
		openAIGatewayPoolListTimeoutExtraKey:    openAIGatewayPoolMaxSeconds,
		openAIGatewayPoolProbeTimeoutExtraKey:   gatewayPoolProbeTimeoutMaxSeconds,
		openAIGatewayPoolGatewayWindowExtraKey:  openAIGatewayPoolMaxSeconds,
	} {
		if raw := extra[key]; raw != nil {
			if _, ok := gatewayPoolInteger(raw, limit); !ok {
				return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_SETTING_INVALID", "%s must be an integer between 1 and %d", key, limit)
			}
		}
	}
	if _, ok := gatewayPoolCooldownResetHours(extra[openAIGatewayPoolCooldownResetHoursKey]); !ok {
		return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_SETTING_INVALID",
			"%s must be an integer between 0 and %d", openAIGatewayPoolCooldownResetHoursKey, gatewayPoolCooldownResetMaxHours)
	}
	if raw := extra[openAIGatewayPoolRecoveryExtraKey]; raw != nil {
		value, ok := gatewayPoolCooldownResetHours(raw)
		if !ok || value > gatewayPoolRecoveryMax {
			return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_SETTING_INVALID",
				"%s must be an integer between 0 and %d", openAIGatewayPoolRecoveryExtraKey, gatewayPoolRecoveryMax)
		}
	}
	probe := &Account{
		ID: account.ID, Platform: account.Platform, Type: account.Type,
		ParentAccountID: account.ParentAccountID, Credentials: account.Credentials, Extra: extra,
	}
	if !probe.UsesGatewayPool() {
		return nil
	}
	// 地址与凭据在**写入时**拦（配置已不在实例级，没有启动期可拦）。缺了就只能在转发时 fail
	// closed，让账号带着一个必然失败的配置落库等于埋雷。
	if err := config.ValidateAbsoluteHTTPURL(probe.gatewayPoolBaseURL()); err != nil {
		return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_BASE_URL_INVALID",
			"account %d enables %s so %s must be an absolute http(s) url: %v",
			account.ID, openAIGatewayPoolExtraKey, openAIGatewayPoolBaseURLExtraKey, err)
	}
	if probe.gatewayPoolConsumerKey() == "" {
		return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_CONSUMER_KEY_REQUIRED",
			"account %d enables %s so %s must be set",
			account.ID, openAIGatewayPoolExtraKey, OpenAIGatewayPoolConsumerKeyExtraKey)
	}
	// 降智防护的档位 2026-10-03 删了，所以这里也不再校验那个键。**刻意不改成「带这个键就报错」**：
	// 校验看的是合并后的 extra，存量行里那个死键还在，报错会让它们一存就失败。
	return nil
}

// 双语单串：这两条会原样进网关客户端的错误响应和 Ops 错误日志，而转发面没有 i18n 协商通道
// （与 cyberSessionBlockedClientMsg 同口径；管理端那边走 reason code + 前端 i18n）。
// 刻意只说类别，不带地址、不带 consumer key——凭据一个字都不许出现在报错里。
const (
	gatewayPoolNoSlotClientMsg = "网关池当前没有满血槽位，这一发按失败处理（绝不退回降智路由），" +
		"稍后重试即可 / The gateway pool has no full-strength slot right now, so this request fails " +
		"instead of falling back to the degraded route; retry shortly"
	gatewayPoolUnavailableClientMsg = "网关池不可用或配置有误（检查账号的池子地址与 consumer key）" +
		" / The gateway pool is unreachable or misconfigured (check this account's pool base_url and " +
		"consumer key)"
)

// OpenAIGatewayPoolReason 是网关池这一侧失败的原因码。
//
// 有它之前，池子没票 / 池子容器重启 / 退避中 / state-echo 判降智，普通使用者在 Codex CLI
// 里看到的统一是一句 `{"error":{"type":"upstream_error","message":"Upstream request failed"}}`
// ——分不出「稍后重试就行」和「运维得去改配置」，也不知道该等多久。上面那三条双语说明
// 写得很清楚，却只进了 Ops 日志，一个字到不了客户端。
// 同型先例：OpenAITurnStateHoldReason（注释写的正是「客户端一眼能看出不是上游故障」）。
const OpenAIGatewayPoolReason = GatewayFailureReason("openai_gateway_pool")

// gatewayPoolHopelessCodes 是「重试不会好」的池子错误码：要人去改 key 或改参数。
//
// 分流线刻意按**重试会不会好**划，不按谁的责任。no_exit / public_closed / upstream_rejected
// 也不是我们的错，但它们**会自愈**，所以「稍后重试即可」对它们是对的，留在 NoSlot 那条。
var gatewayPoolHopelessCodes = map[string]struct{}{
	gwpool.CodeConsumerRejected: {}, // 我们的 consumer key 配错 / 被吊销（owner 一拆就会变）
	gwpool.CodeBadRequest:       {}, // 我们发出去的参数不合法，池子刻意不给 retry_after
}

// gatewayPoolClientMessage 挑出该交给客户端的那一条，空串 = 这不是池子的错。
//
// 判序不能动：errOpenAIGatewayPoolRouteDegraded 自己就包着 ErrPool，排在后面会被吃掉。
//
// 码要在 ErrNoSlot 之前看：*PoolError **全部** Unwrap 到 ErrNoSlot（pkg/gwpool 的设计，
// 让 fail-closed 判断不用逐码改），所以仅按 errors.Is 分流的话十一个码只剩一句话。
// 2026-10-02 现场：池子回 consumer_rejected（owner 拆分后 key 变了），客户端却收到
// 「没有满血槽位……稍后重试即可」，而那一刻池子有 51 个空闲网关、一个槽位都不缺。
func gatewayPoolClientMessage(err error) string {
	var refused *gwpool.PoolError
	if errors.As(err, &refused) && refused.Code == gwpool.CodeConsumerRateLimited {
		return "网关池取票频率受限，请按 Retry-After 重试 / Gateway ticket request rate limited; retry after Retry-After"
	}
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errOpenAIGatewayPoolRouteDegraded):
		return gatewayPoolDegradedClientMsg
	case errors.Is(err, errOpenAIGatewayPoolWarmExhausted):
		return gatewayPoolWarmExhaustedClientMsg
	case errors.Is(err, errOpenAIGatewayPoolWarmUnverified):
		return gatewayPoolWarmUnverifiedClientMsg
	case gatewayPoolHopeless(err):
		return gatewayPoolUnavailableClientMsg
	case errors.Is(err, gwpool.ErrNoSlot):
		return gatewayPoolNoSlotClientMsg
	case errors.Is(err, gwpool.ErrPool):
		return gatewayPoolUnavailableClientMsg
	default:
		return ""
	}
}

// gatewayPoolRetryAfter 报告该让客户端歇多久再来（0 = 不报）。
//
// 经 UpstreamFailoverError.ResponseHeaders 交给 handler 转出；结构化池退避优先，
// 未指定时用统一客户端退避，避免客户端立即重发扩大供给压力。
func gatewayPoolRetryAfter(err error) time.Duration {
	var refused *gwpool.PoolError
	if errors.As(err, &refused) && refused.RetryAfter > 0 {
		return refused.RetryAfter
	}
	return openAIGatewayPoolClientRetryAfter
}

// openAIGatewayPoolClientRetryAfter 是池子没给具体时长时报给客户端的 Retry-After。
//
// 不以候选品质或本地 CD 档位推断这个客户端重试间隔。
const openAIGatewayPoolClientRetryAfter = 30 * time.Second

// gatewayPoolHopeless 报告这个错误的码属不属于「重试不会好」。
// 没报码（池子回了闭集外的值）⇒ false：不凭空把供给不足升级成「你配错了」。
func gatewayPoolHopeless(err error) bool {
	var refused *gwpool.PoolError
	if !errors.As(err, &refused) {
		return false
	}
	_, ok := gatewayPoolHopelessCodes[refused.Code]
	return ok
}

// gatewayPoolClientError 给池子侧的失败套一层双语说明。
//
// 一处收口而不是逐条改措辞：池子的原始错误（HTTP 401/403/503、解码失败、connection refused）
// 都在 pkg/gwpool 里，它们是诊断信息；运维要看的是「这一发为什么失败、该去改什么」。
// 用 %w 包裹 ⇒ errors.Is(err, gwpool.ErrPool) 仍然成立，传输错误分类器的豁免不受影响。
func gatewayPoolClientError(err error) error {
	if err == nil {
		return nil
	}
	// 和 gatewayPoolClientMessage 同一条分流线：客户端看到的那串就是这里拼的前缀
	// （2026-10-02 那发 503 的正文），两处必须一致，否则日志和客户端各说一套。
	if errors.Is(err, gwpool.ErrNoSlot) && !gatewayPoolHopeless(err) {
		return fmt.Errorf("%s: %w", gatewayPoolNoSlotClientMsg, err)
	}
	return fmt.Errorf("%s: %w", gatewayPoolUnavailableClientMsg, err)
}

// gatewayPoolDowngradesWSUpstream 报告这一发本该走的 WS 上游要改走 HTTP/SSE，并把理由打出来。
// **选路层的两个点共用**：HTTP 主路径据此改决策（resolveOpenAIWSDecisionByGatewayPool），
// WS 入站据此强制 HTTP 桥（openai_ws_forwarder_ingress.go 的 forceHTTPBridge）。
// 刻意不改 OpenAIWSProtocolResolver.Resolve：它有三个消费方，只有两个该降级。
//   - 调度器的 isOpenAIAccountTransportCompatible（openai_account_scheduler.go）：**不改**。
//     它问的是「这个账号能不能接 WS 客户端」，在那里降级会让网关池账号整个选不出来（没有可用账号）。
//   - 粘性 previous_response_id 绑定（openai_ws_forwarder_support.go）：**要降级**，那条路已按
//     gatewayPoolTakesOverWSUpstream 补上（否则绑定留着、下一发被钉回来又走 HTTP）。
//   - 本文件这两个选路点（HTTP 主路径 + WS 入站）：降级。
//
// 为什么必须降级而不是报错：mode_router_v2 开着时账号**不用显式开 WS** 也会被判成 WS 上游
// （ResolveOpenAIResponsesWebSocketV2Mode 回落到全局 ingress_mode_default），于是每一发都在
// AttachRoute 上撞 ErrGatewayPoolWSIncompatible。
//
// 日志是每请求一条：这个组合只出现在开了网关池的那一两个账号上，而降级的理由必须看得见。
func gatewayPoolDowngradesWSUpstream(decision OpenAIWSProtocolDecision, account *Account) bool {
	if !gatewayPoolTakesOverWSUpstream(decision, account) {
		return false
	}
	slog.Info("gwpool_downgrades_ws_to_http_sse", "account_id", account.ID,
		"ws_transport", decision.Transport, "ws_reason", decision.Reason,
		"reason", "pool routing requires per-turn verification and replacement, not a pinned upstream WebSocket")
	return true
}

// gatewayPoolTakesOverWSUpstream 是不打日志的纯判据：这个账号本该走的 WS 上游实际走 HTTP/SSE。
// 给「只是想知道实际传输是什么」的消费方用（粘性 previous_response_id 绑定），它们每请求都问，
// 不该跟着刷降级日志。
func gatewayPoolTakesOverWSUpstream(decision OpenAIWSProtocolDecision, account *Account) bool {
	return decision.Transport != OpenAIUpstreamTransportHTTPSSE && account.UsesGatewayPool()
}

// resolveOpenAIWSDecisionByGatewayPool 与 resolveOpenAIWSDecisionByClientTransport 同型：
// 决策产出后的后置过滤器，挂在消费决策的地方。
func resolveOpenAIWSDecisionByGatewayPool(
	decision OpenAIWSProtocolDecision,
	account *Account,
) OpenAIWSProtocolDecision {
	if !gatewayPoolDowngradesWSUpstream(decision, account) {
		return decision
	}
	return openAIWSHTTPDecision("gwpool_takeover")
}

// mergeOpenAIGatewayPoolConsumerKey 把提交上来的 consumer key 并进 incoming。
//
// 页面从不回显原值（dto 把它脱敏成 bool），所以 incoming 里**只有非空字符串**才算「要改成这个」，
// 空串 / 原样提交回来的 bool 都是「别动」：剔掉该项，再从 existing 续上原值。
// 没有这一道，每次保存账号都会把凭据抹掉。
//
// existing 传 nil 用于**部分更新**（jsonb 合并）：剔掉就天然不动库里那份，不能从某个账号的
// existing 里回填——批量更新共用一份 input.Extra，回填会把一个账号的凭据写进其它账号。
//
// 清除凭据刻意没有入口：误抹凭据比少一个清除按钮代价大，关开关即可停用。
func mergeOpenAIGatewayPoolConsumerKey(existing, incoming map[string]any) {
	if incoming == nil {
		return
	}
	if value, ok := incoming[OpenAIGatewayPoolConsumerKeyExtraKey].(string); ok &&
		strings.TrimSpace(value) != "" {
		return
	}
	delete(incoming, OpenAIGatewayPoolConsumerKeyExtraKey)
	if kept, ok := existing[OpenAIGatewayPoolConsumerKeyExtraKey]; ok {
		incoming[OpenAIGatewayPoolConsumerKeyExtraKey] = kept
	}
}

// openAIGatewayPoolPair is one cached route pair. until is the pool's advisory
// valid_for_s timestamp, never a reuse/expiry decision or verification deadline.
// version 是池子给这**一张具体的票**的身份（cookie_version）：本地精确清理与
// 「这张不行、别再给我」（exclude_versions）都靠它，落库后还能和池子的日志对上账。
// 可以为空（池子没报 / 报了畸形值）—— 那只是少了这几个能力，不是丢票的理由。
//
// 必须保持可比较（sync.Map 的 CompareAndDelete 要用，见 gatewayPoolDiscardUnsent）。
type openAIGatewayPoolPair struct {
	cookie            string
	gateway           string
	datacenterCountry string
	// region 是池子报的「铸这张票的出口属于哪个大区」。只用于账号卡片上按大区归档落点，
	// 一个判定都不接（口径见 gwpool.Pair.Region）。空 = 池子没报。
	region         string
	version        string
	until          time.Time // advisory only, not a quality or route-expiry decision
	routeExpiresAt time.Time // explicit cookie expiry only; zero = unknown
	invalidated    bool      // confirmed state-echo rejection, independent of TTL
	invalidatedAt  time.Time
	// since 是取票时刻，只用于识别缓存窗口；不能作为上游首次接触的起点。
	//
	// **不能拿 until 推**：until 是池子的交付租约（gwpool 的 DeliverTTL，150 秒），
	// 不是实际使用起点，也不是 Cookie 到期时间。
	//
	since time.Time
	// firstSent 排除明确未发送的前置失败；响应不明的传输尝试保守计时。
	// 不包括取票等待；缺失时票龄回落最严档，不编造首次接触。
	firstSent time.Time
	roundID   string // 不含票据/凭据；同缓存窗口续票时保留。
}

// openAICodexCredentialIdentity 解析「凭证域身份」：影子行自己不持凭据，必须按母账号算。
// 它同时是池子侧的 account_id 与 pair 缓存键——同一个 ChatGPT 账号的多个本地行（克隆行、影子行）
// 烧的是同一个 (账号 × 网关) 单位，按本地行分键会让池子把它们当几个独立单位排班，也会让一个单位
// 同时持有两张 pair、把两个网关一起烧掉。
type openAICodexCredentialIdentity func(ctx context.Context, account *Account) (string, error)

// codexCredentialIdentity 是上面那个解析器的生产实现（构造器注入，裸结构体的单测里为 nil）。
// 约定见 docs/conventions/codex-outbound-identity.md：owner 必须过 codexAccountIdentitySource /
// 影子解析，不能直接拿本地行算。这里走 repo 而不是 gin 上下文，因为出站挂钩点拿不到 *gin.Context。
func (s *OpenAIGatewayService) codexCredentialIdentity(ctx context.Context, account *Account) (string, error) {
	if account == nil {
		return "", errGatewayPoolMemberIdentity
	}
	source := account
	if account.IsShadow() {
		resolved, err := resolveCredentialAccount(ctx, s.accountRepo, account)
		if err != nil {
			// 影子行解析不出母账号就是坏行（它的凭据本来也来自母账号）。宁可失败也不要按影子行
			// 自己上报——那会把同一个单位记成两个槽位，而供给只有个位数张。
			return "", err
		}
		source = resolved
	}
	return gatewayPoolMemberIdentity(source)
}

// Pool references use the already resolved credential source, including member.
// No workspace-only or row-ID fallback may collapse or split credential domains.
func gatewayPoolUpstreamAccountID(identity string) string {
	member, ok := strings.CutPrefix(identity, gatewayPoolMemberIdentityPrefix)
	if !ok {
		return ""
	}
	return member
}

// gatewayPoolTakeover 报告这一发该由池子出 cookie。
func (s *openAICodexCookieStore) gatewayPoolTakeover(account *Account) bool {
	return s != nil && account.UsesGatewayPool()
}

// The injected resolver handles shadow credential sources. Offline callers use
// the same member validation and never invent a row/workspace fallback.
func (s *openAICodexCookieStore) gatewayPoolIdentity(ctx context.Context, account *Account) (string, error) {
	if s.identity != nil {
		return s.identity(ctx, account)
	}
	return gatewayPoolMemberIdentity(account)
}

// gatewayPoolLedgerKey 是本地账本的键：上游账号 + 网关名。
//
// 键**绝不能**是本地账号行 ID：同一份 Codex 凭据可能挂在多个 sub2api 账号行上（克隆行、
// 影子行），按行记会让每一行都以为自己还有满血窗口，其实烧的是同一个 (上游账号 × 网关) 单位。
// \x00 当分隔符——身份与网关名都不可能含 NUL，拼不出歧义键。
func gatewayPoolLedgerKey(identity, gateway string) string {
	return identity + "\x00" + gateway
}

// gatewayPoolUsedRecently 查本地账本：这个上游账号在 window 内拿到过这个网关的票没有。
//
// 为什么不能只信池子的 used_by_you：池子按它发的 consumer key 认账号，而同一份 Codex 凭据
// 可能挂在多个账号行、各自配着不同的 key；池子那本账对不上真正被烧掉的那个单位。
func (s *openAICodexCookieStore) gatewayPoolUsedRecently(identity, gateway string, window time.Duration) bool {
	_, used := s.gatewayPoolUsedAt(identity, gateway, window)
	return used
}

// gatewayPoolUsedAt 同上，但把**什么时候碰的**一起带回来：挑落点时要在「全都碰过」的那一格里
// 挑碰得最早的那个（见 gatewayPoolPick），光一个 bool 排不出序。
func (s *openAICodexCookieStore) gatewayPoolUsedAt(identity, gateway string, window time.Duration) (time.Time, bool) {
	value, ok := s.poolUsed.Load(gatewayPoolLedgerKey(identity, gateway))
	var at time.Time
	if ok {
		at, _ = value.(time.Time)
	}
	if !at.After(s.gatewayPoolCooldownClearAt(identity)) {
		at = time.Time{}
	}
	base := gatewayPoolCooldownBase(window)
	until := at.Add(time.Duration(base) * time.Second)
	s.poolCooldownMu.Lock()
	key := gatewayPoolLedgerKey(identity, gateway)
	if c, found := s.poolCooldown[key]; found {
		s.refreshGatewayPoolCooldown(&c, identity, window, at, time.Now().UTC())
		s.poolCooldown[key] = c
		until = c.Until
		if touchedUntil := at.Add(time.Duration(c.WindowSeconds) * time.Second); !c.Cleared && touchedUntil.After(until) {
			until = touchedUntil
		}
	}
	s.poolCooldownMu.Unlock()
	return at, time.Now().Before(until)
}

// gatewayPoolMarkUsed 记一笔「这个上游账号碰过这个网关」。
//
// 不需要清理：条目只会被同一个键原地覆盖，键空间是 (上游账号 × 网关) 的笛卡尔积（个位数账号 ×
// 两百来个网关），而判定本来就是按时间算的，过期条目不会影响结论。
func (s *openAICodexCookieStore) gatewayPoolMarkUsed(identity, gateway string, expectedReset ...time.Time) {
	if identity == "" || gateway == "" {
		return
	}
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	if len(expectedReset) > 0 && !expectedReset[0].Equal(s.gatewayPoolCooldownResetAt(identity)) {
		return
	}
	s.poolUsed.Store(gatewayPoolLedgerKey(identity, gateway), time.Now())
}

// gatewayPoolHydrateUsed 把账号行上那份**落库的**落点记录补回内存账本。
//
// poolUsed 是进程内存、重启即失，而这本账决定两件事：挑落点时过滤掉哪些候选
// （gatewayPoolPick），以及裸取时 /cookie 带哪些 exclude。空账本的后果不是「少避开几个」，
// 是**把刚烧过的网关原样发回来**：
//
//	2026-10-02 现场，池子对同一个号说「45 个候选网关都还在 4h 冷却里」，而我们这一发的
//	gwpool_pair_taken 打的是 excluded=6 —— 那 6 是重启之后重新数起来的。
//
// 补的是 openAIGatewayHistoryExtraKey 那条记录，它按账号行记、报的是真相的子集（见那个常量
// 的注释）⇒ 只会少避、不会错避，正是这个用途要的方向。
//
// 时间只往后对齐：内存里那条若更新（这个进程自己刚取的票），不许被落库的旧读数盖回去。
// 所以每次取票前调一遍是幂等的，不另记「这个账号补过了」。
func (s *openAICodexCookieStore) gatewayPoolHydrateUsed(account *Account, identity string) {
	if s == nil || identity == "" {
		return
	}
	rec, ok := readOpenAIGatewayHistory(account)
	if !ok {
		return
	}
	rec = gatewayPoolHistoryForTag(rec, gatewayPoolLedgerTag(identity))
	if rec.LedgerTag != "" {
		s.applyGatewayPoolCooldownClear(identity, rec.CooldownReset.ClearedAt,
			gatewayPoolCooldownBase(account.gatewayPoolGatewayWindow()))
		s.applyGatewayPoolCooldownReset(identity, rec.CooldownReset.LastAt,
			gatewayPoolCooldownBase(account.gatewayPoolGatewayWindow()))
	}
	for gateway, seen := range rec.Seen {
		if gateway == "" || seen.At.IsZero() {
			continue
		}
		key := gatewayPoolLedgerKey(identity, gateway)
		s.poolKnown.Store(key, struct{}{})
		// 旧版无身份绑定的记录只恢复触碰时间，不信任其中的自适应学习字段。
		if rec.LedgerTag != "" {
			s.hydrateCooldown(identity, gateway, seen.Cooldown, gatewayPoolCooldownBase(account.gatewayPoolGatewayWindow()), seen.At)
		}
		if !seen.At.After(s.gatewayPoolCooldownClearAt(identity)) {
			continue
		}
		if prev, loaded := s.poolUsed.Load(key); loaded {
			if at, isTime := prev.(time.Time); isTime && !seen.At.After(at) {
				continue
			}
		}
		s.poolUsed.Store(key, seen.At)
	}
}

// gatewayPoolBurnedGateways 列出这个身份在窗口内烧过的网关，**最近烧的在前**，最多 limit 项。
//
// 这本账本原先只用来在客户端过滤 /gateways 的候选，一旦退回裸取（列不出来、挑不出来、点名的
// 那个被别人租走）这份知识就全浪费了——而裸取恰好是最需要它的时候。带成 /cookie 的 exclude
// 之后，池子自己挑的时候也避得开。
//
// 排序而不是随便给 64 项：池子的 exclude 有 64 项上限，超了会拒掉**整条**请求，所以超限时
// 必须自己裁，而最近烧的那些最可能还在冷却期里（最该排除）。
func (s *openAICodexCookieStore) gatewayPoolBurnedGateways(identity string, window time.Duration, limit int) []string {
	if s == nil || identity == "" || limit <= 0 {
		return nil
	}
	prefix := identity + "\x00"
	type burned struct {
		gateway string
		at      time.Time
	}
	var all []burned
	s.poolUsed.Range(func(key, value any) bool {
		name, ok := key.(string)
		if !ok {
			return true
		}
		gateway, matched := strings.CutPrefix(name, prefix)
		if !matched || gateway == "" {
			return true
		}
		at, ok := value.(time.Time)
		if !ok || !s.gatewayPoolUsedRecently(identity, gateway, window) {
			return true
		}
		all = append(all, burned{gateway: gateway, at: at})
		return true
	})
	slices.SortFunc(all, func(a, b burned) int { return b.at.Compare(a.at) })
	if len(all) > limit {
		all = all[:limit]
	}
	out := make([]string, 0, len(all))
	for _, item := range all {
		out = append(out, item.gateway)
	}
	return out
}

// gatewayPoolBackoffEntry 是一次退避：到点 + **当初是哪个码让我们退的**。
//
// 为什么要留码：退避一开，后面整段时间里业务请求一个池子请求都不发（见 gatewayPoolPair），
// 可报的只有这个退避本身。只记到点的话 consumer_rejected（要人去改 key）和 all_cooling
// （等几小时）在退避期里长得一模一样，而 gatewayPoolClientMessage 要靠码分流文案。
type gatewayPoolBackoffEntry struct {
	Until time.Time
	Code  string
}

// gatewayPoolBackoffFor 报告这个身份现在还要停多久不取票（0 = 可以取），以及当初的码。
func (s *openAICodexCookieStore) gatewayPoolBackoffFor(identity string) (time.Duration, string) {
	value, ok := s.poolBackoff.Load(identity)
	if !ok {
		return 0, ""
	}
	entry, ok := value.(gatewayPoolBackoffEntry)
	if !ok {
		return 0, ""
	}
	return time.Until(entry.Until), entry.Code
}

// gatewayPoolBackoff 按池子的错误码决定要不要停这个身份的取票，并返回「停多久」（0 = 不停）。
//
// 为什么必须分流：原先所有失败一视同仁，客户端的重试环每轮都会在池子侧触发一次发现铸票
// （池子的闸是每个号 30 分钟一次），N 个账号就是 30 分钟内 N 发白烧的上游请求。
//
//	all_cooling                 这个身份在**当时那批候选网关**上都还在冷却 ⇒ 退避但**不采信
//	                            retry_after**，见下面的封顶
//	rate_limited                池子**铸票**预算满了，和「有没有票给我」无关 ⇒ **不退避**
//	no_exit                     池子整体故障（出口全熔断），换网关再试只是把同一个错重复一遍
//	upstream_rejected           池子侧的账号/凭据问题，与我们无关，等
//	consumer_rejected           我们自己的 consumer key 没被受理（配错 / 被吊销）：重试一万次
//	                            也还是这个结果，而池子会按契约给 300s。运维要去改 key
//	bad_request                 我们发出去的参数不合法（要改代码或账号配置），池子刻意不给
//	                            retry_after ⇒ 时长本地兜（openAIGatewayPoolBadRequestBackoff）
//	其余（no_live_pair / no_gateway / mint_failed / public_closed / degraded / 没报码）
//	                            可等待或一次性，交给 wait 与既有的一次裸取重试，不退避
func gatewayPoolBackoff(err error) (time.Duration, string) {
	var refused *gwpool.PoolError
	if !errors.As(err, &refused) {
		return 0, ""
	}
	var fallback time.Duration
	switch refused.Code {
	case gwpool.CodeAllCooling:
		// **只有这个码的 retry_after 不是全局事实**，所以单独一支。池子算它用的是
		// `slot_cooldown`（默认 4h，而那个数按 codex-full-strength-tickets.md 至今没测准）
		// 减去那一行在**这次那批候选网关**上的已歇时长 ⇒ 它回答的是「在刚才那批网关上你最早
		// 何时出冷却」，不是「池子在此之前给不了你票」。候选集随时会变：新账号上传、发现新
		// 网关、保温补票，任何一件都能让答案立刻失效。
		//
		// 2026-10-02 现场：据此静默 3h47m，而 22 分钟后池子就有 3 个这个号从没碰过、带着活票
		// 的网关；期间每一发业务请求都直接 502（fail-closed，绝不回落降智路由）。
		//
		// 封顶到 fallback 而不是干脆不退避：真全烧时还是该慢下来。而「别猛敲」池子侧本来就有
		// 自己的闸——失败会排一次发现铸票，那条队列对每个铸票号 30 分钟才放一发，所以这里敲得
		// 再勤也烧不出更多上游请求；这一分钟换的只是 HTTP 往返和日志量。
		if refused.RetryAfter > 0 && refused.RetryAfter < openAIGatewayPoolDefaultBackoff {
			return refused.RetryAfter, refused.Code // 池子说更短就听它的
		}
		return openAIGatewayPoolDefaultBackoff, refused.Code
	case gwpool.CodeRateLimited:
		// **一秒都不退避。** 这个码管的是池子的**铸票**预算，不是「它有没有票给我」。
		//
		// 池子的闸按 (铸票账号 × 区域) 算，6 发/小时，铸票和 state-echo 共用
		// （gwpool mint.go 的 spend / VerifyFull）。撞上它只说明「我现在得替你铸、但这个
		// 号在这个区域铸不动了」——而池子只在一张活票都没有时才铸（SPEC 第 8 节第 3 步）。
		// 别的托管账号铸出的票、别的区域的票、池子续活的票，在它说的那
		// 「一小时」里全都是可交付的。采信那个 retry_after 就是拿**池子内部某个号的配额**
		// 去停掉**我们所有业务请求**，最长静默一小时，期间每一发聊天直接 502。
		// 这和 all_cooling 那次（现场静默 3h47m，TestGatewayPoolCapsAllCoolingBackoff）
		// 是同一个错，只是天花板换成了 1 小时。
		//
		// 不退避也不会打出风暴：这个闸是池子进程内的滑动窗口，满了**一发上游都不发**，
		// 拒绝是即时的，代价只有一次 HTTP 往返。相反，退避期里池子刚补上的票我们拿不到。
		return 0, refused.Code
	case gwpool.CodeConsumerRateLimited:
		fallback = time.Second
	case gwpool.CodeNoExit,
		gwpool.CodeUpstreamRejected, gwpool.CodeConsumerRejected:
		// 这几个的 retry_after 是**全局**的：整池出口熔断、凭据被否决，
		// 都不随候选网关集变化 ⇒ 照旧采信。
		fallback = openAIGatewayPoolDefaultBackoff
	case gwpool.CodeBadRequest:
		fallback = openAIGatewayPoolBadRequestBackoff
	default:
		return 0, refused.Code
	}
	if refused.RetryAfter > 0 {
		return refused.RetryAfter, refused.Code // pkg 侧已钳进 [0, MaxRetryAfter]
	}
	return fallback, refused.Code
}

func gatewayPoolMissingTicket(err error) bool {
	var refused *gwpool.PoolError
	return errors.As(err, &refused) &&
		(refused.Code == gwpool.CodeNoLivePair || refused.Code == gwpool.CodeNoGateway)
}

// gatewayPoolPick makes a local decision from metadata and current local CD.
// It never uses the pool's historical consumer touches to rank or admit a route.
func (s *openAICodexCookieStore) gatewayPoolPick(
	ctx context.Context,
	account *Account,
	identity string,
	gateways []gwpool.Gateway,
	blocked map[string]bool,
) string {
	s.noteGatewayPoolCountries(account, gateways)
	if s.poolCooldownPersist != nil {
		defer s.poolCooldownPersist(ctx, account, identity)
	}
	// /gateways 只列此刻有活 pair 的网关 ⇒ 列表长度就是池子的可交付网关数。账号卡片拿它当
	// 「还剩几个落点没用」的分母（见 OpenAIGatewayPoolApplied.PoolLive）。
	var eligible []gwpool.Gateway
	// free =「池子此刻可交付、而且**本地账本**说这个号还没烧过」的落点数，也就是
	// 「池子剩余」。每次拉到清单都拿它和本地账本现对一遍，不缓存、不事后推算。
	//
	// 两条纪律：
	//
	//  1. **不能拿「可交付总数 − 账本里用过的个数」去减。** 那两个集合不是包含关系：账本是
	//     过去一个窗口里碰过的网关名（票早过期的也在里面），清单是此刻还有活票的，相减能出
	//     负数，夹到 0 就成了「池子用完了」（2026-10-03 现网：账本 67、可交付 62，报成 0）。
	//  2. **只问本地账本，不看 candidate.UsedByYou。** 池子那本账记的是 LastTouch，而铸票和
	//     扫描也写它（见池子 gateways.go 的注释）—— 一个在池子里也当铸票者的号，几乎每个它
	//     铸过的网关都会被标成 used_by_you，于是这个数恒为 0。而「这个 (账号 × 网关) 的满血
	//     窗口烧没烧」只有本地账本答得准：它记的是**这个号真的打过业务请求**的那些落点。
	//     选择、报数和耗尽共用同一准入；/cookie 仍执行池端短期保护和真实退避。
	free := 0
	window := account.gatewayPoolGatewayWindow()
	for _, candidate := range gateways {
		s.noteGatewayPoolFeedbackPolicy(account, identity, candidate.Name, candidate.Cooldown)
		_, burned := s.gatewayPoolUsedAt(identity, candidate.Name, window)
		if !candidate.ReadyAt(time.Now()) || blocked[candidate.Name] {
			continue
		}
		if !burned {
			free++
		}
		if !burned {
			eligible = append(eligible, candidate)
		}
	}
	// 两个数一起记：live 是「池子此刻能交付几个」，free 是「其中这个号还没烧过几个」。
	// 成对下发，卡片才能把 free=0（全烧过了）和「没问到清单」分开。
	openAIGatewayPoolSinkFrom(ctx).notePoolCounts(len(gateways), free)
	if len(eligible) > 0 {
		// FIFO and measured local ranking are owned here, not by pool history.
		ranking := s.gatewayPoolRankCandidates(ctx, account, identity, eligible)
		return s.gatewayPoolCandidateQueue(identity).pick(ranking.candidates, ranking)
	}
	s.gatewayPoolCandidateQueue(identity).pick(nil)
	return ""
}

type gatewayPoolFetchResult struct {
	pair openAIGatewayPoolPair
	took bool
}

// gatewayPoolPair 取该身份当前可用的 pair：窗口内复用缓存，否则向池子要一张。
//
// 同一身份的并发请求用 singleflight 收口成一次 /cookie：池子一个网关一周期只出一张 pair，
// 并发各要一张就是白烧供给。共享的那次取用自己的 ctx（WithoutCancel + 独立超时）——否则第一名
// 的客户端一断开，排在它后面的同账号请求会被连坐成 502。
//
// fresh 报告这张票是不是**这一发自己取回来的**（缓存复用时为 false）：只有自己取的那张才可能
// 确证未发时本地丢弃；共享和缓存复用方没有清理权。
func (s *openAICodexCookieStore) gatewayPoolPair(ctx context.Context, account *Account, identity string) (pair openAIGatewayPoolPair, fresh bool, err error) {
	finish := s.gatewayPoolInventoryOperation(identity)
	defer finish()
	cached, state := s.cachedPoolPair(identity)
	if state == openAIGatewayPoolPairLive {
		return cached, false, nil
	}
	// 退避只挡**取新票**：手里那张还活着就照常用（上面已经返回）。
	// 不在 singleflight 里判：退避期一个池子请求都不该发，包括排队的那些。
	if remaining, code := s.gatewayPoolBackoffFor(identity); remaining > 0 {
		// 身份不进报错（它含上游 account_id）；账号 id 进日志，与本文件其它日志同口径。
		slog.Debug("gwpool_backoff_active", "account_id", account.ID,
			"code", code, "remaining_s", int(remaining.Seconds()))
		// 把当初那个码原样带回去：退避期里一个池子请求都不发，这是唯一的线索来源。
		// 包成 *PoolError 而不是另造一种错 ⇒ gatewayPoolClientMessage 的分流一个字都不用改。
		// Status 0 = 这一发没发出去，没有真实 HTTP 状态可报。
		// RetryAfter 带上剩余时长：它一路走到客户端的 Retry-After 头（见 gatewayPoolRetryAfter）。
		// 退避期里每一发业务请求都是 0.2 秒的 503，而 Codex CLI 对 503 会立刻重发 ——
		// 2026-10-02 现场一次退避循环打出 200 发 503，用户那头看起来就是卡死。
		return openAIGatewayPoolPair{}, false, fmt.Errorf(
			"%w: pool asked this account to back off for another %ds",
			&gwpool.PoolError{Code: code, RetryAfter: remaining}, int(remaining.Seconds()))
	}
	pool, err := s.poolClient(account)
	if err != nil {
		return openAIGatewayPoolPair{}, false, err
	}
	// Confirmed rejection or the route credential's own deadline rotates.
	// Crossing valid_for_s alone never changes the cached ticket.
	waitCtx, waitCancel := context.WithTimeout(ctx, account.gatewayPoolFetchTimeout())
	defer waitCancel()
	if err := waitCtx.Err(); err != nil {
		return openAIGatewayPoolPair{}, false, err
	}
	taken, shared, fetchErr := s.poolFetch.do(waitCtx, identity, account.gatewayPoolFetchTimeout(), func(fetchCtx context.Context) (gatewayPoolFetchResult, error) {
		// 排在后面的请求醒来时第一名可能已经取到了。
		if pair, cached := s.cachedPoolPair(identity); cached == openAIGatewayPoolPairLive {
			return gatewayPoolFetchResult{pair: pair}, nil
		}
		callCtx, cancel := context.WithTimeout(fetchCtx, account.gatewayPoolFetchTimeout())
		defer cancel()
		var excludeVersions []string
		// Reject the retired ticket explicitly; choose its replacement locally.
		if state == openAIGatewayPoolPairStale && cached.version != "" {
			excludeVersions = []string{cached.version}
		}
		pair, resetAt, err := s.gatewayPoolTakePair(callCtx, pool, account, identity, excludeVersions)
		if err != nil {
			return gatewayPoolFetchResult{}, err
		}
		if err := fetchCtx.Err(); err != nil {
			return gatewayPoolFetchResult{}, err
		}
		if pair.routeExpired(time.Now()) {
			return gatewayPoolFetchResult{}, fmt.Errorf("%w: route credential expired before publication", gwpool.ErrNoSlot)
		}
		if !s.publishGatewayPoolFetchedPair(identity, pair, resetAt) {
			return gatewayPoolFetchResult{}, errGatewayPoolGenerationChanged
		}
		return gatewayPoolFetchResult{pair: pair, took: true}, nil
	}, func() func() {
		workerFinish := s.gatewayPoolInventoryOperation(identity)
		return func() {
			workerFinish()
			if s.poolUsageFinished != nil {
				s.poolUsageFinished(context.WithoutCancel(ctx), account)
			}
		}
	})
	if fetchErr != nil {
		return openAIGatewayPoolPair{}, false, fetchErr
	}
	// 仅独占新取的调用者持有本地清理权；共享和缓存复用均没有。
	return taken.pair, !shared && taken.took, nil
}

// gatewayPoolTakePair acquires only the ticket needed now. Local cooldown and
// the clear/reset epoch are rechecked before admission. The shared fetch caller
// publishes the pair under that same epoch; advisory TTL is not a gate.
func (s *openAICodexCookieStore) gatewayPoolTakePair(
	ctx context.Context,
	pool *gwpool.Client,
	account *Account,
	identity string,
	excludeVersions []string,
) (pair openAIGatewayPoolPair, resetAt time.Time, err error) {
	// Hydrate persisted local cooldown before selecting any candidate.
	if err := s.hydrateGatewayPoolSharedHistory(ctx, account, identity); err != nil {
		return pair, resetAt, fmt.Errorf("%w: unable to read current gateway cooldown", gwpool.ErrPool)
	}
	resetAt = s.gatewayPoolCooldownResetAt(identity)
	blocked := make(map[string]bool)
	if stale, state := s.cachedPoolPair(identity); state == openAIGatewayPoolPairStale {
		blocked[stale.gateway] = true
		if stale.version != "" && !slices.Contains(excludeVersions, stale.version) {
			excludeVersions = append(excludeVersions, stale.version)
		}
	}
	request := gwpool.CookieRequest{
		Account:         gatewayPoolUpstreamAccountID(identity),
		ExcludeVersions: excludeVersions,
		MinRemaining:    openAIGatewayPoolMinRemaining,
	}
	model, _ := ctx.Value(gatewayPoolProbeModelKey{}).(string)
	var generation uint64
	var got gwpool.Pair
	var steer string
	const maxDirectoryAttempts = 2
	for attempt := range maxDirectoryAttempts {
		listCtx, cancel := context.WithTimeout(ctx, account.gatewayPoolListTimeout())
		catalog, listErr := pool.Catalog(listCtx, request.Account, gatewayPoolAccountTag(account, identity), model, generation)
		cancel()
		if listErr != nil {
			if ctx.Err() == nil && errors.Is(listErr, context.DeadlineExceeded) {
				listErr = errors.Join(gwpool.ErrCatalogUnavailable, listErr)
			}
			err = listErr
			break // unreadable inventory is not exhaustion
		}
		generation = catalog.Generation
		steer = s.gatewayPoolPick(ctx, account, identity, catalog.Gateways, blocked)
		if !resetAt.Equal(s.gatewayPoolCooldownResetAt(identity)) {
			return pair, resetAt, errGatewayPoolGenerationChanged
		}
		if steer == "" {
			err = &gwpool.PoolError{Code: gwpool.CodeAllCooling}
			continue // only the second, freshly refreshed directory confirms zero
		}
		request.Gateway = steer
		got, err = pool.Cookie(ctx, request)
		if !gatewayPoolMissingTicket(err) || attempt == maxDirectoryAttempts-1 {
			break
		}
		// The next iteration demands a newer directory, then chooses explicitly.
	}
	if err != nil {
		// 池子明说了「先别来」就按身份记下来：只对本次请求生效的退避挡不住重试环，
		// 而每一轮重试都会在池子侧触发一次发现铸票。
		if backoff, code := gatewayPoolBackoff(err); backoff > 0 {
			s.rememberGatewayPoolBackoff(identity, resetAt, backoff, code)
			slog.Warn("gwpool_backoff_started", "account_id", account.ID,
				"code", code, "backoff_s", int(backoff.Seconds()))
		}
		var refused *gwpool.PoolError
		candidateFailure := gatewayPoolMissingTicket(err) ||
			(errors.As(err, &refused) && refused.Code == gwpool.CodeAllCooling)
		if candidateFailure && !resetAt.Equal(s.gatewayPoolCooldownResetAt(identity)) {
			return pair, resetAt, errGatewayPoolGenerationChanged
		}
		// 刻意**不删**缓存里那张过期的：它是「别再给我这一个」的依据，删掉之后下一发会走
		// 不带 force / 不带 exclude_versions 的 /cookie，池子可能原样把烧过的那张发回来。
		return pair, resetAt, err
	}
	if !resetAt.Equal(s.gatewayPoolCooldownResetAt(identity)) {
		return pair, resetAt, errGatewayPoolGenerationChanged
	}
	// 取到票了 ⇒ 之前那次退避（如果有）已经过去，清掉，不留一个过期的到点值。
	s.poolBackoff.Delete(identity)
	// Only the two route cookies may cross the trust boundary into ChatGPT.
	cookie := routePairOf(strings.Split(got.Cookie, ";"))
	if cookie == "" {
		return pair, resetAt, fmt.Errorf("%w: cookie response carried no route pair", gwpool.ErrPool)
	}
	gateway := strings.TrimSpace(got.Gateway)
	if gateway != steer || slices.Contains(excludeVersions, got.Version) {
		return pair, resetAt, fmt.Errorf("%w: pool returned an unrequested route or rejected version", gwpool.ErrPool)
	}
	now := time.Now()
	country := got.DatacenterCountry
	if country == "" {
		value, _ := s.poolDatacenterCountries.Load(account.gatewayPoolBaseURL() + "\x00" + gateway)
		country, _ = value.(string)
	}
	pair = openAIGatewayPoolPair{
		cookie: cookie, gateway: gateway, datacenterCountry: country,
		region: strings.TrimSpace(got.Region), version: got.Version,
		until: now.Add(got.ValidFor), routeExpiresAt: gatewayPoolRouteExpiresAt(cookie),
		since: now, roundID: gatewayPoolContactRoundID(identity, got.Version, now),
	}
	if err := ctx.Err(); err != nil {
		return openAIGatewayPoolPair{}, resetAt, err
	}
	if pair.routeExpired(time.Now()) {
		return openAIGatewayPoolPair{}, resetAt, fmt.Errorf("%w: route credential expired", gwpool.ErrNoSlot)
	}
	// The pool may ignore exclude or deliver after the listing becomes stale.
	if !s.beginGatewayPoolAttemptAt(identity, gateway, account.gatewayPoolGatewayWindow(), &resetAt) {
		if !resetAt.Equal(s.gatewayPoolCooldownResetAt(identity)) {
			return openAIGatewayPoolPair{}, resetAt, errGatewayPoolGenerationChanged
		}
		return openAIGatewayPoolPair{}, resetAt, &gwpool.PoolError{Code: gwpool.CodeAllCooling, RetryAfter: time.Minute}
	}
	slog.Info("gwpool_pair_taken", "account_id", account.ID, "gateway", gateway,
		"steered_to", steer, "excluded", len(request.Exclude),
		"valid_for_s", int(got.ValidFor.Seconds()))
	if account.IsOpenAITurnStatePairModeEnabled() {
		slog.Warn("gwpool_overrides_turn_state_pair_mode", "account_id", account.ID, "gateway", gateway)
	}
	return pair, resetAt, nil
}

// gatewayPoolDiscardUnsent returns local cleanup for an exclusively acquired
// ticket. Never clear CD or send a remote release. A concurrent replacement or
// observed send wins over cleanup, including a send on the same version.
func (s *openAICodexCookieStore) gatewayPoolDiscardUnsent(identity string, pair openAIGatewayPoolPair) func() {
	if pair.version == "" {
		return nil
	}
	return func() {
		value, ok := s.poolPairs.Load(identity)
		if !ok {
			return
		}
		current, ok := value.(openAIGatewayPoolPair)
		if ok && current.version == pair.version && current.firstSent.IsZero() {
			s.poolPairs.CompareAndDelete(identity, current)
		}
	}
}

// 缓存槽的三种状态。stale 与 none 的区别决定下一次取 pair 要不要 force：
// 手里那张过期了就得换**一个不同的网关**，而冷启动（none）照常走调度。
type openAIGatewayPoolPairState int

const (
	openAIGatewayPoolPairNone openAIGatewayPoolPairState = iota
	openAIGatewayPoolPairLive
	openAIGatewayPoolPairStale
)

// cachedPoolPair 读缓存槽：live 的那张可以直接用，stale 只用来判「该换一张不同的」。
func (s *openAICodexCookieStore) cachedPoolPair(identity string) (openAIGatewayPoolPair, openAIGatewayPoolPairState) {
	if identity == "" {
		return openAIGatewayPoolPair{}, openAIGatewayPoolPairNone
	}
	value, ok := s.poolPairs.Load(identity)
	if !ok {
		return openAIGatewayPoolPair{}, openAIGatewayPoolPairNone
	}
	pair, ok := value.(openAIGatewayPoolPair)
	if !ok || pair.cookie == "" {
		return openAIGatewayPoolPair{}, openAIGatewayPoolPairNone
	}
	if pair.invalidated || pair.routeExpired(time.Now()) {
		return pair, openAIGatewayPoolPairStale
	}
	return pair, openAIGatewayPoolPairLive
}

// OpenAIGatewayPoolApplied 是「这一发**真的**把池子那张 pair 注入了出站头」的 per-request 读数，
// usage_logs 的「已覆写 / 被改派」徽标只认它。
//
// 为什么不能回读 pair 缓存（2026-10-02 前就是那样，是个会说假话的实现）：回读只能查「接管开着 +
// 缓存里有票」，**手上没有 URL** ⇒ 认不出这一发是不是推理面。而非推理面的请求落回罐回放
// （见 AttachRoute），缓存里那张 stale 票又永不删 ⇒ 账号取过一次票之后，每一发
// 非推理面且记用量的请求（/codex/alpha/search、/codex/images/* 这些）都会落库成「已覆写 + 池子的
// 网关 + 池子的 cookie」，而出站实际带的是罐里那组。那正好把这张卡片唯一要回答的问题反过来答了。
//
// AccountID 是**这一发实际用的账号**：故障转移在同一个 ctx 里换号重试，不带账号就会把前一个号
// 注入过的读数记到后一个号的用量行上。用量侧按它校验（见 routePairInUse）。
type OpenAIGatewayPoolApplied struct {
	cooldownResetAt   time.Time
	LedgerTag         string
	AccountID         int64
	Cookie            string
	Gateway           string
	DatacenterCountry string
	// Region 是池子说的「这张票是哪个大区铸的」，给账号卡片按大区归档落点用。空 = 不知道。
	Region  string
	Version string
	RoundID string
	// Verdict 是这一发的 state-echo 读数："" = 没判（判据关着、没送票、非 200）、
	// openAIGatewayVerdictFull、openAIGatewayVerdictDegraded。
	//
	// 挂在这个快照上而不是 sink 上另存一份：一次客户端请求里可能先后落在几个落点上
	// （queue 档的预热一张张试、故障转移换号重试），而丢弃行各自快照走自己那一份 Applied
	// ⇒ 每条用量行读到的是它自己那一发的结论。
	Verdict string
	// PoolLive 是**池子此刻报的可交付网关数**（/gateways 只列有活 pair 的，所以列表长度
	// 就是它）。0 = 这一发未取得池清单（如列表打不开）。
	//
	// 它是整个池子的读数、不是这一发的，挂在这个快照上纯粹是搭车：这条已经是「池子对这一发
	// 说了什么」通向用量侧的现成管道。由 snapshot() 从 sink 合进来，不走 mark() —— 取清单
	// 发生在注入之前，让 mark 覆盖它就等于永远是 0。
	PoolLive int
	// PoolFree 是上面那些里**本地账本说这个号还没烧过**的个数 ——「池子剩余」。
	//
	// 必须由池子那一遍循环当场数出来，**不能用「PoolLive − 本地账本条目数」去减**：账本
	// 装的是过去一个窗口里碰过的网关名（票早过期的也在），和「此刻还有活票的」不是包含
	// 关系，相减会出负数，夹到 0 就成了「池子用完了」——而池子可能正有几十个落点可交付。
	// 只在 PoolLive > 0 时有意义；那时 0 是**真的 0**（全烧过了），不是「没测到」。
	PoolFree int
	// FullHeldMs 是这一发判降智时量到的满血时长（毫秒）：从「这张票验出满血」那一刻到
	// 「判成降智」那一刻。0 = 没量到（这张票没验过满血，没有窗口起点）。
	//
	// 两头都在**同一张票的生命里**，所以有界。卡片上那一段只能用这个数 —— 拿账本里的
	// 「判满血的时刻」和「判降智的时刻」相减是错的：后者要等下一次真的打到这个网关才会写，
	// 中间空闲全算进去，现网实测能得出 22655 秒。
	FullHeldMs int64
}

// openAIGatewayPoolSink 是 ctx 里承载的那个指针。
//
// 分工照 turn-state 那套（服务侧写、用量侧读），区别只有一处：AttachRoute 是三个出站挂钩点的
// 共用入口，手上**没有 gin.Context**（约定见 codexCredentialIdentity 附近），所以只能用 ctx 带
// 一个指针进去，由转发入口在返回前 publish 到 OpenAIForwardResult 上。
//
// **只写不清**：清的那一版有个真实的交错——侧信道（装饰性 GET，同 ctx）在主请求注入之后跑一遍
// 「没注入」就会把标记擦掉。换号重试靠 AccountID 校验挡，不靠清。
type openAIGatewayPoolSink struct {
	mu         sync.Mutex
	waitBudget *gatewayPoolWaitHolder
	applied    OpenAIGatewayPoolApplied
	// discarded 是本次请求里被 state-echo 判成降智、整发丢掉的那些上游尝试
	// （openai_gwpool_state_echo.go）。它们都是**真实发生过的**上游请求，要落可审计的用量行。
	discarded []OpenAIGatewayPoolDiscardedAttempt
	// model 是这一发的出站模型名，由 buildUpstreamRequest 在**压缩之前**从明文体里记一笔。
	//
	// 按实际出站模型选择垫话/补齐state（缓存按凭据×模型，非pair），而传输层只拿到
	// *http.Request —— 而且双开账号的出站体是 zstd，裸解 JSON 必然失败
	// （compressCodexRequestBody）。所以在还看得见明文的那一层记下来。
	model string
	// poolLive / poolFree 是池子最近一次清单的两个读数（见 OpenAIGatewayPoolApplied）。
	// 存在 sink 上而不是 applied 上：取清单发生在注入之前，放进 applied 会被 mark() 覆盖。
	poolLive int
	poolFree int
}

type openAIGatewayPoolSinkCtxKey struct{}

// openAIGatewayPoolSinkGinKey 是同一个 sink 在 gin 上下文里的键。
//
// 为什么要存两份：ctx 那份给出站挂钩点写（AttachRoute 手上没有 gin.Context）；gin 那份给
// **转发返回之后**的用量侧读 —— 丢弃行要在转发失败时也落（两发都判降智 ⇒ 整条请求失败 ⇒
// OpenAIForwardResult 恒为 nil，publish 到结果上的读数一条都到不了用量侧）。
const openAIGatewayPoolSinkGinKey = "openai_gwpool_sink"

// withOpenAIGatewayPoolSink 在转发入口挂一个 sink。**新增一条能打到 chatgpt.com 的转发入口时
// 要加这一行**，否则那条路上的用量行恒为「没覆写」（安全方向：宁可少报，不许假报）。
func withOpenAIGatewayPoolSink(ctx context.Context, c *gin.Context) (context.Context, *openAIGatewayPoolSink) {
	sink := &openAIGatewayPoolSink{waitBudget: &gatewayPoolWaitHolder{}}
	if c != nil {
		if value, exists := c.Get(gatewayPoolWaitGinKey); exists {
			if budget, ok := value.(*gatewayPoolWaitHolder); ok {
				sink.waitBudget = budget
			}
		} else {
			c.Set(gatewayPoolWaitGinKey, sink.waitBudget)
		}
		c.Set(openAIGatewayPoolSinkGinKey, sink)
	}
	return context.WithValue(ctx, openAIGatewayPoolSinkCtxKey{}, sink), sink
}

func openAIGatewayPoolSinkFrom(ctx context.Context) *openAIGatewayPoolSink {
	if ctx == nil {
		return nil
	}
	sink, _ := ctx.Value(openAIGatewayPoolSinkCtxKey{}).(*openAIGatewayPoolSink)
	return sink
}

// OpenAIGatewayPoolDiscardedAttempt 是一次「发出去了、响应头到手、被 state-echo 判成降智后整发
// 丢掉」的上游尝试。
//
// Applied 原样带着那一发的注入读数（网关名 + 票号 + 账号）：丢弃行唯一要回答的问题就是「哪个
// 落点被判降智了」，而用量侧的路由对卡片（routePairInUse）本来就是按这个结构体渲染的 ⇒ 直接复用，
// 不另加列、不另起一套。
//
// **刻意不带上游的 x-request-id。** 从前这里有个 RequestID 字段，注释说它是计费幂等键 ——
// 后来幂等键改成了用量侧自己定（固定前缀 + 客户端请求 id + attempt 序号，见
// openai_gateway_usage.go 那段论证），这个字段就再没人读过。而对账也不需要它：
// 丢弃行里的 Applied 已经带着网关名和票号，池子那边的日志正是按网关记的。
type OpenAIGatewayPoolDiscardedAttempt struct {
	Applied OpenAIGatewayPoolApplied
}

// noteDiscarded 记一次被判降智丢弃的上游尝试。
func (s *openAIGatewayPoolSink) noteDiscarded(attempt OpenAIGatewayPoolDiscardedAttempt) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.discarded = append(s.discarded, attempt)
}

// takeDiscardedOpenAIGatewayPoolAttempts 把这次请求的丢弃读数**取走**（读完清空）。
//
// 清空而不是只读：用量侧的钩子在成功与失败两条路上都会被调到，而每一条丢弃行只该落一次。
func takeDiscardedOpenAIGatewayPoolAttempts(c *gin.Context) []OpenAIGatewayPoolDiscardedAttempt {
	if c == nil {
		return nil
	}
	value, ok := c.Get(openAIGatewayPoolSinkGinKey)
	if !ok {
		return nil
	}
	sink, _ := value.(*openAIGatewayPoolSink)
	if sink == nil {
		return nil
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	taken := sink.discarded
	sink.discarded = nil
	return taken
}

// mark 由 AttachRoute 在**写完 Cookie 头之后**调：写在前面的话「写头失败」也会被记成已注入。
func (s *openAIGatewayPoolSink) mark(applied OpenAIGatewayPoolApplied) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = applied
}

// noteModel / modelOf 记取这一发的出站模型名（见字段注释）。空串 = 不知道。
func (s *openAIGatewayPoolSink) noteModel(model string) {
	if s == nil {
		return
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.model = model
}

func (s *openAIGatewayPoolSink) modelOf() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.model
}

// noteVerdict 把 state-echo 的结论补到当前标记上（gatewayPoolRouteDegraded 调）。
//
// 网关对不上就不记：换票重试时 mark 已经被下一张票覆盖，拿上一发的结论去改它会把结论
// 记到错的落点上。
func (s *openAIGatewayPoolSink) noteVerdict(gateway, verdict string) {
	if s == nil || gateway == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applied.Gateway != gateway {
		return
	}
	s.applied.Verdict = verdict
}

// snapshot 读回这一发的标记（出站挂钩点之后、同一个 ctx 上的调用方用）。
// notePoolCounts 记下池子这一发清单的两个读数。live<=0 整对丢掉：列表打不开时该保留上一次
// 问到的那一对，报 0 会让卡片说「池子一个落点都没有」。
//
// **成对写**：free 的含义依附于 live（「这 live 个里有 free 个没烧过」），拆开写会出现
// 新 free 配旧 live 的组合，而那个组合从来没有同时成立过。
func (s *openAIGatewayPoolSink) notePoolCounts(live, free int) {
	if s == nil || live <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poolLive, s.poolFree = live, free
}

// noteFullHeld 只补到同一账号、网关、票号的尝试。换票时 mark 必须清掉旧时长，
// 否则上一网关被判死的时长会跟着成功重试落到下一网关。
func (s *openAIGatewayPoolSink) noteFullHeld(attempt OpenAIGatewayPoolApplied, d time.Duration) {
	if s == nil || d <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applied.AccountID == attempt.AccountID && s.applied.Gateway == attempt.Gateway &&
		s.applied.Version == attempt.Version {
		s.applied.FullHeldMs = d.Milliseconds()
	}
}

func (s *openAIGatewayPoolSink) snapshot() OpenAIGatewayPoolApplied {
	if s == nil {
		return OpenAIGatewayPoolApplied{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 在读的时候合进来，不在 mark 里写：取清单在注入之前，让 mark 覆盖它就恒为 0。
	applied := s.applied
	applied.PoolLive, applied.PoolFree = s.poolLive, s.poolFree
	return applied
}

// publish 把标记落到转发结果上（result 为 nil = 这一发失败了，没有用量行要写）。
func (s *openAIGatewayPoolSink) publish(result *OpenAIForwardResult) {
	if s == nil || result == nil {
		return
	}
	result.GatewayPoolApplied = s.snapshot()
}

// AttachRoute 是出站挂 Cookie 的唯一入口：池子接管时 __cflb / __oailb 用池子那张，否则原样走
// 罐回放。三个出站挂钩点（HTTP 主咽喉 doOpenAIUpstream、WS 连接池 dialConn、WS 透传适配器）
// 都经这里，所以「钉死在坏网关」在三条路上一起修掉。
//
// cleanup 非 nil 仅属于独占新取票者，确证未发时执行本地精确版本清理；不访问池子、
// 不清 CD。共享取票、缓存复用及未接管均没有清理权。
func (s *openAICodexCookieStore) AttachRoute(
	ctx context.Context,
	account *Account,
	rawURL string,
	headers http.Header,
) (cleanup func(), err error) {
	if s == nil || headers == nil || !openAICodexCookiesApply(account) {
		return nil, nil
	}
	u := openAICodexCookieURL(rawURL)
	// 主机过滤：罐分支由 chatgptcookies 自己兜（IsChatGPTURL），接管分支绕开了罐就得自己兜。
	// 不兜的话 pair 会被发给 api.openai.com 这类第三方主机，而且**白烧一张池子 pair**（根本没
	// 碰到那个网关，读数却照样上报），供给只有个位数张。
	// 注意这一道在取票**之前**：所以「主机过滤否掉」这条路上永远无需清理新票。
	if u == nil || !chatgptcookies.IsChatGPTURL(u) {
		return nil, nil
	}
	if !s.gatewayPoolTakeover(account) {
		s.Attach(account, rawURL, headers)
		return nil, nil
	}
	// 兜底闸必须在取 pair **之前**：WS 预热（min_idle 默认 4，无业务请求也拨）绝不能消耗池子的
	// 槽位，而复用连接上的后续轮次也拿不到新窗口。正常情况下选路层已经把 WS 改成了 HTTP/SSE，
	// 走到这里说明有路径绕过了选路层。
	// 同样在取票之前 ⇒ WS 降级那条路上也无需清理新票。
	if isWebSocketURL(rawURL) {
		return nil, ErrGatewayPoolWSIncompatible
	}
	// 覆写范围**只有 Codex 推理面**，没有开关。侧信道（装饰性 GET）、/codex/alpha/search、
	// /codex/realtime/calls 这些也打在 chatgpt.com 上，但它们不是推理轮次——给它们取一张票
	// 等于白烧一个 (上游账号 × 网关) 单位，而供给是个位数张/小时。
	// 落回罐回放（而不是不挂 cookie）：这些调用本来就靠罐。
	if u.Path != openAIGatewayPoolInferencePath {
		s.Attach(account, rawURL, headers)
		return nil, nil
	}
	identity, err := s.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return nil, err
	}
	pair, fresh, err := s.gatewayPoolPair(ctx, account, identity)
	if err != nil {
		// 包括池子的九个拒绝码（都 Unwrap 到 gwpool.ErrNoSlot）。往上抛给既有失败路径，
		// 不回落罐回放。
		return nil, gatewayPoolClientError(err)
	}
	if fresh {
		cleanup = s.gatewayPoolDiscardUnsent(identity, pair)
	}
	outbound := pair.cookie
	// 只替换这两项：__cf_bm / cf_clearance / _cfuvid 是**本出口自己**拿到的 Cloudflare 令牌，
	// 丢掉会让 CF 重新发挑战（这和「跨出口回放 __cf_bm 自相矛盾」不是一回事——那说的是别人出口
	// 铸的值，这里是本出口自己的）。始终 Set：入站客户端的 Cookie 不能漏到出站。
	parts := make([]string, 0, 4)
	for _, item := range s.jarCookieParts(account, rawURL) {
		name, _, _ := strings.Cut(item, "=")
		if !slices.Contains(openAICodexRouteCookieNames[:], strings.TrimSpace(name)) {
			parts = append(parts, item)
		}
	}
	headers.Set("Cookie", strings.Join(append(parts, outbound), "; "))
	// 写完头**之后**才记标记：使用记录那张卡片据此断言「这一发真的注入了」。
	openAIGatewayPoolSinkFrom(ctx).mark(OpenAIGatewayPoolApplied{
		AccountID: account.ID, Cookie: pair.cookie, Gateway: pair.gateway,
		LedgerTag: gatewayPoolLedgerTag(identity),
		Region:    pair.region, Version: pair.version, RoundID: pair.roundID,
		DatacenterCountry: pair.datacenterCountry,
		cooldownResetAt:   s.gatewayPoolCooldownResetAt(identity),
	})
	return cleanup, nil
}

// isWebSocketURL 报告这是 WS 拨号地址。罐把 wss:// 归一成 https 后就看不出来了，所以按原始串判。
func isWebSocketURL(rawURL string) bool {
	switch scheme, _, _ := strings.Cut(strings.TrimSpace(rawURL), ":"); strings.ToLower(scheme) {
	case "ws", "wss":
		return true
	default:
		return false
	}
}
