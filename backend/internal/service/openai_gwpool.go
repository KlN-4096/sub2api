package service

// 网关池（gwpool）对接 —— 两个钩子，全部在这个文件里。
//
// 背景（docs/tasks/gateway-pool.md、docs/conventions/codex-full-strength-tickets.md）：降智的
// 作用单位是 **(账号 × 网关)**。路由 pair（__cflb + __oailb）**只决定路由、不携带健康度**，
// 而且可以跨账号使用；某账号去一个它从没碰过的网关，会拿到约 183 秒的满血窗口。池子负责发现
// 网关、维护活 pair 并下发，这里只消费。
//
//   - 出：构造上游请求时向池子要一张 pair，写进出站 Cookie 头的 __cflb / __oailb 两项，**顶掉**
//     klno.5（2026-09-25）那套按账号罐回放。那套回放的已知毛病正是把账号钉死在一个网关上——
//     罐里存着上游上次下发的 __oailb，下一发又把它带回去，于是 pro1 被钉在 unified-126、
//     pro3 被钉在 unified-121。
//   - 挑：要票前先 GET /gateways，在池子说「有活 pair、你没烧过」的网关里再滤掉
//     本地账本里近 4 小时碰过的，点名取票（/cookie?gateway=）。多那一道本地账本是因为池子
//     按它发的 consumer key 记账，而同一份 Codex 凭据可能挂在多个 sub2api 账号行上，按行记会
//     让两边都以为自己还有满血窗口。**列不出来 / 挑不出来一律退回裸取**（池子自己挑），
//     绝不因此让这一发失败。两个端点都带 ?account=<上游 account_id>：一把 consumer key 能替
//     多个上游账号取票，不报的话池子把槽位记在上传者头上（跨账号取到的票 verified_full 恒为
//     false——池子没有那个账号的凭据、验不了，这不是丢票的理由）。
//   - 回：**不回报满血/降智判定**。池子在交付那一刻就记了 LastTouch 和 LastVerdict，verdict 还是
//     它自己用 state-echo 验出来的；而转发路径上一个可用判据都不剩（见 openAIGatewayPoolExtraKey
//     附近的说明），回报只能填 "unknown"，等于把池子刚验出来的 "full" 覆盖掉，让刚验过满血的
//     槽位提前被拿去烧。「这张票坏了」由取 pair 时的 force=1 + exclude_versions 承载。
//   - 还：取到票但**一个字节都没发出去**时 POST /release 把槽位还回去（gatewayPoolRelease）。
//     这不是回报——它不含任何健康度判定，只说「我没用它」，而判断「发没发出去」的确证在转发
//     路径上（gatewayPoolReleasesUnsent）。尽力而为：失败只记日志。
//   - 停：池子的拒绝码分流（gatewayPoolBackoff）。all_cooling / rate_limited / no_exit /
//     upstream_rejected 按**身份**退避 retry_after_seconds，退避期内一个池子请求都不发——
//     一视同仁的话客户端那种「失败就重发」的环每轮都会在池子侧触发一次发现铸票。
//
// 配置**全在账号 extra 上**：池子发的 consumer key 是按账号发的，放实例级等于一个 sub2api 实例
// 里所有账号共用同一个池子身份。开关关着（或缺地址）时整条链路与接入前逐字节一致（走原来的
// Attach）。
//
// 池子没有满血槽位时回 503 ⇒ 这里把错误原样抛给调用方走既有失败路径，**绝不退回 cookie 回放**
// （用户原则：宁可 503 也不放降智）。
//
// **WS 上游自动降级成 HTTP/SSE**：WS 连接复用 60 分钟（openAIWSConnMaxAge），而满血窗口只有约
// 150 秒，pair 只在握手挂一次 ⇒ 复用的连接会一直压在同一个已经烧完的网关上，而且预热
// （min_idle 默认 4）会在无业务请求时就拨连接。对齐连接寿命与请求级窗口的代价远大于收益，
// 所以这个账号本该走 WS 的请求在**选路那一层**改走 HTTP/SSE
// （resolveOpenAIWSDecisionByGatewayPool + WS 入站的 forceHTTPBridge），而不是让请求失败。
// ErrGatewayPoolWSIncompatible 退化成兜底：真有路径绕过选路层拨了 WS，仍然在取 pair 之前拒掉。

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
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
	// openAIGatewayPoolFetchTimeout 兜住一次取 pair。不跟随业务 ctx 的取消（见 gatewayPoolPair）。
	//
	// 这个数要盖住**池子那边一次交付的全部工作**，而不只是网络往返：池子开着
	// verify_full_at_deliver 时，每试一个网关都要用交付对象的凭据经住宅代理打一发真实
	// state-echo（池子给每发上游 60s），而 max_tries_per_deliver 默认 2 ⇒ 一次 /cookie
	// 合法地要跑十秒量级。原来的 8s 盖不住，于是：
	//
	//   2026-10-02 线上 caddy 访问日志，458 次 /cookie —— **301 次（66%）是 status 0**
	//   （响应还没写出来，客户端先断了），它们的 p90 = 7.97s，正好钉在这条 8s 线上；
	//   而真正返回 200 的那 47 次最长 11.28s。
	//
	// 代价不只是失败：被掐断的那一发 state-echo **已经打到上游了**，(账号 × 网关) 照烧、
	// 每区域每小时 6 发的预算照扣（state-echo 与铸票共用，见池子 mint.go 的 VerifyFull），
	// 一张票都没交付。池子侧看到的是 "请求被调用方取消"，报给客户端的却是 no_live_pair，
	// 于是现场读起来像「池子没票」，其实票就在它手上。
	//
	// 25s = 实测上限 11.28s 的两倍余量，仍在池子 wait 契约上限 30s 之内。业务请求确实会多
	// 挂一会儿再失败，但换来的是那 66% 从「挂 8s 然后 502」变成「挂 5s 然后拿到票」。
	// 嫌长的账号用 openai_gwpool_fetch_timeout_s 单独调。
	openAIGatewayPoolFetchTimeout = 25 * time.Second
	// openAIGatewayPoolListTimeout 兜住那次「列网关」。它是**优化**，绝不能吃掉取票的预算：
	// 列不出来就退回池子自己挑，所以给一个远小于 FetchTimeout 的额度。
	openAIGatewayPoolListTimeout = 2 * time.Second
	// openAIGatewayPoolSteeringExtraKey 决定要不要自己挑落点（见 gatewayPoolPick）。
	// **缺省即开**，与接这个键之前的写死行为一致；只有显式写 false 才退回「池子自己挑」。
	openAIGatewayPoolSteeringExtraKey = "openai_gwpool_steering"
	// openAIGatewayPoolGatewayWindowExtraKey 是本地账本的保留窗口（秒）。缺省 / 非正数走默认值。
	//
	// 没进 openAIGatewayPoolConfigExtraKeys：它没有跨字段约束，写什么都不会让账号配到一个
	// 必然失败的状态（最坏只是挑落点变松或变严）。同理另外三个旋钮也不进。
	openAIGatewayPoolGatewayWindowExtraKey = "openai_gwpool_gateway_window_s"
	// openAIGatewayPoolMinRemaining 是能接受的 pair 最低剩余寿命（池子的 min_remaining）。
	//
	// 定这个值看的**不是**「够这一发用」：路由只在**建连那一刻**生效，连上之后这一轮跑多久都不
	// 换路。看的是缓存语义——一张票按身份缓存、窗口内所有请求共用，到点才换 ⇒ 剩余寿命就是
	// 「这张票还能服务多少发请求」。接一张只剩 5 秒的票等于下一发立刻再取一张，而供给是个位数
	// 张/小时（docs/tasks/gateway-pool.md 的容量模型），这种放大是这个功能最不能出的毛病。
	// 60s 的三条边界：① 够一轮推理里那几发连续建连（侧信道 + 主请求在秒级内完成）；
	// ② 远小于实测满血窗口 170–200s，不会把池子大半库存筛掉；③ 在池子钳位区间 [30,3600] 内。
	openAIGatewayPoolMinRemaining = 60 * time.Second
	// openAIGatewayPoolWait 是取票时愿意等池子现铸多久（池子的 wait，契约上限 30s）。
	//
	// 直接报上限、由 gwpool 自己钳到本次调用的剩余预算（它那边有 ctx 死线，算得比这里准）：
	// 等不到就是这一发失败，所以「能等多久就等多久」永远不比「等得更少」差。
	// 不做成账号旋钮：真正要调的是取票超时（openai_gwpool_fetch_timeout_s），wait 跟着它走。
	openAIGatewayPoolWait = 30 * time.Second
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
	// openAIGatewayPoolGatewayWindow 是默认窗口。4 小时的出处：一个 (上游账号 × 网关) 单位烧掉
	// 之后的再生周期，docs/conventions/codex-full-strength-tickets.md 的「保守估算」——
	// **那篇文档明说这个数至今没测准**（静置 30 分钟到 4 小时，满血率恒在 3/11，与时长无关），
	// 所以它是个工程上的保守取值，不是实测结论。真测准了就该改这里。
	openAIGatewayPoolGatewayWindow = 4 * time.Hour
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

// gatewayPoolGatewayWindow 读本地账本的保留窗口。配不对（0 / 负数 / 非数字）就是默认 4 小时：
// 这个值只影响挑网关的严格程度，配坏了不该让账号取不到票。
func (a *Account) gatewayPoolGatewayWindow() time.Duration {
	return a.gatewayPoolSeconds(openAIGatewayPoolGatewayWindowExtraKey, openAIGatewayPoolGatewayWindow)
}

// gatewayPoolFetchTimeout / gatewayPoolListTimeout 同理：留空就是写死那会儿的 8s / 2s。
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

// gatewayPoolSteering 报告要不要自己挑落点。缺省 / 非 bool = 开（接这个键之前的写死行为），
// 只有显式 false 才关 ⇒ getExtraBool 在这里用不了，它把「没配」和「配了 false」读成同一个值。
func (a *Account) gatewayPoolSteering() bool {
	if a == nil || a.Extra == nil {
		return true
	}
	enabled, ok := a.Extra[openAIGatewayPoolSteeringExtraKey].(bool)
	return !ok || enabled
}

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
	openAIGatewayPoolBaseURLExtraKey,
	OpenAIGatewayPoolConsumerKeyExtraKey,
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
	// 档位也在写入时拦：gatewayPoolGuard 对认不出的值回默认档（**不能**在转发时 fail closed，
	// 那会让一个手滑的值停掉整个账号），所以「保存成功了但配的那一档没生效」是这一项唯一的
	// 失败形态 —— 而 extra 是 JSONB、管理 API 与批量导入直收，`"Queue"` / `"OFF"` / `1` 都进得来。
	// 同型先例：ValidateOpenAITurnStateHunterExtra 的「保存成功但什么都不做是最难排查的失败」。
	if raw, present := extra[openAIGatewayPoolGuardExtraKey]; present {
		text, isText := raw.(string)
		switch mode := gatewayPoolGuardMode(strings.TrimSpace(text)); {
		case !isText,
			mode != gatewayPoolGuardOff && mode != gatewayPoolGuardCut &&
				mode != gatewayPoolGuardRetry && mode != gatewayPoolGuardQueue:
			return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_GUARD_INVALID",
				"account %d: %s must be one of off/cut/retry/queue, got %v",
				account.ID, openAIGatewayPoolGuardExtraKey, raw)
		}
	}
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
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errOpenAIGatewayPoolRouteDegraded):
		return gatewayPoolDegradedClientMsg
	case errors.Is(err, errOpenAIGatewayPoolWarmNoModel):
		return gatewayPoolWarmNoModelClientMsg
	case errors.Is(err, errOpenAIGatewayPoolWarmExhausted):
		return gatewayPoolWarmExhaustedClientMsg
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
		"reason", "a WebSocket connection is reused for up to 60 minutes while a route pair's "+
			"full-strength window lasts ~150s, and prewarm dials with no request to serve")
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

// openAIGatewayPoolPair 是缓存住的一张 pair。until 是**满血窗口**的到点（池子的 valid_for_s），
// 不是 cookie 的过期时刻：窗口内复用同一张，过了再要一张。
// version 是池子给这**一张具体的票**的身份（cookie_version）：还票（/release）与
// 「这张不行、别再给我」（exclude_versions）都靠它，落库后还能和池子的日志对上账。
// 可以为空（池子没报 / 报了畸形值）—— 那只是少了这几个能力，不是丢票的理由。
//
// 必须保持可比较（sync.Map 的 CompareAndDelete 要用，见 gatewayPoolRelease）。
type openAIGatewayPoolPair struct {
	cookie  string
	gateway string
	// region 是池子报的「铸这张票的出口属于哪个大区」。只用于账号卡片上按大区归档落点，
	// 一个判定都不接（口径见 gwpool.Pair.Region）。空 = 池子没报。
	region  string
	version string
	until   time.Time
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
	return openAIGatewayPoolAccountKey(source), nil
}

// openAIGatewayPoolAccountKey 是池子侧的账号标识，缺凭证域身份时退回本地行
// （与 turn-state 溯源的 id:N 兜底同口径）。
func openAIGatewayPoolAccountKey(account *Account) string {
	if namespace := codexAccountIdentityNamespace(account); namespace != "" {
		return namespace
	}
	if account == nil {
		return ""
	}
	return "id:" + strconv.FormatInt(account.ID, 10)
}

// gatewayPoolUpstreamAccountID 从凭证域身份里取出**上游** account_id，即池子的 ?account=。
//
// 为什么必须报给池子：一把 consumer key 可以替多个上游账号取票，而满血窗口是
// (上游账号 × 网关) 的 ⇒ 不报的话池子把槽位记在上传者头上，它的 used_by_you 讲的是别人的历史。
//
// 为什么从身份串里解而不是直接读 account.GetChatGPTAccountID()：身份串已经过了影子行 → 母账号
// 的解析（影子行自己不持凭据，直接读会拿到空串），这里不想把那套 repo 解析再走一遍。
//
// **这是一处刻意接受的耦合**：串的格式归 codexAccountIdentityNamespace 所有，形如
// chatgpt:<account_id>[:user:<user_id>]。那边改了格式，这里的前缀就对不上 ⇒ 返回空串 ⇒
// 不带 ?account= ⇒ 池子按 consumer key 的上传者记，也就是接这个参数之前的行为。
// 换句话说**解析失败是静默降级而不是报错**：少一点精度，不会让任何一发请求失败。
// 其余形态（seed: / setup-token: / id:<行> 兜底）本来就没有上游 account_id，走的是同一条降级路。
func gatewayPoolUpstreamAccountID(identity string) string {
	rest, ok := strings.CutPrefix(identity, "chatgpt:")
	if !ok {
		return ""
	}
	accountID, _, _ := strings.Cut(rest, ":")
	return accountID
}

// gatewayPoolTakeover 报告这一发该由池子出 cookie。
func (s *openAICodexCookieStore) gatewayPoolTakeover(account *Account) bool {
	return s != nil && account.UsesGatewayPool()
}

// gatewayPoolIdentity 取缓存键 / 回报用的凭证域身份。没注入解析器（裸结构体单测）时退回按本地行算。
func (s *openAICodexCookieStore) gatewayPoolIdentity(ctx context.Context, account *Account) (string, error) {
	if s.identity != nil {
		return s.identity(ctx, account)
	}
	return openAIGatewayPoolAccountKey(account), nil
}

// gatewayPoolLedgerIdentity 把凭证域身份收敛到**上游账号**粒度：去掉 :user:<user_id> 那一段。
//
// 降智的作用单位是 (上游账号 × 网关)。同一个 chatgpt_account_id 下的两份凭据（两个
// chatgpt_user_id，例如工作区里的两个人）对 OpenAI 就是同一个账号，烧的是同一个窗口——
// 分开记账会让第二个 user 以为自己还有满血落点。池子的 used_by_you 正好也按 account_id 算、
// 能兜住这一条，但那是撞巧，账本自己就该对。
//
// 其余形态（seed: / setup-token: / id:<行> 兜底）本来就没有上游 account_id，原样当键：
// 它们只能按自己那份凭据算，收不动。
func gatewayPoolLedgerIdentity(identity string) string {
	if accountID := gatewayPoolUpstreamAccountID(identity); accountID != "" {
		return "chatgpt:" + accountID
	}
	return identity
}

// gatewayPoolLedgerKey 是本地账本的键：上游账号 + 网关名。
//
// 键**绝不能**是本地账号行 ID：同一份 Codex 凭据可能挂在多个 sub2api 账号行上（克隆行、
// 影子行），按行记会让每一行都以为自己还有满血窗口，其实烧的是同一个 (上游账号 × 网关) 单位。
// \x00 当分隔符——身份与网关名都不可能含 NUL，拼不出歧义键。
func gatewayPoolLedgerKey(identity, gateway string) string {
	return gatewayPoolLedgerIdentity(identity) + "\x00" + gateway
}

// gatewayPoolUsedRecently 查本地账本：这个上游账号在 window 内拿到过这个网关的票没有。
//
// 为什么不能只信池子的 used_by_you：池子按它发的 consumer key 认账号，而同一份 Codex 凭据
// 可能挂在多个账号行、各自配着不同的 key；池子那本账对不上真正被烧掉的那个单位。
func (s *openAICodexCookieStore) gatewayPoolUsedRecently(identity, gateway string, window time.Duration) bool {
	value, ok := s.poolUsed.Load(gatewayPoolLedgerKey(identity, gateway))
	if !ok {
		return false
	}
	at, ok := value.(time.Time)
	return ok && time.Since(at) < window
}

// gatewayPoolMarkUsed 记一笔「这个上游账号碰过这个网关」。
//
// 不需要清理：条目只会被同一个键原地覆盖，键空间是 (上游账号 × 网关) 的笛卡尔积（个位数账号 ×
// 两百来个网关），而判定本来就是按时间算的，过期条目不会影响结论。
func (s *openAICodexCookieStore) gatewayPoolMarkUsed(identity, gateway string) {
	if identity == "" || gateway == "" {
		return
	}
	s.poolUsed.Store(gatewayPoolLedgerKey(identity, gateway), time.Now())
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
	prefix := gatewayPoolLedgerIdentity(identity) + "\x00"
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
		if !ok || time.Since(at) >= window {
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
	value, ok := s.poolBackoff.Load(gatewayPoolLedgerIdentity(identity))
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
		// 别的托管账号铸出的票、别的区域的票、我们自己 /pair/renew 续活的票，在它说的那
		// 「一小时」里全都是可交付的。采信那个 retry_after 就是拿**池子内部某个号的配额**
		// 去停掉**我们所有业务请求**，最长静默一小时，期间每一发聊天直接 502。
		// 这和 all_cooling 那次（现场静默 3h47m，TestGatewayPoolCapsAllCoolingBackoff）
		// 是同一个错，只是天花板换成了 1 小时。
		//
		// 不退避也不会打出风暴：这个闸是池子进程内的滑动窗口，满了**一发上游都不发**，
		// 拒绝是即时的，代价只有一次 HTTP 往返。相反，退避期里池子刚补上的票我们拿不到。
		return 0, refused.Code
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

// gatewayPoolRetriesBare 报告「点名的那个没取到」之后值不值得再裸取一次。
// no_exit 不值得：池子的出口全熔断，换哪个网关都是同一个错。退避中的码同理（见 gatewayPoolBackoff）。
func gatewayPoolRetriesBare(err error) bool {
	backoff, _ := gatewayPoolBackoff(err)
	return backoff == 0 && errors.Is(err, gwpool.ErrNoSlot)
}

// gatewayPoolPick 挑一个这个身份近期没碰过的网关，返回空串 = 挑不出来，退回裸取（池子自己挑）。
//
// 池子不知道「烧过」是按 (上游账号 × 网关) 算的——它只看自己那本账，所以池子的
// pair_ready / used_by_you 与本地账本是**且**的关系。
func (s *openAICodexCookieStore) gatewayPoolPick(
	ctx context.Context,
	pool *gwpool.Client,
	account *Account,
	identity string,
) string {
	if !account.gatewayPoolSteering() {
		return "" // 账号明确要求「由池子按调度选」。
	}
	listCtx, cancel := context.WithTimeout(ctx, account.gatewayPoolListTimeout())
	defer cancel()
	gateways, err := pool.Gateways(listCtx, gatewayPoolUpstreamAccountID(identity))
	if err != nil {
		// 列表是优化不是闸门：池子没加这个端点 / 临时打不开时照常裸取。绝不能因为列不出来
		// 就让这一发失败——接这个端点之前的行为就是兜底。
		slog.Debug("gwpool_gateways_unavailable", "account_id", account.ID, "error", err)
		return ""
	}
	var pick string
	var pickedAt time.Time
	window := account.gatewayPoolGatewayWindow()
	for _, gateway := range gateways {
		if !gateway.PairReady || gateway.UsedByYou {
			continue
		}
		if s.gatewayPoolUsedRecently(identity, gateway.Name, window) {
			continue
		}
		// 多个候选时挑**最久没碰**的。零值（池子说没碰过）早于任何时刻，天然最优。
		if pick == "" || gateway.LastUsedAt.Before(pickedAt) {
			pick, pickedAt = gateway.Name, gateway.LastUsedAt
		}
	}
	return pick
}

// gatewayPoolPair 取该身份当前可用的 pair：窗口内复用缓存，否则向池子要一张。
//
// 同一身份的并发请求用 singleflight 收口成一次 /cookie：池子一个网关一周期只出一张 pair，
// 并发各要一张就是白烧供给。共享的那次取用自己的 ctx（WithoutCancel + 独立超时）——否则第一名
// 的客户端一断开，排在它后面的同账号请求会被连坐成 502。
//
// fresh 报告这张票是不是**这一发自己取回来的**（缓存复用时为 false）：只有自己取的那张才可能
// 「取了票但一个字节都没发出去」，也只有它能还（见 gatewayPoolRelease）。
func (s *openAICodexCookieStore) gatewayPoolPair(ctx context.Context, account *Account, identity string) (pair openAIGatewayPoolPair, fresh bool, err error) {
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
		return openAIGatewayPoolPair{}, false, fmt.Errorf(
			"%w: pool asked this account to back off for another %ds",
			&gwpool.PoolError{Code: code}, int(remaining.Seconds()))
	}
	pool, err := s.poolClient(account)
	if err != nil {
		return openAIGatewayPoolPair{}, false, err
	}
	// 租着的那张过了建议窗口 ⇒ 要一张**不同的**网关（force=1）。池子的 valid_for_s 只是建议值
	// （ttl_is_advisory），换不换由这边判；不带 force 的话池子可能把同一张再发回来。
	force := state == openAIGatewayPoolPairStale
	fetchCtx := context.WithoutCancel(ctx)
	// took 只在**这一发自己真的从池子取回一张**之后置真。不能只看 singleflight 的 shared：
	// 领头者的闭包内层也会复查缓存（别人刚取完、键已删的那一刻），命中就直接返回别人那张，
	// 而此时没有跟随者 ⇒ shared == false ⇒ 会被当成「我取的」而拿到还票闭包，于是把**别人正在
	// 用的**那张还给池子并从缓存删掉。每个调用方有自己的 took，跟随者的闭包不在自己的 goroutine
	// 里跑 ⇒ 恒 false，没有竞态。
	took := false
	fetched, err, shared := s.poolFetch.Do(identity, func() (any, error) {
		// 排在后面的请求醒来时第一名可能已经取到了。
		if pair, cached := s.cachedPoolPair(identity); cached == openAIGatewayPoolPairLive {
			return pair, nil
		}
		callCtx, cancel := context.WithTimeout(fetchCtx, account.gatewayPoolFetchTimeout())
		defer cancel()
		// 自己挑落点：池子按它发的 consumer key 记账，认不出「同一份凭据挂在多个账号行上」，
		// 所以这里按凭证域身份的本地账本再滤一道。挑不出来时 Gateway 为空 = 由池子按调度选
		// （接 /gateways 之前的行为，永远是兜底）。
		// 列表与取票同在 singleflight 里 ⇒ 同身份并发只列一次，不另加一层缓存。
		steer := s.gatewayPoolPick(callCtx, pool, account, identity)
		request := gwpool.CookieRequest{
			// Account = 这一发真正要用的那个上游账号（池子的 ?account=）：池子按它记槽位，
			// 不报就记在 consumer key 上传者的头上。
			Account: gatewayPoolUpstreamAccountID(identity),
			Gateway: steer,
			Force:   force,
			// 本地账本里还在窗口内的网关：点名时它是多余的（挑的时候已经滤过），
			// **裸取时它是唯一能把这份知识用上的地方**。
			Exclude:      s.gatewayPoolBurnedGateways(identity, account.gatewayPoolGatewayWindow(), gwpool.MaxExcludeItems),
			MinRemaining: openAIGatewayPoolMinRemaining,
			Wait:         openAIGatewayPoolWait,
		}
		// 手里那张转成 stale = 「这张具体的票不行了」。exclude_versions 正是这句话的精确表达，
		// 比 force（只说「换个网关」）准；两者一起带（force 仍然要，池子据它保证换网关）。
		if force && cached.version != "" {
			request.ExcludeVersions = []string{cached.version}
		}
		got, err := pool.Cookie(callCtx, request)
		if steer != "" && gatewayPoolRetriesBare(err) {
			// 点名的那个在「列表」与「取票」之间被别人租走了。这一发什么都没交付、没烧任何
			// 槽位，所以退回裸取一次——不然自己挑网关反而把本来能成的请求打成失败。
			//
			// **只退一次，绝不能改成循环重试**：裸取的失败意味着池子现在真的一张都没有，
			// 重试只会在供给见底时把每个业务请求放大成一串池子请求（风暴）。
			// 「点名失败就换一个候选再点」同理不做：候选都是同一张列表来的，它过期了就全过期。
			// 该退避的错误码（no_exit 等）走不到这里：换网关只是把同一个错重复一遍。
			request.Gateway = ""
			got, err = pool.Cookie(callCtx, request)
		}
		if err != nil {
			// 池子明说了「先别来」就按身份记下来：只对本次请求生效的退避挡不住重试环，
			// 而每一轮重试都会在池子侧触发一次发现铸票。
			if backoff, code := gatewayPoolBackoff(err); backoff > 0 {
				s.poolBackoff.Store(gatewayPoolLedgerIdentity(identity),
					gatewayPoolBackoffEntry{Until: time.Now().Add(backoff), Code: code})
				slog.Warn("gwpool_backoff_started", "account_id", account.ID,
					"code", code, "backoff_s", int(backoff.Seconds()))
			}
			// 刻意**不删**缓存里那张过期的：它是「别再给我这一个」的依据，删掉之后下一发会走
			// 不带 force / 不带 exclude_versions 的 /cookie，池子可能原样把烧过的那张发回来。
			return nil, err
		}
		// 取到票了 ⇒ 之前那次退避（如果有）已经过去，清掉，不留一个过期的到点值。
		s.poolBackoff.Delete(gatewayPoolLedgerIdentity(identity))
		// 池子是外部服务 = 信任边界：只留 __cflb / __oailb，别的名字不往 chatgpt.com 发，
		// 也保证落库读数（同样是 routePairOf 的产物）与实际出站一致。
		cookie := routePairOf(strings.Split(got.Cookie, ";"))
		if cookie == "" {
			return nil, fmt.Errorf("%w: cookie response carried no route pair", gwpool.ErrPool)
		}
		gateway := strings.TrimSpace(got.Gateway)
		if gateway == "" {
			// 池子没报网关名就自己从 __oailb 里解——回报必须记**实际注入的那张**。
			gateway = openAICodexRouteGateway(cookie)
		}
		pair := openAIGatewayPoolPair{
			cookie:  cookie,
			gateway: gateway,
			region:  strings.TrimSpace(got.Region),
			version: got.Version,
			until:   time.Now().Add(got.ValidFor),
		}
		took = true
		s.poolPairs.Store(identity, pair)
		// 记账用**实际拿到的**那个网关名，而不是我们点名的那个：裸取和池子忽略点名时都只能
		// 从响应里知道落点。
		s.gatewayPoolMarkUsed(identity, pair.gateway)
		// 刻意不打 version：它是这张票的身份，和 cookie 本体一样不进日志。
		slog.Info("gwpool_pair_taken", "account_id", account.ID, "gateway", pair.gateway,
			"steered_to", steer, "excluded", len(request.Exclude), "valid_for_s", int(got.ValidFor.Seconds()),
			"verified_full", got.VerifiedFull, "ttl_is_advisory", got.TTLIsAdvisory, "forced", force)
		// 猎手 pair 模式把票连带的 pair 种回罐里（openai_turn_state_pair.go），而接管后罐里的
		// __cflb/__oailb 不再出站 ⇒ 它会被静默忽略。只在换 pair 这一刻 warn 一次，不改行为。
		if account.IsOpenAITurnStatePairModeEnabled() {
			slog.Warn("gwpool_overrides_turn_state_pair_mode", "account_id", account.ID,
				"gateway", pair.gateway)
		}
		return pair, nil
	})
	if err != nil {
		return openAIGatewayPoolPair{}, false, err
	}
	taken, _ := fetched.(openAIGatewayPoolPair)
	// 两个条件都要：shared = 这次结果是别人那一趟取回来的（singleflight 合并）；
	// took = 闭包真的向池子取了一张（而不是在内层复查时命中了缓存）。
	return taken, !shared && took, nil
}

// gatewayPoolRelease 还票：这一发取了票但**一个字节都没发出去**，把槽位还给池子
// （POST /release，池子只在交付后 DeliverTTL 内受理）。
//
// 返回一个闭包而不是直接调用：知道「发没发出去」的是转发路径，不是这里。
// 尽力而为——失败只记日志，绝不影响主流程，所以不返回 error。
//
// 还票同时**把缓存里那张删掉**：槽位都还了还继续拿它出站等于对池子说谎。只在缓存里仍然是
// 这一张时删（CompareAndDelete），期间被别人换过就不动。
//
// 还票**不动本地账本**（poolUsed 里那条「碰过这个网关」留着）：刻意的。万一这次「还」判错了
// （见下面的竞态），把账本一起撤掉会让这个身份立刻再去点同一个网关，拿到的是已经烧掉的窗口 ⇒
// 降智，而这正是整个功能要防的东西。代价是那个落点我们自己 4 小时内不再用（池子可以给别人），
// 用一点供给换「错判绝不落到降智上」。
//
// ponytail: 残留竞态 —— 取到票之后、这一发失败之前的那几微秒里，同身份的另一发请求可能已经读走
// 了缓存里这张并真的发了出去，那时这次「还」是在说谎。要堵死得给每张票记发放计数（pair 改成
// 指针 + 原子计数）。没做的理由只有两条：窗口是微秒级，而且要「另一发恰好读走」与「这一发恰好
// 死在发送前」同时发生；而不还的代价是**每次**「取了票没发出去」（ctx 取消、主机校验否掉这些
// 日常事件）都永久浪费一个槽位，供给只有个位数张/小时。真观察到池子抱怨再上计数。
// （原先这里还写着「池子交付前自己会验满血，错还最多让它多验一次」—— 那个前提没了，
// verify_full_at_deliver 现在默认关，别再按它推理。）
func (s *openAICodexCookieStore) gatewayPoolRelease(
	account *Account,
	identity string,
	pair openAIGatewayPoolPair,
) func() {
	if pair.version == "" {
		return nil // 池子没报这张票的身份 ⇒ 还不了。
	}
	return func() {
		pool, err := s.poolClient(account)
		if err != nil {
			return
		}
		s.gatewayPoolDropPair(identity, pair.version)
		// 自带 ctx：这个闭包正是在「业务 ctx 已经死了」的路径上被调的。
		ctx, cancel := context.WithTimeout(context.Background(), account.gatewayPoolFetchTimeout())
		defer cancel()
		if err := pool.Release(ctx, pair.version); err != nil {
			slog.Debug("gwpool_release_failed", "account_id", account.ID, "gateway", pair.gateway, "error", err)
			return
		}
		slog.Info("gwpool_pair_released", "account_id", account.ID, "gateway", pair.gateway)
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
	if !time.Now().Before(pair.until) {
		return pair, openAIGatewayPoolPairStale
	}
	return pair, openAIGatewayPoolPairLive
}

// gatewayPoolDropPair 按**票号**删掉缓存里那张票。
//
// 不按整个结构体比（CompareAndDelete(identity, 手上那份副本)）：取了票之后缓存里那张还会被原地
// 改（判了降智会标 Stale），拿旧副本去比会对不上 ⇒ 该删的没删（还票时票已经还了、本地却还
// 拿着它继续出站）。票号唯一标识一张票，对不上说明缓存里已经是另一张了 —— 那张不该删。
func (s *openAICodexCookieStore) gatewayPoolDropPair(identity, version string) {
	value, ok := s.poolPairs.Load(identity)
	if !ok {
		return
	}
	if cached, isPair := value.(openAIGatewayPoolPair); isPair && cached.version == version {
		s.poolPairs.CompareAndDelete(identity, cached)
	}
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
	AccountID int64
	Cookie    string
	Gateway   string
	// Region 是池子说的「这张票是哪个大区铸的」，给账号卡片按大区归档落点用。空 = 不知道。
	Region  string
	Version string
	// Verdict 是这一发的 state-echo 读数："" = 没判（判据关着、没送票、非 200）、
	// openAIGatewayVerdictFull、openAIGatewayVerdictDegraded。
	//
	// 挂在这个快照上而不是 sink 上另存一份：换票重试那一圈里前一个落点判降智、后一个判满血，
	// 而丢弃行各自快照走自己那一份 Applied ⇒ 两条用量行读到各自的结论。
	Verdict string
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
	mu      sync.Mutex
	applied OpenAIGatewayPoolApplied
	// discarded 是本次请求里被 state-echo 判成降智、整发丢掉的那些上游尝试
	// （openai_gwpool_state_echo.go）。它们都是**真实发生过的**上游请求，要落可审计的用量行。
	discarded []OpenAIGatewayPoolDiscardedAttempt
	// model 是这一发的出站模型名，由 buildUpstreamRequest 在**压缩之前**从明文体里记一笔。
	//
	// queue 档的垫话必须用同一个模型（state 绑在 (账号 × 模型 × 这张票) 上），而传输层只拿到
	// *http.Request —— 而且双开账号的出站体是 zstd，裸解 JSON 必然失败
	// （compressCodexRequestBody）。所以在还看得见明文的那一层记下来。
	model string
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
	sink := &openAIGatewayPoolSink{}
	if c != nil {
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
	// Retried：丢掉之后换票重发了（degraded_retries=1）。false = 这一发就是客户端最终拿到的那个
	// 错误的来源（只截断，或重试那一发又判降智）。
	Retried bool
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

// snapshot 读回这一发的标记（出站挂钩点之后、同一个 ctx 上的调用方用，见 gatewayPoolRenew）。
func (s *openAIGatewayPoolSink) snapshot() OpenAIGatewayPoolApplied {
	if s == nil {
		return OpenAIGatewayPoolApplied{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

// publish 把标记落到转发结果上（result 为 nil = 这一发失败了，没有用量行要写）。
func (s *openAIGatewayPoolSink) publish(result *OpenAIForwardResult) {
	if s == nil || result == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result.GatewayPoolApplied = s.applied
}

// AttachRoute 是出站挂 Cookie 的唯一入口：池子接管时 __cflb / __oailb 用池子那张，否则原样走
// 罐回放。三个出站挂钩点（HTTP 主咽喉 doOpenAIUpstream、WS 连接池 dialConn、WS 透传适配器）
// 都经这里，所以「钉死在坏网关」在三条路上一起修掉。
//
// release 非 nil 时表示「这一发**自己**向池子取了一张新票」：调用方在确知这一发一个字节都没发
// 出去的路径上调它把槽位还回去（尽力而为，见 gatewayPoolRelease）。缓存复用、罐回放、以及所有
// 不接管的情况都是 nil。
func (s *openAICodexCookieStore) AttachRoute(
	ctx context.Context,
	account *Account,
	rawURL string,
	headers http.Header,
) (release func(), err error) {
	if s == nil || headers == nil || !openAICodexCookiesApply(account) {
		return nil, nil
	}
	u := openAICodexCookieURL(rawURL)
	// 主机过滤：罐分支由 chatgptcookies 自己兜（IsChatGPTURL），接管分支绕开了罐就得自己兜。
	// 不兜的话 pair 会被发给 api.openai.com 这类第三方主机，而且**白烧一张池子 pair**（根本没
	// 碰到那个网关，读数却照样上报），供给只有个位数张。
	// 注意这一道在取票**之前**：所以「主机过滤否掉」这条路上永远没有票要还。
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
	// 同样在取票之前 ⇒ WS 降级那条路上也没有票要还。
	if isWebSocketURL(rawURL) {
		return nil, ErrGatewayPoolWSIncompatible
	}
	// 覆写范围**只有 Codex 推理面**，没有开关。侧信道（装饰性 GET）、/codex/alpha/search、
	// /codex/realtime/calls 这些也打在 chatgpt.com 上，但它们不是推理轮次——给它们取一张票
	// 等于白烧一个 (上游账号 × 网关) 单位，而供给是个位数张/小时。
	// （2026-10-02 删掉了 openai_gwpool_all_models 那个开关：它唯一的用法是把这笔浪费打开。）
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
		release = s.gatewayPoolRelease(account, identity, pair)
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
		Region: pair.region, Version: pair.version,
	})
	return release, nil
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
