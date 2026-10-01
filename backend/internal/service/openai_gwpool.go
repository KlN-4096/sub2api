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
//   - 回：**不回报**。池子在交付那一刻就记了 LastTouch 和 LastVerdict，verdict 还是它自己用
//     state-echo 验出来的；而转发路径上一个可用判据都不剩（见 openAIGatewayPoolExtraKey 附近
//     的说明），回报只能填 "unknown"，等于把池子刚验出来的 "full" 覆盖掉，让刚验过满血的槽位
//     提前被拿去烧。「这张票坏了」这个信息由取 pair 时的 force=1 承载，不需要另一条回报。
//
// 配置**全在账号 extra 上**：池子发的 consumer key 是按账号发的，放实例级等于一个 sub2api 实例
// 里所有账号共用同一个池子身份。开关关着（或缺地址）时整条链路与接入前逐字节一致（走原来的
// Attach）。
//
// 池子没有满血槽位时回 503 ⇒ 这里把错误原样抛给调用方走既有失败路径，**绝不退回 cookie 回放**
// （用户原则：宁可 503 也不放降智）。
//
// **与 WS 上游互斥**：WS 连接复用 60 分钟（openAIWSConnMaxAge），而满血窗口只有约 150 秒，
// pair 只在握手挂一次 ⇒ 复用的连接会一直压在同一个已经烧完的网关上，而且预热（min_idle 默认 4）
// 会在无业务请求时就拨连接。对齐连接寿命与请求级窗口的代价远大于收益，所以直接互斥：
// 账号同时开两者时管理端写入直接拒绝，运行期 WS 拨号也拒（**预热因此拿不到 pair**）。

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

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
	// openAIGatewayPoolFetchTimeout 兜住一次取 pair。不跟随业务 ctx 的取消（见 gatewayPoolPair）。
	openAIGatewayPoolFetchTimeout = 8 * time.Second
	// openAIGatewayPoolListTimeout 兜住那次「列网关」。它是**优化**，绝不能吃掉取票的预算：
	// 列不出来就退回池子自己挑，所以给一个远小于 FetchTimeout 的额度。
	openAIGatewayPoolListTimeout = 2 * time.Second
	// openAIGatewayPoolGatewayWindowExtraKey 是本地账本的保留窗口（秒）。缺省 / 非正数走默认值。
	//
	// **刻意没有页面入口**，只能直接改 extra JSON：4 小时这个值基本不需要调，给它一个输入框
	// 等于多一处要维护的东西（也因此没进 openAIGatewayPoolConfigExtraKeys——它没有跨字段约束）。
	openAIGatewayPoolGatewayWindowExtraKey = "openai_gwpool_gateway_window_s"
	// openAIGatewayPoolGatewayWindow 是默认窗口。4 小时的出处：一个 (上游账号 × 网关) 单位烧掉
	// 之后的再生周期，docs/conventions/codex-full-strength-tickets.md 的「保守估算」——
	// **那篇文档明说这个数至今没测准**（静置 30 分钟到 4 小时，满血率恒在 3/11，与时长无关），
	// 所以它是个工程上的保守取值，不是实测结论。真测准了就该改这里。
	openAIGatewayPoolGatewayWindow = 4 * time.Hour
)

// ErrGatewayPoolWSIncompatible 是运行期的互斥闸：这个账号开着网关池，不能走 WS 上游。
// 管理端写入已经拦过同样的组合，这一道管手改 DB / 恢复备份造出来的行，并保证**预热拨号拿不到
// pair**（检查在取 pair 之前）。
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
	if seconds := a.getExtraInt(openAIGatewayPoolGatewayWindowExtraKey); seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return openAIGatewayPoolGatewayWindow
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
	// \x00 当分隔符：base_url 过了 URL 校验，不可能含 NUL，拼不出歧义键。
	cacheKey := baseURL + "\x00" + consumerKey
	if cached, ok := s.poolClients.Load(cacheKey); ok {
		if client, ok := cached.(*gwpool.Client); ok {
			return client, nil
		}
	}
	client := gwpool.New(baseURL, consumerKey)
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

// openAIGatewayPoolConfigExtraKeys 是会改变网关池配置或「网关池 ↔ WS 上游」互斥关系的 extra 键。
// 部分更新（UpdateAccountExtra / 批量）只在碰到它们时才去加载账号做合并校验。
var openAIGatewayPoolConfigExtraKeys = []string{
	openAIGatewayPoolExtraKey,
	openAIGatewayPoolBaseURLExtraKey,
	OpenAIGatewayPoolConsumerKeyExtraKey,
	"openai_oauth_responses_websockets_v2_enabled",
	"openai_apikey_responses_websockets_v2_enabled",
	"responses_websockets_v2_enabled",
	"openai_ws_enabled",
	"openai_ws_force_http",
}

// touchesOpenAIGatewayPoolConfig 报告这份 extra 更新碰到了网关池配置或互斥关系里的任一边。
func touchesOpenAIGatewayPoolConfig(extra map[string]any) bool {
	for _, key := range openAIGatewayPoolConfigExtraKeys {
		if _, ok := extra[key]; ok {
			return true
		}
	}
	return false
}

// validateOpenAIGatewayPoolAccountExtra 校验账号级网关池配置：开关开着就必须配齐地址与凭据，
// 且不能同时开 WS 上游（见文件头）。extra 必须是**合并后**的最终形态，否则部分更新会从另一半绕过去。
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
	// force_http 的账号永远不会拨 WS，不算冲突。
	if !probe.IsOpenAIResponsesWebSocketV2Enabled() || probe.IsOpenAIWSForceHTTPEnabled() {
		return nil
	}
	return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_WS_UPSTREAM_CONFLICT",
		"account %d cannot enable %s together with the Responses WebSocket v2 upstream: a WebSocket "+
			"connection is reused for up to 60 minutes while a full-strength route pair lasts ~150s, "+
			"so every reused turn would ride a burnt gateway. Turn one of them off.",
		account.ID, openAIGatewayPoolExtraKey)
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
type openAIGatewayPoolPair struct {
	cookie  string
	gateway string
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
	listCtx, cancel := context.WithTimeout(ctx, openAIGatewayPoolListTimeout)
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
func (s *openAICodexCookieStore) gatewayPoolPair(ctx context.Context, account *Account, identity string) (openAIGatewayPoolPair, error) {
	pair, state := s.cachedPoolPair(identity)
	if state == openAIGatewayPoolPairLive {
		return pair, nil
	}
	pool, err := s.poolClient(account)
	if err != nil {
		return openAIGatewayPoolPair{}, err
	}
	// 租着的那张过了建议窗口 ⇒ 要一张**不同的**网关（force=1）。池子的 valid_for_s 只是建议值
	// （ttl_is_advisory），换不换由这边判；不带 force 的话池子可能把同一张再发回来。
	force := state == openAIGatewayPoolPairStale
	fetchCtx := context.WithoutCancel(ctx)
	fetched, err, _ := s.poolFetch.Do(identity, func() (any, error) {
		// 排在后面的请求醒来时第一名可能已经取到了。
		if pair, cached := s.cachedPoolPair(identity); cached == openAIGatewayPoolPairLive {
			return pair, nil
		}
		callCtx, cancel := context.WithTimeout(fetchCtx, openAIGatewayPoolFetchTimeout)
		defer cancel()
		// upstream = 这一发真正要用的那个上游账号（池子的 ?account=）：池子按它记槽位，
		// 不报就记在 consumer key 上传者的头上。
		upstream := gatewayPoolUpstreamAccountID(identity)
		// 自己挑落点：池子按它发的 consumer key 记账，认不出「同一份凭据挂在多个账号行上」，
		// 所以这里按凭证域身份的本地账本再滤一道。挑不出来时 steer 为空 = 由池子按调度选
		// （接 /gateways 之前的行为，永远是兜底）。
		// 列表与取票同在 singleflight 里 ⇒ 同身份并发只列一次，不另加一层缓存。
		steer := s.gatewayPoolPick(callCtx, pool, account, identity)
		got, err := pool.Cookie(callCtx, upstream, steer, force)
		if steer != "" && errors.Is(err, gwpool.ErrNoSlot) {
			// 点名的那个在「列表」与「取票」之间被别人租走了。这一发什么都没交付、没烧任何
			// 槽位，所以退回裸取一次——不然自己挑网关反而把本来能成的请求打成失败。
			//
			// **只退一次，绝不能改成循环重试**：裸取的 503 意味着池子现在真的一张都没有，
			// 重试只会在供给见底时把每个业务请求放大成一串池子请求（风暴）。
			// 「点名失败就换一个候选再点」同理不做：候选都是同一张列表来的，它过期了就全过期。
			got, err = pool.Cookie(callCtx, upstream, "", force)
		}
		if err != nil {
			// 刻意**不删**缓存里那张过期的：它是「别再给我这一个」的依据，删掉之后下一发会走
			// 不带 force 的 /cookie，池子可能原样把烧过的那张发回来。force 的 503 不在这里重试。
			return nil, err
		}
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
			until:   time.Now().Add(got.ValidFor),
		}
		s.poolPairs.Store(identity, pair)
		// 记账用**实际拿到的**那个网关名，而不是我们点名的那个：裸取和池子忽略点名时都只能
		// 从响应里知道落点。
		s.gatewayPoolMarkUsed(identity, pair.gateway)
		slog.Info("gwpool_pair_taken", "account_id", account.ID, "gateway", pair.gateway,
			"steered_to", steer, "valid_for_s", int(got.ValidFor.Seconds()),
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
		return openAIGatewayPoolPair{}, err
	}
	taken, _ := fetched.(openAIGatewayPoolPair)
	return taken, nil
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

// livePoolPair 读缓存里还在满血窗口内的那张。
func (s *openAICodexCookieStore) livePoolPair(identity string) (openAIGatewayPoolPair, bool) {
	pair, state := s.cachedPoolPair(identity)
	return pair, state == openAIGatewayPoolPairLive
}

// gatewayPoolPairInUse 回读这一发出站实际带的那张 pair，给 usage_logs 的路由对读数用
// （池子接管时罐不再参与出站，回读罐会记成别的网关）。
func (s *openAICodexCookieStore) gatewayPoolPairInUse(ctx context.Context, account *Account) (string, bool) {
	if !s.gatewayPoolTakeover(account) {
		return "", false
	}
	identity, err := s.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return "", false
	}
	pair, ok := s.livePoolPair(identity)
	if !ok {
		return "", false
	}
	return pair.cookie, true
}

// AttachRoute 是出站挂 Cookie 的唯一入口：池子接管时 __cflb / __oailb 用池子那张，否则原样走
// 罐回放。三个出站挂钩点（HTTP 主咽喉 doOpenAIUpstream、WS 连接池 dialConn、WS 透传适配器）
// 都经这里，所以「钉死在坏网关」在三条路上一起修掉。
func (s *openAICodexCookieStore) AttachRoute(
	ctx context.Context,
	account *Account,
	rawURL string,
	headers http.Header,
) error {
	if s == nil || headers == nil || !openAICodexCookiesApply(account) {
		return nil
	}
	u := openAICodexCookieURL(rawURL)
	// 主机过滤：罐分支由 chatgptcookies 自己兜（IsChatGPTURL），接管分支绕开了罐就得自己兜。
	// 不兜的话 pair 会被发给 api.openai.com 这类第三方主机，而且**白烧一张池子 pair**（根本没
	// 碰到那个网关，读数却照样上报），供给只有个位数张。
	if u == nil || !chatgptcookies.IsChatGPTURL(u) {
		return nil
	}
	if !s.gatewayPoolTakeover(account) {
		s.Attach(account, rawURL, headers)
		return nil
	}
	// 互斥闸必须在取 pair **之前**：WS 预热（min_idle 默认 4，无业务请求也拨）绝不能消耗池子的
	// 槽位，而复用连接上的后续轮次也拿不到新窗口。
	if isWebSocketURL(rawURL) {
		return ErrGatewayPoolWSIncompatible
	}
	identity, err := s.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return err
	}
	pair, err := s.gatewayPoolPair(ctx, account, identity)
	if err != nil {
		// 包括池子 503（gwpool.ErrNoSlot）。往上抛给既有失败路径，不回落罐回放。
		return err
	}
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
	headers.Set("Cookie", strings.Join(append(parts, pair.cookie), "; "))
	return nil
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
