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
//   - 回：**取到 pair 的那一刻就回报** `(上游账号, 池子说的那个网关, 判定)`。刻意不挂在计费
//     落库那条路上：那里的网关是事后从响应/罐里重新推导的（借来的健康 pair 恰恰是「上游不下发
//     Set-Cookie」那一类，推导会读回罐里钉死的旧网关），而且预热、dial 失败、4xx/5xx、传输错误
//     都不落 usage ⇒ 整批触碰漏报。取 pair ⇒ 必然触碰，这个时点才是完整的。
//
// 两道开关都必须开：全局 cfg.Gwpool.Enabled（决定有没有客户端）+ 账号级 extra 开关。
// 任何一道关着，整条链路与接入前逐字节一致（走原来的 Attach）。
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
	// openAIGatewayPoolFetchTimeout 兜住一次取 pair。不跟随业务 ctx 的取消（见 gatewayPoolPair）。
	openAIGatewayPoolFetchTimeout = 8 * time.Second
	// openAIGatewayPoolTouchTimeout 兜住回报。回报是账本，不能拖业务请求。
	openAIGatewayPoolTouchTimeout = 3 * time.Second
	// openAIGatewayPoolVerdictUnknown：转发路径上没有可用的降智判据。
	//
	// 2026-09-23 起模型标签会说谎、turn-state 一律 780、x-codex-safety-buffering-* 健康账号也带
	// （codex-full-strength-tickets.md 第二节「已证伪」清单），仓库里能在一次真实转发上直接读出
	// 的判据一个都不剩。**刻意不为回报新造判据**——池子自己用 state-echo 验。
	openAIGatewayPoolVerdictUnknown = "unknown"
)

// ErrGatewayPoolWSIncompatible 是运行期的互斥闸：这个账号开着网关池，不能走 WS 上游。
// 管理端写入已经拦过同样的组合，这一道管手改 DB / 恢复备份造出来的行，并保证**预热拨号拿不到
// pair**（检查在取 pair 之前）。
var ErrGatewayPoolWSIncompatible = errors.New(
	"openai gateway pool is enabled on this account: the WebSocket upstream reuses one connection for " +
		"up to 60 minutes while a full-strength route pair lasts ~150s, so the two cannot be combined")

// newOpenAIGatewayPoolClient 按配置建客户端；全局开关关着（或没配地址）就返回 nil = 不接管。
// 配置的合法性在 config.Validate 里已经拦过，这里只做最后一道空值判断。
func newOpenAIGatewayPoolClient(cfg *config.Config) *gwpool.Client {
	if cfg == nil || !cfg.Gwpool.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.Gwpool.BaseURL) == "" {
		return nil
	}
	return gwpool.New(cfg.Gwpool.BaseURL, cfg.Gwpool.ConsumerKey)
}

// UsesGatewayPool 报告这个账号的 Codex 路由 cookie 由网关池下发。
//
// 范围与 cookie 回放完全一致（openAICodexCookiesApply / IsOpenAIOAuthLike）：只有本地持有
// ChatGPT 凭据的账号才会往 chatgpt.com 发推理请求，cpr 是原样中继、API Key 账号的上游不是它。
func (a *Account) UsesGatewayPool() bool {
	return a != nil && a.IsOpenAIOAuthLike() && a.getExtraBool(openAIGatewayPoolExtraKey)
}

// openAIGatewayPoolExclusivityExtraKeys 是会改变「网关池 ↔ WS 上游」这对互斥关系的 extra 键。
// 部分更新（UpdateAccountExtra / 批量）只在碰到它们时才去加载账号做合并校验。
var openAIGatewayPoolExclusivityExtraKeys = []string{
	openAIGatewayPoolExtraKey,
	"openai_oauth_responses_websockets_v2_enabled",
	"openai_apikey_responses_websockets_v2_enabled",
	"responses_websockets_v2_enabled",
	"openai_ws_enabled",
	"openai_ws_force_http",
}

// touchesOpenAIGatewayPoolExclusivity 报告这份 extra 更新碰到了互斥关系里的任一边。
func touchesOpenAIGatewayPoolExclusivity(extra map[string]any) bool {
	for _, key := range openAIGatewayPoolExclusivityExtraKeys {
		if _, ok := extra[key]; ok {
			return true
		}
	}
	return false
}

// validateOpenAIGatewayPoolAccountExtra 拦住「网关池 + WS 上游」这个组合（见文件头）。
// extra 必须是**合并后**的最终形态，否则部分更新会从另一半绕过去。
func validateOpenAIGatewayPoolAccountExtra(account *Account, extra map[string]any) error {
	if account == nil {
		return nil
	}
	probe := &Account{
		ID: account.ID, Platform: account.Platform, Type: account.Type,
		ParentAccountID: account.ParentAccountID, Credentials: account.Credentials, Extra: extra,
	}
	// force_http 的账号永远不会拨 WS，不算冲突。
	if !probe.UsesGatewayPool() || !probe.IsOpenAIResponsesWebSocketV2Enabled() ||
		probe.IsOpenAIWSForceHTTPEnabled() {
		return nil
	}
	return infraerrors.Newf(http.StatusBadRequest, "GWPOOL_WS_UPSTREAM_CONFLICT",
		"account %d cannot enable %s together with the Responses WebSocket v2 upstream: a WebSocket "+
			"connection is reused for up to 60 minutes while a full-strength route pair lasts ~150s, "+
			"so every reused turn would ride a burnt gateway. Turn one of them off.",
		account.ID, openAIGatewayPoolExtraKey)
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

// gatewayPoolTakeover 报告这一发该由池子出 cookie。
func (s *openAICodexCookieStore) gatewayPoolTakeover(account *Account) bool {
	return s != nil && s.pool != nil && account.UsesGatewayPool()
}

// gatewayPoolIdentity 取缓存键 / 回报用的凭证域身份。没注入解析器（裸结构体单测）时退回按本地行算。
func (s *openAICodexCookieStore) gatewayPoolIdentity(ctx context.Context, account *Account) (string, error) {
	if s.identity != nil {
		return s.identity(ctx, account)
	}
	return openAIGatewayPoolAccountKey(account), nil
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
		// gateway 留空 = 由池子按调度选（它知道每个槽位歇了多久，这里不替它决定）。
		got, err := s.pool.Cookie(callCtx, "", force)
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
		slog.Info("gwpool_pair_taken", "account_id", account.ID, "gateway", pair.gateway,
			"valid_for_s", int(got.ValidFor.Seconds()), "verified_full", got.VerifiedFull,
			"ttl_is_advisory", got.TTLIsAdvisory, "forced", force)
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

// reportGatewayPoolTouch 回报一次触碰：取到 pair ⇒ 这一发必然碰到那个网关。
//
// 每一次注入都报，不只是新取那一次：窗口内复用 150 秒期间这个槽位一直在被碰，只报取用那一刻
// 会让池子以为它多歇了 150 秒，而「取歇得最久的」正是它的调度依据。
//
// 异步 + 独立超时：账本不准不该让业务请求变慢，ctx 在响应写完后随时会被取消。
func (s *openAICodexCookieStore) reportGatewayPoolTouch(ctx context.Context, accountID int64, identity string, pair openAIGatewayPoolPair) {
	if s == nil || s.pool == nil || identity == "" || pair.gateway == "" {
		return
	}
	pool := s.pool
	touch := gwpool.Touch{
		AccountID: identity,
		Gateway:   pair.gateway,
		At:        time.Now(),
		Verdict:   openAIGatewayPoolVerdictUnknown,
	}
	bgCtx := context.WithoutCancel(ctx)
	go func() {
		reportCtx, cancel := context.WithTimeout(bgCtx, openAIGatewayPoolTouchTimeout)
		defer cancel()
		if err := pool.Touch(reportCtx, touch); err != nil {
			slog.Warn("gwpool_touch_failed", "account_id", accountID, "gateway", touch.Gateway, "error", err)
		}
	}()
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
	s.reportGatewayPoolTouch(ctx, account.ID, identity, pair)
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
