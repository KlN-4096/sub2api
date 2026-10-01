// Package gwpool 是网关池（E:/Project/GO/gwpool，SPEC.md 第 10 节）的消费端 HTTP 客户端。
//
// 池子负责发现 Codex 后端网关、维护活的路由 pair（__cflb + __oailb）并下发。本包做三件事：
// 列网关（GET /gateways）、取一张 pair（GET /cookie）、把没用过的票还回去（POST /release）。
// 满血验证、续期全在池子那边，这里不复制任何判据；列表只用来**挑落点**——按
// (上游账号 × 网关) 算的「烧过没」只有消费端知道。
//
// 刻意**没有触碰回报**：票是池子发的、满血也是池子验的——交付那一刻它自己就写了槽位的
// last_touch，验证时写了 last_verdict。转发路径上一个降智判据都不剩（模型标签会说谎、
// turn-state 一律 780、safety-buffering 头健康账号也带），消费端能回报的只有 "unknown"，
// 而 unknown 回报过去只会覆盖掉池子的真判定，让刚验过满血的槽位提前被拿去烧。
// 「这张票坏了」由取 pair 时的 force=1 承载。
//
// 红线：consumer key 只进 Authorization 头，不进日志、不进错误串；cookie 全文同样不进日志。
package gwpool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrPool 标记「失败出在池子这一侧」，池子的每一个错误都包着它。
//
// 存在的理由：池子的传输错误（容器没起、换端口、重启）长得和真实上游/代理故障一模一样
// （connection refused / no such host），而 sub2api 的传输错误分类器是按这些字符串判
// 「代理持久故障」并把账号停调度 10 分钟 + 告警的。重启池子是日常操作，不能让它停掉真账号，
// 所以消费端在进分类器之前先 errors.Is 掉这个标记。
var ErrPool = errors.New("gwpool")

// ErrNoSlot 是「这一次没给票」。消费端据此走失败路径，**不得退回降级的 cookie 回放**
// （用户原则：宁可 503 也不放降智）。结构化失败（*PoolError）全部 Unwrap 到它：九个错误码
// 讲的都是同一件事——这一发没拿到票；**为什么**没拿到由 Code 承载，分流看码不看状态码。
var ErrNoSlot = fmt.Errorf("%w: no full-strength slot available", ErrPool)

// 池子失败响应的闭集错误码。契约冻结，池子侧与消费侧共用同一份名字。
const (
	CodeNoGateway        = "no_gateway"        // 池子空
	CodeAllCooling       = "all_cooling"       // 你的槽位全在冷却
	CodeNoLivePair       = "no_live_pair"      // 没有活票且现铸没成
	CodeRateLimited      = "rate_limited"      //
	CodeNoExit           = "no_exit"           // 池子的出口全熔断
	CodeUpstreamRejected = "upstream_rejected" // 上游否决（400/401/403）
	CodeMintFailed       = "mint_failed"       //
	CodePublicClosed     = "public_closed"     // 匿名此刻不开放
	CodeDegraded         = "degraded"          //
	// CodeConsumerRejected 是**我们自己**的 consumer key 不被受理（/cookie 与 /release 的认证失败
	// 都走它）。与 CodeUpstreamRejected 分开：那个讲的是池子的上游被 OpenAI 否决。
	CodeConsumerRejected = "consumer_rejected"
	// CodeBadRequest 是出站请求本身不合法（400 参数不合法 / 404 点名的账号不存在，池子把两者
	// 合成一个：对调用方是同一件事「你的请求错了，改了再来」）。池子**刻意不给**
	// retry_after_seconds —— 重试不会变好。
	CodeBadRequest = "bad_request"
)

// knownCodes 是上面那个闭集。**这是信任边界**：码会决定消费端要不要停取票、停多久，所以池子报
// 了集合外的值一律当「没报码」（走通用失败路径），不照抄一个没约定过的字符串去做分流决策。
var knownCodes = map[string]struct{}{
	CodeNoGateway: {}, CodeAllCooling: {}, CodeNoLivePair: {}, CodeRateLimited: {},
	CodeNoExit: {}, CodeUpstreamRejected: {}, CodeMintFailed: {}, CodePublicClosed: {},
	CodeDegraded: {}, CodeConsumerRejected: {}, CodeBadRequest: {},
}

// PoolError 是池子结构化的「这次没给票」：{"error":{"code":…,"retry_after_seconds":…}}。
//
// 刻意**不收 message**：那是池子给人看的自由文本（外部输入），消费端一个判据都不从它取，
// 客户端看到的文案是本仓库自己的双语单串。少收一个字段 = 少一处要校验长度与控制字符的地方。
type PoolError struct {
	// Code 是闭集之一；池子没报、或报了集合外的值时为空。
	Code string
	// Status 是 HTTP 状态码，只做诊断。
	Status int
	// RetryAfter 是池子要求的退避时长，0 = 它没说。已钳进 [0, MaxRetryAfter]。
	RetryAfter time.Duration
}

func (e *PoolError) Error() string {
	code := e.Code
	if code == "" {
		code = "unspecified"
	}
	msg := fmt.Sprintf("pool refused: code=%s http=%d", code, e.Status)
	if e.RetryAfter > 0 {
		msg += fmt.Sprintf(" retry_after=%s", e.RetryAfter)
	}
	return msg
}

// Unwrap 到 ErrNoSlot（它自己又包着 ErrPool）：九个码全是「这一发没拿到票」⇒ 既有的
// fail-closed 与「池子故障不要停真账号调度」两条判断都不用改。
func (e *PoolError) Unwrap() error { return ErrNoSlot }

const (
	// defaultRequestTimeout 兜住单次池子调用，只在调用方没给超时时用。池子通常是同机服务、
	// 毫秒级返回；它卡住不能把业务请求拖死。
	//
	// **它会盖住调用方的 ctx deadline**：http.Client.Timeout 与 ctx 取较小者，所以写死一个值
	// 等于让消费端那边配的「取票超时」在超过它之后失效（2026-10-02 前就是这个毛病——账号配 8s
	// 实际仍然 5s 断）。因此 New 收一个超时参数，这里只做缺省。
	defaultRequestTimeout = 5 * time.Second
	// maxResponseBytes 读响应的上限。pair 里 __oailb 是约 300 字符的 JWT，4 KiB 足够。
	maxResponseBytes = 4 << 10
	// maxListBytes 是 /gateways 的上限。cookie 那 4 KiB 对一张列表太紧，而截断会让整份 JSON
	// 解不开（= 挑不出网关，退回池子自己挑），所以这里给宽一点。
	maxListBytes = 64 << 10
	// MaxRetryAfter 钳住 retry_after_seconds / Retry-After。池子报一个 10 年会把这个身份
	// **永久饿死**（消费端按身份记退避），所以上限硬钳在这里。
	MaxRetryAfter = time.Hour
	// maxCodeLen / maxVersionLen 是两个外部字符串的长度上限。code 只用来查闭集（长的本来就不在
	// 集合里，早剪早省事）；cookie_version 要进 URL query、JSON 体和使用记录的一列。
	maxCodeLen    = 64
	maxVersionLen = 128
	// maxGatewayLen 与消费侧 usage_logs 那一列同值。网关名同样是外部输入：它会进本地账本的
	// 复合键、进日志、进 usage_logs.route_pair_pool_gateway，而 NUL(0x00) 是**合法 UTF-8**
	// （ToValidUTF8 不会剔掉它）、Postgres 的 TEXT 直接拒收 ⇒ 一条脏行能连带让整批用量插入失败。
	maxGatewayLen = 64
	// MaxExcludeItems / maxExcludeItemLen 是 exclude / exclude_versions 的契约上限：最多 64 项，
	// 每项 ≤64 字符。超限池子会拒掉**整条**请求（= 取不到票），所以出站前自己先裁。
	// 项数上限导出：调用方要按它裁自己那本账（裁掉哪些由它定，这里只保证不发非法请求）。
	MaxExcludeItems   = 64
	maxExcludeItemLen = 64
	// min_remaining 的契约钳位区间（池子那边也钳，这里先钳是为了别发一个必然被改写的值）。
	minRemainingFloor = 30 * time.Second
	minRemainingCeil  = time.Hour
	// maxValidFor 钳住响应里的 valid_for_s。**物理上限**：__oailb 的 exp-iat 恒为 3900s，
	// 比这更长的票不存在。不钳的后果是整个功能被静默抵消——池子报 1e9（或把单位写成毫秒）
	// ⇒ 消费端的到点落在几十年后 ⇒ 缓存永远 live ⇒ 满血窗口过了还在拿烧掉的路由跑业务，
	// 而且池子再也不被调用（连 gwpool_pair_taken 都不再出现），没有任何人看得见。
	maxValidFor = 3900 * time.Second
	// maxWait 是 wait 的契约上限。
	maxWait = 30 * time.Second
	// waitSafetyMargin 是 wait 与本次调用死线之间必须留的余量：池子等满 wait 之后还要把响应
	// 写回来。给 0 等于「池子刚要回复、这边已经断了」。
	waitSafetyMargin = time.Second
)

// Pair 是 /cookie 的一次下发。
type Pair struct {
	// Gateway 形如 "unified-142"。
	Gateway string
	// Cookie 是直接写进出站 Cookie 头的整串 "__cflb=...; __oailb=..."。
	Cookie string
	// ValidFor 是**满血窗口**的剩余量（池子的 valid_for_s），不是 cookie 的有效期。
	// 窗口内同一张 pair 可以复用，过了就该再要一张。
	ValidFor time.Duration
	// PairRemaining 是**这张 pair 自己**的剩余寿命（池子的 pair_remaining_s = __cflb 的死期
	// 减现在）。和 ValidFor 是两根轴：ValidFor = min(满血窗口剩余, pair 剩余)，绝大多数时候
	// 等于那 183 秒的窗口，分不出 pair 还剩 50 分钟还是 8 分钟 ⇒ 判「这张要不要续」只能用这个。
	// 0 = 池子没报（还没升级）：消费端按**不续**处理（见 openAIGatewayPoolRenewBelow）。
	PairRemaining time.Duration
	// VerifiedFull 是池子交付前自己验过满血。只做读数，消费端不拿它当闸门。
	VerifiedFull bool
	// TTLIsAdvisory：池子声明 valid_for_s 只是建议值，换不换 pair 由消费端自己判。
	// 纯读数——消费端的逻辑本来就是「自己判这张不行了就 force 换一张」，不按这个字段分流。
	TTLIsAdvisory bool
	// Version 是**这一张具体的票**的不透明稳定身份（池子的 cookie_version）。两个用处：
	// 还票（Release）与「这张不行，别再给我」（CookieRequest.ExcludeVersions）。
	// 池子没报 / 报了畸形值时为空串——那只是少了这两个能力，不是丢弃这张票的理由。
	Version string
}

// CookieRequest 是取一张 pair 的全部参数。
//
// 为什么是结构体而不是继续加位置参数：七项里五项可选，`Cookie(ctx, acct, gw, true, nil, nil, 0, 0)`
// 在调用点读不出任何意思。
type CookieRequest struct {
	// Account 是**这一发真正要用的那个上游账号**（access_token 里的 chatgpt_account_id）。
	// 一把 consumer key 可以替多个上游账号取票，而满血窗口是 (上游账号 × 网关) 的 ⇒ 不报它的话
	// 池子会把槽位记在**上传者**那一行上，白白划掉一个对本账号还满血的落点。空串 = 按上传者记。
	// 跨账号取到的票 verified_full 恒为 false（池子手上没有那个账号的凭据，验不了），
	// 这是契约里的硬限制，**不是丢弃这张票的理由**。
	Account string
	// Gateway 点名落点，空 = 池子按自己的调度选。
	Gateway string
	// Force 表示「我现在租着的那张不行了」：池子保证给一个**不同**的网关，换不出来就报
	// no_live_pair / all_cooling（不会把原来那张再发一遍）。
	Force bool
	// Exclude 是「这些网关我已经烧过了，别给我」。消费端的本地账本按 (上游账号 × 网关) 记，
	// 比池子按 consumer key 记的那本准；带上去之后**裸取也能避开烧过的落点**。
	// 最近烧的放前面：超过 64 项时尾部会被裁掉。
	Exclude []string
	// ExcludeVersions 是「这几张具体的票不行」。比 Force 精确：Force 只说「换个网关」。
	ExcludeVersions []string
	// MinRemaining 是能接受的最低剩余寿命。0 = 用池子配的 DeliverFloor。
	MinRemaining time.Duration
	// Wait 是愿意等池子现铸多久。池子只在「没有活票 / 池子空」这两种可等待的失败上等。
	// **它会被钳到本次调用的死线之内**（见 Cookie）：池子还在等、这边先超时等于白等一场。
	Wait time.Duration
}

// query 把参数编成 /cookie 的查询串。budget 是本次调用还剩多少时间（≤0 = 没有死线）。
func (r CookieRequest) query(budget time.Duration) url.Values {
	query := url.Values{}
	if account := strings.TrimSpace(r.Account); account != "" {
		query.Set("account", account)
	}
	if gateway := strings.TrimSpace(r.Gateway); gateway != "" {
		query.Set("gateway", gateway)
	}
	if r.Force {
		query.Set("force", "1")
	}
	if list := joinExcludes(r.Exclude); list != "" {
		query.Set("exclude", list)
	}
	if list := joinExcludes(r.ExcludeVersions); list != "" {
		query.Set("exclude_versions", list)
	}
	if r.MinRemaining > 0 {
		query.Set("min_remaining", strconv.Itoa(int(clampDuration(r.MinRemaining, minRemainingFloor, minRemainingCeil).Seconds())))
	}
	// wait 的两道钳位：契约上限 30s，以及本次调用的剩余预算。后者**必须是代码**而不是注释——
	// 调用方把 wait 算错（或配了一个更短的取票超时）时，不该出现「池子等 25s、客户端 8s 就断」。
	wait := clampDuration(r.Wait, 0, maxWait)
	if budget > 0 && wait > budget-waitSafetyMargin {
		wait = budget - waitSafetyMargin
	}
	if wait > 0 {
		query.Set("wait", strconv.Itoa(int(wait.Seconds())))
	}
	return query
}

// joinExcludes 裁出一条合法的 exclude 串：空项丢掉，超长项丢掉（带上去池子会拒整条请求），
// 最多 64 项（尾部裁掉 —— 调用方把最近烧的放在前面）。
func joinExcludes(items []string) string {
	kept := make([]string, 0, min(len(items), MaxExcludeItems))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" || len(item) > maxExcludeItemLen {
			continue
		}
		kept = append(kept, item)
		if len(kept) == MaxExcludeItems {
			break
		}
	}
	return strings.Join(kept, ",")
}

func clampDuration(value, low, high time.Duration) time.Duration {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}

// sanitizeOpaque 收口池子给的不透明字符串（cookie_version、错误码）：**这是信任边界**。
// 它们会进 URL query、JSON 体、数据库和页面，所以控制字符一律判废（返回空串 = 当池子没报），
// 超长同样判废而不是截断——截断出来的 version 还回去是另一张票。
func sanitizeOpaque(value string, maxLen int) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxLen {
		return ""
	}
	if strings.ContainsFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return ""
	}
	return value
}

// Gateway 是 /gateways 列表里的一项，只留挑网关用得上的字段（live / target / valid_for_s
// 是池子自己的读数，消费端不按它们分流）。
type Gateway struct {
	// Name 形如 "unified-167"。
	Name string
	// PairReady：这个网关有活 pair，且剩余寿命够交付。
	PairReady bool
	// UsedByYou：池子记着你这个账号在这个网关上烧过。它只覆盖池子自己那本账，
	// 按凭证域身份算的那本在消费端（见 service 层的本地账本）。
	UsedByYou bool
	// LastUsedAt 是池子记的「你上次碰它」的时刻。零值 = 没碰过，也是最优候选。
	LastUsedAt time.Time
}

// Client 是一个池子实例的客户端。并发安全。
type Client struct {
	base        *url.URL
	consumerKey string
	http        *http.Client
}

// New 建客户端。baseURL 的合法性由配置期的 config.ValidateAbsoluteHTTPURL 负责；这里解不开就
// 返回 nil，调用方按「没接管」处理。
//
// timeout 是单次调用的上限，必须 >= 调用方打算给的最大 ctx deadline，否则它会把那个 deadline
// 盖掉（http.Client.Timeout 与 ctx 取较小者）。<=0 取 defaultRequestTimeout。
// 消费端按 (base_url, consumer key, timeout) 缓存客户端 —— 超时进缓存键，不然改了配置拿到的
// 还是旧客户端。
//
// 端点一律用 url.JoinPath 拼：base_url 带 query 或不带结尾斜杠时字符串拼接会把路径拼坏。
//
// 显式给 Transport 而不是用 http.DefaultTransport：池子通常是 127.0.0.1 上的同机服务，
// 不能让进程的 HTTP_PROXY 把它代理出去。
func New(baseURL, consumerKey string, timeout time.Duration) *Client {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || base == nil || base.Host == "" {
		return nil
	}
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	return &Client{
		base:        base,
		consumerKey: strings.TrimSpace(consumerKey),
		http: &http.Client{
			Timeout:   timeout,
			Transport: &http.Transport{},
		},
	}
}

// endpoint 拼出池子的某个端点，丢掉 base_url 自带的 query/fragment。
func (c *Client) endpoint(path string) string {
	u := c.base.JoinPath(path)
	u.RawQuery, u.Fragment = "", ""
	return u.String()
}

// Cookie 取一张 pair。
//
// 失败一律返回错误，由调用方按失败处理；池子给了结构化错误体时返回 *PoolError（带闭集错误码与
// 退避时长，调用方据此分流）。**不在这里重试**：force 失败就是失败，自动重试会把池子供给烧干。
func (c *Client) Cookie(ctx context.Context, request CookieRequest) (Pair, error) {
	if c == nil {
		return Pair{}, fmt.Errorf("%w: client is nil", ErrPool)
	}
	var budget time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}
	endpoint := c.endpoint("cookie")
	if encoded := request.query(budget).Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Pair{}, fmt.Errorf("%w: build cookie request: %w", ErrPool, err)
	}
	resp, err := c.do(req)
	if err != nil {
		return Pair{}, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return Pair{}, refusal(resp)
	}
	var payload struct {
		Gateway        string `json:"gateway"`
		Cookie         string `json:"cookie"`
		ValidForS      int    `json:"valid_for_s"`
		VerifiedFull   bool   `json:"verified_full"`
		TTLIsAdvisory  bool   `json:"ttl_is_advisory"`
		CookieVersion  string `json:"cookie_version"`
		PairRemainingS int    `json:"pair_remaining_s"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return Pair{}, fmt.Errorf("%w: decode cookie response: %w", ErrPool, err)
	}
	// 这里是信任边界（外部服务的响应 → 要带着该账号的 Authorization 发给 chatgpt.com 的头）：
	// 空 cookie 等于裸打（落点不可控），窗口 ≤0 的 pair 本来就过期，控制字符会劈开出站头。
	// cookie 名字的收口在消费侧（只留 __cflb / __oailb），这里只拦明显畸形。
	if strings.TrimSpace(payload.Cookie) == "" {
		return Pair{}, fmt.Errorf("%w: cookie response carried no cookie", ErrPool)
	}
	if strings.ContainsFunc(payload.Cookie, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return Pair{}, fmt.Errorf("%w: cookie response carried control characters", ErrPool)
	}
	if payload.ValidForS <= 0 {
		return Pair{}, fmt.Errorf("%w: cookie response carried a non-positive valid_for_s", ErrPool)
	}
	return Pair{
		Gateway:       sanitizeOpaque(payload.Gateway, maxGatewayLen),
		Cookie:        strings.TrimSpace(payload.Cookie),
		ValidFor:      clampDuration(time.Duration(payload.ValidForS)*time.Second, 0, maxValidFor),
		VerifiedFull:  payload.VerifiedFull,
		TTLIsAdvisory: payload.TTLIsAdvisory,
		Version:       sanitizeOpaque(payload.CookieVersion, maxVersionLen),
		// 和 ValidFor 同样钳进 [0, maxValidFor]：缺失/负数 ⇒ 0 ⇒ 消费端不续（安全方向），
		// 报一个大数被钳到 3900s 仍然远在续期阈值之上 ⇒ 同样不续。
		PairRemaining: clampDuration(time.Duration(payload.PairRemainingS)*time.Second, 0, maxValidFor),
	}, nil
}

// Release 把一张**一个字节都没发出去**的票还给池子，让它把槽位还给我们自己
// （池子只在交付后 DeliverTTL 内受理，太晚 / 找不到 / 已还过都是 409）。
//
// 调用方把它当**尽力而为**：失败只记日志，不影响主流程。版本串为空（池子没报 cookie_version）
// 时直接返回——没有身份就还不了，这不是错误。
func (c *Client) Release(ctx context.Context, version string) error {
	if c == nil {
		return fmt.Errorf("%w: client is nil", ErrPool)
	}
	version = sanitizeOpaque(version, maxVersionLen)
	if version == "" {
		return nil
	}
	body, err := json.Marshal(map[string]string{"cookie_version": version})
	if err != nil {
		return fmt.Errorf("%w: build release request: %w", ErrPool, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("release"), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: build release request: %w", ErrPool, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	// 409 = 太晚 / 找不到 / 已还过。它和「网络不通」对调用方是同一回事（记日志走人），
	// 所以不给它单独的 sentinel。
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("%w: release returned HTTP %d", ErrPool, resp.StatusCode)
	}
	return nil
}

// Renew 把这一发从上游换回来的**新路由对**回传池子，让它续上这张 pair 的寿命。
//
// 为什么只有消费端能做这件事：__cflb 的 3600s 寿命只有**业务请求**能刷新，而刷新的前提是这一发
// 不带 __oailb（两件齐发时上游什么都不回 —— 2026-10-02 实测 F 组）。池子自己手上没有业务请求，
// 刷不出来。
//
// 回传的是**事实**（一串 cookie），不是判断。别往这个 body 里加满血/降智/质量字段：转发路径上
// 没有可信的降智判据，带这类字段的 /touch 端点 10-01 就是因为这个被删掉的。
//
// 返回池子给**续期后那张票**的新 cookie_version（池子没换票号时为空串，调用方留用旧的）。
// 调用方把它当**尽力而为**：失败只记日志，不影响业务请求。
func (c *Client) Renew(ctx context.Context, version, cookie string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("%w: client is nil", ErrPool)
	}
	version = sanitizeOpaque(version, maxVersionLen)
	cookie = strings.TrimSpace(cookie)
	if version == "" || cookie == "" {
		return "", nil // 没有票号就没法指认续的是哪张；没有新 cookie 就没什么可续。
	}
	body, err := json.Marshal(map[string]string{"cookie_version": version, "cookie": cookie})
	if err != nil {
		return "", fmt.Errorf("%w: build renew request: %w", ErrPool, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("pair/renew"), bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("%w: build renew request: %w", ErrPool, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return "", refusal(resp)
	}
	var payload struct {
		OK            bool   `json:"ok"`
		CookieVersion string `json:"cookie_version"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return "", fmt.Errorf("%w: decode renew response: %w", ErrPool, err)
	}
	// 200 但 ok=false 是池子明说「没续上」：当失败处理，别把一个没生效的续期记成成功。
	// valid_for_s 刻意不读：续期延长的是**路由钉死**的寿命，不是满血窗口，而本地到点只能来自
	// 交付时的 valid_for_s（见消费端 openAIGatewayPoolPair.until）。
	if !payload.OK {
		return "", fmt.Errorf("%w: renew was not accepted", ErrPool)
	}
	// **信任边界**：票号是我们之后用来还票（/release）和弃票（exclude_versions）的身份，而响应
	// 是外部输入 —— 池子（或冒充它的东西）回一个属于**别人那张在用票**的票号，之后两个动作就全
	// 打在别人头上了。票号按契约是 sha256(cookie) 的前 12 位十六进制、确定性可本地复算
	// （SPEC 第 10 节），所以这里直接复算比对。
	// 不符就**当池子没换票号**（留用旧的）而不是整次失败：续期本身已经生效了，而旧票号至少
	// 还是我们自己那张票的身份。池子那边若对 cookie 做了归一化（空格/顺序），这条也会退到这个
	// 安全方向上。
	next := sanitizeOpaque(payload.CookieVersion, maxVersionLen)
	if next != "" && !strings.EqualFold(next, cookieVersionOf(cookie)) {
		// 票号与 cookie 本体同级：一个字都不进日志。
		slog.Warn("gwpool_renew_version_mismatch")
		return "", nil
	}
	return next, nil
}

// cookieVersionOf 按契约复算票号：sha256(cookie) 的前 12 位十六进制。
func cookieVersionOf(cookie string) string {
	sum := sha256.Sum256([]byte(cookie))
	return hex.EncodeToString(sum[:])[:12]
}

// refusal 把非 200 的响应读成 *PoolError。
//
// **这是信任边界**：错误体和 Retry-After 都是外部输入，而 code 决定「要不要停取票」、
// retry_after 决定「停多久」。所以 code 过闭集白名单，退避时长钳进 [0, MaxRetryAfter]。
// 解不开 / 没报码时返回一个只有状态码的 *PoolError —— 仍然 Unwrap 到 ErrNoSlot，
// 消费端照旧 fail-closed，只是没有分流依据。
func refusal(resp *http.Response) error {
	out := &PoolError{Status: resp.StatusCode}
	var payload struct {
		Error struct {
			Code              string  `json:"code"`
			RetryAfterSeconds float64 `json:"retry_after_seconds"`
		} `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload) == nil {
		if code := sanitizeOpaque(payload.Error.Code, maxCodeLen); code != "" {
			if _, ok := knownCodes[code]; ok {
				out.Code = code
			}
		}
		if seconds := payload.Error.RetryAfterSeconds; seconds > 0 {
			out.RetryAfter = clampDuration(time.Duration(seconds)*time.Second, 0, MaxRetryAfter)
		}
	}
	// 体里没给就读头（契约说两者同时出现；体被截断 / 解不开时头还在）。只认秒数形态——
	// Retry-After 的 HTTP-date 形态依赖双方时钟，这里不需要那点精度。
	if out.RetryAfter == 0 {
		if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds > 0 {
			out.RetryAfter = clampDuration(time.Duration(seconds)*time.Second, 0, MaxRetryAfter)
		}
	}
	return out
}

// Gateways 列出池子眼里的网关，给消费端自己挑一个落点。
//
// 调度仍在池子那边，这里只读它的账：**挑不挑得出来都不影响能不能取到票**，列不出来就裸取
// （由池子按调度选）。所以任何失败都原样返回错误让调用方退化，不包装成一个「空列表」假装成功。
//
// account 与 Cookie 的那个同义、同样必须带：used_by_you / last_used_at 报的是**那个上游账号的**
// 槽位历史，不报就变成上传者的历史（见 Cookie 的说明）。空串 = 按上传者算。
func (c *Client) Gateways(ctx context.Context, account string) ([]Gateway, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: client is nil", ErrPool)
	}
	endpoint := c.endpoint("gateways")
	if account = strings.TrimSpace(account); account != "" {
		endpoint += "?" + url.Values{"account": {account}}.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: build gateways request: %w", ErrPool, err)
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: gateways request returned HTTP %d", ErrPool, resp.StatusCode)
	}
	var payload struct {
		// Account 是池子回显的「这次是替谁问的」，用来自检有没有报对账号。
		Account  string `json:"account"`
		Gateways []struct {
			Name      string `json:"name"`
			PairReady bool   `json:"pair_ready"`
			UsedByYou bool   `json:"used_by_you"`
			// 收成字符串再自己解：池子在「没碰过」时给的是缺省，但给成空串 / null 时
			// time.Time 会连带让**整份列表**解码失败，而这个字段只用来排序。
			LastUsedAt string `json:"last_used_at"`
		} `json:"gateways"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxListBytes)).Decode(&payload); err != nil {
		return nil, fmt.Errorf("%w: decode gateways response: %w", ErrPool, err)
	}
	// 自检：回显的身份必须就是报上去的那个，不然 used_by_you / last_used_at 是**别人的**历史，
	// 拿它挑落点等于瞎挑。回显为空 = 池子还没报这个字段，不作数（标识不进错误串）。
	if account != "" && payload.Account != "" && payload.Account != account {
		return nil, fmt.Errorf("%w: gateways response answered for another account", ErrPool)
	}
	gateways := make([]Gateway, 0, len(payload.Gateways))
	for _, item := range payload.Gateways {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue // 没名字就点不了名。
		}
		// 解不开当没碰过（零值）：排序用的字段不值得为格式问题废掉整张列表。
		lastUsedAt, _ := time.Parse(time.RFC3339, strings.TrimSpace(item.LastUsedAt))
		gateways = append(gateways, Gateway{
			Name: name, PairReady: item.PairReady,
			UsedByYou: item.UsedByYou, LastUsedAt: lastUsedAt,
		})
	}
	return gateways, nil
}

// do 挂上 consumer key 并发请求。consumer key 只在这里出现一次，且只进 Authorization 头：
// net/http 的传输错误包成 *url.Error，里面的 URL 已被 stripPassword 处理，头不会进错误串。
func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.consumerKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.consumerKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// net/http 把失败包成 *url.Error，里面带**整条 URL**，而 /cookie 的 query 含
		// account=<上游 chatgpt_account_id>、exclude、exclude_versions。这个错误会随消费端的
		// Ops 错误日志落库，而那边的脱敏正则只盖 key/client_secret/token 那几个词 ⇒ 身份原样
		// 进日志。就地剥掉 query（这个错误是本函数这条链刚造出来的，没有别处在读它），
		// 剥 query 而不是只保留 Err：ErrPool 之外的调用方还靠 *url.Error 的字面特征分类传输故障。
		var transport *url.Error
		if errors.As(err, &transport) {
			transport.URL = stripQuery(transport.URL)
		}
		return nil, fmt.Errorf("%w: request failed: %w", ErrPool, err)
	}
	return resp, nil
}

// stripQuery 去掉 query 与 fragment，解不开就退回主机前缀（宁可少给信息也不泄露身份）。
func stripQuery(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		if scheme, _, found := strings.Cut(raw, "?"); found {
			return scheme
		}
		return raw
	}
	parsed.RawQuery, parsed.Fragment = "", ""
	return parsed.String()
}
