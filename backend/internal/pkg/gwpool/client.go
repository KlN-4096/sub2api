// Package gwpool 是网关池（E:/Project/GO/gwpool，SPEC.md 第 10 节）的消费端 HTTP 客户端。
//
// Lists gateways, acquires one route pair at a time and submits observations.
// Quality verification and local cooldown decisions belong to the service.
//
// 红线：consumer key 只进 Authorization 头，不进日志、不进错误串；cookie 全文同样不进日志。
package gwpool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// Temporary directory failure is unknown supply, never proof of exhaustion.
// Keep it distinct from authentication/protocol errors and ticket shortages.
var ErrCatalogUnavailable = fmt.Errorf("%w: directory temporarily unavailable", ErrPool)

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
	// CodeConsumerRejected is rejection of our consumer key, not the upstream credential.
	CodeConsumerRejected    = "consumer_rejected"
	CodeConsumerRateLimited = "consumer_rate_limited"
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
	CodeDegraded: {}, CodeConsumerRejected: {}, CodeConsumerRateLimited: {}, CodeBadRequest: {},
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
	// Catalogs include contact/recommendation history; ordinary 106-gateway
	// responses already exceed 64 KiB. Bound the whole response, not a JSON prefix.
	maxListBytes = 2 << 20
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
)

// Pair 是 /cookie 的一次下发。
type Pair struct {
	// Gateway 形如 "unified-142"。
	Gateway           string
	DatacenterCountry string
	// Region 是**铸这张票的出口**所属的大区（池子那九个 key 之一，SPEC 第 4 节）。
	// 空 = 池子没报（老版本池子 / 它自己也反查不到），消费端按「未归类」处理，不要猜：
	// 网关 = (大区 × 账号)，同一个网关名在不同账号眼里可能来自不同大区，事后反查不出来。
	//
	// 它**不是**「这一发会落在哪个大区」—— cflb 跨出口回放会漂（docs 第四节），
	// 真实落点只有响应里的新 __oailb 说得准。只做读数，不接任何判定。
	Region string
	// Cookie 是直接写进出站 Cookie 头的整串 "__cflb=...; __oailb=..."。
	Cookie string
	// ValidFor is advisory, never a quality or cookie-expiry decision.
	ValidFor time.Duration
	// Advisory deadline calculated by the pool with unspecified provenance.
	// It must not be used as a hard credential-expiry boundary.
	RouteExpiresAt time.Time
	// PairRemaining 是**这张 pair 自己**的剩余寿命（池子的 pair_remaining_s = __cflb 的死期
	// 减现在）。和 ValidFor 是两根轴：ValidFor = min(满血窗口剩余, pair 剩余)，绝大多数时候
	// 等于那 183 秒的窗口，分不出 pair 还剩 50 分钟还是 8 分钟 ⇒ 判「这张要不要续」只能用这个。
	// 0 = 池子没报（还没升级）。消费端目前只读它做日志口径，不据此做任何动作。
	PairRemaining time.Duration
	// VerifiedFull 是池子交付前自己验过满血。只做读数，消费端不拿它当闸门。
	VerifiedFull bool
	// TTLIsAdvisory：池子声明 valid_for_s 只是建议值，换不换 pair 由消费端自己判。
	// 纯读数——消费端的逻辑本来就是「自己判这张不行了就 force 换一张」，不按这个字段分流。
	TTLIsAdvisory bool
	// Version 是**这一张具体的票**的不透明稳定身份（池子的 cookie_version）。两个用处：
	// Exact-version local cleanup and CookieRequest.ExcludeVersions.
	// Missing or malformed versions cannot support exact-version cleanup.
	Version string
}

// CookieRequest 是取一张 pair 的全部参数。
//
// 为什么是结构体而不是继续加位置参数：七项里五项可选，`Cookie(ctx, acct, gw, true, nil, nil, 0, 0)`
// 在调用点读不出任何意思。
type CookieRequest struct {
	// Account is the complete upstream member identity, used only for rate
	// limiting and telemetry at the pool. Both Account and Gateway are required.
	Account string
	// Gateway is chosen locally; the pool must not substitute another route.
	Gateway string
	// Exclude explicitly rejects named gateways, without delegating selection.
	Exclude []string
	// ExcludeVersions rejects these exact ticket versions.
	ExcludeVersions []string
	// MinRemaining 是能接受的最低剩余寿命。0 = 用池子配的 DeliverFloor。
	MinRemaining time.Duration
}

func (r CookieRequest) query() url.Values {
	query := url.Values{}
	if account := strings.TrimSpace(r.Account); account != "" {
		query.Set("account", account)
	}
	if gateway := strings.TrimSpace(r.Gateway); gateway != "" {
		query.Set("gateway", gateway)
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
// 超长同样判废而不是截断，不能将截断后的 version 用于精确匹配。
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
	// AvailableUntil bounds metadata freshness only, not route quality.
	AvailableUntil    time.Time
	Cooldown          *CooldownRecommendation
	Contacts          []ContactStats
	Priority          *GatewayPriority
	DatacenterCountry string
}

func (g Gateway) ReadyAt(now time.Time) bool {
	return g.PairReady && (g.AvailableUntil.IsZero() || now.Before(g.AvailableUntil))
}

type GatewayPriority struct {
	Model   string `json:"model"`
	Full    int    `json:"full"`
	Samples int    `json:"samples"`
}

func (p *GatewayPriority) Valid(model string) bool {
	return p != nil && p.Model == model && p.Samples >= 5 && p.Samples <= 8192 &&
		p.Full >= 0 && p.Full <= p.Samples
}

func datacenterCountry(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	if len(value) != 2 || value[0] < 'A' || value[0] > 'Z' || value[1] < 'A' || value[1] > 'Z' {
		return ""
	}
	return value
}

// Client 是一个池子实例的客户端。并发安全。
type Client struct {
	base        *url.URL
	consumerKey string
	http        *http.Client
	catalog     catalogCache
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
	if sanitizeOpaque(request.Account, 128) == "" || sanitizeOpaque(request.Gateway, maxGatewayLen) == "" {
		return Pair{}, &PoolError{Code: CodeBadRequest}
	}
	endpoint := c.endpoint("cookie")
	if encoded := request.query().Encode(); encoded != "" {
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
	var payload cookiePayload
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return Pair{}, fmt.Errorf("%w: decode cookie response: %w", ErrPool, err)
	}
	return payload.pair()
}

// cookiePayload is the single-ticket wire response.
type cookiePayload struct {
	Gateway           string    `json:"gateway"`
	DatacenterCountry string    `json:"datacenter_country"`
	Region            string    `json:"region"`
	Cookie            string    `json:"cookie"`
	ValidForS         int       `json:"valid_for_s"`
	VerifiedFull      bool      `json:"verified_full"`
	TTLIsAdvisory     bool      `json:"ttl_is_advisory"`
	CookieVersion     string    `json:"cookie_version"`
	PairRemainingS    int       `json:"pair_remaining_s"`
	RouteExpiresAt    time.Time `json:"route_expires_at"`
}

// pair 校验并转成 Pair。
//
// 这里是信任边界（外部服务的响应 → 要带着该账号的 Authorization 发给 chatgpt.com 的头）：
// 空 cookie 等于裸打（落点不可控），控制字符会劈开出站头；建议窗口不参与拒收。
// cookie 名字的收口在消费侧（只留 __cflb / __oailb），这里只拦明显畸形。
func (p cookiePayload) pair() (Pair, error) {
	if strings.TrimSpace(p.Cookie) == "" {
		return Pair{}, fmt.Errorf("%w: cookie response carried no cookie", ErrPool)
	}
	if strings.ContainsFunc(p.Cookie, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return Pair{}, fmt.Errorf("%w: cookie response carried control characters", ErrPool)
	}
	return Pair{
		Gateway: sanitizeOpaque(p.Gateway, maxGatewayLen),
		// 同样过 sanitizeOpaque：池子的响应是信任边界，这个串会进账号 extra 和前端。
		Region:            sanitizeOpaque(p.Region, maxGatewayLen),
		Cookie:            strings.TrimSpace(p.Cookie),
		ValidFor:          clampDuration(time.Duration(p.ValidForS)*time.Second, 0, maxValidFor),
		RouteExpiresAt:    p.RouteExpiresAt,
		VerifiedFull:      p.VerifiedFull,
		TTLIsAdvisory:     p.TTLIsAdvisory,
		Version:           sanitizeOpaque(p.CookieVersion, maxVersionLen),
		DatacenterCountry: datacenterCountry(p.DatacenterCountry),
		// 和 ValidFor 同样钳进 [0, maxValidFor]：缺失/负数 ⇒ 0 ⇒ 消费端不续（安全方向），
		// 报一个大数被钳到 3900s 仍然远在续期阈值之上 ⇒ 同样不续。
		PairRemaining: clampDuration(time.Duration(p.PairRemainingS)*time.Second, 0, maxValidFor),
	}, nil
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

// Gateways returns a short-lived metadata snapshot. Selection and cooldown are
// local; any failed refresh is unknown supply, never a successful empty list.
func (c *Client) Gateways(ctx context.Context, account string, accountTag ...string) ([]Gateway, error) {
	tag := ""
	if len(accountTag) > 0 {
		tag = accountTag[0]
	}
	return c.GatewaysForModel(ctx, account, tag, "")
}

func (c *Client) GatewaysForModel(ctx context.Context, account, accountTag, model string) ([]Gateway, error) {
	catalog, err := c.Catalog(ctx, account, accountTag, model, 0)
	return catalog.Gateways, err
}

func (c *Client) fetchGateways(ctx context.Context, account, accountTag, model string) ([]Gateway, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: client is nil", ErrPool)
	}
	endpoint := c.endpoint("gateways")
	query := url.Values{}
	if account = strings.TrimSpace(account); account != "" {
		query.Set("account", account)
	}
	if model = sanitizeOpaque(model, 128); model != "" {
		query.Set("model", model)
	}
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: build gateways request: %w", ErrPool, err)
	}
	if validCooldownTag(accountTag) {
		req.Header.Set(cooldownAccountHeader, accountTag)
	}
	req.Header.Set(cooldownMaxHeader, strconv.Itoa(CooldownMaxSeconds))
	started := c.catalog.timeNow()
	resp, err := c.do(req)
	if err != nil {
		return nil, errors.Join(ErrCatalogUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		cause := ErrPool
		if resp.StatusCode >= http.StatusInternalServerError {
			cause = ErrCatalogUnavailable
		}
		return nil, fmt.Errorf("%w: gateways request returned HTTP %d", cause, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxListBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read gateways response: %w", ErrPool, err)
	}
	if len(body) > maxListBytes {
		return nil, fmt.Errorf("%w: gateways response exceeds %d bytes", ErrPool, maxListBytes)
	}
	var payload struct {
		// Account 是池子回显的「这次是替谁问的」，用来自检有没有报对账号。
		Account  string `json:"account"`
		Gateways []struct {
			Name              string                  `json:"name"`
			PairReady         bool                    `json:"pair_ready"`
			ValidForS         int                     `json:"valid_for_s"`
			Cooldown          *CooldownRecommendation `json:"cooldown"`
			Contacts          []ContactStats          `json:"contacts"`
			Priority          *GatewayPriority        `json:"priority"`
			DatacenterCountry string                  `json:"datacenter_country"`
		} `json:"gateways"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("%w: decode gateways response: %w", ErrPool, err)
	}
	if payload.Gateways == nil {
		return nil, fmt.Errorf("%w: gateways response requires an explicit array", ErrPool)
	}
	// 自检：回显的身份必须就是报上去的那个，不然 used_by_you / last_used_at 是**别人的**历史，
	// 拿它挑落点等于瞎挑。回显为空 = 池子还没报这个字段，不作数（标识不进错误串）。
	if account != "" && payload.Account != "" && payload.Account != account {
		return nil, fmt.Errorf("%w: gateways response answered for another account", ErrPool)
	}
	gateways := make([]Gateway, 0, len(payload.Gateways))
	for _, item := range payload.Gateways {
		name := sanitizeOpaque(item.Name, maxGatewayLen)
		if name == "" {
			return nil, fmt.Errorf("%w: gateways response contains an unnamed entry", ErrPool)
		}
		if item.Cooldown != nil && !item.Cooldown.Valid() {
			item.Cooldown = nil
		}
		contacts := make([]ContactStats, 0, len(item.Contacts))
		if !item.Priority.Valid(model) {
			item.Priority = nil
		}
		for _, row := range item.Contacts {
			if row.Gateway == name && row.Valid() {
				contacts = append(contacts, row)
			}
		}
		var availableUntil time.Time
		if item.ValidForS > 0 {
			availableUntil = started.Add(clampDuration(time.Duration(item.ValidForS)*time.Second, 0, maxValidFor))
		}
		gateways = append(gateways, Gateway{
			Name: name, PairReady: item.PairReady,
			AvailableUntil: availableUntil,
			Cooldown:       item.Cooldown,
			Contacts:       contacts,
			Priority:       item.Priority, DatacenterCountry: datacenterCountry(item.DatacenterCountry),
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
