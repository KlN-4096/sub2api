// Package gwpool 是网关池（E:/Project/GO/gwpool，SPEC.md 第 10 节）的消费端 HTTP 客户端。
//
// 池子负责发现 Codex 后端网关、维护活的路由 pair（__cflb + __oailb）并下发。本包只做一件事：
// 取一张 pair（GET /cookie）。调度、满血验证、续期全在池子那边，这里不复制任何判据。
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// ErrNoSlot 是池子 503：没有满血槽位可分配。消费端据此走失败路径，**不得退回降级的 cookie 回放**
// （用户原则：宁可 503 也不放降智）。
var ErrNoSlot = fmt.Errorf("%w: no full-strength slot available", ErrPool)

const (
	// requestTimeout 兜住单次池子调用。池子是同机服务，正常是毫秒级；它卡住不能把业务请求拖死。
	requestTimeout = 5 * time.Second
	// maxResponseBytes 读响应的上限。pair 里 __oailb 是约 300 字符的 JWT，4 KiB 足够。
	maxResponseBytes = 4 << 10
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
	// VerifiedFull 是池子交付前自己验过满血。只做读数，消费端不拿它当闸门。
	VerifiedFull bool
	// TTLIsAdvisory：池子声明 valid_for_s 只是建议值，换不换 pair 由消费端自己判。
	// 纯读数——消费端的逻辑本来就是「自己判这张不行了就 force 换一张」，不按这个字段分流。
	TTLIsAdvisory bool
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
// 端点一律用 url.JoinPath 拼：base_url 带 query 或不带结尾斜杠时字符串拼接会把路径拼坏。
//
// 显式给 Transport 而不是用 http.DefaultTransport：池子通常是 127.0.0.1 上的同机服务，
// 不能让进程的 HTTP_PROXY 把它代理出去。
func New(baseURL, consumerKey string) *Client {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || base == nil || base.Host == "" {
		return nil
	}
	return &Client{
		base:        base,
		consumerKey: strings.TrimSpace(consumerKey),
		http: &http.Client{
			Timeout:   requestTimeout,
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

// Cookie 取一张 pair。gateway 为空则由池子按调度选。
//
// force 表示「我现在租着的那张不行了」：池子保证给一个**不同**的网关，换不出来就 503
// （不会把原来那张再发一遍）。调用方自己判什么时候该 force，池子不操心。
//
// 池子回 503 时返回 ErrNoSlot；其余非 200 与传输错误都返回错误，一律由调用方按失败处理。
// **不在这里重试**：force 失败就是失败，自动重试会把池子供给烧干。
func (c *Client) Cookie(ctx context.Context, gateway string, force bool) (Pair, error) {
	if c == nil {
		return Pair{}, fmt.Errorf("%w: client is nil", ErrPool)
	}
	query := url.Values{}
	if gateway = strings.TrimSpace(gateway); gateway != "" {
		query.Set("gateway", gateway)
	}
	if force {
		query.Set("force", "1")
	}
	endpoint := c.endpoint("cookie")
	if encoded := query.Encode(); encoded != "" {
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
	if resp.StatusCode == http.StatusServiceUnavailable {
		return Pair{}, ErrNoSlot
	}
	if resp.StatusCode != http.StatusOK {
		return Pair{}, fmt.Errorf("%w: cookie request returned HTTP %d", ErrPool, resp.StatusCode)
	}
	var payload struct {
		Gateway       string `json:"gateway"`
		Cookie        string `json:"cookie"`
		ValidForS     int    `json:"valid_for_s"`
		VerifiedFull  bool   `json:"verified_full"`
		TTLIsAdvisory bool   `json:"ttl_is_advisory"`
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
		Gateway:       strings.TrimSpace(payload.Gateway),
		Cookie:        strings.TrimSpace(payload.Cookie),
		ValidFor:      time.Duration(payload.ValidForS) * time.Second,
		VerifiedFull:  payload.VerifiedFull,
		TTLIsAdvisory: payload.TTLIsAdvisory,
	}, nil
}

// do 挂上 consumer key 并发请求。consumer key 只在这里出现一次，且只进 Authorization 头：
// net/http 的传输错误包成 *url.Error，里面的 URL 已被 stripPassword 处理，头不会进错误串。
func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.consumerKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.consumerKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: request failed: %w", ErrPool, err)
	}
	return resp, nil
}
