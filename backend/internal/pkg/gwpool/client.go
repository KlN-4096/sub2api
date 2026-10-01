// Package gwpool 是网关池（E:/Project/GO/gwpool，SPEC.md 第 10 节）的消费端 HTTP 客户端。
//
// 池子负责发现 Codex 后端网关、维护活的路由 pair（__cflb + __oailb）并下发。本包做两件事：
// 列网关（GET /gateways）和取一张 pair（GET /cookie）。满血验证、续期全在池子那边，这里不
// 复制任何判据；列表只用来**挑落点**——按 (上游账号 × 网关) 算的「烧过没」只有消费端知道。
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
	// maxListBytes 是 /gateways 的上限。cookie 那 4 KiB 对一张列表太紧，而截断会让整份 JSON
	// 解不开（= 挑不出网关，退回池子自己挑），所以这里给宽一点。
	maxListBytes = 64 << 10
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
// account 是**这一发真正要用的那个上游账号**（access_token 里的 chatgpt_account_id）。
// 一把 consumer key 可以替多个上游账号取票，而满血窗口是 (上游账号 × 网关) 的 ⇒ 不报它的话
// 池子会把槽位记在**上传者**那一行上，白白划掉一个对本账号还满血的落点。空串 = 按上传者记。
// 跨账号取到的票 `verified_full` 恒为 false（池子手上没有那个账号的凭据，验不了），
// 这是契约里的硬限制，**不是丢弃这张票的理由**。
//
// force 表示「我现在租着的那张不行了」：池子保证给一个**不同**的网关，换不出来就 503
// （不会把原来那张再发一遍）。调用方自己判什么时候该 force，池子不操心。
//
// 池子回 503 时返回 ErrNoSlot；其余非 200 与传输错误都返回错误，一律由调用方按失败处理。
// **不在这里重试**：force 失败就是失败，自动重试会把池子供给烧干。
func (c *Client) Cookie(ctx context.Context, account, gateway string, force bool) (Pair, error) {
	if c == nil {
		return Pair{}, fmt.Errorf("%w: client is nil", ErrPool)
	}
	query := url.Values{}
	if account = strings.TrimSpace(account); account != "" {
		query.Set("account", account)
	}
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
		return nil, fmt.Errorf("%w: request failed: %w", ErrPool, err)
	}
	return resp, nil
}
