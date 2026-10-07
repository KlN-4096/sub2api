package service

// Turn-State pair 模式（**实验性·未完善**，2026-09-26 加）。
//
// 由来：spumon1/SUCK_MY_ASTRA 的默认形态——票不在业务出口上铸，而是在一个**新鲜出口**上铸，
// 然后连带铸票那次响应的路由 cookie（__cflb / __oailb）一起回放到业务流量上。本机
// 2026-09-26 在 pro1 上实测：新鲜出口铸的票 + 同行 pair 回放到本机出口，上游不重铸就答对
// 糖果题（2/2），重铸就答错（8/8）；本出口自己铸的票 0/2 被接受。**读数可用，不是解法**
//（memory suck-my-astra-review）。所以这套东西标实验性，别当降智解药用。
//
// 与旧猎手路径的区别只有两条：
//
//   - **判据只认做题。** 2026-09-23 起上游把 turn-state 一律铸成 780 字符，292/312/332 的
//     长度判据全部失效（openAITurnStateHealthy 在 780 上恒 false），模型标签也开始说谎。
//     pair 探测因此**直接发糖果题**、读完整个回答，答案含 "21" 才算这张票值得入池；票长只
//     作信息记录。不再「先发 hi 铸票、再发题验票」——一次请求同时拿到票、pair 和判据，省
//     一半额度（用户 2026-09-26 定）。
//     **标定范围**：这道题只在 gpt-5.6-sol@medium 上验过恒答 21。pair 探测按**要猎的模型**
//     出题，本身答不对这道题的弱模型（mini 之类）会稳定判错、永远入不了池。
//
//   - **票带着 pair 和自己的有效期入池。** pair 回放的有效窗口比票的 1 小时寿命短得多，
//     具体多短由用户自己实测（ticket_ttl_seconds，默认 240 秒），与账号级的
//     openai_turn_state_stale_after_minutes 分开算。
//
// 刻意**不做**的事：上游在注入后又铸了新票，只在候选上记一次计数给页面看——不判这张票失效、
// 不停账号调度。「重铸」是账号权重的读数，不是票坏了的证据，这条判据 2026-09-19 已经错过一次
//（见 observeOpenAITurnStateMint 末尾的长注释）。票坏了的硬证据仍然只有上游回
// invalid_encrypted_content 这一个。

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	// openAITurnStatePairCookieURL 是把 pair 种回罐里时用的地址。罐只看 scheme + 主机
	// （openAICodexCookieURL / chatgptcookies.IsChatGPTHost），写成真实出站地址是为了
	// 一眼看出这批 cookie 会在哪条请求上被带出去。自定义 base URL 的账号匹配不上，
	// 那种账号 pair 模式本来也不适用（猎手只对直连 ChatGPT 的 oauth / setup-token 开）。
	openAITurnStatePairCookieURL = "https://chatgpt.com/backend-api/codex/responses"
	// openAITurnStatePairCookieKeep 是一条候选最多带几个 cookie；
	// openAITurnStatePairCookieValueKeep 是单条的长度上限。pair 要进 extra（随账号列表接口
	// 下发、进每份 DB dump），得有界。实测 __cflb 约 40 字符、__oailb 是 ES256 JWT 约 300。
	openAITurnStatePairCookieKeep      = 2
	openAITurnStatePairCookieValueKeep = 1024
)

// openAITurnStatePairCookieNames 是随票回放的 cookie 白名单：只有钉路由的那两个。
//
// 刻意不取 chatgptcookies 的整张白名单：__cf_bm 之类是 Cloudflare 按 TLS 指纹 + IP 发的
// 机器人管理令牌，从另一个出口回放它只会自相矛盾。SUCK_MY_ASTRA 合并的也正是这两个。
var openAITurnStatePairCookieNames = map[string]struct{}{
	"__cflb":  {},
	"__oailb": {},
}

// IsOpenAITurnStatePairModeEnabled 报告账号开着 pair 模式的猎手。
func (a *Account) IsOpenAITurnStatePairModeEnabled() bool {
	if a == nil || !a.IsOpenAIOAuthLike() {
		return false
	}
	cfg, ok := readOpenAITurnStateHunterConfig(a)
	return ok && cfg.Enabled && cfg.PairMode
}

// openAITurnStatePair 是随票一起入池的东西：铸票那次响应的路由 cookie，以及这条票自己的
// 有效期（秒）。
type openAITurnStatePair struct {
	Cookies    []string
	TTLSeconds int
}

// openAITurnStatePairCookies 从铸票响应里取出要随票回放的 cookie，写成 `name=value`。
//
// 只留 name=value，丢掉 Expires / Max-Age / Domain 这些属性：属性带回罐里会让 cookie 按
// 上游给的窗口自己过期（甚至当场就过期），而这一对的可用窗口由 ticket_ttl_seconds 说了算。
func openAITurnStatePairCookies(resp http.Header) []string {
	if len(resp) == 0 {
		return nil
	}
	cookies := (&http.Response{Header: resp}).Cookies()
	out := make([]string, 0, len(cookies))
	for _, cookie := range cookies {
		if cookie == nil {
			continue
		}
		if _, ok := openAITurnStatePairCookieNames[cookie.Name]; !ok {
			continue
		}
		value := cookie.Name + "=" + cookie.Value
		if len(value) > openAITurnStatePairCookieValueKeep || strings.TrimSpace(cookie.Value) == "" {
			continue
		}
		out = append(out, value)
		if len(out) >= openAITurnStatePairCookieKeep {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// seedOpenAITurnStatePairCookies 把候选带的 pair 种回该账号的 cookie 罐，出站时由
// codexCookies.Attach 写成 Cookie 头（openai_codex_cookies.go）。
//
// 每次注入都种一次，不是只种一次：业务响应自己的 Set-Cookie 会盖掉罐里的值
// （doOpenAIUpstream 收尾处的 Store），不重种下一条请求就只带票、不带 pair。
//
// 走罐而不是直接写 Cookie 头，是因为出站头在 buildUpstreamRequest 里就定了，而 Attach 在
// 更后面无条件 Set 一次 Cookie——写在头上会被它盖掉。
//
// **必须给显式寿命**（ttlSeconds，即这张票的有效期）。入池时属性被剥干净了，不补 Max-Age 就
// 是 session cookie，而罐是进程级的 sync.Map、没有任何删除或过期入口（chatgptcookies.Jar 只有
// 存/取）：那样一来 pair 会钉在这个账号的**所有** chatgpt.com 出站上——没注入票的普通请求、
// 注入了非 pair 候选的请求、WS 握手、额度侧信道——而且关掉 pair 模式、关掉猎手、关掉自动接管
// 都停不下来，只有重启进程或上游主动下发同名 cookie 才会变。`__oailb` 钉的是后端网关主机，
// 等于把猎手那个出口的网关永久钉给真实流量。带上 Max-Age 之后，回放窗口的唯一权威就是
// ticket_ttl_seconds：票一过期 cookiejar 自己把这对丢掉。
//
// 代价（已知、接受）：在票的有效期内，同账号那些**没**注入这张票的请求也会带上这对 cookie
// （罐是按账号一只，Attach 无条件）。真实 Codex 客户端本身就是一只进程级罐无条件回放，且
// 2026-09-25 直连实测 cookie 回放不改变服务质量，所以不为此再拆一条按请求的通道。
func (s *OpenAIGatewayService) seedOpenAITurnStatePairCookies(account *Account, cookies []string, ttlSeconds int) {
	if s == nil || account == nil || len(cookies) == 0 {
		return
	}
	if ttlSeconds <= 0 {
		ttlSeconds = defaultOpenAITurnStateHuntTicketTTL
	}
	maxAge := "; Max-Age=" + strconv.Itoa(ttlSeconds)
	header := http.Header{}
	for _, cookie := range cookies {
		name, _, ok := strings.Cut(cookie, "=")
		if !ok {
			continue
		}
		// 池里的脏数据（手改过 extra、旧版本写下的别的 cookie）不往罐里放。罐自己也有一层
		// 白名单，这里这一道是为了「pair 只回放钉路由的那两个」这条规矩不靠罐来兜。
		if _, allowed := openAITurnStatePairCookieNames[strings.TrimSpace(name)]; !allowed {
			continue
		}
		// 显式补 Path=/：属性在入池时被丢掉了（openAITurnStatePairCookies），不补的话罐会按
		// 「铸票地址的目录」当默认作用域，出站打 /responses/compact、/images、WS 这些别的路径
		// 时 pair 就静默漏掉了。Cloudflare 自己发的也是 Path=/。Max-Age 见函数注释。
		header.Add("Set-Cookie", cookie+"; Path=/"+maxAge)
	}
	if len(header) == 0 {
		return
	}
	s.codexCookies.Store(account, openAITurnStatePairCookieURL, header)
}

// noteOpenAITurnStatePairRemint 记「注入这张票之后上游又铸了一张新的」。
//
// **只是读数**：计数写在候选上给页面看，不判失效、不停号（见文件头）。找不到那条候选就
// 什么都不做——它可能已经被别的 session 挤出池子或自然过期了，为一条读数补写没有意义。
func (s *OpenAIGatewayService) noteOpenAITurnStatePairRemint(c *gin.Context, account *Account, injected string) {
	if s == nil || account == nil || injected == "" || !account.IsOpenAITurnStatePairModeEnabled() {
		return
	}
	ctx := turnStateOpCtx(c)
	mu := openAITurnStatePoolLock(account.ID)
	mu.Lock()
	defer mu.Unlock()

	pool := s.loadOpenAITurnStatePoolFresh(ctx, account)
	for i := range pool {
		if pool[i].Blob != injected {
			continue
		}
		pool[i].Reminted++
		s.persistOpenAITurnStatePool(c, account, pool)
		slog.Info("openai_turn_state_pair_remint", "account_id", account.ID,
			"model", pool[i].Model, "reminted", pool[i].Reminted)
		return
	}
}
