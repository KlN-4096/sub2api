package service

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	gocache "github.com/patrickmn/go-cache"
	"github.com/tidwall/gjson"
)

// 真客户端除推理外还会打账号面的只读 GET，网关此前一条都不发，上游看到的是一个
// 「只有 /responses 的账号」——真设备不存在这种形态：
//
//	GET /backend-api/wham/settings/user   每个线程首个请求查一次。git 归属策略在世界状态
//	    贡献阶段读它（16ff14c: ext/git-attribution/src/policy.rs:52-102，线程级缓存、未命中
//	    才查），请求带 cache-control: no-cache, no-store（backend-client/src/client.rs:486-498）。
//
// 刻意不发 /backend-api/wham/config/bundle：它唯一的调用方 cloud-config 先过
// cloud_config_eligible_auth（cloud-config/src/service.rs:50-58），要求 plan 是
// business_like / education_like / Enterprise（protocol/src/account.rs:67-80，Team 都被排除），
// Plus / Pro / Free 账号的真客户端一辈子不发这个端点。对典型双开账号（个人 Plus/Pro）补发它
// 是凭空多一个特征，与本模块的目的相反。
//
// 头集合与真客户端 backend-client 的 headers()（backend-client/src/client.rs:245-265）一致：
// user-agent / authorization / chatgpt-account-id，FedRAMP 账号再加 x-openai-fedramp；
// 不发 originator 与 version——那两个来自推理面的 OpenAI Provider，backend-client 不走它。
//
// 只对双开账号发。响应体直接丢弃：网关不消费这个端点的内容，发它只是让账号的出站流量形态
// 与真客户端一致。任何失败都只记日志，不影响正在进行的推理请求。
const (
	chatGPTSettingsUserURL = "https://chatgpt.com/backend-api/wham/settings/user"

	// codexSideCallTimeout 单次侧信道请求超时。侧信道与推理共用账号的连接池
	// （账号隔离下 maxConnsPerHost = account.Concurrency，repository/http_upstream.go
	// resolvePoolSettings），所以超时要短：并发额度极小的账号上，一条挂住的侧信道会占着
	// 一个连接槽。OpenAI 默认走 H2（resolveProtocolMode → openai_h2），多路复用下占不到槽；
	// 只有 openai_h1 / openai_h1_fallback 两条路径上 MaxConnsPerHost 是硬上限会排队，
	// 而 account.Concurrency 的库内默认值是 3（ent/schema/account.go），不是注释曾写的 10/16。
	// 刻意不给侧信道换 concurrency 或 profile 去要独立池：cacheKey 不含池配置、
	// poolKey 含，换了会让同一账号的客户端在两套配置间反复重建。
	codexSideCallTimeout = 10 * time.Second
	// codexSideThreadTTL 线程去重窗口。gocache.Add 命中时直接返回错误、不续期，所以这是
	// 「首见后固定 2 小时」而不是滑动窗口：连续活跃超过 2 小时的线程会再查一次
	// （真客户端把策略缓存在 thread_store 里活整个线程，ext/git-attribution/src/lib.rs:33-90）。
	// 长线程偶尔多一条 GET 与真客户端换新线程的形态没有区别，不值得为续期自己扫表。
	codexSideThreadTTL = 2 * time.Hour
	// codexSideThreadMaxEntries 去重窗口的条目上限，防客户端用一次性 thread-id 撑内存。
	// 一条约 100 字节，10 万条 ~10 MB；真实形态下一个账号同时活跃的线程是个位数。
	codexSideThreadMaxEntries = 100000
	// codexSideCallResponseLimit 丢弃响应体时的读取上限。超过就直接关连接（连接不再复用），
	// 不是"取消传输"——上游已经发出的字节还在路上。
	codexSideCallResponseLimit = 1 << 20
)

// codexSideCallState 持有线程去重窗口。gocache.Add 在键仍存在时返回错误，正好用作
// 「首次出现」的原子判定，同时自带 TTL 清理，不需要自己扫表。
type codexSideCallState struct {
	threadSeen *gocache.Cache
}

func newCodexSideCallState() *codexSideCallState {
	return &codexSideCallState{threadSeen: gocache.New(codexSideThreadTTL, 10*time.Minute)}
}

// codexSideCallHeaderNames 是真客户端 backend-client 会发的头，逐个从已定稿的推理请求上取，
// 保证侧信道与推理面自报同一个客户端、同一个账号。
var codexSideCallHeaderNames = []string{"authorization", "chatgpt-account-id", "user-agent", "x-openai-fedramp"}

func codexSideCallHeaders(src http.Header) http.Header {
	out := make(http.Header, len(codexSideCallHeaderNames))
	for _, name := range codexSideCallHeaderNames {
		if value := src.Get(name); strings.TrimSpace(value) != "" {
			out.Set(name, value)
		}
	}
	return out
}

func codexSideThreadKey(accountID int64, threadID string) string {
	return strconv.FormatInt(accountID, 10) + "|" + threadID
}

// scheduleCodexSideCalls 在出站 /responses 请求定稿之后、真正发出之前调用，按线程首见的节奏
// 异步补一条只读 GET。刻意留在这里而不是等响应回来：真客户端的 settings/user 发生在构造世界
// 状态时，严格早于 POST（ext/git-attribution/src/lib.rs:32-90 是 contribute_world_state 贡献者），
// 挪到响应之后与它模仿的对象顺序相反。换号重试时每个账号各查一次同样正确——去重键是
// (被转发的账号行, 线程)，每一行对应一台"设备"，两台设备各查一次是真实形态；同一母账号
// 下的多条影子行会各查一次，那正是多设备共用一份凭据时上游本来就会看到的。
//
// req 只用于读取已定稿的身份头，不会被改动。构造器未初始化去重窗口时（单元测试里的裸结构体）
// 整体停用，出站字节与调用前一致。
func (s *OpenAIGatewayService) scheduleCodexSideCalls(c *gin.Context, account *Account, req *http.Request) {
	// 只有常规推理请求代表「线程有活动」；compact / 搜索 / models 不触发。
	if req == nil || req.Method != http.MethodPost || req.URL == nil ||
		!strings.HasSuffix(strings.TrimRight(req.URL.Path, "/"), "/responses") {
		return
	}
	s.scheduleCodexSettingsUser(c, account, req.Header, req.Header.Get("thread-id"))
}

// 长连接的后续帧不一定重建握手头，线程必须取本帧最终身份，不能读首轮握手的旧 thread-id。
func (s *OpenAIGatewayService) scheduleCodexWSSideCalls(c *gin.Context, account *Account, headers http.Header, payload []byte) {
	if gjson.GetBytes(payload, "type").Str != "response.create" {
		return
	}
	threadID := headers.Get("thread-id")
	if thread := gjson.GetBytes(payload, "client_metadata.thread_id"); thread.Exists() {
		if thread.Type != gjson.String {
			return
		}
		threadID = thread.Str
	}
	s.scheduleCodexSettingsUser(c, account, headers, threadID)
}

// HTTP 与三条 WS 共用身份门控、去重和出口。调用前身份已定稿，不再读取入站会话别名。
func (s *OpenAIGatewayService) scheduleCodexSettingsUser(c *gin.Context, account *Account, src http.Header, threadID string) {
	if s == nil || s.codexSideCalls == nil || s.httpUpstream == nil || account == nil {
		return
	}
	if !codexDeviceWireProfileEnabled(c, account) {
		return
	}
	headers := codexSideCallHeaders(src)
	source := codexAccountIdentitySource(c, account)
	if headers.Get("authorization") == "" && (source == nil || !source.IsOpenAIAgentIdentity()) {
		return
	}
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return
	}
	// 去重窗口的基数由客户端决定：thread-id 每请求换一个的客户端会让条目无上限增长，
	// 同时把账号面请求量翻倍——正是这个功能要消除的异常形态。到顶就停发，宁可少一条
	// 装饰性 GET，也不让客户端拿它撑内存。
	if s.codexSideCalls.threadSeen.ItemCount() >= codexSideThreadMaxEntries {
		return
	}
	key := codexSideThreadKey(account.ID, threadID)
	if err := s.codexSideCalls.threadSeen.Add(key, true, gocache.DefaultExpiration); err != nil {
		return // 这个线程已经查过
	}
	// 传副本而不是账号本体：goroutine 活过本次请求，而请求路径上还会改同一个对象
	// （agent identity 会替换 Credentials）。当下字段不相交，但让后台协程持有可变的请求态
	// 对象，安全性只靠"插件路由现在恰好不读那些字段"这一个偶然事实。
	snapshot := *account
	snapshot.Credentials = maps.Clone(account.Credentials)
	s.dispatchCodexSideCall(chatGPTSettingsUserURL, headers, codexWireTimezoneProxyURL(account), &snapshot, key)
}

// dispatchCodexSideCall 异步发一次 GET 并丢弃响应体，不阻塞推理请求。
// 走 doOpenAIUpstream 而不是 httpUpstream.Do：装了 OAuth 能力插件的部署里，推理面由插件
// 接管出站，侧信道必须与它陪跑的 /responses 走同一条传输栈，否则两者从不同出口发出。
func (s *OpenAIGatewayService) dispatchCodexSideCall(
	url string, headers http.Header, proxyURL string, account *Account, key string,
) {
	go func() {
		started := false
		// 裸 goroutine 不在 middleware.Recovery 的保护范围内，这里 panic 会带走整个进程。
		// 侧信道是"发出去就不管"的装饰性请求，任何异常都不该影响正在服务的请求。
		defer func() {
			if !started {
				// 认证/构造失败时还没有发出请求，下一轮允许重试，不占住两小时窗口。
				s.codexSideCalls.threadSeen.Delete(key)
			}
			_ = recover()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), codexSideCallTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return
		}
		// 池化 WS 的 Agent Identity assertion 只在拨号时注入，复用连接也不会回填
		// 构造器头。GET 独立复用现有认证逻辑；普通 OAuth 只是克隆原来的定稿头。
		refreshed, err := s.refreshOpenAIAgentIdentityHeaders(ctx, account, headers)
		if err != nil {
			slog.Debug("codex_side_call_auth_failed", "account_id", account.ID, "error", err)
			return
		}
		req.Header = refreshed
		// 真客户端对 settings/user 显式禁缓存（backend-client/src/client.rs:486-498）。
		req.Header.Set("cache-control", "no-cache, no-store")
		req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))
		started = true
		resp, err := s.doOpenAIUpstream(req, proxyURL, account)
		if err != nil || resp == nil {
			slog.Debug("codex_side_call_failed", "url", url, "account_id", account.ID, "error", err)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, codexSideCallResponseLimit))
	}()
}
