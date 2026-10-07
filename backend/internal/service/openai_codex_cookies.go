package service

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/chatgptcookies"
	"golang.org/x/sync/singleflight"
)

// openAICodexCookieStore：推理面的 ChatGPT cookie 回放，按账号各一只罐。
//
// 真实 Codex ≥0.156.1 用进程级罐在 HTTP 与 WSS 握手上回放 __cflb / __oailb / __cf_bm 等
// （http-client/src/chatgpt_cloudflare_cookies.rs、websocket-client/src/lib.rs）。一个网关进程
// 服务多个账号，共用一只罐会把两个账号钉到同一条路由 / 同一个 Cloudflare 会话上，所以按
// account.ID 隔离；主机与名字的过滤在 chatgptcookies 里。只对本地持有 ChatGPT 凭据的账号
// 生效（IsOpenAIOAuthLike）：cpr 走原样中继、API Key 账号的上游不是 chatgpt.com。
//
// 2026-09-25 直连实测：__oailb 是钉后端网关主机的 ES256 JWT（1 小时），回放与否不改变服务
// 质量——这里对齐的是真实客户端的形态，不是治降智的手段。
type openAICodexCookieStore struct {
	jars sync.Map // openAICodexCookieJarKey(account) → *chatgptcookies.Jar

	// 网关池接管（openai_gwpool.go）。配置全在账号 extra 上，客户端按 (base_url, consumer key)
	// 缓存——每请求新建 gwpool.New 会各带一个 http.Transport，连接池永不复用。
	poolClients sync.Map // base_url + "\x00" + consumer key → *gwpool.Client
	// identity 解析凭证域身份（影子行按母账号算），是 pair 缓存键。
	// 由构造器注入；裸结构体（单元测试）里为 nil，退回按本地行算。
	identity       openAICodexCredentialIdentity
	poolPairs      sync.Map // 凭证域身份 → openAIGatewayPoolPair
	poolFetch      gatewayPoolSharedWork[gatewayPoolFetchResult]
	poolEarlyAt    sync.Map // credential domain -> last durable early reservation
	poolEarlyLocks sync.Map // ledger domain (may span distinct pair-cache identities) -> *sync.Mutex
	poolEarlySkips sync.Map // account ID -> last throttled skip reason
	poolEarlyClaim func(context.Context, *Account, string, time.Time) error
	// poolVerified 是「这个身份手上那张票**验过是满血**」（openai_gwpool_warm.go）。
	//
	// 必须单独记，**不能拿 poolPairs 的 Live 当「验过」**：Live 的唯一含义是取票那一刻写的
	// route credential未到期且未被拒绝，和验没验过是两件事。两条路都会留下一张 Live 的未验票 ——
	// 并发取票（singleflight 的 shared 对领头者也为真 ⇒ 谁都拿不到还票闭包），以及同一份凭据
	// 挂在多个账号行上（键是凭证域身份，别的行取的票这一行照样看得见）。换票即失效。
	//
	// 判出满血的时刻用于观测窗口时长（openai_gwpool_window.go）。
	poolVerified            sync.Map // 凭证域身份 → gatewayPoolVerifiedMark
	poolProbeObserved       func(context.Context, *Account, gatewayPoolProbeObservation)
	poolContactLocks        sync.Map // 凭证域摘要 -> *sync.Mutex；只协调本进程。
	poolContactPicks        sync.Map // 凭证域摘要 -> *atomic.Uint64；每五次保留一次原顺序探索。
	poolDatacenterCountries sync.Map // pool base URL + gateway -> historical datacenter country
	poolFeedbackPolicies    sync.Map // binding+gateway -> latest reporting-policy observation
	poolFeedbackPolicyPrune atomic.Int64
	poolRounds              gatewayPoolRounds
	poolRestLocks           sync.Map // ledger tag -> *sync.Mutex, serialized recovery decisions
	poolRestState           sync.Map // ledger tag -> gatewayPoolRestState; includes fail-closed pending writes
	poolUsageLocks          sync.Map // ledger tag -> *sync.Mutex, durable usage rounds
	poolUsageCache          sync.Map // ledger tag -> *gatewayPoolUsageLedger, immutable committed snapshot
	poolUsageSessionOnce    sync.Once
	poolUsageSession        string
	poolUsageAttempt        func(context.Context, *Account, string, string, OpenAIGatewayPoolApplied, time.Time, bool)
	poolUsageFinished       func(context.Context, *Account)
	poolUsageSettle         func(context.Context, *Account, string)
	poolProgress            gatewayPoolProgressTracker
	poolRotationAccounts    sync.Map // account ID -> fresh opt-in observed during preference hydration
	// poolWarm 把同一张票上的并发预热判据收口成一次（openai_gwpool_warm.go）。
	// 取票本身已经被 poolFetch 收成一次 ⇒ 同身份的并发请求手里是**同一张票**、同一个
	// (上游账号 × 网关) 单元；跨模型共享 A/B，各自取消不影响其它等待者，
	// 而供给只有个位数张/小时。键按票号分，换了票要重新验。
	poolWarm            gatewayPoolProbeFlights
	poolPrepare         gatewayPoolSharedWork[struct{}]
	poolPrepareProgress sync.Map // full credential identity -> last shared *gatewayPoolProgressRun
	poolCandidateQueues sync.Map // full credential identity -> *gatewayPoolCandidateQueue
	// poolUsed 是「这个凭证域身份最近碰过哪些网关」的本地账本，用来挑一个没烧过的落点，
	// 并作为 /cookie 的 exclude 带给池子（裸取时也能避开烧过的落点）。
	// 池子按它发的 consumer key 记账，认不出同一份凭据挂在多个账号行上（见 gatewayPoolLedgerKey）。
	poolUsed               sync.Map // 凭证域身份 + "\x00" + 网关名 → time.Time
	poolCooldownMu         sync.Mutex
	poolCooldown           map[string]gatewayPoolCooldown
	poolCooldownReset      sync.Map // ledger identity -> last committed reset barrier; writes hold poolCooldownMu
	poolCooldownClear      sync.Map // ledger identity -> last durable manual clear; history stays intact
	poolCooldownResetLocks sync.Map // ledger identity -> *sync.Mutex; timer writes only
	poolManualRetry        sync.Map // ledger identity -> an active administrator-triggered preparation
	poolRecommendations    sync.Map // ledger identity × gateway → gatewayPoolRecommendation
	poolCooldownPersist    func(context.Context, *Account, string)
	poolHistoryLocks       sync.Map // 本地行 ID → *sync.Mutex，串行合并已有 history。
	accountByID            func(context.Context, int64) (*Account, error)
	historyByTag           func(context.Context, string) ([]Account, error)
	poolHistoryLoad        singleflight.Group
	// poolBackoff 是「池子让这个身份先别取票」的到点（见 gatewayPoolBackoff）。
	// 按**上游账号**记而不是按请求：退避只对本次请求生效的话，重试环照旧每轮都去敲池子。
	poolBackoff sync.Map // 账本身份（上游账号粒度）→ time.Time
	// poolSpare 是批量取票剩下的备用票（/cookie?count=，见 gatewayPoolTakeBatch）。
	//
	// 验满血那条路的形状是「取一张 → 验 → 不满血再取一张」，每轮一个 HTTP 往返，而池子内部
	// 可能顺带现铸（取票超时默认 25s），三轮就能把 90 秒的预热预算花光在往返上。一发拿 N 张
	// 之后第 2..N 轮**一个往返都不用打**。
	//
	// 架子上这张不是浪费而是预取：它在自己的满血窗口内对**后面的**业务请求一样有效。
	// 窗口过了还没人用才算损失，而那只发生在这个身份突然没请求的时候。
	// 键按凭证域身份，和 poolPairs 同一个口径（同一份凭据的几个账号行共用）。
	poolSpare     sync.Map // 凭证域身份 → *gatewayPoolTicketBatch
	poolInventory sync.Map // 凭证域身份 → *gatewayPoolInventoryState
}

// openAICodexCookieJarKey：本地行 ID + 凭证域身份。同一行重新授权成另一个 ChatGPT 身份时
// 旧身份的 __oailb（钉后端网关主机）不能跟着新身份出站，所以身份变了就换罐；只按身份不按行则
// 缺 chatgpt_user_id 的工作区会共用一只罐。
func openAICodexCookieJarKey(account *Account) string {
	return strconv.FormatInt(account.ID, 10) + "|" + codexAccountIdentityNamespace(account)
}

func (s *openAICodexCookieStore) jar(account *Account) *chatgptcookies.Jar {
	key := openAICodexCookieJarKey(account)
	if v, ok := s.jars.Load(key); ok {
		if jar, ok := v.(*chatgptcookies.Jar); ok {
			return jar
		}
	}
	fresh := chatgptcookies.NewJar()
	if v, loaded := s.jars.LoadOrStore(key, fresh); loaded {
		if jar, ok := v.(*chatgptcookies.Jar); ok {
			return jar
		}
	}
	return fresh
}

func openAICodexCookiesApply(account *Account) bool {
	return account != nil && account.IsOpenAIOAuthLike()
}

// openAICodexCookieURL 把出站地址归一到罐认的形态：罐只认 https；WS 握手的 wss:// 与 HTTP
// 是同一台主机的同一批 cookie。其它 scheme 返回 nil。
func openAICodexCookieURL(raw string) *url.URL {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u == nil {
		return nil
	}
	switch strings.ToLower(u.Scheme) {
	case "https", "wss":
		clone := *u
		clone.Scheme = "https"
		return &clone
	default:
		return nil
	}
}

// Attach 把该账号罐里适用于 rawURL 的 cookie 写成 headers 的 Cookie 头。覆盖而不追加：入站
// Cookie 本就不在任何出站白名单里，出站不该带客户端的值。罐为空时不写头。
func (s *openAICodexCookieStore) Attach(account *Account, rawURL string, headers http.Header) {
	if s == nil || headers == nil || !openAICodexCookiesApply(account) {
		return
	}
	parts := s.jarCookieParts(account, rawURL)
	if len(parts) == 0 {
		return
	}
	headers.Set("Cookie", strings.Join(parts, "; "))
}

// jarCookieParts 取罐里适用于 rawURL 的 cookie，写成 "name=value"。网关池接管时也要读它：
// 池子只负责 __cflb / __oailb 两项，__cf_bm 之类本出口自己的 Cloudflare 令牌仍从罐里带。
func (s *openAICodexCookieStore) jarCookieParts(account *Account, rawURL string) []string {
	if s == nil || !openAICodexCookiesApply(account) {
		return nil
	}
	u := openAICodexCookieURL(rawURL)
	if u == nil {
		return nil
	}
	cookies := s.jar(account).Cookies(u)
	if len(cookies) == 0 {
		return nil
	}
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}
	return parts
}

// Store 把响应（HTTP 响应或 WS 握手响应）头里的 Set-Cookie 收进该账号的罐。
func (s *openAICodexCookieStore) Store(account *Account, rawURL string, resp http.Header) {
	if s == nil || len(resp) == 0 || !openAICodexCookiesApply(account) {
		return
	}
	u := openAICodexCookieURL(rawURL)
	if u == nil {
		return
	}
	cookies := (&http.Response{Header: resp}).Cookies()
	if len(cookies) == 0 {
		return
	}
	s.jar(account).SetCookies(u, cookies)
}
