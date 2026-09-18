package service

// Turn-state 292 猎手。
//
// 背景（memory turn-state-block-count-cause / turn-state-remint-rules）：Codex 上游在每个
// 新会话第一回合按「账号权重 × 当时出口 IP」铸一张 X-Codex-Turn-State，292 字符是正常
// 路由、312 是降智路由；票只在铸造后 3600s 内有效，且只有自然铸造才反映权重。降智账号
// 自己铸的全是 312，候选池只出不进，唯一一张 292 到期后自动接管就永久空窗。猎手的
// 工作就是在票到期前开新会话反复摇骰子：换一个出口 IP 发一条最小探测，头到手即断，
// 摇到 292 就入池，交给自动接管注入真实流量。
//
// 与仓库约定的关系：docs/conventions/codex-outbound-identity.md 禁止双开账号按 cron 发
// 自造 /responses；猎手是**显式开启的例外**（用户 2026-09-18 拍板），并且探测沿真实
// 转发管线出站（stageCodexOAuthIdentity + buildUpstreamRequest），头/体投影与真实流量同一套
// 代码；已知差异列在 buildOpenAITurnStateProbe 上。
//
// 不碰账号状态：探测遇到 401/429/5xx/传输错误只退避、只记日志，真实流量自己会发现真问题。
// 走的是 hunt 代理，把它的故障算到账号头上会误停真实流量。
//
// ponytail: 单 goroutine 严格串行、每次探测之间随机间隔，配置里没有并发度这一项。
// 单账号命中率个位数百分比（实测 4.4%），串行已经够用；真要并行就要处理同账号并发
// 铸造互相干扰的问题，那是另一个功能。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
)

const (
	// openAITurnStateHunterExtraKey 是管理员写的配置：见 openAITurnStateHunterConfig。
	openAITurnStateHunterExtraKey = "openai_turn_state_hunter"
	// openAITurnStateHuntExtraKey 是猎手写的运行态：见 openAITurnStateHuntState。调度中性键。
	openAITurnStateHuntExtraKey = "openai_turn_state_hunt"

	openAITurnStateHunterLeaderLockKey = "openai:turn-state-hunter:leader"
	// 锁不续期：一轮的软预算 + 一次探测超时 < 锁 TTL，锁不会在探测进行中过期；超过预算
	// 就收手，下个 tick 重新拿锁接着猎（NextAt 不动）。
	openAITurnStateHunterLeaderLockTTL = 20 * time.Minute
	openAITurnStateHunterCycleBudget   = 15 * time.Minute
	openAITurnStateHunterInterval      = 60 * time.Second
	openAITurnStateHuntProbeTimeout    = 60 * time.Second
	openAITurnStateHuntErrorPeekBytes  = 1024
	openAITurnStateHuntLastKeep        = 10
	// 同一个出口 IP 铸出 312 之后 7 天内不再探（用户 2026-09-18 定的口径）：铸什么由
	// 「账号权重 × 出口」定，出口权重不会几小时就变，再试同一个出口就是白付额度。
	// 只对固定出口有意义，轮换端点选不了出口。
	openAITurnStateHuntExitCooldown  = 7 * 24 * time.Hour
	openAITurnStateHuntExitsKeep     = 128
	openAITurnStateHuntExitEchoLimit = 15 * time.Second

	openAITurnStateHunterMaxModels      = 8
	openAITurnStateHunterMaxProxies     = 64
	openAITurnStateHunterMaxPerHourCap  = 600
	openAITurnStateHunterMaxLeadMinutes = 55
	openAITurnStateHunterMaxMinutes     = 24 * 60
	openAITurnStateHunterMaxGapSeconds  = 600

	defaultOpenAITurnStateHuntMaxPerHour      = 30
	defaultOpenAITurnStateHuntLeadMinutes     = 10
	defaultOpenAITurnStateHuntRetryMinutes    = 10
	defaultOpenAITurnStateHuntIdleMinutes     = 60
	defaultOpenAITurnStateHuntGapSeconds      = 20
	defaultOpenAITurnStateHuntReasoningEffort = "high"

	// ctxKeyTurnStateProbe 标记合成的探测上下文：观测入口据此不把探测算作真实流量。
	ctxKeyTurnStateProbe = "openai_turn_state_probe_ctx"
)

// openAITurnStateHunterConfig 是 extra.openai_turn_state_hunter 的形态。
//
//	{"enabled":true,"models":["gpt-6-astra"],"proxy_ids":[20,21],
//	 "max_per_hour":30,"lead_minutes":10,"retry_minutes":10,"idle_minutes":60,
//	 "gap_seconds":20,"reasoning_effort":"high"}
//
// 数值 0 表示取默认；idle_minutes 显式写负数表示「不设空闲门槛」。
type openAITurnStateHunterConfig struct {
	Enabled  bool     `json:"enabled"`
	Models   []string `json:"models"`
	ProxyIDs []int64  `json:"proxy_ids"`
	// MaxPerHour 每小时探测上限（跨模型共用）。命中率约 4.4%/次时，30 次内命中的概率约 74%。
	MaxPerHour int `json:"max_per_hour"`
	// LeadMinutes 票到期前多少分钟开窗。
	LeadMinutes int `json:"lead_minutes"`
	// RetryMinutes 一轮没摇到时退避多久。
	RetryMinutes int `json:"retry_minutes"`
	// IdleMinutes 该模型多少分钟内没有真实请求就不猎：没人用的票到期即废，且让脏号能静置恢复。
	IdleMinutes int `json:"idle_minutes"`
	// GapSeconds 两次探测之间的基准间隔，实际取 0.5×–1.5× 随机。
	GapSeconds int `json:"gap_seconds"`
	// ReasoningEffort 探测请求的思考强度，与真实请求形态对齐；头到手即断，上游收到
	// RST_STREAM 后停止生成，输出 token 只剩断流前那一瞬。
	ReasoningEffort string `json:"reasoning_effort"`
}

// applyDefaults 补默认值并压上限。运行侧不能信任 extra 里的数字：数据导入
// （account_data.go）不过 handler 校验，直接落库。
func (cfg *openAITurnStateHunterConfig) applyDefaults() {
	cfg.MaxPerHour = openAITurnStateHuntBound(cfg.MaxPerHour, defaultOpenAITurnStateHuntMaxPerHour, openAITurnStateHunterMaxPerHourCap)
	cfg.LeadMinutes = openAITurnStateHuntBound(cfg.LeadMinutes, defaultOpenAITurnStateHuntLeadMinutes, openAITurnStateHunterMaxLeadMinutes)
	cfg.RetryMinutes = openAITurnStateHuntBound(cfg.RetryMinutes, defaultOpenAITurnStateHuntRetryMinutes, openAITurnStateHunterMaxMinutes)
	cfg.GapSeconds = openAITurnStateHuntBound(cfg.GapSeconds, defaultOpenAITurnStateHuntGapSeconds, openAITurnStateHunterMaxGapSeconds)
	if cfg.IdleMinutes == 0 {
		cfg.IdleMinutes = defaultOpenAITurnStateHuntIdleMinutes
	} else if cfg.IdleMinutes > openAITurnStateHunterMaxMinutes {
		cfg.IdleMinutes = openAITurnStateHunterMaxMinutes
	}
	models := make([]string, 0, len(cfg.Models))
	for _, m := range cfg.Models {
		if m = strings.TrimSpace(m); m != "" {
			models = append(models, m)
		}
	}
	if len(models) > openAITurnStateHunterMaxModels {
		models = models[:openAITurnStateHunterMaxModels]
	}
	cfg.Models = models
	if len(cfg.ProxyIDs) > openAITurnStateHunterMaxProxies {
		cfg.ProxyIDs = cfg.ProxyIDs[:openAITurnStateHunterMaxProxies]
	}
	effort := strings.TrimSpace(cfg.ReasoningEffort)
	if _, known := openAITurnStateHuntReasoningEfforts[effort]; !known {
		effort = defaultOpenAITurnStateHuntReasoningEffort
	}
	cfg.ReasoningEffort = effort
}

// openAITurnStateHuntBound：≤0 取默认，超上限取上限。
func openAITurnStateHuntBound(v, def, max int) int {
	if v <= 0 {
		return def
	}
	if v > max {
		return max
	}
	return v
}

func (cfg openAITurnStateHunterConfig) lead() time.Duration {
	return time.Duration(cfg.LeadMinutes) * time.Minute
}
func (cfg openAITurnStateHunterConfig) retry() time.Duration {
	return time.Duration(cfg.RetryMinutes) * time.Minute
}
func (cfg openAITurnStateHunterConfig) gap() time.Duration {
	return time.Duration(cfg.GapSeconds) * time.Second
}

// readOpenAITurnStateHunterConfig 走 json 往返而不是裸断言：extra 是 JSONB，同一个键在不同
// 读路径上的具体 Go 类型不保证相同（与 readOpenAITurnStatePool 同一套取舍）。
func readOpenAITurnStateHunterConfig(a *Account) (openAITurnStateHunterConfig, bool) {
	var cfg openAITurnStateHunterConfig
	if a == nil || a.Extra == nil {
		return cfg, false
	}
	raw, ok := a.Extra[openAITurnStateHunterExtraKey]
	if !ok || raw == nil {
		return cfg, false
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return cfg, false
	}
	if err := json.Unmarshal(encoded, &cfg); err != nil {
		return cfg, false
	}
	cfg.applyDefaults()
	return cfg, true
}

// IsOpenAITurnStateHunterEnabled 报告账号是否开了猎手。只对直连 ChatGPT 的 oauth /
// setup-token 账号成立：cpr 的出口由 codex-proxy-rs 决定，换代理换不到出口。
func (a *Account) IsOpenAITurnStateHunterEnabled() bool {
	if a == nil || !a.IsOpenAIOAuthLike() {
		return false
	}
	cfg, ok := readOpenAITurnStateHunterConfig(a)
	return ok && cfg.Enabled
}

// openAITurnStateHuntedModel 报告猎手是否在为该模型补票：只有这些模型才值得对未判定
// 的会话无条件注入——别的模型猎手不补票，注了也只是白付「注入后不重铸」的观测代价。
func (a *Account) openAITurnStateHuntedModel(model string) bool {
	if a == nil || !a.IsOpenAIOAuthLike() {
		return false
	}
	cfg, ok := readOpenAITurnStateHunterConfig(a)
	if !ok || !cfg.Enabled {
		return false
	}
	model = strings.TrimSpace(model)
	for _, m := range cfg.Models {
		if strings.EqualFold(strings.TrimSpace(m), model) {
			return true
		}
	}
	return false
}

// openAITurnStateHuntAttempt 是最近一次探测的记录，账号页 tooltip 直接展示。
type openAITurnStateHuntAttempt struct {
	At      time.Time `json:"at"`
	Model   string    `json:"model"`
	ProxyID int64     `json:"proxy_id"`
	Proxy   string    `json:"proxy"`
	Status  int       `json:"status"`
	Chars   int       `json:"chars"`
	Healthy bool      `json:"healthy"`
	// LatencyMs 是响应头到手的耗时（实测 0.5–2.3s）：探测在这一刻就断，后面不再计时。
	LatencyMs int64 `json:"latency_ms"`
	// Exit 是探测前解析到的出口 IP，只有固定出口有；轮换端点由供应商按连接选出口，为空。
	Exit  string `json:"exit,omitempty"`
	Error string `json:"error,omitempty"`
}

// openAITurnStateHuntExit 记一个出口 IP 最近一次探测的结果，冷却判定的依据。
type openAITurnStateHuntExit struct {
	IP      string    `json:"ip"`
	ProxyID int64     `json:"proxy_id"`
	At      time.Time `json:"at"`
	Healthy bool      `json:"healthy"`
}

// openAITurnStateHuntState 是 extra.openai_turn_state_hunt 的形态，每次探测后写一次。
type openAITurnStateHuntState struct {
	NextAt    time.Time                    `json:"next_at"`
	HourStart time.Time                    `json:"hour_start"`
	HourCount int                          `json:"hour_count"`
	Cursor    int                          `json:"cursor"`
	Last      []openAITurnStateHuntAttempt `json:"last"`
	Exits     []openAITurnStateHuntExit    `json:"exits,omitempty"`
	LastError string                       `json:"last_error,omitempty"`
	// CapWait 标记 NextAt 是「撞上限等窗」定的（而不是出错退避）：上限调高后本窗还有余量
	// 就不用等到点，立刻恢复。
	CapWait   bool      `json:"cap_wait,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// waiting 报告现在是否还该等：退避照等；撞上限的等待在上限调高后自动解除。
func (st *openAITurnStateHuntState) waiting(cfg openAITurnStateHunterConfig, now time.Time) bool {
	if !now.Before(st.NextAt) {
		return false
	}
	return !st.CapWait || st.HourCount >= cfg.MaxPerHour
}

// noteExit 记录出口的最新结果（同一「代理 × IP」只留最新一条，最多 128 条，够 64 个固定
// 代理各换一次 IP）。两个代理共用一个出口时各留一条，这样下一轮两个代理都能不回声就跳过。
func (st *openAITurnStateHuntState) noteExit(attempt openAITurnStateHuntAttempt) {
	if attempt.Exit == "" || attempt.Error != "" {
		return
	}
	st.recordExit(openAITurnStateHuntExit{IP: attempt.Exit, ProxyID: attempt.ProxyID, At: attempt.At, Healthy: attempt.Healthy})
}

func (st *openAITurnStateHuntState) recordExit(entry openAITurnStateHuntExit) {
	kept := make([]openAITurnStateHuntExit, 0, len(st.Exits)+1)
	kept = append(kept, entry)
	for _, e := range st.Exits {
		if e.IP != entry.IP || e.ProxyID != entry.ProxyID {
			kept = append(kept, e)
		}
	}
	if len(kept) > openAITurnStateHuntExitsKeep {
		kept = kept[:openAITurnStateHuntExitsKeep]
	}
	st.Exits = kept
}

// exitCoolingDown 报告该出口是否在冷却期：最近一次铸的是 312 且不到 7 天。铸出 292 的出口
// 不冷却——它对别的模型也大概率是好出口。条目按新到旧排，第一条命中的就是最新结果。
func (st *openAITurnStateHuntState) exitCoolingDown(ip string, now time.Time) bool {
	for _, e := range st.Exits {
		if e.IP == ip {
			return !e.Healthy && now.Sub(e.At) < openAITurnStateHuntExitCooldown
		}
	}
	return false
}

// aliasExit 把「这个代理也走这个出口」记下来，沿用该出口已有的结果与时间：下一轮这个
// 代理靠 lastExitOf 就能跳过，不用再回声。
func (st *openAITurnStateHuntState) aliasExit(proxyID int64, ip string) {
	for _, e := range st.Exits {
		if e.IP == ip {
			st.recordExit(openAITurnStateHuntExit{IP: ip, ProxyID: proxyID, At: e.At, Healthy: e.Healthy})
			return
		}
	}
}

// lastExitOf 取该代理最近一次探测解析到的出口 IP。
func (st *openAITurnStateHuntState) lastExitOf(proxyID int64) string {
	for _, e := range st.Exits {
		if e.ProxyID == proxyID {
			return e.IP
		}
	}
	return ""
}

func readOpenAITurnStateHuntState(a *Account) openAITurnStateHuntState {
	var st openAITurnStateHuntState
	if a == nil || a.Extra == nil {
		return st
	}
	raw, ok := a.Extra[openAITurnStateHuntExtraKey]
	if !ok || raw == nil {
		return st
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return openAITurnStateHuntState{}
	}
	if err := json.Unmarshal(encoded, &st); err != nil {
		return openAITurnStateHuntState{}
	}
	return st
}

func (st *openAITurnStateHuntState) rollHour(now time.Time) {
	if st.HourStart.IsZero() || !now.Before(st.HourStart.Add(time.Hour)) {
		st.HourStart = now
		st.HourCount = 0
	}
}

func (st *openAITurnStateHuntState) push(attempt openAITurnStateHuntAttempt) {
	st.Last = append([]openAITurnStateHuntAttempt{attempt}, st.Last...)
	if len(st.Last) > openAITurnStateHuntLastKeep {
		st.Last = st.Last[:openAITurnStateHuntLastKeep]
	}
	st.HourCount++
	st.LastError = attempt.Error
	st.UpdatedAt = attempt.At
	st.noteExit(attempt)
}

// openAITurnStateHuntProxyRepo 是猎手对代理仓储的全部依赖；ProxyRepository 满足它。
type openAITurnStateHuntProxyRepo interface {
	ListByIDs(ctx context.Context, ids []int64) ([]Proxy, error)
}

// OpenAITurnStateHunterService 是猎手的定时器外壳，仿 OpenAIQuotaAutoResetService。
type OpenAITurnStateHunterService struct {
	gateway     *OpenAIGatewayService
	accountRepo AccountRepository
	proxyRepo   openAITurnStateHuntProxyRepo
	// exitProber 解析固定出口的 IP（与代理管理页的「检测」同一个探针，走独立连接）。
	// nil 时退化成按代理 ID 去重。
	exitProber ProxyExitInfoProber
	lockCache  LeaderLockCache
	db         *sql.DB
	owner      string
	interval   time.Duration

	// now / sleep 可注入：测试里把随机间隔归零，不真睡。
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error

	ctx    context.Context
	cancel context.CancelFunc
	start  sync.Once
	stop   sync.Once
	wg     sync.WaitGroup
}

func NewOpenAITurnStateHunterService(gateway *OpenAIGatewayService, accountRepo AccountRepository, proxyRepo openAITurnStateHuntProxyRepo, exitProber ProxyExitInfoProber, interval time.Duration) *OpenAITurnStateHunterService {
	if interval <= 0 {
		interval = openAITurnStateHunterInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &OpenAITurnStateHunterService{
		gateway:     gateway,
		accountRepo: accountRepo,
		proxyRepo:   proxyRepo,
		exitProber:  exitProber,
		owner:       uuid.NewString(),
		interval:    interval,
		now:         time.Now,
		sleep:       sleepContext,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// SetLeaderLock 注入跨实例互斥：多实例同时探测会成倍烧额度。
func (s *OpenAITurnStateHunterService) SetLeaderLock(lockCache LeaderLockCache, db *sql.DB) {
	if s == nil {
		return
	}
	s.lockCache = lockCache
	s.db = db
}

func (s *OpenAITurnStateHunterService) Start() {
	if s == nil || s.gateway == nil || s.accountRepo == nil || s.proxyRepo == nil {
		return
	}
	s.start.Do(func() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			ticker := time.NewTicker(s.interval)
			defer ticker.Stop()
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-ticker.C:
					s.runOnce(s.ctx)
				}
			}
		}()
	})
}

func (s *OpenAITurnStateHunterService) Stop() {
	if s == nil {
		return
	}
	s.stop.Do(func() {
		s.cancel()
		s.wg.Wait()
	})
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// runOnce 是一次 tick：持 leader lock，遍历开了猎手的账号。一轮探测可能跑十几分钟，
// ticker 的下一次 tick 会等它结束（channel 缓冲 1，多余的 tick 合并）；整轮受软预算约束，
// 见 openAITurnStateHunterCycleBudget。
func (s *OpenAITurnStateHunterService) runOnce(ctx context.Context) {
	defer func() {
		// 猎手是后台 goroutine，panic 会带走整个进程；探测出的任何意外只值一条日志。
		if r := recover(); r != nil {
			slog.Error("openai_turn_state_hunt_panic", "panic", r)
		}
	}()
	release, ok := tryAcquireSingletonLeaderLock(ctx, s.lockCache, s.db, openAITurnStateHunterLeaderLockKey, s.owner, openAITurnStateHunterLeaderLockTTL)
	if !ok {
		return
	}
	if release != nil {
		defer release()
	}
	// 预算从拿到锁那一刻起算：列账号的耗时也吃锁。
	deadline := s.now().Add(openAITurnStateHunterCycleBudget)
	accounts, err := s.accountRepo.ListByPlatform(ctx, PlatformOpenAI)
	if err != nil {
		slog.Warn("openai_turn_state_hunt_list_failed", "error", err)
		return
	}
	for i := range accounts {
		if ctx.Err() != nil || s.now().After(deadline) {
			return
		}
		account := &accounts[i]
		// 猎手依赖自动接管：票只入池不注入等于白猎。ListByPlatform 本身只返回 active，
		// 这里的状态检查是双保险。
		//
		// 刻意不看 Schedulable：停了调度的降智账号正是要先猎到票再放回调度的那种
		// （池耗尽停号也只动 Schedulable），看它就死锁。停调度的账号没有真实流量，
		// 默认的空闲门槛会自己刹车；显式 idle_minutes=-1 表示用户就是要它一直猎。
		if account.Status != StatusActive || !account.IsOpenAITurnStateHunterEnabled() || !account.IsOpenAITurnStateAutoEnabled() {
			continue
		}
		s.huntAccount(ctx, account, deadline)
	}
}

// huntAccount 对一个账号跑一轮：先算哪些模型缺票，再在这些模型间轮流探测。
func (s *OpenAITurnStateHunterService) huntAccount(ctx context.Context, account *Account, deadline time.Time) {
	cfg, _ := readOpenAITurnStateHunterConfig(account)
	now := s.now()
	st := readOpenAITurnStateHuntState(account)
	if st.waiting(cfg, now) {
		return
	}
	st.CapWait = false // 进入新一轮就不再是「等窗」状态；只有再次撞上限才重新标
	st.rollHour(now)
	if st.HourCount >= cfg.MaxPerHour {
		return
	}
	wanted := s.modelsNeedingTicket(ctx, account, cfg, now)
	if len(wanted) == 0 {
		return
	}
	proxies := s.loadHuntProxies(ctx, cfg.ProxyIDs, now)
	if len(proxies) == 0 {
		st.LastError = "no usable hunt proxy"
		st.NextAt = now.Add(cfg.retry())
		st.UpdatedAt = now
		s.persist(ctx, account, st)
		slog.Warn("openai_turn_state_hunt_no_proxy", "account_id", account.ID, "proxy_ids", cfg.ProxyIDs)
		return
	}
	s.huntModels(ctx, account, cfg, &st, proxies, wanted, deadline)
}

// modelsNeedingTicket 返回「有真实流量且票要到期」的模型。
func (s *OpenAITurnStateHunterService) modelsNeedingTicket(ctx context.Context, account *Account, cfg openAITurnStateHunterConfig, now time.Time) []string {
	pool := s.gateway.loadOpenAITurnStatePoolFresh(ctx, account)
	ttl := account.openAITurnStateStaleAfter()
	wanted := make([]string, 0, len(cfg.Models))
	for _, model := range cfg.Models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		if cfg.IdleMinutes > 0 && !s.gateway.openAITurnStateTrafficSince(account.ID, model, now.Add(-time.Duration(cfg.IdleMinutes)*time.Minute)) {
			continue
		}
		if expiresAt, ok := openAITurnStateNewestUsableExpiry(pool, model, ttl, now); ok && expiresAt.Sub(now) > cfg.lead() {
			continue
		}
		wanted = append(wanted, model)
	}
	return wanted
}

// openAITurnStateNewestUsableExpiry 取该模型最晚到期的可用票的到期时刻。
// 铸造戳解不出来的票在 usable() 里按「不过期」处理，但它的到期时刻未知，不能当
// 「还够用」——曾经按 now+ttl 算，那一条会永远压住猎手。显式跳过它（零值 +ttl 本来
// 也当不上 newest，这里只是把意图写明），只看有真实铸造戳的票。
func openAITurnStateNewestUsableExpiry(pool []openAITurnStateCandidate, model string, ttl time.Duration, now time.Time) (time.Time, bool) {
	var newest time.Time
	found := false
	for _, c := range pool {
		if c.MintedAt.IsZero() || !c.usable(model, ttl, now) {
			continue
		}
		expiresAt := c.MintedAt.Add(ttl)
		if !found || expiresAt.After(newest) {
			newest = expiresAt
			found = true
		}
	}
	return newest, found
}

// loadHuntProxies 按配置顺序取代理，跳过停用/过期的。配置了但取不到的记 warn：
// 绑了代理就不许静默回落到别的出口（codex-outbound-identity.md 第 53 条）。
func (s *OpenAITurnStateHunterService) loadHuntProxies(ctx context.Context, ids []int64, now time.Time) []Proxy {
	if len(ids) == 0 {
		return nil
	}
	loaded, err := s.proxyRepo.ListByIDs(ctx, ids)
	if err != nil {
		slog.Warn("openai_turn_state_hunt_proxy_load_failed", "error", err)
		return nil
	}
	byID := make(map[int64]Proxy, len(loaded))
	for _, p := range loaded {
		byID[p.ID] = p
	}
	out := make([]Proxy, 0, len(ids))
	for _, id := range ids {
		p, ok := byID[id]
		if !ok || !p.IsActive() || p.IsExpired(now) {
			slog.Warn("openai_turn_state_hunt_proxy_skipped", "proxy_id", id, "found", ok)
			continue
		}
		out = append(out, p)
	}
	return out
}

// huntModels 在缺票的模型间轮流摇骰子：每次探测换下一个模型，命中的模型出列，直到全部
// 命中、额度用尽、预算用完或出错。轮流而不是逐个，是为了多模型时不让第一个模型吃光
// 整小时的额度。
func (s *OpenAITurnStateHunterService) huntModels(ctx context.Context, account *Account, cfg openAITurnStateHunterConfig, st *openAITurnStateHuntState, proxies []Proxy, pending []string, deadline time.Time) {
	// 固定出口按整轮记「用过」，不分模型：同一个 IP 铸出 312 之后再试它就是白付一次额度。
	used := make(map[int64]bool, len(proxies))
	probed := 0
	for len(pending) > 0 {
		if ctx.Err() != nil || s.now().After(deadline) {
			return // 下个 tick 重新拿锁接着猎，NextAt 不动
		}
		// 上限是按小时窗算的：一轮可能跨过小时边界，每次探测前都要滚一次窗。
		st.rollHour(s.now())
		if st.HourCount >= cfg.MaxPerHour {
			st.NextAt = st.HourStart.Add(time.Hour)
			st.CapWait = true
			s.persist(ctx, account, *st)
			return
		}
		proxy, ok := nextOpenAITurnStateHuntProxy(proxies, st, used)
		if !ok {
			break // 固定出口都用过一遍、又没有轮换端点：这一轮到此为止
		}
		exit, cooling := s.resolveHuntExit(ctx, st, proxy)
		if cooling {
			slog.Debug("openai_turn_state_hunt_exit_cooling", "account_id", account.ID, "proxy_id", proxy.ID, "exit", exit)
			continue // 不算额度、不睡：换下一个出口
		}
		model := pending[0]
		attempt := s.probe(ctx, account, model, cfg, proxy)
		attempt.Exit = exit
		probed++
		st.push(attempt)
		slog.Info("openai_turn_state_hunt_attempt",
			"account_id", account.ID, "model", model, "proxy_id", proxy.ID, "proxy", proxy.Name, "exit", exit,
			"status", attempt.Status, "chars", attempt.Chars, "healthy", attempt.Healthy, "error", attempt.Error,
			"latency_ms", attempt.LatencyMs, "hour_count", st.HourCount)
		if attempt.Status != http.StatusOK || attempt.Error != "" {
			// 出错只退避，不改账号状态：走的是 hunt 代理，故障算到账号头上会误停真实流量。
			st.NextAt = s.now().Add(openAITurnStateHuntBackoff(attempt.Status))
			s.persist(ctx, account, *st)
			return
		}
		if attempt.Healthy {
			pending = pending[1:]
		} else {
			pending = append(pending[1:], model)
		}
		s.persist(ctx, account, *st)
		if len(pending) == 0 {
			return // 全部命中：不退避，票到期前 lead_minutes 再开窗
		}
		// 睡眠也在预算内：预算 + 一次探测超时 < 锁 TTL 这条不变量靠这里守住，
		// 否则 gap_seconds 一调大，锁就会在轮次中过期、另一实例并发探测同一账号。
		wait := openAITurnStateHuntJitter(cfg.gap())
		if s.now().Add(wait).After(deadline) {
			return
		}
		if err := s.sleep(ctx, wait); err != nil {
			return
		}
	}
	if probed == 0 {
		// 固定出口全在冷却：不留痕迹的话页面只会显示上一次的结果和「待命」，几小时不动没人看得懂。
		st.LastError = "all hunt exits cooling"
		st.UpdatedAt = s.now()
	}
	st.NextAt = s.now().Add(cfg.retry())
	s.persist(ctx, account, *st)
}

// resolveHuntExit 解析固定出口的 IP 并判冷却。轮换端点由供应商按连接选出口，解析不了
// 也不用判（每次都是新出口）。解析走 exitProber 的独立连接：固定出口不管哪条连接都是
// 同一个 IP，所以回声看到的就是探测会用的。一轮内每个固定代理只到这里一次（used 保证）。
func (s *OpenAITurnStateHunterService) resolveHuntExit(ctx context.Context, st *openAITurnStateHuntState, proxy Proxy) (exit string, cooling bool) {
	if openAITurnStateHuntProxyRotating(proxy) {
		return "", false
	}
	now := s.now()
	// 这个代理上次解析到的出口还在冷却：不用回声就能跳过。
	if last := st.lastExitOf(proxy.ID); last != "" && st.exitCoolingDown(last, now) {
		return last, true
	}
	exit = s.echoHuntExit(ctx, proxy)
	if exit != "" && st.exitCoolingDown(exit, now) {
		st.aliasExit(proxy.ID, exit) // 记住「这个代理也走这个出口」，下一轮不再回声
		return exit, true
	}
	return exit, false
}

func (s *OpenAITurnStateHunterService) echoHuntExit(ctx context.Context, proxy Proxy) string {
	if s.exitProber == nil {
		return ""
	}
	proxyURL, err := resolveConfiguredProxyURL(ctx, nil, &proxy.ID, &proxy)
	if err != nil || strings.TrimSpace(proxyURL) == "" {
		return ""
	}
	echoCtx, cancel := context.WithTimeout(ctx, openAITurnStateHuntExitEchoLimit)
	defer cancel()
	info, _, err := s.exitProber.ProbeProxy(echoCtx, proxyURL)
	if err != nil || info == nil {
		slog.Warn("openai_turn_state_hunt_exit_echo_failed", "proxy_id", proxy.ID, "error", err)
		return ""
	}
	return strings.TrimSpace(info.IP)
}

// nextOpenAITurnStateHuntProxy 按游标轮转。轮换端点（每条新连接换 IP）可以反复用；
// 固定出口一轮只用一次。
func nextOpenAITurnStateHuntProxy(proxies []Proxy, st *openAITurnStateHuntState, used map[int64]bool) (Proxy, bool) {
	for range proxies {
		if st.Cursor < 0 || st.Cursor >= len(proxies) {
			st.Cursor = 0
		}
		p := proxies[st.Cursor]
		st.Cursor = (st.Cursor + 1) % len(proxies)
		if openAITurnStateHuntProxyRotating(p) {
			return p, true
		}
		if used[p.ID] {
			continue
		}
		used[p.ID] = true
		return p, true
	}
	return Proxy{}, false
}

// openAITurnStateHuntProxyRotating 判 webshare 轮换端点：用户名以 -rotate 结尾
// （{user}-rotate 或 {user}-{country}-rotate）。2026-09-18 在线路机实测：-rotate 每条新代理
// 连接换一个出口；{user}-{country}-{N} 是列表里第 N 个固定出口，两次连接同一 IP——那种
// 端点就当固定出口配，一轮只用一次。
func openAITurnStateHuntProxyRotating(p Proxy) bool {
	return strings.HasSuffix(strings.ToLower(p.Host), "webshare.io") &&
		strings.HasSuffix(strings.ToLower(p.Username), "-rotate")
}

func openAITurnStateHuntJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	return base/2 + time.Duration(rand.Int64N(int64(base)))
}

func openAITurnStateHuntBackoff(status int) time.Duration {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return 6 * time.Hour // 凭据问题，猎手自己修不了；真实流量会触发刷新/停号
	case http.StatusTooManyRequests:
		return time.Hour
	default:
		return 15 * time.Minute
	}
}

// probe 发一条探测：新会话、不带 turn-state、经 hunt 代理出站，头到手即断。
func (s *OpenAITurnStateHunterService) probe(ctx context.Context, account *Account, model string, cfg openAITurnStateHunterConfig, proxy Proxy) openAITurnStateHuntAttempt {
	attempt := openAITurnStateHuntAttempt{At: s.now(), Model: model, ProxyID: proxy.ID, Proxy: proxy.Name}
	hunt := proxy
	proxyURL, err := resolveConfiguredProxyURL(ctx, nil, &hunt.ID, &hunt)
	if err != nil || strings.TrimSpace(proxyURL) == "" {
		attempt.Error = fmt.Sprintf("hunt proxy %d unusable: %v", proxy.ID, err)
		return attempt
	}
	// 浅拷贝换出口：只有代理绑定不同。侧信道（settings/user）读的也是这份绑定，
	// 与探测从同一个出口发出。
	//
	// httpUpstream 按「账号 × 代理 URL」缓存客户端（默认 account_proxy 隔离），探测与真实流量
	// 的条目互不影响；若把 connection_pool_isolation 改成 account，同账号只留一个条目，探测
	// 会把真实流量的传输层挤掉——本仓库不用那个模式。
	egress := *account
	egress.ProxyID = &hunt.ID
	egress.Proxy = &hunt

	probeCtx, cancel := context.WithTimeout(ctx, openAITurnStateHuntProbeTimeout)
	defer cancel()
	c, req, err := s.gateway.buildOpenAITurnStateProbe(probeCtx, &egress, model, cfg.ReasoningEffort)
	if err != nil {
		attempt.Error = "build probe: " + sanitizeUpstreamErrorMessage(err.Error())
		return attempt
	}
	// 每次探测一条新代理连接：webshare 的 -rotate 按连接换出口，而缓存的客户端会复用
	// HTTP/2 隧道，不关连接就一直从同一个出口发。req.Close 让 http2 把这条连接标成
	// doNotReuse（用完即关、下一条重新 CONNECT），HTTP/1.1 则响应后直接关；H2 上不会多发
	// 任何头。固定出口也这么做，探测不留长连接。
	req.Close = true
	started := time.Now()
	// 直接走 httpUpstream，不经 doOpenAIUpstream 的插件路径：插件协议不携带 req.Close，
	// 插件进程自己池化连接，一装上就会静默地从同一条隧道反复探测，换不了出口。
	resp, err := s.gateway.httpUpstream.Do(req, proxyURL, egress.ID, egress.Concurrency)
	attempt.LatencyMs = time.Since(started).Milliseconds()
	if err != nil {
		attempt.Error = sanitizeUpstreamErrorMessage(err.Error())
		return attempt
	}
	// 头到手即断：不读 SSE，cancel 让 HTTP/2 发 RST_STREAM，上游停止生成。
	defer func() { _ = resp.Body.Close() }()
	attempt.Status = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		peek, _ := io.ReadAll(io.LimitReader(resp.Body, openAITurnStateHuntErrorPeekBytes))
		attempt.Error = sanitizeUpstreamErrorMessage(strings.TrimSpace(extractUpstreamErrorMessage(peek)))
		if attempt.Error == "" {
			attempt.Error = http.StatusText(resp.StatusCode)
		}
		return attempt
	}
	blob := extractOpenAICodexTurnState(resp.Header)
	if blob == "" {
		attempt.Error = "no turn-state in response"
		return attempt
	}
	attempt.Chars = len(blob)
	attempt.Healthy = openAITurnStateHealthy(blob)
	// 与真实响应同一个入口：记铸造者、写形态观测、健康则入池。传原账号而不是出口副本，
	// 池子按账号 ID 读写，两者本来相同，这里只是不让副本流出去。
	s.gateway.relayOpenAICodexTurnState(c, account, resp.Header)
	return attempt
}

func (s *OpenAITurnStateHunterService) persist(ctx context.Context, account *Account, st openAITurnStateHuntState) {
	if s.accountRepo == nil {
		return
	}
	encoded, err := json.Marshal(st)
	if err != nil {
		return
	}
	var generic map[string]any
	if err := json.Unmarshal(encoded, &generic); err != nil {
		return
	}
	if account.Extra == nil {
		account.Extra = map[string]any{}
	}
	account.Extra[openAITurnStateHuntExtraKey] = generic
	if err := s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{openAITurnStateHuntExtraKey: generic}); err != nil {
		slog.Warn("openai_turn_state_hunt_persist_failed", "account_id", account.ID, "error", err)
	}
}

// ---- 真实流量水位（空闲门槛的依据） ----

func openAITurnStateTrafficKey(accountID int64, model string) string {
	return strconv.FormatInt(accountID, 10) + "\x1f" + strings.ToLower(strings.TrimSpace(model))
}

// noteOpenAITurnStateTraffic 记该账号该模型最近一次真实请求的时刻。只在内存里：重启后
// 猎手等到下一条真实请求才开工，代价是那条请求的首回合按自然铸造。
func (s *OpenAIGatewayService) noteOpenAITurnStateTraffic(accountID int64, model string, now time.Time) {
	if s == nil || strings.TrimSpace(model) == "" {
		return
	}
	s.openaiTurnStateTraffic.Store(openAITurnStateTrafficKey(accountID, model), now)
}

func (s *OpenAIGatewayService) openAITurnStateTrafficSince(accountID int64, model string, since time.Time) bool {
	if s == nil {
		return false
	}
	raw, ok := s.openaiTurnStateTraffic.Load(openAITurnStateTrafficKey(accountID, model))
	if !ok {
		return false
	}
	last, ok := raw.(time.Time)
	return ok && last.After(since)
}

func openAITurnStateProbeContext(c *gin.Context) bool {
	if c == nil {
		return false
	}
	raw, _ := c.Get(ctxKeyTurnStateProbe)
	flag, _ := raw.(bool)
	return flag
}

// ---- 探测请求：沿真实转发管线出站 ----

// openAITurnStateProbeIdentity 是一个全新 Codex 会话的身份：codex 0.153 真实形态，
// session == thread（新线程）、turn 独立、window = "<thread>:1"，均为 UUIDv7；
// installation 按账号稳定派生（真客户端的 installation_id 是本机固定值）。
type openAITurnStateProbeIdentity struct {
	session, thread, turn, window, contextWindow, installation string
}

func newOpenAITurnStateProbeIdentity(account *Account) openAITurnStateProbeIdentity {
	thread := uuid.Must(uuid.NewV7()).String()
	return openAITurnStateProbeIdentity{
		session:       thread,
		thread:        thread,
		turn:          uuid.Must(uuid.NewV7()).String(),
		window:        thread + ":1",
		contextWindow: uuid.Must(uuid.NewV7()).String(),
		installation:  deriveStableUUIDv4("turn-state-hunter:" + codexAccountIdentityNamespace(account)),
	}
}

func (ids openAITurnStateProbeIdentity) turnMetadata() string {
	encoded, _ := json.Marshal(map[string]any{
		"installation_id":   ids.installation,
		"session_id":        ids.session,
		"thread_id":         ids.thread,
		"turn_id":           ids.turn,
		"root_turn_id":      ids.turn,
		"window_id":         ids.window,
		"context_window_id": ids.contextWindow,
		"window_number":     1,
	})
	return string(encoded)
}

// newOpenAITurnStateProbeContext 合成一个入站上下文，形态照 Codex CLI 直连的第一回合
// （见 openai_codex_fingerprint_convergence_test.go 的 newConvTestContext）。管线的
// 头/体投影全从它读入站信息，所以这里必须像一个真客户端。
func newOpenAITurnStateProbeContext(ids openAITurnStateProbeIdentity) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	identity := resolveCodexOutboundIdentity("")
	h := c.Request.Header
	h.Set("User-Agent", identity.userAgent)
	h.Set("originator", identity.originator)
	h.Set("content-type", "application/json")
	h.Set("session-id", ids.session)
	h.Set("thread-id", ids.thread)
	h.Set("x-client-request-id", ids.thread)
	h.Set("x-codex-installation-id", ids.installation)
	h.Set("x-codex-window-id", ids.window)
	h.Set(openAIWSTurnMetadataHeader, ids.turnMetadata())
	// 探测必须自然铸造：不参与自动接管注入，也不算真实流量。
	markOpenAITurnStateAutoSkipped(c)
	c.Set(ctxKeyTurnStateProbe, true)
	return c
}

// openAITurnStateProbeBody 是最小的第一回合请求体：字段集照真实 Codex CLI，内容只有
// 一个 "hi"。instructions 用该模型的真实 base prompt（与真客户端一致；头到手即断，
// 成本只有这段输入的 token）。
func openAITurnStateProbeBody(model, effort string, ids openAITurnStateProbeIdentity) map[string]any {
	return map[string]any{
		"model":        model,
		"instructions": openai.CodexBaseInstructionsForModel(model),
		"input": []any{map[string]any{
			"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": "hi"}},
		}},
		"tools":               []any{},
		"tool_choice":         "auto",
		"parallel_tool_calls": false,
		"reasoning":           map[string]any{"effort": effort, "summary": "auto"},
		"store":               false,
		"stream":              true,
		"include":             []any{"reasoning.encrypted_content"},
		"prompt_cache_key":    ids.session,
		"client_metadata": map[string]any{
			"session_id":              ids.session,
			"thread_id":               ids.thread,
			"turn_id":                 ids.turn,
			"root_turn_id":            ids.turn,
			"x-codex-installation-id": ids.installation,
			"x-codex-window-id":       ids.window,
			"x-codex-turn-metadata":   ids.turnMetadata(),
		},
	}
}

// buildOpenAITurnStateProbe 走与 Forward 相同的 Codex OAuth 出站序列：身份来源 → OAuth
// 转换 → 身份收口暂存 → buildUpstreamRequest。返回的上下文供响应侧复用
// （relayOpenAICodexTurnState 要从它读模型、注入标记与铸造者）。
//
// 与真实流量的已知差异（刻意不补：铸什么只看账号权重与出口 IP）：没有入站 API Key，
// 身份来源按「无 key」派生；installation_id 是猎手自己的稳定派生值；请求体没有
// environment_context，头里没有 x-codex-inference-call-id；探测请求 req.Close=true，
// 走到 HTTP/1.1（openai_http2 关闭或 http 代理触发 H1 回退的 10 分钟内）时会多发一个
// Connection: close 头，H2 路径没有这条差异；探测直接走 httpUpstream，不经插件路径——
// 装了接管 oauth 出站的插件时，探测与真实流量的传输层/TLS 指纹不同（插件协议不带
// req.Close，走插件就换不了出口，两害取其轻）。
func (s *OpenAIGatewayService) buildOpenAITurnStateProbe(ctx context.Context, account *Account, model, effort string) (*gin.Context, *http.Request, error) {
	if s == nil || account == nil {
		return nil, nil, errors.New("turn-state probe: gateway or account is nil")
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, nil, errors.New("turn-state probe: model is empty")
	}
	ids := newOpenAITurnStateProbeIdentity(account)
	c := newOpenAITurnStateProbeContext(ids)
	if _, err := s.prepareCodexAccountIdentitySource(ctx, c, account); err != nil {
		return nil, nil, err
	}
	decoded := openAITurnStateProbeBody(model, effort, ids)
	result := applyCodexOAuthTransformWithOptions(decoded, codexOAuthTransformOptions{IsCodexCLI: true})
	if result.Error != nil {
		return nil, nil, result.Error
	}
	stageCodexOAuthIdentity(c, account, decoded, false)
	upstreamModel := model
	if result.NormalizedModel != "" {
		upstreamModel = result.NormalizedModel
	}
	SetOpsUpstreamModel(c, upstreamModel)
	body, err := json.Marshal(decoded)
	if err != nil {
		return nil, nil, err
	}
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, nil, err
	}
	req, err := s.buildUpstreamRequest(ctx, c, account, body, token, true, ids.session, true)
	if err != nil {
		return nil, nil, err
	}
	return c, req, nil
}

// ---- 配置校验 ----

var openAITurnStateHuntReasoningEfforts = map[string]struct{}{
	"minimal": {}, "low": {}, "medium": {}, "high": {}, "xhigh": {},
}

// ValidateOpenAITurnStateHunterExtra 校验 extra.openai_turn_state_hunter。
// 开着的猎手必须同时有模型和代理，否则「保存成功但什么都不做」是最难排查的失败。
func ValidateOpenAITurnStateHunterExtra(extra map[string]any) error {
	if extra == nil {
		return nil
	}
	raw, ok := extra[openAITurnStateHunterExtraKey]
	if !ok {
		return nil
	}
	if raw == nil {
		delete(extra, openAITurnStateHunterExtraKey)
		return nil
	}
	table, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be an object", openAITurnStateHunterExtraKey)
	}
	enabled := false
	if v, ok := table["enabled"]; ok && v != nil {
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("%s.enabled must be a boolean", openAITurnStateHunterExtraKey)
		}
		enabled = b
	}
	models, err := openAITurnStateHunterStringList(table, "models", openAITurnStateHunterMaxModels)
	if err != nil {
		return err
	}
	proxies, err := openAITurnStateHunterIDList(table, "proxy_ids", openAITurnStateHunterMaxProxies)
	if err != nil {
		return err
	}
	for key, limit := range map[string]int{
		"max_per_hour": openAITurnStateHunterMaxPerHourCap, "lead_minutes": openAITurnStateHunterMaxLeadMinutes,
		"retry_minutes": openAITurnStateHunterMaxMinutes, "gap_seconds": openAITurnStateHunterMaxGapSeconds,
	} {
		if err := openAITurnStateHunterIntField(table, key, 0, limit); err != nil {
			return err
		}
	}
	if err := openAITurnStateHunterIntField(table, "idle_minutes", -1, openAITurnStateHunterMaxMinutes); err != nil {
		return err
	}
	if v, ok := table["reasoning_effort"]; ok && v != nil {
		effort, ok := v.(string)
		if !ok {
			return fmt.Errorf("%s.reasoning_effort must be a string", openAITurnStateHunterExtraKey)
		}
		if effort = strings.TrimSpace(effort); effort != "" {
			if _, known := openAITurnStateHuntReasoningEfforts[effort]; !known {
				return fmt.Errorf("%s.reasoning_effort %q is not a known effort", openAITurnStateHunterExtraKey, effort)
			}
		}
	}
	if enabled && (models == 0 || proxies == 0) {
		return fmt.Errorf("%s is enabled but has no models or no proxy_ids", openAITurnStateHunterExtraKey)
	}
	// 开窗提前量必须小于票的寿命（可配 openai_turn_state_stale_after_minutes），否则
	// 「任何票都永远不够用」，猎手每小时打满上限也停不下来。
	lead := float64(defaultOpenAITurnStateHuntLeadMinutes) // 没写就是默认值，同样要比
	if v, ok := openAITurnStateHunterNumber(table["lead_minutes"]); ok && v > 0 {
		lead = v
	}
	if stale, ok := openAITurnStateHunterNumber(extra[openAITurnStateStaleMinExtraKey]); ok && stale > 0 && lead >= stale {
		return fmt.Errorf("%s.lead_minutes (%d) must be smaller than %s (%d)", openAITurnStateHunterExtraKey, int(lead), openAITurnStateStaleMinExtraKey, int(stale))
	}
	return nil
}

func openAITurnStateHunterStringList(table map[string]any, key string, limit int) (int, error) {
	v, ok := table[key]
	if !ok || v == nil {
		return 0, nil
	}
	list, ok := v.([]any)
	if !ok {
		return 0, fmt.Errorf("%s.%s must be an array", openAITurnStateHunterExtraKey, key)
	}
	if len(list) > limit {
		return 0, fmt.Errorf("%s.%s exceeds %d entries", openAITurnStateHunterExtraKey, key, limit)
	}
	count := 0
	for _, item := range list {
		str, ok := item.(string)
		if !ok || strings.TrimSpace(str) == "" {
			return 0, fmt.Errorf("%s.%s must contain non-empty strings", openAITurnStateHunterExtraKey, key)
		}
		count++
	}
	return count, nil
}

func openAITurnStateHunterIDList(table map[string]any, key string, limit int) (int, error) {
	v, ok := table[key]
	if !ok || v == nil {
		return 0, nil
	}
	list, ok := v.([]any)
	if !ok {
		return 0, fmt.Errorf("%s.%s must be an array", openAITurnStateHunterExtraKey, key)
	}
	if len(list) > limit {
		return 0, fmt.Errorf("%s.%s exceeds %d entries", openAITurnStateHunterExtraKey, key, limit)
	}
	for _, item := range list {
		id, ok := openAITurnStateHunterNumber(item)
		if !ok || id <= 0 || id != float64(int64(id)) {
			return 0, fmt.Errorf("%s.%s must contain positive integer ids", openAITurnStateHunterExtraKey, key)
		}
	}
	return len(list), nil
}

func openAITurnStateHunterIntField(table map[string]any, key string, minimum, maximum int) error {
	v, ok := table[key]
	if !ok || v == nil {
		return nil
	}
	n, ok := openAITurnStateHunterNumber(v)
	if !ok || n != float64(int64(n)) || int(n) < minimum || int(n) > maximum {
		return fmt.Errorf("%s.%s must be an integer in [%d, %d]", openAITurnStateHunterExtraKey, key, minimum, maximum)
	}
	return nil
}

func openAITurnStateHunterNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
