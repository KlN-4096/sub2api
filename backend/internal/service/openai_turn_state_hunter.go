package service

// Turn-state 292 猎手。
//
// 已失效、已废弃（2026-09-23）：2026-09-21 起把 292 注入请求已经换不来正常服务，猎手的前提
// 不存在了。只保留、不再扩展，计划在后续几个版本移除（连同 hold / recovery / 自动接管 / 手填覆写）。
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
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sort"
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
	// openAITurnStateHuntProbeText 是猎手探测的用户消息：头到手即断，内容只要能触发一次生成。
	openAITurnStateHuntProbeText = "hi"
	// openAITurnStateProbeBillTimeout 是探测记账（读 key、扣费事务、用量行）自己的上限，与探测预算分开。
	openAITurnStateProbeBillTimeout = 30 * time.Second
	openAITurnStateHuntLastKeep     = 10
	// 同一个出口 IP 铸出 312 之后 7 天内不再探（用户 2026-09-18 定的口径）：铸什么由
	// 「账号权重 × 出口」定，出口权重不会几小时就变，再试同一个出口就是白付额度。
	// 只对固定出口有意义，轮换端点选不了出口。
	openAITurnStateHuntExitCooldown  = 7 * 24 * time.Hour
	openAITurnStateHuntExitsKeep     = 128
	openAITurnStateHuntExitEchoLimit = 15 * time.Second
	// 同一代理连续几次传输错误才算本轮用不了：http2: client connection lost 这类偶发错误
	// 隔一个 gap 重试一次就好，不该让唯一的出口白等一轮 retry_minutes（2026-09-19 用户反馈）。
	// openAITurnStateHuntFailureStrikes 同一代理连续失败多少次才算本轮用不了。所有失败一视同仁
	// （传输层错误、401/403、429、5xx）：轮换端点每次连接换一个出口，摇到被 Cloudflare 拒的 IP
	// 就是 403，跟凭据、跟账号都没关系——上一分钟同一个代理还在出 292（2026-09-19 现网）。
	openAITurnStateHuntFailureStrikes = 3
	// openAITurnStateHuntFailureBackoff 一轮里每个代理都连续失败到出局后等多久。
	openAITurnStateHuntFailureBackoff = time.Hour

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
	Enabled bool `json:"enabled"`
	// Models 手选的要猎模型；AutoModels 为真时忽略。
	Models   []string `json:"models"`
	ProxyIDs []int64  `json:"proxy_ids"`
	// AutoModels 按真实请求自动定要猎的模型：空闲窗口内有真实流量的模型都猎（水位只在进程内，
	// 重启后要等首条真实请求）；注入点对任何模型都按「猎手在补票」处理。
	AutoModels bool `json:"auto_models,omitempty"`
	// RotatingProxyIDs 里的代理按「每条新连接换出口」处理：一轮内可反复用、不回声、不按 IP
	// 冷却。webshare 的 -rotate 端点不用填（自动识别）；别的供应商（B2Proxy 等）的轮换模式
	// 从用户名看不出来，必须显式勾。
	RotatingProxyIDs []int64 `json:"rotating_proxy_ids,omitempty"`
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
	// HoldWhenDegraded 要猎的模型拿不出可注入的 292 时把**该模型**在本账号上停一个空闲窗口、
	// 本次请求换号/503，猎到票立即放回，到期后由下一条请求再拉起（openai_turn_state_hold.go）。
	HoldWhenDegraded bool `json:"hold_when_degraded,omitempty"`
	// UsageAPIKeyID >0 时每次 200 探测按标准用量路径落一行、挂在这把 key 下计费
	//（request_type=probe；输入 token 本地估算，输出 0）。0 = 不记。
	UsageAPIKeyID int64 `json:"usage_api_key_id,omitempty"`
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
	if len(cfg.RotatingProxyIDs) > openAITurnStateHunterMaxProxies {
		cfg.RotatingProxyIDs = cfg.RotatingProxyIDs[:openAITurnStateHunterMaxProxies]
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
func (s *OpenAIGatewayService) openAITurnStateHuntedModel(a *Account, model string) bool {
	if a == nil || !a.IsOpenAIOAuthLike() {
		return false
	}
	cfg, ok := readOpenAITurnStateHunterConfig(a)
	if !ok || !cfg.Enabled {
		return false
	}
	model = strings.TrimSpace(model)
	if cfg.AutoModels {
		// 真实请求到哪个模型就猎哪个——但得够格（见 openAITurnStateAutoHuntable）。曾被本功能停过
		// 的模型（model_rate_limits 里留着本功能的条目，到期与否都算）也算在管：暂停只停一个空闲
		// 窗口，重启 / 另一实例的铸造记忆是空的，不这么算就会在到期后放一条请求裸奔（第一轮评审 S1）。
		// 条目被清限流（人工恢复状态、额度自动重置、账号测试成功都走 ClearModelRateLimits 整键删）才
		// 失效：那之后再叠一次重启，该模型会裸奔一条请求（下一次铸造就重新记住），有界，不另存一份
		// 记忆。画图排除在兜底之前：手选模式下停了 gpt-image-2 再切自动，
		// 不能靠兜底把它猎下去。
		if openAITurnStateImageModel(model) {
			return false
		}
		if _, everHeld := openAITurnStateHoldResetAt(a, model); everHeld {
			return true
		}
		return s.openAITurnStateAutoHuntable(a, model)
	}
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
	// LatencyMs 是响应头到手的耗时（实测 0.5–2.3s）：猎手探测在这一刻就断，后面不再计时。
	// 恢复探测要读完回答，它的 LatencyMs 是到读完为止。
	LatencyMs int64 `json:"latency_ms"`
	// Exit 是探测前解析到的出口 IP，只有固定出口有；轮换端点由供应商按连接选出口，为空。
	Exit string `json:"exit,omitempty"`
	// Answer 是恢复探测读到的模型回答（糖果题，期望 "21"）；猎手探测头到手即断，没有。
	Answer string `json:"answer,omitempty"`
	Error  string `json:"error,omitempty"`
	// transport 标记错误发生在代理/传输层（请求已发出但没拿到响应）：请求多半没到上游，
	// 不计小时额度。不落库。
	transport bool
	// preflight 标记请求**根本没发出**（构造探测时就拿不到 token 之类）。这是账号配置的
	// 问题，换出口、重试都改变不了结果，所以它是唯一不走重试的失败。不落库。
	preflight bool
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
	CapWait bool `json:"cap_wait,omitempty"`
	// Gate 记录上一次被门槛挡住的原因（idle / fresh），正在猎时为空。不留痕的话
	// 「票还新鲜」「无真实流量」「模型名配错」在页面上长得一模一样。
	Gate      string    `json:"gate,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

const (
	openAITurnStateHuntGateIdle  = "idle"
	openAITurnStateHuntGateFresh = "fresh"
)

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
//
// 别名条目带的是旧条目的 At，会排在比它新的条目前面——exitCoolingDown 按 IP 取第一条，
// 别名与原条目是同一个 IP 的同一份结论，结果不受影响；但 Exits 不再严格按时间排序。
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
	if !attempt.transport {
		// 小时上限守的是上游侧的花费：连代理都没连上的尝试不算，否则几条死代理每轮白吃额度、
		// 好出口按比例挨饿（第二轮评审 R1：3 条代理 2 条死，一次真探测就把整点封掉）。
		st.HourCount++
	}
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
	// apiKeys 取探测记账用的 API Key（连带 User/Group）并承接额度更新；nil 时不记用量。
	apiKeys openAITurnStateHuntAPIKeys
	// subscriptions 解析订阅型分组的有效订阅；nil 时订阅型分组的 key 不记用量。
	subscriptions openAITurnStateHuntSubscriptions
	// recordUsage 默认是 gateway.RecordUsage，测试里换成捕获函数。
	recordUsage func(ctx context.Context, input *OpenAIRecordUsageInput) error
	lockCache   LeaderLockCache
	db          *sql.DB
	owner       string
	interval    time.Duration

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
	svc := &OpenAITurnStateHunterService{
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
	if gateway != nil {
		svc.recordUsage = gateway.RecordUsage
	}
	return svc
}

// openAITurnStateHuntAPIKeys 是探测记账需要的 API Key 侧能力：*APIKeyService 满足它。
type openAITurnStateHuntAPIKeys interface {
	GetByID(ctx context.Context, id int64) (*APIKey, error)
	APIKeyQuotaUpdater
}

// SetAPIKeys 装配探测记账用的 API Key 服务；不装配则探测不进使用记录。
func (s *OpenAITurnStateHunterService) SetAPIKeys(apiKeys openAITurnStateHuntAPIKeys) {
	if s != nil {
		s.apiKeys = apiKeys
	}
}

// openAITurnStateHuntSubscriptions 是订阅解析能力：*SubscriptionService 满足它。
type openAITurnStateHuntSubscriptions interface {
	GetActiveSubscription(ctx context.Context, userID, groupID int64) (*UserSubscription, error)
}

// SetSubscriptions 装配订阅解析；不装配则订阅型分组的记账 key 不记用量。
func (s *OpenAITurnStateHunterService) SetSubscriptions(subs openAITurnStateHuntSubscriptions) {
	if s != nil {
		s.subscriptions = subs
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
	var sessions []*openAITurnStateHuntSession
	// 恢复探测每个 tick 最多做一个账号：它会真的发一条请求并读完回答（最长 3 分钟），而这一段的预算是留给
	// 猎手的。间隔 30–90 分钟、tick 一分钟一次，几十个账号也轮得过来（第一轮评审 S4）。
	recoveryDone := false
	for i := range accounts {
		// 除上面那一条之外，这一段只做门槛判定（每个账号几条 SELECT），账号再多也吃不掉 15 分钟
		// 预算，所以不需要「下个 tick 从没轮到的账号开始」的游标；探测本身在下面交错跑。
		if ctx.Err() != nil || s.now().After(deadline) {
			return
		}
		account := &accounts[i]
		// 停调度的维护放在开关判定之前：猎手/接管关掉之后被本功能停着的账号也要放回。
		s.syncHold(ctx, account, s.now())
		// 恢复探测是独立开关（猎手关着也跑），间隔 30–90 分钟，绝大多数 tick 在这里直接返回。
		if !recoveryDone && s.probeRecovery(ctx, account, deadline) {
			recoveryDone = true
		}
		// 猎手依赖自动接管：票只入池不注入等于白猎。ListByPlatform 本身只返回 active，
		// 这里的状态检查是双保险。
		//
		// 刻意不看 Schedulable：停了调度（temp_unschedulable / 手动停调度）的降智账号正是
		// 要先猎到票再放回调度的那种，看它就死锁。停调度的账号没有真实流量，默认的空闲
		// 门槛会自己刹车；显式 idle_minutes=-1 表示用户就是要它一直猎。
		// 池耗尽停号走的是 SetError（status=error），ListByPlatform 直接就不返回它——那条路
		// 要人工关接管再启用，猎手救不了。
		if account.Status != StatusActive || !account.IsOpenAITurnStateHunterEnabled() || !account.IsOpenAITurnStateAutoEnabled() {
			continue
		}
		if sess := s.openHunt(ctx, account); sess != nil {
			sessions = append(sessions, sess)
		}
	}
	s.huntSessions(ctx, sessions, deadline)
}

// openAITurnStateHuntSession 是一轮里一个缺票账号的进度。多账号交错探测：每次挑 readyAt
// 最早的账号探一次，每个账号各守各的 gap_seconds。曾经是按账号串行、一个账号探到命中/出错/
// 预算耗尽才轮到下一个：排在前面的账号一直 312 就把 15 分钟预算吃光，后面的账号开了窗也只
// 能在预算末尾抢到一次探测，票在排队里过期（2026-09-19 用户反馈）。
type openAITurnStateHuntSession struct {
	account *Account
	cfg     openAITurnStateHunterConfig
	st      openAITurnStateHuntState
	proxies []Proxy
	pending []string
	// used 本轮不再用的代理：固定出口探过一次、或连续失败够数。strikes 记每个代理连续失败
	// 的次数（任何失败都算：传输层、401/403、429、5xx），中间成功过就清零。
	// struckOut 记有多少条代理是被 strike 打出局的——它等于代理总数时说明这一轮谁都没法用，
	// 退避一小时而不是 retry_minutes（正常探完一轮没命中不算，那个照旧走 retry）。
	used      map[int64]bool
	strikes   map[int64]int
	struckOut int
	probed    int
	readyAt   time.Time
}

// openHunt 算一个账号这轮要不要猎、猎哪些模型；不猎返回 nil（并按需留痕）。
func (s *OpenAITurnStateHunterService) openHunt(ctx context.Context, account *Account) *openAITurnStateHuntSession {
	cfg, _ := readOpenAITurnStateHunterConfig(account)
	now := s.now()
	st := readOpenAITurnStateHuntState(account)
	if st.waiting(cfg, now) {
		return nil
	}
	st.rollHour(now)
	if st.HourCount >= cfg.MaxPerHour {
		return nil
	}
	wanted, gate := s.modelsNeedingTicket(ctx, account, cfg, now)
	if len(wanted) == 0 {
		// 被门槛挡住也要留痕，只在原因变化时落库（别每 tick 写一次）。这次落库写回的是整个
		// st，CapWait 必须原样保留——抹掉它等于把「上限调高立刻恢复」变成死等到点。
		if st.Gate != gate {
			st.Gate = gate
			s.persist(ctx, account, st)
		}
		return nil
	}
	// 真的开猎才算「不再等窗」，只有再次撞上限才重新标。门槛痕迹也在这里清掉并落库，
	// 否则页面还写着上一次的「票未到期」。
	st.CapWait = false
	if st.Gate != "" {
		st.Gate = ""
		s.persist(ctx, account, st)
	}
	proxies := s.loadHuntProxies(ctx, cfg.ProxyIDs, now)
	if len(proxies) == 0 {
		st.LastError = "no usable hunt proxy"
		st.NextAt = now.Add(cfg.retry())
		st.UpdatedAt = now
		s.persist(ctx, account, st)
		slog.Warn("openai_turn_state_hunt_no_proxy", "account_id", account.ID, "proxy_ids", cfg.ProxyIDs)
		return nil
	}
	return &openAITurnStateHuntSession{
		account: account, cfg: cfg, st: st, proxies: proxies, pending: wanted,
		used: make(map[int64]bool, len(proxies)), strikes: make(map[int64]int, len(proxies)), readyAt: now,
	}
}

// huntSessions 在缺票的账号间交错探测直到都结束或预算用完。预算用完就收手，下个 tick 重新
// 拿锁接着猎（NextAt 不动）。
func (s *OpenAITurnStateHunterService) huntSessions(ctx context.Context, sessions []*openAITurnStateHuntSession, deadline time.Time) {
	for len(sessions) > 0 {
		if ctx.Err() != nil || s.now().After(deadline) {
			return
		}
		next := 0
		for i, sess := range sessions {
			if sess.readyAt.Before(sessions[next].readyAt) {
				next = i
			}
		}
		sess := sessions[next]
		// 睡眠也在预算内：预算 + 一次探测超时 < 锁 TTL 这条不变量靠这里守住，
		// 否则 gap_seconds 一调大，锁就会在轮次中过期、另一实例并发探测同一账号。
		if sess.readyAt.After(deadline) {
			return
		}
		if wait := sess.readyAt.Sub(s.now()); wait > 0 {
			if err := s.sleep(ctx, wait); err != nil {
				return
			}
		}
		if s.huntStep(ctx, sess, deadline) {
			sessions = append(sessions[:next], sessions[next+1:]...)
			continue
		}
		sess.readyAt = s.now().Add(openAITurnStateHuntJitter(sess.cfg.gap()))
	}
}

// modelsNeedingTicket 返回「有真实流量且票要到期」的模型。
// modelsNeedingTicket 报告哪些模型该猎；一个都不猎时第二个返回值是原因（idle / fresh）。
//
// 空闲判定放在读池之前：读池是一次 GetByID（生产实现连带 loadProxies / loadAccountGroups
// 共 3 条 SELECT），完全空闲的账号每 tick 都不该付这笔钱。
//
// 模型名要填**映射后的上游名**：水位是按 OpsUpstreamModelKey 记的，按下游名配会永远
// 卡在 idle——这也是 Gate 要留痕的原因之一。
func (s *OpenAITurnStateHunterService) modelsNeedingTicket(ctx context.Context, account *Account, cfg openAITurnStateHunterConfig, now time.Time) ([]string, string) {
	active := make([]string, 0, len(cfg.Models))
	// 被降智暂停停着的模型不受空闲门槛约束：账号已经出了轮转，不会再有真实流量来刷水位，
	// 按门槛停猎就是死锁——暂停本身就是需求信号，猎到票才放得回去。
	held := openAITurnStateHeldModels(account, now)
	models := cfg.Models
	if cfg.AutoModels {
		// 自动模式：空闲窗口内有真实流量的模型就是要猎的模型（窗口关掉就是进程内见过的全部）。
		var since time.Time
		if cfg.IdleMinutes > 0 {
			since = now.Add(-time.Duration(cfg.IdleMinutes) * time.Minute)
		}
		models = s.gateway.openAITurnStateTrafficModels(account, since)
		for _, h := range held {
			if !containsFold(models, h) {
				models = append(models, h)
			}
		}
	}
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		if cfg.IdleMinutes > 0 && !containsFold(held, model) && !s.gateway.openAITurnStateTrafficSince(account.ID, model, now.Add(-time.Duration(cfg.IdleMinutes)*time.Minute)) {
			continue
		}
		active = append(active, model)
	}
	if len(active) == 0 {
		return nil, openAITurnStateHuntGateIdle
	}
	pool := s.gateway.loadOpenAITurnStatePoolFresh(ctx, account)
	ttl := account.openAITurnStateStaleAfter()
	wanted := make([]string, 0, len(active))
	for _, model := range active {
		if expiresAt, ok := openAITurnStateNewestUsableExpiry(pool, model, ttl, now); ok && expiresAt.Sub(now) > cfg.lead() {
			continue
		}
		wanted = append(wanted, model)
	}
	if len(wanted) == 0 {
		return nil, openAITurnStateHuntGateFresh
	}
	return wanted, ""
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
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		p, ok := byID[id]
		if !ok || !p.IsActive() || p.IsExpired(now) {
			slog.Warn("openai_turn_state_hunt_proxy_skipped", "proxy_id", id, "found", ok)
			continue
		}
		// URL 都拼不出来不是瞬时故障：不进轮次，否则一条配错的代理会把整轮掐掉。
		if u, err := resolveConfiguredProxyURL(ctx, nil, &p.ID, &p); err != nil || strings.TrimSpace(u) == "" {
			slog.Warn("openai_turn_state_hunt_proxy_skipped", "proxy_id", id, "found", true, "error", err)
			continue
		}
		out = append(out, p)
	}
	return out
}

// huntStep 给一个账号探一次（出口在冷却就换下一个再探）。缺票的模型轮流摇骰子：每次探测换
// 下一个模型，命中的出列——轮流而不是逐个，是为了多模型时不让第一个模型吃光整小时的额度。
// 返回 true 表示这个账号本轮结束：全部命中、出错退避、撞上限、或代理都用过了。
func (s *OpenAITurnStateHunterService) huntStep(ctx context.Context, sess *openAITurnStateHuntSession, deadline time.Time) bool {
	account, cfg, st := sess.account, sess.cfg, &sess.st
	for {
		if ctx.Err() != nil || s.now().After(deadline) {
			return false // 下个 tick 重新拿锁接着猎，NextAt 不动
		}
		// 上限是按小时窗算的：一轮可能跨过小时边界，每次探测前都要滚一次窗。
		st.rollHour(s.now())
		if st.HourCount >= cfg.MaxPerHour {
			st.NextAt = st.HourStart.Add(time.Hour)
			st.CapWait = true
			s.persist(ctx, account, *st)
			return true
		}
		// 固定出口按整轮记「用过」，不分模型：同一个 IP 铸出 312 之后再试它就是白付一次额度。
		proxy, ok := nextOpenAITurnStateHuntProxy(cfg, sess.proxies, st, sess.used)
		if !ok {
			break // 固定出口都用过一遍、又没有轮换端点：这一轮到此为止
		}
		exit, cooling := s.resolveHuntExit(ctx, cfg, st, proxy)
		if cooling {
			slog.Debug("openai_turn_state_hunt_exit_cooling", "account_id", account.ID, "proxy_id", proxy.ID, "exit", exit)
			continue // 不算额度、不睡：换下一个出口
		}
		model := sess.pending[0]
		attempt := s.probe(ctx, account, model, cfg, proxy)
		attempt.Exit = exit
		sess.probed++
		st.push(attempt)
		slog.Info("openai_turn_state_hunt_attempt",
			"account_id", account.ID, "model", model, "proxy_id", proxy.ID, "proxy", proxy.Name, "exit", exit,
			"status", attempt.Status, "chars", attempt.Chars, "healthy", attempt.Healthy, "error", attempt.Error,
			"latency_ms", attempt.LatencyMs, "hour_count", st.HourCount)
		if attempt.preflight {
			// 请求没发出去：不是出口的问题，换代理重试只会把同一条错误抄 N 遍。
			st.NextAt = s.now().Add(cfg.retry())
			s.persist(ctx, account, *st)
			return true
		}
		if attempt.transport || attempt.Status != http.StatusOK || attempt.Error != "" {
			// 所有失败一视同仁地重试，不看状态码：一条响应不该终结整轮。原先 401/403 被当成
			// 「凭据坏了，猎手修不了」直接退避 6 小时，但轮换端点每次连接换一个出口，摇到被
			// 边缘拒掉的 IP 就是 403——上一分钟同一个代理还在出 292，于是一个一次性的坏 IP
			// 让整个账号停 6 小时，手里另外三条好代理连试都没试（2026-09-19 现网 + 用户决定）。
			//
			// 同一代理连续 openAITurnStateHuntFailureStrikes 次才算本轮用不了；整轮由代理条数
			// 有界，全部出局才退避——一个坏代理既不能把整轮掐掉，也不能反复烧额度。
			// 失败不改账号状态：走的是 hunt 代理，故障算到账号头上会误停真实流量。
			sess.strikes[proxy.ID]++
			if sess.strikes[proxy.ID] >= openAITurnStateHuntFailureStrikes {
				sess.used[proxy.ID] = true
				sess.struckOut++
			} else {
				delete(sess.used, proxy.ID) // 固定出口选中即标用过；这次不算，放回去重试
			}
			s.persist(ctx, account, *st)
			// 预算里等不到重试（gap 配得大 / 撞在预算末尾）也当这轮到此为止：strike 是轮次内的计数，
			// 不这么做的话下个 tick 又从 strike 1 数起，死代理会每 60 秒被拨一次而 retry_minutes 永不生效
			//（第二轮评审 1）。最坏一次 jitter 是 1.5 倍 gap。
			if len(sess.used) < len(sess.proxies) && !s.now().Add(cfg.gap()*3/2).After(deadline) {
				return !s.huntSessionCurrent(ctx, sess)
			}
			break // 代理都出局了 / 等不到重试：走下面的退避
		}
		sess.strikes[proxy.ID] = 0
		if attempt.Healthy {
			sess.pending = sess.pending[1:]
		} else {
			sess.pending = append(sess.pending[1:], model)
		}
		s.persist(ctx, account, *st)
		if attempt.Healthy {
			// 命中即放回，不等整轮结束（多账号交错时整轮可能还有十几分钟）。手里的账号是 tick 开头的
			// 快照，轮次中真实请求新写的暂停不在里面：重读再算（第二轮评审 B1）。
			if latest, err := s.accountRepo.GetByID(ctx, account.ID); err == nil && latest != nil {
				s.syncHold(ctx, latest, s.now())
			} else {
				slog.Warn("openai_turn_state_hold_release_skipped", "account_id", account.ID, "error", err)
			}
		}
		if len(sess.pending) == 0 {
			return true // 全部命中：不退避，票到期前 lead_minutes 再开窗
		}
		// 探完再看配置有没有变（探测前看会白睡一个 gap 才发现）：变了就结束本轮。
		return !s.huntSessionCurrent(ctx, sess)
	}
	if sess.probed == 0 {
		// 固定出口全在冷却：不留痕迹的话页面只会显示上一次的结果和「待命」，几小时不动没人看得懂。
		st.LastError = "all hunt exits cooling"
		st.UpdatedAt = s.now()
	}
	// 每条代理都连续失败到出局：这一轮谁都没法用，等一小时再来，别用 retry_minutes 反复空转。
	// 没出局的情况（正常探完一轮没命中、出口全在冷却）照旧走 retry。
	if sess.struckOut > 0 && sess.struckOut >= len(sess.proxies) {
		st.NextAt = s.now().Add(openAITurnStateHuntFailureBackoff)
		s.persist(ctx, account, *st)
		return true
	}
	st.NextAt = s.now().Add(cfg.retry())
	s.persist(ctx, account, *st)
	return true
}

// huntSessionCurrent 每次没命中的探测之后重读账号：管理员改了猎手配置（填记账 key、换代理、改间隔）
// 或关了猎手/接管，不该等这轮跑完（最长 15 分钟）才生效（2026-09-19 用户反馈：填了记账 key 十几
// 分钟不见用量行）。变了就结束本轮（返回 false），下个 tick 按新配置从头开轮。读不到账号按手里的
// 继续，一次 DB 抖动不该掐掉轮次。配置比较走 JSON 字符串：两边都是 JSONB 读出来的 map，键序稳定。
func (s *OpenAITurnStateHunterService) huntSessionCurrent(ctx context.Context, sess *openAITurnStateHuntSession) bool {
	latest, err := s.accountRepo.GetByID(ctx, sess.account.ID)
	if err != nil || latest == nil {
		return true
	}
	if latest.Status != StatusActive || !latest.IsOpenAITurnStateHunterEnabled() || !latest.IsOpenAITurnStateAutoEnabled() {
		return false
	}
	return openAITurnStateHunterConfigJSON(latest) == openAITurnStateHunterConfigJSON(sess.account)
}

func openAITurnStateHunterConfigJSON(a *Account) string {
	if a == nil || a.Extra == nil {
		return ""
	}
	raw, err := json.Marshal(a.Extra[openAITurnStateHunterExtraKey])
	if err != nil {
		return ""
	}
	return string(raw)
}

// resolveHuntExit 解析固定出口的 IP 并判冷却。轮换端点由供应商按连接选出口，解析不了
// 也不用判（每次都是新出口）。解析走 exitProber 的独立连接：固定出口不管哪条连接都是
// 同一个 IP，所以回声看到的就是探测会用的。一轮内每个固定代理只到这里一次（used 保证）。
func (s *OpenAITurnStateHunterService) resolveHuntExit(ctx context.Context, cfg openAITurnStateHunterConfig, st *openAITurnStateHuntState, proxy Proxy) (exit string, cooling bool) {
	if openAITurnStateHuntProxyRotating(cfg, proxy) {
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

// nextOpenAITurnStateHuntProxy 按游标轮转。轮换端点（每条新连接换 IP）可以反复用，
// 固定出口一轮只用一次；连不上的（used 由调用方置位）两种都不再用。
func nextOpenAITurnStateHuntProxy(cfg openAITurnStateHunterConfig, proxies []Proxy, st *openAITurnStateHuntState, used map[int64]bool) (Proxy, bool) {
	for range proxies {
		if st.Cursor < 0 || st.Cursor >= len(proxies) {
			st.Cursor = 0
		}
		p := proxies[st.Cursor]
		st.Cursor = (st.Cursor + 1) % len(proxies)
		if used[p.ID] {
			continue
		}
		if !openAITurnStateHuntProxyRotating(cfg, p) {
			used[p.ID] = true
		}
		return p, true
	}
	return Proxy{}, false
}

// openAITurnStateHuntProxyRotating 判轮换端点：显式勾在 rotating_proxy_ids 里的，或 webshare
// 的 -rotate 用户名（{user}-rotate / {user}-{country}-rotate）。2026-09-18 在线路机实测：-rotate
// 每条新代理连接换一个出口；{user}-{country}-{N} 是列表里第 N 个固定出口，两次连接同一
// IP——那种端点就当固定出口配，一轮只用一次。别的供应商（B2Proxy 等）轮换/粘性从用户名
// 看不出来，只认显式配置——猜错成「固定」的代价是一轮只探一次再等 10 分钟、312 还把出口
// 冷却 7 天（2026-09-19 用户反馈）。
func openAITurnStateHuntProxyRotating(cfg openAITurnStateHunterConfig, p Proxy) bool {
	for _, id := range cfg.RotatingProxyIDs {
		if id == p.ID {
			return true
		}
	}
	return strings.HasSuffix(strings.ToLower(p.Host), "webshare.io") &&
		strings.HasSuffix(strings.ToLower(p.Username), "-rotate")
}

func openAITurnStateHuntJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	return base/2 + time.Duration(rand.Int64N(int64(base)))
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
	// 每次探测一条新代理连接：webshare 的 -rotate 按连接换出口，而缓存的客户端会复用
	// HTTP/2 隧道，不关连接就一直从同一个出口发。closeConn 让 http2 把这条连接标成
	// doNotReuse（用完即关、下一条重新 CONNECT），HTTP/1.1 则响应后直接关；H2 上不会多发
	// 任何头。固定出口也这么做，探测不留长连接。
	s.doProbe(ctx, account, &egress, proxyURL, true, model, cfg, openAITurnStateHuntProbeText, false, &attempt)
	return attempt
}

// doProbe 是两种探测共用的发送与判定：猎手换出口探（probe），恢复探测走账号自己的出口
// （probeOwnExit）。egress 是已设好出口的账号副本，account 是入池/记账用的原账号。text 是
// 用户消息；readAnswer=true（恢复探测）读完整个 SSE 体、按回答判健康、用上游真实用量记账，
// false（猎手）头到手即断、按 turn-state 长度判。
func (s *OpenAITurnStateHunterService) doProbe(ctx context.Context, account, egress *Account, proxyURL string, closeConn bool, model string, cfg openAITurnStateHunterConfig, text string, readAnswer bool, attempt *openAITurnStateHuntAttempt) {
	timeout := openAITurnStateHuntProbeTimeout
	if readAnswer {
		timeout = openAITurnStateRecoveryProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c, req, err := s.gateway.buildOpenAITurnStateProbe(probeCtx, egress, model, cfg.ReasoningEffort, text)
	if err != nil {
		attempt.Error = "build probe: " + sanitizeUpstreamErrorMessage(err.Error())
		attempt.preflight = true
		return
	}
	req.Close = closeConn
	started := time.Now()
	// 直接走 httpUpstream，不经 doOpenAIUpstream 的插件路径：插件协议不携带 req.Close，
	// 插件进程自己池化连接，一装上就会静默地从同一条隧道反复探测，换不了出口。
	resp, err := s.gateway.httpUpstream.Do(req, proxyURL, egress.ID, egress.Concurrency)
	attempt.LatencyMs = time.Since(started).Milliseconds()
	if err != nil {
		attempt.Error = sanitizeUpstreamErrorMessage(err.Error())
		attempt.transport = true
		return
	}
	defer func() { _ = resp.Body.Close() }()
	attempt.Status = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		peek, _ := io.ReadAll(io.LimitReader(resp.Body, openAITurnStateHuntErrorPeekBytes))
		attempt.Error = sanitizeUpstreamErrorMessage(strings.TrimSpace(extractUpstreamErrorMessage(peek)))
		if attempt.Error == "" {
			attempt.Error = http.StatusText(resp.StatusCode)
		}
		return
	}
	var usage *OpenAIUsage
	if readAnswer {
		// 恢复探测要的是回答本身：读完整个流（有上限），耗时算到读完为止。
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, openAITurnStateRecoveryReadLimit))
		attempt.LatencyMs = time.Since(started).Milliseconds()
		_ = resp.Body.Close()
		if readErr != nil {
			attempt.Error = "read answer: " + sanitizeUpstreamErrorMessage(readErr.Error())
		} else if answer, healthy, got, ok := openAITurnStateRecoveryAnswer(raw); !ok {
			attempt.Error = "no completed response"
		} else {
			attempt.Answer = answer
			attempt.Healthy = healthy
			usage = got
		}
	} else {
		// 头到手即断：不读 SSE。提前关体让 HTTP/2 发 RST_STREAM（req.Close 的连接随之关掉），
		// 上游立刻停止生成——必须排在下面的入池读写库**之前**，数据库慢的时候不能让上游多
		// 生成一秒。cancel 留给 defer：c 的请求上下文就是 probeCtx，入池还要用它读写库。
		_ = resp.Body.Close()
	}
	// 200 就是一次计费请求，不管铸没铸出 turn-state。排在入池之后：记账是一串读写库
	//（GetByID、倍率、扣费事务、用量行），DB 慢的时候不能让猎到的票晚入池。记账不能挂在
	// probeCtx 上：恢复探测读回答可能把 3 分钟花光，ctx 一到期这笔真实发生的请求就 0 计费
	//（第二轮评审 S3）；给它自己的 30 秒。
	billCtx, cancelBill := context.WithTimeout(context.WithoutCancel(probeCtx), openAITurnStateProbeBillTimeout)
	defer cancelBill()
	defer s.recordProbeUsage(billCtx, account, cfg, c, req, resp.Header, attempt, usage, text)
	blob := extractOpenAICodexTurnState(resp.Header)
	if blob == "" {
		if !readAnswer {
			attempt.Error = "no turn-state in response"
		}
		return
	}
	attempt.Chars = len(blob)
	if !readAnswer {
		attempt.Healthy = openAITurnStateHealthy(blob)
	}
	// 与真实响应同一个入口：记铸造者、写形态观测、健康则入池。传原账号而不是出口副本，
	// 池子按账号 ID 读写，两者本来相同，这里只是不让副本流出去。
	s.gateway.relayOpenAICodexTurnState(c, account, resp.Header)
}

// recordProbeUsage 把一次 200 探测按标准用量路径落一行，挂在配置的 API Key 下走正常计费
// （余额/额度/倍率与人工流量同一套），request_type=probe 与人工流量区分。
//
// 猎手探测头到手即断，上游不会发 usage 事件：输入 token 用本地 tiktoken 估算（含该模型的
// base prompt，与 /responses/input_tokens 的本地回退同一个估算器），输出恒 0——上游在
// RST_STREAM 前生成了多少无从得知，金额因此是输入侧估算值。恢复探测读完了流，usage 非 nil
// 时按上游 response.completed 里的真实用量记。请求 ID 用上游 x-request-id（计费幂等键要求
// 每次探测唯一），没有就自造。
func (s *OpenAITurnStateHunterService) recordProbeUsage(ctx context.Context, account *Account, cfg openAITurnStateHunterConfig, c *gin.Context, req *http.Request, upstream http.Header, attempt *openAITurnStateHuntAttempt, usage *OpenAIUsage, text string) {
	if s == nil || s.apiKeys == nil || s.recordUsage == nil || cfg.UsageAPIKeyID <= 0 || account == nil || req == nil || attempt == nil {
		return
	}
	key, err := s.apiKeys.GetByID(ctx, cfg.UsageAPIKeyID)
	if err != nil || key == nil || key.User == nil {
		slog.Warn("openai_turn_state_probe_usage_key_unavailable", "account_id", account.ID, "api_key_id", cfg.UsageAPIKeyID, "error", err)
		return
	}
	// 订阅型分组按订阅额度计费，不能绕到余额上（RecordUsage 没拿到订阅就走余额扣费，还允许透支）。
	var subscription *UserSubscription
	if key.Group != nil && key.Group.IsSubscriptionType() {
		if s.subscriptions == nil {
			slog.Warn("openai_turn_state_probe_usage_subscription_unavailable", "account_id", account.ID, "api_key_id", key.ID)
			return
		}
		subscription, err = s.subscriptions.GetActiveSubscription(ctx, key.User.ID, key.Group.ID)
		if err != nil || subscription == nil {
			slog.Warn("openai_turn_state_probe_usage_no_active_subscription", "account_id", account.ID, "api_key_id", key.ID, "error", err)
			return
		}
	}
	requestID := strings.TrimSpace(upstream.Get("x-request-id"))
	if requestID == "" {
		requestID = "turn_state_probe:" + uuid.NewString()
	}
	effort := cfg.ReasoningEffort
	endpoint := req.URL.Path
	sessionID := ""
	if c != nil && c.Request != nil {
		sessionID = c.Request.Header.Get("session-id")
	}
	billed := OpenAIUsage{InputTokens: openAITurnStateProbeInputTokens(attempt.Model, effort, text)}
	if usage != nil {
		billed = *usage
	}
	input := &OpenAIRecordUsageInput{
		Result: &OpenAIForwardResult{
			RequestID:        requestID,
			UpstreamHeaders:  upstream,
			Usage:            billed,
			Model:            attempt.Model,
			UpstreamEndpoint: endpoint,
			ReasoningEffort:  &effort,
			Stream:           true,
			Duration:         time.Duration(attempt.LatencyMs) * time.Millisecond,
		},
		APIKey:           key,
		User:             key.User,
		Account:          account,
		Subscription:     subscription,
		InboundEndpoint:  "turn-state-probe",
		UpstreamEndpoint: endpoint,
		UserAgent:        req.Header.Get("User-Agent"),
		SessionID:        sessionID,
		APIKeyService:    s.apiKeys,
		PricingAt:        attempt.At,
		RequestType:      RequestTypeTurnStateProbe,
	}
	if err := s.recordUsage(ctx, input); err != nil {
		slog.Warn("openai_turn_state_probe_usage_record_failed", "account_id", account.ID, "api_key_id", cfg.UsageAPIKeyID, "error", err)
	}
}

// openAITurnStateProbeInputTokens 估算探测体的输入 token。探测体只有 instructions（base
// prompt）+ 一条用户消息 + 空工具表，身份字段不计数，所以用零值身份即可。估不出来记 1：
// 与 input_tokens 端点的本地回退同一个下限，行仍然要落（它确实发生了）。
func openAITurnStateProbeInputTokens(model, effort, text string) int {
	raw, err := json.Marshal(openAITurnStateProbeBody(model, effort, openAITurnStateProbeIdentity{}, text))
	if err != nil {
		return openAIInputTokensFallbackMinimum
	}
	var req openAIInputTokensCountRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return openAIInputTokensFallbackMinimum
	}
	n, err := estimateOpenAIInputTokens(req)
	if err != nil || n <= 0 {
		return openAIInputTokensFallbackMinimum
	}
	return n
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
	s.openaiTurnStateTraffic.Store(openAITurnStateTrafficKey(accountID, model), openAITurnStateTrafficMark{model: strings.TrimSpace(model), at: now})
}

// openAITurnStateTrafficMark 是水位条目：键里的模型名被小写化了，原样的名字存在值里，
// 自动模式要拿它去探测（上游模型名大小写敏感与否不赌）。
type openAITurnStateTrafficMark struct {
	model string
	at    time.Time
}

// noteOpenAITurnStateMinted 记「上游给该账号该模型自然铸过 turn-state」。只在内存里：重启后
// 由下一条未注入的真实响应重新建立。
func (s *OpenAIGatewayService) noteOpenAITurnStateMinted(accountID int64, model string) {
	if s == nil || strings.TrimSpace(model) == "" {
		return
	}
	s.openaiTurnStateMinted.Store(openAITurnStateTrafficKey(accountID, model), struct{}{})
}

// openAITurnStateImageModel 判画图模型：gpt-image-2 会铸 312，但画图不参与降智判定（用户口径）。
// ponytail: 子串匹配，将来名字含 image 的文本模型会被静默排除；到时改成前缀表。
func openAITurnStateImageModel(model string) bool {
	return strings.Contains(strings.ToLower(model), "image")
}

// openAITurnStateAutoHuntable 自动定模型时该模型够不够格猎（2026-09-19 按生产 30 天用量记录定的口径）：
//   - 上游给它铸过 turn-state。codex-auto-review / gpt-reserve / gpt-5.5 / gpt-5.3-codex-spark /
//     gpt-5.4-mini 这些从不铸，探了也拿不到票，只烧小时额度。铸造记忆在进程内；重启后先看
//     池子——池里有它的票（哪怕过期）就是铸过的证据，否则自动模式要等首条自然铸造才猎、才注入；
//   - 不是画图模型。
func (s *OpenAIGatewayService) openAITurnStateAutoHuntable(a *Account, model string) bool {
	if s == nil || a == nil || openAITurnStateImageModel(model) {
		return false
	}
	key := openAITurnStateTrafficKey(a.ID, model)
	if _, minted := s.openaiTurnStateMinted.Load(key); minted {
		return true
	}
	for _, c := range readOpenAITurnStatePool(a) {
		if strings.EqualFold(strings.TrimSpace(c.Model), strings.TrimSpace(model)) {
			s.openaiTurnStateMinted.Store(key, struct{}{}) // 补记，免得每次都解析池
			return true
		}
	}
	return false
}

// openAITurnStateTrafficModels 列出该账号 since 之后有过真实请求、且够格自动猎的模型
// （since 零值 = 不限时间），按名字排序让轮询顺序稳定。
func (s *OpenAIGatewayService) openAITurnStateTrafficModels(a *Account, since time.Time) []string {
	if s == nil || a == nil {
		return nil
	}
	prefix := openAITurnStateTrafficKey(a.ID, "")
	var out []string
	s.openaiTurnStateTraffic.Range(func(key, value any) bool {
		k, _ := key.(string)
		mark, ok := value.(openAITurnStateTrafficMark)
		if !ok || !strings.HasPrefix(k, prefix) || (!since.IsZero() && !mark.at.After(since)) || !s.openAITurnStateAutoHuntable(a, mark.model) {
			return true
		}
		out = append(out, mark.model)
		return true
	})
	sort.Strings(out)
	return out
}

func containsFold(list []string, want string) bool {
	for _, item := range list {
		if strings.EqualFold(strings.TrimSpace(item), want) {
			return true
		}
	}
	return false
}

func (s *OpenAIGatewayService) openAITurnStateTrafficSince(accountID int64, model string, since time.Time) bool {
	if s == nil {
		return false
	}
	raw, ok := s.openaiTurnStateTraffic.Load(openAITurnStateTrafficKey(accountID, model))
	if !ok {
		return false
	}
	mark, ok := raw.(openAITurnStateTrafficMark)
	return ok && mark.at.After(since)
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

// openAITurnStateProbeBody 是最小的第一回合请求体：字段集照真实 Codex CLI，内容只有一条
// 用户消息（猎手是 "hi"，恢复探测是糖果题）。instructions 用该模型的真实 base prompt（与真
// 客户端一致；猎手头到手即断，成本只有这段输入的 token）。
func openAITurnStateProbeBody(model, effort string, ids openAITurnStateProbeIdentity, text string) map[string]any {
	return map[string]any{
		"model":        model,
		"instructions": openai.CodexBaseInstructionsForModel(model),
		"input": []any{map[string]any{
			"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": text}},
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
func (s *OpenAIGatewayService) buildOpenAITurnStateProbe(ctx context.Context, account *Account, model, effort, text string) (*gin.Context, *http.Request, error) {
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
	decoded := openAITurnStateProbeBody(model, effort, ids, text)
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
	if err := validateOpenAITurnStateRecoveryExtra(extra); err != nil {
		return err
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
	autoModels := false
	for _, key := range []string{"hold_when_degraded", "auto_models"} {
		v, ok := table[key]
		if !ok || v == nil {
			continue
		}
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("%s.%s must be a boolean", openAITurnStateHunterExtraKey, key)
		}
		if key == "auto_models" {
			autoModels = b
		}
	}
	models, err := openAITurnStateHunterStringList(table, "models", openAITurnStateHunterMaxModels)
	if err != nil {
		return err
	}
	proxies, err := openAITurnStateHunterIDList(table, "proxy_ids", openAITurnStateHunterMaxProxies)
	if err != nil {
		return err
	}
	if _, err := openAITurnStateHunterIDList(table, "rotating_proxy_ids", openAITurnStateHunterMaxProxies); err != nil {
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
	if err := openAITurnStateHunterIntField(table, "usage_api_key_id", 0, math.MaxInt32); err != nil {
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
	if enabled && ((models == 0 && !autoModels) || proxies == 0) {
		return fmt.Errorf("%s is enabled but has no models (or auto_models) or no proxy_ids", openAITurnStateHunterExtraKey)
	}
	// 开窗提前量必须小于票的寿命（可配 openai_turn_state_stale_after_minutes），否则
	// 「任何票都永远不够用」，猎手每小时打满上限也停不下来。
	lead := float64(defaultOpenAITurnStateHuntLeadMinutes) // 没写就是默认值，同样要比
	if v, ok := openAITurnStateHunterNumber(table["lead_minutes"]); ok && v > 0 {
		lead = v
	}
	// stale_after 按运行时同一口径读：getExtraInt = int(parseExtraFloat64)，数字串 "5" 也收、
	// 小数截断。口径不一致这条交叉校验就会被 "5" 静默跳过、被 10.5 绕过。
	if stale := float64(int(parseExtraFloat64(extra[openAITurnStateStaleMinExtraKey]))); stale > 0 && lead >= stale {
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
