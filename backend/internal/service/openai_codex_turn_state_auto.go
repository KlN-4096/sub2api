package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// 自动接管 turn-state：检测到某个 session 落在 312（降智）后，把该账号最近一条
// 有效的 292 注入该 session 的后续请求；票一直用到自铸造起 1 小时自然过期。
//
// 候选失效只有一个来源——上游以 invalid_encrypted_content 拒绝这条 blob。曾经
// 「注入 292 后上游仍铸出 312」也算失效，那个判据是错的：实测 578 条现网样本里新铸
// 的块数由账号当时的权重决定，与请求带的那张票无关（「10 块 → 11 块」一次都没发生
// 过），所以上游铸 312 是账号权重的读数，不是这张票坏了的证据。而 fail_threshold
// 默认是 1，按那个判据注入一次就报废一张票，池子几分钟见底。
// 候选全部失效则停掉账号调度并写明原因。
//
// 为什么只在「已知 312 的 session」上注入，而不是无条件注入——实测 4262 条
// /responses：请求不带 turn-state 时 87.1% 会铸出新 blob，带了则只有 8.0%。
// 注入会把「能观测到长度」的机会压掉一个数量级，所以只在确实需要的 session 上注入，
// 其余路径保持真客户端形态（一轮首帧不带、后续带）。
const (
	// openAITurnStateAutoExtraKey 总开关。开启后手填覆写完全失效（系统接管）。
	openAITurnStateAutoExtraKey = "openai_turn_state_auto"
	// openAITurnStatePoolExtraKey 候选池，系统维护。只在自动接管开着时写。
	openAITurnStatePoolExtraKey = "openai_turn_state_pool"
	// openAITurnStateObservedExtraKey 每个模型最近一次观测到的形态（不含 blob）。
	// 对所有 Codex 账号都写，带节流，纯展示用。
	openAITurnStateObservedExtraKey = "openai_turn_state_observed"
	// 以下三个有配置项但不开放前端，按需用 API/DB 改。
	openAITurnStatePoolSizeExtraKey   = "openai_turn_state_pool_size"
	openAITurnStateFailThreshExtraKey = "openai_turn_state_fail_threshold"
	// openAITurnStateStaleMinExtraKey 是候选的有效期（分钟）。默认 60：实测对家实时池
	// 每张卡的「到期」都精确等于 Fernet 铸造戳 + 1 小时。键名沿用旧写法，避免已写进
	// extra 的值失效。
	openAITurnStateStaleMinExtraKey = "openai_turn_state_stale_after_minutes"
)

const (
	defaultOpenAITurnStatePoolSize      = 3
	defaultOpenAITurnStateFailThreshold = 1
	// defaultOpenAITurnStateStaleMinutes 是候选有效期：292 自铸造起可用 1 小时。
	defaultOpenAITurnStateStaleMinutes = 60
	// openAITurnStateSessionTTL 是 session 长度状态的存活期。turn-state blob 实测
	// 存活中位 2.6 分钟、最长 33 分钟，1 小时足够覆盖一个会话的活跃期。
	openAITurnStateSessionTTL = time.Hour
)

// turn-state 覆写来源，落 usage_logs.turn_state_source。
const (
	turnStateSourceManual = "manual"
	turnStateSourceAuto   = "auto"
	// 曾经还有 auto_stale（过保鲜期仍注入）。候选过期改成硬门槛后不再产生，
	// 历史行与用量筛选项里的这个取值由 usagestats.TurnStateFilterAutoStale 承接。
)

// gin 上下文键：注入时暂存，响应侧观测到新铸 blob 时读回来做失效判定。
const (
	ctxKeyTurnStateInjected = "openai_turn_state_injected"
	ctxKeyTurnStateSource   = "openai_turn_state_source"
	// ctxKeyTurnStateSkipAuto 由 WS 入口置位：该 gin.Context 属于一条长连接，
	// 自动接管的判定/失效判定都按「一次请求」设计，跟进去会失真。
	// 注意 WS ingress 的 HTTP 桥（openai_ws_http_bridge.go）会复用同一个 c 去走
	// passthrough 的出站构建，那条路径必须靠这个标记挡住。
	ctxKeyTurnStateSkipAuto = "openai_turn_state_skip_auto"
	// ctxKeyTurnStateObserved 记本次上下文已观测过的 blob：
	// applyAttemptResponseHeaders 有两个调用点，幂等只靠 c.Writer.Written()，
	// 同一条 blob 会被观测两次。观测路径现在不碰 FailStreak、也不会触发禁用（失效
	// 只由 invalid_encrypted_content 驱动），所以这道闸是纯开销保护：挡掉重复的
	// 形态写入与入池读写，别让每条响应白付两次。
	//
	// 字面量刻意与 extra 键 openAITurnStateObservedExtraKey 区分开：两个命名空间今天
	// 不相交，但重名会让下一个人一不小心写成 c.Set(openAITurnStateObservedExtraKey, …)
	// 而静默把这道承重的闸冲掉。
	ctxKeyTurnStateObserved = "openai_turn_state_observed_ctx"
	// ctxKeyTurnStateRejected 与 ctxKeyTurnStateObserved 同理：非 WSv2 路径会在
	// 剥掉 encrypted reasoning items 后重试一次，同一次注入可能撞两回 400。
	ctxKeyTurnStateRejected = "openai_turn_state_rejected"
	// ctxKeyTurnStateSent 记本次出站实际带的 blob，落 usage_logs.turn_state_sent。
	ctxKeyTurnStateSent = "openai_turn_state_sent"
)

// openAITurnStateCandidate 是候选池里的一条。
//
// Model 是铸出这条 blob 时实际发往上游的模型。turn-state 与模型强绑定，跨模型注入
// 拿不到满血路由、还会白撞一次 invalid_encrypted_content，所以候选必须带模型、
// 按模型取。没有 Model 的条目（本功能上线前写进去的）取不出来，等自然过期即可。
type openAITurnStateCandidate struct {
	Blob       string    `json:"blob"`
	Model      string    `json:"model,omitempty"`
	MintedAt   time.Time `json:"minted_at"`
	Failed     bool      `json:"failed,omitempty"`
	FailStreak int       `json:"fail_streak,omitempty"`
}

// usable 判这条候选此刻能不能注给 model。有效期是硬门槛，见 pickOpenAITurnStateCandidate。
func (c openAITurnStateCandidate) usable(model string, ttl time.Duration, now time.Time) bool {
	return c.alive(model) && !c.expired(ttl, now)
}

// alive 只看「没被判失效」，不看有效期：过期是「这轮不注入」，不是降级链被消耗，
// 不然池子自然老化就会把账号误停掉。
func (c openAITurnStateCandidate) alive(model string) bool {
	if c.Failed || strings.TrimSpace(c.Blob) == "" || c.Model == "" || model == "" {
		return false
	}
	// 大小写不敏感，与手填覆写表（OpenAICodexTurnStateOverride）的 EqualFold 同一套判据。
	// 两端都来自 SetOpsUpstreamModel 的同一份值，目前恒等；上游哪天改了模型名的
	// 大小写，精确比较会让整个功能静默失效，而不是报错。
	return strings.EqualFold(c.Model, model)
}

// openAITurnStateSessionState 记录某个 session 是否需要注入。
//
// 只有「未注入请求」铸出的 blob 才更新它：注入生效后上游会开始铸 292，若拿它回写
// 就会把 needsInjection 抹掉、下一轮又变回 312，在两个状态间来回跳。
type openAITurnStateSessionState struct {
	needsInjection bool
	expiresAt      time.Time
}

// openAITurnStatePoolMu 按账号串行化候选池读改写。extra 是 JSONB key 级合并，
// 并发下丢一次入池只是少一个候选，但丢一次 failed 标记会让失效账号继续打上游。
var openAITurnStatePoolMu sync.Map // accountID -> *sync.Mutex

func openAITurnStatePoolLock(accountID int64) *sync.Mutex {
	v, _ := openAITurnStatePoolMu.LoadOrStore(accountID, &sync.Mutex{})
	mu, _ := v.(*sync.Mutex)
	return mu
}

// IsOpenAITurnStateAutoEnabled 报告账号是否开启了自动接管。
// 与手填覆写同一条适用范围：只有最终落到 ChatGPT Codex 后端的账号才认这个头。
func (a *Account) IsOpenAITurnStateAutoEnabled() bool {
	if a == nil || !a.TargetsChatGPTCodexUpstream() {
		return false
	}
	return a.getExtraBool(openAITurnStateAutoExtraKey)
}

func (a *Account) openAITurnStatePoolSize() int {
	if n := a.getExtraInt(openAITurnStatePoolSizeExtraKey); n > 0 {
		return n
	}
	return defaultOpenAITurnStatePoolSize
}

func (a *Account) openAITurnStateFailThreshold() int {
	if n := a.getExtraInt(openAITurnStateFailThreshExtraKey); n > 0 {
		return n
	}
	return defaultOpenAITurnStateFailThreshold
}

func (a *Account) openAITurnStateStaleAfter() time.Duration {
	if n := a.getExtraInt(openAITurnStateStaleMinExtraKey); n > 0 {
		return time.Duration(n) * time.Minute
	}
	return defaultOpenAITurnStateStaleMinutes * time.Minute
}

// readOpenAITurnStatePool 读候选池。解析失败按空池处理——宁可不注入，也不要拿
// 半个损坏的结构去改写出站头。
func readOpenAITurnStatePool(account *Account) []openAITurnStateCandidate {
	if account == nil {
		return nil
	}
	raw, ok := account.Extra[openAITurnStatePoolExtraKey]
	if !ok || raw == nil {
		return nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var pool []openAITurnStateCandidate
	if err := json.Unmarshal(encoded, &pool); err != nil {
		return nil
	}
	return pool
}

// pickOpenAITurnStateCandidate 取第一条未失效且未过期的候选。
//
// 过期是硬门槛：实测对家的实时池六张卡，每张的「到期」都精确等于 Fernet 铸造戳 + 1
// 小时，所以 292 的可用期就是 1 小时。过期的 blob 注进去只会白白换来一次
// invalid_encrypted_content，不如不注入、直接等下一条自然铸出的 292。
//
// 注意：这里返回 false 只表示「这一轮不注入」，不是降级链被消耗，所以不会触发禁用。
func pickOpenAITurnStateCandidate(pool []openAITurnStateCandidate, model string, ttl time.Duration, now time.Time) (openAITurnStateCandidate, string, bool) {
	if model = strings.TrimSpace(model); model == "" {
		return openAITurnStateCandidate{}, "", false
	}
	for _, c := range pool {
		if c.usable(model, ttl, now) {
			return c, turnStateSourceAuto, true
		}
	}
	return openAITurnStateCandidate{}, "", false
}

// openAITurnStateModelAlive 判该模型下还有没有未失效的候选，用于耗尽判定。
func openAITurnStateModelAlive(pool []openAITurnStateCandidate, model string) bool {
	for _, c := range pool {
		if c.alive(model) {
			return true
		}
	}
	return false
}

// openAITurnStateRequestModel 取本次出站实际用的模型。各出站路径都会在分发前
// SetOpsUpstreamModel，注入点与观测点都在其后，所以这里读到的就是本次尝试的模型。
// 取不到就返回空——宁可不注入，也不要把票记到错误的模型名下。
func openAITurnStateRequestModel(c *gin.Context) string {
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.GetString(OpsUpstreamModelKey))
}

// expired 判候选是否已过铸造后 ttl。MintedAt 为零值说明信封解不出来，按不过期处理：
// 宁可注进去撞一次 400，也不要因为解码失败静默停掉整个功能。
func (c openAITurnStateCandidate) expired(ttl time.Duration, now time.Time) bool {
	return !c.MintedAt.IsZero() && !now.Before(c.MintedAt.Add(ttl))
}

// openAITurnStateSessionlessKey 是客户端不发 session-id 时的占位会话名。
//
// 不能就此返回空键：空键既查不到判定、也记不下判定，自动接管对 curl 和不发该头的
// 第三方客户端就等于根本没开，还不报错不打日志。退化成「账号 + 模型」粒度后功能
// 覆盖全部客户端，代价是这些请求共用一个降智判定——跨会话注入实测有效，这个代价
// 不成立。用不可能与真实 session id 相撞的字面量。
const openAITurnStateSessionlessKey = "\x00no-session"

// openAITurnStateSessionKey 把 session 状态按「凭证域 + 会话 + 模型」分域。
//
// 分账号：同一个 session id 在不同账号下是两段独立的上游会话，混用会让 A 账号的
// 降智判定作用到 B 账号。分模型：turn-state 与模型强绑定，同一 session 换模型就是
// 另一张票，A 模型被判降智不代表 B 模型也要注入。
func openAITurnStateSessionKey(c *gin.Context, account *Account, sessionID string) string {
	if sessionID = strings.TrimSpace(sessionID); sessionID == "" {
		sessionID = openAITurnStateSessionlessKey
	}
	owner := openAICodexTurnStateOwner(c, account)
	if owner == "" {
		return ""
	}
	model := openAITurnStateRequestModel(c)
	if model == "" {
		return ""
	}
	return owner + "\x1f" + sessionID + "\x1f" + model
}

// openAITurnStateRequestSessionID 取客户端会话标识。两种形态都要认：仓库里其它读会话头
// 的地方全都做双形态回退，只认连字符形态会让只发 session_id 的客户端静默失去这个功能。
func openAITurnStateRequestSessionID(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	return extractClientSessionID(c.Request.Header)
}

// sessionNeedsTurnStateInjection 查该 session 是否已被判定为降智。
func (s *OpenAIGatewayService) sessionNeedsTurnStateInjection(key string) bool {
	if s == nil || key == "" {
		return false
	}
	raw, ok := s.openaiTurnStateSessions.Load(key)
	if !ok {
		return false
	}
	st, ok := raw.(openAITurnStateSessionState)
	if !ok || (!st.expiresAt.IsZero() && time.Now().After(st.expiresAt)) {
		s.openaiTurnStateSessions.Delete(key)
		return false
	}
	return st.needsInjection
}

func (s *OpenAIGatewayService) setSessionTurnStateNeedsInjection(key string, needs bool) {
	if s == nil || key == "" {
		return
	}
	s.openaiTurnStateSessions.Store(key, openAITurnStateSessionState{
		needsInjection: needs,
		expiresAt:      time.Now().Add(openAITurnStateSessionTTL),
	})
	s.sweepOpenAITurnStateSessions()
}

// sweepOpenAITurnStateSessions 与 turn-state 溯源表同型的机会式清扫。
func (s *OpenAIGatewayService) sweepOpenAITurnStateSessions() {
	if s.openaiTurnStateSessionWrites.Add(1)%256 != 0 {
		return
	}
	now := time.Now()
	s.openaiTurnStateSessions.Range(func(key, value any) bool {
		st, ok := value.(openAITurnStateSessionState)
		if !ok || (!st.expiresAt.IsZero() && now.After(st.expiresAt)) {
			s.openaiTurnStateSessions.Delete(key)
		}
		return true
	})
}

// resolveOpenAITurnStateOverride 决定本次出站带什么 turn-state 覆写值。
//
// 优先级：自动接管 > 手填。开了自动就完全忽略 extra.openai_turn_state_override
// （值保留不删，关掉开关即恢复）——这是用户要求的「系统接管」语义。
//
// 返回空串表示不改写出站头。
func (s *OpenAIGatewayService) resolveOpenAITurnStateOverride(c *gin.Context, account *Account) (string, string) {
	if account == nil {
		return "", ""
	}
	// 两条路都按模型取票：turn-state 绑死在铸它的那个模型上，注给别的模型只会白撞
	// 一次 invalid_encrypted_content。取不到本次模型时两条路都不注入。
	model := openAITurnStateRequestModel(c)
	if !account.IsOpenAITurnStateAutoEnabled() {
		if manual := account.OpenAICodexTurnStateOverride(model); manual != "" {
			markOpenAITurnStateInjected(c, manual, turnStateSourceManual)
			return manual, turnStateSourceManual
		}
		return "", ""
	}
	if openAITurnStateAutoSkipped(c) {
		return "", ""
	}
	// 只在已判定降智的 session 上注入，其余保持真客户端形态——除非猎手在为本次模型
	// 补票：那时形态读数由猎手的探测提供，不再需要拿真实流量的首回合去试权重，池里
	// 有票就直接注，新会话第一回合也不裸奔。猎手不管的模型仍走「先判定再注入」。
	key := openAITurnStateSessionKey(c, account, openAITurnStateRequestSessionID(c))
	degraded := s.sessionNeedsTurnStateInjection(key)
	if !degraded && !account.openAITurnStateHuntedModel(model) {
		return "", ""
	}
	// 必须读新鲜池，不能读请求手里的 account 快照。
	//
	// 那个快照来自调度器的 Redis 副本（hydrateSelectedAccount → scheduler_snapshot_service），
	// 而 openai_turn_state_pool 在 schedulerNeutralExtraKeys 里——池子写入刻意不触发
	// 快照重建（否则每条响应都要重建一次调度快照），于是副本最多陈旧一整个
	// full_rebuild_interval_seconds（默认 300s）。两个后果都不能忍：
	//   - 刚补进池的新票要等下一轮 rebuild 才注得出去，「补票」这件事等于慢五分钟；
	//   - 已判 Failed 的候选在陈旧副本里仍是 alive，会被反复注入，而 record 侧从新鲜池
	//     里找不到这条 blob → changed 恒 false → 耗尽判定和停号整段都走不到。
	//
	// ponytail: 代价是降智 session 的每个请求多一次 GetByID。本功能是单账号诊断用途、
	// 低并发，且这条路径本来就会在响应收尾时同步写一次库；真要上量再加个短 TTL 缓存。
	pool := s.loadOpenAITurnStatePoolFresh(turnStateOpCtx(c), account)
	candidate, source, ok := pickOpenAITurnStateCandidate(
		pool, model, account.openAITurnStateStaleAfter(), time.Now())
	if !ok {
		// 「判了降智但拿不出票」是接管停摆的唯一形态，不打日志就只能靠猜。猎手路径上
		// 池空是还没摇到票的常态，每条请求都刷一行只会淹没日志。
		if degraded {
			logOpenAITurnStateAuto("account=%d model=%s degraded but no usable candidate (pool=%d)",
				account.ID, model, len(pool))
		}
		return "", ""
	}
	markOpenAITurnStateInjected(c, candidate.Blob, source)
	return candidate.Blob, source
}

// markOpenAITurnStateInjected 把本次覆写值与来源存进请求上下文。
// 手填与自动接管都要存：使用记录读来源，WS 帧填充读值判断「本次是不是覆写」
// （applyCodexWSFrameWireProfile），响应侧读值做失效判定。
func markOpenAITurnStateInjected(c *gin.Context, blob, source string) {
	if c == nil || blob == "" {
		return
	}
	c.Set(ctxKeyTurnStateInjected, blob)
	c.Set(ctxKeyTurnStateSource, source)
}

// markOpenAITurnStateAutoSkipped 声明本次上下文不参与自动接管（WS 入口调用）。
func markOpenAITurnStateAutoSkipped(c *gin.Context) {
	if c != nil {
		c.Set(ctxKeyTurnStateSkipAuto, true)
	}
}

func openAITurnStateAutoSkipped(c *gin.Context) bool {
	if c == nil {
		return false
	}
	skip, _ := c.Get(ctxKeyTurnStateSkipAuto)
	flag, _ := skip.(bool)
	return flag
}

// clearOpenAITurnStateInjected 清掉上一次 failover attempt 留下的注入标记。
// c 在整个重试循环里是同一个：不清的话换号之后仍会读到上一个账号注入的 blob，
// 失效判定会记到新账号头上，session 判定也永远更新不了。
func clearOpenAITurnStateInjected(c *gin.Context) {
	if c != nil {
		c.Set(ctxKeyTurnStateInjected, "")
		c.Set(ctxKeyTurnStateSource, "")
	}
}

// markOpenAITurnStateSent 记下本次出站实际带的 turn-state（空值不记，保持 NULL）。
func markOpenAITurnStateSent(c *gin.Context, account *Account, sent string) {
	if c == nil || account == nil || !account.TargetsChatGPTCodexUpstream() {
		return
	}
	c.Set(ctxKeyTurnStateSent, strings.TrimSpace(sent))
}

// OpenAITurnStateUsageSent 供 handler 在还持有 gin.Context 时取出出站值。
func OpenAITurnStateUsageSent(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if v, ok := c.Get(ctxKeyTurnStateSent); ok {
		sent, _ := v.(string)
		return sent
	}
	return ""
}

// openAITurnStateInjectedFromContext 返回本次请求注入的覆写值，没有则空串。
func openAITurnStateInjectedFromContext(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if v, ok := c.Get(ctxKeyTurnStateInjected); ok {
		blob, _ := v.(string)
		return blob
	}
	return ""
}

// OpenAITurnStateUsageSource 取出本次请求实际注入的 turn-state 覆写来源，没注入返回空串。
//
// 由 handler 在还持有 gin.Context 时调用，随 OpenAIRecordUsageInput 交给异步计费，
// 与 ExtractClientSessionID 同一套路。
func OpenAITurnStateUsageSource(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if v, ok := c.Get(ctxKeyTurnStateSource); ok {
		source, _ := v.(string)
		return source
	}
	return ""
}

// observeOpenAITurnStateMint 是响应侧唯一入口：上游铸出新 blob 时调用。
//
// 三件事，分别有各自的门禁：
//  1. **形态观测**（所有 Codex 账号，但只认本次没注入的自然铸造）：把这次铸出来的
//     块数/字符数记进 openai_turn_state_observed，健康与否都记。账号页靠它回答「这个
//     号现在铸的是 292 还是 312」，而那是判断该不该开自动接管的前提。带节流，见
//     noteOpenAITurnStateObservation。
//  2. **候选入池**（只在自动接管开着时）：健康 blob 进候选池供注入用。这条是同步
//     读库+写库，只有主动开了接管的诊断账号付这个钱。
//  3. **session 降智判定**（只在自动接管开着时）：未注入的请求铸出的形态决定该
//     session 后续要不要注入。
//
// 任何一步都不该阻塞响应，失败只记日志。
func (s *OpenAIGatewayService) observeOpenAITurnStateMint(c *gin.Context, account *Account, minted string) {
	if s == nil || account == nil || !account.TargetsChatGPTCodexUpstream() {
		return
	}
	minted = strings.TrimSpace(minted)
	if minted == "" {
		return
	}
	if c != nil {
		if seen, _ := c.Get(ctxKeyTurnStateObserved); seen == minted {
			return
		}
		c.Set(ctxKeyTurnStateObserved, minted)
	}
	// 真实流量水位：猎手只在有人用的模型上续票。探测自己不算。
	if !openAITurnStateProbeContext(c) {
		s.noteOpenAITurnStateTraffic(account.ID, openAITurnStateRequestModel(c), time.Now())
	}
	healthy := openAITurnStateHealthy(minted)
	injected := openAITurnStateInjectedFromContext(c)

	// 形态观测对所有 Codex 账号都做，与接管开关无关：账号页要能回答「这个号现在铸的是
	// 292 还是 312」，而那正是判断该不该开接管的前提——关着就什么都不记的话，「没开
	// 接管」「开了但池空」「正在铸 312」在页面上长得一模一样，等于鸡生蛋。
	//
	// 但**只认自然铸造**（本次没注入），与下面的 session 判定同一道闸。这个入口是
	// 「上游响应里带了这个头就调」，而带 turn-state 的请求只有 8.0% 会重铸、另外 92%
	// 上游原样回带——注入一开，记下来的就是我们自己那张 292 的回声。页面于是在账号
	// 仍然铸 312 的时候报绿，运维看绿关掉接管，立刻又吃 312；「正在铸 312」和「接管
	// 正在生效」被合并成同一种显示，恰好毁掉这条记录存在的理由。
	//
	// 只记形态、不记 blob：blob 是上游令牌，无条件存进每个账号的 extra 就等于让它随
	// 账号列表接口下发、进每一份 DB dump。形态（块数/字符数）足够回答上面那个问题。
	if injected == "" {
		s.noteOpenAITurnStateObservation(c, account, minted, healthy)
	}

	if !account.IsOpenAITurnStateAutoEnabled() {
		return
	}

	// 入池刻意留在接管门禁之后：它是同步的读库+写库（GetByID 连带 loadProxies /
	// loadAccountGroups 共 3 条 SELECT，再加一条 UPDATE），就在响应首字节之前、还持着
	// 账号锁。而「请求不带 turn-state 时 87.1% 会铸出新值」（见文件头），放到门禁之前
	// 等于给每个 Codex 账号的每一条响应都加上这笔开销——同一行 accounts 每请求一次
	// UPDATE，行锁排队加死元组堆积。只有主动开了接管的诊断账号该付这个钱。
	//
	// 入池不分「本次有没有注入」：一个 session 被判降智后每条请求都带注入，若入池只认
	// 未注入的请求，降智账号就补不到票——池子只出不进，候选到期后自动接管静默停摆。
	// （补票实际来自同账号其它未降智的 session：降智 session 注入后上游照样铸 312。）
	if healthy {
		s.pushOpenAITurnStateCandidate(c, account, minted)
	}

	// 只在本次没注入时回写 session 判定。
	//
	// 这道闸原本的理由是「注入一生效就把降智标记抹掉，两个状态来回跳」，那个理由已经
	// 不成立：铸什么由账号当时的权重定，与请求带的票无关（见下方长注释），注入并不会
	// 把铸造结果掰成 292，所以注入时的观测并不比不注入时脏。
	//
	// 留着它是因为成本不对称：判过降智之后继续注入几乎不要钱（票已经在池里，注入不
	// 消耗它），而凭单次健康铸造就停掉注入，下一轮撞上权重抖动就是用户实打实吃一个
	// 降智回合。代价是恢复判定要晚一步——账号权重回正后，该 session 仍会一直注到
	// session 标记自己过期（openAITurnStateSessionTTL），此后的请求重新按自然铸造判。
	// 探测的 session 是一次性的（每次新 UUID，永远不会再出现），不给它留 session 判定。
	if injected == "" && !openAITurnStateProbeContext(c) {
		if key := openAITurnStateSessionKey(c, account, openAITurnStateRequestSessionID(c)); key != "" {
			s.setSessionTurnStateNeedsInjection(key, !healthy)
		}
	}

	// 刻意不在这里记候选的成败。
	//
	// 这里曾经在「注入了 292、上游仍铸出 312」时给该候选记一次失败，而 fail_threshold
	// 默认是 1——注入一次就报废一张票，池子几分钟见底，接着客户端回带的 312 原样裸奔
	// 出站，池子恰好空到底时还会直接触发耗尽停号。
	//
	// 判据本身是错的：实测 578 条现网样本里，新铸的块数由账号当时的权重决定，与请求
	// 带的那张票无关（「10 块 → 11 块」一次都没发生过，而「注入有效期内的 292、上游
	// 仍铸 312」是常态）。上游铸 312 是账号权重的读数，不是这张票坏了的证据。
	//
	// 票坏了的硬证据只有一个：上游回 invalid_encrypted_content，那条走
	// noteOpenAITurnStateRejected。票在此之前一直用到自然过期。
}

// openAITurnStateObservation 是这个账号最近一次自然铸造出来的 turn-state 形态。
//
// **一个账号只存一条，不按模型建表。** 铸什么由账号当时的权重决定，与请求带的票无关，
// 也与模型无关——这是个账号级读数。按模型建表会付两笔没必要的代价：
//
//   - 丢失更新。整张表作为一个顶层键写进 extra，而 JSONB `||` 是顶层键粒度替换。两个
//     模型的请求各自持有自己的账号快照（每个请求都从 scheduler cache 独立反序列化），
//     从选号到响应收尾隔着整个上游往返，后写的那张表会把先写的那个模型整条抹掉。抹掉
//     之后该模型下一条响应查不到上一条 → 节流失效 → 再写一次 → 再有机会丢，双模型并发
//     下是个自激循环，正好撞在「不能给每条响应加同步读写」上。
//   - 无界增长。模型键取自下游请求体里的原串（只 TrimSpace，不归一大小写、不过白名单），
//     大小写变体各占一格，模型本身还随版本轮换。兄弟表 openai_turn_state_override 为此
//     设了 maxOpenAITurnStateOverrideModels 上限，这里连淘汰都没有。
//
// 单条记录把两个问题一起消掉：并发写覆盖的是同样有效的值，条目数恒为 1。Model 降级成
// 记录里的一个字段，只用来说明「这个读数是哪个模型的请求带回来的」。
//
// 刻意不含 blob：这条记录对所有 Codex 账号都写，而 blob 是上游令牌——无条件存进
// extra 就等于让它随账号列表接口下发、进每一份 DB dump 和账号导出。块数与字符数
// 足够回答「现在铸的是 292 还是 312」，那是这条记录存在的全部目的。
type openAITurnStateObservation struct {
	Model    string    `json:"model"`
	Blocks   int       `json:"blocks"`
	Chars    int       `json:"chars"`
	Healthy  bool      `json:"healthy"`
	MintedAt time.Time `json:"minted_at"`
	// ObservedAt 是写下这条记录的时刻，只用于节流。不能拿 MintedAt 当节流基准：那是
	// blob 自己的信封时间戳，观测到一条已经很老的 blob 时，写进去的还是那个老时间戳，
	// 下一条响应再判一次还是过期 —— 退化成每响应一次 UPDATE。
	ObservedAt time.Time `json:"observed_at"`
}

// openAITurnStateObserveInterval 是形态观测的写节流窗口。
//
// 刻意与候选有效期（openai_turn_state_stale_after_minutes，可调）解耦：那是「票还能不能
// 用」，这是「多久往库里写一次」，共用一个值的话把 stale_after 调小就会把写频顶上去。
const openAITurnStateObserveInterval = 5 * time.Minute

// readOpenAITurnStateObservation 读形态观测记录。解析失败按「没有」处理。
//
// 走 marshal/unmarshal 而不是裸类型断言：extra 是 JSONB，同一个键在「刚写进去」和
// 「从 DB / Redis 读回来」两条路径上的具体 Go 类型不保证相同，裸断言失败是静默的。
func readOpenAITurnStateObservation(a *Account) (openAITurnStateObservation, bool) {
	if a == nil {
		return openAITurnStateObservation{}, false
	}
	raw, ok := a.Extra[openAITurnStateObservedExtraKey]
	if !ok || raw == nil {
		return openAITurnStateObservation{}, false
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return openAITurnStateObservation{}, false
	}
	var rec openAITurnStateObservation
	if err := json.Unmarshal(encoded, &rec); err != nil {
		return openAITurnStateObservation{}, false
	}
	return rec, true
}

// noteOpenAITurnStateObservation 记一次形态观测。所有 Codex 账号都走这条。
//
// 带节流：形态没变且上一条写下还不到 openAITurnStateObserveInterval 就不写。上游铸新值
// 的频率很高（不带 turn-state 的请求 87.1% 会铸），不节流的话这里就是每响应一次 UPDATE，
// 而 UpdateExtra 对中性键仍会连带一次 GetByID（3 条 SELECT）+ 一次 Redis SetAccount。
// 形态变了要立刻写——那正是要看的事，不该被节流窗口压住。
//
// 读的是请求手里的账号快照，不是 GetByID。单条记录没有候选池那种「并发请求的 Failed 位
// 不能被抹掉」的不变量要守：并发写覆盖的是同样有效的读数，快照陈旧最多让节流多放过
// 几次写。
func (s *OpenAIGatewayService) noteOpenAITurnStateObservation(c *gin.Context, account *Account, blob string, healthy bool) {
	if s == nil || s.accountRepo == nil || account == nil {
		return
	}
	now := time.Now().UTC()
	next := openAITurnStateObservation{
		Model:      openAITurnStateRequestModel(c),
		Chars:      len(blob),
		Healthy:    healthy,
		MintedAt:   openAITurnStateMintedAt(blob, now),
		ObservedAt: now,
	}
	if env, ok := parseOpenAITurnStateEnvelope(blob); ok {
		next.Blocks = env.CipherBlocks
	}

	if prev, ok := readOpenAITurnStateObservation(account); ok &&
		prev.Blocks == next.Blocks && prev.Chars == next.Chars &&
		now.Before(prev.ObservedAt.Add(openAITurnStateObserveInterval)) {
		return
	}

	encoded, err := json.Marshal(next)
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
	account.Extra[openAITurnStateObservedExtraKey] = generic
	if err := s.accountRepo.UpdateExtra(turnStateOpCtx(c), account.ID, map[string]any{
		openAITurnStateObservedExtraKey: generic,
	}); err != nil {
		logOpenAITurnStateAuto("persist observation failed: account=%d err=%v", account.ID, err)
	}
}

// turnStateOpCtx 取一个不随请求取消的 ctx：候选池维护要在响应收尾后照常落库。
func turnStateOpCtx(c *gin.Context) context.Context {
	if c != nil && c.Request != nil {
		return context.WithoutCancel(c.Request.Context())
	}
	return context.Background()
}

// loadOpenAITurnStatePoolFresh 在锁内重新读账号，拿最新候选池。
//
// 请求手里的 *Account 是选号时刻的快照，而候选池是在响应收尾时才改的——中间隔着
// 整个上游流式回合（秒级到分钟级）。拿陈旧快照做读-改-写，会把并发请求刚写下的
// Failed 标记抹掉：失效候选复活，降级链永远走不到耗尽，账号也就永远不会被停。
// 读失败退回请求快照：宁可写一次旧的，也不要把一次失效判定整个丢掉。
func (s *OpenAIGatewayService) loadOpenAITurnStatePoolFresh(ctx context.Context, account *Account) []openAITurnStateCandidate {
	if s.accountRepo != nil {
		if latest, err := s.accountRepo.GetByID(ctx, account.ID); err == nil && latest != nil {
			return readOpenAITurnStatePool(latest)
		}
	}
	return readOpenAITurnStatePool(account)
}

// noteOpenAITurnStateRejected 上游以 invalid_encrypted_content 拒绝了本次请求。
//
// 只在**本次确实注入过** turn-state 时才算到候选头上：这个错码的主用途是 reasoning
// 的 encrypted_content lineage（见 openai_encrypted_content_lineage.go），跟 turn-state
// 没关系；不加这道闸就会把别人的锅记到候选上、把好候选判死。
//
// 这是候选失效的**唯一**来源。曾经「注入 292、上游仍铸 312」也会记一次失败，那条
// 判据已移除——铸什么由账号当时的权重决定，与请求带的那张票无关，把它当失效证据
// 会在阈值 1 下一次烧掉一张票。
func (s *OpenAIGatewayService) noteOpenAITurnStateRejected(c *gin.Context, account *Account) {
	if s == nil || account == nil || !account.IsOpenAITurnStateAutoEnabled() {
		return
	}
	injected := openAITurnStateInjectedFromContext(c)
	if injected == "" {
		return
	}
	if c != nil {
		if seen, _ := c.Get(ctxKeyTurnStateRejected); seen == injected {
			return
		}
		c.Set(ctxKeyTurnStateRejected, injected)
	}
	logOpenAITurnStateAuto("account=%d injected turn-state rejected by upstream (invalid_encrypted_content)", account.ID)
	s.recordOpenAITurnStateFailure(c, account, injected)
}

// pushOpenAITurnStateCandidate 把新铸的健康 blob 推入候选池栈顶。
func (s *OpenAIGatewayService) pushOpenAITurnStateCandidate(c *gin.Context, account *Account, blob string) {
	ctx := turnStateOpCtx(c)
	mu := openAITurnStatePoolLock(account.ID)
	mu.Lock()
	defer mu.Unlock()

	model := openAITurnStateRequestModel(c)
	if model == "" {
		return // 归不到模型的票没法用，收了也只是占位
	}

	pool := s.loadOpenAITurnStatePoolFresh(ctx, account)
	for _, existing := range pool {
		if existing.Blob == blob {
			return // 同一条 blob 会在一轮内被反复回带，别重复入池
		}
	}
	// 铸造时刻从信封里读（实测比观测时刻早 1–367 秒）；解不出来才退回观测时刻。
	now := time.Now().UTC()
	minted := openAITurnStateMintedAt(blob, now)

	// 限深按模型算：全局截断会让活跃模型把冷门模型的票挤光，那个模型就永远补不上。
	//
	// 刻意不在这里按有效期清理。过期与失效是两件事：alive() 不看有效期，为的是
	// 「池子自然老化」不要被当成降级链走完而停掉账号。入池时把过期条目物理删掉
	// 等于绕过那条不变量——同一模型下最后一条新鲜候选失败时，本该还剩的格子已经
	// 没了。过期条目靠本模型的配额压力自然出局，数量有界（每模型至多 size 条）。
	size := account.openAITurnStatePoolSize()
	kept := make([]openAITurnStateCandidate, 0, len(pool)+1)
	perModel := map[string]int{model: 1}
	for _, existing := range pool {
		if existing.Model == "" || perModel[existing.Model] >= size {
			continue
		}
		perModel[existing.Model]++
		kept = append(kept, existing)
	}
	pool = append([]openAITurnStateCandidate{{Blob: blob, Model: model, MintedAt: minted}}, kept...)
	s.persistOpenAITurnStatePool(c, account, pool)
}

// recordOpenAITurnStateFailure 给一条候选记一次失败；候选耗尽时停掉账号调度。
//
// 唯一的调用方是 noteOpenAITurnStateRejected——上游明确拒绝这条 blob 才算失败。
// 刻意没有「成功」的对侧动作（曾经有过一个重置 FailStreak 的分支）：票用得好好的
// 时候上游不给任何信号，而「上游铸出 292」不是这张票的功劳（铸什么由账号权重定），
// 拿它去重置计数只是把同一个误判换个方向再做一遍。
func (s *OpenAIGatewayService) recordOpenAITurnStateFailure(c *gin.Context, account *Account, injected string) {
	model := openAITurnStateRequestModel(c)
	ctx := turnStateOpCtx(c)
	mu := openAITurnStatePoolLock(account.ID)
	mu.Lock()
	defer mu.Unlock()

	pool := s.loadOpenAITurnStatePoolFresh(ctx, account)
	threshold := account.openAITurnStateFailThreshold()
	changed, exhausted := false, false
	for i := range pool {
		if pool[i].Blob != injected {
			continue
		}
		pool[i].FailStreak++
		changed = true
		if pool[i].FailStreak >= threshold {
			pool[i].Failed = true
		}
		break
	}
	if !changed {
		// 注入发生在出站、拒绝发生在上游往返之后，这中间同账号其它 session 只要推进
		// 满一个池深（默认 3）就会把这张票挤出池子。等 400 回来就找不到它了——失效
		// 记不上、耗尽判不出、号停不掉，而失效来源本来就只剩这一个稀有事件。
		// 不打日志的话这种丢失完全不可见。
		logOpenAITurnStateAuto(
			"account=%d model=%s rejected turn-state is no longer in pool, failure dropped (pool=%d)",
			account.ID, model, len(pool))
		return
	}
	// 耗尽只看「该模型下还有没有未失效的候选」，不看有效期：过期是等新票，不是降级链走完。
	exhausted = model != "" && !openAITurnStateModelAlive(pool, model)
	s.persistOpenAITurnStatePool(c, account, pool)
	if exhausted {
		s.disableAccountForExhaustedTurnState(c, account, model, pool)
	}
}

// persistOpenAITurnStatePool 写回候选池。调用方必须已持有账号锁。
//
// ponytail: 这是响应路径上的一次同步 DB 写（首个输出事件与下游首字节之间），
// 且持锁。本功能是单账号诊断用途、低并发，先按最简做法落地；真要上量再改成
// 「异步 + 按账号合批」。openai_turn_state_pool 已在 schedulerNeutralExtraKeys
// 里，这次写入不会牵连调度快照重建。
func (s *OpenAIGatewayService) persistOpenAITurnStatePool(c *gin.Context, account *Account, pool []openAITurnStateCandidate) {
	if s.accountRepo == nil {
		return
	}
	encoded, err := json.Marshal(pool)
	if err != nil {
		return
	}
	var generic []any
	if err := json.Unmarshal(encoded, &generic); err != nil {
		return
	}
	if account.Extra == nil {
		account.Extra = map[string]any{}
	}
	account.Extra[openAITurnStatePoolExtraKey] = generic

	if err := s.accountRepo.UpdateExtra(turnStateOpCtx(c), account.ID, map[string]any{
		openAITurnStatePoolExtraKey: generic,
	}); err != nil {
		logOpenAITurnStateAuto("persist pool failed: account=%d err=%v", account.ID, err)
	}
}

// disableAccountForExhaustedTurnState 候选全部失效：停调度 + 写明原因。
//
// 用 schedulable=false 而不是 temp_unschedulable_until：候选池已空，到点自动恢复
// 只会立刻再失败一轮。要人工介入（补一条新的 292 或关掉自动接管）才有意义。
func (s *OpenAIGatewayService) disableAccountForExhaustedTurnState(c *gin.Context, account *Account, model string, pool []openAITurnStateCandidate) {
	if s.accountRepo == nil {
		return
	}
	ctx := turnStateOpCtx(c)
	reason := buildExhaustedTurnStateReason(model, pool)
	// 先写原因再停调度：反过来一旦 SetError 失败，管理页看到的就是一个没有任何
	// 理由的停用账号，比「还在跑但已经标了红」难排查得多。
	if err := s.accountRepo.SetError(ctx, account.ID, reason); err != nil {
		logOpenAITurnStateAuto("set error failed: account=%d err=%v", account.ID, err)
		return
	}
	if err := s.accountRepo.SetSchedulable(ctx, account.ID, false); err != nil {
		logOpenAITurnStateAuto("disable failed: account=%d err=%v", account.ID, err)
		return
	}
	account.Schedulable = false
	logOpenAITurnStateAuto("account=%d disabled: %s", account.ID, reason)
}

// buildExhaustedTurnStateReason 写给管理页看的禁用原因：必须一眼看懂「为什么停了」。
func buildExhaustedTurnStateReason(model string, pool []openAITurnStateCandidate) string {
	var b strings.Builder
	// 失效的唯一来源是上游明确拒绝这条 blob（invalid_encrypted_content）。曾经「注入
	// 292、上游仍铸 312」也算失败，那个判据已移除——铸什么由账号权重定，与带的票无关。
	fmt.Fprintf(&b,
		"模型 %s 下 turn-state 自动接管的候选已全部失效：注入后被上游以 "+
			"invalid_encrypted_content 拒绝，已停止调度。",
		model)
	// 只列这个模型的失效候选：别的模型的票与这次判定无关，列出来只会误导排查。
	failed := make([]openAITurnStateCandidate, 0, len(pool))
	for _, c := range pool {
		if c.Failed && c.Model == model {
			failed = append(failed, c)
		}
	}
	sort.Slice(failed, func(i, j int) bool { return failed[i].MintedAt.After(failed[j].MintedAt) })
	for i, c := range failed {
		blob := c.Blob
		if len(blob) > 16 {
			blob = blob[:16] + "…"
		}
		// 字符数在前（运维实际在说 292/312），块数作为判据补在后面。
		// 解不出信封就只写字符数，别打印「密文 0 块」——那不是事实，是解码失败。
		shape := fmt.Sprintf("%d 字符(信封解不开)", len(c.Blob))
		if env, ok := parseOpenAITurnStateEnvelope(c.Blob); ok {
			shape = fmt.Sprintf("%d 字符/密文 %d 块", len(c.Blob), env.CipherBlocks)
		}
		// 「累计」不是「连续」：成功重置那条分支已随错误判据一起删掉，FailStreak 再没有
		// 清零路径。默认阈值 1 时看不出差别，配成 2 时语义就是「这张票一辈子累计 2 次」。
		fmt.Fprintf(&b, " 候选%d=%s(%s，铸于 %s，累计失败 %d 次)",
			i+1, blob, shape, c.MintedAt.Format("01-02 15:04"), c.FailStreak)
	}
	// 账号已经停调度，不可能自己再铸出新 blob——恢复路径必须是人工的，别写成「等它自愈」。
	_, _ = b.WriteString(" 处理：关闭自动接管开关后重新启用账号，或先关开关、手填一条新的健康 turn-state 再启用。")
	return b.String()
}

func logOpenAITurnStateAuto(format string, args ...any) {
	logger.LegacyPrintf("service.openai_turn_state_auto", format, args...)
}
