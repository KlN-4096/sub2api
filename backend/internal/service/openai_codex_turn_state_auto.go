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
// 有效的 292 注入该 session 的后续请求；注入后上游若仍铸出 312，判定该候选失效，
// 降级到下一条；候选全部失效则停掉账号调度并写明原因。
//
// 为什么只在「已知 312 的 session」上注入，而不是无条件注入——实测 4262 条
// /responses：请求不带 turn-state 时 87.1% 会铸出新 blob，带了则只有 8.0%。
// 注入会把「能观测到长度」的机会压掉一个数量级，所以只在确实需要的 session 上注入，
// 其余路径保持真客户端形态（一轮首帧不带、后续带）。
const (
	// openAITurnStateAutoExtraKey 总开关。开启后手填覆写完全失效（系统接管）。
	openAITurnStateAutoExtraKey = "openai_turn_state_auto"
	// openAITurnStatePoolExtraKey 候选池，系统维护。
	openAITurnStatePoolExtraKey = "openai_turn_state_pool"
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
	// openAIHealthyTurnStateLen 是「不降智」的字符长度，只在信封解不开时兜底、
	// 以及前端展示用。真正的判据是密文块数，见 openai_codex_turn_state_envelope.go。
	openAIHealthyTurnStateLen = 292
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
	// 同一条 blob 可能被观测两次 → FailStreak 双增、禁用动作连发两次。
	ctxKeyTurnStateObserved = "openai_turn_state_observed"
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
	// 大小写不敏感，与 openAITurnStateModelAllowed 的名单匹配保持同一套判据。
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

// openAITurnStateSessionKey 把 session 状态按「凭证域 + 模型」分域。
//
// 分账号：同一个 session id 在不同账号下是两段独立的上游会话，混用会让 A 账号的
// 降智判定作用到 B 账号。分模型：turn-state 与模型强绑定，同一 session 换模型就是
// 另一张票，A 模型被判降智不代表 B 模型也要注入。
func openAITurnStateSessionKey(c *gin.Context, account *Account, sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ""
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
	// 名单同时约束手填与自动接管：turn-state 换模型就不认，注给名单外的模型只会
	// 白撞一次 invalid_encrypted_content。
	if !account.openAITurnStateModelAllowed(openAITurnStateRequestModel(c)) {
		return "", ""
	}
	if !account.IsOpenAITurnStateAutoEnabled() {
		if manual := account.OpenAICodexTurnStateOverride(); manual != "" {
			markOpenAITurnStateInjected(c, manual, turnStateSourceManual)
			return manual, turnStateSourceManual
		}
		return "", ""
	}
	if openAITurnStateAutoSkipped(c) {
		return "", ""
	}
	// 只在已判定降智的 session 上注入，其余保持真客户端形态。
	key := openAITurnStateSessionKey(c, account, openAITurnStateRequestSessionID(c))
	if !s.sessionNeedsTurnStateInjection(key) {
		return "", ""
	}
	candidate, source, ok := pickOpenAITurnStateCandidate(
		readOpenAITurnStatePool(account), openAITurnStateRequestModel(c),
		account.openAITurnStateStaleAfter(), time.Now())
	if !ok {
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
// 两件事：
//  1. 未注入的请求铸出的长度，决定该 session 是否需要注入（注入后铸出的不回写，
//     否则注入一生效就把降智标记抹掉，下一轮又变回来，来回跳）。
//  2. 注入过的请求铸出 312 → 本次注入的候选失效；铸出 292 → 候选有效，streak 清零。
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
	healthy := openAITurnStateHealthy(minted)

	if !account.IsOpenAITurnStateAutoEnabled() {
		return
	}
	injected := openAITurnStateInjectedFromContext(c)

	if injected == "" {
		// 自然铸造：它才代表这个 session 真实落在哪个档位，也只有它配当候选
		//（注入请求铸出的 blob 会把刚投进去的候选挤出定深池，失效判定还没攒够就没了）。
		if key := openAITurnStateSessionKey(c, account, openAITurnStateRequestSessionID(c)); key != "" {
			s.setSessionTurnStateNeedsInjection(key, !healthy)
		}
		if healthy {
			s.pushOpenAITurnStateCandidate(c, account, minted)
		}
		return
	}
	s.recordOpenAITurnStateOutcome(c, account, injected, healthy)
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
// 与「铸出更多块」那条判据并列，走同一个 recordOpenAITurnStateOutcome：
// 阈值、降级链、耗尽停号的语义完全一致。
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
	s.recordOpenAITurnStateOutcome(c, account, injected, false)
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

// recordOpenAITurnStateOutcome 记录一次注入的结果；候选耗尽时停掉账号调度。
func (s *OpenAIGatewayService) recordOpenAITurnStateOutcome(c *gin.Context, account *Account, injected string, healthy bool) {
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
		if healthy {
			if pool[i].FailStreak != 0 {
				pool[i].FailStreak = 0
				changed = true
			}
		} else {
			pool[i].FailStreak++
			changed = true
			if pool[i].FailStreak >= threshold {
				pool[i].Failed = true
			}
		}
		break
	}
	if !changed {
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
	fmt.Fprintf(&b,
		"疑似降智：模型 %s 下 turn-state 自动接管的候选已全部失效（注入密文 %d 块的健康 blob 后，"+
			"上游仍铸出更多块），已停止调度。块数只能把明文框进 %d 字节的窗口，所以这是疑似判据，不是确证。",
		model, openAIHealthyTurnStateBlocks, openAITurnStateAESBlockBytes)
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
		// 解不出信封就退回字符长度，别打印「密文 0 块」——那不是事实，是解码失败。
		shape := fmt.Sprintf("%d 字符(信封解不开)", len(c.Blob))
		if env, ok := parseOpenAITurnStateEnvelope(c.Blob); ok {
			shape = fmt.Sprintf("密文 %d 块", env.CipherBlocks)
		}
		fmt.Fprintf(&b, " 候选%d=%s(%s，铸于 %s，连续失败 %d 次)",
			i+1, blob, shape, c.MintedAt.Format("01-02 15:04"), c.FailStreak)
	}
	// 账号已经停调度，不可能自己再铸出新 blob——恢复路径必须是人工的，别写成「等它自愈」。
	_, _ = b.WriteString(" 处理：关闭自动接管开关后重新启用账号，或先关开关、手填一条新的健康 turn-state 再启用。")
	return b.String()
}

func logOpenAITurnStateAuto(format string, args ...any) {
	logger.LegacyPrintf("service.openai_turn_state_auto", format, args...)
}
