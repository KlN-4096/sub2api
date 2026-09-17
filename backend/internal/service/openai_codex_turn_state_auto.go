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
	openAITurnStateStaleMinExtraKey   = "openai_turn_state_stale_after_minutes"
)

const (
	defaultOpenAITurnStatePoolSize      = 3
	defaultOpenAITurnStateFailThreshold = 1
	defaultOpenAITurnStateStaleMinutes  = 60
	// openAIHealthyTurnStateLen 是「不降智」的 turn-state 长度。292/312 是实测的
	// 两个取值（Fernet 信封，密文相差一个 AES 块），运维口径按用户确认：292 = 不降智。
	openAIHealthyTurnStateLen = 292
	// openAIDegradedTurnStateLen 是实测的另一个取值，仅用于禁用原因的措辞。
	// 判定本身只看「是不是 292」，不枚举长度。
	openAIDegradedTurnStateLen = 312
	// openAITurnStateSessionTTL 是 session 长度状态的存活期。turn-state blob 实测
	// 存活中位 2.6 分钟、最长 33 分钟，1 小时足够覆盖一个会话的活跃期。
	openAITurnStateSessionTTL = time.Hour
)

// turn-state 覆写来源，落 usage_logs.turn_state_source。
const (
	turnStateSourceManual    = "manual"
	turnStateSourceAuto      = "auto"
	turnStateSourceAutoStale = "auto_stale"
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
)

// openAITurnStateCandidate 是候选池里的一条。
type openAITurnStateCandidate struct {
	Blob       string    `json:"blob"`
	MintedAt   time.Time `json:"minted_at"`
	Failed     bool      `json:"failed,omitempty"`
	FailStreak int       `json:"fail_streak,omitempty"`
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

// pickOpenAITurnStateCandidate 取栈顶第一条未失效的候选。
// 超过保鲜期的候选照样返回（只是来源标成 auto_stale）：292 本来就稀缺，按年龄跳过
// 会让降级链被「过期」而不是「失效」消耗掉，也就验证不了 1 小时到底会不会过期。
func pickOpenAITurnStateCandidate(pool []openAITurnStateCandidate, staleAfter time.Duration, now time.Time) (openAITurnStateCandidate, string, bool) {
	for _, c := range pool {
		if c.Failed || strings.TrimSpace(c.Blob) == "" {
			continue
		}
		source := turnStateSourceAuto
		if !c.MintedAt.IsZero() && now.Sub(c.MintedAt) > staleAfter {
			source = turnStateSourceAutoStale
		}
		return c, source, true
	}
	return openAITurnStateCandidate{}, "", false
}

// openAITurnStateSessionKey 把 session 状态按凭证域分域：同一个 session id 在不同
// 账号下是两段独立的上游会话，混用会让 A 账号的降智判定作用到 B 账号。
func openAITurnStateSessionKey(c *gin.Context, account *Account, sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ""
	}
	owner := openAICodexTurnStateOwner(c, account)
	if owner == "" {
		return ""
	}
	return owner + "\x1f" + sessionID
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
		readOpenAITurnStatePool(account), account.openAITurnStateStaleAfter(), time.Now())
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
	healthy := len(minted) == openAIHealthyTurnStateLen

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

// pushOpenAITurnStateCandidate 把新铸的健康 blob 推入候选池栈顶。
func (s *OpenAIGatewayService) pushOpenAITurnStateCandidate(c *gin.Context, account *Account, blob string) {
	ctx := turnStateOpCtx(c)
	mu := openAITurnStatePoolLock(account.ID)
	mu.Lock()
	defer mu.Unlock()

	pool := s.loadOpenAITurnStatePoolFresh(ctx, account)
	for _, existing := range pool {
		if existing.Blob == blob {
			return // 同一条 blob 会在一轮内被反复回带，别重复入池
		}
	}
	pool = append([]openAITurnStateCandidate{{Blob: blob, MintedAt: time.Now().UTC()}}, pool...)
	if size := account.openAITurnStatePoolSize(); len(pool) > size {
		pool = pool[:size]
	}
	s.persistOpenAITurnStatePool(c, account, pool)
}

// recordOpenAITurnStateOutcome 记录一次注入的结果；候选耗尽时停掉账号调度。
func (s *OpenAIGatewayService) recordOpenAITurnStateOutcome(c *gin.Context, account *Account, injected string, healthy bool) {
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
	if _, _, ok := pickOpenAITurnStateCandidate(pool, account.openAITurnStateStaleAfter(), time.Now()); !ok {
		exhausted = true
	}
	s.persistOpenAITurnStatePool(c, account, pool)
	if exhausted {
		s.disableAccountForExhaustedTurnState(c, account, pool)
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
func (s *OpenAIGatewayService) disableAccountForExhaustedTurnState(c *gin.Context, account *Account, pool []openAITurnStateCandidate) {
	if s.accountRepo == nil {
		return
	}
	ctx := turnStateOpCtx(c)
	reason := buildExhaustedTurnStateReason(pool)
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
func buildExhaustedTurnStateReason(pool []openAITurnStateCandidate) string {
	var b strings.Builder
	fmt.Fprintf(&b,
		"疑似降智：turn-state 自动接管的候选已全部失效（注入 %d 字符的健康 blob 后，上游仍铸出 %d 字符），已停止调度。",
		openAIHealthyTurnStateLen, openAIDegradedTurnStateLen)
	failed := make([]openAITurnStateCandidate, 0, len(pool))
	for _, c := range pool {
		if c.Failed {
			failed = append(failed, c)
		}
	}
	sort.Slice(failed, func(i, j int) bool { return failed[i].MintedAt.After(failed[j].MintedAt) })
	for i, c := range failed {
		blob := c.Blob
		if len(blob) > 16 {
			blob = blob[:16] + "…"
		}
		fmt.Fprintf(&b, " 候选%d=%s(铸于 %s，连续失败 %d 次)",
			i+1, blob, c.MintedAt.Format("01-02 15:04"), c.FailStreak)
	}
	// 账号已经停调度，不可能自己再铸出新 blob——恢复路径必须是人工的，别写成「等它自愈」。
	_, _ = b.WriteString(" 处理：关闭自动接管开关后重新启用账号，或先关开关、手填一条新的健康 turn-state 再启用。")
	return b.String()
}

func logOpenAITurnStateAuto(format string, args ...any) {
	logger.LegacyPrintf("service.openai_turn_state_auto", format, args...)
}
