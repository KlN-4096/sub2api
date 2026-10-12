package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// turn-state 的出站记录与形态观测。
//
// 自动接管 / 候选池 / 猎手 / 降智暂停 / pair / 恢复探测 / 手填覆写已于 2026-10-10 删除：它们
// 都建立在「注入 292 能换回正常服务」上，2026-09-21 起失效。留下的都是读数：形态观测
// （openai_turn_state_observed），以及用量表 Turn-State 入站 / 出站两列的落库——出站值
// ctxKeyTurnStateSent / markOpenAITurnStateSent / OpenAITurnStateUsageSent，入站值见
// openai_codex_turn_state.go 的 usageCodexTurnStatePtr。

// openAITurnStateObservedExtraKey 是账号最近一次观测到的形态（不含 blob）。
// 对所有 Codex 账号都写，带节流，纯展示用。
const openAITurnStateObservedExtraKey = "openai_turn_state_observed"

// ctxKeyTurnStateSent 记本次出站实际带的 blob，落 usage_logs.turn_state_sent。
const ctxKeyTurnStateSent = "openai_turn_state_sent"

// openAITurnStateRequestModel 取本次出站实际用的模型。各出站路径都会在分发前
// SetOpsUpstreamModel，观测点在其后，所以这里读到的就是本次尝试的模型。
func openAITurnStateRequestModel(c *gin.Context) string {
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.GetString(OpsUpstreamModelKey))
}

// markOpenAITurnStateSent 记下本次出站实际带的 turn-state（空值不记，保持 NULL）。
// 用量表「Turn-State 出站」列的数据源。
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

// observeOpenAITurnStateMint 是响应侧唯一入口：上游铸出新 blob 时调用，只做形态观测。
// 同一条 blob 在一次响应里可能被送两次（applyAttemptResponseHeaders 有两个调用点），
// 第二次由节流挡掉：第一次已把读数写回同一份账号快照。
//
// 只记形态、不记 blob：blob 是上游令牌，无条件存进每个账号的 extra 就等于让它随账号列表
// 接口下发、进每一份 DB dump。失败只记日志，不阻塞响应。
func (s *OpenAIGatewayService) observeOpenAITurnStateMint(c *gin.Context, account *Account, minted string) {
	if s == nil || account == nil || !account.TargetsChatGPTCodexUpstream() {
		return
	}
	minted = strings.TrimSpace(minted)
	if minted == "" {
		return
	}
	s.noteOpenAITurnStateObservation(c, account, minted, openAITurnStateHealthy(minted))
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
//     大小写变体各占一格，模型本身还随版本轮换，而这里没有淘汰。
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
// 读的是请求手里的账号快照，不是 GetByID：并发写覆盖的是同样有效的读数，快照陈旧最多让
// 节流多放过几次写。落库放在后台：调用点在响应首字节之前。
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
	// goroutine 只带走 ctx、ID 和值：gin.Context 在响应结束后会被回收复用。
	ctx, accountID := turnStateOpCtx(c), account.ID
	go func() {
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := s.accountRepo.UpdateExtra(writeCtx, accountID, map[string]any{
			openAITurnStateObservedExtraKey: generic,
		}); err != nil {
			logOpenAITurnState("persist observation failed: account=%d err=%v", accountID, err)
		}
	}()
}

// turnStateOpCtx 取一个不随请求取消的 ctx：形态观测要在响应收尾后照常落库。
func turnStateOpCtx(c *gin.Context) context.Context {
	if c != nil && c.Request != nil {
		return context.WithoutCancel(c.Request.Context())
	}
	return context.Background()
}

func logOpenAITurnState(format string, args ...any) {
	logger.LegacyPrintf("service.openai_turn_state", format, args...)
}
