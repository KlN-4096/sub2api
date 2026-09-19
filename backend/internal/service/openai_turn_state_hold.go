package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// 降智暂停：猎手管的模型拿不出可注入的 292 时，把账号临时停调度、让本次请求换号（没
// 别的号就 503），猎到新票自动放回。用户定的口径：一直暂停直到猎到；撞了小时上限就等
// 下一窗接着猎；一直猎不到就一直报错，不设安全阀。
//
// 暂停状态借 temp_unschedulable_until / _reason 两列，不另存运行态：SetTempUnschedulable
// 是一条原子 UPDATE 并同步刷调度快照，调度器立刻跳过它；猎手每 tick 从 ListByPlatform
// 读到的就是新鲜值，不用和 hunt 运行态的整块读改写抢；账号页现成的「临时不可调度」
// 也就直接能看见。原因串带模型名，放回时按同一个模型判「票到手没有」。
const (
	openAITurnStateHoldReasonPrefix = "turn_state_hold:"
	// openAITurnStateHoldTTL 是单次停调度的时长，猎手每 tick 续期。不写成永久是给猎手
	// 死掉（进程没起、失去 leader）留回退：到期自动放回，下一条请求再判一次。
	openAITurnStateHoldTTL = 24 * time.Hour
	// openAITurnStateHoldRenewBelow 剩余不足这么多就续期，每 12 小时一次 UPDATE。
	openAITurnStateHoldRenewBelow = 12 * time.Hour
	// ctxKeyTurnStateHold 记本次请求因缺票被拦下的模型；出站构造完请求头后据此换号。
	ctxKeyTurnStateHold = "openai_turn_state_hold"
)

// OpenAITurnStateHoldReason 是换号错误的原因码，handler 据此给客户端回 503 与说明。
const OpenAITurnStateHoldReason = GatewayFailureReason("openai_turn_state_hold")

// openAITurnStateHoldEnabled 报告该模型缺票时要不要停调度：猎手开着、管这个模型、开了暂停。
func (s *OpenAIGatewayService) openAITurnStateHoldEnabled(a *Account, model string) bool {
	if !s.openAITurnStateHuntedModel(a, model) {
		return false
	}
	cfg, _ := readOpenAITurnStateHunterConfig(a)
	return cfg.HoldWhenDegraded
}

// openAITurnStateHeldModel 返回账号当前因缺票被停调度的模型；不是本功能停的返回空。
func openAITurnStateHeldModel(a *Account, now time.Time) string {
	if a == nil || a.TempUnschedulableUntil == nil || !now.Before(*a.TempUnschedulableUntil) {
		return ""
	}
	model, ok := strings.CutPrefix(a.TempUnschedulableReason, openAITurnStateHoldReasonPrefix)
	if !ok {
		return ""
	}
	return strings.TrimSpace(model)
}

// holdOpenAITurnStateIfUnfilled 在注入点池里拿不出票时调用：猎手管这个模型且开了暂停，
// 客户端自己回带的又不是本账号新鲜的 292，就停调度并标记本次换号。
//
// 回带判定读客户端原始头而不是出站头：出站头此时已被 guardOpenAICodexTurnStateEcho
// 剥过异账号 blob，这里用同一个「谁铸的」判据补回那道闸。回带的票信封解不出铸造戳
// 就按不新鲜处理——判不了寿命的票不能当放行依据。探测上下文不拦：探测本来就裸发。
func (s *OpenAIGatewayService) holdOpenAITurnStateIfUnfilled(c *gin.Context, account *Account, model string) {
	if s == nil || c == nil || account == nil || openAITurnStateProbeContext(c) || !s.openAITurnStateHoldEnabled(account, model) {
		return
	}
	now := time.Now()
	if c.Request != nil {
		inbound := strings.TrimSpace(c.Request.Header.Get(openAICodexTurnStateHeader))
		if inbound != "" && openAITurnStateHealthy(inbound) && !s.openAICodexTurnStateMintedByOther(c, account, inbound) {
			if minted := openAITurnStateMintedAt(inbound, time.Time{}); !minted.IsZero() && now.Before(minted.Add(account.openAITurnStateStaleAfter())) {
				return
			}
		}
	}
	c.Set(ctxKeyTurnStateHold, model)
	// 已经因这个模型停着（快照更新前的并发请求）：只换号，别再各写一次库、各刷一次快照。
	if s.accountRepo == nil || openAITurnStateHeldModel(account, now) == model {
		return
	}
	reason := openAITurnStateHoldReasonPrefix + model
	if err := s.accountRepo.SetTempUnschedulable(turnStateOpCtx(c), account.ID, now.Add(openAITurnStateHoldTTL), reason); err != nil {
		logOpenAITurnStateAuto("account=%d model=%s hold scheduling failed: %v", account.ID, model, err)
		return
	}
	logOpenAITurnStateAuto("account=%d model=%s no usable ticket, scheduling held until hunter finds one", account.ID, model)
}

// openAITurnStateHoldError 把注入点的拦截变成换号错误：本账号排除、下一个账号继续；
// 全池耗尽时 handler 按 OpenAITurnStateHoldReason 回 503 与 ClientMessage。
func openAITurnStateHoldError(c *gin.Context) error {
	if c == nil {
		return nil
	}
	model := strings.TrimSpace(c.GetString(ctxKeyTurnStateHold))
	if model == "" {
		return nil
	}
	return &UpstreamFailoverError{
		StatusCode:       http.StatusServiceUnavailable,
		Reason:           OpenAITurnStateHoldReason,
		ClientStatusCode: http.StatusServiceUnavailable,
		ClientMessage:    fmt.Sprintf("account has no healthy x-codex-turn-state for %s and is paused until the hunter finds one", model),
	}
}

// syncHold 每 tick 维护停调度：开关关了或票到手就放回，仍缺票就续期。
//
// 本功能写的 until 是 24 小时，比 401/429 那些冷却都长，而 SetTempUnschedulable 只允许把
// until 往后推：暂停期间别的原因写不进这一行（那些更短的冷却被吞掉——账号反正停着，放回
// 后凭据仍坏会再被 401 停一次）。放回前重读一次：这一轮探测最长 15 分钟，手里的账号可能
// 已被管理员「恢复调度」或换了原因，那时就不该再动。
func (s *OpenAITurnStateHunterService) syncHold(ctx context.Context, account *Account, now time.Time) {
	model := openAITurnStateHeldModel(account, now)
	if model == "" {
		return
	}
	release := ""
	switch {
	case account.Status != StatusActive || !account.IsOpenAITurnStateAutoEnabled() || !s.gateway.openAITurnStateHoldEnabled(account, model):
		release = "hold disabled"
	default:
		pool := s.gateway.loadOpenAITurnStatePoolFresh(ctx, account)
		if _, _, ok := pickOpenAITurnStateCandidate(pool, model, account.openAITurnStateStaleAfter(), now); ok {
			release = "ticket pooled"
		}
	}
	if release != "" {
		if latest, err := s.accountRepo.GetByID(ctx, account.ID); err == nil && latest != nil && openAITurnStateHeldModel(latest, now) == "" {
			account.TempUnschedulableUntil, account.TempUnschedulableReason = latest.TempUnschedulableUntil, latest.TempUnschedulableReason
			return // 已经被别的路径放回或换了原因
		}
		if err := s.releaseHold(ctx, account.ID); err != nil {
			slog.Warn("openai_turn_state_hold_release_failed", "account_id", account.ID, "model", model, "error", err)
			return
		}
		account.TempUnschedulableUntil, account.TempUnschedulableReason = nil, ""
		slog.Info("openai_turn_state_hold_released", "account_id", account.ID, "model", model, "reason", release)
		return
	}
	if account.TempUnschedulableUntil.Sub(now) >= openAITurnStateHoldRenewBelow {
		return
	}
	until := now.Add(openAITurnStateHoldTTL)
	if err := s.accountRepo.SetTempUnschedulable(ctx, account.ID, until, openAITurnStateHoldReasonPrefix+model); err != nil {
		slog.Warn("openai_turn_state_hold_renew_failed", "account_id", account.ID, "model", model, "error", err)
		return
	}
	account.TempUnschedulableUntil = &until
}

// releaseHold 放回：有限流服务就走它（DB + Redis 副本 + 运行态阻断一起清，管理页的「临时
// 不可调度」弹窗读的是 Redis 副本，只清 DB 会让它把已放回的账号继续显示成停着），没有就只清 DB。
func (s *OpenAITurnStateHunterService) releaseHold(ctx context.Context, accountID int64) error {
	if s.gateway != nil && s.gateway.rateLimitService != nil {
		return s.gateway.rateLimitService.ReleaseTempUnschedulable(ctx, accountID)
	}
	return s.accountRepo.ClearTempUnschedulable(ctx, accountID)
}
