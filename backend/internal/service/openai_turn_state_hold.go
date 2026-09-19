package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// 降智暂停：猎手管的模型拿不出可注入的 292 时，把**这个模型**在本账号上临时停调度、让本次
// 请求换号（没别的号就 503），猎到新票立即放回。用户定的口径：缺票的模型绝不在本账号上裸奔；
// 撞了小时上限就等下一窗接着猎；一直猎不到就一直报错，不设安全阀。
//
// 停的单位是模型不是账号（2026-09-19 用户指出账号级会让 sol 缺票连坐有票的 astra）：借
// extra.model_rate_limits[<model>] 这条现成的模型级限流——调度资格检查按请求模型读它，Redis
// 调度快照也带这个键，SetModelRateLimit 是一条原子 UPDATE 并同步刷快照。停多长是一个空闲窗口
// （idle_minutes），到期**不续期**：窗口内该模型的请求到不了本账号；到期后下一条请求还缺票就
// 再停一次（照样换号/503）。有人用就一直处于「停着 → 到期 → 再停」的循环，效果等于一直停；
// 没人再用的模型（terra 偶尔一次）到期后自然结束，猎手也不再为它烧额度。被任何路径（人工恢复
// 状态、额度自动重置、账号测试成功——都走 ClearModelRateLimits 整键删；token 刷新只清账号级临时
// 停调度，碰不到它）清掉都无害——下一条请求会重新拉起，所以那些路径不用绕开它。
// 唯一的代价：自动模式的「曾被停过」记忆也在这条条目里（openAITurnStateHuntedModel），清掉再叠
// 一次重启，该模型会裸奔一条请求（下一次铸造就重新记住），有界，不另存一份。
const (
	// openAITurnStateHoldLimitReason 是 model_rate_limits 条目的 reason，猎手据此认出自己停的。
	openAITurnStateHoldLimitReason = "turn_state_hold"
	// openAITurnStateHoldDefaultTTL 空闲门槛关掉（idle_minutes<=0）时的暂停时长。
	openAITurnStateHoldDefaultTTL = time.Hour
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

// openAITurnStateHoldTTL 暂停时长 = 空闲窗口：与猎手「多久没流量就不猎」同一个尺度，
// 没人再请求的模型在同一个窗口里同时失去暂停和猎手。
func openAITurnStateHoldTTL(cfg openAITurnStateHunterConfig) time.Duration {
	if cfg.IdleMinutes > 0 {
		return time.Duration(cfg.IdleMinutes) * time.Minute
	}
	return openAITurnStateHoldDefaultTTL
}

// openAITurnStateHoldResetAt 读 model_rate_limits 里本功能写的那条：不是本功能写的返回 false。
// 类型断言与 modelRateLimitResetAt 同一套（JSONB 读回来的是 map[string]any）。
func openAITurnStateHoldResetAt(a *Account, model string) (time.Time, bool) {
	if a == nil || a.Extra == nil || model == "" {
		return time.Time{}, false
	}
	limits, ok := a.Extra[modelRateLimitsKey].(map[string]any)
	if !ok {
		return time.Time{}, false
	}
	entry, ok := limits[model].(map[string]any)
	if !ok {
		return time.Time{}, false
	}
	if reason, _ := entry["reason"].(string); reason != openAITurnStateHoldLimitReason {
		return time.Time{}, false
	}
	raw, _ := entry["rate_limit_reset_at"].(string)
	resetAt, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}, false
	}
	return resetAt, true
}

// openAITurnStateModelHeld 报告该模型此刻是否因缺票被停着。
func openAITurnStateModelHeld(a *Account, model string, now time.Time) bool {
	resetAt, ok := openAITurnStateHoldResetAt(a, strings.TrimSpace(model))
	return ok && now.Before(resetAt)
}

// openAITurnStateHeldModels 列出此刻被停着的模型，按名排序让猎手轮询顺序稳定。
func openAITurnStateHeldModels(a *Account, now time.Time) []string {
	if a == nil || a.Extra == nil {
		return nil
	}
	limits, ok := a.Extra[modelRateLimitsKey].(map[string]any)
	if !ok {
		return nil
	}
	var out []string
	for model := range limits {
		if openAITurnStateModelHeld(a, model, now) {
			out = append(out, model)
		}
	}
	sort.Strings(out)
	return out
}

// holdOpenAITurnStateIfUnfilled 在注入点池里拿不出票时调用：猎手管这个模型且开了暂停，
// 客户端自己回带的又不是本账号新鲜的 292，就停这个模型并标记本次换号。
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
	// 已经停着（快照更新前的并发请求、绕过调度过滤的粘性会话）：只换号，别再各写一次库、各刷一次快照。
	if s.accountRepo == nil || openAITurnStateModelHeld(account, model, now) {
		return
	}
	cfg, _ := readOpenAITurnStateHunterConfig(account)
	resetAt := now.Add(openAITurnStateHoldTTL(cfg))
	if err := s.accountRepo.SetModelRateLimit(turnStateOpCtx(c), account.ID, model, resetAt, openAITurnStateHoldLimitReason); err != nil {
		logOpenAITurnStateAuto("account=%d model=%s hold scheduling failed: %v", account.ID, model, err)
		return
	}
	// 请求手里的快照同步：同一快照上的并发请求走上面的短路。
	setAccountModelRateLimitSnapshot(account, model, resetAt, openAITurnStateHoldLimitReason, now)
	logOpenAITurnStateAuto("account=%d model=%s no usable ticket, model held until %s or until hunter finds one", account.ID, model, resetAt.UTC().Format(time.RFC3339))
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

// syncHold 每 tick 维护暂停：开关关了或票到手就放回；仍缺票就让它自然到期（不续期，下一条
// 请求会再拉起）。放回前重读一次（一次调用最多读一次）：手里的账号可能是 tick 开头的快照，
// 期间条目可能已被清掉或换成别的原因（管理员清限流后真实请求撞了 spark 429），那时不该再写。
// 轮次中新写的暂停要靠调用方先重读账号再传进来（见 huntStep 命中分支），这里的重读只挡「别写」。
func (s *OpenAITurnStateHunterService) syncHold(ctx context.Context, account *Account, now time.Time) {
	var pool []openAITurnStateCandidate
	poolLoaded := false
	var latest *Account
	for _, model := range openAITurnStateHeldModels(account, now) {
		release := ""
		switch {
		case account.Status != StatusActive || !account.IsOpenAITurnStateAutoEnabled() || !s.gateway.openAITurnStateHoldEnabled(account, model):
			release = "hold disabled"
		default:
			if !poolLoaded {
				pool, poolLoaded = s.gateway.loadOpenAITurnStatePoolFresh(ctx, account), true
			}
			if _, _, ok := pickOpenAITurnStateCandidate(pool, model, account.openAITurnStateStaleAfter(), now); ok {
				release = "ticket pooled"
			}
		}
		if release == "" {
			continue
		}
		if latest == nil {
			l, err := s.accountRepo.GetByID(ctx, account.ID)
			if err != nil || l == nil {
				slog.Warn("openai_turn_state_hold_release_failed", "account_id", account.ID, "model", model, "error", err)
				return
			}
			latest = l
		}
		if !openAITurnStateModelHeld(latest, model, now) {
			continue // 已经被别的路径清掉或换了原因
		}
		if err := s.releaseHold(ctx, account, model, now); err != nil {
			slog.Warn("openai_turn_state_hold_release_failed", "account_id", account.ID, "model", model, "error", err)
			continue
		}
		slog.Info("openai_turn_state_hold_released", "account_id", account.ID, "model", model, "reason", release)
	}
}

// releaseHold 放回：把该模型的暂停写成已到期。仓储没有按 scope 删除的方法，而 SetModelRateLimit
// 是同一条原子 UPDATE + 调度快照同步；留下的过期条目对调度无效，人工「恢复调度」会连带清掉。
func (s *OpenAITurnStateHunterService) releaseHold(ctx context.Context, account *Account, model string, now time.Time) error {
	expired := now.Add(-time.Second)
	if err := s.accountRepo.SetModelRateLimit(ctx, account.ID, model, expired, openAITurnStateHoldLimitReason); err != nil {
		return err
	}
	setAccountModelRateLimitSnapshot(account, model, expired, openAITurnStateHoldLimitReason, now)
	return nil
}
