package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"
)

// 降智恢复探测：用账号**自己的常驻出口**隔一段不固定的时间发一条探测，连续 streak_target 次
// 铸出 292 就判定「账号权重回来了」，写下标记（用户 2026-09-19 定：只标记、写日志，不自动改
// 任何配置——判错了也不会让账号裸奔）。连续同样多次失败就进 cooldown_hours 冷却：降智是账号
// 权重的事，静置几十小时才见恢复，中间反复探只是白付额度。
//
// 与猎手的分工：猎手换出口，回答「现在哪个 IP 能铸 292」；恢复探测固定走账号自己的出口，
// 回答「不换 IP 的话这个号还降不降智」。所以它是**独立开关**，猎手关着也照跑。
//
// 判定恢复后就停止探测，直到真实流量又自然铸出 312（resetOpenAITurnStateRecovery 清标记）
// 才从头攒——这也是标记能一直挂着的前提。
const (
	// openAITurnStateRecoveryExtraKey 是管理员写的配置。
	openAITurnStateRecoveryExtraKey = "openai_turn_state_recovery"
	// openAITurnStateRecoveryStateExtraKey 是探测写的运行态。调度中性键。
	openAITurnStateRecoveryStateExtraKey = "openai_turn_state_recovery_state"

	defaultOpenAITurnStateRecoveryStreak        = 5
	defaultOpenAITurnStateRecoveryMinMinutes    = 30
	defaultOpenAITurnStateRecoveryMaxMinutes    = 90
	defaultOpenAITurnStateRecoveryCooldownHours = 16

	openAITurnStateRecoveryMaxStreak        = 50
	openAITurnStateRecoveryMaxMinutes       = 1440
	openAITurnStateRecoveryMaxCooldownHours = 168
	openAITurnStateRecoveryLastKeep         = 5
	// openAITurnStateRecoveryTrafficWindow 是「最近有流量的模型」往回看的窗口（配置没填模型时）。
	openAITurnStateRecoveryTrafficWindow = 24 * time.Hour
)

// openAITurnStateRecoveryConfig 是 extra.openai_turn_state_recovery 的形态。
type openAITurnStateRecoveryConfig struct {
	Enabled bool `json:"enabled,omitempty"`
	// Model 探哪个模型。留空 = 最近有真实流量的那个，再没有就用最近一次形态观测的模型。
	// 降智是账号级的（312 = 请求被顶到别的模型上），探一个模型就够。
	Model string `json:"model,omitempty"`
	// StreakTarget 连续多少次 292 算恢复；连续同样多次失败进冷却。
	StreakTarget int `json:"streak_target,omitempty"`
	// MinMinutes / MaxMinutes 是两次探测之间的随机间隔，用户口径「模拟正常人使用」。
	MinMinutes int `json:"min_minutes,omitempty"`
	MaxMinutes int `json:"max_minutes,omitempty"`
	// CooldownHours 连续失败够数后的冷却时长。
	CooldownHours   int    `json:"cooldown_hours,omitempty"`
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
	// UsageAPIKeyID 记账用的 API Key；留空回落猎手配置里的那把。独立一项是因为本功能
	// 猎手关着也能开，而猎手的那个字段在猎手关着时页面上根本没有入口（第一轮评审 S3）。
	UsageAPIKeyID int64 `json:"usage_api_key_id,omitempty"`
}

// applyDefaults 补默认值并压上限，与猎手配置同一套取舍（extra 可能绕过 handler 校验落库）。
func (cfg *openAITurnStateRecoveryConfig) applyDefaults() {
	cfg.StreakTarget = openAITurnStateHuntBound(cfg.StreakTarget, defaultOpenAITurnStateRecoveryStreak, openAITurnStateRecoveryMaxStreak)
	cfg.MinMinutes = openAITurnStateHuntBound(cfg.MinMinutes, defaultOpenAITurnStateRecoveryMinMinutes, openAITurnStateRecoveryMaxMinutes)
	cfg.MaxMinutes = openAITurnStateHuntBound(cfg.MaxMinutes, defaultOpenAITurnStateRecoveryMaxMinutes, openAITurnStateRecoveryMaxMinutes)
	if cfg.MaxMinutes < cfg.MinMinutes {
		cfg.MaxMinutes = cfg.MinMinutes // 填反了按固定间隔走，别让区间变负数
	}
	cfg.CooldownHours = openAITurnStateHuntBound(cfg.CooldownHours, defaultOpenAITurnStateRecoveryCooldownHours, openAITurnStateRecoveryMaxCooldownHours)
	cfg.Model = strings.TrimSpace(cfg.Model)
	effort := strings.TrimSpace(cfg.ReasoningEffort)
	if _, known := openAITurnStateHuntReasoningEfforts[effort]; !known {
		effort = defaultOpenAITurnStateHuntReasoningEffort
	}
	cfg.ReasoningEffort = effort
}

func (cfg openAITurnStateRecoveryConfig) cooldown() time.Duration {
	return time.Duration(cfg.CooldownHours) * time.Hour
}

// interval 取一个随机间隔：用户要求 5 次探测的间隔不固定，像真人偶尔开一次对话。
func (cfg openAITurnStateRecoveryConfig) interval() time.Duration {
	lo := time.Duration(cfg.MinMinutes) * time.Minute
	hi := time.Duration(cfg.MaxMinutes) * time.Minute
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Int64N(int64(hi-lo)+1))
}

// readOpenAITurnStateRecoveryConfig 走 json 往返，与 readOpenAITurnStateHunterConfig 同一套取舍。
func readOpenAITurnStateRecoveryConfig(a *Account) (openAITurnStateRecoveryConfig, bool) {
	var cfg openAITurnStateRecoveryConfig
	if a == nil || a.Extra == nil {
		return cfg, false
	}
	raw, ok := a.Extra[openAITurnStateRecoveryExtraKey]
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

// openAITurnStateRecoveryState 是 extra.openai_turn_state_recovery_state 的形态。
type openAITurnStateRecoveryState struct {
	// Streak 连续铸出 292 的次数；FailStreak 连续失败（312 / 非 200 / 传输错误）的次数。
	// 两者互斥：一次成功清零失败计数，反之亦然。
	Streak     int `json:"streak,omitempty"`
	FailStreak int `json:"fail_streak,omitempty"`
	// NextAt 下一次探测时刻。冷却也写在这里（冷却与随机间隔对调用方是同一件事：等到点）。
	NextAt time.Time `json:"next_at"`
	// RecoveredAt 判定恢复的时刻。非零 = 标记挂着、不再探测。
	//
	// 两个时间字段用 omitzero 而不是 omitempty：omitempty 对 struct 不生效，零值会落库成
	// "0001-01-01T00:00:00Z"，而 JS 的 Date 认这个字符串——页面从第一次探测起就写着「已恢复」
	//（第一轮评审 B2）。前端另有 >0 的保险，老数据里已经写进去的零值也不会误判。
	RecoveredAt time.Time `json:"recovered_at,omitzero"`
	// CoolingUntil 只为页面能说清「在冷却」而不是「在等下一次」，判定仍看 NextAt。
	CoolingUntil time.Time                    `json:"cooling_until,omitzero"`
	Last         []openAITurnStateHuntAttempt `json:"last,omitempty"`
	LastError    string                       `json:"last_error,omitempty"`
	UpdatedAt    time.Time                    `json:"updated_at"`
}

func readOpenAITurnStateRecoveryState(a *Account) openAITurnStateRecoveryState {
	var st openAITurnStateRecoveryState
	if a == nil || a.Extra == nil {
		return st
	}
	raw, ok := a.Extra[openAITurnStateRecoveryStateExtraKey]
	if !ok || raw == nil {
		return st
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return st
	}
	if err := json.Unmarshal(encoded, &st); err != nil {
		return openAITurnStateRecoveryState{}
	}
	return st
}

func (st *openAITurnStateRecoveryState) push(attempt openAITurnStateHuntAttempt) {
	st.Last = append([]openAITurnStateHuntAttempt{attempt}, st.Last...)
	if len(st.Last) > openAITurnStateRecoveryLastKeep {
		st.Last = st.Last[:openAITurnStateRecoveryLastKeep]
	}
	st.LastError = attempt.Error
}

// probeRecovery 是恢复探测的一次判定 + 至多一次探测。挂在猎手的 ticker 上，但**不看猎手开关**。
// 返回是否真的探了：调用方据此把每个 tick 的恢复探测限制在一个账号（见 runOnce）。
func (s *OpenAITurnStateHunterService) probeRecovery(ctx context.Context, account *Account, deadline time.Time) bool {
	if s == nil || s.gateway == nil || s.accountRepo == nil || account == nil {
		return false
	}
	if account.Status != StatusActive || !account.IsOpenAIOAuthLike() {
		return false
	}
	cfg, ok := readOpenAITurnStateRecoveryConfig(account)
	if !ok || !cfg.Enabled {
		return false
	}
	now := s.now()
	st := readOpenAITurnStateRecoveryState(account)
	// 判定恢复后不再探：结论已经有了，继续探只是白付额度。
	if !st.RecoveredAt.IsZero() || now.Before(st.NextAt) || now.After(deadline) {
		return false
	}
	model := s.recoveryModel(account, cfg, now)
	if model == "" {
		// 不知道该探哪个模型（进程内没流量记录、也没观测过）：等下一窗，别瞎探一个上游不认的名字。
		// 也按一次失败记：配错了的账号最多写 streak_target 次就进冷却，不会每 30–90 分钟白写一次库。
		st.LastError = "no model to probe"
		st.Streak, st.FailStreak = 0, st.FailStreak+1
		if st.FailStreak >= cfg.StreakTarget {
			st.FailStreak = 0
			st.CoolingUntil = now.Add(cfg.cooldown())
			st.NextAt = st.CoolingUntil
		} else {
			st.NextAt = now.Add(cfg.interval())
		}
		st.UpdatedAt = now
		s.persistRecovery(ctx, account, st)
		return false
	}
	attempt := s.probeOwnExit(ctx, account, model, cfg)
	st.push(attempt)
	st.CoolingUntil = time.Time{}
	if attempt.Healthy {
		st.FailStreak, st.Streak = 0, st.Streak+1
		st.NextAt = now.Add(cfg.interval())
		if st.Streak >= cfg.StreakTarget {
			st.RecoveredAt = s.now()
			slog.Info("openai_turn_state_recovered", "account_id", account.ID, "model", model, "streak", st.Streak)
		}
	} else {
		// 312、非 200、传输错误都算一次失败：这个计数只决定「要不要歇会儿再探」，
		// 把它们分开只会多一条路径，省下的额度是同一笔。
		st.Streak, st.FailStreak = 0, st.FailStreak+1
		if st.FailStreak >= cfg.StreakTarget {
			st.FailStreak = 0 // 冷却结束后重新数，不要一醒来就又满
			st.CoolingUntil = now.Add(cfg.cooldown())
			st.NextAt = st.CoolingUntil
			slog.Info("openai_turn_state_recovery_cooldown", "account_id", account.ID, "model", model,
				"until", st.CoolingUntil.UTC().Format(time.RFC3339))
		} else {
			st.NextAt = now.Add(cfg.interval())
		}
	}
	st.UpdatedAt = s.now()
	s.persistRecovery(ctx, account, st)
	slog.Info("openai_turn_state_recovery_attempt",
		"account_id", account.ID, "model", model, "proxy_id", attempt.ProxyID, "status", attempt.Status,
		"chars", attempt.Chars, "healthy", attempt.Healthy, "error", attempt.Error,
		"streak", st.Streak, "fail_streak", st.FailStreak)
	return true
}

// recoveryModel 决定探哪个模型：配置 → 窗口内**最近**一次真实流量的 → 最近一次形态观测的。
// 取最近而不是字母序第一个（openAITurnStateTrafficModels 排过序）：文案写的就是「最近有流量的模型」，
// 字母序会让 astra/luna 混用的号永远探 luna（第一轮评审 S2）。
// 画图模型一律排除：它们结构上就铸 312，探它等于每轮必然 5 连败进冷却（S1）。
func (s *OpenAITurnStateHunterService) recoveryModel(account *Account, cfg openAITurnStateRecoveryConfig, now time.Time) string {
	if cfg.Model != "" {
		return cfg.Model
	}
	if model := s.gateway.openAITurnStateLatestTrafficModel(account, now.Add(-openAITurnStateRecoveryTrafficWindow)); model != "" {
		return model
	}
	if obs, ok := readOpenAITurnStateObservation(account); ok {
		if model := strings.TrimSpace(obs.Model); model != "" && !openAITurnStateImageModel(model) {
			return model
		}
	}
	return ""
}

// probeOwnExit 走账号自己的出口探一次。与猎手探测的两处刻意不同：
//   - 不换代理：这条探测问的就是「常驻出口上还降不降智」。
//   - 不设 req.Close：猎手靠关连接逼出新出口，这里出口是固定的；而连接池按「账号 × 代理 URL」
//     缓存，关连接会把真实流量正在用的那条 HTTP/2 隧道一起带走。复用连接也更像真人。
func (s *OpenAITurnStateHunterService) probeOwnExit(ctx context.Context, account *Account, model string, cfg openAITurnStateRecoveryConfig) openAITurnStateHuntAttempt {
	attempt := openAITurnStateHuntAttempt{At: s.now(), Model: model}
	egress := *account
	if account.ProxyID != nil {
		attempt.ProxyID = *account.ProxyID
	}
	proxyURL, proxy, err := s.accountExitProxyURL(ctx, account)
	if err != nil {
		attempt.Error = fmt.Sprintf("account exit unusable: %v", sanitizeUpstreamErrorMessage(err.Error()))
		return attempt
	}
	if proxy != nil {
		attempt.Proxy = proxy.Name
		egress.Proxy = proxy
	}
	// 记账：本功能自己的 key 优先，没填就用猎手那把（两种探测都是这个账号的额度，通常同一把）。
	usageKeyID := cfg.UsageAPIKeyID
	if usageKeyID <= 0 {
		hunter, _ := readOpenAITurnStateHunterConfig(account)
		usageKeyID = hunter.UsageAPIKeyID
	}
	s.doProbe(ctx, account, &egress, proxyURL, false, model,
		openAITurnStateHunterConfig{ReasoningEffort: cfg.ReasoningEffort, UsageAPIKeyID: usageKeyID}, &attempt)
	return attempt
}

// accountExitProxyURL 解析账号自己的出口。没绑代理就是直连（空 URL，httpUpstream 照发）。
// 账号行没带上代理对象时按 ID 补读一次：ListByPlatform 不保证连带。
func (s *OpenAITurnStateHunterService) accountExitProxyURL(ctx context.Context, account *Account) (string, *Proxy, error) {
	if account.ProxyID == nil {
		return "", nil, nil
	}
	proxy := account.Proxy
	if proxy == nil {
		loaded, err := s.proxyRepo.ListByIDs(ctx, []int64{*account.ProxyID})
		if err != nil {
			return "", nil, err
		}
		if len(loaded) == 0 {
			return "", nil, fmt.Errorf("account proxy %d was not found", *account.ProxyID)
		}
		proxy = &loaded[0]
	}
	url, err := resolveConfiguredProxyURL(ctx, nil, account.ProxyID, proxy)
	if err != nil {
		return "", nil, err
	}
	return url, proxy, nil
}

func (s *OpenAITurnStateHunterService) persistRecovery(ctx context.Context, account *Account, st openAITurnStateRecoveryState) {
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
	account.Extra[openAITurnStateRecoveryStateExtraKey] = generic
	if err := s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{openAITurnStateRecoveryStateExtraKey: generic}); err != nil {
		slog.Warn("openai_turn_state_recovery_persist_failed", "account_id", account.ID, "error", err)
	}
}

// resetOpenAITurnStateRecovery 在账号又自然铸出 312 时清掉「已恢复」标记与连胜，从头攒。
// 只有标记挂着时才写库：这是响应热路径，判定恢复之后每条 312 都写一次就太贵了。
func (s *OpenAIGatewayService) resetOpenAITurnStateRecovery(ctx context.Context, account *Account) {
	if s == nil || s.accountRepo == nil || account == nil {
		return
	}
	st := readOpenAITurnStateRecoveryState(account)
	if st.RecoveredAt.IsZero() && st.Streak == 0 {
		return
	}
	st.RecoveredAt, st.Streak = time.Time{}, 0
	st.UpdatedAt = time.Now().UTC()
	encoded, err := json.Marshal(st)
	if err != nil {
		return
	}
	var generic map[string]any
	if err := json.Unmarshal(encoded, &generic); err != nil {
		return
	}
	account.Extra[openAITurnStateRecoveryStateExtraKey] = generic
	if err := s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{openAITurnStateRecoveryStateExtraKey: generic}); err != nil {
		slog.Warn("openai_turn_state_recovery_reset_failed", "account_id", account.ID, "error", err)
		return
	}
	slog.Info("openai_turn_state_recovery_reset", "account_id", account.ID)
}

// validateOpenAITurnStateRecoveryExtra 校验管理员写的恢复探测配置，随猎手配置一起在
// ValidateOpenAITurnStateHunterExtra 里调用（handler 的三个入口不用各加一次）。
func validateOpenAITurnStateRecoveryExtra(extra map[string]any) error {
	if extra == nil {
		return nil
	}
	raw, ok := extra[openAITurnStateRecoveryExtraKey]
	if !ok {
		return nil
	}
	if raw == nil {
		delete(extra, openAITurnStateRecoveryExtraKey)
		return nil
	}
	table, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be an object", openAITurnStateRecoveryExtraKey)
	}
	if v, ok := table["enabled"]; ok && v != nil {
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("%s.enabled must be a boolean", openAITurnStateRecoveryExtraKey)
		}
	}
	if v, ok := table["model"]; ok && v != nil {
		if _, ok := v.(string); !ok {
			return fmt.Errorf("%s.model must be a string", openAITurnStateRecoveryExtraKey)
		}
	}
	for _, key := range []string{"streak_target", "min_minutes", "max_minutes", "cooldown_hours", "usage_api_key_id"} {
		v, ok := table[key]
		if !ok || v == nil {
			continue
		}
		n, ok := v.(float64)
		if !ok || n != float64(int(n)) || n < 1 {
			return fmt.Errorf("%s.%s must be a positive integer", openAITurnStateRecoveryExtraKey, key)
		}
	}
	if v, ok := table["reasoning_effort"]; ok && v != nil {
		effort, ok := v.(string)
		if !ok {
			return fmt.Errorf("%s.reasoning_effort must be a string", openAITurnStateRecoveryExtraKey)
		}
		if _, known := openAITurnStateHuntReasoningEfforts[strings.TrimSpace(effort)]; !known {
			return fmt.Errorf("%s.reasoning_effort is not supported", openAITurnStateRecoveryExtraKey)
		}
	}
	return nil
}
