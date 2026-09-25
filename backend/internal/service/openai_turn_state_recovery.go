package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"
	"unicode"

	"github.com/tidwall/gjson"
)

// 降智恢复探测：用账号**自己的常驻出口**隔一段不固定的时间发一道糖果题并读完回答；最近
// streak_target（总次数）次探测里答对（"21"）够 success_target（成功次数）次就判定「账号恢复了」，
// 写下标记（用户 2026-09-19 定：只标记、写日志，不自动改任何配置——判错了也不会让账号裸奔）。
// 连续 streak_target 次失败就进 cooldown_hours 冷却并清掉这一批结果：降智是账号权重的事，
// 静置几十小时才见恢复，中间反复探只是白付额度。
//
// 判据为什么是做题（2026-09-25 从「连续 N 次 292」换过来）：09-23 起上游的模型标签与 turn-state
// 长度都不再反映实际服务模型（标 astra 实为 luna 水平、票全员 780），只剩做题能判——降智账号
// 答 29 / 36 / 空，健康账号稳定答 21。默认探 gpt-5.6-sol@medium（用户在正常 Codex 里多次实测恒 21）。
//
// 与猎手的分工：猎手换出口，回答「现在哪个 IP 能铸 292」；恢复探测固定走账号自己的出口，
// 回答「不换 IP 的话这个号还降不降智」。所以它是**独立开关**，猎手关着也照跑。
//
// 判定恢复后就停止探测；关掉开关即清空运行态（含标记），再开从头攒。真实流量自然铸出 312
// 也清（resetOpenAITurnStateRecovery；780 时代基本不再触发，留着无害）。
const (
	// openAITurnStateRecoveryExtraKey 是管理员写的配置。
	openAITurnStateRecoveryExtraKey = "openai_turn_state_recovery"
	// openAITurnStateRecoveryStateExtraKey 是探测写的运行态。调度中性键。
	openAITurnStateRecoveryStateExtraKey = "openai_turn_state_recovery_state"

	defaultOpenAITurnStateRecoveryModel         = "gpt-5.6-sol"
	defaultOpenAITurnStateRecoveryEffort        = "medium"
	defaultOpenAITurnStateRecoveryStreak        = 5 // 总次数 = 判定窗口
	defaultOpenAITurnStateRecoverySuccess       = 4 // 窗口内答对次数
	defaultOpenAITurnStateRecoveryMinMinutes    = 30
	defaultOpenAITurnStateRecoveryMaxMinutes    = 90
	defaultOpenAITurnStateRecoveryCooldownHours = 16

	openAITurnStateRecoveryMaxStreak        = 50
	openAITurnStateRecoveryMaxMinutes       = 1440
	openAITurnStateRecoveryMaxCooldownHours = 168
	openAITurnStateRecoveryLastKeep         = 5
	// openAITurnStateRecoveryProbeTimeout 要等整个回答：sol@medium 做糖果题实测 20–60s，留 3 分钟。
	openAITurnStateRecoveryProbeTimeout = 3 * time.Minute
	openAITurnStateRecoveryReadLimit    = 2 << 20
	openAITurnStateRecoveryAnswerKeep   = 64
	// openAITurnStateRecoveryExpectedAnswer 是糖果题的正确答案；降智账号典型答 29 / 36 / 空。
	openAITurnStateRecoveryExpectedAnswer = "21"
)

// openAITurnStateRecoveryPrompt 与人工核验用的是同一道题（G:/temp/Claude_temp/hdrcheck.py）。
const openAITurnStateRecoveryPrompt = "在一个黑色的袋子里放有三种口味的糖果，每种糖果有两种不同的形状（圆形和五角星形，不同的形状靠手感可以分辨）。数量统计如下：\n" +
	"          苹果味  桃子味  西瓜味\n圆形        7       9       8\n五角星形    7       6       4\n" +
	"参赛者需要在活动前决定摸出的糖果数目。最少取出多少个糖果，才能保证手中同时拥有不同形状的苹果味和桃子味的糖？" +
	"（手中有圆形苹果味搭配五角星桃子味，或圆形桃子味搭配五角星苹果味，都算满足）\n只输出一个数字，不要任何解释。"

// openAITurnStateRecoveryConfig 是 extra.openai_turn_state_recovery 的形态。
type openAITurnStateRecoveryConfig struct {
	Enabled bool `json:"enabled,omitempty"`
	// Model 探哪个模型。留空 = gpt-5.6-sol（用户实测该模型在正常 Codex 里恒答 21）。
	Model string `json:"model,omitempty"`
	// StreakTarget 是总次数：判定窗口的大小，也是连续失败进冷却的门槛。
	StreakTarget int `json:"streak_target,omitempty"`
	// SuccessTarget 是成功次数：最近 StreakTarget 次里答对够这么多次就判恢复（成功率 = 成功次数/总次数）。
	SuccessTarget int `json:"success_target,omitempty"`
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
	// 默认 4 也不能超过总次数，否则永远判不了恢复。
	cfg.SuccessTarget = min(openAITurnStateHuntBound(cfg.SuccessTarget, defaultOpenAITurnStateRecoverySuccess, cfg.StreakTarget), cfg.StreakTarget)
	cfg.MinMinutes = openAITurnStateHuntBound(cfg.MinMinutes, defaultOpenAITurnStateRecoveryMinMinutes, openAITurnStateRecoveryMaxMinutes)
	cfg.MaxMinutes = openAITurnStateHuntBound(cfg.MaxMinutes, defaultOpenAITurnStateRecoveryMaxMinutes, openAITurnStateRecoveryMaxMinutes)
	if cfg.MaxMinutes < cfg.MinMinutes {
		cfg.MaxMinutes = cfg.MinMinutes // 填反了按固定间隔走，别让区间变负数
	}
	cfg.CooldownHours = openAITurnStateHuntBound(cfg.CooldownHours, defaultOpenAITurnStateRecoveryCooldownHours, openAITurnStateRecoveryMaxCooldownHours)
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultOpenAITurnStateRecoveryModel
	}
	effort := strings.TrimSpace(cfg.ReasoningEffort)
	if _, known := openAITurnStateHuntReasoningEfforts[effort]; !known {
		effort = defaultOpenAITurnStateRecoveryEffort
	}
	cfg.ReasoningEffort = effort
}

func (cfg openAITurnStateRecoveryConfig) cooldown() time.Duration {
	return time.Duration(cfg.CooldownHours) * time.Hour
}

// interval 取一个随机间隔：用户要求探测的间隔不固定，像真人偶尔开一次对话。
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
	// Results 是判定窗口：最近至多 streak_target 次探测的成败，新的在前。冷却时清空。
	Results []bool `json:"results,omitempty"`
	// FailStreak 连续失败（答错 / 非 200 / 传输错误）的次数，一次成功清零；够总次数进冷却。
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

// pushResult 把一次成败推进判定窗口，窗口只留最近 window 次。
func (st *openAITurnStateRecoveryState) pushResult(ok bool, window int) {
	st.Results = append([]bool{ok}, st.Results...)
	if window > 0 && len(st.Results) > window {
		st.Results = st.Results[:window]
	}
}

// successes 是窗口内答对的次数。
func (st openAITurnStateRecoveryState) successes() int {
	n := 0
	for _, ok := range st.Results {
		if ok {
			n++
		}
	}
	return n
}

// dirty 表示运行态里还有东西（关掉开关时要清一次）。
func (st openAITurnStateRecoveryState) dirty() bool {
	return len(st.Results) > 0 || st.FailStreak > 0 || !st.RecoveredAt.IsZero() ||
		!st.CoolingUntil.IsZero() || len(st.Last) > 0 || st.LastError != ""
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
	now := s.now()
	st := readOpenAITurnStateRecoveryState(account)
	cfg, ok := readOpenAITurnStateRecoveryConfig(account)
	if !ok || !cfg.Enabled {
		// 关掉开关即清空运行态（含「已恢复」标记），再开从头攒。只在还有东西时写一次库。
		if st.dirty() {
			s.persistRecovery(ctx, account, openAITurnStateRecoveryState{UpdatedAt: now})
		}
		return false
	}
	// 旧判据（连续 292）打下的标记没有判定窗口，随它永久停探等于把老行钉死：清掉重新攒。
	// 新代码判恢复时 Results 至少有 SuccessTarget 条，清标记与关开关都连窗口一起清，撞不到这条。
	if !st.RecoveredAt.IsZero() && len(st.Results) == 0 {
		st.RecoveredAt = time.Time{}
		st.UpdatedAt = now
		s.persistRecovery(ctx, account, st) // 立刻落库：别让页面在下次探测前还写着「已恢复」
	}
	// 判定恢复后不再探：结论已经有了，继续探只是白付额度。
	if !st.RecoveredAt.IsZero() || now.Before(st.NextAt) || now.After(deadline) {
		return false
	}
	attempt := s.probeOwnExit(ctx, account, cfg)
	st.push(attempt)
	st.pushResult(attempt.Healthy, cfg.StreakTarget)
	st.CoolingUntil = time.Time{}
	if attempt.Healthy {
		st.FailStreak = 0
		st.NextAt = now.Add(cfg.interval())
		if st.successes() >= cfg.SuccessTarget {
			st.RecoveredAt = s.now()
			slog.Info("openai_turn_state_recovered", "account_id", account.ID, "model", cfg.Model,
				"successes", st.successes(), "window", len(st.Results))
		}
	} else {
		// 答错、非 200、传输错误都算一次失败：这个计数只决定「要不要歇会儿再探」，
		// 把它们分开只会多一条路径，省下的额度是同一笔。
		st.FailStreak++
		if st.FailStreak >= cfg.StreakTarget {
			st.FailStreak = 0 // 冷却结束后重新数，不要一醒来就又满
			st.Results = nil  // 这一批作废，醒来从头攒
			st.CoolingUntil = now.Add(cfg.cooldown())
			st.NextAt = st.CoolingUntil
			slog.Info("openai_turn_state_recovery_cooldown", "account_id", account.ID, "model", cfg.Model,
				"until", st.CoolingUntil.UTC().Format(time.RFC3339))
		} else {
			st.NextAt = now.Add(cfg.interval())
		}
	}
	st.UpdatedAt = s.now()
	s.persistRecovery(ctx, account, st)
	slog.Info("openai_turn_state_recovery_attempt",
		"account_id", account.ID, "model", cfg.Model, "proxy_id", attempt.ProxyID, "status", attempt.Status,
		"answer", attempt.Answer, "healthy", attempt.Healthy, "error", attempt.Error,
		"successes", st.successes(), "window", len(st.Results), "fail_streak", st.FailStreak)
	return true
}

// probeOwnExit 走账号自己的出口探一次。与猎手探测的两处刻意不同：
//   - 不换代理：这条探测问的就是「常驻出口上还降不降智」。
//   - 不设 req.Close：猎手靠关连接逼出新出口，这里出口是固定的；而连接池按「账号 × 代理 URL」
//     缓存，关连接会把真实流量正在用的那条 HTTP/2 隧道一起带走。复用连接也更像真人。
func (s *OpenAITurnStateHunterService) probeOwnExit(ctx context.Context, account *Account, cfg openAITurnStateRecoveryConfig) openAITurnStateHuntAttempt {
	attempt := openAITurnStateHuntAttempt{At: s.now(), Model: cfg.Model}
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
	s.doProbe(ctx, account, &egress, proxyURL, false, cfg.Model,
		openAITurnStateHunterConfig{ReasoningEffort: cfg.ReasoningEffort, UsageAPIKeyID: usageKeyID},
		openAITurnStateRecoveryPrompt, true, &attempt)
	return attempt
}

// openAITurnStateRecoveryAnswer 从探测的 SSE 体里取模型回答与真实用量：只认 response.completed
// 里 message 项的 output_text（与网关非流式路径同一个终态提取器）。没有终态事件（流被截断、
// response.failed）返回 ok=false；终态里没带 usage 时 usage 为 nil，记账退回输入估算。
func openAITurnStateRecoveryAnswer(raw []byte) (answer string, usage *OpenAIUsage, ok bool) {
	final, ok := extractCodexFinalResponse(string(raw))
	if !ok {
		return "", nil, false
	}
	var text strings.Builder
	gjson.GetBytes(final, "output").ForEach(func(_, item gjson.Result) bool {
		if item.Get("type").String() != "message" {
			return true
		}
		item.Get("content").ForEach(func(_, part gjson.Result) bool {
			if part.Get("type").String() == "output_text" {
				_, _ = text.WriteString(part.Get("text").String())
			}
			return true
		})
		return true
	})
	if got, ok := extractOpenAIUsageFromJSONBytes(final); ok {
		usage = &got
	}
	return normalizeOpenAITurnStateRecoveryAnswer(text.String()), usage, true
}

// normalizeOpenAITurnStateRecoveryAnswer 去掉首尾的空白、标点与符号：题目要求「只输出一个数字」，
// 但模型偶尔会写成 "21。"、"**21**" 或 "「21」"，那仍是答对。只削两头，"21.5"、"210" 照样判错。
func normalizeOpenAITurnStateRecoveryAnswer(s string) string {
	s = strings.TrimFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r) })
	return truncateUTF8(s, openAITurnStateRecoveryAnswerKeep)
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

// resetOpenAITurnStateRecovery 在账号又自然铸出 312 时清掉「已恢复」标记与判定窗口，从头攒。
// 只有标记或窗口非空时才写库：这是响应热路径，判定恢复之后每条 312 都写一次就太贵了。
func (s *OpenAIGatewayService) resetOpenAITurnStateRecovery(ctx context.Context, account *Account) {
	if s == nil || s.accountRepo == nil || account == nil {
		return
	}
	st := readOpenAITurnStateRecoveryState(account)
	if st.RecoveredAt.IsZero() && len(st.Results) == 0 {
		return
	}
	st.RecoveredAt, st.Results = time.Time{}, nil
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
	for _, key := range []string{"streak_target", "success_target", "min_minutes", "max_minutes", "cooldown_hours", "usage_api_key_id"} {
		v, ok := table[key]
		if !ok || v == nil {
			continue
		}
		n, ok := v.(float64)
		if !ok || n != float64(int(n)) || n < 1 {
			return fmt.Errorf("%s.%s must be a positive integer", openAITurnStateRecoveryExtraKey, key)
		}
	}
	// 成功次数超过总次数永远判不了恢复；applyDefaults 会压顶，但管理员填错要当场知道。
	if success, ok := table["success_target"].(float64); ok {
		window := float64(defaultOpenAITurnStateRecoveryStreak)
		if v, ok := table["streak_target"].(float64); ok {
			window = v
		}
		if success > window {
			return fmt.Errorf("%s.success_target must not exceed streak_target", openAITurnStateRecoveryExtraKey)
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
