package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// CPR admin API 额度适配器。
//
// 走 CPR 的 admin API 而不是响应头：CPR 会**故意剥掉** x-codex-primary-* 这批
// 配额响应头（gateway-protocol/src/openai/events.rs 的 is_codex_quota_header_name，
// 注释写明"会让下游 Codex 客户端显示账户额度提示，不能穿过账号隔离边界"），
// 所以中继账号的额度只能从 admin 侧拉。
//
// 产出刻意落在 *OpenAIRateLimit 上：下游的 buildCodexWindowExtraUpdates 已经会把
// primary/secondary 归一化成 codex_5h_* / codex_7d_* extra 键，前端读的就是这些键。
// 于是展示与 OAuth 账号完全一致，不需要新增任何展示字段。

const (
	cprAdminAPIKeyHeader   = "x-api-key"
	cprAdminRequestTimeout = 10 * time.Second
	// CPR 的 resetAtDisplay / rateLimitedUntil 都是 presenter 里 china_datetime 的产物：
	// UTC+8 的 "%Y-%m-%d %H:%M:%S"。原始时间戳在 wire 层被丢弃了
	// （accounts/presenter.rs 的 quota_window_view 解构出 reset_at 却只发 reset_at_display），
	// 只能按固定格式反解。解不出就留空，不猜。
	cprDisplayTimeLayout = "2006-01-02 15:04:05"
	cprDisplayTimeOffset = 8 * 60 * 60
)

// CPR admin 侧的错误分类。调用方要区分"CPR 配置坏了"和"这个账号坏了"——
// 前者是运维问题不该把账号标记成不可用，后者才是。
var (
	ErrCPRNotConfigured   = errors.New("cpr account is not fully configured")
	ErrCPRAdminKeyInvalid = errors.New("cpr admin api key rejected")
	ErrCPRAccountMissing  = errors.New("cpr account not found upstream")
)

// CPR 账号的五态。派生自 gateway-core/src/account/model.rs 的 resolve_account_status，
// 只有 normal 会被 CPR 的调度器放行。
const (
	CPRAccountStatusNormal         = "normal"
	CPRAccountStatusQuotaExhausted = "quota_exhausted"
	CPRAccountStatusRateLimited    = "rate_limited"
	CPRAccountStatusDisabled       = "disabled"
	CPRAccountStatusError          = "error"
)

// CPRAccountState 是一次 admin 查询的归一化结果。
type CPRAccountState struct {
	AccountID        string
	Status           string
	PlanType         string
	Email            string
	Enabled          bool
	ErrorReason      string
	LimitReached     bool
	RateLimitedUntil *time.Time
	RateLimit        *OpenAIRateLimit
	FetchedAt        time.Time
}

// Schedulable 报告 CPR 是否会把请求路由给这个账号。CPR 的 scheduling_blocker
// 只放行 normal，其余四态一律选不中（且不会 fallback 到组外账号）。
func (s *CPRAccountState) Schedulable() bool {
	return s != nil && s.Status == CPRAccountStatusNormal
}

// CPRQuotaService 查询 CPR 的 admin API。
//
// 不走账号代理：CPR 是本机服务，直连才对。admin_base_url 指向远端时同样直连，
// 这是有意的——那条链路不是上游推理流量，不应该占用账号的出口。
type CPRQuotaService struct {
	client *http.Client
	cfg    *config.Config
}

func NewCPRQuotaService(cfg *config.Config) *CPRQuotaService {
	return &CPRQuotaService{client: &http.Client{Timeout: cprAdminRequestTimeout}, cfg: cfg}
}

// --- CPR admin API 的 wire 结构（只取我们用得到的字段）---

type cprAdminEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type cprAccountDetailData struct {
	Account cprAccountView `json:"account"`
}

type cprAccountView struct {
	ID          string          `json:"id"`
	Email       string          `json:"email"`
	PlanType    string          `json:"planType"`
	Status      string          `json:"status"`
	ErrorReason string          `json:"errorReason"`
	Enabled     bool            `json:"enabled"`
	Quota       cprAccountQuota `json:"quota"`
}

type cprAccountQuota struct {
	LimitReached     bool             `json:"limitReached"`
	RateLimitedUntil string           `json:"rateLimitedUntil"`
	Windows          []cprQuotaWindow `json:"windows"`
}

type cprQuotaWindow struct {
	Role           string   `json:"role"`
	LimitID        string   `json:"limitId"`
	WindowSeconds  *int64   `json:"windowSeconds"`
	UsedPercent    *float64 `json:"usedPercent"`
	LimitReached   bool     `json:"limitReached"`
	ResetAtDisplay string   `json:"resetAtDisplay"`
}

// FetchAccountState 拉取一个 CPR 账号的状态与额度。
func (s *CPRQuotaService) FetchAccountState(ctx context.Context, account *Account) (*CPRAccountState, error) {
	if s == nil || s.client == nil {
		return nil, ErrCPRNotConfigured
	}
	if !account.IsCPR() {
		return nil, fmt.Errorf("%w: not a cpr account", ErrCPRNotConfigured)
	}
	adminBase := account.GetCPRAdminBaseURL()
	adminKey := account.GetCPRAdminAPIKey()
	cprAccountID := account.GetCPRAccountID()
	if adminBase == "" || adminKey == "" || cprAccountID == "" {
		return nil, ErrCPRNotConfigured
	}
	// 与网关地址过同一套 security.url_allowlist 判定：否则打开白名单后会出现
	// "网关地址被校验、admin 地址随便填"的缺口，x-api-key 会发去任意主机。
	validatedAdminBase, err := validateOutboundURLWithConfig(s.cfg, adminBase)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid admin_base_url: %v", ErrCPRNotConfigured, err)
	}
	adminBase = validatedAdminBase

	// accountId 是管理端自由文本：不转义的话含空格会让 NewRequest 解析失败，
	// 含 & / # 会注入额外 query 参数。
	endpoint := buildOpenAIEndpointURL(strings.TrimRight(adminBase, "/"), "/api/admin/accounts/detail") +
		"?accountId=" + url.QueryEscape(cprAccountID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build cpr admin request: %w", err)
	}
	req.Header.Set(cprAdminAPIKeyHeader, adminKey)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cpr admin request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// admin 响应是小 JSON；限一下防止对端异常时吃内存。
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read cpr admin response: %w", err)
	}

	var envelope cprAdminEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode cpr admin response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || envelope.Code != http.StatusOK {
		switch envelope.Code {
		case 40103, 40101:
			return nil, fmt.Errorf("%w: %s", ErrCPRAdminKeyInvalid, envelope.Message)
		case 40401:
			return nil, fmt.Errorf("%w: %s", ErrCPRAccountMissing, envelope.Message)
		default:
			return nil, fmt.Errorf("cpr admin error %d: %s", envelope.Code, envelope.Message)
		}
	}

	var detail cprAccountDetailData
	if err := json.Unmarshal(envelope.Data, &detail); err != nil {
		return nil, fmt.Errorf("decode cpr account detail: %w", err)
	}

	now := time.Now()
	return buildCPRAccountState(&detail.Account, now), nil
}

// buildCPRAccountState 把 CPR 的账号视图翻译成 sub2api 的额度模型。
func buildCPRAccountState(view *cprAccountView, now time.Time) *CPRAccountState {
	if view == nil {
		return nil
	}
	state := &CPRAccountState{
		AccountID:    strings.TrimSpace(view.ID),
		Status:       strings.TrimSpace(view.Status),
		PlanType:     strings.TrimSpace(view.PlanType),
		Email:        strings.TrimSpace(view.Email),
		ErrorReason:  strings.TrimSpace(view.ErrorReason),
		Enabled:      view.Enabled,
		LimitReached: view.Quota.LimitReached,
		FetchedAt:    now,
	}
	if until := parseCPRDisplayTime(view.Quota.RateLimitedUntil); until != nil {
		state.RateLimitedUntil = until
	}
	state.RateLimit = buildCPRRateLimit(view.Quota, now)
	return state
}

// buildCPRRateLimit 把 windows[] 折成 primary/secondary 两个窗口。
//
// CPR 的 role 有 primary / secondary / monthly 三种；sub2api 的模型只有 5h 与 7d
// 两档（由 Normalize 按窗口长度归类），monthly 没有对应位置，丢弃而不是硬塞。
func buildCPRRateLimit(quota cprAccountQuota, now time.Time) *OpenAIRateLimit {
	limit := &OpenAIRateLimit{LimitReached: quota.LimitReached}
	matched := false
	for _, window := range quota.Windows {
		converted := convertCPRWindow(window, now)
		if converted == nil {
			continue
		}
		switch strings.TrimSpace(window.Role) {
		case "primary":
			limit.PrimaryWindow = converted
			matched = true
		case "secondary":
			limit.SecondaryWindow = converted
			matched = true
		}
	}
	if !matched {
		return nil
	}
	limit.Allowed = !quota.LimitReached
	return limit
}

func convertCPRWindow(window cprQuotaWindow, now time.Time) *OpenAIRateLimitWindow {
	// 百分比是这条记录存在的理由；没有它这个窗口没有展示价值。
	if window.UsedPercent == nil || window.WindowSeconds == nil || *window.WindowSeconds <= 0 {
		return nil
	}
	converted := &OpenAIRateLimitWindow{
		UsedPercent:        *window.UsedPercent,
		LimitWindowSeconds: *window.WindowSeconds,
	}
	if resetAt := parseCPRDisplayTime(window.ResetAtDisplay); resetAt != nil {
		converted.ResetAt = resetAt.Unix()
		remaining := int64(resetAt.Sub(now).Seconds())
		if remaining < 0 {
			// 快照比重置时间还旧：窗口已经翻过去了。报 0（"现在就重置"）而不是
			// 负数，负数会让下游算出一个过去的 reset_at。
			remaining = 0
		}
		converted.ResetAfterSeconds = remaining
	}
	return converted
}

// refreshCPRCodexSnapshot 拉一次 CPR admin 额度并写进账号的 codex_* extra。
//
// 失败时保留上一次快照：admin 接口不可用是运维问题，不该让展示归零，更不该
// 被误读成"这个账号没额度了"。
func (s *AccountUsageService) refreshCPRCodexSnapshot(ctx context.Context, account *Account, usage *UsageInfo, now time.Time) {
	if s == nil || s.cprQuotaService == nil || account == nil {
		return
	}
	state, err := s.cprQuotaService.FetchAccountState(ctx, account)
	if err != nil {
		slog.Warn("cpr_account_state_query_failed", "account_id", account.ID, "error", err)
		return
	}
	updates := buildCPRCodexExtraUpdates(state)
	if len(updates) == 0 {
		return
	}
	mergeAccountExtra(account, updates)
	s.persistOpenAICodexProbeSnapshot(account.ID, updates)
	if usage != nil {
		if usage.UpdatedAt == nil {
			usage.UpdatedAt = &now
		}
		applyExtraToUsage(usage, account.Extra, now)
	}
}

// buildCPRCodexExtraUpdates 复用 OAuth 那条链的归一化函数，产出同一组 codex_* 键，
// 因此前端展示与 OAuth 账号完全一致，不需要任何新的展示字段。
//
// 没有窗口时返回空 map，调用方据此一个字节都不写：
//   - mergeAccountExtra 只写不删，推进 codex_usage_updated_at 会把上一次的
//     codex_5h_* 标成"刚刷新"，显示一个陈旧百分比。
//   - 若在这里塞入无人消费的状态键，len(updates) 恒不为零，
//     每一次 /usage 请求都会触发一次 UpdateExtra 写库。
//
// 行为与 OAuth 那条路一致（buildCodexPrimaryWindowExtraUpdates 返回 nil 时同样什么
// 都不写），代价是时效判定继续认为快照缺失、下次还会再拉一次——对本机 admin 调用
// 可以接受。CPR 账号的状态与错误原因在 CPR 自己后台就能看到，不在这里重复存。
func buildCPRCodexExtraUpdates(state *CPRAccountState) map[string]any {
	if state == nil {
		return nil
	}
	return buildCodexWindowExtraUpdates(state.RateLimit, state.FetchedAt)
}

// parseCPRDisplayTime 反解 CPR 的 UTC+8 显示时间。解不出返回 nil——
// 上游格式一旦变化这里会静默退化成"没有重置时间"，而不是给出错误的时间。
func parseCPRDisplayTime(value string) *time.Time {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "—" {
		return nil
	}
	loc := time.FixedZone("UTC+8", cprDisplayTimeOffset)
	parsed, err := time.ParseInLocation(cprDisplayTimeLayout, trimmed, loc)
	if err != nil {
		return nil
	}
	return &parsed
}
