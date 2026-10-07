package service

import (
	"fmt"
	"strings"
	"time"
)

const (
	BillingTypeBalance      int8 = 0 // 钱包余额
	BillingTypeSubscription int8 = 1 // 订阅套餐
)

type RequestType int16

const (
	RequestTypeUnknown      RequestType = 0
	RequestTypeSync         RequestType = 1
	RequestTypeStream       RequestType = 2
	RequestTypeWSV2         RequestType = 3
	RequestTypeCyberBlocked RequestType = 4 // cyber_policy 命中（透传但被上游安全策略拒绝）
	RequestTypeLive         RequestType = 5
	// RequestTypeTurnStateProbe 是 292 猎手的探测（openai_turn_state_hunter.go）：挂在配置的
	// API Key 下按标准路径计费，输入 token 为本地估算、输出恒 0。
	RequestTypeTurnStateProbe RequestType = 6
	// RequestTypeGatewayPoolDegraded 是被 state-echo 判成降智后整发丢掉的那次上游尝试
	// （openai_gwpool_state_echo.go）。它真的到了上游、上游真的跑了，所以要落行；但判定点在
	// 「响应头到手、响应体一个字节都没读」的时刻，**输入与输出 token 都观测不到**
	// （两者都来自上游 response.completed 事件里的 usage）⇒ 这种行恒为 0 token / 0 金额。
	// 刻意不估算：编出来的数字进了计费表，事后没人分得清哪条是真的。
	RequestTypeGatewayPoolDegraded RequestType = 7
)

func (t RequestType) IsValid() bool {
	switch t {
	case RequestTypeUnknown, RequestTypeSync, RequestTypeStream, RequestTypeWSV2, RequestTypeCyberBlocked, RequestTypeLive,
		RequestTypeTurnStateProbe, RequestTypeGatewayPoolDegraded:
		return true
	default:
		return false
	}
}

func (t RequestType) Normalize() RequestType {
	if t.IsValid() {
		return t
	}
	return RequestTypeUnknown
}

func (t RequestType) String() string {
	switch t.Normalize() {
	case RequestTypeSync:
		return "sync"
	case RequestTypeStream:
		return "stream"
	case RequestTypeWSV2:
		return "ws_v2"
	case RequestTypeCyberBlocked:
		return "cyber"
	case RequestTypeLive:
		return "live"
	case RequestTypeTurnStateProbe:
		return "probe"
	case RequestTypeGatewayPoolDegraded:
		return "gwpool_degraded"
	default:
		return "unknown"
	}
}

func RequestTypeFromInt16(v int16) RequestType {
	return RequestType(v).Normalize()
}

func ParseUsageRequestType(value string) (RequestType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "unknown":
		return RequestTypeUnknown, nil
	case "sync":
		return RequestTypeSync, nil
	case "stream":
		return RequestTypeStream, nil
	case "ws_v2":
		return RequestTypeWSV2, nil
	case "cyber":
		return RequestTypeCyberBlocked, nil
	case "live":
		return RequestTypeLive, nil
	case "probe":
		return RequestTypeTurnStateProbe, nil
	case "gwpool_degraded":
		return RequestTypeGatewayPoolDegraded, nil
	default:
		return RequestTypeUnknown, fmt.Errorf(
			"invalid request_type, allowed values: unknown, sync, stream, ws_v2, cyber, live, probe, gwpool_degraded")
	}
}

func RequestTypeFromLegacy(stream bool, openAIWSMode bool) RequestType {
	if openAIWSMode {
		return RequestTypeWSV2
	}
	if stream {
		return RequestTypeStream
	}
	return RequestTypeSync
}

func ApplyLegacyRequestFields(requestType RequestType, fallbackStream bool, fallbackOpenAIWSMode bool) (stream bool, openAIWSMode bool) {
	switch requestType.Normalize() {
	case RequestTypeSync:
		return false, false
	case RequestTypeStream:
		return true, false
	case RequestTypeWSV2:
		return true, true
	default:
		return fallbackStream, fallbackOpenAIWSMode
	}
}

type UsageLog struct {
	ID        int64
	UserID    int64
	APIKeyID  int64
	AccountID int64
	RequestID string
	Model     string
	// RequestedModel is the client-requested model name recorded for stable user/admin display.
	// Empty should be treated as Model for backward compatibility with historical rows.
	RequestedModel string
	// UpstreamModel is the actual model sent to the upstream provider after mapping.
	// Nil means no mapping was applied (requested model was used as-is).
	UpstreamModel *string
	// UpstreamResponseModel is the model declared by the successful upstream
	// response before client-facing model rewrites or protocol conversion.
	UpstreamResponseModel *string
	// UpstreamModelMismatch is nil when no upstream model was observed. Otherwise
	// it compares UpstreamResponseModel with the actual model sent upstream.
	UpstreamModelMismatch *bool
	// ChannelID 渠道 ID
	ChannelID *int64
	// ModelMappingChain 模型映射链，如 "a→b→c"
	ModelMappingChain *string
	// BillingTier 计费层级标签（per_request/image 模式）
	BillingTier *string
	// BillingMode 计费模式：token/image
	BillingMode *string
	// ServiceTier records the billable request tier, e.g. OpenAI "priority" / "flex"
	// or Anthropic "fast".
	ServiceTier *string
	// ReasoningEffort is the effective effort recorded for this request after
	// group policy rewriting and model-family remapping (e.g. max -> xhigh).
	// OpenAI: "low" / "medium" / "high" / "xhigh"; Claude: "low" / "medium" / "high" / "max".
	// Nil means not provided / not applicable.
	ReasoningEffort *string
	// RequestedReasoningEffort is the client-requested effort before mapping.
	// Nil means historical rows, or that no explicit/suffix-derived effort was observed.
	RequestedReasoningEffort *string
	// InboundEndpoint is the client-facing API endpoint path, e.g. /v1/chat/completions.
	InboundEndpoint *string
	// UpstreamEndpoint is the normalized upstream endpoint path, e.g. /v1/responses.
	UpstreamEndpoint *string

	GroupID        *int64
	SubscriptionID *int64

	InputTokens         int
	OutputTokens        int
	CacheCreationTokens int
	CacheReadTokens     int

	CacheCreation5mTokens int `gorm:"column:cache_creation_5m_tokens"`
	CacheCreation1hTokens int `gorm:"column:cache_creation_1h_tokens"`

	ImageInputTokens  int
	ImageInputCost    float64
	ImageOutputTokens int
	ImageOutputCost   float64

	InputCost                 float64
	OutputCost                float64
	CacheCreationCost         float64
	CacheReadCost             float64
	TotalCost                 float64
	ActualCost                float64
	RateMultiplier            float64
	LongContextBillingApplied bool
	// AccountRateMultiplier 账号计费倍率快照（nil 表示历史数据，按 1.0 处理）
	AccountRateMultiplier *float64
	// AccountStatsCost 账号统计定价预计算费用（nil = 使用默认公式 total_cost × account_rate_multiplier）
	AccountStatsCost *float64

	BillingType        int8
	RequestType        RequestType
	Stream             bool
	OpenAIWSMode       bool
	NativeCompactionV2 bool
	DurationMs         *int
	FirstTokenMs       *int
	UserAgent          *string
	IPAddress          *string
	// SessionID is the explicit client-provided request correlation identifier
	// (e.g. the session_id / X-Session-Id headers). Nil when the client sent no
	// valid session header. It is never derived from prompt_cache_key or content.
	SessionID *string
	// UpstreamRequestID 是直接上游在响应头中声明的请求标识，只读账户
	// extra.upstream_request_id_header 指定的头；账户未指定头名、WS 轮次
	// 与上游没有该头的路径为 nil。
	UpstreamRequestID *string
	// TurnState 是上游本次响应头里新铸的 x-codex-turn-state（不透明 Fernet 密文）。
	// 非 Codex 上游、以及拿不到上游响应头的路径为 nil。
	TurnState *string
	// TurnStateOverridden 表示本次出站实际注入了 turn-state 覆写值。
	// nil 表示账号类型不适用（非 Codex 上游）。
	TurnStateOverridden *bool
	// TurnStateSource 是覆写来源：manual（手填）/ auto（自动接管）/
	// auto_stale（自动接管，候选已过保鲜期但仍在用）。没注入为 nil。
	TurnStateSource *string
	// TurnStateSent 是本次出站实际带的 turn-state（客户端回带的或注入的）。
	// 与 TurnState（上游新铸的）分开：带了 turn-state 的请求只有 8% 会拿到新铸值。
	TurnStateSent *string
	// SafetyBufferingEnabled / SafetyBufferingFasterModel 是上游响应头 x-codex-safety-buffering-*
	// 的读数（openai_codex_safety_buffering.go）。上游没带、非 Codex 上游、OAuth WS 轮次（暂不取事件里的头）为 nil。
	SafetyBufferingEnabled     *bool
	SafetyBufferingFasterModel *string
	// RouteGateway / RoutePair 是这一发**生效的**路由对读数（openai_codex_route_cookies.go）：
	// 网关段从 __oailb 的 JWT 载荷解出，整串留着以便原样复现。两者都只作观测，不作判据。
	//
	// **「生效的」不等于「观测到的落点」。** 上游下发了新 __oailb 时这一列是观测值；上游什么都
	// 不回时它读的是**我们自己发出去那张** ⇒ 只是「我们要求它去哪」。而上游恰恰只在改派时才
	// 下发新 cookie（带着活 pair 的健康请求一个都不回），所以
	// `RouteGateway` 与 `RoutePairPoolGateway` 相同的那些行**基本都是没观测到**，
	// 不是「确认落在承诺的网关上」——「不回新 oailb 就是没换网关」恒为真、无法证伪
	// （2026-10-02 作废的两条判据之一）。
	//
	// 后果要知道：消费者回放池子的票之后到底落在哪，现网数据答不了。想测只能靠别的手段
	// （只送 cflb、摘掉 oailb，读回来的那张），不能靠这两列。
	RouteGateway *string
	RoutePair    *string
	// RoutePairOverridden 表示这一发出站的路由对由网关池下发（openai_gwpool.go），
	// 而不是账号罐里回放的那一组。nil 表示账号类型不适用（非 Codex 上游）。
	RoutePairOverridden *bool
	// RoutePairPoolGateway 是池子交付 pair 时说的那个网关。与 RouteGateway 不一致 = 上游下发了
	// 新的 __oailb 把这一发改派走了（注入被拒）。没走池子时为 nil。
	RoutePairPoolGateway *string
	// RoutePairPoolVersion 是池子给这张票的身份（cookie_version），用来和池子侧的交付/验证
	// 日志对上账。不是 cookie 本体。没走池子 / 池子没报时为 nil。
	RoutePairPoolVersion *string

	// Cache TTL Override 标记（管理员强制替换了缓存 TTL 计费）
	CacheTTLOverridden bool

	// 图片生成字段
	ImageCount         int
	ImageSize          *string
	ImageInputSize     *string
	ImageOutputSize    *string
	ImageSizeSource    *string
	ImageSizeBreakdown map[string]int
	MediaType          *string

	// 视频生成字段（Grok 视频按秒计费；video_count>0 的行不要求 image_size）
	VideoCount           int
	VideoResolution      *string
	VideoDurationSeconds *int

	CreatedAt time.Time

	User         *User
	APIKey       *APIKey
	Account      *Account
	Group        *Group
	Subscription *UserSubscription
}

func (u *UsageLog) TotalTokens() int {
	return u.InputTokens + u.OutputTokens + u.CacheCreationTokens + u.CacheReadTokens
}

func (u *UsageLog) EffectiveRequestType() RequestType {
	if u == nil {
		return RequestTypeUnknown
	}
	if normalized := u.RequestType.Normalize(); normalized != RequestTypeUnknown {
		return normalized
	}
	return RequestTypeFromLegacy(u.Stream, u.OpenAIWSMode)
}

func (u *UsageLog) SyncRequestTypeAndLegacyFields() {
	if u == nil {
		return
	}
	requestType := u.EffectiveRequestType()
	u.RequestType = requestType
	u.Stream, u.OpenAIWSMode = ApplyLegacyRequestFields(requestType, u.Stream, u.OpenAIWSMode)
}
