package service

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// 双开兼容桥（/v1/messages → Codex）的出站身份。
//
// 桥没有 Codex 客户端，基线出站只有网关自造的下划线 session_id 别名，与同账号真客户端的
// /responses 是两种形态。双开时把桥伪装成一个真客户端会话：在入站侧合成一套"客户端原始身份"
// ——会话/线程 UUIDv7（整个会话不变，protocol/src/session_id.rs:19-24、thread_id.rs:27-32）、
// 每轮新铸的 turn UUIDv7（core/src/session/mod.rs:981 new_submission_id 是 now_v7）、窗口
// "<thread>:0"（core/src/session/mod.rs:4190 current_window，序号从 0 起）、每窗口一枚
// context_window_id v7（core/src/state/auto_compact_window.rs:13）、request_kind="turn" 的
// turn-metadata（core/src/responses_metadata.rs turn_metadata_payload / client_metadata /
// compatibility_headers）——写进入站头与请求体 client_metadata / prompt_cache_key（真客户端默认
// PCK = session_id，core/src/client.rs:504-516），然后交给与 /responses 完全相同的账号隔离 →
// 指纹收敛 → 线协议投影管线派生出站。桥自己不再单独写任何会话头。
//
// 合成的 turn-metadata 只带身份、轮次字段与取自请求体的 model / reasoning_effort；真客户端还带 sandbox /
// sandbox_mode / thread_source / auto_review_enabled / node_repl_* / workspaces / analytics_enabled 等
// 环境与配置事实，桥没有对应的真实来源，不编造（用户决定：sandbox / thread_source / turn_trigger / extra 不做；
// 这里的 extra 指产品 / app-server 自定义元数据——model / reasoning_effort 在 Rust 里虽也落在 extra 映射，
// 但不是编造，取自请求体）。
type openAICompatBridgeSession struct {
	SessionID       string
	ContextWindowID string
	ExpiresAt       time.Time
}

// openAICompatBridgeTurnMetadata 的字段序 = codex-rs CodexTurnMetadataPayload 的声明序
// （core/src/responses_metadata.rs:510-567）；serde 按声明序输出，网关后续只做原位改写。
type openAICompatBridgeTurnMetadata struct {
	InstallationID      string `json:"installation_id"`
	SessionID           string `json:"session_id"`
	ThreadID            string `json:"thread_id"`
	AgentName           string `json:"agent_name"`
	TurnID              string `json:"turn_id"`
	WindowID            string `json:"window_id"`
	WindowNumber        uint64 `json:"window_number"`
	ContextWindowID     string `json:"context_window_id"`
	RequestKind         string `json:"request_kind"`
	TurnStartedAtUnixMs int64  `json:"turn_started_at_unix_ms"`
	// codex 0.156 起 ExecutionMetadata 写进 extra（BTreeMap，flatten 在声明字段之后按键名排序）：本轮 model
	// 与选中的 reasoning effort（rust-v0.156.1 session/session.rs:667、turn_metadata.rs:76-94）。取自桥的
	// 注入时的出站体，不是编造：桥体里的 effort 就是它选的档位（Anthropic output_config.effort 映射，
	// max→xhigh、其余原样），没有真客户端 ultra / persistent 那种解析。注入之后分组推理强度策略还可能改体里
	// 的 effort（openai_gateway_messages.go），metadata 保留注入时的档位，与直连 /responses 同一口径（effort
	// 不在发送边界同步）；体里的 model 被改则发送边界会同步（codexTurnMetadataExecutionValues）。
	Model           string `json:"model,omitempty"`
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
}

// 根线程的 agent_name：protocol/src/agent_path.rs:18 AgentPath::ROOT（core/src/turn_metadata.rs:200-203）。
const openAICompatBridgeAgentName = "/root"

// openAICompatBridgeSessionKey 按凭证域命名空间 + 下游 API key + 桥会话键定位会话：共用一份
// ChatGPT 凭证的多个本地行 / spark 影子行刻意共享出站身份（与 scopeCodexAccountIdentityValue
// 的隔离空间一致），换行或 failover 时会话不变；没有命名空间时退回本地行 ID。
func openAICompatBridgeSessionKey(c *gin.Context, account *Account, promptCacheKey string) string {
	key := strings.TrimSpace(promptCacheKey)
	if account == nil || key == "" {
		return ""
	}
	namespace := codexAccountIdentityNamespace(codexAccountIdentitySource(c, account))
	if namespace == "" {
		namespace = "id:" + strconv.FormatInt(account.ID, 10)
	}
	apiKeyID := int64(0)
	if c != nil {
		apiKeyID = getAPIKeyIDFromContext(c)
	}
	return strings.Join([]string{namespace, strconv.FormatInt(apiKeyID, 10), key}, "\x00")
}

// openAICompatBridgeSession 取（或首铸）该桥会话的身份。sync.Map 上用 LoadOrStore + CompareAndSwap
// 收口：并发首轮只会有一枚胜出，其余读到它；命中即续期，TTL 是空闲窗口而不是首铸后的定时轮换；
// 过期或实例重启后重新铸造——上游看到的是一个新会话，不是错形态。
func (s *OpenAIGatewayService) openAICompatBridgeSession(c *gin.Context, account *Account, promptCacheKey string) (openAICompatBridgeSession, bool) {
	if s == nil {
		return openAICompatBridgeSession{}, false
	}
	key := openAICompatBridgeSessionKey(c, account, promptCacheKey)
	if key == "" {
		return openAICompatBridgeSession{}, false
	}
	now := time.Now()
	ttl := s.openAIWSResponseStickyTTL()
	fresh := openAICompatBridgeSession{
		SessionID:       uuid.Must(uuid.NewV7()).String(),
		ContextWindowID: uuid.Must(uuid.NewV7()).String(),
		ExpiresAt:       now.Add(ttl),
	}
	for {
		raw, loaded := s.openaiCompatBridgeSessions.LoadOrStore(key, fresh)
		if !loaded {
			return fresh, true
		}
		existing, ok := raw.(openAICompatBridgeSession)
		expired := !existing.ExpiresAt.IsZero() && now.After(existing.ExpiresAt)
		if !ok || existing.SessionID == "" || expired {
			if s.openaiCompatBridgeSessions.CompareAndSwap(key, raw, fresh) {
				return fresh, true
			}
			continue
		}
		renewed := existing
		renewed.ExpiresAt = now.Add(ttl)
		if s.openaiCompatBridgeSessions.CompareAndSwap(key, raw, renewed) {
			return existing, true
		}
	}
}

// injectOpenAICompatBridgeIdentity 只在桥 + 双开 + 有桥会话键时生效：把合成的客户端原始身份写进
// 入站头（session-id / thread-id / x-codex-window-id / x-codex-turn-metadata）与请求体
// （prompt_cache_key、client_metadata、恒发的 tool_choice），返回还原入站头的函数（同一 gin 上下文
// 会被 failover 到下一账号复用，合成的头不能漏给别的账号）与是否注入。
func (s *OpenAIGatewayService) injectOpenAICompatBridgeIdentity(c *gin.Context, account *Account, reqBody map[string]any, promptCacheKey string) (restore func(), injected bool) {
	restore = func() {}
	if s == nil || c == nil || c.Request == nil || reqBody == nil || !codexDeviceWireProfileEnabled(c, account) {
		return restore, false
	}
	session, ok := s.openAICompatBridgeSession(c, account, promptCacheKey)
	if !ok {
		return restore, false
	}
	installationID := ""
	if ids := resolveCodexFingerprintIDsWithBody(c, account, nil, nil); ids != nil {
		installationID = ids.installationID
	}
	turnID := uuid.Must(uuid.NewV7()).String()
	windowID := session.SessionID + ":0"
	model, _ := reqBody["model"].(string)
	effort := ""
	if reasoning, ok := reqBody["reasoning"].(map[string]any); ok {
		effort, _ = reasoning["effort"].(string)
	}
	payload, err := marshalOpenAIUpstreamJSON(openAICompatBridgeTurnMetadata{
		InstallationID:      installationID,
		SessionID:           session.SessionID,
		ThreadID:            session.SessionID,
		AgentName:           openAICompatBridgeAgentName,
		TurnID:              turnID,
		WindowID:            windowID,
		WindowNumber:        0,
		ContextWindowID:     session.ContextWindowID,
		RequestKind:         "turn",
		TurnStartedAtUnixMs: time.Now().UnixMilli(),
		Model:               model,
		ReasoningEffort:     effort,
	})
	if err != nil {
		return restore, false
	}
	turnMetadata := string(bytes.TrimSpace(payload))

	original := c.Request.Header
	inbound := original.Clone()
	inbound.Set("session-id", session.SessionID)
	inbound.Set("thread-id", session.SessionID)
	inbound.Set("x-codex-window-id", windowID)
	inbound.Set(openAIWSTurnMetadataHeader, turnMetadata)
	c.Request.Header = inbound
	restore = func() { c.Request.Header = original }

	reqBody["prompt_cache_key"] = session.SessionID
	clientMetadata, _ := reqBody["client_metadata"].(map[string]any)
	if clientMetadata == nil {
		clientMetadata = make(map[string]any)
	}
	clientMetadata["session_id"] = session.SessionID
	clientMetadata["thread_id"] = session.SessionID
	clientMetadata["turn_id"] = turnID
	clientMetadata["x-codex-window-id"] = windowID
	clientMetadata[openAIWSTurnMetadataHeader] = turnMetadata
	reqBody["client_metadata"] = clientMetadata
	if _, ok := reqBody["tool_choice"]; !ok {
		// 真客户端恒发 tool_choice（codex-api/src/common.rs:289 无 skip_serializing_if），默认 "auto"。
		reqBody["tool_choice"] = "auto"
	}
	return restore, true
}
