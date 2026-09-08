package service

// klno 实验性指纹收敛（按账号开关：accounts.extra["codex_experimental_fingerprint_convergence"] = true）。
//
// 目标：同一把 API key 的出站身份在 HTTP / 透传 / WS 三条路径上与真 Codex 客户端形态一致，
// 逐条依据 openai/codex codex-rs（commit 16ff14c）：
//  1. HTTP 出站补齐真客户端恒发、但上游 HTTP 白名单丢弃的头：session-id / thread-id
//     （codex-api/src/requests/headers.rs build_session_headers）、x-codex-parent-thread-id、
//     x-openai-subagent（core/src/responses_metadata.rs、core/src/client.rs:793）。WS 路径本就转发。
//  2. x-client-request-id 恒等于 thread-id（codex-api/src/endpoint/responses.rs:120、core/src/client.rs:1245）。
//  3. 补出 session-id 后，不再发真客户端不存在的 session_id / conversation_id 下划线别名；
//     补不出（入站无会话头）时保留上游别名，否则请求会零会话身份出站。
//  4. root_turn_id / parent_turn_id 与 turn_id 同类派生；parent_thread_id / forked_from_thread_id 与
//     thread 同类；context_window_id 单独一类（core/src/session/mod.rs current_window：它是
//     AutoCompactWindowIds.window_id，v7）；x-codex-window-id / window_id 真实形态是
//     "<thread_id>:<window_number>"（同一函数 format!("{thread_id}:{window_number}")），
//     派生为 "<派生 thread_id>:<window_number>"，保持与 thread-id 的可见关联。
//  5. 原始值为 UUIDv7 时派生结果保持 v7 并保留 48 位时间戳（codex 的 session/thread/turn/window
//     均为 Uuid::now_v7；installation_id 为 v4，派生仍为 v4）。
//
// 开关关闭时以上全部不生效，出站与上游完全一致。开启开关会让该账号的 v7 类身份一次性轮换。

import (
	"crypto/sha256"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const codexFingerprintConvergenceExtraKey = "codex_experimental_fingerprint_convergence"

// codexFingerprintConvergenceEnabled 仅对 OAuth 类 OpenAI 账号生效；接受 bool / "true" / "1"。
func codexFingerprintConvergenceEnabled(account *Account) bool {
	if account == nil || !account.IsOpenAIOAuthLike() || account.Extra == nil {
		return false
	}
	switch v := account.Extra[codexFingerprintConvergenceExtraKey].(type) {
	case bool:
		return v
	case string:
		t := strings.ToLower(strings.TrimSpace(v))
		return t == "true" || t == "1"
	case float64:
		return v != 0
	}
	return false
}

// 上游字段表之外、真客户端 client_metadata / x-codex-turn-metadata 里同样携带的身份字段。
var codexConvergenceIdentityFields = []struct {
	name string
	kind string
}{
	{name: "root_turn_id", kind: "turn"},
	{name: "parent_turn_id", kind: "turn"},
	{name: "parent_thread_id", kind: "thread"},
	{name: "x-codex-parent-thread-id", kind: "thread"},
	{name: "forked_from_thread_id", kind: "thread"},
	{name: "context_window_id", kind: "context_window"},
}

// applyCodexConvergenceIdentityFields 由 applyCodexAccountIdentityFields 末尾调用，覆盖
// body client_metadata、嵌入其中的 turn-metadata 以及头部 turn-metadata 三处。
func applyCodexConvergenceIdentityFields(values map[string]any, account *Account, apiKeyID int64) bool {
	if values == nil || !codexFingerprintConvergenceEnabled(account) {
		return false
	}
	changed := false
	for _, field := range codexConvergenceIdentityFields {
		raw, ok := values[field.name].(string)
		if !ok || strings.TrimSpace(raw) == "" {
			continue
		}
		next := scopeCodexAccountIdentityValue(account, apiKeyID, field.kind, raw)
		if next != raw {
			values[field.name] = next
			changed = true
		}
	}
	return changed
}

// codex 的 window_id 形态："<thread uuid>:<window_number>"。
var codexConvergenceWindowIDPattern = regexp.MustCompile(`^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):([0-9]+)$`)

// deriveCodexConvergenceWindowValue 由 scopeCodexAccountIdentityValue 调用：开关开启且 window 类
// 原始值形如 "<thread>:<n>" 时，thread 部分按 thread 类派生、序号原样保留。
func deriveCodexConvergenceWindowValue(account *Account, apiKeyID int64, kind, raw string) (string, bool) {
	if kind != "window" || !codexFingerprintConvergenceEnabled(account) {
		return "", false
	}
	m := codexConvergenceWindowIDPattern.FindStringSubmatch(raw)
	if m == nil {
		return "", false
	}
	return scopeCodexAccountIdentityValue(account, apiKeyID, "thread", m[1]) + ":" + m[2], true
}

// deriveCodexConvergenceIdentityValue 由 scopeCodexAccountIdentityValue 调用：开关开启且原始值是
// 规范小写 UUIDv7 时，保留前 48 位时间戳，其余位由 seed 的 sha256 填充，版本位置 7。
func deriveCodexConvergenceIdentityValue(account *Account, seed, raw string) (string, bool) {
	if !codexFingerprintConvergenceEnabled(account) {
		return "", false
	}
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed.Version() != 7 || parsed.String() != raw {
		return "", false
	}
	h := sha256.Sum256([]byte(seed))
	var derived uuid.UUID
	copy(derived[:], h[:16])
	copy(derived[0:6], parsed[0:6]) // unix_ts_ms
	derived[6] = (derived[6] & 0x0f) | 0x70
	derived[8] = (derived[8] & 0x3f) | 0x80
	return derived.String(), true
}

// 真客户端恒发、上游 HTTP 白名单未放行的头；kind 为空表示原样透传（值不是身份 ID）。
var codexConvergenceInboundHeaders = []struct {
	name string
	kind string
}{
	{name: "session-id", kind: "session"},
	{name: "thread-id", kind: "thread"},
	{name: "x-codex-parent-thread-id", kind: "thread"},
	{name: "x-openai-subagent", kind: ""},
}

// applyCodexFingerprintConvergenceHeaders 在 applyStagedCodexFingerprintHeaders 之后、终态身份收口
// 之前调用（HTTP / 透传 / WS 三处相同相对位置）。
func applyCodexFingerprintConvergenceHeaders(c *gin.Context, account *Account, headers http.Header) {
	if headers == nil || !codexFingerprintConvergenceEnabled(account) || codexAccountIdentityNamespace(account) == "" {
		return
	}
	apiKeyID := getAPIKeyIDFromContext(c)
	var inbound http.Header
	if c != nil && c.Request != nil {
		inbound = c.Request.Header
	}
	// 1) 补齐被丢弃的头；WS 路径已转发并派生过的保持不动
	if inbound != nil {
		for _, field := range codexConvergenceInboundHeaders {
			if headers.Get(field.name) != "" {
				continue
			}
			raw := strings.TrimSpace(inbound.Get(field.name))
			if raw == "" {
				continue
			}
			if field.kind == "" {
				headers.Set(field.name, raw)
				continue
			}
			headers.Set(field.name, scopeCodexAccountIdentityValue(account, apiKeyID, field.kind, raw))
		}
	}
	// 2) x-client-request-id == thread-id
	if threadID := strings.TrimSpace(headers.Get("thread-id")); threadID != "" {
		headers.Set("x-client-request-id", threadID)
	}
	// 3) 真客户端没有的下划线别名。仅在确实补出了 session-id 时才删：入站没带会话头的
	// 客户端（现网 Codex Desktop 一条都不带）补不出连字符头，此时删别名会让请求出站
	// 零会话身份——真 Codex 客户端不存在这种形态，且上游据此做缓存亲和，删掉会打散
	// 路由（现网 pro1 HTTP 命中率 96% → 22%）。
	if strings.TrimSpace(headers.Get("session-id")) != "" {
		headers.Del("session_id")
		headers.Del("conversation_id")
	}
}
