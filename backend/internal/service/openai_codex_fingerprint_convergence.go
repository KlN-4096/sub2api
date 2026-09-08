package service

// klno 实验性指纹收敛（按账号开关：accounts.extra["codex_experimental_fingerprint_convergence"] = true）。
//
// 目标：同一把 API key 的出站身份在 HTTP / 透传 / WS 三条路径上与真 Codex 客户端形态一致，
// 逐条依据 openai/codex codex-rs（commit 16ff14c）：
//  1. HTTP 出站补齐真客户端恒发、但上游 HTTP 白名单丢弃的头：session-id / thread-id
//     （codex-api/src/requests/headers.rs build_session_headers）、x-codex-parent-thread-id、
//     x-openai-subagent（core/src/responses_metadata.rs、core/src/client.rs:793）。WS 路径本就转发。
//  2. x-client-request-id 恒等于 thread-id（codex-api/src/endpoint/responses.rs:120、core/src/client.rs:1245）。
//  3. 入站没有连字符会话头时（apikey 中继会剥掉它们），依次从请求体 client_metadata、出站
//     x-codex-turn-metadata 里那一份已派生的 session_id / thread_id / parent_thread_id 重建，
//     与直连形态逐字节相同。
//  4. 补出 session-id 后，不再发真客户端不存在的 session_id / conversation_id 下划线别名；
//     仍补不出（体内也没有）时保留上游别名，否则请求会零会话身份出站。
//  5. root_turn_id / parent_turn_id 与 turn_id 同类派生；parent_thread_id / forked_from_thread_id 与
//     thread 同类；context_window_id 单独一类（core/src/session/mod.rs current_window：它是
//     AutoCompactWindowIds.window_id，v7）；x-codex-window-id / window_id 真实形态是
//     "<thread_id>:<window_number>"（同一函数 format!("{thread_id}:{window_number}")），
//     派生为 "<派生 thread_id>:<window_number>"，保持与 thread-id 的可见关联。
//  6. 原始值为 UUIDv7 时派生结果保持 v7 并保留 48 位时间戳（codex 的 session/thread/turn/window
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
	"github.com/tidwall/gjson"
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

// codexConvergenceSeedKind 由 scopeCodexAccountIdentityValue 调用：开关开启时把整个"会话族"
// 并成一类。codex 里这三者本就是同一个 UUID——根会话的 session_id 就是根线程的 ID
// （core/src/session/session.rs:791 SessionId::from(thread_id)），prompt_cache_key 默认又直接
// 返回 session_id（core/src/client.rs:515）。分成三类各自派生，相等的原始值会变成三个不同的
// UUID：上游看到的每个请求都成了"子代理线程"，而且头、体、turn-metadata 三处对不上。
// 原始值本就不同（子代理的 thread_id、显式 prompt_cache_key override）时派生结果仍然不同，
// 关系两侧都保住。复合形态的 prompt_cache_key 在这之前已被 composite 分支接走。
func codexConvergenceSeedKind(account *Account, kind string) string {
	switch kind {
	case "session", "prompt-cache":
		if codexFingerprintConvergenceEnabled(account) {
			return "thread"
		}
	}
	return kind
}

// Preserve a proven root-turn relationship when session/full replaces turn_id.
// A child turn's different root belongs to its parent and must not be overwritten.
func preserveCodexConvergenceRootTurn(metadata map[string]any, ids *codexFingerprintIDs) {
	if metadata == nil || ids == nil || !ids.convergence || ids.turnID == "" {
		return
	}
	if ids.mode != codexFingerprintSession && ids.mode != codexFingerprintFull {
		return
	}
	turn, _ := metadata["turn_id"].(string)
	root, _ := metadata["root_turn_id"].(string)
	if strings.TrimSpace(turn) != "" && root == turn {
		metadata["root_turn_id"] = ids.turnID
	}
}

const codexConvergenceUUIDPattern = `[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`

// codex 的 window_id 形态："<thread uuid>:<window_number>"。
var codexConvergenceWindowIDPattern = regexp.MustCompile(`^(` + codexConvergenceUUIDPattern + `):([0-9]+)$`)

// codex 的子代理 prompt_cache_key 形态："<session_source>:<parent_thread_id>"
// （core/src/client.rs:512 format!("{source}:{parent_thread_id}")，另有
// guardian/review_session.rs:304 的 "guardian:{parent_thread_id}"）。
var codexConvergencePromptCacheKeyPattern = regexp.MustCompile(`^([A-Za-z0-9_-]{1,64}):(` + codexConvergenceUUIDPattern + `)$`)

// deriveCodexConvergenceCompositeValue 由 scopeCodexAccountIdentityValue 调用：开关开启且原始值
// 是 codex 的复合形态时，只派生其中的 UUID 部分、保留整体形态。整串直接哈希会压成一个裸
// UUID，而真客户端在这两个分支上从不发裸 UUID。
func deriveCodexConvergenceCompositeValue(account *Account, apiKeyID int64, kind, raw string) (string, bool) {
	if !codexFingerprintConvergenceEnabled(account) {
		return "", false
	}
	switch kind {
	case "window": // "<thread>:<n>"：thread 部分按 thread 类派生，序号原样保留
		if m := codexConvergenceWindowIDPattern.FindStringSubmatch(raw); m != nil {
			return scopeCodexAccountIdentityValue(account, apiKeyID, "thread", m[1]) + ":" + m[2], true
		}
	case "prompt-cache": // "<source>:<parent_thread>"：source 原样，thread 部分按 thread 类派生
		if m := codexConvergencePromptCacheKeyPattern.FindStringSubmatch(raw); m != nil {
			return m[1] + ":" + scopeCodexAccountIdentityValue(account, apiKeyID, "thread", m[2]), true
		}
	}
	return "", false
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

// 体内会话身份 -> 出站头名。真客户端两侧同源：CodexResponsesMetadata 既写 client_metadata
// 又建连字符头（core/src/responses_metadata.rs、codex-api/src/requests/headers.rs）。
var codexConvergenceBodyToHeader = [][2]string{
	{"session_id", "session-id"},
	{"thread_id", "thread-id"},
	{"x-codex-parent-thread-id", "x-codex-parent-thread-id"},
	// 子代理标记同样两侧同源（responses_metadata.rs 的 client_metadata() 与
	// compatibility_headers() 都写它）。中继剥掉头后只剩体内那份，会出现
	// 「体内声明子代理、头上没有」的自相矛盾——正是收敛要消除的形态。
	{"x-openai-subagent", "x-openai-subagent"},
}

const codexConvergenceStagedBodyIdentityContextKey = "codex_convergence_body_identity"

// codexConvergenceStagedBodyIdentity 是请求体命名空间化之后暂存的会话身份。带 accountID：
// failover 到另一账号、而新账号那条路径跳过暂存（如 compact 形态）时，旧账号的派生值不得
// 被读走用于新账号的出站头。
type codexConvergenceStagedBodyIdentity struct {
	accountID int64
	headers   map[string]string
}

// stageCodexConvergenceBodyIdentityMap 在 applyCodexAccountIdentityClientMetadataMap 之后调用
// （非透传路径）。传入的必须是已命名空间化的 body。
func stageCodexConvergenceBodyIdentityMap(c *gin.Context, account *Account, body map[string]any) {
	if !codexFingerprintConvergenceEnabled(account) || body == nil {
		stageCodexConvergenceBodyIdentity(c, account, nil)
		return
	}
	clientMetadata, _ := body["client_metadata"].(map[string]any)
	staged := map[string]string{}
	for _, pair := range codexConvergenceBodyToHeader {
		value, _ := clientMetadata[pair[0]].(string)
		setCodexConvergenceStagedValue(staged, pair[1], value)
	}
	if staged["x-codex-parent-thread-id"] == "" {
		parent, _ := clientMetadata["parent_thread_id"].(string)
		setCodexConvergenceStagedValue(staged, "x-codex-parent-thread-id", parent)
	}
	embedded, _ := clientMetadata[openAIWSTurnMetadataHeader].(string)
	fillCodexConvergenceIdentityFrom(staged, gjson.Parse(embedded))
	stageCodexConvergenceBodyIdentity(c, account, staged)
}

// stageCodexConvergenceBodyIdentityRaw 是透传/WS 热路径的等价物：gjson 只取这几个字段，
// 不整体 Unmarshal 可能有数 MB 的请求体。
func stageCodexConvergenceBodyIdentityRaw(c *gin.Context, account *Account, body []byte) {
	if !codexFingerprintConvergenceEnabled(account) || len(body) == 0 {
		stageCodexConvergenceBodyIdentity(c, account, nil)
		return
	}
	clientMetadata := gjson.GetBytes(body, "client_metadata")
	staged := map[string]string{}
	fillCodexConvergenceIdentityFrom(staged, clientMetadata)
	fillCodexConvergenceIdentityFrom(staged, codexConvergenceEmbeddedMetadata(clientMetadata))
	stageCodexConvergenceBodyIdentity(c, account, staged)
}

func codexConvergenceMetadataString(metadata gjson.Result, name string) string {
	value := metadata.Get(name)
	if !metadata.IsObject() || value.Type != gjson.String {
		return ""
	}
	return strings.TrimSpace(value.Str)
}

func codexConvergenceEmbeddedMetadata(metadata gjson.Result) gjson.Result {
	value := metadata.Get(openAIWSTurnMetadataHeader)
	if value.Type != gjson.String {
		return gjson.Result{}
	}
	return gjson.Parse(value.Str)
}

// fillCodexConvergenceIdentityFrom 从一份身份元数据（client_metadata 或其中嵌入的
// turn-metadata）补齐还没取到的字段，已有值不覆盖。两者在本阶段都已被
// applyCodexAccountIdentity* 命名空间化，同源同值。
func fillCodexConvergenceIdentityFrom(staged map[string]string, metadata gjson.Result) {
	if !metadata.IsObject() {
		return
	}
	for _, pair := range codexConvergenceBodyToHeader {
		if staged[pair[1]] == "" {
			setCodexConvergenceStagedValue(staged, pair[1], codexConvergenceMetadataString(metadata, pair[0]))
		}
	}
	if staged["x-codex-parent-thread-id"] == "" {
		setCodexConvergenceStagedValue(staged, "x-codex-parent-thread-id", codexConvergenceMetadataString(metadata, "parent_thread_id"))
	}
}

func setCodexConvergenceStagedValue(staged map[string]string, name, value string) {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		staged[name] = trimmed
	}
}

func stageCodexConvergenceBodyIdentity(c *gin.Context, account *Account, staged map[string]string) {
	if c == nil {
		return
	}
	// Always replace the previous attempt/turn, including an empty body. A cache key alone
	// is not independent evidence of a client session and must never be staged as one.
	if account == nil || len(staged) == 0 {
		c.Set(codexConvergenceStagedBodyIdentityContextKey, nil)
		return
	}
	c.Set(codexConvergenceStagedBodyIdentityContextKey, codexConvergenceStagedBodyIdentity{accountID: account.ID, headers: staged})
}

func stagedCodexConvergenceBodyIdentity(c *gin.Context, account *Account) map[string]string {
	if c == nil || account == nil {
		return nil
	}
	value, ok := c.Get(codexConvergenceStagedBodyIdentityContextKey)
	if !ok {
		return nil
	}
	staged, ok := value.(codexConvergenceStagedBodyIdentity)
	if !ok || staged.accountID != account.ID {
		return nil
	}
	return staged.headers
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

// codexConvergenceTurnMetadataIdentity 从出站 x-codex-turn-metadata 里取会话身份。该头到这里
// 已被 applyCodexAccountIdentityHeaders 命名空间化过，与请求体 client_metadata 同源同值，
// 直接复用不再派生。它是最后一道兜底：不需要调用点显式暂存就能用上。
func codexConvergenceTurnMetadataIdentity(headers http.Header) map[string]string {
	raw := strings.TrimSpace(headers.Get(openAIWSTurnMetadataHeader))
	if raw == "" {
		return nil
	}
	metadata := gjson.Parse(raw)
	if !metadata.IsObject() {
		return nil
	}
	values := map[string]string{}
	fillCodexConvergenceIdentityFrom(values, metadata)
	return values
}

// Resolve raw session evidence, then scope and project exactly once per WS request.
// Both WS entries and subsequent response.create frames must use this same sequence.
func applyCodexIdentityToWSPayload(c *gin.Context, account *Account, payload []byte) ([]byte, error) {
	stageCodexFingerprintIDs(c, nil)
	source := codexAccountIdentitySource(c, account)
	stageCodexConvergenceBodyIdentity(c, source, nil)
	var ids *codexFingerprintIDs
	if codexFingerprintConvergenceEnabled(source) {
		ids = resolveCodexFingerprintIDsWithBody(c, account, nil, gjson.GetBytes(payload, "client_metadata"))
	}
	next, _, err := applyCodexAccountIdentityClientMetadataRaw(payload, source, getAPIKeyIDFromContext(c))
	if err != nil {
		return payload, err
	}
	if ids != nil {
		rewritten, changed, err := applyCodexFingerprintClientMetadataRaw(next, ids)
		if err != nil {
			return payload, err
		}
		if changed {
			next = rewritten
		}
	}
	stageCodexFingerprintIDs(c, ids)
	stageCodexConvergenceBodyIdentityRaw(c, source, next)
	return next, nil
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
	staged := stagedCodexConvergenceBodyIdentity(c, account)
	fromTurnMetadata := codexConvergenceTurnMetadataIdentity(headers)
	// 1) 补齐被丢弃的头；WS 路径已转发并派生过的保持不动。
	// 取值顺序：入站头 > 请求体暂存 > 出站 turn-metadata。后两者都是已派生的值，直接复用；
	// 它们存在的意义是中继会剥掉连字符头（现网 31.108 的 apikey 中继就剥 session-id /
	// thread-id / x-codex-parent-thread-id，只留体内 client_metadata 和 turn-metadata）。
	// turn-metadata 这一路不依赖任何调用点接线，所以某条路径漏接暂存时仍然能补出头来。
	for _, field := range codexConvergenceInboundHeaders {
		if headers.Get(field.name) != "" {
			continue
		}
		if raw := strings.TrimSpace(inbound.Get(field.name)); raw != "" {
			if field.kind == "" {
				headers.Set(field.name, raw)
			} else {
				headers.Set(field.name, scopeCodexAccountIdentityValue(account, apiKeyID, field.kind, raw))
			}
			continue
		}
		if value := staged[field.name]; value != "" {
			headers.Set(field.name, value)
			continue
		}
		if value := fromTurnMetadata[field.name]; value != "" {
			headers.Set(field.name, value)
		}
	}
	// 2) x-client-request-id == thread-id
	if threadID := strings.TrimSpace(headers.Get("thread-id")); threadID != "" {
		headers.Set("x-client-request-id", threadID)
	}
	// 3) 真客户端没有的下划线别名。仅在确实补出了 session-id 时才删：入站和请求体都拿不到
	// 会话身份时补不出连字符头，此时删别名会让请求零会话身份出站——真 Codex 客户端不存在
	// 这种形态，且上游据此做缓存亲和，删掉会打散路由（现网 pro1 HTTP 命中率 96% → 22%）。
	if strings.TrimSpace(headers.Get("session-id")) != "" {
		headers.Del("session_id")
		headers.Del("conversation_id")
	}
}
