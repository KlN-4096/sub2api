package service

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// codexFingerprintIDsContextKey 是暂存在 gin context 的收敛 ID 集合键。
// 由 Forward（非透传）或 forwardOpenAIPassthrough（透传）解析后写入，请求
// 构造器读取用于出站头改写——请求体与出站头必须共享同一份 IDs，保证
// turn_id 等随机字段一致。
const codexFingerprintIDsContextKey = "codex_fingerprint_ids"

// stageCodexFingerprintIDs 将本 attempt 解析出的收敛 ID 暂存到 gin context。
// 必须无条件覆写（含 nil）：failover 从收敛账号切到 off 账号时，上一账号的
// IDs 不得残留并被误应用到新账号的出站头（typed-nil 由应用侧 nil 守卫吸收）。
func stageCodexFingerprintIDs(c *gin.Context, ids *codexFingerprintIDs) {
	if c != nil {
		c.Set(codexFingerprintIDsContextKey, ids)
	}
}

func stagedCodexFingerprintIDs(c *gin.Context, account *Account) *codexFingerprintIDs {
	if c == nil || account == nil || !account.UsesOpenAICodexProtocol() {
		return nil
	}
	value, ok := c.Get(codexFingerprintIDsContextKey)
	if !ok {
		return nil
	}
	ids, ok := value.(*codexFingerprintIDs)
	if !ok || ids == nil || ids.accountID != account.ID {
		return nil
	}
	return ids
}

// applyStagedCodexFingerprintHeaders 读取 context 暂存的收敛 ID 并改写出站头。
// 非透传与透传两个请求构造器共用本函数，防止应用语义漂移。仅解析该
// snapshot 的 OAuth 账号可读取，避免 stale context 跨账号 failover 泄漏。
func applyStagedCodexFingerprintHeaders(c *gin.Context, account *Account, h http.Header) {
	applyCodexFingerprintHeaders(h, stagedCodexFingerprintIDs(c, account))
}

func applyStagedCodexFingerprintClientMetadata(c *gin.Context, account *Account, reqBody map[string]any) bool {
	return applyCodexFingerprintClientMetadata(reqBody, stagedCodexFingerprintIDs(c, account))
}

// codexFingerprintMode 控制 OAuth 账号出站请求的设备指纹收敛强度。
// 多人共享同一 OAuth 账号时，每个用户的 Codex 客户端会携带各自不同的
// installation_id / session_id / thread_id，上游据此判定设备数和会话数。
// 收敛模式将这些标识改写为账号级恒定值，减少上游可见的设备/会话指纹。
type codexFingerprintMode string

const (
	// codexFingerprintOff 不做任何收敛，原样透传客户端标识。
	// 这是默认值：收敛是显式 opt-in 的（见 GetCodexFingerprintMode）。
	codexFingerprintOff codexFingerprintMode = "off"
	// codexFingerprintDevice 仅收敛 installation_id 为账号级恒定值。
	// 上游看到 1 台设备 + 多会话（每用户各自的 session）。
	codexFingerprintDevice codexFingerprintMode = "device"
	// codexFingerprintSession 收敛 installation_id + session_id，
	// thread_id 按客户端原始 session-id 确定性派生（每个真实 Codex 会话一个独立线程）。
	// 上游看到 1 台设备 + 1 会话 + N 线程，最接近正常用户 spawn 子代理的模式。
	codexFingerprintSession codexFingerprintMode = "session"
	// codexFingerprintFull 收敛所有标识：installation_id + session_id + thread_id。
	// 上游看到 1 台设备 + 1 会话 + 1 线程，最激进。
	codexFingerprintFull codexFingerprintMode = "full"
)

const (
	codexFingerprintModeExtraKey = "codex_fingerprint_mode"
	codexFingerprintSeedExtraKey = "codex_fingerprint_seed"
)

func canonicalCodexFingerprintSeed(value any) (string, bool) {
	raw, ok := value.(string)
	if !ok {
		return "", false
	}
	trimmed := strings.TrimSpace(raw)
	parsed, err := uuid.Parse(trimmed)
	if err != nil || parsed == uuid.Nil || trimmed != parsed.String() {
		return "", false
	}
	return trimmed, true
}

func newCodexFingerprintSeed() string {
	return uuid.NewString()
}

func stripCodexFingerprintSeed(extra map[string]any) map[string]any {
	if extra == nil {
		return nil
	}
	stripped := maps.Clone(extra)
	delete(stripped, codexFingerprintSeedExtraKey)
	return stripped
}

func codexFingerprintModeFromExtra(extra map[string]any) codexFingerprintMode {
	if extra == nil {
		return codexFingerprintOff
	}
	raw, _ := extra[codexFingerprintModeExtraKey].(string)
	switch codexFingerprintMode(strings.TrimSpace(raw)) {
	case codexFingerprintOff, codexFingerprintDevice, codexFingerprintSession, codexFingerprintFull:
		return codexFingerprintMode(strings.TrimSpace(raw))
	default:
		return codexFingerprintOff
	}
}

func codexFingerprintModeRequiresSeed(mode codexFingerprintMode) bool {
	switch mode {
	case codexFingerprintDevice, codexFingerprintSession, codexFingerprintFull:
		return true
	default:
		return false
	}
}

func codexFingerprintSeed(extra map[string]any) (string, bool) {
	if extra == nil {
		return "", false
	}
	return canonicalCodexFingerprintSeed(extra[codexFingerprintSeedExtraKey])
}

func prepareCodexFingerprintExtraForCreate(platform, accountType string, extra map[string]any) map[string]any {
	prepared := stripCodexFingerprintSeed(extra)
	if platform != PlatformOpenAI || (accountType != AccountTypeOAuth && accountType != AccountTypeSetupToken) || !codexFingerprintModeRequiresSeed(codexFingerprintModeFromExtra(prepared)) {
		return prepared
	}
	if prepared == nil {
		prepared = make(map[string]any, 1)
	}
	prepared[codexFingerprintSeedExtraKey] = newCodexFingerprintSeed()
	return prepared
}

func prepareCodexFingerprintExtraForUpdate(account *Account, extra map[string]any) map[string]any {
	prepared := stripCodexFingerprintSeed(extra)
	if account == nil || !account.IsOpenAIOAuthLike() {
		return prepared
	}
	if seed, ok := codexFingerprintSeed(account.Extra); ok {
		if prepared == nil {
			prepared = make(map[string]any, 1)
		}
		prepared[codexFingerprintSeedExtraKey] = seed
		return prepared
	}
	if codexFingerprintModeRequiresSeed(codexFingerprintModeFromExtra(prepared)) {
		if prepared == nil {
			prepared = make(map[string]any, 1)
		}
		prepared[codexFingerprintSeedExtraKey] = newCodexFingerprintSeed()
	}
	return prepared
}

func sanitizedCodexFingerprintExtraUpdates(updates map[string]any) map[string]any {
	if updates == nil {
		return nil
	}
	sanitized := maps.Clone(updates)
	delete(sanitized, codexFingerprintSeedExtraKey)
	return sanitized
}

// ShouldEnsureCodexFingerprintSeedForExtraUpdates reports whether a JSONB key-level
// extra update is enabling Codex fingerprint convergence and therefore must atomically
// preserve or create the system-managed per-account seed in the repository update.
func ShouldEnsureCodexFingerprintSeedForExtraUpdates(updates map[string]any) bool {
	if updates == nil {
		return false
	}
	return codexFingerprintModeRequiresSeed(codexFingerprintModeFromExtra(updates))
}

// GetCodexFingerprintMode 从账号 extra JSON 读取指纹收敛模式。
//
// **收敛是显式 opt-in**：未设置、空值或非法值一律按 off 处理，只有管理员
// 明确配置 device / session / full 才收敛。
//
// 历史：v0.1.175（#5553）把缺省值当作 session，导致升级后存量 OAuth 账号
// （普遍没有这个 extra 键）的每个非透传请求都被静默改写 installation /
// session / thread / turn / window 五类标识；#5555、#5556、#5582 报告的额度
// 缩水都卡在该版本边界，并有"回退 v0.1.173 即恢复"与"新账号开收敛后降额"
// 的 A/B 实测。上游的配额判定策略不可观测，因此这里取兼容安全的一侧：
// 不显式 opt-in 就保持 v0.1.175 之前的客户端身份（#5610）。
func (a *Account) GetCodexFingerprintMode() codexFingerprintMode {
	if a == nil || !a.IsOpenAIOAuthLike() {
		return codexFingerprintOff
	}
	return codexFingerprintModeFromExtra(a.Extra)
}

// deriveStableUUIDv4 从种子确定性派生一个 UUIDv4 格式的字符串。
// 同一种子永远返回同一值。
func deriveStableUUIDv4(seed string) string {
	h := sha256.Sum256([]byte(seed))
	b := h[:16]
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 1
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(b[0:4]),
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16])
}

// resolveConvergedInstallationID 返回账号级恒定的 installation_id。
// 优先使用管理员配置的真实 device_id，无则从系统管理的账号随机种子确定性派生。
func resolveConvergedInstallationID(account *Account, seed string) string {
	if account == nil {
		return ""
	}
	if deviceID := account.GetOpenAIDeviceID(); deviceID != "" {
		return deviceID
	}
	if seed == "" {
		return ""
	}
	return deriveStableUUIDv4("sub2api:codex-install-id:v2:" + seed)
}

// resolveConvergedSessionID 返回账号级恒定的 session_id。
func resolveConvergedSessionID(seed string) string {
	if seed == "" {
		return ""
	}
	return deriveStableUUIDv4("sub2api:codex-session-id:v2:" + seed)
}

// resolveConvergedThreadID 按客户端原始 session-id 确定性派生 thread_id。
// 每个真实 Codex 会话（不同客户端启动实例）获得一个独立线程，
// 模拟正常用户 spawn 子代理或开多窗口的模式。
func resolveConvergedThreadID(seed, clientSessionID string) string {
	if seed == "" || clientSessionID == "" {
		return ""
	}
	return deriveStableUUIDv4("sub2api:codex-thread-id:v2:" + seed + ":" + clientSessionID)
}

// codexFingerprintIDs 收敛后的完整 ID 集合。
// 由 resolveCodexFingerprintIDs 一次性生成，同一个实例在头改写和体改写之间共享，
// 确保所有载体中的 turn_id 等随机字段一致。体改写时还会补记原始
// client_metadata.session_id，用于识别 root prompt_cache_key 的默认值。
type codexFingerprintIDs struct {
	accountID                     int64
	mode                          codexFingerprintMode
	installationID                string
	sessionID                     string
	threadID                      string
	turnID                        string
	windowID                      string
	windowNumber                  uint64
	turnStartedAtUnixMs           int64
	originalBodySessionID         string
	originalBodySessionIDCaptured bool
	// klno 实验性指纹收敛开关；开启时才启用下面这条旁证链。关闭时保持上游行为：
	// 证明不了 prompt_cache_key 是会话默认值就不碰它。
	convergence bool
	// 原始会话证据命名空间化之后的副本，与请求体的 prompt_cache_key 同阶段。
	scopedClientSessionID string
}

// resolveCodexFingerprintIDs 按收敛模式计算出站 ID 集合。
// clientSessionID 是客户端原始的 session-id 头值（连字符形式），用于 session 模式下
// 的 thread_id 派生——每个真实 Codex 会话得到一个独立线程。
// 返回 nil 表示 off 模式，不需要改写。
// 注意：包含随机生成的 turn_id，调用方必须只调用一次并共享结果给头改写和体改写。
func resolveCodexFingerprintIDs(account *Account, clientSessionID string, mode codexFingerprintMode) *codexFingerprintIDs {
	return resolveCodexFingerprintIDsWithWindow(account, clientSessionID, mode, "")
}

// clientWindowNumber 是客户端当前的上下文窗口序号（x-codex-window-id 的 ":<n>" 部分，
// 或 turn-metadata.window_number）。空串按真实客户端的初始值 0 处理
// （codex-rs core/src/state/auto_compact_window.rs:51 window_number: 0），每次 auto-compact 递增。
func resolveCodexFingerprintIDsWithWindow(account *Account, clientSessionID string, mode codexFingerprintMode, clientWindowNumber string) *codexFingerprintIDs {
	if account == nil || mode == codexFingerprintOff {
		return nil
	}
	seed, ok := codexFingerprintSeed(account.Extra)
	if !ok {
		return nil
	}

	ids := &codexFingerprintIDs{
		accountID:           account.ID,
		mode:                mode,
		turnStartedAtUnixMs: time.Now().UnixMilli(),
		convergence:         codexFingerprintConvergenceEnabled(account),
	}

	ids.installationID = resolveConvergedInstallationID(account, seed)
	if ids.installationID == "" {
		return nil
	}
	// OrDefault 只返回至多 19 位十进制非负整数，必定落在 uint64 范围内。
	ids.windowNumber, _ = strconv.ParseUint(codexWindowNumberOrDefault(clientWindowNumber), 10, 64)
	windowNumber := strconv.FormatUint(ids.windowNumber, 10)

	switch mode {
	case codexFingerprintDevice:
		return ids

	case codexFingerprintSession:
		ids.sessionID = resolveConvergedSessionID(seed)
		ids.threadID = resolveConvergedThreadID(seed, clientSessionID)
		if ids.threadID == "" {
			ids.threadID = ids.sessionID
		}
		ids.turnID = uuid.Must(uuid.NewV7()).String()
		ids.windowID = ids.threadID + ":" + windowNumber
		return ids

	case codexFingerprintFull:
		ids.sessionID = resolveConvergedSessionID(seed)
		ids.threadID = ids.sessionID
		ids.turnID = uuid.Must(uuid.NewV7()).String()
		ids.windowID = ids.threadID + ":" + windowNumber
		return ids
	}

	return nil
}

// extractClientSessionID 从请求头中提取客户端原始的会话标识。
// 优先取 session-id（连字符形式，Codex CLI 标准），回退到 session_id（下划线形式）。
// 返回的值尚未被 isolateOpenAISessionID 改写，是客户端的真实标识。
func extractClientSessionID(h http.Header) string {
	if v := strings.TrimSpace(h.Get("session-id")); v != "" {
		return v
	}
	return strings.TrimSpace(h.Get("session_id"))
}

// codexWindowNumberOrDefault 归一化窗口序号；非法或缺失时回落到真实客户端的初始值 0。
func codexWindowNumberOrDefault(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !codexWindowNumberPattern.MatchString(raw) {
		return "0"
	}
	return raw
}

// extractClientWindowNumber 从 x-codex-window-id 的 "<thread>:<n>" 里取序号，
// 取不到时回退 turn-metadata.window_number。
func extractClientWindowNumber(h http.Header) string {
	for _, name := range [...]string{"x-codex-window-id", "window_id"} {
		if m := codexConvergenceWindowIDPattern.FindStringSubmatch(strings.TrimSpace(h.Get(name))); m != nil {
			return m[2]
		}
	}
	meta := gjson.Parse(h.Get(openAIWSTurnMetadataHeader))
	if number := codexWindowNumberFromJSON(meta.Get("window_number")); number != "" {
		return number
	}
	if m := codexConvergenceWindowIDPattern.FindStringSubmatch(meta.Get("window_id").String()); m != nil {
		return m[2]
	}
	return ""
}

// resolveCodexFingerprintIDsFromRequest 从客户端原始请求头中提取 session-id，
// 结合账号配置一次性解析收敛 ID 集合。调用方应将返回的 ids 同时传给
// applyCodexFingerprintHeaders 和 applyCodexFingerprintClientMetadata。
// clientHeaders 为 nil 时从 c 取；账号探测那种没有 gin context 的调用方直接传头。
func resolveCodexFingerprintIDsFromRequest(c *gin.Context, account *Account, clientHeaders http.Header) *codexFingerprintIDs {
	return resolveCodexFingerprintIDsWithBody(c, account, clientHeaders, nil)
}

// Resolve before account-scoping the body: a body-only client must derive the same
// per-session thread as a direct client, not hash an already-derived session again.
func resolveCodexFingerprintIDsWithBody(c *gin.Context, account *Account, clientHeaders http.Header, rawClientMetadata any) *codexFingerprintIDs {
	if account == nil {
		return nil
	}
	mode := account.GetCodexFingerprintMode()
	if mode == codexFingerprintOff {
		return nil
	}
	if clientHeaders == nil && c != nil && c.Request != nil {
		clientHeaders = c.Request.Header
	}
	clientSessionID := ""
	source := codexAccountIdentitySource(c, account)
	convergence := codexFingerprintConvergenceEnabled(source)
	if clientHeaders != nil {
		clientSessionID = extractClientSessionID(clientHeaders)
	}
	if clientSessionID == "" && convergence {
		clientSessionID = codexFingerprintSessionEvidence(rawClientMetadata, true)
	}
	if clientSessionID == "" && convergence && clientHeaders != nil {
		clientSessionID = codexConvergenceMetadataString(gjson.Parse(clientHeaders.Get(openAIWSTurnMetadataHeader)), "session_id")
	}
	// 后续 WS 帧的窗口会递增，握手头却固定不变；当前 body 优先，头仅兜底。
	clientWindowNumber := codexFingerprintWindowNumberEvidence(rawClientMetadata)
	if clientWindowNumber == "" && clientHeaders != nil {
		clientWindowNumber = extractClientWindowNumber(clientHeaders)
	}
	ids := resolveCodexFingerprintIDsWithWindow(account, clientSessionID, mode, clientWindowNumber)
	if ids == nil {
		return nil
	}
	ids.convergence = convergence
	if convergence && clientSessionID != "" && c != nil {
		// 入站头是原始值，而判定时拿到的 prompt_cache_key 已经被命名空间化过。这里先派生
		// 一份同阶段的副本，否则 H(S) 与 S 永远不等，默认缓存键会被误判成显式覆盖值。
		ids.scopedClientSessionID = scopeCodexAccountIdentityValue(
			source, getAPIKeyIDFromContext(c), "session", clientSessionID)
	}
	return ids
}

// applyCodexFingerprintHeaders 按预计算的收敛 ID 改写出站 HTTP 头中的设备指纹。
// 在 buildUpstreamRequest 的白名单透传之后、enforceCodexIdentityHeaders 之前调用。
func applyCodexFingerprintHeaders(h http.Header, ids *codexFingerprintIDs) {
	if h == nil || ids == nil {
		return
	}

	// 所有非 off 模式都收敛 installation_id
	h.Set("x-codex-installation-id", ids.installationID)

	if ids.mode == codexFingerprintDevice {
		rewriteCodexTurnMetadataFields(h, map[string]any{
			"installation_id": ids.installationID,
		}, ids)
		return
	}

	// session / full 模式：改写所有相关头
	h.Set("x-codex-window-id", ids.windowID)
	h.Set("x-client-request-id", ids.threadID)
	// 连字符形式和下划线形式都改写，保证一致
	h.Set("session-id", ids.sessionID)
	h.Set("session_id", ids.sessionID)
	h.Set("thread-id", ids.threadID)

	rewriteCodexTurnMetadataFields(h, map[string]any{
		"installation_id":         ids.installationID,
		"session_id":              ids.sessionID,
		"thread_id":               ids.threadID,
		"turn_id":                 ids.turnID,
		"window_id":               ids.windowID,
		"window_number":           ids.windowNumber,
		"turn_started_at_unix_ms": ids.turnStartedAtUnixMs,
	}, ids)
}

// rewriteCodexTurnMetadataFields 解析 x-codex-turn-metadata 头中的 JSON，
// 替换指定字段后回写。合法对象保留未指定字段（如 sandbox、thread_source）；
// 非法/非对象值重建为最小合法 metadata，避免 flat 与 embedded identity 分裂。
func rewriteCodexTurnMetadataFields(h http.Header, fields map[string]any, ids *codexFingerprintIDs) {
	raw := h.Get(openAIWSTurnMetadataHeader)
	if strings.TrimSpace(raw) == "" {
		return
	}
	h.Set(openAIWSTurnMetadataHeader, rewriteCodexFingerprintTurnMetadata(raw, fields, ids))
}

func rewriteCodexFingerprintTurnMetadata(raw string, fields map[string]any, ids *codexFingerprintIDs) string {
	return rewriteCodexTurnMetadataJSON(raw, true, func(metadata map[string]any) map[string]any {
		updates := maps.Clone(fields)
		root, _ := metadata["root_turn_id"].(string)
		preserveCodexConvergenceRootTurn(metadata, ids)
		if next, _ := metadata["root_turn_id"].(string); next != root {
			updates["root_turn_id"] = next
		}
		return updates
	})
}

// applyCodexFingerprintClientMetadata 按预计算的收敛 ID 改写请求体中的 client_metadata。
// 使用与头改写相同的 ids 实例，确保 turn_id 等随机字段一致。
func applyCodexFingerprintClientMetadata(reqBody map[string]any, ids *codexFingerprintIDs) bool {
	if reqBody == nil || ids == nil {
		return false
	}

	captureCodexFingerprintOriginalBodySessionID(ids, reqBody["client_metadata"])
	existing, _ := reqBody["client_metadata"].(map[string]any)
	if existing == nil {
		existing = make(map[string]any)
	}

	modified := false
	if applyCodexFingerprintToClientMetadataMap(existing, ids) {
		reqBody["client_metadata"] = existing
		modified = true
	}
	if applyCodexFingerprintPromptCacheKey(reqBody, ids) {
		modified = true
	}
	return modified
}

// applyCodexFingerprintToClientMetadataMap 是 client_metadata 改写的共享核心，
// map 版（非透传，body 已解码）与 raw 字节版（透传热路径）都经由它，保证两条
// 路径的收敛语义永不漂移。
func applyCodexFingerprintToClientMetadataMap(existing map[string]any, ids *codexFingerprintIDs) bool {
	if existing == nil || ids == nil {
		return false
	}

	modified := false

	if ids.installationID != "" {
		existing["x-codex-installation-id"] = ids.installationID
		modified = true
	}

	if ids.mode == codexFingerprintDevice {
		rewriteClientMetadataEmbeddedTurnMetadata(existing, map[string]any{
			"installation_id": ids.installationID,
		}, ids)
		return modified
	}

	// session / full 模式
	preserveCodexConvergenceRootTurn(existing, ids)
	existing["session_id"] = ids.sessionID
	existing["thread_id"] = ids.threadID
	existing["turn_id"] = ids.turnID
	existing["x-codex-window-id"] = ids.windowID

	rewriteClientMetadataEmbeddedTurnMetadata(existing, map[string]any{
		"installation_id":         ids.installationID,
		"session_id":              ids.sessionID,
		"thread_id":               ids.threadID,
		"turn_id":                 ids.turnID,
		"window_id":               ids.windowID,
		"window_number":           ids.windowNumber,
		"turn_started_at_unix_ms": ids.turnStartedAtUnixMs,
	}, ids)
	return true
}

func captureCodexFingerprintOriginalBodySessionID(ids *codexFingerprintIDs, clientMetadata any) {
	if ids == nil || ids.originalBodySessionIDCaptured {
		return
	}
	ids.originalBodySessionIDCaptured = true
	ids.originalBodySessionID = codexFingerprintSessionEvidence(clientMetadata, ids.convergence)
}

// Shared by raw-body resolution and already-scoped body capture. The caller owns
// the phase; this reader only accepts independent string-valued session carriers.
func codexFingerprintSessionEvidence(clientMetadata any, allowEmbedded bool) string {
	var session, embedded string
	switch metadata := clientMetadata.(type) {
	case map[string]any:
		session, _ = metadata["session_id"].(string)
		embedded, _ = metadata[openAIWSTurnMetadataHeader].(string)
	case map[string]string:
		session = metadata["session_id"]
		embedded = metadata[openAIWSTurnMetadataHeader]
	case gjson.Result:
		session = codexConvergenceMetadataString(metadata, "session_id")
		embedded = codexConvergenceMetadataString(metadata, openAIWSTurnMetadataHeader)
	}
	if session = strings.TrimSpace(session); session != "" {
		return session
	}
	if allowEmbedded {
		return codexConvergenceMetadataString(gjson.Parse(embedded), "session_id")
	}
	return ""
}

// codexWindowNumberPattern 只接受十进制序号，避免把未知形态猜着解析。
var codexWindowNumberPattern = regexp.MustCompile(`^[0-9]{1,19}$`)

// codexWindowNumberFromJSON 把 window_number 归一化成字符串：真实客户端发的是数字，
// 但中继链上有把它序列化成字符串的。非整数一律当作缺失。
func codexWindowNumberFromJSON(value gjson.Result) string {
	var raw string
	switch value.Type {
	case gjson.Number:
		// 不经 float64：大的合法整数也要保持原值。
		raw = value.Raw
	case gjson.String:
		raw = strings.TrimSpace(value.Str)
	}
	if codexWindowNumberPattern.MatchString(raw) {
		return raw
	}
	return ""
}

// codexFingerprintWindowNumberEvidence 与 codexFingerprintSessionEvidence 同构：
// 优先从当前请求/帧的 client_metadata 取窗口；没有有效证据时才回退到头。
func codexFingerprintWindowNumberEvidence(clientMetadata any) string {
	var window, embedded string
	switch metadata := clientMetadata.(type) {
	case map[string]any:
		window, _ = metadata["x-codex-window-id"].(string)
		embedded, _ = metadata[openAIWSTurnMetadataHeader].(string)
	case map[string]string:
		window = metadata["x-codex-window-id"]
		embedded = metadata[openAIWSTurnMetadataHeader]
	case gjson.Result:
		window = codexConvergenceMetadataString(metadata, "x-codex-window-id")
		embedded = codexConvergenceMetadataString(metadata, openAIWSTurnMetadataHeader)
	}
	if m := codexConvergenceWindowIDPattern.FindStringSubmatch(strings.TrimSpace(window)); m != nil {
		return m[2]
	}
	meta := gjson.Parse(embedded)
	if number := codexWindowNumberFromJSON(meta.Get("window_number")); number != "" {
		return number
	}
	if m := codexConvergenceWindowIDPattern.FindStringSubmatch(meta.Get("window_id").String()); m != nil {
		return m[2]
	}
	return ""
}

// shouldRewriteCodexFingerprintPromptCacheKey 判定 prompt_cache_key 是不是"会话默认键"，
// 只有是的时候才允许 session/full 模式把它改写成账号会话常量。取证分三级，缺证据就不动：
//
//	体内有 client_metadata.session_id   -> 与之相等才算（上游原有规则）
//	收敛开启且缺体内 session_id         -> 允许嵌入/入站 turn-metadata、入站 session 头作旁证；
//	                                     只比较同阶段派生值
//	没有独立会话证据                    -> 保留缓存键；字段缺失不能证明它是默认值
//
// 复合形态一律排除：那是 codex 子代理分支自己的键（client.rs:512 "{source}:{parent_thread_id}"、
// guardian/review_session.rs:304），本就不等于 session_id，改写会抹掉父线程关系。
func shouldRewriteCodexFingerprintPromptCacheKey(ids *codexFingerprintIDs, promptCacheKey string) bool {
	if ids == nil || ids.sessionID == "" {
		return false
	}
	if ids.mode != codexFingerprintSession && ids.mode != codexFingerprintFull {
		return false
	}
	if ids.convergence && codexConvergencePromptCacheKeyPattern.MatchString(strings.TrimSpace(promptCacheKey)) {
		return false
	}
	if ids.originalBodySessionIDCaptured && ids.originalBodySessionID != "" {
		return promptCacheKey == ids.originalBodySessionID
	}
	if !ids.convergence {
		return false
	}
	if ids.scopedClientSessionID != "" {
		return promptCacheKey == ids.scopedClientSessionID
	}
	return false
}

func applyCodexFingerprintPromptCacheKey(reqBody map[string]any, ids *codexFingerprintIDs) bool {
	if reqBody == nil {
		return false
	}
	promptCacheKey, ok := reqBody["prompt_cache_key"].(string)
	if !ok || strings.TrimSpace(promptCacheKey) == "" || !shouldRewriteCodexFingerprintPromptCacheKey(ids, promptCacheKey) {
		return false
	}
	if promptCacheKey == ids.sessionID {
		return false
	}
	reqBody["prompt_cache_key"] = ids.sessionID
	return true
}

// applyCodexFingerprintClientMetadataRaw 在原始 JSON 字节上改写 client_metadata，
// 供透传路径使用——透传是热路径，禁止对可能高达数十 MB 的 body 做全量
// Unmarshal（见 forwardOpenAIPassthrough 的轻量提取注释）。实现为：gjson 提取
// client_metadata 小对象单独解码，经共享核心改写后 sjson 一次性拼回，body
// 其余字节原样保留；root prompt_cache_key 仅在可证明是 body session 默认值时
// 做标量改写。语义与 applyCodexFingerprintClientMetadata 逐点一致（含
// "非对象值整体替换为收敛集合"的行为）。
func applyCodexFingerprintClientMetadataRaw(body []byte, ids *codexFingerprintIDs) ([]byte, bool, error) {
	if len(body) == 0 || ids == nil {
		return body, false, nil
	}
	// 非 JSON 对象的 body（数组/标量/畸形）没有 client_metadata 语义，
	// sjson 在这类根上写字段会改写整体结构，直接放行保持原样。
	root := gjson.ParseBytes(body)
	if !root.IsObject() {
		captureCodexFingerprintOriginalBodySessionID(ids, nil)
		return body, false, nil
	}

	existing := map[string]any{}
	if cm := gjson.GetBytes(body, "client_metadata"); cm.IsObject() {
		captureCodexFingerprintOriginalBodySessionID(ids, cm)
		if err := json.Unmarshal([]byte(cm.Raw), &existing); err != nil {
			return body, false, fmt.Errorf("decode client_metadata for fingerprint: %w", err)
		}
	} else {
		captureCodexFingerprintOriginalBodySessionID(ids, nil)
	}

	next := body
	modified := false
	if applyCodexFingerprintToClientMetadataMap(existing, ids) {
		// 上游编码器：不转义 HTML，内嵌 turn-metadata 里的 <>& 才能与刚保住的原字节一致。
		raw, err := marshalOpenAIUpstreamJSON(existing)
		if err != nil {
			return body, false, fmt.Errorf("encode converged client_metadata: %w", err)
		}
		var setErr error
		next, setErr = sjson.SetRawBytes(body, "client_metadata", raw)
		if setErr != nil {
			return body, false, fmt.Errorf("splice converged client_metadata: %w", setErr)
		}
		modified = true
	}
	promptCacheKey := gjson.GetBytes(body, "prompt_cache_key")
	if promptCacheKey.Exists() && promptCacheKey.Type == gjson.String && strings.TrimSpace(promptCacheKey.String()) != "" && shouldRewriteCodexFingerprintPromptCacheKey(ids, promptCacheKey.String()) {
		rewritten, err := sjson.SetBytes(next, "prompt_cache_key", ids.sessionID)
		if err != nil {
			return body, false, fmt.Errorf("splice converged prompt_cache_key: %w", err)
		}
		next = rewritten
		modified = true
	}
	return next, modified, nil
}

// rewriteClientMetadataEmbeddedTurnMetadata 改写 client_metadata 中内嵌的
// x-codex-turn-metadata JSON 字符串里的指定字段。非法/非对象值会重建，
// 避免 flat client_metadata 与 embedded metadata 暴露两套身份。
func rewriteClientMetadataEmbeddedTurnMetadata(clientMetadata map[string]any, fields map[string]any, ids *codexFingerprintIDs) {
	raw, ok := clientMetadata["x-codex-turn-metadata"].(string)
	if !ok || raw == "" {
		return
	}
	clientMetadata[openAIWSTurnMetadataHeader] = rewriteCodexFingerprintTurnMetadata(raw, fields, ids)
}
