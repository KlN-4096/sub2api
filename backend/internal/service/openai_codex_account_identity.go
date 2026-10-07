package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codexAccountIdentityNamespaceVersion = "v1"

const codexAccountIdentitySourceContextKey = "openai_codex_account_identity_source"

// prepareCodexAccountIdentitySource resolves credential shadows once per selected
// attempt. The handler reuses gin.Context across failover attempts, so every entry
// point overwrites the staged source before projecting outbound identity.
func (s *OpenAIGatewayService) prepareCodexAccountIdentitySource(ctx context.Context, c *gin.Context, account *Account) (*Account, error) {
	source := account
	if account != nil && account.IsShadow() {
		resolved, err := resolveCredentialAccount(ctx, s.accountRepo, account)
		if err != nil {
			return nil, err
		}
		source = resolved
	}
	if c != nil {
		c.Set(codexAccountIdentitySourceContextKey, source)
	}
	return source, nil
}

func codexAccountIdentitySource(c *gin.Context, fallback *Account) *Account {
	if c != nil {
		if staged, ok := c.Get(codexAccountIdentitySourceContextKey); ok {
			if source, ok := staged.(*Account); ok && source != nil {
				return source
			}
		}
	}
	return fallback
}

// codexAccountIdentityNamespace returns a stable, credential-scoped namespace.
// Multiple local rows that use the same ChatGPT account intentionally share the
// same namespace. Setup tokens use an irreversible bearer fingerprint because
// they have no refresh lifecycle or imported account metadata. Refreshable OAuth
// otherwise falls back only to a persistent fingerprint seed: local row IDs are
// deployment-relative and must never become upstream identity.
func codexAccountIdentityNamespace(account *Account) string {
	if account == nil || !account.IsOpenAIOAuthLike() {
		return ""
	}
	if upstreamAccountID := strings.TrimSpace(account.GetChatGPTAccountID()); upstreamAccountID != "" {
		if upstreamUserID := strings.TrimSpace(account.GetCredential("chatgpt_user_id")); upstreamUserID != "" {
			return "chatgpt:" + upstreamAccountID + ":user:" + upstreamUserID
		}
		return "chatgpt:" + upstreamAccountID
	}
	if seed, ok := codexFingerprintSeed(account.Extra); ok {
		return "seed:" + seed
	}
	if account.Type == AccountTypeSetupToken {
		if token := strings.TrimSpace(account.GetOpenAIAccessToken()); token != "" {
			sum := sha256.Sum256([]byte("openai-setup-token:" + token))
			return fmt.Sprintf("setup-token:%x", sum[:16])
		}
	}
	return ""
}

// isolateOpenAIUpstreamSessionID preserves the existing API-key isolation while
// adding the selected OAuth credential namespace. A scheduler failover therefore
// cannot send the same session/conversation identity through two upstream accounts.
func isolateOpenAIUpstreamSessionID(apiKeyID int64, account *Account, raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	namespace := codexAccountIdentityNamespace(account)
	if namespace == "" {
		return isolateOpenAISessionID(apiKeyID, raw)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("u%d:a%s:%s", apiKeyID, namespace, raw)))
	return fmt.Sprintf("%x", sum[:8])
}

func scopeCodexAccountIdentityValue(account *Account, apiKeyID int64, kind, raw string) string {
	raw = strings.TrimSpace(raw)
	namespace := codexAccountIdentityNamespace(account)
	if raw == "" || namespace == "" {
		return raw
	}
	// klno 实验性指纹收敛：复合形态（window "<thread>:<n>"、prompt-cache "<source>:<thread>"）
	// 只派生其中的 UUID 部分并保留整体形态
	if derived, ok := deriveCodexIdentityCompositeValue(account, apiKeyID, kind, raw); ok {
		return derived
	}
	seed := fmt.Sprintf(
		"sub2api:codex-account-identity:%s:user:%d:account:%s:kind:%s:value:%s",
		codexAccountIdentityNamespaceVersion,
		apiKeyID,
		namespace,
		// klno 实验性指纹收敛：session 类并入 thread 类，保住 codex 的
		// session_id == 根线程 ID 关系
		codexIdentitySeedKind(kind),
		raw,
	)
	// klno 实验性指纹收敛：原始值为 UUIDv7 时保持 v7 形态（见 openai_codex_fingerprint_convergence.go）
	if derived, ok := deriveCodexConvergenceIdentityValue(account, seed, raw); ok {
		return derived
	}
	return deriveStableUUIDv4(seed)
}

var codexAccountIdentityFields = []struct {
	name string
	kind string
}{
	{name: "installation_id", kind: "installation"},
	{name: "x-codex-installation-id", kind: "installation"},
	{name: "session_id", kind: "session"},
	{name: "session-id", kind: "session"},
	{name: "thread_id", kind: "thread"},
	{name: "thread-id", kind: "thread"},
	{name: "turn_id", kind: "turn"},
	{name: "turn-id", kind: "turn"},
	{name: "window_id", kind: "window"},
	{name: "x-codex-window-id", kind: "window"},
	{name: "x-client-request-id", kind: "request"},
}

func applyCodexAccountIdentityFields(values map[string]any, account *Account, apiKeyID int64) bool {
	if values == nil || codexAccountIdentityNamespace(account) == "" {
		return false
	}
	changed := false
	for _, field := range codexAccountIdentityFields {
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
	// klno 实验性指纹收敛：root_turn_id / parent_* / context_window_id 与同类字段同源派生
	if applyCodexConvergenceIdentityFields(values, account, apiKeyID) {
		changed = true
	}
	return changed
}

func applyCodexAccountIdentityEmbeddedMetadata(values map[string]any, account *Account, apiKeyID int64) bool {
	raw, ok := values[openAIWSTurnMetadataHeader].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return false
	}
	next := scopeCodexAccountTurnMetadata(raw, account, apiKeyID)
	if next == raw {
		return false
	}
	values[openAIWSTurnMetadataHeader] = next
	return true
}

func scopeCodexAccountTurnMetadata(raw string, account *Account, apiKeyID int64) string {
	return rewriteCodexTurnMetadataJSON(raw, false, func(metadata map[string]any) map[string]any {
		before := maps.Clone(metadata)
		if !applyCodexAccountIdentityFields(metadata, account, apiKeyID) {
			return nil
		}
		fields := make(map[string]any)
		for name, value := range metadata {
			if text, ok := value.(string); ok && text != before[name] {
				fields[name] = text
			}
		}
		return fields
	})
}

func applyCodexAccountIdentityClientMetadataMap(requestBody map[string]any, account *Account, apiKeyID int64) bool {
	if requestBody == nil || codexAccountIdentityNamespace(account) == "" {
		return false
	}
	changed := false
	clientMetadata, _ := requestBody["client_metadata"].(map[string]any)
	originalBodySessionID := ""
	if clientMetadata != nil {
		originalBodySessionID, _ = clientMetadata["session_id"].(string)
		if applyCodexAccountIdentityFields(clientMetadata, account, apiKeyID) {
			changed = true
		}
		if applyCodexAccountIdentityEmbeddedMetadata(clientMetadata, account, apiKeyID) {
			changed = true
		}
	}
	if raw, ok := requestBody["prompt_cache_key"].(string); ok && strings.TrimSpace(raw) != "" {
		kind := "prompt-cache"
		if strings.TrimSpace(originalBodySessionID) != "" && raw == originalBodySessionID {
			kind = "session"
		}
		next := scopeCodexAccountIdentityValue(account, apiKeyID, kind, raw)
		if next != raw {
			requestBody["prompt_cache_key"] = next
			changed = true
		}
	}
	return changed
}

// applyCodexAccountIdentityClientMetadataRaw scopes only the small identity
// subobjects with gjson/sjson. The passthrough hot path never unmarshals the
// potentially multi-megabyte request body.
func applyCodexAccountIdentityClientMetadataRaw(body []byte, account *Account, apiKeyID int64) ([]byte, bool, error) {
	if len(body) == 0 || codexAccountIdentityNamespace(account) == "" {
		return body, false, nil
	}
	root := gjson.ParseBytes(body)
	if !root.IsObject() {
		return body, false, nil
	}

	next := body
	changed := false
	originalBodySessionID := ""
	if cm := gjson.GetBytes(body, "client_metadata"); cm.IsObject() {
		clientMetadata := map[string]any{}
		if err := json.Unmarshal([]byte(cm.Raw), &clientMetadata); err != nil {
			return body, false, fmt.Errorf("decode client_metadata for account identity: %w", err)
		}
		originalBodySessionID, _ = clientMetadata["session_id"].(string)
		metadataChanged := applyCodexAccountIdentityFields(clientMetadata, account, apiKeyID)
		if applyCodexAccountIdentityEmbeddedMetadata(clientMetadata, account, apiKeyID) {
			metadataChanged = true
		}
		if metadataChanged {
			raw, err := marshalOpenAIUpstreamJSON(clientMetadata)
			if err != nil {
				return body, false, fmt.Errorf("encode account-scoped client_metadata: %w", err)
			}
			var setErr error
			next, setErr = sjson.SetRawBytes(next, "client_metadata", raw)
			if setErr != nil {
				return body, false, fmt.Errorf("splice account-scoped client_metadata: %w", setErr)
			}
			changed = true
		}
	}
	if promptCacheKey := gjson.GetBytes(body, "prompt_cache_key"); promptCacheKey.Type == gjson.String && strings.TrimSpace(promptCacheKey.String()) != "" {
		raw := promptCacheKey.String()
		kind := "prompt-cache"
		if strings.TrimSpace(originalBodySessionID) != "" && raw == originalBodySessionID {
			kind = "session"
		}
		scoped := scopeCodexAccountIdentityValue(account, apiKeyID, kind, raw)
		if scoped != raw {
			rewritten, err := sjson.SetBytes(next, "prompt_cache_key", scoped)
			if err != nil {
				return body, false, fmt.Errorf("splice account-scoped prompt_cache_key: %w", err)
			}
			next = rewritten
			changed = true
		}
	}
	return next, changed, nil
}

func applyCodexAccountIdentityHeaders(headers http.Header, account *Account, apiKeyID int64) {
	if headers == nil || codexAccountIdentityNamespace(account) == "" {
		return
	}
	for _, field := range codexAccountIdentityFields {
		// Underscore session/conversation headers are rebuilt separately from the
		// prompt cache key by each request builder.
		if field.name == "session_id" {
			continue
		}
		raw := strings.TrimSpace(headers.Get(field.name))
		if raw != "" {
			headers.Set(field.name, scopeCodexAccountIdentityValue(account, apiKeyID, field.kind, raw))
		}
	}
	if raw := headers.Get(openAIWSTurnMetadataHeader); strings.TrimSpace(raw) != "" {
		headers.Set(openAIWSTurnMetadataHeader, scopeCodexAccountTurnMetadata(raw, account, apiKeyID))
	}
}

// stageCodexOAuthIdentity 把 Codex OAuth 请求体里的身份字段收口，并把出站头要用的
// IDs 暂存进上下文。Forward 与 turn-state 猎手的探测共用这一段，**顺序敏感**：
//
//  1. 带真实 device_id 时补齐 client_metadata 安装标识（compact 形态没有 client_metadata，跳过）。
//  2. 解析指纹 IDs。compact 形态只跳过请求体侧：真实 Codex 的 compact 请求体同样没有
//     client_metadata（codex-rs core/src/client.rs 的 compact 请求结构无该字段），但出站头
//     照发 x-codex-installation-id 与 session-id / thread-id（同文件 compact_conversation_history
//     的 extra_headers）。故头侧的 IDs 解析与暂存不能跟着体侧一起跳过，否则同一账号的
//     compact 请求会带着另一套按客户端原值派生的设备身份出站。
//  3. 账号命名空间改写 client_metadata。命名空间与指纹收敛正交：保留每个客户端的身份基数，
//     但 failover 之后绝不把同一组 Codex IDs 复用到另一份 OAuth 凭据上。
//  4. 指纹收敛改写 client_metadata：请求体和出站头共享同一份 IDs（turn_id 等随机字段一致）。
//  5. stageCodexFingerprintIDs 无条件覆写（含 nil）：failover 从收敛账号切到 off 账号时，
//     上一账号的 IDs 不得残留。
//  6. 暂存体内已派生的会话身份，供出站头在入站没有连字符会话头时重建。排在指纹改写
//     之后，否则 session/full 模式会存下一份过期的 session；compact 形态跳过，那时体内
//     还是客户端原值，不能拿来当出站头。
//
// 返回请求体是否被改写。调用方若要读原始 prompt_cache_key，必须在调用前读。
func stageCodexOAuthIdentity(c *gin.Context, account *Account, decoded map[string]any, isCompactRequest bool) bool {
	modified := !isCompactRequest && applyCodexClientMetadata(decoded, account)
	var fpIDs *codexFingerprintIDs
	if isCompactRequest {
		fpIDs = resolveCodexFingerprintIDsFromRequest(c, account, nil)
		if applyCodexCompactPromptCacheKey(c, account, decoded) {
			modified = true
		}
	} else {
		fpIDs = resolveCodexFingerprintIDsWithBody(c, account, nil, decoded["client_metadata"])
	}
	source := codexAccountIdentitySource(c, account)
	if !isCompactRequest && applyCodexAccountIdentityClientMetadataMap(decoded, source, getAPIKeyIDFromContext(c)) {
		modified = true
	}
	if !isCompactRequest && fpIDs != nil && applyCodexFingerprintClientMetadata(decoded, fpIDs) {
		modified = true
	}
	stageCodexFingerprintIDs(c, fpIDs)
	if !isCompactRequest {
		stageCodexConvergenceBodyIdentityMap(c, source, decoded)
	}
	return modified
}
