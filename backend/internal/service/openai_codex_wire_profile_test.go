package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func wireProfileTestAccount(enabled bool) *Account {
	a := newTestOAuthAccount(9101, map[string]any{
		codexFingerprintModeExtraKey:        "device",
		codexFingerprintConvergenceExtraKey: enabled,
	})
	a.Status, a.Schedulable, a.Concurrency = StatusActive, true, 1
	a.Credentials = map[string]any{"access_token": "offline-token", "chatgpt_account_id": "offline-account"}
	return a
}

func wireProfileTestService() (*OpenAIGatewayService, *httpUpstreamRecorder) {
	up := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(
			`{"id":"offline","object":"response","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)),
	}}
	return &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up, toolCorrector: NewCodexToolCorrector()}, up
}

func wireProfileTestBody(t *testing.T) []byte {
	t.Helper()
	body, err := sjson.SetBytes(convTestBody(t), "stream", false)
	require.NoError(t, err)
	body, err = sjson.SetBytes(body, "instructions", "Offline test.")
	require.NoError(t, err)
	return body
}

func TestCodexDeviceWireProfileHTTP(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, passthrough := range []bool{false, true} {
			name := "map"
			if passthrough {
				name = "raw"
			}
			if enabled {
				name += "/enabled"
			} else {
				name += "/disabled"
			}
			t.Run(name, func(t *testing.T) {
				body := wireProfileTestBody(t)
				fullMetadata, err := sjson.Set(convTestTurnMetadata(), "tool_namespaces_info", []string{"offline-tool"})
				require.NoError(t, err)
				body, err = sjson.SetBytes(body, "client_metadata.x-codex-turn-metadata", fullMetadata)
				require.NoError(t, err)
				c := newConvTestContext(t, body)
				c.Request.Header.Set("x-codex-turn-metadata", fullMetadata)
				account := wireProfileTestAccount(enabled)
				account.Extra["openai_passthrough"] = passthrough
				svc, up := wireProfileTestService()
				_, _ = svc.Forward(context.Background(), c, account, body)
				require.NotNil(t, up.lastReq)
				wantInstall := resolveConvergedInstallationID(account, testCodexFingerprintSeed)
				require.Equal(t, wantInstall, gjson.GetBytes(up.lastBody, "client_metadata.x-codex-installation-id").String())
				bodyMetadata := gjson.GetBytes(up.lastBody, "client_metadata.x-codex-turn-metadata").String()
				require.True(t, gjson.Get(bodyMetadata, "tool_namespaces_info").Exists())
				require.Equal(t, resolveCodexOutboundIdentity("").version, up.lastReq.Header.Get("version"),
					"version 是 provider 头（model-provider-info/src/lib.rs:397），钉到规范身份")
				headerMetadata := gjson.Parse(up.lastReq.Header.Get(openAIWSTurnMetadataHeader))
				require.Equal(t, wantInstall, headerMetadata.Get("installation_id").String())
				require.Equal(t, !enabled, headerMetadata.Get("tool_namespaces_info").Exists())
				if enabled {
					require.Empty(t, up.lastReq.Header.Get("x-codex-installation-id"))
					require.Equal(t, up.lastReq.Header.Get("session-id"), gjson.GetBytes(up.lastBody, "prompt_cache_key").String())
					require.Empty(t, up.lastReq.Header.Get("session_id"))
				} else {
					require.Equal(t, wantInstall, up.lastReq.Header.Get("x-codex-installation-id"))
				}
			})
		}
	}
}

func TestCodexDeviceWireProfileImages(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			account := wireProfileTestAccount(enabled)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(nil))
			c.Request.Header.Set("originator", "codex-tui")
			c.Request.Header.Set("User-Agent", codexCLIUserAgent)
			svc, up := wireProfileTestService()
			_, _ = svc.ForwardImages(context.Background(), c, account, nil,
				&OpenAIImagesRequest{Endpoint: "generations", Model: "gpt-image-2", Prompt: "offline"}, "")
			require.NotNil(t, up.lastReq)
			wantInstall := resolveConvergedInstallationID(account, testCodexFingerprintSeed)
			if enabled {
				require.Equal(t, wantInstall, gjson.GetBytes(up.lastBody, "client_metadata.x-codex-installation-id").String())
				require.Empty(t, up.lastReq.Header.Get("x-codex-installation-id"))
				require.Empty(t, up.lastReq.Header.Get("OpenAI-Beta"))
				// 自建的 Responses body 同样要按真客户端的字段序出站（16ff14c common.rs:282）。
				keys := topLevelKeys(t, up.lastBody)
				require.Equal(t, "model", keys[0], "images 出站体首键必须是 model：%v", keys)
				require.Less(t, indexOf(keys, "instructions"), indexOf(keys, "input"), "%v", keys)
				require.Less(t, indexOf(keys, "input"), indexOf(keys, "tools"), "%v", keys)
				require.Equal(t, "client_metadata", keys[len(keys)-1], "%v", keys)
			} else {
				require.False(t, gjson.GetBytes(up.lastBody, "client_metadata").Exists())
				require.Equal(t, wantInstall, up.lastReq.Header.Get("x-codex-installation-id"))
				require.Equal(t, "responses=experimental", up.lastReq.Header.Get("OpenAI-Beta"))
			}
			require.Equal(t, resolveCodexOutboundIdentity("").version, up.lastReq.Header.Get("version"),
				"version 是 provider 头（model-provider-info/src/lib.rs:397），钉到规范身份")
		})
	}
}

func TestCodexDeviceWireProfileCompact(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, cacheKey := range []string{convTestSession, "custom-cache", "guardian:" + convTestThread} {
			name := "map/"
			if passthrough {
				name = "raw/"
			}
			t.Run(name+cacheKey, func(t *testing.T) {
				account := wireProfileTestAccount(true)
				account.Extra["openai_passthrough"] = passthrough
				body, err := sjson.DeleteBytes(wireProfileTestBody(t), "client_metadata")
				require.NoError(t, err)
				body, err = sjson.SetBytes(body, "prompt_cache_key", cacheKey)
				require.NoError(t, err)
				c := newConvTestContext(t, body)
				c.Request.URL.Path = "/v1/responses/compact"
				svc, up := wireProfileTestService()
				_, _ = svc.Forward(context.Background(), c, account, body)
				require.NotNil(t, up.lastReq)
				require.False(t, gjson.GetBytes(up.lastBody, "client_metadata").Exists())
				require.Equal(t, resolveConvergedInstallationID(account, testCodexFingerprintSeed),
					up.lastReq.Header.Get("x-codex-installation-id"))
				// 会话默认键按 session 命名空间派生，与出站会话头同源；自定义/复合键
				// 按 prompt-cache 派生。两条入口必须给出同一个值——非透传 compact 整段
				// 跳过了 client_metadata 的 namespace，若自定义键原样出站，不同用户的
				// 相同缓存键会在同一 OAuth 账号下互撞、读到别人的前缀缓存。
				wantCache := scopeCodexAccountIdentityValue(account, 77, "prompt-cache", cacheKey)
				if cacheKey == convTestSession {
					wantCache = up.lastReq.Header.Get("session-id")
					require.NotEmpty(t, wantCache)
				}
				require.Equal(t, wantCache, gjson.GetBytes(up.lastBody, "prompt_cache_key").String())
				require.Empty(t, up.lastReq.Header.Get("x-client-request-id"))
				require.Equal(t, resolveCodexOutboundIdentity("").version, up.lastReq.Header.Get("version"))
			})
		}
	}
}

// handler 的 compact 白名单放行 prompt_cache_key 后，投影未开的账号（现网 pro2/pro3）
// 出站必须字节级维持原样——这条键在这些账号上没有任何 namespace 兜底，留着就会让
// 不同用户的相同缓存键在同一 OAuth 账号下互撞。
func TestCodexDeviceWireProfileCompactDropsCacheKeyWhenProfileOff(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, cacheKey := range []string{convTestSession, "custom-cache"} {
			name := "map/"
			if passthrough {
				name = "raw/"
			}
			t.Run(name+cacheKey, func(t *testing.T) {
				account := wireProfileTestAccount(false)
				account.Extra["openai_passthrough"] = passthrough
				body, err := sjson.DeleteBytes(wireProfileTestBody(t), "client_metadata")
				require.NoError(t, err)
				body, err = sjson.SetBytes(body, "prompt_cache_key", cacheKey)
				require.NoError(t, err)
				c := newConvTestContext(t, body)
				c.Request.URL.Path = "/v1/responses/compact"
				svc, up := wireProfileTestService()
				_, _ = svc.Forward(context.Background(), c, account, body)
				require.NotNil(t, up.lastReq)
				require.False(t, gjson.GetBytes(up.lastBody, "prompt_cache_key").Exists(),
					"投影未开时 compact 不得带出 prompt_cache_key")
			})
		}
	}
}

func TestCodexDeviceWireProfileCompactEvidence(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		metadata   string
		legacy     string
		cacheKey   string
		wantScoped bool
	}{
		{"metadata_only", "", convTestTurnMetadata(), "", convTestSession, true},
		{"metadata_over_legacy_alias", "", convTestTurnMetadata(), "legacy-correlation", convTestSession, true},
		{"legacy_alias_is_not_native_evidence", "", "", convTestSession, convTestSession, false},
		{"non_string_metadata", "", `{"session_id":123}`, "", convTestSession, false},
		{"whitespace_is_explicit", "", convTestTurnMetadata(), "", " " + convTestSession + " ", false},
		{"metadata_padding_differs", "", `{"session_id":" ` + convTestSession + ` "}`, "", convTestSession, false},
		{"matching_metadata_padding", "", `{"session_id":" ` + convTestSession + ` "}`, "", " " + convTestSession + " ", true},
		{"header_padding_differs", " " + convTestSession + " ", convTestTurnMetadata(), "", convTestSession, false},
		{"matching_header_padding", " " + convTestSession + " ", convTestTurnMetadata(), "", " " + convTestSession + " ", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account := wireProfileTestAccount(true)
			body, err := sjson.DeleteBytes(wireProfileTestBody(t), "client_metadata")
			require.NoError(t, err)
			body, err = sjson.SetBytes(body, "prompt_cache_key", tt.cacheKey)
			require.NoError(t, err)
			c := newConvTestContext(t, body)
			c.Request.URL.Path = "/v1/responses/compact"
			c.Request.Header.Set("session-id", tt.header)
			c.Request.Header.Set(openAIWSTurnMetadataHeader, tt.metadata)
			c.Request.Header.Set("session_id", tt.legacy)
			svc, up := wireProfileTestService()
			_, _ = svc.Forward(context.Background(), c, account, body)
			require.NotNil(t, up.lastReq)
			// 旁证不成立时不按 session 派生，但仍要做账号隔离（prompt-cache 命名空间）——
			// 原样出站会让不同用户的相同缓存键在同一 OAuth 账号下互撞。
			want := scopeCodexAccountIdentityValue(account, 77, "prompt-cache", tt.cacheKey)
			if tt.wantScoped {
				want = up.lastReq.Header.Get("session-id")
				require.NotEmpty(t, want)
				require.NotEqual(t, tt.cacheKey, want)
			}
			require.Equal(t, want, gjson.GetBytes(up.lastBody, "prompt_cache_key").String())
			require.False(t, gjson.GetBytes(up.lastBody, "client_metadata").Exists())
		})
	}
}

func TestCodexDeviceWireProfileGuards(t *testing.T) {
	c := newConvTestContext(t, nil)
	base := http.Header{}
	base.Set("version", "0.153.4")
	base.Set("OpenAI-Beta", "responses=experimental, independent=enabled")
	base.Set("x-codex-installation-id", "existing-carrier")
	base.Set("session_id", "fallback-session")
	base.Set("conversation_id", "fallback-session")
	base.Set(openAIWSTurnMetadataHeader, `{"session_id":"S","tool_namespaces_info":["tool"]}`)
	for _, mode := range []string{"off", "session", "full"} {
		account := wireProfileTestAccount(true)
		account.Extra[codexFingerprintModeExtraKey] = mode
		h := base.Clone()
		applyCodexDeviceWireProfile(c, account, h, false)
		require.Equal(t, base, h, "other mode %s must remain unchanged", mode)
	}
	account := wireProfileTestAccount(false)
	h := base.Clone()
	applyCodexDeviceWireProfile(c, account, h, false)
	require.Equal(t, base, h, "opt-out must remain unchanged")

	account = wireProfileTestAccount(true)
	h = base.Clone()
	applyCodexDeviceWireProfile(c, account, h, false)
	require.Equal(t, "existing-carrier", h.Get("x-codex-installation-id"), "no lazy IDs or removal without staging")
	stageCodexFingerprintIDs(c, resolveCodexFingerprintIDsFromRequest(c, account, nil))
	applyCodexDeviceWireProfile(c, account, h, false)
	require.Empty(t, h.Get("x-codex-installation-id"))
	require.Equal(t, "independent=enabled", h.Get("OpenAI-Beta"))
	require.Equal(t, "0.153.4", h.Get("version"), "version 是 provider 头（model-provider-info/src/lib.rs:397），投影不碰")
	require.Equal(t, "fallback-session", h.Get("session_id"))
	require.Equal(t, "fallback-session", h.Get("conversation_id"))
	require.False(t, gjson.Get(h.Get(openAIWSTurnMetadataHeader), "tool_namespaces_info").Exists())
	once := h.Clone()
	applyCodexDeviceWireProfile(c, account, h, false)
	require.Equal(t, once, h, "projection must be idempotent")
	h.Set("OpenAI-Beta", openAIWSBetaV2Value)
	h.Set(openAICodexTurnStateHeader, "turn-state-token")
	applyCodexDeviceWireProfile(c, account, h, true)
	require.Equal(t, openAIWSBetaV2Value, h.Get("OpenAI-Beta"))
	require.Empty(t, h.Get(openAICodexTurnStateHeader), "WS 握手不带 turn-state（client.rs:1241 传 None）")
	// HTTP 路径不动 turn-state 头：真客户端的 HTTP /responses 就是用头带它（client.rs:2135）。
	h.Set(openAICodexTurnStateHeader, "turn-state-token")
	applyCodexDeviceWireProfile(c, account, h, false)
	require.Equal(t, "turn-state-token", h.Get(openAICodexTurnStateHeader))

	other := wireProfileTestAccount(true)
	other.ID++
	h = base.Clone()
	applyCodexDeviceWireProfile(c, other, h, false)
	require.Equal(t, "existing-carrier", h.Get("x-codex-installation-id"), "other account must not reuse staged IDs")
}

func TestCodexDeviceWireProfileAlphaMetadata(t *testing.T) {
	body := []byte(`{"id":"session","model":"gpt-5.5","input":[]}`)
	c := newConvTestContext(t, body)
	c.Request.URL.Path = "/v1/alpha/search"
	c.Request.Header.Set(openAIWSTurnMetadataHeader,
		`{"session_id":"session","thread_id":"thread","turn_id":"turn","parent_thread_id":"parent",
		"installation_id":"client","window_id":"window","window_number":2,"context_window_id":"context",
		"agent_name":"agent","parent_turn_id":"parent-turn","root_turn_id":"root-turn",
		"request_kind":"compaction","compaction":{"trigger":"auto"},"history_ingest_requested":true,
		"forked_from_ordinal_exclusive":2,"tool_namespaces_info":["tool"],
		"model":"gpt-5.5","reasoning_effort":"high","node_repl_disabled":false,"codex_version":"0.0.1"}`)
	svc, _ := wireProfileTestService()
	req, err := svc.buildOpenAIAlphaSearchRequest(context.Background(), c, wireProfileTestAccount(true), body, "offline-token")
	require.NoError(t, err)
	defer func() { _ = req.Body.Close() }()
	sent, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	meta := gjson.Parse(req.Header.Get(openAIWSTurnMetadataHeader))
	for _, field := range []string{
		"installation_id", "window_id", "window_number", "context_window_id",
		"agent_name", "parent_turn_id", "root_turn_id", "request_kind", "compaction",
		"history_ingest_requested", "forked_from_ordinal_exclusive", "tool_namespaces_info",
	} {
		require.False(t, meta.Get(field).Exists(), "MCP projection must omit %s", field)
	}
	require.NotEmpty(t, meta.Get("thread_id").String())
	require.NotEmpty(t, meta.Get("turn_id").String())
	require.NotEmpty(t, meta.Get("parent_thread_id").String(), "MCP retains parent thread")
	require.Equal(t, "gpt-5.5", meta.Get("model").String())
	require.Equal(t, "high", meta.Get("reasoning_effort").String())
	require.Equal(t, "false", meta.Get("node_repl_disabled").Raw)
	require.Equal(t, meta.Get("session_id").String(), gjson.GetBytes(sent, "id").String())
	require.Empty(t, req.Header.Get("session-id"))
	require.Empty(t, req.Header.Get("x-codex-installation-id"))
	require.Empty(t, req.Header.Get("OpenAI-Beta"))
	// version 头钉到规范身份，metadata.codex_version 与它同源。
	require.Equal(t, resolveCodexOutboundIdentity("").version, req.Header.Get("version"))
	require.Equal(t, req.Header.Get("version"), meta.Get("codex_version").String())
	require.NotEqual(t, "0.0.1", meta.Get("codex_version").String())
}

func TestCodexDeviceWireProfilePreservesUnknownMetadata(t *testing.T) {
	c := newConvTestContext(t, nil)
	account := wireProfileTestAccount(true)
	raw := `{"large":9007199254740993,"fraction":1.2300,"escaped":"\u0061","repeat":1,"repeat":2,"tool_namespaces_info":["tool"],"unknown":null}`
	h := http.Header{}
	h.Set(openAIWSTurnMetadataHeader, raw)
	applyCodexDeviceWireProfile(c, account, h, false)
	next := h.Get(openAIWSTurnMetadataHeader)
	require.False(t, gjson.Get(next, "tool_namespaces_info").Exists())
	for _, field := range []string{"large", "fraction", "escaped", "repeat", "unknown"} {
		require.Equal(t, gjson.Get(raw, field).Raw, gjson.Get(next, field).Raw, "must not re-encode %s", field)
	}
	require.Contains(t, next, `"repeat":1,"repeat":2`)
	for _, invalid := range []string{`{"tool_namespaces_info":[`, `[{"tool_namespaces_info":[]}]`, `null`} {
		h.Set(openAIWSTurnMetadataHeader, invalid)
		applyCodexDeviceWireProfile(c, account, h, false)
		require.Equal(t, invalid, h.Get(openAIWSTurnMetadataHeader), "must not repair malformed/unrecognized metadata")
	}
}

func TestCodexDeviceWireProfileInferenceCallID(t *testing.T) {
	const traceID = "bbd9bf7b-cb3d-48e7-bdcb-1c4bba7ee0a1"
	for _, enabled := range []bool{false, true} {
		for _, passthrough := range []bool{false, true} {
			for _, path := range []string{"/v1/responses", "/v1/responses/compact"} {
				for _, inbound := range []string{"", traceID} {
					name := "disabled/map"
					if enabled {
						name = "enabled/map"
					}
					if passthrough {
						name += "/raw"
					}
					t.Run(name+path+"/"+inbound, func(t *testing.T) {
						body := wireProfileTestBody(t)
						account := wireProfileTestAccount(enabled)
						account.Extra["openai_passthrough"] = passthrough
						c := newConvTestContext(t, body)
						c.Request.URL.Path = path
						c.Request.Header.Set("x-codex-inference-call-id", inbound)
						svc, up := wireProfileTestService()
						_, _ = svc.Forward(context.Background(), c, account, body)
						require.NotNil(t, up.lastReq)
						got := up.lastReq.Header.Get("x-codex-inference-call-id")
						if !enabled || path != "/v1/responses" || inbound == "" {
							require.Empty(t, got)
							return
						}
						// 真客户端每次 attempt 新铸 v4（rollout-trace/src/inference.rs:347-349）：不是原值直通，
						// 同一个下游请求再发一次（重试 / failover 到另一账号）也不会重复同一个值。
						require.NotEqual(t, inbound, got)
						parsed, err := uuid.Parse(got)
						require.NoError(t, err, "UUID 形态：%s", got)
						require.Equal(t, uuid.Version(4), parsed.Version())
						require.Equal(t, strings.ToLower(got), got)
						c2 := newConvTestContext(t, body)
						c2.Request.URL.Path = path
						c2.Request.Header.Set("x-codex-inference-call-id", inbound)
						_, _ = svc.Forward(context.Background(), c2, account, body)
						require.NotEqual(t, got, up.lastReq.Header.Get("x-codex-inference-call-id"), "每次出站新铸")
					})
				}
			}
		}
	}

	c := newConvTestContext(t, nil)
	c.Request.Header.Set("x-codex-inference-call-id", traceID)
	account := wireProfileTestAccount(true)
	for _, path := range []string{"/v1/responses", "/v1/alpha/search", "/v1/images/generations"} {
		c.Request.URL.Path = path
		headers := make(http.Header)
		applyCodexDeviceWireProfile(c, account, headers, true)
		require.Empty(t, headers.Get("x-codex-inference-call-id"), "never add the HTTP trace to a WS handshake")
		if path != "/v1/responses" {
			applyCodexDeviceWireProfile(c, account, headers, false)
			require.Empty(t, headers.Get("x-codex-inference-call-id"), "not a direct HTTP Responses request")
		}
	}
}

func TestCodexDeviceWireProfileAlphaProjectionGuards(t *testing.T) {
	c := newConvTestContext(t, nil)
	c.Request.URL.Path = "/v1/alpha/search"
	raw := `{"installation_id":"I","parent_turn_id":"P","session_id":"S","large":9007199254740993,"fraction":1.2300,"unknown":null}`
	for _, mode := range []string{"off", "device", "session", "full"} {
		for _, enabled := range []bool{false, true} {
			account := wireProfileTestAccount(enabled)
			account.Extra[codexFingerprintModeExtraKey] = mode
			h := make(http.Header)
			h.Set(openAIWSTurnMetadataHeader, raw)
			applyCodexAlphaSearchWireProfile(c, account, h, nil)
			if !enabled || mode != "device" {
				require.Equal(t, raw, h.Get(openAIWSTurnMetadataHeader))
				continue
			}
			next := h.Get(openAIWSTurnMetadataHeader)
			require.False(t, gjson.Get(next, "installation_id").Exists())
			require.False(t, gjson.Get(next, "parent_turn_id").Exists())
			for _, field := range []string{"session_id", "large", "fraction", "unknown"} {
				require.Equal(t, gjson.Get(raw, field).Raw, gjson.Get(next, field).Raw)
			}
			applyCodexAlphaSearchWireProfile(c, account, h, nil)
			require.Equal(t, next, h.Get(openAIWSTurnMetadataHeader), "projection is idempotent")
		}
	}
	for _, raw := range []string{"", `null`, `[]`, `{"installation_id":`} {
		h := make(http.Header)
		h.Set(openAIWSTurnMetadataHeader, raw)
		applyCodexAlphaSearchWireProfile(c, wireProfileTestAccount(true), h, nil)
		require.Equal(t, raw, h.Get(openAIWSTurnMetadataHeader))
	}
}

func TestCodexDeviceWireProfileCompactAccessProgramsGuards(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","access_programs":{"cyber":"standard"}}`)
	c := newConvTestContext(t, body)
	c.Request.URL.Path = "/v1/responses/compact"
	for _, mode := range []string{"off", "device", "session", "full"} {
		for _, enabled := range []bool{false, true} {
			account := wireProfileTestAccount(enabled)
			account.Extra[codexFingerprintModeExtraKey] = mode
			next, err := filterCodexCompactAccessPrograms(c, account, body)
			require.NoError(t, err)
			require.Equal(t, enabled && mode == "device", gjson.GetBytes(next, "access_programs").Exists())
		}
	}
	account := wireProfileTestAccount(true)
	account.Type = AccountTypeAPIKey
	next, err := filterCodexCompactAccessPrograms(c, account, body)
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(next, "access_programs").Exists(), "API-key requests retain their prior compact schema")

	// Native v2 is an ordinary /responses request: this legacy-only projection must not alter it.
	c.Request.URL.Path = "/v1/responses"
	next, err = filterCodexCompactAccessPrograms(c, wireProfileTestAccount(false), body)
	require.NoError(t, err)
	require.Equal(t, body, next)
}

// version 头与 MCP 投影里的 codex_version 必须同源。两者都由客户端提供且互不相同时，
// 只有对齐才不会在一个请求里自报两个版本；入站没有该字段则不补。
func TestCodexAlphaSearchWireProfileAlignsCodexVersion(t *testing.T) {
	c := newConvTestContext(t, nil)
	c.Request.URL.Path = "/v1/alpha/search"
	account := wireProfileTestAccount(true)

	// 出站 body 的 model 已是账号映射后的值（openai_alpha_search.go ForwardAlphaSearch
	// 里的 ReplaceModelInBody），metadata 必须跟着走，否则同一请求自报两个模型。
	outboundBody := []byte(`{"model":"gpt-5.6-sol","query":"x"}`)
	h := make(http.Header)
	h.Set("version", "9.9.9")
	h.Set(openAIWSTurnMetadataHeader, `{ "session_id":"s", "codex_version":"0.0.1", "model":"gpt-5.5" }`)
	applyCodexAlphaSearchWireProfile(c, account, h, outboundBody)
	next := h.Get(openAIWSTurnMetadataHeader)
	// 只改这两个值：键序、空白与其余字段原样。
	require.Equal(t, `{ "session_id":"s", "codex_version":"9.9.9", "model":"gpt-5.6-sol" }`, next)
	applyCodexAlphaSearchWireProfile(c, account, h, outboundBody)
	require.Equal(t, next, h.Get(openAIWSTurnMetadataHeader), "projection is idempotent")

	// 没有模型映射时 body.model 与客户端一致，这一步是恒等变换。
	h = make(http.Header)
	h.Set("version", "9.9.9")
	h.Set(openAIWSTurnMetadataHeader, `{"codex_version":"9.9.9","model":"gpt-5.5"}`)
	applyCodexAlphaSearchWireProfile(c, account, h, []byte(`{"model":"gpt-5.5"}`))
	require.Equal(t, `{"codex_version":"9.9.9","model":"gpt-5.5"}`, h.Get(openAIWSTurnMetadataHeader))

	// 出站值取不到时保留客户端原值，不写空串。
	h = make(http.Header)
	h.Set(openAIWSTurnMetadataHeader, `{"codex_version":"0.0.1","model":"gpt-5.5"}`)
	applyCodexAlphaSearchWireProfile(c, account, h, nil)
	require.Equal(t, `{"codex_version":"0.0.1","model":"gpt-5.5"}`, h.Get(openAIWSTurnMetadataHeader))

	// 入站没有 codex_version：不补，避免给非 codex 客户端造一个它不会发的字段。
	h = make(http.Header)
	h.Set("version", "9.9.9")
	h.Set(openAIWSTurnMetadataHeader, `{"session_id":"s"}`)
	applyCodexAlphaSearchWireProfile(c, account, h, nil)
	require.False(t, gjson.Get(h.Get(openAIWSTurnMetadataHeader), "codex_version").Exists())

	// 未开投影的账号不改动。
	h = make(http.Header)
	h.Set("version", "9.9.9")
	h.Set(openAIWSTurnMetadataHeader, `{"codex_version":"0.0.1"}`)
	applyCodexAlphaSearchWireProfile(c, wireProfileTestAccount(false), h, nil)
	require.Equal(t, `{"codex_version":"0.0.1"}`, h.Get(openAIWSTurnMetadataHeader))
}

// WS turn-state 的位置：真客户端握手传 None（core/src/client.rs:1241），每帧的
// client_metadata["x-codex-turn-state"] 才是它的载体（client.rs:1793）。这里只测帧收口
// helper 自身的语义；三条生产路径的出站帧见 openai_codex_ws_wire_profile_test.go。
func TestCodexDeviceWireProfileWSTurnStateInFrame(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			c := newConvTestContext(t, nil)
			account := wireProfileTestAccount(enabled)

			frame := []byte(`{"client_metadata":{"session_id":"s"},"type":"response.create","model":"m"}`)
			out := applyCodexWSFrameWireProfile(c, account, frame, "turn-state-token")
			if enabled {
				require.Equal(t, "turn-state-token", gjson.GetBytes(out, "client_metadata."+openAICodexTurnStateHeader).String())
				require.Equal(t, []string{"type", "model", "client_metadata"}, topLevelKeys(t, out), "帧字段序对齐 ResponseCreateWsRequest")
				requireCodexWSStreamRequestStart(t, out, "")
			} else {
				require.Equal(t, string(frame), string(out), "未开投影的账号帧字节不变")
			}
			// 帧自带的 turn-state 不覆盖；没有 turn-state 不补；时间戳无条件重盖（真客户端
			// core/src/client.rs:2105-2111 用 HashMap::insert，发送前必盖）；非 response.create
			// 帧不动；client_metadata 不是对象时不塞键。
			own := []byte(`{"type":"response.create","client_metadata":{"x-codex-turn-state":"client-own","x-codex-ws-stream-request-start-ms":"7"}}`)
			ownOut := applyCodexWSFrameWireProfile(c, account, own, "turn-state-token")
			require.Equal(t, "client-own", gjson.GetBytes(ownOut, "client_metadata.x-codex-turn-state").String())
			if enabled {
				requireCodexWSStreamRequestStart(t, ownOut, "")
				require.NotEqual(t, "7", gjson.GetBytes(ownOut, "client_metadata.x-codex-ws-stream-request-start-ms").String(),
					"发送边界重盖：客户端那一跳的戳不代表出站这一跳")
			} else {
				require.Equal(t, "7", gjson.GetBytes(ownOut, "client_metadata.x-codex-ws-stream-request-start-ms").String())
			}
			none := []byte(`{"type":"response.create","model":"m"}`)
			noneOut := applyCodexWSFrameWireProfile(c, account, none, "  ")
			require.False(t, gjson.GetBytes(noneOut, "client_metadata."+openAICodexTurnStateHeader).Exists())
			require.Equal(t, enabled, gjson.GetBytes(noneOut, "client_metadata."+codexWSStreamRequestStartKey).Exists(), "双开盖时间戳，其余不动：%s", noneOut)
			other := []byte(`{"type":"session.update","z":1,"a":2}`)
			require.Equal(t, string(other), string(applyCodexWSFrameWireProfile(c, account, other, "turn-state-token")))
			// 注入值用不转义 HTML 的编码器：真客户端出线是 serde_json::to_string。样本同时含非 ASCII
			// （触发 sjson 退回 encoding/json.Marshal 的条件）与 HTML 字符，纯 ASCII 样本测不出差异。
			esc := applyCodexWSFrameWireProfile(c, account, []byte(`{"type":"response.create","model":"m"}`), "a<b>&c é")
			if enabled {
				require.Contains(t, string(esc), `"a<b>&c é"`, "%s", esc)
				require.NotContains(t, string(esc), `\u003c`)
				require.NotContains(t, string(esc), `\u00e9`)
			}
			scalar := []byte(`{"type":"response.create","client_metadata":"x","model":"m"}`)
			require.Equal(t, "x", gjson.GetBytes(applyCodexWSFrameWireProfile(c, account, scalar, "turn-state-token"), "client_metadata").String(), "标量 client_metadata 不被换成对象")

			// 握手：双开删掉，其他配置原样。
			h := make(http.Header)
			h.Set(openAICodexTurnStateHeader, "turn-state-token")
			applyCodexDeviceWireProfile(c, account, h, true)
			require.Equal(t, !enabled, h.Get(openAICodexTurnStateHeader) != "")
		})
	}
}
