package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
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
				require.NotEmpty(t, up.lastReq.Header.Get("version"))
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
			} else {
				require.False(t, gjson.GetBytes(up.lastBody, "client_metadata").Exists())
				require.Equal(t, wantInstall, up.lastReq.Header.Get("x-codex-installation-id"))
				require.Equal(t, "responses=experimental", up.lastReq.Header.Get("OpenAI-Beta"))
			}
			require.NotEmpty(t, up.lastReq.Header.Get("version"))
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
				require.NotEmpty(t, up.lastReq.Header.Get("version"))
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
	require.Equal(t, "0.153.4", h.Get("version"))
	require.Equal(t, "fallback-session", h.Get("session_id"))
	require.Equal(t, "fallback-session", h.Get("conversation_id"))
	require.False(t, gjson.Get(h.Get(openAIWSTurnMetadataHeader), "tool_namespaces_info").Exists())
	once := h.Clone()
	applyCodexDeviceWireProfile(c, account, h, false)
	require.Equal(t, once, h, "projection must be idempotent")
	h.Set("OpenAI-Beta", openAIWSBetaV2Value)
	applyCodexDeviceWireProfile(c, account, h, true)
	require.Equal(t, openAIWSBetaV2Value, h.Get("OpenAI-Beta"))

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
		`{"session_id":"session","installation_id":"client","tool_namespaces_info":["tool"]}`)
	svc, _ := wireProfileTestService()
	req, err := svc.buildOpenAIAlphaSearchRequest(context.Background(), c, wireProfileTestAccount(true), body, "offline-token")
	require.NoError(t, err)
	defer func() { _ = req.Body.Close() }()
	sent, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	meta := gjson.Parse(req.Header.Get(openAIWSTurnMetadataHeader))
	require.False(t, meta.Get("tool_namespaces_info").Exists())
	require.NotEmpty(t, meta.Get("installation_id").String())
	require.Equal(t, meta.Get("session_id").String(), gjson.GetBytes(sent, "id").String())
	require.Empty(t, req.Header.Get("session-id"))
	require.Empty(t, req.Header.Get("x-codex-installation-id"))
	require.Empty(t, req.Header.Get("OpenAI-Beta"))
	require.NotEmpty(t, req.Header.Get("version"))
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
