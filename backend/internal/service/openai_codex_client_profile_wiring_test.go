//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/codexrelease"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 账号预设必须落到每条出站路径上：任何一条改回按 UA 收口（enforceCodexIdentityHeadersWithUA +
// GetOpenAIUserAgent）都要在这里红。账号同时配了自定义 UA，顺带钉住预设优先。

var profileWiringSelection = CodexClientProfile{Client: "desktop", Platform: "macos", VersionMode: "latest", VersionLag: 1}

func profileWiringAccount() *Account {
	account := wireProfileTestAccount(false)
	account.Extra[codexAccountUserAgentExtraKey] = "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466 (codex-tui; 0.153.4)"
	account.Extra[codexClientProfileExtraKey] = map[string]any{"client": "desktop", "platform": "macos", "versionMode": "latest", "versionLag": 1}
	return account
}

func profileWiringContext(path string) *gin.Context {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, path, http.NoBody)
	c.Request.Header.Set("User-Agent", "codex_cli_rs/0.150.0 (Ubuntu 24.04; x86_64) xterm")
	c.Request.Header.Set("originator", "codex_cli_rs")
	return c
}

func requireProfileWired(t *testing.T, label string, h http.Header) {
	t.Helper()
	want := profileWiringSelection.identity(codexPublishedReleases())
	require.Equal(t, "Codex Desktop/0.163.0-alpha.11.2 (Mac OS 15.7.1; arm64) unknown (Codex Desktop; 26.1016.62342)", want.userAgent)
	require.Equal(t, want.userAgent, h.Get("User-Agent"), label)
	require.Equal(t, want.version, h.Get("Version"), label)
	require.Equal(t, want.originator, h.Get("Originator"), label)
}

func TestCodexAccountProfileReachesEveryOutboundPath(t *testing.T) {
	withCodexCanonicalVersion(t, "0.164.0")
	withCodexProfileRuntime(t, &codexProfileRuntime{releases: testCodexReleases()})
	ctx := context.Background()
	body := []byte(`{"model":"gpt-5.4","input":[],"stream":true}`)

	t.Run("http", func(t *testing.T) {
		req, err := (&OpenAIGatewayService{}).buildUpstreamRequest(ctx, profileWiringContext("/v1/responses"), profileWiringAccount(), body, "tok", true, "", true)
		require.NoError(t, err)
		requireProfileWired(t, "http", req.Header)
	})
	t.Run("passthrough", func(t *testing.T) {
		req, err := (&OpenAIGatewayService{}).buildUpstreamRequestOpenAIPassthrough(ctx, profileWiringContext("/v1/responses"), profileWiringAccount(), body, "tok")
		require.NoError(t, err)
		requireProfileWired(t, "passthrough", req.Header)
	})
	t.Run("ws", func(t *testing.T) {
		headers, _, err := (&OpenAIGatewayService{}).buildOpenAIWSHeaders(ctx, profileWiringContext("/v1/responses"), profileWiringAccount(), "tok",
			OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2}, true, "", "", "", "", "")
		require.NoError(t, err)
		requireProfileWired(t, "ws", headers)
	})
	t.Run("alpha_search", func(t *testing.T) {
		req, err := (&OpenAIGatewayService{}).buildOpenAIAlphaSearchRequest(ctx, profileWiringContext("/v1/alpha/search"), profileWiringAccount(), []byte(`{"query":"x"}`), "tok")
		require.NoError(t, err)
		requireProfileWired(t, "alpha_search", req.Header)
	})
	t.Run("messages_bridge", func(t *testing.T) {
		up := &httpUpstreamRecorder{responses: codexBridgeUpstreamResponses(1)}
		runCodexBridgeTurn(t, codexBridgeTestService(up), profileWiringAccount(), codexBridgeTestBody("profile-wiring"))
		require.Len(t, up.requests, 1)
		h := up.requests[0].Header
		want := profileWiringSelection.identity(codexPublishedReleases())
		require.Equal(t, want.userAgent, h.Get("User-Agent"))
		require.Equal(t, want.version, h.Get("Version"))
	})
	t.Run("live", func(t *testing.T) {
		h := http.Header{}
		(&OpenAIGatewayService{}).applyLiveUpstreamIdentityHeaders(h, profileWiringAccount())
		requireProfileWired(t, "live", h)
	})
	t.Run("models", func(t *testing.T) {
		request, _, err := (&OpenAIGatewayService{cfg: &config.Config{}}).buildCodexModelsManifestRequest(ctx, profileWiringAccount(), "")
		require.NoError(t, err)
		requireProfileWired(t, "models", request.headers)
		requireModelsClientVersion(t, request.url, "0.163.0")
	})
	t.Run("model_sync", func(t *testing.T) {
		req, err := (&AccountTestService{}).buildOpenAIOAuthUpstreamModelsRequest(ctx, profileWiringAccount())
		require.NoError(t, err)
		requireProfileWired(t, "model_sync", req.Header)
		requireModelsClientVersion(t, req.URL.String(), "0.163.0")
	})
	t.Run("account_test", func(t *testing.T) {
		resp := newJSONResponse(http.StatusOK, "")
		resp.Body = io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\"}\n\n"))
		upstream := &queuedHTTPUpstream{responses: []*http.Response{resp}}
		account := profileWiringAccount()
		account.Credentials = map[string]any{"access_token": "test-token"}
		c, _ := newTestContext()
		require.NoError(t, (&AccountTestService{httpUpstream: upstream}).testOpenAIAccountConnection(c, account, "gpt-5.4", "", ""))
		require.Len(t, upstream.requests, 1)
		requireProfileWired(t, "account_test", upstream.requests[0].Header)
	})
	t.Run("quota", func(t *testing.T) {
		account := profileWiringAccount()
		svc := &OpenAIQuotaService{accountRepo: &quotaUserAgentTestRepo{account: account}}
		headers, _, err := svc.buildCodexQuotaHeaders(quotaUserAgentCallForTest(account, false))
		require.NoError(t, err)
		require.Equal(t, profileWiringSelection.identity(codexPublishedReleases()).userAgent, headers["user-agent"])
	})
}

func requireModelsClientVersion(t *testing.T, rawURL, want string) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	require.Equal(t, want, parsed.Query().Get("client_version"), "client_version is the whole version, like codex-rs client_version_to_whole")
}

// 影子行与母账号是同一个上游客户端：推理面按本次尝试的凭据行（母账号）取预设，影子行自己不能设。
func TestCodexShadowRowUsesParentProfile(t *testing.T) {
	withCodexCanonicalVersion(t, "0.164.0")
	withCodexProfileRuntime(t, &codexProfileRuntime{releases: testCodexReleases()})
	parent := profileWiringAccount()
	shadow := wireProfileTestAccount(false)
	shadow.ID = 9102
	shadow.ParentAccountID = &parent.ID

	c := profileWiringContext("/v1/responses")
	c.Set(codexAccountIdentitySourceContextKey, parent)
	svc := &OpenAIGatewayService{accountRepo: &quotaUserAgentTestRepo{account: parent}}
	req, err := svc.buildUpstreamRequest(context.Background(), c, shadow, []byte(`{"model":"gpt-5.4","input":[]}`), "tok", true, "", true)
	require.NoError(t, err)
	requireProfileWired(t, "shadow inference", req.Header)

	err = validateOpenAIQualityGuardExtra(shadow, map[string]any{
		codexClientProfileExtraKey: map[string]any{"client": "cli", "platform": "linux", "versionMode": "latest"},
	})
	require.ErrorContains(t, err, "parent account")
}

// 关闭强制统一时预设与自定义 UA 同等对待：写进 User-Agent 再配对，而不是被忽略。
func TestCodexProfileWithIdentityEnforcementDisabled(t *testing.T) {
	withCodexCanonicalVersion(t, "0.164.0")
	withCodexProfileRuntime(t, &codexProfileRuntime{releases: testCodexReleases()})
	SetCodexIdentityEnforcementEnabled(false)
	t.Cleanup(func() { SetCodexIdentityEnforcementEnabled(true) })

	account := profileWiringAccount()
	h := http.Header{}
	h.Set("User-Agent", "codex_cli_rs/0.150.0 (Ubuntu 24.04; x86_64) xterm")
	h.Set("originator", "codex_cli_rs")
	enforceCodexClientIdentityHeaders(h, account, account)
	want := profileWiringSelection.identity(codexPublishedReleases())
	require.Equal(t, want.userAgent, h.Get("User-Agent"))
	require.Equal(t, want.originator, h.Get("originator"))
}

func TestCodexProfileTouchesQualityGuardValidation(t *testing.T) {
	require.True(t, touchesOpenAIQualityGuardConfig(map[string]any{codexClientProfileExtraKey: nil}),
		"partial and bulk updates must validate the profile too")
}

// 非 ChatGPT 上游不跟预设：API key 的 /models 与模型同步、Command Code 仍是预设之前的身份。
func TestCodexThirdPartySurfacesIgnoreProfiles(t *testing.T) {
	withCodexCanonicalVersion(t, "0.164.0")
	global := &CodexClientProfile{Client: "desktop", Platform: "macos", VersionMode: "latest"}
	withCodexProfileRuntime(t, &codexProfileRuntime{global: global, releases: testCodexReleases()})
	require.Equal(t, "Codex Desktop", resolveCodexOutboundIdentity("").originator, "precondition: the global identity is Desktop")
	legacy := resolveCodexUserAgentIdentity("")
	require.Equal(t, "codex_cli_rs", legacy.originator)

	h := http.Header{}
	applyOpenCodeUpstreamUserAgent(nil, "https://api.commandcode.ai/v1/responses", h)
	require.Equal(t, legacy.userAgent, h.Get("User-Agent"), "Command Code keeps the pre-profile identity")

	apiKey := wireProfileTestAccount(false)
	apiKey.Type = AccountTypeAPIKey
	apiKey.Credentials = map[string]any{"api_key": "sk-offline", "base_url": "https://upstream.example/v1"}
	request, _, err := (&OpenAIGatewayService{cfg: &config.Config{}}).buildCodexModelsManifestRequest(context.Background(), apiKey, "")
	require.NoError(t, err)
	require.Equal(t, legacy.userAgent, request.headers.Get("User-Agent"))
}

func TestCodexReleaseServiceLegacyUserAgentFollowsRawSetting(t *testing.T) {
	withCodexProfileRuntime(t, nil)
	repo := &codexProfileSettingRepo{values: map[string]string{SettingKeyOpenAICodexUserAgent: ""}}
	svc := NewCodexReleaseService(repo, nil, nil)
	svc.reload()
	require.Equal(t, "builtin", svc.ClientProfileView().Effective.Source, "an empty panel value is not a custom User-Agent")

	repo.values[SettingKeyOpenAICodexUserAgent] = "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466 (codex-tui; 0.153.4)"
	svc.reload()
	require.Equal(t, "legacy_user_agent", svc.ClientProfileView().Effective.Source)
	require.Equal(t, "codex_cli_rs", resolveCodexOutboundIdentity("").originator)
}

func TestCodexReleaseServiceMergesOtherInstances(t *testing.T) {
	withCodexProfileRuntime(t, nil)
	repo := &codexProfileSettingRepo{values: map[string]string{}}
	svc := NewCodexReleaseService(repo, nil, nil)
	svc.state = *testCodexReleases()

	newer := `{"desktop":{"macos":{"history":[{"core_version":"0.165.0","desktop_version":"26.1100.1","desktop_build":"21700"}]}},"cli":["0.165.0"]}`
	repo.values[SettingKeyOpenAICodexReleases] = newer
	svc.reload()
	require.Equal(t, "21700", codexPublishedReleases().desktopHistory("macos")[0].DesktopBuild, "a newer build written by another instance is adopted")
	require.Equal(t, "0.165.0", codexPublishedReleases().CLI[0])

	older := `{"desktop":{"macos":{"history":[{"core_version":"0.160.0","desktop_version":"26.900.1","desktop_build":"20000"}]}}}`
	repo.values[SettingKeyOpenAICodexReleases] = older
	svc.reload()
	require.Equal(t, "21700", codexPublishedReleases().desktopHistory("macos")[0].DesktopBuild, "a stale write never rolls the version back")
}

func TestCodexCoreVersionGate(t *testing.T) {
	require.True(t, codexCoreVersionOK(codexUpstreamMinVersion))
	require.True(t, codexCoreVersionOK("0.163.0-alpha.11.2"))
	require.False(t, codexCoreVersionOK(codexUpstreamMinVersion+"-alpha.1"), "a pre-release of the floor is below it")
	require.False(t, codexCoreVersionOK("0.143.9"))
	require.False(t, codexCoreVersionOK(""))

	_, err := codexPlatformReleases{}.accept(codexrelease.Release{CoreVersion: "0.140.0", DesktopVersion: "26.1.1", DesktopBuild: "21000"})
	require.Error(t, err, "an artifact below the upstream floor is not accepted")
}

// 检查失败（如代理不可用）记进状态并写库，running 在检查结束后清；手动触发时返回的视图已是 refreshing。
func TestCodexReleaseServiceRecordsFailedChecks(t *testing.T) {
	withCodexProfileRuntime(t, nil)
	previous := activeCodexReleaseService.Load()
	t.Cleanup(func() { activeCodexReleaseService.Store(previous) })
	repo := &codexProfileSettingRepo{values: map[string]string{}}
	svc := NewCodexReleaseService(repo, nil, errors.New("proxy unavailable"))
	svc.Start()
	t.Cleanup(svc.Stop)
	idle := func() bool { return !svc.running.Load() }
	require.Eventually(t, idle, 5*time.Second, 5*time.Millisecond)

	view := svc.ClientProfileView()
	require.Equal(t, "proxy unavailable", view.Desktop[codexrelease.Linux].Error)
	require.False(t, view.Desktop[codexrelease.Linux].Verified)
	require.Contains(t, repo.values[SettingKeyOpenAICodexReleases], "proxy unavailable")

	svc.TriggerRefresh()
	require.True(t, svc.ClientProfileView().Refreshing)
	require.Eventually(t, idle, 5*time.Second, 5*time.Millisecond)
}
