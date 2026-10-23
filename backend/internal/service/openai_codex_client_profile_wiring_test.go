//go:build unit

package service

import (
	"context"
	"encoding/json"
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
	require.Equal(t, "Codex Desktop/0.163.0-alpha.11.2 (Mac OS 26.7.1; arm64) unknown (Codex Desktop; 26.1016.62342)", want.userAgent)
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

	newer := `{"desktop":{"macos":{"history":[{"core_version":"0.165.0","desktop_version":"26.1100.1","desktop_build":"21700"}]}},"cli":["0.165.0"],` +
		`"os":{"macos":{"version":"27.0.1","verified_at":"2026-10-24T00:00:00Z"}}}`
	repo.values[SettingKeyOpenAICodexReleases] = newer
	svc.reload()
	require.Equal(t, "21700", codexPublishedReleases().desktopHistory("macos")[0].DesktopBuild, "a newer build written by another instance is adopted")
	require.Equal(t, "0.165.0", codexPublishedReleases().CLI[0])
	require.Equal(t, "27.0.1", codexPublishedReleases().platformDefaults("macos").osVersion, "a synced OS version written by another instance is adopted")

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
	require.Equal(t, CodexOSVersionStatus{OSType: "Ubuntu", Version: "26.4.0", CheckedAt: view.OS[codexrelease.Linux].CheckedAt, Error: "proxy unavailable"},
		view.OS[codexrelease.Linux], "a failed OS check keeps the built-in version and shows why")
	require.NotNil(t, view.OS[codexrelease.Linux].CheckedAt)
	var stored codexReleaseState
	require.NoError(t, json.Unmarshal([]byte(repo.values[SettingKeyOpenAICodexReleases]), &stored))
	require.Equal(t, "proxy unavailable", stored.Desktop[codexrelease.Linux].Error)
	require.Equal(t, "proxy unavailable", stored.OS[codexrelease.Linux].Error, "the failed OS check is persisted too")
	require.NotNil(t, stored.OS[codexrelease.Linux].CheckedAt)
	require.Empty(t, stored.OS[codexrelease.Linux].Version)

	svc.TriggerRefresh()
	require.True(t, svc.ClientProfileView().Refreshing)
	require.Eventually(t, idle, 5*time.Second, 5*time.Millisecond)
}

// 系统版本：预设覆盖 > 同步值 > 写死值；面板的预设占位与系统版本行同源。
func TestCodexProfileFollowsSyncedOSVersions(t *testing.T) {
	releases := testCodexReleases()
	releases.OS = map[codexrelease.Platform]codexOSRelease{codexrelease.MacOS: {Version: "27.0.1"}}
	withCodexProfileRuntime(t, &codexProfileRuntime{releases: releases})

	desktop := CodexClientProfile{Client: "desktop", Platform: "macos", VersionMode: "latest"}
	require.Equal(t, "Codex Desktop/0.164.0-alpha.2 (Mac OS 27.0.1; arm64) unknown (Codex Desktop; 26.1020.10000)", desktop.identity(releases).userAgent)
	desktop.OSVersion = "26.5.2"
	require.Contains(t, desktop.identity(releases).userAgent, "(Mac OS 26.5.2; arm64)", "an explicit override beats the synced version")
	cli := CodexClientProfile{Client: "cli", Platform: "windows", VersionMode: "latest"}
	require.Contains(t, cli.identity(releases).userAgent, "(Windows 10.0.26200; x86_64)", "an unsynced platform keeps the built-in version")

	view := NewCodexReleaseService(&codexProfileSettingRepo{values: map[string]string{}}, nil, nil).ClientProfileView()
	require.Equal(t, CodexOSVersionStatus{OSType: "Mac OS", Version: "27.0.1", Synced: true}, view.OS[codexrelease.MacOS])
	require.Equal(t, CodexOSVersionStatus{OSType: "Ubuntu", Version: "26.4.0"}, view.OS[codexrelease.Linux])
	require.Equal(t, "27.0.1", view.Presets[0].OSVersion, "preset placeholders show the synced version")
}

func TestCodexOSVersionsMergeByVerification(t *testing.T) {
	t1 := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	t2, t3 := t1.Add(24*time.Hour), t1.Add(25*time.Hour)
	ours := codexReleaseState{OS: map[codexrelease.Platform]codexOSRelease{
		codexrelease.MacOS: {Version: "26.7.1", VerifiedAt: &t2, CheckedAt: &t3, Error: "feed down"},
	}}
	older := codexReleaseState{OS: map[codexrelease.Platform]codexOSRelease{codexrelease.MacOS: {Version: "26.6.0", VerifiedAt: &t1}}}
	require.Equal(t, ours.OS, ours.merge(older).OS, "an older verification never rolls the version back, and our latest error stays")

	newer := codexReleaseState{OS: map[codexrelease.Platform]codexOSRelease{
		codexrelease.MacOS:   {Version: "27.0.1", VerifiedAt: &t3},
		codexrelease.Linux:   {Version: "26.4.0", VerifiedAt: &t1},
		codexrelease.Windows: {CheckedAt: &t3, Error: "feed down"},
	}}
	merged := ours.merge(newer).OS
	require.Equal(t, "27.0.1", merged[codexrelease.MacOS].Version)
	require.Equal(t, "26.4.0", merged[codexrelease.Linux].Version)
	require.NotContains(t, merged, codexrelease.Windows, "another instance's failure without a result is not adopted")
	require.Equal(t, "26.7.1", ours.OS[codexrelease.MacOS].Version, "merge never mutates the published state")
}

func TestCodexOSVersionRefresh(t *testing.T) {
	settled := time.Now().AddDate(0, 0, -100).Format(time.DateOnly)
	fresh := time.Now().AddDate(0, 0, -10).Format(time.DateOnly)
	feed := `{"result":{"releases":[{"name":"28","releaseDate":"` + fresh + `","latest":{"name":"28.0"}},` +
		`{"name":"27","releaseDate":"` + settled + `","latest":{"name":"27.2"}}]}}`
	var requested []string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requested = append(requested, r.URL.String())
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(feed)), Header: http.Header{}, Request: r}, nil
	})}
	svc := NewCodexReleaseService(&codexProfileSettingRepo{values: map[string]string{}}, client, nil)
	svc.refreshOSVersion(codexrelease.MacOS)
	entry := svc.state.OS[codexrelease.MacOS]
	require.Equal(t, []string{"https://endoflife.date/api/v1/products/macos/"}, requested)
	require.Equal(t, "27.2.0", entry.Version, "the newest major waits out the settle window; os_info pads to three parts")
	require.NotNil(t, entry.VerifiedAt)
	require.Empty(t, entry.Error)

	failing := NewCodexReleaseService(&codexProfileSettingRepo{values: map[string]string{}}, nil, errors.New("proxy unavailable"))
	failing.state.OS = svc.state.OS
	failing.refreshOSVersion(codexrelease.MacOS)
	kept := failing.state.OS[codexrelease.MacOS]
	require.Equal(t, "27.2.0", kept.Version, "a failed check keeps the last synced version")
	require.Equal(t, entry.VerifiedAt, kept.VerifiedAt)
	require.Equal(t, "proxy unavailable", kept.Error)
	require.Equal(t, "", svc.state.OS[codexrelease.MacOS].Error, "the update replaced the map instead of writing into the shared one")
}

// 检查成功的系统版本要随发布记录落库并发布（重启与其它实例靠它）；官方安装包这里一律 404。
func TestCodexReleaseServicePersistsSyncedOSVersions(t *testing.T) {
	withCodexProfileRuntime(t, nil)
	settled := time.Now().AddDate(0, 0, -100).Format(time.DateOnly)
	feeds := map[string]string{
		"/api/v1/products/macos/":   `{"result":{"releases":[{"name":"27","releaseDate":"` + settled + `","latest":{"name":"27.1"}}]}}`,
		"/api/v1/products/windows/": `{"result":{"releases":[{"name":"11-26h2-w","releaseDate":"` + settled + `","latest":{"name":"10.0.26300"}}]}}`,
		"/api/v1/products/ubuntu/":  `{"result":{"releases":[{"name":"26.04","releaseDate":"` + settled + `","isLts":true,"latest":{"name":"26.04.1"}}]}}`,
	}
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		feed, ok := feeds[r.URL.Path]
		status := http.StatusOK
		if r.URL.Host != "endoflife.date" || !ok {
			status, feed = http.StatusNotFound, ""
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(feed)), Header: http.Header{}, Request: r}, nil
	})}
	repo := &codexProfileSettingRepo{values: map[string]string{}}
	svc := NewCodexReleaseService(repo, client, nil)
	svc.refresh()

	var stored codexReleaseState
	require.NoError(t, json.Unmarshal([]byte(repo.values[SettingKeyOpenAICodexReleases]), &stored))
	want := map[codexrelease.Platform]string{codexrelease.MacOS: "27.1.0", codexrelease.Windows: "10.0.26300", codexrelease.Linux: "26.4.0"}
	for platform, version := range want {
		require.Equal(t, version, stored.OS[platform].Version, platform)
		require.NotNil(t, stored.OS[platform].VerifiedAt, platform)
		require.Empty(t, stored.OS[platform].Error, platform)
		require.NotEmpty(t, stored.Desktop[platform].Error, "the installer checks failed against the stub: %s", platform)
		require.Equal(t, version, codexPublishedReleases().platformDefaults(string(platform)).osVersion, "published: %s", platform)
		status := svc.ClientProfileView().OS[platform]
		require.True(t, status.Synced, platform)
		require.Equal(t, version, status.Version, platform)
		require.Equal(t, stored.OS[platform].VerifiedAt.Unix(), status.VerifiedAt.Unix(), "the panel shows when it was last synced: %s", platform)
	}
}
