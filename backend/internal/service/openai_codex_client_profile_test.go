//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/codexrelease"
	"github.com/stretchr/testify/require"
)

func withCodexProfileRuntime(t *testing.T, rt *codexProfileRuntime) {
	t.Helper()
	previous := codexProfileRuntimeState.Load()
	codexProfileRuntimeState.Store(rt)
	t.Cleanup(func() { codexProfileRuntimeState.Store(previous) })
}

func withCodexCanonicalVersion(t *testing.T, version string) {
	t.Helper()
	SetCodexCanonicalUserAgentResolver(func() string { return buildCodexCLIUserAgent(version) })
	t.Cleanup(func() { SetCodexCanonicalUserAgentResolver(nil) })
}

func testCodexReleases() *codexReleaseState {
	return &codexReleaseState{
		Desktop: map[codexrelease.Platform]codexPlatformReleases{
			codexrelease.MacOS: {History: []codexrelease.Release{
				{CoreVersion: "0.164.0-alpha.2", DesktopVersion: "26.1020.10000", DesktopBuild: "21600"},
				{CoreVersion: "0.163.0-alpha.11.2", DesktopVersion: "26.1016.62342", DesktopBuild: "21477"},
			}},
		},
		CLI: []string{"0.163.0", "0.162.0"},
	}
}

func TestCodexClientProfileIdentityShapes(t *testing.T) {
	withCodexCanonicalVersion(t, "0.164.0")
	releases := testCodexReleases()
	cases := []struct {
		name    string
		profile CodexClientProfile
		want    codexOutboundIdentity
	}{
		{
			name:    "desktop_latest",
			profile: CodexClientProfile{Client: "desktop", Platform: "macos", VersionMode: "latest"},
			want: codexOutboundIdentity{
				userAgent:  "Codex Desktop/0.164.0-alpha.2 (Mac OS 15.7.1; arm64) unknown (Codex Desktop; 26.1020.10000)",
				originator: "Codex Desktop", version: "0.164.0-alpha.2",
			},
		},
		{
			name:    "desktop_lag_beyond_history_takes_oldest",
			profile: CodexClientProfile{Client: "desktop", Platform: "macos", VersionMode: "latest", VersionLag: 5},
			want: codexOutboundIdentity{
				userAgent:  "Codex Desktop/0.163.0-alpha.11.2 (Mac OS 15.7.1; arm64) unknown (Codex Desktop; 26.1016.62342)",
				originator: "Codex Desktop", version: "0.163.0-alpha.11.2",
			},
		},
		{
			name:    "desktop_without_verified_history_uses_seed",
			profile: CodexClientProfile{Client: "desktop", Platform: "linux", VersionMode: "latest"},
			want: codexOutboundIdentity{
				userAgent:  "Codex Desktop/0.163.0-alpha.11.1 (Linux 6.8.0; x86_64) unknown (Codex Desktop; 26.1016.61520)",
				originator: "Codex Desktop", version: "0.163.0-alpha.11.1",
			},
		},
		{
			name:    "cli_core_follows_live_cli_version",
			profile: CodexClientProfile{Client: "cli", Platform: "windows", VersionMode: "latest"},
			want: codexOutboundIdentity{
				userAgent: "codex_cli_rs/0.164.0 (Windows 10.0.26100; x86_64) unknown", originator: "codex_cli_rs", version: "0.164.0",
			},
		},
		{
			name:    "cli_lag_walks_observed_history",
			profile: CodexClientProfile{Client: "cli", Platform: "windows", VersionMode: "latest", VersionLag: 2},
			want: codexOutboundIdentity{
				userAgent: "codex_cli_rs/0.162.0 (Windows 10.0.26100; x86_64) unknown", originator: "codex_cli_rs", version: "0.162.0",
			},
		},
		{
			name: "cli_exec_fixed_with_overrides",
			profile: CodexClientProfile{Client: "cli", Platform: "linux", CLIEntry: "exec", VersionMode: "fixed", CodexVersion: "0.160.0",
				OSType: "Ubuntu", OSVersion: "24.4.0", Terminal: "xterm-256color"},
			want: codexOutboundIdentity{
				userAgent:  "codex_exec/0.160.0 (Ubuntu 24.4.0; x86_64) xterm-256color (codex_exec; 0.160.0)",
				originator: "codex_exec", version: "0.160.0",
			},
		},
		{
			name:    "desktop_fixed",
			profile: CodexClientProfile{Client: "desktop", Platform: "windows", VersionMode: "fixed", CodexVersion: "0.163.0-alpha.11", DesktopVersion: "26.1016.51333", OSVersion: "10.0.26200"},
			want: codexOutboundIdentity{
				userAgent:  "Codex Desktop/0.163.0-alpha.11 (Windows 10.0.26200; x86_64) unknown (Codex Desktop; 26.1016.51333)",
				originator: "Codex Desktop", version: "0.163.0-alpha.11",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.profile.validate())
			require.Equal(t, tc.want, tc.profile.identity(releases))
		})
	}
}

func TestParseCodexClientProfileRejectsInvalidSelections(t *testing.T) {
	valid := `{"client":"desktop","platform":"macos","versionMode":"latest"}`
	_, err := ParseCodexClientProfile(json.RawMessage(valid))
	require.NoError(t, err)
	for _, raw := range []string{
		`{"client":"desktop","platform":"macos","versionMode":"latest","originator":"x"}`,
		`{"client":"desktop","platform":"macos","versionMode":"latest","cliEntry":"exec"}`,
		`{"client":"cli","platform":"macos","versionMode":"latest","cliEntry":"tui"}`,
		`{"client":"cli","platform":"android","versionMode":"latest"}`,
		`{"client":"tui","platform":"macos","versionMode":"latest"}`,
		`{"client":"cli","platform":"macos","versionMode":"latest","codexVersion":"0.160.0"}`,
		`{"client":"cli","platform":"macos","versionMode":"latest","versionLag":11}`,
		`{"client":"cli","platform":"macos","versionMode":"fixed"}`,
		`{"client":"cli","platform":"macos","versionMode":"fixed","codexVersion":"0.140.0"}`,
		`{"client":"cli","platform":"macos","versionMode":"fixed","codexVersion":"0.160.0","versionLag":1}`,
		`{"client":"cli","platform":"macos","versionMode":"fixed","codexVersion":"0.160.0","desktopVersion":"26.1.1"}`,
		`{"client":"desktop","platform":"macos","versionMode":"fixed","codexVersion":"0.160.0"}`,
		`{"client":"desktop","platform":"macos","versionMode":"fixed","codexVersion":"0.160.0","desktopVersion":"26.x"}`,
		`{"client":"desktop","platform":"macos","versionMode":"latest","osType":"Mac OS (x)"}`,
		`{"client":"desktop","platform":"macos","versionMode":"latest","terminal":" unknown"}`,
		`{"client":"desktop","platform":"macos","versionMode":"sometimes"}`,
	} {
		_, err := ParseCodexClientProfile(json.RawMessage(raw))
		require.Error(t, err, raw)
	}
}

func TestCodexGlobalIdentityFollowsRuntime(t *testing.T) {
	withCodexCanonicalVersion(t, "0.164.0")
	cliCanonical := resolveCodexOutboundIdentity("")
	require.Equal(t, "codex_cli_rs", cliCanonical.originator, "without a published runtime the canonical CLI chain is unchanged")

	releases := testCodexReleases()
	withCodexProfileRuntime(t, &codexProfileRuntime{releases: releases})
	require.Equal(t, codexBuiltinClientProfile.identity(releases), resolveCodexOutboundIdentity(""),
		"nothing configured: built-in Desktop macOS latest")
	userAgent, originator := CodexCanonicalAuthIdentity()
	require.Equal(t, "Codex Desktop", originator, "the auth plane follows the same global identity")
	require.Contains(t, userAgent, "(Codex Desktop; 26.1020.10000)")

	codexProfileRuntimeState.Store(&codexProfileRuntime{legacyUA: true, releases: releases})
	require.Equal(t, cliCanonical, resolveCodexOutboundIdentity(""), "a global custom User-Agent wins over the built-in default")

	global := &CodexClientProfile{Client: "cli", Platform: "linux", CLIEntry: "exec", VersionMode: "latest"}
	codexProfileRuntimeState.Store(&codexProfileRuntime{global: global, legacyUA: true, releases: releases})
	require.Equal(t, global.identity(releases), resolveCodexOutboundIdentity(""), "an explicit global profile wins over the custom User-Agent")

	const accountUA = "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466 (codex-tui; 0.153.4)"
	legacy := resolveCodexOutboundIdentity(accountUA)
	require.Equal(t, "0.164.0", legacy.version, "a legacy custom UA keeps the CLI version chain, not the global profile's")
}

func TestCodexAccountProfileDrivesEnforcedHeaders(t *testing.T) {
	withCodexCanonicalVersion(t, "0.164.0")
	withCodexProfileRuntime(t, &codexProfileRuntime{releases: testCodexReleases()})
	account := wireProfileTestAccount(true)
	account.Extra[codexAccountUserAgentExtraKey] = "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466 (codex-tui; 0.153.4)"
	account.Extra[codexClientProfileExtraKey] = map[string]any{"client": "desktop", "platform": "macos", "versionMode": "latest", "versionLag": 1}

	h := http.Header{}
	h.Set("user-agent", "curl/8")
	h.Set("originator", "whatever")
	enforceCodexClientIdentityHeaders(h, account, account)
	require.Equal(t, "Codex Desktop/0.163.0-alpha.11.2 (Mac OS 15.7.1; arm64) unknown (Codex Desktop; 26.1016.62342)", h.Get("user-agent"),
		"the profile wins over the account's custom UA and the Desktop trailer keeps the app version")
	require.Equal(t, "Codex Desktop", h.Get("originator"))
	require.Equal(t, "0.163.0-alpha.11.2", h.Get("version"))

	forced := &OpenAIGatewayService{cfg: &config.Config{}}
	forced.cfg.Gateway.ForceCodexCLI = true
	h = http.Header{}
	h.Set("originator", "x")
	forced.enforceCodexClientIdentity(nil, h, account)
	require.Equal(t, resolveCodexOutboundIdentity("").userAgent, h.Get("user-agent"), "ForceCodexCLI pins the gateway identity")

	svc := &OpenAIQuotaService{accountRepo: &quotaUserAgentTestRepo{account: account}}
	quota, _, err := svc.buildCodexQuotaHeaders(quotaUserAgentCallForTest(account, false))
	require.NoError(t, err)
	require.Equal(t, codexClientIdentityFor(account, account).userAgent, quota["user-agent"], "the quota plane presents the same client")

	account.Extra[codexClientProfileExtraKey] = map[string]any{"client": "desktop", "platform": "macos", "versionMode": "nope"}
	require.Equal(t, resolveCodexOutboundIdentity(account.GetOpenAIUserAgent()), codexClientIdentityFor(account, account),
		"an invalid stored profile falls back to the custom UA")
}

func TestCodexClientProfileExtraValidation(t *testing.T) {
	oauth := wireProfileTestAccount(true)
	require.NoError(t, validateOpenAIQualityGuardExtra(oauth, map[string]any{
		codexClientProfileExtraKey: map[string]any{"client": "cli", "platform": "linux", "versionMode": "latest"},
	}))
	require.NoError(t, validateOpenAIQualityGuardExtra(oauth, map[string]any{codexClientProfileExtraKey: nil}), "null clears")
	require.Error(t, validateOpenAIQualityGuardExtra(oauth, map[string]any{
		codexClientProfileExtraKey: map[string]any{"client": "cli", "platform": "linux"},
	}))
	apiKey := wireProfileTestAccount(true)
	apiKey.Type = AccountTypeAPIKey
	require.Error(t, validateOpenAIQualityGuardExtra(apiKey, map[string]any{
		codexClientProfileExtraKey: map[string]any{"client": "cli", "platform": "linux", "versionMode": "latest"},
	}))
}

func TestCodexDesktopReleaseHistoryAcceptsOnlyNewerBuilds(t *testing.T) {
	release := func(build, core, app string) codexrelease.Release {
		return codexrelease.Release{CoreVersion: core, DesktopVersion: app, DesktopBuild: build, Artifact: codexrelease.Identity{ETag: build}}
	}
	var entry codexPlatformReleases
	entry, err := entry.accept(release("100", "0.160.0-alpha.1", "26.1.1"))
	require.NoError(t, err)

	refreshed := release("100", "0.160.0-alpha.1", "26.1.1")
	refreshed.Artifact.ETag = "moved"
	entry, err = entry.accept(refreshed)
	require.NoError(t, err)
	require.Len(t, entry.History, 1, "re-verifying the same build only refreshes its artifact identity")
	require.Equal(t, "moved", entry.History[0].Artifact.ETag)

	_, err = entry.accept(release("100", "0.160.0-alpha.2", "26.1.1"))
	require.Error(t, err, "same build, different versions")
	_, err = entry.accept(release("99", "0.159.0", "26.1.0"))
	require.Error(t, err, "older build")

	for build := 101; build <= 120; build++ {
		entry, err = entry.accept(release(strconv.Itoa(build), "0.161.0", "26.2.1"))
		require.NoError(t, err)
	}
	require.Len(t, entry.History, codexReleaseHistoryLimit)
	require.Equal(t, "120", entry.History[0].DesktopBuild)
}

type codexProfileSettingRepo struct {
	SettingRepository
	values map[string]string
}

func (r *codexProfileSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	if value, ok := r.values[key]; ok {
		return value, nil
	}
	return "", ErrSettingNotFound
}

func (r *codexProfileSettingRepo) Set(_ context.Context, key, value string) error {
	r.values[key] = value
	return nil
}

func TestCodexReleaseServiceSavesAndPublishesGlobalProfile(t *testing.T) {
	withCodexProfileRuntime(t, nil)
	repo := &codexProfileSettingRepo{values: map[string]string{}}
	svc := NewCodexReleaseService(repo, nil, nil)
	svc.reload()
	require.Equal(t, "builtin", svc.ClientProfileView().Effective.Source)
	require.Equal(t, "Codex Desktop", resolveCodexOutboundIdentity("").originator)

	require.Error(t, svc.SaveGlobalClientProfile(context.Background(), json.RawMessage(`{"client":"cli"}`)))
	require.NoError(t, svc.SaveGlobalClientProfile(context.Background(), json.RawMessage(`{"client":"cli","platform":"linux","versionMode":"latest"}`)))
	view := svc.ClientProfileView()
	require.Equal(t, "profile", view.Effective.Source)
	require.Equal(t, "codex_cli_rs", view.Effective.Originator)
	require.Len(t, view.Presets, 6)
	require.False(t, view.Desktop[codexrelease.MacOS].Verified, "no check has run: the seed is shown")

	require.NoError(t, svc.SaveGlobalClientProfile(context.Background(), json.RawMessage(`null`)))
	require.Empty(t, repo.values[SettingKeyOpenAICodexClientProfile])
	require.Equal(t, "Codex Desktop", resolveCodexOutboundIdentity("").originator)
}
