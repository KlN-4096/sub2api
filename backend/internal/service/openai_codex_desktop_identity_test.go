package service

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/stretchr/testify/require"
)

func TestNormalizeCodexClientType(t *testing.T) {
	require.Equal(t, CodexClientTypeCLI, NormalizeCodexClientType(""))
	require.Equal(t, CodexClientTypeCLI, NormalizeCodexClientType("cli"))
	require.Equal(t, CodexClientTypeCLI, NormalizeCodexClientType(" CLI "))
	require.Equal(t, CodexClientTypeCLI, NormalizeCodexClientType("bogus"))
	require.Equal(t, CodexClientTypeDesktop, NormalizeCodexClientType("desktop"))
	require.Equal(t, CodexClientTypeDesktop, NormalizeCodexClientType(" Desktop "))
}

func TestSetCodexClientTypeSnapshot(t *testing.T) {
	t.Cleanup(ResetCodexClientTypeOverride)

	SetCodexClientType("desktop")
	require.Equal(t, CodexClientTypeDesktop, CurrentCodexClientType())
	require.True(t, IsCodexDesktopClient())

	// 非法值回退 CLI，保证误配置安全。
	SetCodexClientType("bogus")
	require.Equal(t, CodexClientTypeCLI, CurrentCodexClientType())
	require.False(t, IsCodexDesktopClient())

	SetCodexClientType("")
	require.Equal(t, CodexClientTypeCLI, CurrentCodexClientType())
}

func TestGetOpenAICodexClientType(t *testing.T) {
	repo := newCodexVersionSyncSettingRepoStub(nil)
	svc := &SettingService{settingRepo: repo}

	// 缺失 key：回退 cli（历史部署无需迁移）。
	require.Equal(t, CodexClientTypeCLI, svc.GetOpenAICodexClientType(context.Background()))

	// 面板写入 desktop：失效缓存后即时生效。
	repo.values[SettingKeyOpenAICodexClientType] = "desktop"
	svc.InvalidateOpenAICodexClientTypeCache()
	require.Equal(t, CodexClientTypeDesktop, svc.GetOpenAICodexClientType(context.Background()))

	// 非法值：归一化回退 cli。
	repo.values[SettingKeyOpenAICodexClientType] = "bogus"
	svc.InvalidateOpenAICodexClientTypeCache()
	require.Equal(t, CodexClientTypeCLI, svc.GetOpenAICodexClientType(context.Background()))
}

func TestBuildCodexDesktopUserAgent(t *testing.T) {
	ua := buildCodexDesktopUserAgent("0.156.0-alpha.1", "26.909.10000")
	// 真实形态：首段 = 内嵌 CLI 版本，尾组 = App 版本（抓包样本对齐）。
	require.True(t, strings.HasPrefix(ua, "Codex Desktop/0.156.0-alpha.1 "), "ua=%q", ua)
	require.True(t, strings.HasSuffix(ua, " (Codex Desktop; 26.909.10000)"), "ua=%q", ua)
	require.True(t, strings.Contains(ua, "(Mac OS 26.5.2; arm64) unknown"), "ua=%q", ua)
	// 首段版本可被 CodexUserAgentVersion 逐字取回（与 version 头同源）。
	require.Equal(t, "0.156.0-alpha.1", openai.CodexUserAgentVersion(ua))
	// 尾组版本可被 CodexDesktopAppVersionFromUA 提取。
	require.Equal(t, "26.909.10000", openai.CodexDesktopAppVersionFromUA(ua))
	// 任一版本非法回退编译期兜底。
	require.Equal(t, codexDesktopUserAgent, buildCodexDesktopUserAgent("not-a-version", "26.909.10000"))
	require.Equal(t, codexDesktopUserAgent, buildCodexDesktopUserAgent("0.156.0-alpha.1", ""))
	require.Equal(t, codexDesktopUserAgent, buildCodexDesktopUserAgent("", ""))
}

func TestSetCodexDesktopUserAgentVersions(t *testing.T) {
	// 候选 UA（面板/账号级显式配置）双版本重建：首段 CLI 版本、尾组 App 版本各自生效，
	// OS 组不被误伤。
	candidate := "Codex Desktop/0.153.0 (Mac OS 26.0.1; arm64) unknown (Codex Desktop; 26.900.1)"
	got := openai.SetCodexDesktopUserAgentVersions(candidate, "0.156.0-alpha.3", "26.911.20000")
	require.Equal(t, "Codex Desktop/0.156.0-alpha.3 (Mac OS 26.0.1; arm64) unknown (Codex Desktop; 26.911.20000)", got)
	// 尾组非官方标识时首段照常重建、尾组保持原样（rewriteCodexUATrailerVersion 保护语义）。
	noTrailer := "Codex Desktop/0.153.0 (Mac OS 26.0.1; arm64) unknown"
	require.Equal(t, "Codex Desktop/0.156.0-alpha.3 (Mac OS 26.0.1; arm64) unknown",
		openai.SetCodexDesktopUserAgentVersions(noTrailer, "0.156.0-alpha.3", "26.911.20000"))
	// 缺任一版本返回空串。
	require.Empty(t, openai.SetCodexDesktopUserAgentVersions(candidate, "", "26.911.20000"))
	require.Empty(t, openai.SetCodexDesktopUserAgentVersions(candidate, "0.156.0-alpha.3", ""))
}

func TestResolveCodexOutboundIdentityDesktopDualVersion(t *testing.T) {
	t.Cleanup(func() {
		ResetCodexClientTypeOverride()
		SetCodexCanonicalUserAgentResolver(nil)
		SetCodexDesktopCLIHeaderVersionResolver(nil)
	})
	SetCodexClientType("desktop")

	// 注入规范 UA（Desktop 双版本形态：首段 CLI 版本 + 尾组 App 版本）。
	SetCodexCanonicalUserAgentResolver(func() string {
		return buildCodexDesktopUserAgent("0.156.0-alpha.3", "26.911.20000")
	})

	identity := resolveCodexOutboundIdentity("")
	// version 头 = UA 首段 CLI 版本（同源），UA 首段与尾组双版本各自保持。
	require.True(t, strings.HasPrefix(identity.userAgent, "Codex Desktop/0.156.0-alpha.3 "), "ua=%q", identity.userAgent)
	require.True(t, strings.HasSuffix(identity.userAgent, "(Codex Desktop; 26.911.20000)"), "ua=%q", identity.userAgent)
	require.Equal(t, "0.156.0-alpha.3", identity.version)
	// originator 与 UA 首段配套（Codex 家族保留大小写）。
	require.Equal(t, "Codex Desktop", identity.originator)

	// 候选 UA（旧版本显式配置）也被重建到生效双版本。
	identity = resolveCodexOutboundIdentity("Codex Desktop/0.150.0 (Mac OS 26.0.1; arm64) unknown (Codex Desktop; 26.880.1)")
	require.True(t, strings.HasPrefix(identity.userAgent, "Codex Desktop/0.156.0-alpha.3 "), "ua=%q", identity.userAgent)
	require.True(t, strings.HasSuffix(identity.userAgent, "(Codex Desktop; 26.911.20000)"), "ua=%q", identity.userAgent)
	require.Equal(t, "0.156.0-alpha.3", identity.version)

	// 未注入解析器时走编译期兜底链：canonical 回落到 codexCLIUserAgent（CLI 常量），
	// codexClientVersionFromUA 提取其版本段，非空且高于门槛时不触发 Desktop 兜底。
	SetCodexCanonicalUserAgentResolver(nil)
	identity = resolveCodexOutboundIdentity("")
	require.Equal(t, codexCLIVersion, identity.version)
}

func TestParseCodexDesktopPanelVersions(t *testing.T) {
	// 空输入：两位置都空，走各自回退链。
	cli, app := parseCodexDesktopPanelVersions("")
	require.Empty(t, cli)
	require.Empty(t, app)
	cli, app = parseCodexDesktopPanelVersions("   ")
	require.Empty(t, cli)
	require.Empty(t, app)

	// 单值（无 |）：仅位置 1（内嵌 CLI 版本）。
	cli, app = parseCodexDesktopPanelVersions("0.154.0-alpha.6.2")
	require.Equal(t, "0.154.0-alpha.6.2", cli)
	require.Empty(t, app)

	// 双位置 `v1|v2`：v1=CLI 版本，v2=App 版本。
	cli, app = parseCodexDesktopPanelVersions("0.154.0-alpha.6.2|26.908.70816")
	require.Equal(t, "0.154.0-alpha.6.2", cli)
	require.Equal(t, "26.908.70816", app)

	// 空位置 1：CLI 版本走兜底，App 版本覆写。
	cli, app = parseCodexDesktopPanelVersions("|26.909.10000")
	require.Empty(t, cli)
	require.Equal(t, "26.909.10000", app)

	// 空位置 2：等价只填位置 1。
	cli, app = parseCodexDesktopPanelVersions("0.154.0-alpha.6|")
	require.Equal(t, "0.154.0-alpha.6", cli)
	require.Empty(t, app)

	// 非法版本按位置独立回退：非法位置为空（回落各自兜底链），合法位置保留。
	cli, app = parseCodexDesktopPanelVersions("bad version|26.908.70816")
	require.Empty(t, cli)
	require.Equal(t, "26.908.70816", app)
	cli, app = parseCodexDesktopPanelVersions("0.154.0-alpha.6.2|bad version")
	require.Equal(t, "0.154.0-alpha.6.2", cli)
	require.Empty(t, app)
}

func TestResolveCodexDesktopPanelVersionsPair(t *testing.T) {
	t.Cleanup(ResetCodexClientTypeOverride)

	// 无面板值：两版本都回编译期兜底。
	repo := newCodexVersionSyncSettingRepoStub(nil)
	svc := &SettingService{settingRepo: repo}
	cli, app := svc.resolveCodexDesktopPanelVersions(context.Background())
	require.Equal(t, codexDesktopCLIVersion, cli)
	require.Equal(t, codexDesktopAppVersion, app)

	// 面板双位置覆写：两位置各自生效。
	repo.values[SettingKeyOpenAICodexClientVersion] = "0.155.0-alpha.1|26.909.10000"
	repo.values[SettingKeyOpenAICodexDesktopClientVersionSynced] = "26.908.70816"
	svc.InvalidateCodexDesktopPanelVersionsCache()
	cli, app = svc.resolveCodexDesktopPanelVersions(context.Background())
	require.Equal(t, "0.155.0-alpha.1", cli)
	require.Equal(t, "26.909.10000", app)

	// 面板留空位置 2：App 版本回落 appcast 同步值（不落兜底常量）。
	repo.values[SettingKeyOpenAICodexClientVersion] = "0.155.0-alpha.1|"
	svc.InvalidateCodexDesktopPanelVersionsCache()
	cli, app = svc.resolveCodexDesktopPanelVersions(context.Background())
	require.Equal(t, "0.155.0-alpha.1", cli)
	require.Equal(t, "26.908.70816", app)

	// 位置 1 面板留空：内嵌 CLI 版本回落 GitHub alpha prerelease 同步值（不落兜底常量）。
	repo.values[SettingKeyOpenAICodexClientVersion] = "|26.909.20000"
	repo.values[SettingKeyOpenAICodexDesktopCLIVersionSynced] = "0.155.0-alpha.6"
	svc.InvalidateCodexDesktopPanelVersionsCache()
	cli, app = svc.resolveCodexDesktopPanelVersions(context.Background())
	require.Equal(t, "0.155.0-alpha.6", cli)
	require.Equal(t, "26.909.20000", app)

	// getter 语义：GetOpenAICodexDesktopClientVersion 取位置 1（CLI 版本，version 头同源），
	// GetOpenAICodexDesktopAppVersion 取位置 2（App 版本），
	// CodexDesktopCLIHeaderVersion 与主版本同源。
	repo.values[SettingKeyOpenAICodexClientVersion] = "0.156.0|26.910.1"
	svc.InvalidateCodexDesktopPanelVersionsCache()
	require.Equal(t, "0.156.0", svc.GetOpenAICodexDesktopClientVersion(context.Background()))
	require.Equal(t, "26.910.1", svc.GetOpenAICodexDesktopAppVersion(context.Background()))
	require.Equal(t, "0.156.0", svc.CodexDesktopCLIHeaderVersion(context.Background()))
}

func TestFirstSparkleShortVersion(t *testing.T) {
	appcast := `<?xml version="1.0" encoding="utf-8"?>
<rss xmlns:sparkle="http://sparkle.andymatuschak.org/xml-namespaces/sparkle">
  <channel>
    <item>
      <title>26.908.70816</title>
      <sparkle:shortVersionString>26.908.70816</sparkle:shortVersionString>
      <enclosure url="https://example.com/ChatGPT.zip"/>
    </item>
    <item>
      <title>26.908.40834</title>
      <sparkle:shortVersionString>26.908.40834</sparkle:shortVersionString>
    </item>
  </channel>
</rss>`
	// 只取第一个 item。
	require.Equal(t, "26.908.70816", firstSparkleShortVersion(appcast))
	require.Empty(t, firstSparkleShortVersion("<rss></rss>"))
	require.Empty(t, firstSparkleShortVersion(""))
}

func TestOpenAICodexDesktopSyncWritesAppcastVersion(t *testing.T) {
	t.Cleanup(ResetCodexClientTypeOverride)
	SetCodexClientType("desktop")

	repo := newCodexVersionSyncSettingRepoStub(nil)
	appcast := &codexDesktopAppcastStub{version: "26.908.70816"}
	svc := NewOpenAICodexVersionSyncService(
		repo, &SettingService{}, &codexVersionSyncGitHubStub{}, openAICodexVersionSyncInterval,
	)
	svc.appcastClient = appcast
	svc.runOnce()

	// github stub 无 releases：CLI 版本源失败 → 成对约束下本次同步整体放弃，
	// 两个 synced 键都不得写入（不允许部分成功）。
	require.Empty(t, repo.syncedWrites())
	_, ok := repo.values[SettingKeyOpenAICodexDesktopClientVersionSynced]
	require.False(t, ok)
	_, ok = repo.values[SettingKeyOpenAICodexDesktopCLIVersionSynced]
	require.False(t, ok)
	// Desktop 分支不得写 CLI 的 synced key。
	_, ok = repo.values[SettingKeyOpenAICodexClientVersionSynced]
	require.False(t, ok)
}

func TestOpenAICodexDesktopSyncWritesCLIVersionFromAlphaPrerelease(t *testing.T) {
	t.Cleanup(ResetCodexClientTypeOverride)
	SetCodexClientType("desktop")

	repo := newCodexVersionSyncSettingRepoStub(nil)
	appcast := &codexDesktopAppcastStub{version: "26.908.70816"}
	github := &codexVersionSyncGitHubStub{releases: []*GitHubRelease{
		{TagName: "rust-v0.155.0-alpha.6", Prerelease: true},
		{TagName: "rust-v0.155.0", Prerelease: false},
		{TagName: "rust-v0.154.0-alpha.5", Prerelease: true},
		{TagName: "rusty-v8-12.0", Prerelease: true},
	}}
	svc := NewOpenAICodexVersionSyncService(repo, &SettingService{}, github, openAICodexVersionSyncInterval)
	svc.appcastClient = appcast
	svc.runOnce()

	// 两源都成功 → 成对原子写入：App 版本来自 appcast，内嵌 CLI 版本取最新 alpha
	// prerelease（stable 与非 rust-v tag 被过滤，只向前推进语义同样适用）。
	require.Equal(t, "26.908.70816", repo.values[SettingKeyOpenAICodexDesktopClientVersionSynced])
	require.Equal(t, "0.155.0-alpha.6", repo.values[SettingKeyOpenAICodexDesktopCLIVersionSynced])
}

func TestOpenAICodexDesktopSyncPairedRollbackOnPartialSource(t *testing.T) {
	t.Cleanup(ResetCodexClientTypeOverride)
	SetCodexClientType("desktop")

	// 预置上一轮成对同步结果。
	repo := newCodexVersionSyncSettingRepoStub(map[string]string{
		SettingKeyOpenAICodexDesktopClientVersionSynced: "26.908.70816",
		SettingKeyOpenAICodexDesktopCLIVersionSynced:    "0.155.0-alpha.6",
	})
	// 本轮 appcast 推进了，但 GitHub alpha 源失败：成对约束要求整体放弃，
	// 两个键都必须保持更新前的值（一个成功一个失败不算成功）。
	appcast := &codexDesktopAppcastStub{version: "26.909.10000"}
	github := &codexVersionSyncGitHubStub{err: fmt.Errorf("rate limited")}
	svc := NewOpenAICodexVersionSyncService(repo, &SettingService{}, github, openAICodexVersionSyncInterval)
	svc.appcastClient = appcast
	svc.runOnce()

	require.Empty(t, repo.syncedWrites())
	require.Equal(t, "26.908.70816", repo.values[SettingKeyOpenAICodexDesktopClientVersionSynced])
	require.Equal(t, "0.155.0-alpha.6", repo.values[SettingKeyOpenAICodexDesktopCLIVersionSynced])
}

func TestOpenAICodexDesktopSyncOnlyMovesForward(t *testing.T) {
	t.Cleanup(ResetCodexClientTypeOverride)
	SetCodexClientType("desktop")

	repo := newCodexVersionSyncSettingRepoStub(map[string]string{
		SettingKeyOpenAICodexDesktopClientVersionSynced: "26.909.10000",
	})
	appcast := &codexDesktopAppcastStub{version: "26.908.70816"}
	svc := NewOpenAICodexVersionSyncService(
		repo, &SettingService{}, &codexVersionSyncGitHubStub{}, openAICodexVersionSyncInterval,
	)
	svc.appcastClient = appcast
	svc.runOnce()

	require.Empty(t, repo.syncedWrites())
}

func TestOpenAICodexDesktopSyncKeepsValueOnFetchFailure(t *testing.T) {
	t.Cleanup(ResetCodexClientTypeOverride)
	SetCodexClientType("desktop")

	repo := newCodexVersionSyncSettingRepoStub(nil)
	appcast := &codexDesktopAppcastStub{version: ""}
	svc := NewOpenAICodexVersionSyncService(
		repo, &SettingService{}, &codexVersionSyncGitHubStub{}, openAICodexVersionSyncInterval,
	)
	svc.appcastClient = appcast
	svc.runOnce()

	require.Empty(t, repo.syncedWrites())
}

type codexDesktopAppcastStub struct {
	version string
}

func (s *codexDesktopAppcastStub) FetchLatestAppVersion(_ context.Context) string {
	return s.version
}
