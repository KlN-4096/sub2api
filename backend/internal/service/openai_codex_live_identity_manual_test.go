package service

// 手动验证用例（真实网络，默认跳过）。
//
// 执行方式：
//
//	CODEX_UA_LIVE=1 go test ./internal/service/ -run TestCodexLiveOutboundIdentityManual -v
//
// 该用例真实拉取官方版本号（GitHub openai/codex Releases + ChatGPT Desktop Sparkle
// appcast），并按「当前 UA 组装链路」输出两种客户端类型（CLI / Desktop）的最终出站
// 身份（User-Agent / originator / version），用于人工核对组装结果是否符合预期。
// 网络受限时可设 UPDATE_GITHUB_TOKEN 提升 GitHub API 限额。

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// liveGitHubReleaseClient 真实 GitHub HTTP 客户端（仅本手动用例使用，直连）。
type liveGitHubReleaseClient struct {
	httpClient *http.Client
	token      string
}

func newLiveGitHubReleaseClient() *liveGitHubReleaseClient {
	return &liveGitHubReleaseClient{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		token:      os.Getenv("UPDATE_GITHUB_TOKEN"),
	}
}

func (c *liveGitHubReleaseClient) do(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com"+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("github api %s: %s %s", path, resp.Status, string(body))
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(out)
}

func (c *liveGitHubReleaseClient) FetchLatestRelease(ctx context.Context, repo string) (*GitHubRelease, error) {
	var release GitHubRelease
	if err := c.do(ctx, "/repos/"+repo+"/releases/latest", &release); err != nil {
		return nil, err
	}
	return &release, nil
}

func (c *liveGitHubReleaseClient) FetchRecentReleases(ctx context.Context, repo string, perPage int) ([]*GitHubRelease, error) {
	var releases []*GitHubRelease
	if err := c.do(ctx, fmt.Sprintf("/repos/%s/releases?per_page=%d", repo, perPage), &releases); err != nil {
		return nil, err
	}
	return releases, nil
}

func TestCodexLiveOutboundIdentityManual(t *testing.T) {
	if os.Getenv("CODEX_UA_LIVE") == "" {
		t.Skip("真实网络用例（默认跳过）。执行：CODEX_UA_LIVE=1 go test ./internal/service/ -run TestCodexLiveOutboundIdentityManual -v")
	}
	ctx, cancel := context.WithTimeout(context.Background(), openAICodexVersionSyncTimeout)
	defer cancel()

	github := newLiveGitHubReleaseClient()
	appcast := NewCodexDesktopAppcastClient(openAICodexDesktopAppcastURL)

	// ---- 第一步：真实拉取官方最新版本号 ----
	releases, err := github.FetchRecentReleases(ctx, openAICodexVersionSyncRepo, openAICodexVersionSyncPerPage)
	require.NoError(t, err, "拉取 GitHub releases 失败：若为 403 rate limit，匿名限额按出口 IP 每小时 60 次计，"+
		"请等待整点重置，或设置 UPDATE_GITHUB_TOKEN（GitHub PAT，public 仓库只读即可，认证后限额 5000/h）后重试")
	cliStable := latestCodexStableReleaseVersion(releases)
	desktopCLILatest := latestCodexAlphaPrereleaseVersion(releases)
	desktopApp := appcast.FetchLatestAppVersion(ctx)

	t.Log("======== 官方最新版本号（真实拉取） ========")
	t.Logf("CLI 最新稳定版（GitHub Releases 稳定版）      : %s", cliStable)
	t.Logf("Desktop 内嵌 CLI 最新 alpha（GitHub prerelease）: %s", desktopCLILatest)
	t.Logf("Desktop App 最新版本（Sparkle appcast）        : %s", desktopApp)

	// ---- 第二步：按当前 UA 组装链路输出两种客户端类型的最终出站身份 ----
	// 与 wire 装配一致：canonical UA 解析器返回按类型拼装的规范 UA，
	// resolveCodexOutboundIdentity 由 canonical 推导自洽的三元组。
	//
	// CLI 部分走的是【原有逻辑，本次零改动】：buildCodexCLIUserAgent（既有函数）+
	// codexCLIUserAgentSuffix（既有指纹常量 `(Ubuntu 22.4.0; x86_64) xterm-256color`，
	// 无尾组）+ GitHub 最新稳定版同步值——与切换 Desktop 前的出站行为完全一致。
	reset := func() {
		ResetCodexClientTypeOverride()
		SetCodexCanonicalUserAgentResolver(nil)
	}
	reset()
	t.Cleanup(reset)

	t.Log("======== 出站身份组装结果（按客户端类型） ========")

	// CLI（默认）：既有逻辑零改动——codex-tui 单版本身份，version 头与 UA 版本段同源。
	SetCodexClientType("")
	SetCodexCanonicalUserAgentResolver(func() string {
		return buildCodexCLIUserAgent(cliStable)
	})
	cliIdentity := resolveCodexOutboundIdentity("")
	t.Logf("[CLI]     User-Agent : %s", cliIdentity.userAgent)
	t.Logf("[CLI]     originator : %s", cliIdentity.originator)
	t.Logf("[CLI]     version 头 : %s", cliIdentity.version)

	// Desktop：Codex Desktop 双版本身份（UA 首段 = version 头 = 内嵌 CLI 最新 alpha；
	// 尾部官方标识组 = appcast App 版本），与真实抓包形态一致。
	SetCodexClientType("desktop")
	SetCodexCanonicalUserAgentResolver(func() string {
		return buildCodexDesktopUserAgent(desktopCLILatest, desktopApp)
	})
	desktopIdentity := resolveCodexOutboundIdentity("")
	t.Logf("[Desktop] User-Agent : %s", desktopIdentity.userAgent)
	t.Logf("[Desktop] originator : %s", desktopIdentity.originator)
	t.Logf("[Desktop] version 头 : %s", desktopIdentity.version)

	// ---- 第三步：健全性断言（组装自洽，不回退到内置常量） ----
	require.NotEmpty(t, cliStable, "未能从 GitHub 解析出最新稳定版")
	require.NotEmpty(t, desktopCLILatest, "未能从 GitHub 解析出最新 alpha prerelease")
	require.NotEmpty(t, desktopApp, "未能从 appcast 解析出最新 App 版本")

	require.Equal(t, buildCodexCLIUserAgent(cliStable), cliIdentity.userAgent)
	require.Equal(t, "codex-tui", cliIdentity.originator)
	require.Equal(t, cliStable, cliIdentity.version)

	require.Equal(t, buildCodexDesktopUserAgent(desktopCLILatest, desktopApp), desktopIdentity.userAgent)
	require.Equal(t, codexDesktopClientName, desktopIdentity.originator)
	require.Equal(t, desktopCLILatest, desktopIdentity.version)
}
