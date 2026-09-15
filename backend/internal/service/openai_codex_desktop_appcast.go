package service

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"regexp"
)

// CodexDesktopAppcastClient 拉取 ChatGPT Desktop 的 Sparkle appcast 并解析最新 App 版本号。
// 与 GitHubReleaseClient 解耦：Desktop 分支的事实接口是 appcast.xml（公开无鉴权），
// 不是 GitHub Releases；独立接口便于测试注入。
type CodexDesktopAppcastClient interface {
	FetchLatestAppVersion(ctx context.Context) string
}

// codexDesktopAppcastClient 默认实现：标准库 http.Client + 正则解析。
// appcast 结构稳定（Sparkle 标准），为只取第一个 item 的 shortVersionString，
// 用受限读取 + 正则足够，无需引入完整 XML 解析的依赖面。
type codexDesktopAppcastClient struct {
	url     string
	maxSize int64
	client  *http.Client
}

// sparkShortVersionPattern 匹配第一个 <sparkle:shortVersionString>26.908.70816</...>。
// 只信第一个 item：feed 按发布时间倒序，第一个即最新版。
var sparkShortVersionPattern = regexp.MustCompile(
	`<sparkle:shortVersionString>\s*([0-9]+(\.[0-9]+)+)\s*</sparkle:shortVersionString>`)

func NewCodexDesktopAppcastClient(url string) CodexDesktopAppcastClient {
	return &codexDesktopAppcastClient{
		url:     url,
		maxSize: openAICodexDesktopAppcastMaxBytes,
		client: &http.Client{
			Timeout: openAICodexVersionSyncTimeout,
		},
	}
}

// FetchLatestAppVersion 返回 appcast 第一条 item 的 App 版本号（如 26.908.70816）；
// 抓取或解析失败时返回空串并落日志，由调用方保持既有值（不清空、不降级）。
func (c *codexDesktopAppcastClient) FetchLatestAppVersion(ctx context.Context) string {
	if c == nil || c.url == "" {
		return ""
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		slog.Warn("openai_codex_desktop_appcast_request_build_failed", "error", err)
		return ""
	}
	resp, err := c.client.Do(req)
	if err != nil {
		slog.Warn("openai_codex_desktop_appcast_fetch_failed", "error", err)
		return ""
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		slog.Warn("openai_codex_desktop_appcast_unexpected_status", "status", resp.StatusCode)
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxSize))
	if err != nil {
		slog.Warn("openai_codex_desktop_appcast_read_failed", "error", err)
		return ""
	}
	version := firstSparkleShortVersion(string(body))
	if version == "" {
		slog.Warn("openai_codex_desktop_appcast_no_version_found")
	}
	return version
}

// firstSparkleShortVersion 从 appcast XML 中提取第一个 sparkle:shortVersionString。
// 非法（非点分数字形态）时返回空串；NormalizeCodexClientVersion 的形态校验
// （最多三段）在写入链另有把关，这里只做宽松提取以便观察 feed 形态变化。
func firstSparkleShortVersion(appcastXML string) string {
	m := sparkShortVersionPattern.FindStringSubmatch(appcastXML)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

// 编译期确保接口实现。
var _ CodexDesktopAppcastClient = (*codexDesktopAppcastClient)(nil)
