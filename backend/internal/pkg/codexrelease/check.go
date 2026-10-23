package codexrelease

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Platform 是 Codex Desktop 平台，只跟默认架构（macOS arm64、Windows x64、Linux amd64）。
type Platform string

const (
	MacOS   Platform = "macos"
	Windows Platform = "windows"
	Linux   Platform = "linux"
)

const (
	officialHost    = "persistent.oaistatic.com"
	appcastURL      = "https://" + officialHost + "/codex-app-prod/appcast.xml"
	maxAppcastBytes = 1 << 20
)

var artifactURLs = map[Platform]string{
	Windows: "https://" + officialHost + "/codex-app-prod/ChatGPT-x64.msix",
	Linux:   "https://" + officialHost + "/codex-app-prod/linux/deb/latest/chatgpt_amd64.deb",
}

// Release 是核验过的官方 Desktop 发布：内置 core 版本（version 头与 User-Agent 首段版本）
// 和应用版本/构建号（User-Agent 尾组）。
type Release struct {
	CoreVersion    string   `json:"core_version"`
	DesktopVersion string   `json:"desktop_version"`
	DesktopBuild   string   `json:"desktop_build"`
	Artifact       Identity `json:"artifact"`
}

// Check 读该平台当前的官方发布。制品与 previous 读自同一份（ETag 与大小相同；macOS 看
// appcast 的版本与构建号）时直接返回 previous，不再下载。从不跟随重定向。
func Check(ctx context.Context, base *http.Client, platform Platform, previous Release) (Release, error) {
	client := *base
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if platform == MacOS {
		return checkMacOS(ctx, &client, previous)
	}
	artifactURL, ok := artifactURLs[platform]
	if !ok {
		return Release{}, fmt.Errorf("codexrelease: unknown platform %q", platform)
	}
	artifact, err := openRange(ctx, &client, artifactURL)
	if err != nil {
		return Release{}, err
	}
	if artifact.identity == previous.Artifact {
		return previous, nil
	}
	read := readWindows
	if platform == Linux {
		read = readLinux
	}
	release, err := read(artifact, artifact.identity.Size)
	if err != nil {
		return Release{}, err
	}
	release.Artifact = artifact.identity
	return release, nil
}

func checkMacOS(ctx context.Context, client *http.Client, previous Release) (Release, error) {
	item, err := fetchAppcast(ctx, client)
	if err != nil {
		return Release{}, err
	}
	if item.version == previous.DesktopVersion && item.build == previous.DesktopBuild {
		return previous, nil
	}
	artifact, err := openRange(ctx, client, item.url)
	if err != nil {
		return Release{}, err
	}
	if artifact.identity.Size != item.length {
		return Release{}, ErrInvalidArtifact
	}
	core, err := readMacOSCore(artifact, item.length)
	if err != nil {
		return Release{}, err
	}
	return Release{CoreVersion: core, DesktopVersion: item.version, DesktopBuild: item.build, Artifact: artifact.identity}, nil
}

type appcastItem struct {
	version, build, url string
	length              int64
}

func fetchAppcast(ctx context.Context, client *http.Client) (appcastItem, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, appcastURL, nil)
	if err != nil {
		return appcastItem{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return appcastItem{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return appcastItem{}, fmt.Errorf("codexrelease: appcast status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAppcastBytes+1))
	if err != nil {
		return appcastItem{}, err
	}
	if len(data) > maxAppcastBytes {
		return appcastItem{}, ErrInvalidArtifact
	}
	return parseAppcast(data)
}

// parseAppcast 取第一个同时带两个 Sparkle 版本（元素，或旧布局里 enclosure 的属性）的条目，
// 且要求附件是带签名的官方 zip。
func parseAppcast(data []byte) (appcastItem, error) {
	var feed struct {
		Items []struct {
			ShortVersion string `xml:"shortVersionString"`
			Version      string `xml:"version"`
			Enclosure    struct {
				URL          string `xml:"url,attr"`
				Length       string `xml:"length,attr"`
				ShortVersion string `xml:"shortVersionString,attr"`
				Version      string `xml:"version,attr"`
				EdSignature  string `xml:"edSignature,attr"`
			} `xml:"enclosure"`
		} `xml:"channel>item"`
	}
	if err := xml.Unmarshal(data, &feed); err != nil {
		return appcastItem{}, ErrInvalidArtifact
	}
	for _, entry := range feed.Items {
		enclosure := entry.Enclosure
		version := firstNonEmpty(entry.ShortVersion, enclosure.ShortVersion)
		build := firstNonEmpty(entry.Version, enclosure.Version)
		if version == "" || build == "" {
			continue
		}
		length, err := strconv.ParseInt(strings.TrimSpace(enclosure.Length), 10, 64)
		if !IsDesktopVersion(version) || BuildSequence(build) == 0 || err != nil || length <= 0 || length > 1<<32-1 ||
			strings.TrimSpace(enclosure.EdSignature) == "" || !officialZipURL(strings.TrimSpace(enclosure.URL)) {
			return appcastItem{}, ErrInvalidArtifact
		}
		return appcastItem{version: version, build: build, url: strings.TrimSpace(enclosure.URL), length: length}, nil
	}
	return appcastItem{}, ErrInvalidArtifact
}

func officialZipURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host == officialHost && u.User == nil && u.Fragment == "" &&
		strings.HasSuffix(strings.ToLower(u.Path), ".zip")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
