package codexrelease

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// 系统版本跟 endoflife.date（第三方汇总的各系统发布表）走，写法照 Codex 取 UA 用的 os_info：
// macOS 26.2 → 26.2.0、Windows 10.0.26200、Ubuntu 只报 VERSION_ID（24.04 → 24.4.0，不带点版本）。
// 新大版本发布满 osVersionSettleDays 天才跟，此前取上一代的最新补丁。
const (
	osFeedURL           = "https://endoflife.date/api/v1/products/"
	maxOSFeedBytes      = 1 << 20
	osVersionSettleDays = 60
	osVersionSettle     = osVersionSettleDays * 24 * time.Hour
)

// osFeedProducts：Linux 只跟 Ubuntu（真机 os_info 报发行版名，不报内核）。
var osFeedProducts = map[Platform]string{MacOS: "macos", Windows: "windows", Linux: "ubuntu"}

// Windows 11 每年的功能更新都是 H2（26H1 只给新机型）；-w 是家庭/专业版，-e 是企业版。
var windowsFeatureUpdate = regexp.MustCompile(`^11-\d{2}h2-w$`)

type osFeedRelease struct {
	Name        string `json:"name"`
	ReleaseDate string `json:"releaseDate"`
	IsLTS       bool   `json:"isLts"`
	Latest      *struct {
		Name string `json:"name"`
	} `json:"latest"`
}

// CheckOSVersion 读该平台当前跟随的系统版本：macOS 取发布满期的最新大版本的最新补丁，
// Windows 取发布满期的最新 Windows 11 H2 功能更新，Linux 取发布满期的最新 Ubuntu LTS。从不跟随重定向。
func CheckOSVersion(ctx context.Context, base *http.Client, platform Platform, now time.Time) (string, error) {
	product, ok := osFeedProducts[platform]
	if !ok {
		return "", fmt.Errorf("codexrelease: unknown platform %q", platform)
	}
	data, err := getLimited(ctx, noRedirect(base), osFeedURL+product+"/", maxOSFeedBytes, product+" feed")
	if err != nil {
		return "", err
	}
	return pickOSVersion(platform, data, now)
}

func pickOSVersion(platform Platform, data []byte, now time.Time) (string, error) {
	var feed struct {
		Result struct {
			Releases []osFeedRelease `json:"releases"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &feed); err != nil {
		return "", fmt.Errorf("codexrelease: %s feed: %w", platform, err)
	}
	if len(feed.Result.Releases) == 0 {
		return "", fmt.Errorf("codexrelease: %s feed lists no releases", platform)
	}
	var best *osFeedRelease
	var bestDate time.Time
	for i := range feed.Result.Releases {
		release := &feed.Result.Releases[i]
		released, err := time.Parse(time.DateOnly, release.ReleaseDate)
		if err != nil || now.Sub(released) < osVersionSettle || !osReleaseEligible(platform, release) {
			continue
		}
		if best == nil || released.After(bestDate) {
			best, bestDate = release, released
		}
	}
	if best == nil {
		return "", fmt.Errorf("codexrelease: no %s release has been out %d days", platform, osVersionSettleDays)
	}
	raw := best.Name
	if platform != Linux {
		if best.Latest == nil {
			return "", fmt.Errorf("codexrelease: %s release %s has no latest version", platform, best.Name)
		}
		raw = best.Latest.Name
	}
	raw = strings.TrimSpace(raw)
	// 外部表的 latest 须属于所选发布：macOS 的补丁以大版本号开头，Windows 是带构建号的 NT 10.0。
	version, ok := osInfoVersion(raw)
	if !ok || platform == MacOS && raw != best.Name && !strings.HasPrefix(raw, best.Name+".") ||
		platform == Windows && !strings.HasPrefix(raw, "10.0.") {
		return "", fmt.Errorf("codexrelease: unexpected %s version %q for release %s", platform, raw, best.Name)
	}
	return version, nil
}

func osReleaseEligible(platform Platform, release *osFeedRelease) bool {
	switch platform {
	case Windows:
		return windowsFeatureUpdate.MatchString(release.Name)
	case Linux:
		return release.IsLTS
	}
	return true
}

// osInfoVersion 按 os_info 的三段语义版本写：缺的段补 0、前导 0 去掉。多出的段只会是 Windows 的
// 修订号（UBR），os_info 在 Windows 上只取三段，丢掉。
func osInfoVersion(raw string) (string, bool) {
	out := []string{"0", "0", "0"}
	for i, part := range strings.Split(raw, ".") {
		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return "", false
		}
		if i < len(out) {
			out[i] = strconv.FormatUint(n, 10)
		}
	}
	return strings.Join(out, "."), true
}
