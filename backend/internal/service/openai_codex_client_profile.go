package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/Wei-Shaw/sub2api/internal/pkg/codexrelease"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/gin-gonic/gin"
)

// Codex 客户端身份预设：Desktop / CLI × macOS / Windows / Linux（参照 codex-proxy-rs 的
// ClientProfileSelection）。账号 extra.codex_client_profile 优先，其次账号自定义 UA，再其次
// 全局预设、全局自定义 UA，都没有时用内置默认 Desktop macOS 跟随最新。
// 预设直接拼出完整 UA，不经 resolveCodexOutboundIdentity：那里会用 CLI 版本链重建版本段，
// 并把 Desktop 尾组的应用版本改成 core 版本。
const (
	codexClientProfileExtraKey         = "codex_client_profile"
	SettingKeyOpenAICodexClientProfile = "openai_codex_client_profile"
	codexMaxVersionLag                 = 10
	codexDesktopOriginator             = "Codex Desktop"
	codexExecOriginator                = "codex_exec"
)

// CodexClientProfile 是一份完整的预设选择；可选字段留空即用平台默认值。
type CodexClientProfile struct {
	Client         string `json:"client"`
	Platform       string `json:"platform"`
	CLIEntry       string `json:"cliEntry,omitempty"`
	VersionMode    string `json:"versionMode"`
	VersionLag     int    `json:"versionLag,omitempty"`
	OSType         string `json:"osType,omitempty"`
	OSVersion      string `json:"osVersion,omitempty"`
	Arch           string `json:"arch,omitempty"`
	Terminal       string `json:"terminal,omitempty"`
	CodexVersion   string `json:"codexVersion,omitempty"`
	DesktopVersion string `json:"desktopVersion,omitempty"`
}

type codexPlatformDefault struct{ osType, osVersion, arch string }

// codexPlatformDefaults 的系统版本只在还没同步成功过时用（同步见 codexrelease.CheckOSVersion），
// 取值是 2026-10-24 按同步规则算出的结果；Linux 按真机 os_info 报发行版 Ubuntu。
var codexPlatformDefaults = map[string]codexPlatformDefault{
	"macos":   {"Mac OS", "26.7.1", "arm64"},
	"windows": {"Windows", "10.0.26200", "x86_64"},
	"linux":   {"Ubuntu", "26.4.0", "x86_64"},
}

var codexBuiltinClientProfile = CodexClientProfile{Client: "desktop", Platform: "macos", VersionMode: "latest"}

// ParseCodexClientProfile 严格解码并校验一份预设（管理端提交值或 extra 存量值）。
func ParseCodexClientProfile(raw any) (*CodexClientProfile, error) {
	data, ok := raw.(json.RawMessage)
	if !ok {
		var err error
		if data, err = json.Marshal(raw); err != nil {
			return nil, err
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var profile CodexClientProfile
	if err := decoder.Decode(&profile); err != nil {
		return nil, fmt.Errorf("invalid codex client profile: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid codex client profile: trailing data after the object")
	}
	if err := profile.validate(); err != nil {
		return nil, err
	}
	return &profile, nil
}

func (p *CodexClientProfile) validate() error {
	if p.Client != "desktop" && p.Client != "cli" {
		return errors.New("client must be desktop or cli")
	}
	if _, ok := codexPlatformDefaults[p.Platform]; !ok {
		return errors.New("platform must be macos, windows or linux")
	}
	if p.CLIEntry != "" && (p.CLIEntry != "exec" || p.Client != "cli") {
		return errors.New("cliEntry is only exec, and only for the cli client")
	}
	for name, value := range map[string]string{"osType": p.OSType, "osVersion": p.OSVersion, "arch": p.Arch, "terminal": p.Terminal} {
		if value != "" && !codexProfileTokenOK(value) {
			return fmt.Errorf("%s must be 1-128 printable ASCII characters without ( ) ; \\ or edge spaces", name)
		}
	}
	switch p.VersionMode {
	case "latest":
		if p.CodexVersion != "" || p.DesktopVersion != "" {
			return errors.New("latest mode takes no fixed versions")
		}
		if p.VersionLag < 0 || p.VersionLag > codexMaxVersionLag {
			return fmt.Errorf("versionLag must be 0-%d", codexMaxVersionLag)
		}
	case "fixed":
		if p.VersionLag != 0 {
			return errors.New("versionLag only applies to latest mode")
		}
		if !codexCoreVersionOK(p.CodexVersion) {
			return fmt.Errorf("codexVersion must be a Codex core version not below %s", codexUpstreamMinVersion)
		}
		if p.Client == "desktop" && !codexrelease.IsDesktopVersion(p.DesktopVersion) {
			return errors.New("desktopVersion must look like 26.1016.62342")
		}
		if p.Client == "cli" && p.DesktopVersion != "" {
			return errors.New("the cli client has no desktopVersion")
		}
	default:
		return errors.New("versionMode must be latest or fixed")
	}
	return nil
}

// codexCoreVersionOK：合法 core 版本且不低于上游门槛；CompareVersions 不看预发布段，
// 门槛版本自己的预发布（0.144.0-alpha.1）同样算低于门槛。
func codexCoreVersionOK(version string) bool {
	if version == "" || NormalizeCodexClientVersion(version) != version {
		return false
	}
	order := CompareVersions(version, codexUpstreamMinVersion)
	return order > 0 || order == 0 && version == codexUpstreamMinVersion
}

func codexProfileTokenOK(value string) bool {
	if len(value) > 128 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "();\\") {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 32 || value[i] > 126 {
			return false
		}
	}
	return true
}

func (p *CodexClientProfile) originator() string {
	switch {
	case p.Client == "desktop":
		return codexDesktopOriginator
	case p.CLIEntry == "exec":
		return codexExecOriginator
	}
	return "codex_cli_rs"
}

// identity 拼出与真实客户端同形的身份：UA `{originator}/{core} ({os} {ver}; {arch}) {terminal}`，
// Desktop 追加 `(Codex Desktop; {应用版本})`，exec 追加 `(codex_exec; {core})`；version 头 = core。
func (p *CodexClientProfile) identity(releases *codexReleaseState) codexOutboundIdentity {
	core, app := p.versions(releases)
	defaults := releases.platformDefaults(p.Platform)
	originator := p.originator()
	ua := fmt.Sprintf("%s/%s (%s %s; %s) %s", originator, core,
		firstNonEmpty(p.OSType, defaults.osType), firstNonEmpty(p.OSVersion, defaults.osVersion),
		firstNonEmpty(p.Arch, defaults.arch), firstNonEmpty(p.Terminal, "unknown"))
	switch originator {
	case codexDesktopOriginator:
		ua += " (" + codexDesktopOriginator + "; " + app + ")"
	case codexExecOriginator:
		ua += " (" + codexExecOriginator + "; " + core + ")"
	}
	return codexOutboundIdentity{userAgent: ua, originator: originator, version: core}
}

// versions：fixed 用配置值；latest 取官方发布历史第 versionLag 项，历史不足时取最旧一项。
func (p *CodexClientProfile) versions(releases *codexReleaseState) (core, app string) {
	if p.VersionMode == "fixed" {
		return p.CodexVersion, p.DesktopVersion
	}
	if p.Client == "desktop" {
		history := releases.desktopHistory(codexrelease.Platform(p.Platform))
		release := history[min(p.VersionLag, len(history)-1)]
		return release.CoreVersion, release.DesktopVersion
	}
	versions := releases.cliVersions()
	return versions[min(p.VersionLag, len(versions)-1)], ""
}

// codexClientProfile 返回账号自己的预设；未配置或存量值非法时为 nil（落回自定义 UA / 全局）。
func (a *Account) codexClientProfile() *CodexClientProfile {
	if a == nil || !a.IsOpenAI() || a.Extra[codexClientProfileExtraKey] == nil {
		return nil
	}
	profile, err := ParseCodexClientProfile(a.Extra[codexClientProfileExtraKey])
	if err != nil {
		return nil
	}
	return profile
}

// validateCodexClientProfileExtra 校验管理端提交的 extra.codex_client_profile（null 表示清除）；
// 只有 OpenAI OAuth / setup-token 走 Codex 身份收口，其它账号设了也不生效，直接拒绝。
// Spark 影子行与母账号是同一个上游客户端，预设只在母账号上设（解析时影子行读母账号的）。
func validateCodexClientProfileExtra(account *Account, extra map[string]any) error {
	raw := extra[codexClientProfileExtraKey]
	if raw == nil {
		return nil
	}
	if !account.IsOpenAIOAuthLike() {
		return infraerrors.New(http.StatusBadRequest, "CODEX_CLIENT_PROFILE_INVALID", "Codex client profile requires an OpenAI OAuth or setup-token account")
	}
	if account.IsShadow() {
		return infraerrors.New(http.StatusBadRequest, "CODEX_CLIENT_PROFILE_INVALID", "set the Codex client profile on the parent account; a spark shadow uses the parent's")
	}
	if _, err := ParseCodexClientProfile(raw); err != nil {
		return infraerrors.New(http.StatusBadRequest, "CODEX_CLIENT_PROFILE_INVALID", err.Error())
	}
	return nil
}

// codexProfileRuntime 由 CodexReleaseService 发布；未发布（单测、离线工具）时全局身份维持
// 既有的规范 CLI 链，账号预设仍可用内置种子版本解析。
type codexProfileRuntime struct {
	global   *CodexClientProfile
	legacyUA bool
	releases *codexReleaseState
}

var codexProfileRuntimeState atomic.Pointer[codexProfileRuntime]

func codexPublishedReleases() *codexReleaseState {
	if rt := codexProfileRuntimeState.Load(); rt != nil {
		return rt.releases
	}
	return nil
}

// codexGlobalClientProfile：全局预设 → （未配全局自定义 UA 时）内置默认；nil 表示走既有规范链。
func codexGlobalClientProfile() (*CodexClientProfile, *codexReleaseState) {
	rt := codexProfileRuntimeState.Load()
	switch {
	case rt == nil, rt.global == nil && rt.legacyUA:
		return nil, nil
	case rt.global != nil:
		return rt.global, rt.releases
	}
	return &codexBuiltinClientProfile, rt.releases
}

// codexClientIdentityFor 解析账号的出站身份：凭据行的预设 → account 的自定义 UA → 全局。
// 预设看凭据行（影子行与母账号是同一个上游客户端）；自定义 UA 沿用各调用点原来读的行。
func codexClientIdentityFor(account, credential *Account) codexOutboundIdentity {
	if profile := credential.codexClientProfile(); profile != nil {
		return profile.identity(codexPublishedReleases())
	}
	return resolveCodexOutboundIdentity(account.GetOpenAIUserAgent())
}

// enforceCodexClientIdentityHeaders 是按账号身份收口的 enforceCodexIdentityHeadersWithUA。
// 关闭强制统一时预设与账号自定义 UA 同等对待：写进 User-Agent，交给配对收口。
func enforceCodexClientIdentityHeaders(h http.Header, account, credential *Account) {
	profile := credential.codexClientProfile()
	if profile == nil {
		enforceCodexIdentityHeadersWithUA(h, account.GetOpenAIUserAgent())
		return
	}
	identity := profile.identity(codexPublishedReleases())
	if h != nil && !codexIdentityEnforcement.Load() {
		h.Set("user-agent", identity.userAgent)
	}
	enforceResolvedCodexIdentity(h, identity)
}

// enforceCodexClientIdentity 是推理面的收口：预设取本次尝试的凭据行（见
// prepareCodexAccountIdentitySource），自定义 UA 取被转发行；ForceCodexCLI 时两者都不看，用全局身份。
func (s *OpenAIGatewayService) enforceCodexClientIdentity(c *gin.Context, h http.Header, account *Account) {
	if s != nil && s.cfg != nil && s.cfg.Gateway.ForceCodexCLI {
		enforceCodexIdentityHeaders(h)
		return
	}
	enforceCodexClientIdentityHeaders(h, account, codexAccountIdentitySource(c, account))
}
