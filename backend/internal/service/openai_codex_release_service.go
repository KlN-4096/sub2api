package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/codexrelease"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/httpclient"
)

const (
	SettingKeyOpenAICodexReleases = "openai_codex_releases"
	codexReleaseHistoryLimit      = codexMaxVersionLag + 1
	codexReleaseCheckInterval     = 24 * time.Hour
	codexReleaseCheckTimeout      = 8 * time.Minute
	codexGlobalProfileReload      = time.Minute
)

var codexReleasePlatforms = []codexrelease.Platform{codexrelease.MacOS, codexrelease.Windows, codexrelease.Linux}

// codexDesktopReleaseSeeds 是 2026-10-23 从三平台官方安装包实测读出的版本，只在尚无核验记录时
// 兜底（首次检查前、或官方源一直不可达）；没有制品标识，首次检查必然完整核验一次。
var codexDesktopReleaseSeeds = map[codexrelease.Platform]codexrelease.Release{
	codexrelease.MacOS:   {CoreVersion: "0.163.0-alpha.11.2", DesktopVersion: "26.1016.62342", DesktopBuild: "21477"},
	codexrelease.Windows: {CoreVersion: "0.163.0-alpha.11.2", DesktopVersion: "26.1016.62342", DesktopBuild: "21477"},
	codexrelease.Linux:   {CoreVersion: "0.163.0-alpha.11.1", DesktopVersion: "26.1016.61520", DesktopBuild: "21421"},
}

type codexPlatformReleases struct {
	History   []codexrelease.Release `json:"history,omitempty"`
	CheckedAt *time.Time             `json:"checked_at,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

// codexOSRelease 是同步到的系统版本；失败只记 CheckedAt/Error，保留上次成功的 Version/VerifiedAt。
type codexOSRelease struct {
	Version    string     `json:"version,omitempty"`
	VerifiedAt *time.Time `json:"verified_at,omitempty"`
	CheckedAt  *time.Time `json:"checked_at,omitempty"`
	Error      string     `json:"error,omitempty"`
}

// codexReleaseState 持久化在 settings（各实例各自检查，读写时按 merge 合并）；发布后只读，更新一律换新值。
type codexReleaseState struct {
	Desktop map[codexrelease.Platform]codexPlatformReleases `json:"desktop"`
	CLI     []string                                        `json:"cli,omitempty"`
	OS      map[codexrelease.Platform]codexOSRelease        `json:"os,omitempty"`
}

// platformDefaults 是预设未覆盖时的系统字段：系统版本取同步值，没同步成功过用写死值。
func (s *codexReleaseState) platformDefaults(platform string) codexPlatformDefault {
	defaults := codexPlatformDefaults[platform]
	if s != nil {
		if version := s.OS[codexrelease.Platform(platform)].Version; version != "" {
			defaults.osVersion = version
		}
	}
	return defaults
}

// desktopHistory 新的在前；无核验记录时用内置种子。
func (s *codexReleaseState) desktopHistory(platform codexrelease.Platform) []codexrelease.Release {
	if s != nil && len(s.Desktop[platform].History) > 0 {
		return s.Desktop[platform].History
	}
	return []codexrelease.Release{codexDesktopReleaseSeeds[platform]}
}

// cliVersions：当前生效的 CLI 版本（面板版本号 → GitHub 正式版同步值 → 内置常量）在前，
// 其后是观察到的历史版本，供滞后档位使用。
func (s *codexReleaseState) cliVersions() []string {
	versions := []string{codexClientVersionFromUA(codexCanonicalUserAgent())}
	if s != nil {
		for _, version := range s.CLI {
			if CompareVersions(version, versions[len(versions)-1]) < 0 {
				versions = append(versions, version)
			}
		}
	}
	return versions
}

// merge 每个平台取构建更新的那份记录、CLI 取版本更新的那份、系统版本取成功得更晚的那份：
// 多实例各自检查、各自写回，合并后谁的结果都不会让版本倒退。
func (s codexReleaseState) merge(other codexReleaseState) codexReleaseState {
	osVersions := maps.Clone(s.OS)
	for platform, entry := range other.OS {
		if current := osVersions[platform]; entry.VerifiedAt != nil && (current.VerifiedAt == nil || entry.VerifiedAt.After(*current.VerifiedAt)) {
			if osVersions == nil {
				osVersions = map[codexrelease.Platform]codexOSRelease{}
			}
			osVersions[platform] = entry
		}
	}
	s.OS = osVersions
	desktop := maps.Clone(s.Desktop)
	if desktop == nil {
		desktop = map[codexrelease.Platform]codexPlatformReleases{}
	}
	for platform, entry := range other.Desktop {
		if entry.latestBuild() > desktop[platform].latestBuild() {
			desktop[platform] = entry
		}
	}
	s.Desktop = desktop
	if len(other.CLI) > 0 && (len(s.CLI) == 0 || CompareVersions(other.CLI[0], s.CLI[0]) > 0) {
		s.CLI = other.CLI
	}
	return s
}

func (p codexPlatformReleases) latestBuild() uint64 {
	if len(p.History) == 0 {
		return 0
	}
	return codexrelease.BuildSequence(p.History[0].DesktopBuild)
}

// accept 只接受更新的构建；同构建重查只刷新制品标识，版本却变了视为异常不接受。
func (p codexPlatformReleases) accept(next codexrelease.Release) (codexPlatformReleases, error) {
	build := codexrelease.BuildSequence(next.DesktopBuild)
	if build == 0 || !codexCoreVersionOK(next.CoreVersion) {
		return p, codexrelease.ErrInvalidArtifact
	}
	older := p.History
	if len(older) > 0 {
		current := older[0]
		switch verified := p.latestBuild(); {
		case build < verified:
			return p, fmt.Errorf("official build %d is older than the verified build %d", build, verified)
		case build == verified && (next.CoreVersion != current.CoreVersion || next.DesktopVersion != current.DesktopVersion):
			return p, fmt.Errorf("official build %d changed versions without a new build", build)
		case build == verified:
			older = older[1:]
		}
	}
	history := append([]codexrelease.Release{next}, older...)
	p.History = history[:min(len(history), codexReleaseHistoryLimit)]
	return p, nil
}

// CodexReleaseService 跟踪官方 Codex Desktop 发布与三平台系统版本（启动一次＋每 24h＋手动），并与全局预设一起
// 发布出站身份解析用的运行时。
type CodexReleaseService struct {
	settingRepo SettingRepository
	client      *http.Client
	clientErr   error

	mu        sync.Mutex
	state     codexReleaseState
	global    *CodexClientProfile
	globalRaw string
	legacyUA  bool
	running   atomic.Bool

	ctx      context.Context
	cancel   context.CancelFunc
	trigger  chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

var activeCodexReleaseService atomic.Pointer[CodexReleaseService]

// CodexReleases 返回已启动的服务；未启动时为 nil。
func CodexReleases() *CodexReleaseService { return activeCodexReleaseService.Load() }

func NewCodexReleaseService(settingRepo SettingRepository, client *http.Client, clientErr error) *CodexReleaseService {
	ctx, cancel := context.WithCancel(context.Background())
	return &CodexReleaseService{
		settingRepo: settingRepo,
		client:      client,
		clientErr:   clientErr,
		state:       codexReleaseState{Desktop: map[codexrelease.Platform]codexPlatformReleases{}},
		ctx:         ctx,
		cancel:      cancel,
		trigger:     make(chan struct{}, 1),
	}
}

func (s *CodexReleaseService) Start() {
	s.reload()
	activeCodexReleaseService.Store(s)
	s.running.Store(true)
	s.wg.Add(1)
	go s.loop()
}

func (s *CodexReleaseService) Stop() {
	s.stopOnce.Do(func() {
		s.cancel()
		s.wg.Wait()
	})
}

// loop 只调度：检查（三平台最长各 8 分钟）在独立 goroutine 里跑，不挡一分钟一次的 reload。
// running 由发起方置位（Start / TriggerRefresh / 定时器 CAS），检查结束才清，同时只有一个在跑。
func (s *CodexReleaseService) loop() {
	defer s.wg.Done()
	s.runRefresh()
	check := time.NewTicker(codexReleaseCheckInterval)
	reload := time.NewTicker(codexGlobalProfileReload)
	defer check.Stop()
	defer reload.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-check.C:
			if s.running.CompareAndSwap(false, true) {
				s.runRefresh()
			}
		case <-s.trigger:
			s.runRefresh()
		case <-reload.C:
			s.reload()
		}
	}
}

// TriggerRefresh 请求立即检查；已有检查在跑时合并。置位在这里做，返回的视图立刻是 refreshing。
func (s *CodexReleaseService) TriggerRefresh() {
	if s.running.CompareAndSwap(false, true) {
		s.trigger <- struct{}{}
	}
}

func (s *CodexReleaseService) runRefresh() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.running.Store(false)
		s.refresh()
	}()
}

func (s *CodexReleaseService) refresh() {
	for _, platform := range codexReleasePlatforms {
		s.mu.Lock()
		entry := s.state.Desktop[platform]
		s.mu.Unlock()
		previous := codexrelease.Release{}
		if len(entry.History) > 0 {
			previous = entry.History[0]
		}
		release, err := s.check(platform, previous)
		if err == nil {
			entry, err = entry.accept(release)
		}
		now := time.Now().UTC()
		entry.CheckedAt, entry.Error = &now, ""
		if err != nil {
			entry.Error = err.Error()
			slog.Warn("codex desktop release check failed", "platform", platform, "error", err)
		}
		s.mu.Lock()
		desktop := make(map[codexrelease.Platform]codexPlatformReleases, len(s.state.Desktop)+1)
		for key, value := range s.state.Desktop {
			desktop[key] = value
		}
		desktop[platform] = entry
		s.state.Desktop = desktop
		s.mu.Unlock()
		if s.ctx.Err() != nil {
			return
		}
		s.refreshOSVersion(platform)
	}
	stored := s.storedReleases()
	s.mu.Lock()
	if live := codexClientVersionFromUA(codexCanonicalUserAgent()); len(s.state.CLI) == 0 || CompareVersions(live, s.state.CLI[0]) > 0 {
		cli := append([]string{live}, s.state.CLI...)
		s.state.CLI = cli[:min(len(cli), codexReleaseHistoryLimit)]
	}
	// 写回前先并入其它实例已写的结果，慢实例不会把旧记录写回去。
	if stored != nil {
		s.state = s.state.merge(*stored)
	}
	data, err := json.Marshal(s.state)
	s.publishLocked()
	s.mu.Unlock()
	if err == nil {
		err = s.settingRepo.Set(s.ctx, SettingKeyOpenAICodexReleases, string(data))
	}
	if err != nil {
		slog.Warn("codex release state not persisted", "error", err)
	}
}

// check 解析的是外部下载的安装包：解析器出 panic 也只算这次检查失败，不能拖垮网关进程。
func (s *CodexReleaseService) check(platform codexrelease.Platform, previous codexrelease.Release) (release codexrelease.Release, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			release, err = codexrelease.Release{}, fmt.Errorf("codex release check panicked: %v", recovered)
		}
	}()
	if s.clientErr != nil {
		return codexrelease.Release{}, s.clientErr
	}
	ctx, cancel := context.WithTimeout(s.ctx, codexReleaseCheckTimeout)
	defer cancel()
	return codexrelease.Check(ctx, s.client, platform, previous)
}

// refreshOSVersion 与 Desktop 发布同周期读系统版本；失败保留上次成功的值。
func (s *CodexReleaseService) refreshOSVersion(platform codexrelease.Platform) {
	s.mu.Lock()
	entry := s.state.OS[platform]
	s.mu.Unlock()
	version, err := s.checkOSVersion(platform)
	now := time.Now().UTC()
	entry.CheckedAt, entry.Error = &now, ""
	if err != nil {
		entry.Error = err.Error()
		slog.Warn("codex os version check failed", "platform", platform, "error", err)
	} else {
		entry.Version, entry.VerifiedAt = version, &now
	}
	s.mu.Lock()
	osVersions := maps.Clone(s.state.OS)
	if osVersions == nil {
		osVersions = map[codexrelease.Platform]codexOSRelease{}
	}
	osVersions[platform] = entry
	s.state.OS = osVersions
	s.mu.Unlock()
}

// checkOSVersion 只发一个请求，超时用客户端自带的一分钟。
func (s *CodexReleaseService) checkOSVersion(platform codexrelease.Platform) (string, error) {
	if s.clientErr != nil {
		return "", s.clientErr
	}
	return codexrelease.CheckOSVersion(s.ctx, s.client, platform, time.Now())
}

// reload 读全局预设、全局自定义 UA 与发布记录（其它实例的保存和检查结果一分钟内生效）。
// 读失败的项保留旧值，不因一次读库失败切换出站身份。
func (s *CodexReleaseService) reload() {
	raw, err := s.settingRepo.GetValue(s.ctx, SettingKeyOpenAICodexClientProfile)
	profileOK := err == nil || errors.Is(err, ErrSettingNotFound)
	if !profileOK {
		slog.Warn("codex client profile setting unreadable", "error", err)
	}
	// 只看原始设置：面板留空时 GetOpenAICodexUserAgent 返回内置默认 UA，永远不为空。
	legacyRaw, err := s.settingRepo.GetValue(s.ctx, SettingKeyOpenAICodexUserAgent)
	legacyOK := err == nil || errors.Is(err, ErrSettingNotFound)
	stored := s.storedReleases()

	s.mu.Lock()
	defer s.mu.Unlock()
	if profileOK && raw != s.globalRaw {
		s.globalRaw, s.global = raw, nil
		if strings.TrimSpace(raw) != "" {
			if s.global, err = ParseCodexClientProfile(json.RawMessage(raw)); err != nil {
				slog.Warn("codex client profile setting invalid, ignored", "error", err)
			}
		}
	}
	if legacyOK {
		s.legacyUA = strings.TrimSpace(legacyRaw) != ""
	}
	if stored != nil {
		s.state = s.state.merge(*stored)
	}
	s.publishLocked()
}

// storedReleases 读库里的发布记录；没有或读不出时为 nil。
func (s *CodexReleaseService) storedReleases() *codexReleaseState {
	raw, err := s.settingRepo.GetValue(s.ctx, SettingKeyOpenAICodexReleases)
	if err != nil || raw == "" {
		return nil
	}
	var state codexReleaseState
	if json.Unmarshal([]byte(raw), &state) != nil || state.Desktop == nil {
		return nil
	}
	return &state
}

// publishLocked 必须持 s.mu：发布顺序与加锁顺序一致，旧快照不会后写覆盖新保存的预设。
func (s *CodexReleaseService) publishLocked() {
	state := s.state
	codexProfileRuntimeState.Store(&codexProfileRuntime{global: s.global, legacyUA: s.legacyUA, releases: &state})
}

// ProvideCodexReleaseService 创建并启动 CodexReleaseService；官方制品与 GitHub 版本同步一样走 update.proxy_url。
func ProvideCodexReleaseService(settingRepo SettingRepository, cfg *config.Config) *CodexReleaseService {
	client, err := httpclient.GetClient(httpclient.Options{ProxyURL: cfg.Update.ProxyURL, Timeout: time.Minute})
	if err != nil && cfg.Security.ProxyFallback.AllowDirectOnError {
		client, err = &http.Client{Timeout: time.Minute}, nil
	}
	svc := NewCodexReleaseService(settingRepo, client, err)
	svc.Start()
	return svc
}

// CodexClientIdentityPreview 是一份身份解析结果（管理端预览用）。
type CodexClientIdentityPreview struct {
	UserAgent  string `json:"user_agent"`
	Originator string `json:"originator"`
	Version    string `json:"version"`
	// Source: profile（全局预设）/ builtin（内置默认）/ legacy_user_agent（全局自定义 UA）。
	Source string `json:"source,omitempty"`
}

type CodexClientProfilePreset struct {
	Profile    CodexClientProfile `json:"profile"`
	Originator string             `json:"originator"`
	OSType     string             `json:"os_type"`
	OSVersion  string             `json:"os_version"`
	Arch       string             `json:"arch"`
	Terminal   string             `json:"terminal"`
}

type CodexPlatformReleaseStatus struct {
	Latest codexrelease.Release `json:"latest"`
	// Verified 为 false 表示还没有成功核验过，Latest 是内置种子。
	Verified  bool                   `json:"verified"`
	History   []codexrelease.Release `json:"history"`
	CheckedAt *time.Time             `json:"checked_at,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

type CodexOSVersionStatus struct {
	OSType  string `json:"os_type"`
	Version string `json:"version"`
	// Synced 为 false 表示还没同步成功过，Version 是写死值；VerifiedAt 是 Version 最近一次同步成功的时刻。
	Synced     bool       `json:"synced"`
	VerifiedAt *time.Time `json:"verified_at,omitempty"`
	CheckedAt  *time.Time `json:"checked_at,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type CodexClientProfileView struct {
	Global    *CodexClientProfile        `json:"global"`
	Effective CodexClientIdentityPreview `json:"effective"`
	// Fallback 是不设全局预设时的身份（全局自定义 UA 或内置默认）。
	Fallback      CodexClientIdentityPreview                           `json:"fallback"`
	Presets       []CodexClientProfilePreset                           `json:"presets"`
	MaxVersionLag int                                                  `json:"max_version_lag"`
	Refreshing    bool                                                 `json:"refreshing"`
	Desktop       map[codexrelease.Platform]CodexPlatformReleaseStatus `json:"desktop"`
	CLI           []string                                             `json:"cli"`
	OS            map[codexrelease.Platform]CodexOSVersionStatus       `json:"os"`
}

func (s *CodexReleaseService) ClientProfileView() CodexClientProfileView {
	rt := codexProfileRuntimeState.Load()
	effective := previewCodexIdentity(resolveCodexOutboundIdentity(""))
	switch {
	case rt != nil && rt.global != nil:
		effective.Source = "profile"
	case rt != nil && rt.legacyUA:
		effective.Source = "legacy_user_agent"
	default:
		effective.Source = "builtin"
	}
	view := CodexClientProfileView{
		Effective:     effective,
		MaxVersionLag: codexMaxVersionLag,
		Refreshing:    s.running.Load(),
		Desktop:       map[codexrelease.Platform]CodexPlatformReleaseStatus{},
		OS:            map[codexrelease.Platform]CodexOSVersionStatus{},
	}
	var releases *codexReleaseState
	if rt != nil {
		view.Global, releases = rt.global, rt.releases
	}
	view.Fallback = previewCodexIdentity(codexBuiltinClientProfile.identity(releases))
	view.Fallback.Source = "builtin"
	if rt != nil && rt.legacyUA {
		view.Fallback = previewCodexIdentity(resolveCodexUserAgentIdentity(""))
		view.Fallback.Source = "legacy_user_agent"
	}
	for _, platform := range []string{"macos", "windows", "linux"} {
		defaults := releases.platformDefaults(platform)
		for _, client := range []string{"desktop", "cli"} {
			profile := CodexClientProfile{Client: client, Platform: platform, VersionMode: "latest"}
			view.Presets = append(view.Presets, CodexClientProfilePreset{
				Profile: profile, Originator: profile.originator(),
				OSType: defaults.osType, OSVersion: defaults.osVersion, Arch: defaults.arch, Terminal: "unknown",
			})
		}
	}
	for _, platform := range codexReleasePlatforms {
		var entry codexPlatformReleases
		if releases != nil {
			entry = releases.Desktop[platform]
		}
		view.Desktop[platform] = CodexPlatformReleaseStatus{
			Latest: releases.desktopHistory(platform)[0], Verified: len(entry.History) > 0,
			History: entry.History, CheckedAt: entry.CheckedAt, Error: entry.Error,
		}
		var osEntry codexOSRelease
		if releases != nil {
			osEntry = releases.OS[platform]
		}
		defaults := releases.platformDefaults(string(platform))
		view.OS[platform] = CodexOSVersionStatus{
			OSType: defaults.osType, Version: defaults.osVersion, Synced: osEntry.Version != "",
			VerifiedAt: osEntry.VerifiedAt, CheckedAt: osEntry.CheckedAt, Error: osEntry.Error,
		}
	}
	view.CLI = releases.cliVersions()
	return view
}

// PreviewClientProfile 按当前已核验的发布解析一份预设。
func (s *CodexReleaseService) PreviewClientProfile(raw json.RawMessage) (CodexClientIdentityPreview, error) {
	profile, err := ParseCodexClientProfile(raw)
	if err != nil {
		return CodexClientIdentityPreview{}, infraerrors.New(http.StatusBadRequest, "CODEX_CLIENT_PROFILE_INVALID", err.Error())
	}
	return previewCodexIdentity(profile.identity(codexPublishedReleases())), nil
}

// SaveGlobalClientProfile 保存全局预设；空或 null 清除（回到全局自定义 UA / 内置默认）。
func (s *CodexReleaseService) SaveGlobalClientProfile(ctx context.Context, raw json.RawMessage) error {
	value := ""
	if trimmed := strings.TrimSpace(string(raw)); trimmed != "" && trimmed != "null" {
		profile, err := ParseCodexClientProfile(raw)
		if err != nil {
			return infraerrors.New(http.StatusBadRequest, "CODEX_CLIENT_PROFILE_INVALID", err.Error())
		}
		data, _ := json.Marshal(profile)
		value = string(data)
	}
	if err := s.settingRepo.Set(ctx, SettingKeyOpenAICodexClientProfile, value); err != nil {
		return err
	}
	s.reload()
	return nil
}

func previewCodexIdentity(identity codexOutboundIdentity) CodexClientIdentityPreview {
	return CodexClientIdentityPreview{UserAgent: identity.userAgent, Originator: identity.originator, Version: identity.version}
}
