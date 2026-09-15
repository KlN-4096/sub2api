package service

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	// openAICodexVersionSyncInterval 自动同步间隔。上游客户端发版频率是天级，
	// 6 小时足够及时跟上，同时把对 GitHub API 的调用压到每天 4 次。
	openAICodexVersionSyncInterval = 6 * time.Hour
	// openAICodexVersionSyncTimeout 单次同步的整体超时。
	openAICodexVersionSyncTimeout = 30 * time.Second
	// openAICodexVersionSyncRepo 官方 Codex 客户端仓库。
	openAICodexVersionSyncRepo = "openai/codex"
	// openAICodexVersionSyncPerPage 回退路径单次拉取的 release 数量（主路径见
	// fetchLatestStableVersion）。该仓库预发布极密集——0.145.0 与 0.146.0 之间隔着 20 多个
	// alpha，实测 30 条里只有 2 条稳定版，第二条已排在第 26 位，因此这个页大小不能再往下调，
	// 否则整页扫不到稳定版、同步会静默停更。
	openAICodexVersionSyncPerPage = 30
	// openAICodexVersionTagPrefix 客户端 release 的 tag 前缀（如 rust-v0.146.0）。
	// 同仓库还有其他组件的 tag（如 rusty-v8-*），必须按前缀过滤，否则会同步到无关版本号。
	openAICodexVersionTagPrefix = "rust-v"
	// openAICodexDesktopAppcastURL ChatGPT Desktop（内嵌 Codex）的 Sparkle 更新源。
	// 公开无鉴权；第一个 <item> 即最新版，<sparkle:shortVersionString> 为 App 版本号
	// （如 26.908.70816），与 CLI 的 0.x 三段版本不同源、不同形态。
	openAICodexDesktopAppcastURL = "https://persistent.oaistatic.com/codex-app-prod/appcast.xml"
	// openAICodexDesktopAppcastMaxBytes appcast 响应体读取上限。feed 只含版本元数据与
	// 下载链接，实测数百 KB 量级；限制读取上限防止端点异常时无界读入内存。
	openAICodexDesktopAppcastMaxBytes = 4 << 20
)

// OpenAICodexVersionSyncService 周期性把官方 Codex 客户端的最新版本号同步到设置，
// 供出站规范身份使用，避免为了跟上游版本而发新版本。
//
// 按出站客户端身份类型（SettingKeyOpenAICodexClientType，管理员面板可选）二选一：
//   - cli（默认，既有行为）：GitHub Releases 最新稳定版 → SettingKeyOpenAICodexClientVersionSynced；
//   - desktop：Sparkle appcast 最新 App 版本 → SettingKeyOpenAICodexDesktopClientVersionSynced，
//     同时把 GitHub 最新 alpha prerelease 同步为内嵌 CLI 版本
//     → SettingKeyOpenAICodexDesktopCLIVersionSynced（Desktop 内嵌的正是 codex-rs
//     alpha 构建；偶发 hotfix 后缀可能与 GitHub tag 有偏差，精确值请抓包后面板覆写）。
//
// 管理员在面板填写的 SettingKeyOpenAICodexClientVersion 仅对 CLI 分支生效且优先级更高，
// 手工固定版本不会被同步覆盖；Desktop 分支的面板双位置输入（`v1|v2`）同理优先。
// 客户端类型可热切换（面板保存即生效），保存类型变化会经 TriggerSyncNow 立即补一次同步，
// 不必等下一个周期。
type OpenAICodexVersionSyncService struct {
	settingRepo    SettingRepository
	settingService *SettingService
	githubClient   GitHubReleaseClient
	appcastClient  CodexDesktopAppcastClient
	interval       time.Duration
	stopCh         chan struct{}
	stopOnce       sync.Once
	wg             sync.WaitGroup
	// syncNow 类型切换等场景的立即补同步信号（buffered 1，连续触发合并为一次）。
	syncNow chan struct{}
}

func NewOpenAICodexVersionSyncService(
	settingRepo SettingRepository,
	settingService *SettingService,
	githubClient GitHubReleaseClient,
	interval time.Duration,
) *OpenAICodexVersionSyncService {
	return &OpenAICodexVersionSyncService{
		settingRepo:    settingRepo,
		settingService: settingService,
		githubClient:   githubClient,
		appcastClient:  NewCodexDesktopAppcastClient(openAICodexDesktopAppcastURL),
		interval:       interval,
		stopCh:         make(chan struct{}),
		syncNow:        make(chan struct{}, 1),
	}
}

// TriggerSyncNow 非阻塞请求立即执行一次同步。服务未启动时静默丢弃。
func (s *OpenAICodexVersionSyncService) TriggerSyncNow() {
	if s == nil || s.syncNow == nil {
		return
	}
	select {
	case s.syncNow <- struct{}{}:
	default:
	}
}

func (s *OpenAICodexVersionSyncService) Start() {
	if s == nil || s.settingRepo == nil || s.interval <= 0 {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		s.runInitial()
		for {
			select {
			case <-ticker.C:
				s.runOnce()
			case <-s.syncNow:
				s.runOnce()
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *OpenAICodexVersionSyncService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

// runInitial 执行启动时的首次同步。若同步值在一个同步周期内已被刷新过则跳过：
// 频繁重启、滚动发布或崩溃重启会让「启动即同步」放大成对上游的连续请求，
// 而版本号是天级变化的，重启后没有立刻重新拉取的必要。
func (s *OpenAICodexVersionSyncService) runInitial() {
	if s.syncedWithinInterval() {
		return
	}
	s.runOnce()
}

// syncedWithinInterval 判断已同步值是否仍在一个同步周期内。
// 借同步目标设置行自身的 UpdatedAt 判断，无需额外记录时间戳的设置项。
// Desktop 分支的双版本是成对更新，两个 synced 键都在周期内才视为新鲜；
// 读取失败或尚无有效同步值时返回 false，让启动同步照常执行。
func (s *OpenAICodexVersionSyncService) syncedWithinInterval() bool {
	if s.interval <= 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), openAICodexVersionSyncTimeout)
	defer cancel()

	keys := []string{SettingKeyOpenAICodexClientVersionSynced}
	if IsCodexDesktopClient() {
		keys = []string{
			SettingKeyOpenAICodexDesktopClientVersionSynced,
			SettingKeyOpenAICodexDesktopCLIVersionSynced,
		}
	}
	for _, syncedKey := range keys {
		setting, err := s.settingRepo.Get(ctx, syncedKey)
		if err != nil || setting == nil || setting.UpdatedAt.IsZero() {
			return false
		}
		if NormalizeCodexClientVersion(setting.Value) == "" {
			return false
		}
		if time.Since(setting.UpdatedAt) >= s.interval {
			return false
		}
	}
	return true
}

func (s *OpenAICodexVersionSyncService) runOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), openAICodexVersionSyncTimeout)
	defer cancel()

	if !s.autoSyncEnabled(ctx) {
		return
	}

	if IsCodexDesktopClient() {
		s.runOnceDesktop(ctx)
		return
	}
	s.runOnceCLI(ctx)
}

// runOnceCLI CLI 分支（既有实现，仅由 runOnce 按类型分发，逻辑零改动）：
// GitHub Releases 最新稳定版 → SettingKeyOpenAICodexClientVersionSynced，只向前推进。
func (s *OpenAICodexVersionSyncService) runOnceCLI(ctx context.Context) {
	// 类型可热切换：启动时是 desktop、未装配 GitHub 客户端的进程切到 CLI 后也可能进本分支，
	// 缺客户端时安静跳过，等待具备同步能力的部署处理。
	if s.githubClient == nil {
		return
	}
	latest := s.fetchLatestStableVersion(ctx)
	if latest == "" {
		return
	}

	current := NormalizeCodexClientVersion(s.currentSyncedVersion(ctx))
	// 只向前推进：上游偶发返回旧数据或重新发布历史 tag 时不把已同步的版本号降级。
	if current != "" && CompareVersions(latest, current) <= 0 {
		return
	}
	if err := s.settingRepo.Set(ctx, SettingKeyOpenAICodexClientVersionSynced, latest); err != nil {
		slog.Warn("openai_codex_version_sync_persist_failed", "version", latest, "error", err)
		return
	}
	s.settingService.InvalidateOpenAICodexClientVersionCache()
	slog.Info("openai_codex_version_synced", "previous", current, "version", latest)
}

// runOnceDesktop Desktop 分支：Desktop 客户端同时声明两个版本号（UA 尾组 App 版本 +
// UA 首段/version 头 内嵌 CLI 版本），两者是同一发布物的配套声明，必须成对更新：
//   - 任一源（appcast / GitHub alpha）拉取失败或解析为空 → 本次同步整体放弃，
//     两个 synced 键都保持更新前的值，绝不允许「一个成功一个失败」的部分成功；
//   - 两个源都成功且任一有推进 → 用 SetMultiple 单条 upsert 语句原子成对写入
//     （要么同时生效，要么同时失败；失败时再按更新前的值防御性回写）。
func (s *OpenAICodexVersionSyncService) runOnceDesktop(ctx context.Context) {
	latestApp := ""
	if s.appcastClient != nil {
		latestApp = s.appcastClient.FetchLatestAppVersion(ctx)
	}
	latestCLI := ""
	if s.githubClient != nil {
		if releases, err := s.githubClient.FetchRecentReleases(ctx, openAICodexVersionSyncRepo, openAICodexVersionSyncPerPage); err != nil {
			slog.Warn("openai_codex_desktop_cli_version_sync_fetch_failed", "error", err)
		} else {
			latestCLI = latestCodexAlphaPrereleaseVersion(releases)
		}
	}
	if latestApp == "" || latestCLI == "" {
		slog.Warn("openai_codex_desktop_version_sync_skipped",
			"app_version", latestApp, "cli_version", latestCLI,
			"reason", "paired update requires both sources to succeed")
		return
	}

	currentApp := NormalizeCodexClientVersion(s.currentDesktopSyncedVersion(ctx))
	currentCLI := NormalizeCodexClientVersion(s.currentDesktopCLISyncedVersion(ctx))
	nextApp, appChanged := forwardCodexVersion(latestApp, currentApp)
	nextCLI, cliChanged := forwardCodexVersion(latestCLI, currentCLI)
	if !appChanged && !cliChanged {
		return
	}

	updates := map[string]string{
		SettingKeyOpenAICodexDesktopClientVersionSynced: nextApp,
		SettingKeyOpenAICodexDesktopCLIVersionSynced:    nextCLI,
	}
	if err := s.settingRepo.SetMultiple(ctx, updates); err != nil {
		slog.Warn("openai_codex_desktop_version_sync_persist_failed", "error", err)
		// 防御性回退：即使存储层部分生效，也把两个键都写回更新前的值。
		if rollbackErr := s.settingRepo.SetMultiple(ctx, map[string]string{
			SettingKeyOpenAICodexDesktopClientVersionSynced: currentApp,
			SettingKeyOpenAICodexDesktopCLIVersionSynced:    currentCLI,
		}); rollbackErr != nil {
			slog.Error("openai_codex_desktop_version_sync_rollback_failed", "error", rollbackErr)
		}
		return
	}
	s.settingService.InvalidateOpenAICodexClientVersionCache()
	s.settingService.InvalidateCodexDesktopPanelVersionsCache()
	slog.Info("openai_codex_desktop_version_synced",
		"previous_app", currentApp, "app", nextApp,
		"previous_cli", currentCLI, "cli", nextCLI)
}

// forwardCodexVersion 只向前推进：latest 合法且大于 current 时返回 (latest, true)，
// 否则返回 (current, false)——latest 为空、非法或未推进时保持 current 不动。
func forwardCodexVersion(latest, current string) (string, bool) {
	latest = NormalizeCodexClientVersion(latest)
	if latest == "" {
		return current, false
	}
	if current != "" && CompareVersions(latest, current) <= 0 {
		return current, false
	}
	return latest, true
}

// currentDesktopCLISyncedVersion 读取 Desktop 分支自动同步写入的内嵌 CLI 版本号。
func (s *OpenAICodexVersionSyncService) currentDesktopCLISyncedVersion(ctx context.Context) string {
	value, err := s.settingRepo.GetValue(ctx, SettingKeyOpenAICodexDesktopCLIVersionSynced)
	if err != nil {
		return ""
	}
	return value
}

// latestCodexAlphaPrereleaseVersion 从 release 列表里挑出最大的 alpha 预发布客户端版本号。
// 过滤条件：tag 前缀为 rust-v、非草稿、prerelease、版本号带 -alpha 后缀。
// 取最大值而非最新发布，避免重新发布历史 tag 造成回退。
func latestCodexAlphaPrereleaseVersion(releases []*GitHubRelease) string {
	best := ""
	for _, release := range releases {
		if release == nil || release.Draft || !release.Prerelease {
			continue
		}
		tag := strings.TrimSpace(release.TagName)
		if !strings.HasPrefix(tag, openAICodexVersionTagPrefix) {
			continue
		}
		version := NormalizeCodexClientVersion(strings.TrimPrefix(tag, openAICodexVersionTagPrefix))
		if version == "" || !strings.Contains(version, "-alpha") {
			continue
		}
		if best == "" || CompareVersions(version, best) > 0 {
			best = version
		}
	}
	return best
}

// fetchLatestStableVersion 取官方最新稳定版客户端版本号；取不到时返回空串，
// 由调用方保持既有值（不清空、不降级），各失败分支自行落日志。
//
// 主路径 /releases/latest：该端点本身就排除 draft 与 prerelease，直接给出最新正式发布，
// 因此不受该仓库预发布密度的影响，也不需要为了「窗口里得有一条稳定版」而多拉数据——
// 实测单条 release 约 0.3MB，而 per_page=30 的列表页约 10MB。
//
// 回退列表扫描：latest 是跨 tag 家族按 published_at 取的，若同仓库其他组件
// （如 rusty-v8-*）某天发了正式 release 而成为 latest，主路径会被 rust-v 前缀过滤挡掉；
// 此时必须扫一页 release 才能继续跟随官方版本，否则版本号会静默停更。
// 两条路径共用同一套过滤（前缀 / draft / prerelease / 版本号形态），语义不会分叉。
func (s *OpenAICodexVersionSyncService) fetchLatestStableVersion(ctx context.Context) string {
	release, err := s.githubClient.FetchLatestRelease(ctx, openAICodexVersionSyncRepo)
	if err != nil {
		slog.Warn("openai_codex_version_sync_latest_fetch_failed", "error", err)
	} else if version := latestCodexStableReleaseVersion([]*GitHubRelease{release}); version != "" {
		return version
	}

	// 主路径没拿到可用版本（抓取失败，或 latest 不是客户端 tag 家族的稳定版）。
	releases, err := s.githubClient.FetchRecentReleases(ctx, openAICodexVersionSyncRepo, openAICodexVersionSyncPerPage)
	if err != nil {
		slog.Warn("openai_codex_version_sync_fetch_failed", "error", err)
		return ""
	}
	version := latestCodexStableReleaseVersion(releases)
	if version == "" {
		slog.Warn("openai_codex_version_sync_no_stable_release", "repo", openAICodexVersionSyncRepo)
	}
	return version
}

// autoSyncEnabled 读取面板开关。缺失或空值视为开启，与设置默认值一致；
// 读取失败时保持开启，避免一次数据库抖动就静默停掉版本跟随。
func (s *OpenAICodexVersionSyncService) autoSyncEnabled(ctx context.Context) bool {
	value, err := s.settingRepo.GetValue(ctx, SettingKeyOpenAICodexVersionAutoSyncEnabled)
	if err != nil {
		return true
	}
	if strings.TrimSpace(value) == "" {
		return true
	}
	return strings.TrimSpace(value) == "true"
}

func (s *OpenAICodexVersionSyncService) currentSyncedVersion(ctx context.Context) string {
	value, err := s.settingRepo.GetValue(ctx, SettingKeyOpenAICodexClientVersionSynced)
	if err != nil {
		return ""
	}
	return value
}

// currentDesktopSyncedVersion 读取 Desktop 分支自动同步写入的 App 版本号。
func (s *OpenAICodexVersionSyncService) currentDesktopSyncedVersion(ctx context.Context) string {
	value, err := s.settingRepo.GetValue(ctx, SettingKeyOpenAICodexDesktopClientVersionSynced)
	if err != nil {
		return ""
	}
	return value
}

// latestCodexStableReleaseVersion 从 release 列表里挑出最大的稳定版客户端版本号。
// 过滤条件：tag 前缀为 rust-v（排除同仓库其他组件的 tag）、非草稿、非预发布、
// 版本号不带 -alpha/-beta 之类后缀。取最大值而非最新发布，避免重新发布历史 tag 造成回退。
// 主路径的单条 /releases/latest 结果也走本函数（单元素切片），保证两条取数路径的过滤语义一致。
func latestCodexStableReleaseVersion(releases []*GitHubRelease) string {
	best := ""
	for _, release := range releases {
		if release == nil || release.Draft || release.Prerelease {
			continue
		}
		tag := strings.TrimSpace(release.TagName)
		if !strings.HasPrefix(tag, openAICodexVersionTagPrefix) {
			continue
		}
		version := NormalizeCodexClientVersion(strings.TrimPrefix(tag, openAICodexVersionTagPrefix))
		if version == "" || strings.Contains(version, "-") {
			continue
		}
		if best == "" || CompareVersions(version, best) > 0 {
			best = version
		}
	}
	return best
}
