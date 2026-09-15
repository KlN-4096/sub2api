package service

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// Codex 出站统一客户端身份选择（settings: openai_codex_client_type = cli | desktop，
// 管理员面板可选、保存即生效）。
//
// cli 维持既有实现（openai_codex_identity.go / setting_gateway_runtime.go），代码零改动；
// 本文件只承载 Desktop 分支。
//
// 真实 Desktop UA 形态（抓包样本）：
//
//	Codex Desktop/{cli} (Mac OS 26.5.2; arm64) unknown (Codex Desktop; {app})
//
// 双版本身份，与直觉相反：
//   - UA 首段 + version 头 = 内嵌 CLI 快照版本（0.x-alpha 形态，如 0.154.0-alpha.6.2）。
//     自动同步走 GitHub openai/codex 最新 alpha prerelease（最佳近似，hotfix 后缀
//     可能有偏差），面板双位置输入的位置 1 可覆写固定；
//   - 尾部官方客户端标识组 = Desktop App 版本（Sparkle shortVersionString，如 26.908.70816）。
//     自动同步走 Sparkle appcast（见 openai_codex_version_sync_service.go）。
//
// UA 属官方 `Codex ` 家族（上游按大小写敏感 starts_with("Codex ") 判定），
// openai.PairCodexClientIdentity 对该家族原样配对，originator 与 UA 首段天然自洽。
//
// 客户端类型经无参解析器下发（见 CurrentCodexClientType），GetOpenAICodexClientVersion /
// GetOpenAICodexCanonicalUserAgent 按类型分支，出站身份链（resolveCodexOutboundIdentity 等）
// 无需感知类型的存储位置。

// Codex 出站统一客户端身份类型（settings: openai_codex_client_type）。
const (
	// CodexClientTypeCLI 默认值：Codex CLI/TUI 身份，行为与既有实现完全一致。
	CodexClientTypeCLI = "cli"
	// CodexClientTypeDesktop Codex Desktop（ChatGPT Desktop 内嵌 Codex）身份。
	CodexClientTypeDesktop = "desktop"
)

// NormalizeCodexClientType 校验并归一化 Codex 客户端类型配置，非法值回退 cli。
func NormalizeCodexClientType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case CodexClientTypeDesktop:
		return CodexClientTypeDesktop
	default:
		return CodexClientTypeCLI
	}
}

// codexDesktopCLIVersion 是 Desktop 分支的编译期兜底内嵌 CLI 版本号（UA 首段 + version 头）。
// 真实客户端内嵌的是 codex-rs alpha 快照；自动同步走 GitHub openai/codex 最新
// alpha prerelease（SettingKeyOpenAICodexDesktopCLIVersionSynced，最佳近似——
// 偶发 hotfix 后缀可能与 tag 有偏差，精确值请抓包后面板覆写），本常量仅作兜底。
const codexDesktopCLIVersion = "0.154.0-alpha.6.2"

// codexDesktopAppVersion 是 Desktop 分支的编译期兜底 App 版本号（Sparkle shortVersionString 形态）。
// 填充 UA 尾部标识组 `(Codex Desktop; {app_version})`；优先级低于 appcast 自动同步值与面板覆写位置 2。
const codexDesktopAppVersion = "26.908.70816"

// codexDesktopUserAgentSuffix 与真实客户端抓包样本对齐：UA 携带 OS / 架构 / 终端指纹段
// （Desktop 桌面环境的终端段为 unknown），缺少该形态易被上游指纹识别为非官方客户端。
const codexDesktopUserAgentSuffix = " (Mac OS 26.5.2; arm64) unknown (Codex Desktop; "

// codexDesktopClientName 是 Desktop 出站 UA 首段客户端名（`Codex ` 家族，保留大小写与空格）。
const codexDesktopClientName = "Codex Desktop"

// codexDesktopUserAgent 编译期兜底 Desktop UA（首段配 codexDesktopCLIVersion、尾组配 codexDesktopAppVersion）。
var codexDesktopUserAgent = codexDesktopClientName + "/" + codexDesktopCLIVersion + codexDesktopUserAgentSuffix + codexDesktopAppVersion + ")"

// codexDesktopVersionPanelKey 面板「Codex 客户端版本号」在 Desktop 分支的双位置分隔符。
// 形态 `v1|v2`：v1 是内嵌 CLI 版本（UA 首段 + version 头），v2 是 App 版本
// （UA 尾组，同 appcast 同步源）。只填一个值时视为 v1，v2 走兜底链。
const codexDesktopVersionPanelSeparator = "|"

// codexClientTypeResolver 由 ProvideSettingService 注入的无参解析器：返回数据库中
// 管理员选择的客户端类型（内部自带 60s TTL 缓存，热路径不触库），面板保存后失效缓存，
// 切换即时生效。
var codexClientTypeResolver atomic.Value // func() string

// codexClientTypeOverride 显式覆盖快照（仅供测试使用），优先级高于注入解析器。
var codexClientTypeOverride atomic.Value // string

// SetCodexClientTypeResolver 注入客户端类型解析器。
func SetCodexClientTypeResolver(resolver func() string) {
	codexClientTypeResolver.Store(resolver)
}

// SetCodexClientType 显式覆盖客户端类型快照，仅供测试使用；非法值回退 CLI。
func SetCodexClientType(clientType string) {
	codexClientTypeOverride.Store(NormalizeCodexClientType(clientType))
}

// ResetCodexClientTypeOverride 清除显式覆盖快照，恢复为「解析器（DB 值）→ 默认 cli」
// 取值链。测试清理请用本函数而非 SetCodexClientType("")：后者会留下一个显式 CLI
// 覆盖，遮蔽注入解析器，导致依赖解析器的用例失真。
func ResetCodexClientTypeOverride() {
	codexClientTypeOverride.Store("")
}

// CurrentCodexClientType 返回当前生效的 Codex 出站客户端身份类型
// （CodexClientTypeCLI / Desktop）。取值优先级：测试覆盖 → 注入解析器
// （settings 值，60s TTL 缓存）→ 默认 cli。
// 未经装配路径（测试、工具）构造的进程行为与既有实现完全一致。
func CurrentCodexClientType() string {
	if v, ok := codexClientTypeOverride.Load().(string); ok && v != "" {
		return v
	}
	if fn, ok := codexClientTypeResolver.Load().(func() string); ok && fn != nil {
		if v := strings.TrimSpace(fn()); v != "" {
			return NormalizeCodexClientType(v)
		}
	}
	return CodexClientTypeCLI
}

// IsCodexDesktopClient 当前是否使用 Desktop 出站身份。
func IsCodexDesktopClient() bool {
	return CurrentCodexClientType() == CodexClientTypeDesktop
}

// buildCodexDesktopUserAgent 按内嵌 CLI 版本（UA 首段 + version 头）与 App 版本
// （UA 尾组）拼出 Desktop 规范 User-Agent。任一版本非法时回退编译期兜底 UA
// （与 buildCodexCLIUserAgent 同语义）。
func buildCodexDesktopUserAgent(cliVersion, appVersion string) string {
	cliVersion = NormalizeCodexClientVersion(cliVersion)
	appVersion = NormalizeCodexClientVersion(appVersion)
	if cliVersion == "" || appVersion == "" {
		return codexDesktopUserAgent
	}
	return codexDesktopClientName + "/" + cliVersion + codexDesktopUserAgentSuffix + appVersion + ")"
}

// codexDesktopPanelVersionsCache 缓存面板「Codex 客户端版本号」在 Desktop 分支的
// 双位置解析结果（进程内缓存，60s TTL）。两个位置（UA 首段 + version 头的 CLI 版本 /
// UA 尾组的 App 版本）同源于一个面板输入框，成对解析、成对缓存，避免每请求两次回源。
type codexDesktopPanelVersionsCache struct {
	cliVersion string
	appVersion string
	expiresAt  int64
}

var (
	codexDesktopPanelVersionsVal atomic.Value // *codexDesktopPanelVersionsCache
	codexDesktopPanelVersionsSF  singleflight.Group
)

// codexDesktopPanelVersionsSFKey singleflight 键。
const codexDesktopPanelVersionsSFKey = "openai_codex_desktop_panel_versions"

// resolveCodexDesktopPanelVersions 解析面板双位置值的生效结果（带 60s 缓存 + singleflight）。
// cliVersion：面板位置 1 → GitHub alpha prerelease 同步值 → 编译期兜底（三个来源全合法）。
// appVersion：面板位置 2 → appcast 同步值 → 编译期兜底（三个来源全合法，必有值）。
// DB 抖动时短缓存兜底值快速返回，不阻塞出站热路径。
func (s *SettingService) resolveCodexDesktopPanelVersions(ctx context.Context) (cliVersion, appVersion string) {
	cliFallback, appFallback := codexDesktopCLIVersion, codexDesktopAppVersion
	if s == nil || s.settingRepo == nil {
		return cliFallback, appFallback
	}
	if cached, ok := codexDesktopPanelVersionsVal.Load().(*codexDesktopPanelVersionsCache); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.cliVersion, cached.appVersion
		}
	}
	result, _, _ := codexDesktopPanelVersionsSF.Do(codexDesktopPanelVersionsSFKey, func() (any, error) {
		if cached, ok := codexDesktopPanelVersionsVal.Load().(*codexDesktopPanelVersionsCache); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached, nil
			}
		}
		if ctx == nil {
			ctx = context.Background()
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openAICodexClientVersionDBTimeout)
		defer cancel()
		values, err := s.settingRepo.GetMultiple(dbCtx, []string{
			SettingKeyOpenAICodexClientVersion,
			SettingKeyOpenAICodexDesktopClientVersionSynced,
			SettingKeyOpenAICodexDesktopCLIVersionSynced,
		})
		if err != nil {
			slog.Warn("failed to get openai codex desktop panel versions", "error", err)
			cached := &codexDesktopPanelVersionsCache{
				cliVersion: cliFallback, appVersion: appFallback,
				expiresAt: time.Now().Add(openAICodexClientVersionErrorTTL).UnixNano(),
			}
			codexDesktopPanelVersionsVal.Store(cached)
			return cached, nil
		}
		panelCLI, panelApp := parseCodexDesktopPanelVersions(values[SettingKeyOpenAICodexClientVersion])
		// 位置 1 取值链：面板覆写 → GitHub alpha prerelease 同步值 → 编译期兜底。
		cli := panelCLI
		if cli == "" {
			cli = NormalizeCodexClientVersion(values[SettingKeyOpenAICodexDesktopCLIVersionSynced])
		}
		if cli == "" {
			cli = cliFallback
		}
		app := panelApp
		if app == "" {
			app = NormalizeCodexClientVersion(values[SettingKeyOpenAICodexDesktopClientVersionSynced])
		}
		if app == "" {
			app = appFallback
		}
		cached := &codexDesktopPanelVersionsCache{
			cliVersion: cli, appVersion: app,
			expiresAt: time.Now().Add(openAICodexClientVersionCacheTTL).UnixNano(),
		}
		codexDesktopPanelVersionsVal.Store(cached)
		return cached, nil
	})
	if cached, ok := result.(*codexDesktopPanelVersionsCache); ok && cached != nil {
		return cached.cliVersion, cached.appVersion
	}
	return cliFallback, appFallback
}

// GetOpenAICodexDesktopClientVersion 返回 Desktop 出站声明的主版本号——UA 首段与
// version 头同源的内嵌 CLI 快照版本（`Codex Desktop/{v}` 的 v）。
// 优先级：面板「Codex 客户端版本号」位置 1（`v1|v2` 的 v1）→ 编译期兜底常量。
// App 版本（位置 2 / appcast 同步值）只出现在 UA 尾部标识组，不参与本取值
// （见 GetOpenAICodexDesktopAppVersion）。
func (s *SettingService) GetOpenAICodexDesktopClientVersion(ctx context.Context) string {
	if s == nil || s.settingRepo == nil {
		return codexDesktopCLIVersion
	}
	cliVersion, _ := s.resolveCodexDesktopPanelVersions(ctx)
	return cliVersion
}

// GetOpenAICodexDesktopAppVersion 返回 Desktop 出站 UA 尾部标识组的 App 版本号
// （`(Codex Desktop; {app_version})`）。优先级：面板位置 2 → appcast 自动同步值 →
// 编译期兜底常量。该值只用于规范 UA 拼装与面板展示，不进 version 头。
func (s *SettingService) GetOpenAICodexDesktopAppVersion(ctx context.Context) string {
	if s == nil || s.settingRepo == nil {
		return codexDesktopAppVersion
	}
	_, appVersion := s.resolveCodexDesktopPanelVersions(ctx)
	return appVersion
}

// parseCodexDesktopPanelVersions 解析面板「Codex 客户端版本号」在 Desktop 分支的双位置值。
// 输入形态与产出（v1 = 内嵌 CLI 版本，v2 = App 版本）：
//   - ""      → 两个位置都为空（v1 走编译期兜底，v2 走 appcast 同步链）；
//   - "v1"    → cli=v1，app=""（等价只填位置 1）；
//   - "v1|v2" → cli=v1，app=v2；
//   - "|v2"   → cli=""（走兜底），app=v2；
//   - "v1|"   → cli=v1，app=""（等价只填位置 1）；
//   - "a|b|c" 等非两段拆分 → 与空输入同义，两位置都为空，避免半配置状态。
//
// 每个位置独立过 NormalizeCodexClientVersion：非法位置为空、回落各自兜底链
// （该输入会拼进出站 UA 与 version 头，不允许不可控内容出站）。
func parseCodexDesktopPanelVersions(raw string) (cliVersion, appVersion string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	parts := strings.Split(raw, codexDesktopVersionPanelSeparator)
	switch len(parts) {
	case 1:
		// 单值：等价只填位置 1（内嵌 CLI 版本），位置 2 走兜底链。
		return NormalizeCodexClientVersion(parts[0]), ""
	case 2:
		return NormalizeCodexClientVersion(parts[0]), NormalizeCodexClientVersion(parts[1])
	default:
		// 多段拆分：与空输入同义，两位置都回落兜底链，避免半配置状态。
		return "", ""
	}
}

// codexDesktopCLIHeaderVersionResolver 是 Desktop 推理面 version 头取值的注入点。
// resolveCodexOutboundIdentity 是无 ctx 的包级收口函数，拿不到 SettingService 实例，
// 与 SetCodexCanonicalUserAgentResolver 同款：由 ProvideSettingService 在装配时注入
// 无参解析器（内部自带 60s TTL 成对缓存，热路径不触库）。未注入时返回兜底常量。
var codexDesktopCLIHeaderVersionResolver atomic.Value // func() string

// SetCodexDesktopCLIHeaderVersionResolver 注入 Desktop version 头解析器。
func SetCodexDesktopCLIHeaderVersionResolver(resolver func() string) {
	codexDesktopCLIHeaderVersionResolver.Store(resolver)
}

// CodexDesktopCLIHeaderVersionForIdentity 供 resolveCodexOutboundIdentity 的 Desktop
// 分支消费：优先走注入解析器（面板位置 1 → 兜底常量），未注入时回兜底常量。
func CodexDesktopCLIHeaderVersionForIdentity() string {
	if resolver, ok := codexDesktopCLIHeaderVersionResolver.Load().(func() string); ok && resolver != nil {
		if version := strings.TrimSpace(resolver()); version != "" {
			return version
		}
	}
	return codexDesktopCLIVersion
}

// CodexDesktopCLIHeaderVersion 返回 Desktop 出站推理面 version 头取值：
// 面板位置 1（`v1|v2` 的 v1，与 UA 首段同源）→ 编译期兜底 codexDesktopCLIVersion。
// 借 resolveCodexDesktopPanelVersions 的成对缓存，无独立回源。
func (s *SettingService) CodexDesktopCLIHeaderVersion(ctx context.Context) string {
	if s == nil || s.settingRepo == nil {
		return codexDesktopCLIVersion
	}
	cliVersion, _ := s.resolveCodexDesktopPanelVersions(ctx)
	return cliVersion
}

// InvalidateCodexDesktopPanelVersionsCache 丢弃 Desktop 双位置版本缓存，下次读取回源。
// 面板保存与 appcast 同步写入后调用，与 InvalidateOpenAICodexClientVersionCache 成对。
func (s *SettingService) InvalidateCodexDesktopPanelVersionsCache() {
	if s == nil {
		return
	}
	codexDesktopPanelVersionsSF.Forget(codexDesktopPanelVersionsSFKey)
	codexDesktopPanelVersionsVal.Store((*codexDesktopPanelVersionsCache)(nil))
}

// cachedCodexClientType 缓存管理员选择的客户端身份类型（进程内缓存，60s TTL）。
type cachedCodexClientType struct {
	value     string
	expiresAt int64
}

var (
	codexClientTypeCacheVal atomic.Value // *cachedCodexClientType
	codexClientTypeSF       singleflight.Group
)

// codexClientTypeSFKey singleflight 键。
const codexClientTypeSFKey = "openai_codex_client_type"

// GetOpenAICodexClientType 返回管理员选择的 Codex 出站客户端身份类型
// （60s TTL 缓存 + singleflight；DB 缺失回退 cli）。出站身份解析等热路径经
// 注入的无参解析器消费本方法，不触库；面板保存后失效缓存，切换即时生效。
func (s *SettingService) GetOpenAICodexClientType(ctx context.Context) string {
	if s == nil || s.settingRepo == nil {
		return CodexClientTypeCLI
	}
	if cached, ok := codexClientTypeCacheVal.Load().(*cachedCodexClientType); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.value
		}
	}
	result, _, _ := codexClientTypeSF.Do(codexClientTypeSFKey, func() (any, error) {
		if cached, ok := codexClientTypeCacheVal.Load().(*cachedCodexClientType); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.value, nil
			}
		}
		if ctx == nil {
			ctx = context.Background()
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openAICodexClientVersionDBTimeout)
		defer cancel()
		value, err := s.settingRepo.GetValue(dbCtx, SettingKeyOpenAICodexClientType)
		clientType := CodexClientTypeCLI
		if err != nil && !errors.Is(err, ErrSettingNotFound) {
			slog.Warn("failed to get openai codex client type setting", "error", err)
		} else {
			clientType = NormalizeCodexClientType(value)
		}
		codexClientTypeCacheVal.Store(&cachedCodexClientType{
			value:     clientType,
			expiresAt: time.Now().Add(openAICodexClientVersionCacheTTL).UnixNano(),
		})
		return clientType, nil
	})
	if v, ok := result.(string); ok && v != "" {
		return v
	}
	return CodexClientTypeCLI
}

// InvalidateOpenAICodexClientTypeCache 丢弃客户端类型缓存，下次读取回源。
// 面板保存后调用，类型切换即时生效。
func (s *SettingService) InvalidateOpenAICodexClientTypeCache() {
	if s == nil {
		return
	}
	codexClientTypeSF.Forget(codexClientTypeSFKey)
	codexClientTypeCacheVal.Store((*cachedCodexClientType)(nil))
}

// codexClientTypeChangeHook 类型变化后置钩子（异步调用）。由 wire 装配同步服务时
// 注入 TriggerSyncNow：管理员切换客户端类型后立即补一次同步，不必等下一个 6h 周期。
var codexClientTypeChangeHook atomic.Value // func()

// SetCodexClientTypeChangeHook 注入客户端类型变化钩子。
func SetCodexClientTypeChangeHook(hook func()) {
	codexClientTypeChangeHook.Store(hook)
}

// notifyCodexClientTypeChanged 类型确实发生变化时触发钩子（异步、非阻塞）。
func notifyCodexClientTypeChanged() {
	if hook, ok := codexClientTypeChangeHook.Load().(func()); ok && hook != nil {
		go hook()
	}
}
