import { apiClient } from '../client'

/** Codex 客户端身份预设（与后端 CodexClientProfile 同形，字段留空 = 平台默认值）。 */
export interface CodexClientProfile {
  client: 'desktop' | 'cli'
  platform: 'macos' | 'windows' | 'linux'
  cliEntry?: 'exec'
  versionMode: 'latest' | 'fixed'
  versionLag?: number
  osType?: string
  osVersion?: string
  arch?: string
  terminal?: string
  codexVersion?: string
  desktopVersion?: string
}

export interface CodexClientIdentityPreview {
  user_agent: string
  originator: string
  version: string
  source?: 'profile' | 'builtin' | 'legacy_user_agent'
}

export interface CodexClientProfilePreset {
  profile: CodexClientProfile
  originator: string
  os_type: string
  os_version: string
  arch: string
  terminal: string
}

export interface CodexDesktopRelease {
  core_version: string
  desktop_version: string
  desktop_build: string
}

export interface CodexPlatformReleaseStatus {
  latest: CodexDesktopRelease
  verified: boolean
  history: CodexDesktopRelease[] | null
  checked_at?: string
  error?: string
}

export interface CodexOSVersionStatus {
  os_type: string
  version: string
  /** false 表示还没同步成功过，version 是内置值；verified_at 是 version 最近一次同步成功的时刻。 */
  synced: boolean
  verified_at?: string
  checked_at?: string
  error?: string
}

export interface CodexClientProfileView {
  global: CodexClientProfile | null
  effective: CodexClientIdentityPreview
  fallback: CodexClientIdentityPreview
  presets: CodexClientProfilePreset[]
  max_version_lag: number
  refreshing: boolean
  desktop: Record<'macos' | 'windows' | 'linux', CodexPlatformReleaseStatus>
  cli: string[]
  os: Record<'macos' | 'windows' | 'linux', CodexOSVersionStatus>
}

const base = '/admin/settings/codex-client-profile'

export async function getCodexClientProfile(): Promise<CodexClientProfileView> {
  const { data } = await apiClient.get<CodexClientProfileView>(base)
  return data
}

export async function updateCodexClientProfile(profile: CodexClientProfile | null): Promise<CodexClientProfileView> {
  const { data } = await apiClient.put<CodexClientProfileView>(base, { profile })
  return data
}

export async function previewCodexClientProfile(profile: CodexClientProfile): Promise<CodexClientIdentityPreview> {
  const { data } = await apiClient.post<CodexClientIdentityPreview>(`${base}/preview`, { profile })
  return data
}

export async function refreshCodexReleases(): Promise<CodexClientProfileView> {
  const { data } = await apiClient.post<CodexClientProfileView>(`${base}/refresh`)
  return data
}

/** 账号 extra 里的存量值只做形状判断，合法性以后端校验为准。 */
export function asCodexClientProfile(value: unknown): CodexClientProfile | null {
  if (!value || typeof value !== 'object') return null
  const profile = value as Partial<CodexClientProfile>
  return typeof profile.client === 'string' && typeof profile.platform === 'string' && typeof profile.versionMode === 'string'
    ? (profile as CodexClientProfile)
    : null
}
