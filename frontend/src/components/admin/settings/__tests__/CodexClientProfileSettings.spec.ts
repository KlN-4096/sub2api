import { beforeEach, describe, expect, it, vi } from 'vitest'
import { defineComponent } from 'vue'
import { flushPromises, mount } from '@vue/test-utils'

const api = vi.hoisted(() => ({
  getCodexClientProfile: vi.fn(),
  updateCodexClientProfile: vi.fn(),
  refreshCodexReleases: vi.fn(),
  previewCodexClientProfile: vi.fn(() => new Promise(() => {})),
}))
vi.mock('@/api/admin/codexClientProfile', () => api)
vi.mock('@/stores', () => ({ useAppStore: () => ({ showError: vi.fn(), showSuccess: vi.fn() }) }))
vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return { ...actual, useI18n: () => ({ t: (key: string, params?: Record<string, string>) => (params ? `${key}:${Object.values(params).join('|')}` : key) }) }
})

import CodexClientProfileSettings from '../CodexClientProfileSettings.vue'

const SelectStub = defineComponent({
  name: 'SelectStub',
  props: { modelValue: { type: [String, Number], default: '' }, options: { type: Array, default: () => [] } },
  emits: ['update:modelValue'],
  template: `<select :value="modelValue" @change="$emit('update:modelValue', $event.target.value)">
    <option v-for="o in options" :key="o.value" :value="o.value">{{ o.label }}</option></select>`,
})

const release = { core_version: '0.163.0-alpha.11.2', desktop_version: '26.1016.62342', desktop_build: '21477' }
function view(overrides: Record<string, unknown> = {}) {
  return {
    global: null,
    effective: { user_agent: 'Codex Desktop/0.163.0-alpha.11.2', originator: 'Codex Desktop', version: '0.163.0-alpha.11.2', source: 'builtin' },
    fallback: { user_agent: 'Codex Desktop/0.163.0-alpha.11.2', originator: 'Codex Desktop', version: '0.163.0-alpha.11.2', source: 'builtin' },
    presets: [{ profile: { client: 'cli', platform: 'linux', versionMode: 'latest' }, originator: 'codex_cli_rs', os_type: 'Ubuntu', os_version: '26.4.0', arch: 'x86_64', terminal: 'unknown' }],
    max_version_lag: 10,
    refreshing: false,
    desktop: { macos: { latest: release, verified: true, history: [release] }, windows: { latest: release, verified: false, history: null }, linux: { latest: release, verified: false, history: null } },
    cli: ['0.163.0'],
    os: {
      macos: { os_type: 'Mac OS', version: '26.7.1', synced: false },
      windows: { os_type: 'Windows', version: '10.0.26200', synced: false },
      linux: { os_type: 'Ubuntu', version: '26.4.0', synced: false },
    },
    ...overrides,
  }
}

describe('CodexClientProfileSettings', () => {
  beforeEach(() => {
    vi.useRealTimers()
    for (const fn of [api.getCodexClientProfile, api.updateCodexClientProfile, api.refreshCodexReleases]) fn.mockReset()
  })

  it('saves the selected global preset', async () => {
    api.getCodexClientProfile.mockResolvedValue(view())
    api.updateCodexClientProfile.mockResolvedValue(view({ global: { client: 'cli', platform: 'linux', versionMode: 'latest' } }))
    const wrapper = mount(CodexClientProfileSettings, { global: { stubs: { Select: SelectStub } } })
    await flushPromises()
    expect(wrapper.text()).toContain('admin.codexClientProfile.effective:admin.codexClientProfile.source.builtin')

    await wrapper.find('[data-testid="codex-profile-preset"]').setValue('cli:linux')
    await wrapper.find('[data-testid="codex-profile-save"]').trigger('click')
    await flushPromises()
    expect(api.updateCodexClientProfile).toHaveBeenCalledWith({ client: 'cli', platform: 'linux', versionMode: 'latest' })
  })

  it('shows the synced OS versions, how old they are and why a sync failed', async () => {
    api.getCodexClientProfile.mockResolvedValue(view({
      os: {
        macos: { os_type: 'Mac OS', version: '27.0.1', synced: true, verified_at: '2026-12-01T00:00:00Z', checked_at: '2026-12-01T00:00:00Z' },
        windows: { os_type: 'Windows', version: '10.0.26200', synced: false, checked_at: '2026-12-01T00:00:00Z', error: 'feed down' },
        linux: { os_type: 'Ubuntu', version: '26.4.0', synced: true, verified_at: '2026-11-01T00:00:00Z', checked_at: '2026-12-01T00:00:00Z', error: 'feed down' },
      },
    }))
    const wrapper = mount(CodexClientProfileSettings, { global: { stubs: { Select: SelectStub } } })
    await flushPromises()
    const macos = wrapper.find('[data-testid="codex-os-macos"]').text()
    expect(macos).toContain('Mac OS 27.0.1')
    expect(macos).toContain('admin.codexClientProfile.syncedAt')
    expect(macos).not.toContain('admin.codexClientProfile.checkedAt')
    expect(macos).not.toContain('admin.codexClientProfile.osBuiltin')
    const windows = wrapper.find('[data-testid="codex-os-windows"]').text()
    expect(windows).toContain('admin.codexClientProfile.osBuiltin')
    expect(windows).toContain('admin.codexClientProfile.checkedAt')
    expect(windows).toContain('feed down')
    const linux = wrapper.find('[data-testid="codex-os-linux"]').text()
    expect(linux).toContain('admin.codexClientProfile.syncedAt')
    expect(linux).toContain('admin.codexClientProfile.checkedAt')
  })

  it('keeps Enter in its inputs from submitting the settings form', async () => {
    api.getCodexClientProfile.mockResolvedValue(view())
    const wrapper = mount(CodexClientProfileSettings, { global: { stubs: { Select: SelectStub } } })
    await flushPromises()
    await wrapper.find('[data-testid="codex-profile-preset"]').setValue('cli:linux')
    const input = wrapper.find('input')
    const event = new KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true })
    input.element.dispatchEvent(event)
    expect(event.defaultPrevented).toBe(true)
  })

  it('does not start polling when a running check reports after unmount', async () => {
    vi.useFakeTimers()
    let resolveLoad: (value: unknown) => void = () => {}
    api.getCodexClientProfile.mockReturnValueOnce(new Promise((resolve) => { resolveLoad = resolve }))
    api.getCodexClientProfile.mockResolvedValue(view({ refreshing: true }))
    const wrapper = mount(CodexClientProfileSettings, { global: { stubs: { Select: SelectStub } } })
    wrapper.unmount()
    resolveLoad(view({ refreshing: true }))
    await flushPromises()
    await vi.advanceTimersByTimeAsync(20000)
    expect(api.getCodexClientProfile).toHaveBeenCalledTimes(1)
  })
})
