import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { defineComponent } from 'vue'
import { flushPromises, mount } from '@vue/test-utils'

const { previewCodexClientProfile } = vi.hoisted(() => ({ previewCodexClientProfile: vi.fn() }))
vi.mock('@/api/admin/codexClientProfile', () => ({ previewCodexClientProfile }))
vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return { ...actual, useI18n: () => ({ t: (key: string) => key }) }
})

import CodexClientProfileFields from '../CodexClientProfileFields.vue'

const SelectStub = defineComponent({
  name: 'SelectStub',
  props: { modelValue: { type: [String, Number], default: '' }, options: { type: Array, default: () => [] } },
  emits: ['update:modelValue'],
  template: `<select :value="modelValue" @change="$emit('update:modelValue', $event.target.value)">
    <option v-for="o in options" :key="o.value" :value="o.value">{{ o.label }}</option></select>`,
})

const presets = [
  { profile: { client: 'desktop', platform: 'macos', versionMode: 'latest' }, originator: 'Codex Desktop', os_type: 'Mac OS', os_version: '26.7.1', arch: 'arm64', terminal: 'unknown' },
  { profile: { client: 'cli', platform: 'linux', versionMode: 'latest' }, originator: 'codex_cli_rs', os_type: 'Ubuntu', os_version: '26.4.0', arch: 'x86_64', terminal: 'unknown' },
]

function mountFields(modelValue: unknown = null, extra: Record<string, unknown> = {}) {
  return mount(CodexClientProfileFields, {
    props: {
      modelValue: modelValue as never, presets: presets as never, maxVersionLag: 10,
      title: 'T', description: 'D', inheritLabel: 'Inherit', inheritPreview: 'global-ua', ...extra,
    },
    global: { stubs: { Select: SelectStub } },
  })
}

const lastEmitted = (wrapper: ReturnType<typeof mountFields>) => {
  const events = wrapper.emitted('update:modelValue') ?? []
  return events[events.length - 1]?.[0]
}

describe('CodexClientProfileFields', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    previewCodexClientProfile.mockReset()
    previewCodexClientProfile.mockResolvedValue({ user_agent: 'codex_exec/0.160.0 (Ubuntu 26.4.0; x86_64) unknown (codex_exec; 0.160.0)' })
  })
  afterEach(() => vi.useRealTimers())

  it('shows the inherited identity until a preset is chosen', () => {
    const wrapper = mountFields()
    expect(wrapper.find('[data-testid="codex-profile-preview"]').text()).toBe('global-ua')
    expect(wrapper.find('[data-testid="codex-profile-version-mode"]').exists()).toBe(false)
    expect(previewCodexClientProfile).not.toHaveBeenCalled()
  })

  it('offers a custom UA entry only when asked, exclusive with the presets', async () => {
    const optionValues = (w: ReturnType<typeof mountFields>) => w.findAll('option').map((o) => (o.element as HTMLOptionElement).value)
    expect(optionValues(mountFields())).not.toContain('custom')

    const wrapper = mountFields({ client: 'cli', platform: 'linux', versionMode: 'latest' }, { customLabel: 'Custom' })
    expect(optionValues(wrapper)).toContain('custom')
    const select = () => wrapper.find('[data-testid="codex-profile-preset"]')

    await select().setValue('custom')
    expect(wrapper.emitted('update:custom')?.at(-1)).toEqual([true])
    expect(lastEmitted(wrapper)).toBeNull()
    await wrapper.setProps({ custom: true })
    expect((select().element as HTMLSelectElement).value).toBe('custom')

    await select().setValue('desktop:macos')
    expect(wrapper.emitted('update:custom')?.at(-1)).toEqual([false])
    expect(lastEmitted(wrapper)).toEqual({ client: 'desktop', platform: 'macos', versionMode: 'latest' })

    await wrapper.setProps({ custom: true, modelValue: null })
    await select().setValue('')
    expect(wrapper.emitted('update:custom')?.at(-1)).toEqual([false])
  })

  it('builds the selection document and previews it', async () => {
    const wrapper = mountFields()
    await wrapper.find('[data-testid="codex-profile-preset"]').setValue('cli:linux')
    expect(lastEmitted(wrapper)).toEqual({ client: 'cli', platform: 'linux', versionMode: 'latest' })

    await wrapper.find('[data-testid="codex-profile-cli-entry"]').setValue('exec')
    await wrapper.find('[data-testid="codex-profile-version-lag"]').setValue(2)
    expect(lastEmitted(wrapper)).toEqual({ client: 'cli', platform: 'linux', cliEntry: 'exec', versionMode: 'latest', versionLag: 2 })

    await wrapper.find('[data-testid="codex-profile-version-mode"]').setValue('fixed')
    await wrapper.find('[data-testid="codex-profile-codex-version"]').setValue('0.160.0')
    await wrapper.find('[data-testid="codex-profile-osType"]').setValue('Ubuntu')
    const fixed = { client: 'cli', platform: 'linux', cliEntry: 'exec', versionMode: 'fixed', codexVersion: '0.160.0', osType: 'Ubuntu' }
    expect(lastEmitted(wrapper)).toEqual(fixed)

    await vi.advanceTimersByTimeAsync(300)
    expect(previewCodexClientProfile).toHaveBeenCalledTimes(1)
    expect(previewCodexClientProfile).toHaveBeenCalledWith(fixed)
    expect(wrapper.find('[data-testid="codex-profile-preview"]').text()).toContain('codex_exec/0.160.0')
  })

  it('switching the preset starts from that preset defaults', async () => {
    const wrapper = mountFields({ client: 'cli', platform: 'linux', versionMode: 'fixed', codexVersion: '0.160.0', osType: 'Ubuntu' })
    expect((wrapper.find('[data-testid="codex-profile-osType"]').element as HTMLInputElement).value).toBe('Ubuntu')
    await wrapper.find('[data-testid="codex-profile-preset"]').setValue('desktop:macos')
    expect(lastEmitted(wrapper)).toEqual({ client: 'desktop', platform: 'macos', versionMode: 'latest' })
    expect(wrapper.find('[data-testid="codex-profile-cli-entry"]').exists()).toBe(false)

    await wrapper.find('[data-testid="codex-profile-preset"]').setValue('')
    expect(lastEmitted(wrapper)).toBeNull()
  })

  it('ignores a preview that lands after switching back to inherit', async () => {
    let resolvePreview: (value: { user_agent: string }) => void = () => {}
    previewCodexClientProfile.mockReturnValueOnce(new Promise((resolve) => { resolvePreview = resolve }))
    const wrapper = mountFields({ client: 'cli', platform: 'linux', versionMode: 'latest' })
    await vi.advanceTimersByTimeAsync(300)
    expect(previewCodexClientProfile).toHaveBeenCalledTimes(1)

    await wrapper.find('[data-testid="codex-profile-preset"]').setValue('')
    await wrapper.setProps({ modelValue: null })
    resolvePreview({ user_agent: 'stale-ua' })
    await flushPromises()
    expect(wrapper.find('[data-testid="codex-profile-preview"]').text()).toBe('global-ua')
  })

  it('shows preview errors from the server', async () => {
    previewCodexClientProfile.mockRejectedValue({ message: 'codexVersion must be a Codex core version not below 0.144.0' })
    const wrapper = mountFields({ client: 'cli', platform: 'linux', versionMode: 'fixed', codexVersion: '0.140.0' })
    await vi.advanceTimersByTimeAsync(300)
    expect(wrapper.find('[data-testid="codex-profile-preview"]').text()).toContain('not below 0.144.0')
  })
})
