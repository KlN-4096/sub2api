import { describe, expect, it, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { reactive } from 'vue'

import VersionBadge from '../VersionBadge.vue'

const appStore = reactive({
  versionLoading: false,
  currentVersion: '0.2.7-klno.4',
  latestVersion: '0.2.7-klno.4',
  hasUpdate: false,
  releaseInfo: null as null | Record<string, string>,
  buildType: 'release',
  upstreamVersion: null as null | Record<string, unknown>,
  fetchVersion: vi.fn()
})
const authStore = reactive({ isAdmin: true })

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string, params?: Record<string, string>) =>
        params?.version ? `${key}:${params.version}` : key
    })
  }
})

vi.mock('@/stores', () => ({
  useAppStore: () => appStore,
  useAuthStore: () => authStore
}))

vi.mock('@/composables/useClipboard', () => ({
  useClipboard: () => ({ copied: false, copyToClipboard: vi.fn() })
}))

describe('VersionBadge', () => {
  beforeEach(() => {
    authStore.isAdmin = true
    appStore.currentVersion = '0.2.7-klno.4'
    appStore.hasUpdate = false
    appStore.upstreamVersion = null
  })

  it('shows upstream X.Y.Z and the fork klno sequence; an upstream release lights the original amber ping', () => {
    appStore.upstreamVersion = {
      current_version: '0.2.7',
      latest_version: '0.2.8',
      has_update: true,
      html_url: 'https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.8'
    }
    const wrapper = mount(VersionBadge, { global: { stubs: { Icon: true } } })

    const upstream = wrapper.get('[data-testid="version-upstream"]')
    expect(upstream.element.tagName).toBe('A')
    expect(upstream.attributes('href')).toBe(
      'https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.8'
    )
    expect(upstream.text()).toBe('v0.2.7')
    expect(upstream.attributes('title')).toBe(
      'version.upstreamLabel v0.2.7：version.upstreamUpdateAvailable:0.2.8'
    )
    expect(upstream.find('.animate-ping').exists()).toBe(true)

    const fork = wrapper.get('[data-testid="version-fork"]')
    expect(fork.text()).toBe('klno.4')
    expect(fork.attributes('title')).toBe('version.forkLabel v0.2.7-klno.4：version.upToDate')
  })

  it('derives the upstream version before the update check returns', () => {
    const wrapper = mount(VersionBadge, { global: { stubs: { Icon: true } } })

    const upstream = wrapper.get('[data-testid="version-upstream"]')
    expect(upstream.text()).toBe('v0.2.7')
    expect(upstream.attributes('href')).toBe('https://github.com/Wei-Shaw/sub2api/releases')
    expect(upstream.find('.animate-ping').exists()).toBe(false)
  })

  it('shows the full version on builds without a klno suffix', () => {
    appStore.currentVersion = '0.2.8'
    const wrapper = mount(VersionBadge, { global: { stubs: { Icon: true } } })

    expect(wrapper.get('[data-testid="version-fork"]').text()).toBe('v0.2.8')
    expect(wrapper.get('[data-testid="version-upstream"]').text()).toBe('v0.2.8')
  })

  it('manual rollback commands point at the fork, not upstream', async () => {
    const wrapper = mount(VersionBadge, { global: { stubs: { Icon: true } } })
    const vm = wrapper.vm as unknown as {
      selectedRollbackVersion: string
      scriptRollbackCommand: string
      dockerRollbackCommand: string
    }
    vm.selectedRollbackVersion = '0.2.7-klno.3'
    await wrapper.vm.$nextTick()

    expect(vm.scriptRollbackCommand).toContain('raw.githubusercontent.com/KlN-4096/sub2api/v0.2.7-klno.3/')
    expect(vm.dockerRollbackCommand).toContain('image: ghcr.io/kln-4096/sub2api:0.2.7-klno.3')
  })
})
