import { enableAutoUnmount, flushPromises, mount } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import TempUnschedStatusModal from '../TempUnschedStatusModal.vue'
import type { Account } from '@/types'
const mocks = vi.hoisted(() => ({ getTempUnschedulableStatus: vi.fn(), getGatewayPoolProgress: vi.fn(), recoverState: vi.fn(), showError: vi.fn() }))
vi.mock('@/api/admin', () => ({ adminAPI: { accounts: mocks } }))
vi.mock('@/api/admin/accounts', () => ({ getGatewayPoolProgress: mocks.getGatewayPoolProgress }))
vi.mock('@/stores/app', () => ({ useAppStore: () => mocks }))
vi.mock('@/utils/format', () => ({ formatDateTime: (value: Date) => value.toISOString() }))
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string, params?: unknown) => key + (params ? JSON.stringify(params) : '') }) }))
enableAutoUnmount(afterEach)
beforeEach(() => vi.clearAllMocks())
afterEach(() => vi.useRealTimers())
function deferred() {
  let resolve!: (value: unknown) => void
  let reject!: (value: unknown) => void
  const promise = new Promise((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}
const active = (message: string) => ({ active: true, gateway_pool_rest: message.startsWith('网关候选'),
  state: { until_unix: message.startsWith('网关候选') ? 0 : Date.now() / 1000 + 3600, error_message: message, rule_index: -1 } })
async function open() {
  const w = mount(TempUnschedStatusModal, { props: { show: false, account: { id: 1, name: 'first' } as Account },
    global: { stubs: { BaseDialog: { props: ['show'], template: '<div v-if="show"><slot /><slot name="footer" /></div>' } } } })
  await w.setProps({ show: true }); return w
}
describe('temporary unschedulable status requests', () => {
  it('shows a failed read as unknown rather than recovered', async () => {
    mocks.getTempUnschedulableStatus.mockRejectedValueOnce(new Error('unavailable'))
    const w = await open(); await flushPromises()
    expect(w.text()).toContain('admin.accounts.tempUnschedulable.failedToLoad')
    expect(w.text()).not.toContain('admin.accounts.tempUnschedulable.notActive')
    expect(w.get('button.btn-primary').attributes('disabled')).toBeDefined()
  })
  it('waits for server publication after a local deadline rather than claiming recovery in the browser', async () => {
    mocks.getTempUnschedulableStatus.mockResolvedValueOnce({ active: true, gateway_pool_rest: true,
      state: { until_unix: 1, triggered_at_unix: 1, error_message: '网关候选低于10，休息后达到40才恢复', rule_index: -1 } })
    mocks.getGatewayPoolProgress.mockResolvedValueOnce({ 1: { runtime: { cooldown_estimate: {
      resume_gateways: 40, eligible_at: '2020-01-01T00:00:00Z'
    } } } })
    const w = await open(); await flushPromises()
    expect(w.text()).not.toContain('admin.accounts.tempUnschedulable.notActive')
    expect(w.text()).toContain('admin.accounts.tempUnschedulable.cooldownFinished')
    expect(w.get('button.btn-primary').attributes('disabled')).toBeUndefined()
  })
  it('supports a legacy rest with only the local estimate', async () => {
    mocks.getTempUnschedulableStatus.mockResolvedValueOnce(active('网关候选低于10，休息后达到50才恢复'))
    mocks.getGatewayPoolProgress.mockResolvedValueOnce({ 1: { runtime: { cooldown_estimate: {
      resume_gateways: 50, eligible_at: new Date(Date.now() + 12 * 3600000).toISOString()
    } } } })
    const w = await open(); await flushPromises()
    expect(w.text()).toContain('admin.accounts.tempUnschedulable.cooldownUntil')
    expect(w.text()).toContain('admin.accounts.tempUnschedulable.remainingHours')
    expect(w.text()).not.toContain('admin.accounts.tempUnschedulable.until')
    expect(mocks.getGatewayPoolProgress).toHaveBeenCalledWith([1])
  })
  it('keeps an unknown estimate unknown instead of using the recheck deadline', async () => {
    mocks.getTempUnschedulableStatus.mockResolvedValueOnce(active('网关候选低于10，休息后达到50才恢复'))
    mocks.getGatewayPoolProgress.mockRejectedValueOnce(new Error('unavailable'))
    const w = await open(); await flushPromises()
    expect(w.text()).toContain('admin.accounts.tempUnschedulable.cooldownUnknown')
    expect(w.get('button.btn-primary').attributes('disabled')).toBeUndefined()
  })
  it('updates the remaining time and waits for state synchronization at the local deadline', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-10-06T12:00:00Z'))
    mocks.getTempUnschedulableStatus.mockResolvedValueOnce(active('网关候选低于10，休息后达到50才恢复'))
    mocks.getGatewayPoolProgress.mockResolvedValueOnce({ 1: { runtime: { cooldown_estimate: {
      resume_gateways: 50, eligible_at: '2026-10-06T12:01:01Z'
    } } } })
    const w = await open(); await flushPromises()
    expect(w.text()).toContain('remainingMinutes{"minutes":2}')
    await vi.advanceTimersByTimeAsync(1000)
    expect(w.text()).toContain('remainingMinutes{"minutes":1}')
    await vi.advanceTimersByTimeAsync(60000)
    expect(w.text()).toContain('admin.accounts.tempUnschedulable.cooldownFinished')
    expect(w.get('button.btn-primary').attributes('disabled')).toBeUndefined()
  })
  it('prefers the persisted resume deadline over a retroactively calculated estimate', async () => {
    const deadline = Date.parse('2099-01-01T12:00:00Z') / 1000
    mocks.getTempUnschedulableStatus.mockResolvedValueOnce({ ...active('网关候选低于10，按40个本地冷却截止恢复'),
      state: { until_unix: deadline, triggered_at_unix: 0, error_message: 'local rest', rule_index: -1 } })
    mocks.getGatewayPoolProgress.mockResolvedValueOnce({ 1: { runtime: { cooldown_estimate: {
      resume_gateways: 40, eligible_at: '2020-01-01T00:00:00Z'
    } } } })
    const w = await open(); await flushPromises()
    expect(w.text()).toContain('2099-01-01T12:00:00.000Z')
    expect(w.text()).not.toContain('2020-01-01')
  })
  it('does not let a late cooldown snapshot replace a different account', async () => {
    const old = deferred()
    mocks.getTempUnschedulableStatus.mockResolvedValueOnce(active('网关候选低于10，休息后达到50才恢复'))
      .mockResolvedValueOnce(active('current-error'))
    mocks.getGatewayPoolProgress.mockReturnValueOnce(old.promise)
    const w = await open(); await flushPromises()
    await w.setProps({ account: { id: 2, name: 'second' } as Account }); await flushPromises()
    old.resolve({ 1: { runtime: { cooldown_estimate: { resume_gateways: 50, eligible_at: '2099-01-01T00:00:00Z' } } } })
    await flushPromises()
    expect(w.text()).toContain('current-error')
    expect(w.text()).not.toContain('2099')
    expect(w.text()).not.toContain('cooldownUntil')
  })
  it('does not let a late response replace the next account status', async () => {
    const old = deferred()
    mocks.getTempUnschedulableStatus.mockReturnValueOnce(old.promise).mockResolvedValueOnce(active('current-error'))
    const w = await open(); await w.setProps({ account: { id: 2, name: 'second' } as Account }); await flushPromises()
    old.resolve(active('old-error')); await flushPromises()
    expect(w.text()).toContain('current-error'); expect(w.text()).not.toContain('old-error')
  })
  it('keeps recovery disabled while loading a different account', async () => {
    const current = deferred()
    mocks.getTempUnschedulableStatus.mockResolvedValueOnce(active('old-error')).mockReturnValueOnce(current.promise)
    const w = await open(); await flushPromises()
    await w.setProps({ account: { id: 2 } as Account })
    expect(w.get('button.btn-primary').attributes('disabled')).toBeDefined()
    current.resolve(active('current-error')); await flushPromises()
    expect(w.get('button.btn-primary').attributes('disabled')).toBeUndefined()
  })
  it('ignores old errors without dismissing the current loading state', async () => {
    const old = deferred(); const current = deferred()
    mocks.getTempUnschedulableStatus.mockReturnValueOnce(old.promise).mockReturnValueOnce(current.promise)
    const w = await open(); await w.setProps({ show: false }); await w.setProps({ show: true })
    old.reject(new Error('obsolete')); await flushPromises()
    expect(mocks.showError).not.toHaveBeenCalled(); expect(w.find('.animate-spin').exists()).toBe(true)
    current.resolve({ active: false }); await flushPromises()
    expect(w.text()).toContain('admin.accounts.tempUnschedulable.notActive')
  })
})
