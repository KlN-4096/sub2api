import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Account } from '@/types'
import type { GatewayPoolProgress } from '@/api/admin/accounts'
import AccountCapacityCell from '../AccountCapacityCell.vue'
import CapacityBadge from '../CapacityBadge.vue'

vi.mock('vue-i18n', async (original) => ({
  ...(await original<typeof import('vue-i18n')>()),
  useI18n: () => ({ t: (key: string) => key })
}))

const account = {
  id: 1, platform: 'openai', type: 'oauth', concurrency: 100,
  current_concurrency: 0, extra: { openai_gwpool: true }
} as unknown as Account

const snapshot = (count: number | null): GatewayPoolProgress => ({
  phase: 'idle', attempt: 0, limit: 0, rejected: 0, active_requests: 0,
  started_at: '', updated_at: '', elapsed_ms: 0,
  runtime: {
    observed_at: new Date().toISOString(), tickets: [], rounds: [], archived: null,
    current_concurrency: count, concurrency_limit: 100
  }
})

describe('gateway capacity snapshot', () => {
  it('uses the same fresh snapshot as gateways and distinguishes unknown from zero', async () => {
    const wrapper = mount(AccountCapacityCell, { props: { account, gatewayProgress: snapshot(7) } })
    const badge = () => wrapper.findComponent(CapacityBadge)
    expect(badge().props('current')).toBe(7)
    expect(account.current_concurrency).toBe(0)
    await wrapper.setProps({ gatewayProgress: snapshot(null) })
    expect(badge().props('current')).toBe('—')
    await wrapper.setProps({ gatewayProgress: snapshot(0) })
    expect(badge().props('current')).toBe(0)
    await wrapper.setProps({ gatewayProgress: snapshot(7), gatewayProgressUnavailable: true })
    expect(badge().props('current')).toBe(7)
    expect(badge().props('colorClass')).toContain('bg-gray-100')
    wrapper.unmount()
  })

  it('leaves non-pool capacity behavior unchanged', () => {
    const wrapper = mount(AccountCapacityCell, { props: { account: { ...account, extra: {}, current_concurrency: 3 } } })
    expect(wrapper.findComponent(CapacityBadge).props('current')).toBe(3)
    wrapper.unmount()
  })
})
