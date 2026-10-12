import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Account } from '@/types'
import type { GatewayPoolProgress } from '@/api/admin/accounts'
import AccountCapacityCell from '../AccountCapacityCell.vue'
import CapacityBadge from '../CapacityBadge.vue'

vi.mock('vue-i18n', async (original) => ({
  ...(await original<typeof import('vue-i18n')>()),
  useI18n: () => ({ t: (key: string, params?: Record<string, unknown>) => (params ? `${key}:${JSON.stringify(params)}` : key) })
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
    await wrapper.setProps({ gatewayProgress: snapshot(7) })
    expect(badge().props('current')).toBe(7)
    expect(badge().props('colorClass')).toContain('bg-yellow-100')
    wrapper.unmount()
  })

  it('freezes the complete capacity display during blur and replaces it on resume', async () => {
    const wrapper = mount(AccountCapacityCell, { props: { account, gatewayProgress: snapshot(7) } })
    try {
      const frame = wrapper.html()
      await wrapper.setProps({ progressPaused: true })
      await wrapper.setProps({ account: { ...account, concurrency: 9 }, gatewayProgress: snapshot(100) })
      expect(wrapper.html()).toBe(frame)
      await wrapper.setProps({ progressPaused: false })
      expect(wrapper.findComponent(CapacityBadge).props('current')).toBe(100)
      expect(wrapper.findComponent(CapacityBadge).props('colorClass')).toContain('bg-red-100')
    } finally { wrapper.unmount() }
  })

  it('leaves non-pool capacity behavior unchanged', () => {
    const wrapper = mount(AccountCapacityCell, { props: { account: { ...account, extra: {}, current_concurrency: 3 } } })
    expect(wrapper.findComponent(CapacityBadge).props('current')).toBe(3)
    wrapper.unmount()
  })

  // Each case changes one rendered input: it must reach the live cell and stay out of a paused frame.
  it('renders every capacity input and keeps each one out of a paused frame', async () => {
    const anthropic = {
      id: 2, platform: 'anthropic', type: 'oauth', concurrency: 5, current_concurrency: 2, extra: {},
      window_cost_limit: 10, current_window_cost: 12, window_cost_sticky_reserve: 5,
      max_sessions: 4, active_sessions: 3, session_idle_timeout_minutes: 7,
      base_rpm: 10, current_rpm: 12, rpm_strategy: 'tiered', rpm_sticky_buffer: 3
    }
    const apikey = {
      id: 3, platform: 'openai', type: 'apikey', concurrency: 5, current_concurrency: 0, extra: {},
      quota_limit: 10, quota_used: 5, quota_daily_limit: 10, quota_daily_used: 5, quota_weekly_limit: 10, quota_weekly_used: 5
    }
    const pool = { ...account, concurrency: 50 }
    const limited = snapshot(7)
    const cases: Array<[string, Record<string, unknown>, Record<string, unknown>]> = [
      ...Object.entries({
        platform: 'openai', type: 'apikey', extra: { openai_gwpool: true }, concurrency: 6, current_concurrency: 3,
        window_cost_limit: 20, current_window_cost: 13, window_cost_sticky_reserve: 1,
        max_sessions: 5, active_sessions: 4, session_idle_timeout_minutes: 9,
        base_rpm: 20, current_rpm: 13, rpm_strategy: 'sticky_exempt', rpm_sticky_buffer: 1
      }).map(([key, value]): [string, Record<string, unknown>, Record<string, unknown>] =>
        [key, { account: anthropic }, { account: { ...anthropic, [key]: value } }]),
      ...Object.entries({
        quota_limit: 20, quota_used: 6, quota_daily_limit: 20, quota_daily_used: 6, quota_weekly_limit: 20, quota_weekly_used: 6
      }).map(([key, value]): [string, Record<string, unknown>, Record<string, unknown>] =>
        [key, { account: apikey }, { account: { ...apikey, [key]: value } }]),
      ['runtime.concurrency_limit', { account: pool, gatewayProgress: limited },
        { account: pool, gatewayProgress: { ...limited, runtime: { ...limited.runtime!, concurrency_limit: 9 } } }]
    ]
    for (const [label, base, next] of cases) {
      const wrapper = mount(AccountCapacityCell, { props: base as any })
      try {
        const frame = wrapper.html()
        await wrapper.setProps({ progressPaused: true })
        await wrapper.setProps(next)
        expect(wrapper.html(), `${label} leaked into the paused frame`).toBe(frame)
        await wrapper.setProps({ progressPaused: false })
        expect(wrapper.html(), `${label} is not rendered`).not.toBe(frame)
      } finally { wrapper.unmount() }
    }
  })
})
