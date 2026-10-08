import { afterEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountGatewayCell from '../AccountGatewayCell.vue'
import GatewayQueueCards from '../GatewayQueueCards.vue'
import type { Account } from '@/types'
import type { GatewayPoolProgress, GatewayPoolQueueView } from '@/api/admin/accounts'

vi.mock('vue-i18n', async importOriginal => ({
  ...(await importOriginal<typeof import('vue-i18n')>()),
  useI18n: () => ({
    t: (key: string, params?: Record<string, unknown>) => params ? `${key}:${JSON.stringify(params)}` : key
  })
}))

afterEach(() => vi.useRealTimers())

const account = {
  id: 1, platform: 'openai', type: 'oauth',
  extra: { openai_gwpool: true, openai_gwpool_gateways: { pool_free: 99, pool_live: 100 } }
} as unknown as Account

function progress(): GatewayPoolProgress {
  const at = new Date().toISOString()
  return {
    phase: 'ready', attempt: 1, limit: 0, rejected: 0, elapsed_ms: 1000,
    active_requests: 1, started_at: at, updated_at: at,
    runtime: {
      observed_at: at, history: {}, rounds: [], archived: null,
      tickets: [{ gateway: 'unified-178', region: 'east-asia', verified_models: ['gpt-6-luna'], verified_at: at }],
      queues: {
        model: 'gpt-6-luna', valid_until: new Date(Date.now() + 30_000).toISOString(),
        quality: { count: 3, gateways: ['unified-11', 'unified-26', 'unified-165'] },
        ordinary: { count: 42, gateways: ['unified-107', 'unified-134', 'unified-156'] }
      }
    }
  }
}

describe('gateway compact queue cards', () => {
  it('hides queue snapshots on non-pool accounts even when runtime data is present', () => {
    const wrapper = mount(AccountGatewayCell, {
      props: { account: { ...account, extra: { ...account.extra, openai_gwpool: false } }, progress: progress() }
    })
    try {
      expect(wrapper.find('[data-testid="account-gateway-queues"]').exists()).toBe(false)
      expect(wrapper.findAll('[data-testid="gateway-queue-card"]')).toHaveLength(0)
    } finally { wrapper.unmount() }
  })

  it.each([
    ['negative count', { quality: { count: -1, gateways: [] } }],
    ['non-finite count', { quality: { count: NaN, gateways: [] } }],
    ['unsafe count', { quality: { count: Number.MAX_SAFE_INTEGER + 1, gateways: ['a', 'b', 'c'] } }],
    ['short sample', { quality: { count: 3, gateways: ['a'] } }],
    ['missing sample', { quality: { count: 3 } }],
    ['blank name', { quality: { count: 1, gateways: [' '] } }],
    ['non-string name', { quality: { count: 1, gateways: [42] } }],
    ['duplicate sample', { quality: { count: 2, gateways: ['a', 'a'] } }],
    ['cross-group duplicate', { quality: { count: 1, gateways: ['unified-107'] } }],
    ['missing model', { model: '' }],
    ['invalid deadline', { valid_until: 'not-a-date' }]
  ])('keeps a malformed %s unknown instead of inventing a queue count', (_name, invalid) => {
    const snapshot = { ...progress().runtime!.queues!, ...invalid } as GatewayPoolQueueView
    const wrapper = mount(GatewayQueueCards, { props: { snapshot, now: Date.now() } })
    try {
      expect(wrapper.findAll('[data-testid="gateway-queue-count"]').map(node => node.text())).toEqual(['—', '—'])
      expect(wrapper.findAll('[data-testid="gateway-queue-name"]')).toHaveLength(0)
    } finally { wrapper.unmount() }
  })

  it('shows two compact card rows from backend classification, not region history', () => {
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: progress() } })
    try {
      const rows = wrapper.findAll('[data-testid="gateway-queue-card"]')
      expect(rows).toHaveLength(2)
      expect(rows[0].get('[data-testid="gateway-queue-count"]').text()).toBe('3')
      expect(rows[1].get('[data-testid="gateway-queue-count"]').text()).toBe('42')
      expect(rows[0].findAll('[data-testid="gateway-queue-name"]').map(node => node.text())).toEqual(['11', '26', '165'])
      expect(rows[1].get('[data-testid="gateway-queue-more"]').text()).toBe('+39')
      expect(rows.every(row => row.classes().includes('rounded-md') && row.classes().includes('bg-gray-100'))).toBe(true)
      expect(wrapper.find('[data-testid="account-gateway-regions"]').exists()).toBe(false)
      expect(wrapper.get('[data-testid="account-gateway-current"]').text()).toContain('✓')
      for (const candidate of wrapper.findAll('[data-testid="gateway-queue-name"]')) {
        expect(candidate.text()).not.toMatch(/[✓!]/)
        expect(candidate.attributes('title')).toContain('unified-')
        expect(candidate.classes().join(' ')).not.toMatch(/emerald|rose/)
      }
    } finally { wrapper.unmount() }
  })

  it('distinguishes unknown and expired inventory from a confirmed empty queue', async () => {
    vi.useFakeTimers()
    const value = progress()
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: value } })
    try {
      await vi.advanceTimersByTimeAsync(30_000)
      expect(wrapper.findAll('[data-testid="gateway-queue-count"]').map(node => node.text())).toEqual(['—', '—'])
      expect(wrapper.get('[data-testid="account-gateway-current"]').text()).toContain('✓')
      for (const queues of [null, undefined]) {
        await wrapper.setProps({ progress: { ...value, runtime: { ...value.runtime!, queues } } })
        expect(wrapper.findAll('[data-testid="gateway-queue-count"]').map(node => node.text())).toEqual(['—', '—'])
        expect(wrapper.text()).not.toContain('99')
      }
      await wrapper.setProps({ progress: { ...value, runtime: { ...value.runtime!, queues: {
        model: 'gpt-6-luna', valid_until: new Date(Date.now() + 30_000).toISOString(),
        quality: { count: 0, gateways: [] }, ordinary: { count: 0, gateways: [] }
      } } } })
      expect(wrapper.findAll('[data-testid="gateway-queue-count"]').map(node => node.text())).toEqual(['0', '0'])
    } finally { wrapper.unmount() }
  })

  it('freezes queue data and time on blur, then switches scope with a reused account row', async () => {
    vi.useFakeTimers()
    const value = progress()
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: value } })
    try {
      await wrapper.setProps({ progressPaused: true })
      const frame = wrapper.get('[data-testid="account-gateway-queues"]').html()
      value.runtime!.queues!.quality.gateways[0] = 'unified-999'
      value.runtime!.queues!.quality.count = 999
      await vi.advanceTimersByTimeAsync(60_000)
      expect(wrapper.get('[data-testid="account-gateway-queues"]').html()).toBe(frame)
      await wrapper.setProps({ progressPaused: false, progress: { ...value, runtime: { ...value.runtime!, queues: null } } })
      expect(wrapper.findAll('[data-testid="gateway-queue-count"]').map(node => node.text())).toEqual(['—', '—'])
      await wrapper.setProps({ account: { ...account, id: 2 }, progress: undefined })
      expect(wrapper.text()).not.toContain('999')
      expect(wrapper.findAll('[data-testid="gateway-queue-count"]').map(node => node.text())).toEqual(['—', '—'])
    } finally { wrapper.unmount() }
  })
})
