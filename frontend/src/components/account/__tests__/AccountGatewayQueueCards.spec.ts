import { afterEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountGatewayCell from '../AccountGatewayCell.vue'
import GatewayQueueCards from '../GatewayQueueCards.vue'
import GatewayUsedHistory from '../GatewayUsedHistory.vue'
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
  it('collapses distinct degraded tickets and keeps the last timed full window outside', async () => {
    const value = progress()
    const now = Date.now()
    const at = (seconds: number) => new Date(now - seconds * 1000).toISOString()
    value.runtime!.ledger_tag = 'member'
    value.runtime!.tickets = []
    value.runtime!.contacts = {
      ledger_tag: 'member', tracking_since: at(300),
      rounds: [
        { report: { id: 'full', gateway: 'unified-124', model: 'luna', source: 'foreground',
          at: at(300), outcome: 'full', window_final: true, full_window_ms: 124000 } },
        ...Array.from({ length: 34 }, (_, index) => ({
          version_hash: `ticket-${index}`, report: { id: `bad-${index}`, gateway: 'unified-120',
            model: 'luna', source: 'foreground', at: at(100 - index), outcome: 'refreshed' }
        }))
      ]
    }
    value.runtime!.history = { seen: {
      'unified-120': { at: at(10), verdict: 'degraded' },
      'unified-124': { at: at(176), full_at: at(300), full_held_ms: 124000,
        cooldown: { until: new Date(now + 65*60000).toISOString(), window_seconds: 3600 } }
    } }
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: value } })
    try {
      const summary = wrapper.get('[data-testid="gateway-used-degraded"]')
      expect(summary.text()).toContain('gatewayUsed.degradedTickets:{"count":34}')
      expect(summary.element.previousElementSibling?.textContent).toContain('gatewayUsed.recent')
      expect(wrapper.findAll('[data-testid="gateway-used-recent"]')).toHaveLength(1)
      expect(wrapper.get('[data-testid="gateway-used-duration"]').text()).toBe('admin.accounts.openai.gatewayUsed.heldFull:{"duration":"124s"}')
      expect(wrapper.get('[data-testid="gateway-used-recent"]').text()).toContain('unified-124')
      expect(wrapper.get('[data-testid="gateway-used-status"]').text()).toContain('"minutes":65')
      expect(wrapper.get('[data-testid="account-gateway-used"]').text()).not.toContain('fullUntimed')
      expect(wrapper.getComponent(GatewayUsedHistory).props('items')).toHaveLength(2)
      await wrapper.setProps({ progressPaused: true })
      const frozen = wrapper.get('[data-testid="account-gateway-used"]').html()
      await wrapper.setProps({ progress: { ...value, runtime: { ...value.runtime!, contacts: {
        ledger_tag: 'member', rounds: []
      } } } })
      expect(wrapper.get('[data-testid="account-gateway-used"]').html()).toBe(frozen)
      await wrapper.setProps({ progressPaused: false })
      expect(wrapper.find('[data-testid="gateway-used-degraded"]').exists()).toBe(false)
    } finally { wrapper.unmount() }
  })

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
    ['invalid deadline', { valid_until: 'not-a-date' }],
    ['invalid observation time', { observed_at: 'not-a-date' }],
    ['bad position', { quality: { count: 1, gateways: ['g'], positions: [0] } }],
    ['reversed order', { quality: { count: 2, gateways: ['a', 'b'], positions: [2, 1] } }],
    ['next is not a name', { next_gateway: 42 }],
    ['next is outside preview', { next_gateway: 'absent' }],
    ['next contradicts first position', {
      quality: { count: 3, gateways: ['unified-11', 'unified-26', 'unified-165'], positions: [1, 2, 3] },
      next_gateway: 'unified-26'
    }],
    ['duplicate global positions', {
      quality: { count: 1, gateways: ['a'], positions: [1] },
      ordinary: { count: 1, gateways: ['b'], positions: [1] }
    }],
    ['position beyond total', { quality: { count: 1, gateways: ['a'], positions: [999] } }]
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
      expect(wrapper.get('[data-testid="account-gateway-queues"]').classes()).toEqual(expect.arrayContaining(['rounded-md', 'bg-gray-50']))
      expect(wrapper.find('[data-testid="account-gateway-regions"]').exists()).toBe(false)
      expect(wrapper.get('[data-testid="account-gateway-current"]').text()).toContain('✓')
      for (const candidate of wrapper.findAll('[data-testid="gateway-queue-entry"]')) {
        expect(candidate.text()).not.toMatch(/[✓!]/)
        expect(candidate.attributes('title')).toContain('unified-')
        expect(candidate.html()).not.toMatch(/emerald|rose/)
      }
    } finally { wrapper.unmount() }
  })

  it('keeps expired inventory visible without treating it as a fresh or empty queue', async () => {
    vi.useFakeTimers()
    const value = progress()
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: value } })
    try {
      await vi.advanceTimersByTimeAsync(30_000)
      expect(wrapper.findAll('[data-testid="gateway-queue-count"]').map(node => node.text())).toEqual(['3', '42'])
      expect(wrapper.find('[data-testid="gateway-queue-stale"]').exists()).toBe(true)
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

  it('keeps individually used gateways with their history details visible', () => {
    const value = progress()
    value.runtime!.history = {
      current: 'unified-178',
      seen: {
        'unified-178': { at: new Date().toISOString(), region: 'east-asia', verdict: 'full', full_held_ms: 87000 },
        'unified-26': { at: new Date(Date.now() - 60_000).toISOString(), region: 'europe', verdict: 'degraded', full_held_ms: 43000 }
      }
    }
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: value } })
    try {
      const history = wrapper.get('[data-testid="account-gateway-used"]')
      expect(history.text()).toContain('26')
      expect(history.get('[data-gateway="unified-26"]').attributes('title')).toContain('43s')
      expect(history.get('[data-gateway="unified-26"]').attributes('data-tone')).toBe('degraded')
      expect(history.find('[data-gateway="unified-178"]').exists()).toBe(false)
      const current = wrapper.getComponent(GatewayUsedHistory).props('items')[0]
      expect(current.title).toContain('87s')
      expect(current.title).toContain('gatewayUsed.current')
      expect(current.duration).toBe('—')
    } finally { wrapper.unmount() }
  })

  it('shows the backend global positions and ordinary exploration as the next attempt', () => {
    const value = progress().runtime!.queues!
    value.quality.positions = [2, 3, 4]
    value.ordinary.positions = [1, 5, 6]
    value.next_gateway = 'unified-107'
    value.stale = true
    value.observed_at = new Date(Date.now() - 60_000).toISOString()
    const wrapper = mount(GatewayQueueCards, { props: { snapshot: value, now: Date.now() } })
    try {
      const next = wrapper.findAll('[data-testid="gateway-queue-entry"]').filter(node => node.attributes('data-next'))
      expect(next.map(node => node.get('[data-testid="gateway-queue-name"]').text())).toEqual(['107'])
      expect(next[0].attributes('title')).toContain('gatewayQueues.next:{"name":"unified-107"}')
      expect(wrapper.findAll('[data-testid="gateway-queue-entry"]').map(node => node.attributes('data-position')))
        .toEqual(['2', '3', '4', '1', '5', '6'])
      expect(wrapper.find('[data-testid="gateway-queue-stale"]').exists()).toBe(true)
    } finally { wrapper.unmount() }
  })

  it('keeps 102 untimed gateways in hover details without inventing ticket counts or outer rows', async () => {
    vi.useFakeTimers()
    const value = progress()
    value.runtime!.tickets = []
    value.runtime!.history = {
      seen: Object.fromEntries(Array.from({ length: 102 }, (_, i) => [`unified-${i}`, {
        at: new Date(Date.now() - i * 1000).toISOString(), region: 'east-asia', verdict: 'degraded'
      }]))
    }
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: value } })
    try {
      expect(wrapper.findAll('[data-testid="gateway-used-recent"]')).toHaveLength(0)
      expect(wrapper.find('[data-testid="gateway-used-degraded"]').exists()).toBe(false)
      expect(wrapper.find('[data-testid="gateway-used-expand"]').exists()).toBe(false)
      expect(wrapper.findAll('[data-testid="gateway-used-region"]')).toHaveLength(0)
      const details = wrapper.getComponent(GatewayUsedHistory)
      expect(details.props('items')).toHaveLength(102)
      expect(details.props('items').every(item => item.region === 'east-asia')).toBe(true)
      await wrapper.setProps({ progressPaused: true })
      const frozen = wrapper.get('[data-testid="account-gateway-used"]').html()
      await vi.advanceTimersByTimeAsync(3_600_000)
      expect(wrapper.get('[data-testid="account-gateway-used"]').html()).toBe(frozen)
      await wrapper.setProps({ account: { ...account, id: 2 }, progress: progress(), progressPaused: false })
      expect(wrapper.find('[data-testid="account-gateway-used"]').exists()).toBe(false)
      expect(wrapper.find('[data-region="east-asia"]').exists()).toBe(false)
    } finally { wrapper.unmount() }
  })

  it('excludes current tickets from the two-row timeline and keeps complete history in use order', () => {
    vi.useFakeTimers()
    const value = progress()
    const ago = (seconds: number) => new Date(Date.now() - seconds * 1000).toISOString()
    value.runtime!.tickets[0].region = 'us-east'
    value.runtime!.history = { seen: {
      'unified-178': { at: ago(15_000), region: 'us-east', verdict: 'full', full_held_ms: 87000 },
      'unified-101': { at: ago(10), region: 'country-us', verdict: 'degraded', full_held_ms: 43000, cooldown: { cleared: true } },
      'unified-201': { at: ago(7200), region: 'us-west', verdict: 'degraded', full_held_ms: 19000, cooldown: { until: ago(-1200), window_seconds: 3600 } },
      'unified-202': { at: ago(10_000), region: 'north-america', verdict: 'degraded', cooldown: { until: ago(-60), window_seconds: 3600 } },
      'unified-300': { at: ago(20_000), region: 'private-route' },
      invalid: { at: 'not-a-date', region: 'east-asia' }
    } }
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: value } })
    try {
      const recent = wrapper.findAll('[data-testid="gateway-used-recent"]')
      expect(recent.map(row => row.attributes('data-gateway'))).toEqual(['unified-101', 'unified-201'])
      expect(recent[0].element.previousElementSibling?.textContent).toContain('gatewayUsed.previous')
      expect(recent[1].element.previousElementSibling?.textContent).toContain('gatewayUsed.earlier')
      expect(recent[0].text()).toContain('unified-101')
      expect(recent[0].get('[data-testid="gateway-used-duration"]').text()).toContain('"duration":"43s"')
      expect(recent[0].get('[data-testid="gateway-used-status"]').text()).toContain('gatewayUsed.cooled')
      expect(recent[1].get('[data-testid="gateway-used-status"]').text()).toContain('"minutes":20')
      const details = wrapper.getComponent(GatewayUsedHistory).props('items')
      expect(details.filter(item => item.region === 'north-america').map(item => item.name))
        .toEqual(['unified-178', 'unified-101', 'unified-201', 'unified-202'])
      expect(details[0].title).toContain('87s')
      expect(details[0].duration).toBe('—')
      expect(details[0].title).toContain('gatewayUsed.current')
      expect(details[3].title).toContain('fullUntimed')
      expect(details.find(item => item.region === '')?.title).toContain('private-route')
      expect(wrapper.get('[data-testid="account-gateway-used"]').text()).not.toContain('invalid')
    } finally { wrapper.unmount() }
  })

  it('does not reuse current history as a previous ticket during verification or on a reused account row', async () => {
    const value = progress()
    value.phase = 'verifying'
    value.gateway = 'unified-178'
    value.runtime!.tickets[0].verified_models = []
    value.runtime!.history = { seen: {
      'unified-178': { at: new Date().toISOString(), full_held_ms: 99000 },
      'unified-26': { at: new Date(Date.now() - 60_000).toISOString() }
    } }
    const wrapper = mount(AccountGatewayCell, { props: { account, progress: value }, global: { stubs: { teleport: true } } })
    try {
      expect(wrapper.findAll('[data-testid="gateway-used-recent"]')).toHaveLength(0)
      expect(wrapper.find('[data-testid="gateway-used-duration"]').exists()).toBe(false)
      await wrapper.get('[data-testid="gateway-used-history-trigger"]').trigger('click')
      expect(wrapper.find('[data-testid="gateway-used-history-panel"]').exists()).toBe(true)
      const oldDetails = wrapper.get('[data-testid="gateway-used-history-panel"]').html()
      await wrapper.setProps({ progressPaused: true })
      const changed = { ...value, runtime: { ...value.runtime!, history: { seen: { 'unified-999': { at: new Date().toISOString() } } } } }
      await wrapper.setProps({ progress: changed })
      expect(wrapper.get('[data-testid="gateway-used-history-panel"]').html()).toBe(oldDetails)
      await wrapper.setProps({ account: { ...account, id: 2 }, progress: changed, progressPaused: false })
      expect(wrapper.find('[data-testid="gateway-used-history-panel"]').exists()).toBe(false)
      expect(wrapper.getComponent(GatewayUsedHistory).props('items').map(item => item.name)).toEqual(['unified-999'])
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
