import { afterEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import GatewayUsedHistory from '../GatewayUsedHistory.vue'

vi.mock('vue-i18n', async importOriginal => ({
  ...(await importOriginal<typeof import('vue-i18n')>()),
  useI18n: () => ({ t: (key: string) => key })
}))
afterEach(() => vi.useRealTimers())
function items(count = 102) {
  return Array.from({ length: count }, (_, index) => ({
    name: `unified-${index}`, region: index % 2 ? 'europe' : 'east-asia',
    title: `detail-${index}`, duration: '43s', status: 'CD 44m', tone: 'degraded' as const
  }))
}
const selector = (name: string) => `[data-testid="gateway-used-history-${name}"]`

describe('gateway history hover', () => {
  it('opens on hover, allows pointer transfer, and cancels delayed closure on entry', async () => {
    vi.useFakeTimers()
    const wrapper = mount(GatewayUsedHistory, { props: { items: items() }, global: { stubs: { teleport: true } } })
    try {
      expect(wrapper.find(selector('panel')).exists()).toBe(false)
      await wrapper.get(selector('trigger')).trigger('mouseenter')
      expect(wrapper.get(selector('trigger')).attributes('aria-expanded')).toBe('true')
      await wrapper.get(selector('trigger')).trigger('mouseleave')
      await wrapper.get(selector('panel')).trigger('mouseenter')
      await vi.advanceTimersByTimeAsync(200)
      expect(wrapper.find(selector('panel')).exists()).toBe(true)
      await wrapper.get(selector('panel')).trigger('mouseleave')
      await vi.advanceTimersByTimeAsync(200)
      expect(wrapper.find(selector('panel')).exists()).toBe(false)
    } finally { wrapper.unmount() }
  })

  it('pins on click, browses all 102 records, filters by region, and clamps pages after data changes', async () => {
    vi.useFakeTimers()
    const wrapper = mount(GatewayUsedHistory, { props: { items: items() }, global: { stubs: { teleport: true } } })
    try {
      await wrapper.get(selector('trigger')).trigger('click')
      await wrapper.get(selector('trigger')).trigger('mouseleave')
      await vi.advanceTimersByTimeAsync(200)
      expect(wrapper.find(selector('panel')).exists()).toBe(true)
      expect(wrapper.get(selector('page')).text()).toBe('1/26 · 102')
      expect(wrapper.findAll(selector('entry'))).toHaveLength(4)
      expect(wrapper.findAll(selector('entry'))[0].text()).toContain('unified-0')
      expect(wrapper.get(selector('prev')).attributes('disabled')).toBeDefined()
      for (let page = 1; page < 26; page++) await wrapper.get(selector('next')).trigger('click')
      expect(wrapper.get(selector('page')).text()).toBe('26/26 · 102')
      expect(wrapper.findAll(selector('entry')).map(row => row.attributes('data-gateway'))).toEqual(['unified-100', 'unified-101'])
      expect(wrapper.get(selector('next')).attributes('disabled')).toBeDefined()
      await wrapper.get(selector('region')).setValue('europe')
      expect(wrapper.get(selector('page')).text()).toBe('1/13 · 51')
      expect(wrapper.findAll(selector('entry')).map(row => row.attributes('data-gateway'))).toEqual(['unified-1', 'unified-3', 'unified-5', 'unified-7'])
      await wrapper.get(selector('next')).trigger('click')
      await wrapper.setProps({ items: items(1) })
      expect(wrapper.get(selector('region')).element).toHaveProperty('value', 'all')
      expect(wrapper.get(selector('page')).text()).toBe('1/1 · 1')
      await wrapper.get(selector('close')).trigger('click')
      expect(wrapper.find(selector('panel')).exists()).toBe(false)
    } finally { wrapper.unmount() }
  })

  it('pins a hovered panel once its controls receive interaction', async () => {
    vi.useFakeTimers()
    const wrapper = mount(GatewayUsedHistory, { props: { items: items() }, global: { stubs: { teleport: true } } })
    try {
      await wrapper.get(selector('trigger')).trigger('mouseenter')
      await wrapper.get(selector('panel')).trigger('pointerdown')
      await wrapper.get(selector('panel')).trigger('mouseleave')
      await vi.advanceTimersByTimeAsync(200)
      expect(wrapper.find(selector('panel')).exists()).toBe(true)
    } finally { wrapper.unmount() }
  })

  it('supports Escape and outside clicks, and removes a teleported panel on unmount', async () => {
    const wrapper = mount(GatewayUsedHistory, { props: { items: items(1) }, attachTo: document.body })
    try {
      await wrapper.get(selector('trigger')).trigger('click')
      expect(document.body.querySelector(selector('panel'))).not.toBeNull()
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
      await wrapper.vm.$nextTick()
      expect(document.body.querySelector(selector('panel'))).toBeNull()
      expect(document.activeElement).toBe(wrapper.get(selector('trigger')).element)
      await wrapper.get(selector('trigger')).trigger('click')
      document.body.dispatchEvent(new Event('pointerdown', { bubbles: true }))
      await wrapper.vm.$nextTick()
      expect(document.body.querySelector(selector('panel'))).toBeNull()
      await wrapper.get(selector('trigger')).trigger('click')
    } finally { wrapper.unmount() }
    expect(document.body.querySelector(selector('panel'))).toBeNull()
  })
})
