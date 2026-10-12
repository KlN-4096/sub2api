import { defineComponent, nextTick } from 'vue'
import { mount } from '@vue/test-utils'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useSharedNowTicker } from '../useNowTicker'

afterEach(() => { vi.useRealTimers() })

describe('shared gateway clock', () => {
  it('uses one timer per cadence and disposes it only after the last subscriber', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(10000)
    const Cell = defineComponent({
      setup() { return { now: useSharedNowTicker(1000) } },
      template: '<div>{{ now }}</div>'
    })
    const baseline = vi.getTimerCount()
    const first = mount(Cell)
    const second = mount(Cell)
    expect(vi.getTimerCount()).toBe(baseline + 1)
    await vi.advanceTimersByTimeAsync(1000)
    await nextTick()
    expect(first.text()).toBe('11000')
    expect(second.text()).toBe('11000')
    first.unmount()
    expect(vi.getTimerCount()).toBe(baseline + 1)
    second.unmount()
    expect(vi.getTimerCount()).toBe(baseline)
    vi.setSystemTime(5000)
    const nextPage = mount(Cell)
    expect(nextPage.text()).toBe('5000')
    nextPage.unmount()
    expect(vi.getTimerCount()).toBe(baseline)
  })

  it('gives a subscriber joining a running clock the current time, not the last tick', () => {
    vi.useFakeTimers()
    vi.setSystemTime(10000)
    const Cell = defineComponent({
      setup() { return { now: useSharedNowTicker(30000) } },
      template: '<div>{{ now }}</div>'
    })
    const firstPage = mount(Cell)
    vi.setSystemTime(10500) // under a second stale: no bump, so mounted rows do not all re-render
    const scrolledIn = mount(Cell)
    expect(scrolledIn.text()).toBe('10000')
    vi.setSystemTime(39000) // a row mounted by paging 29s later, before the next tick
    const nextPage = mount(Cell)
    expect(nextPage.text()).toBe('39000')
    firstPage.unmount()
    scrolledIn.unmount()
    nextPage.unmount()
  })
})
