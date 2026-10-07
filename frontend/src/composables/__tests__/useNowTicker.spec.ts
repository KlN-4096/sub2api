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
})
