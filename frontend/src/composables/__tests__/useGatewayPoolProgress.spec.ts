import { afterEach, describe, expect, it, vi } from 'vitest'
import { computed, defineComponent, ref } from 'vue'
import { flushPromises, mount } from '@vue/test-utils'
import { getGatewayPoolProgress } from '@/api/admin/accounts'
import { useGatewayPoolProgress } from '../useGatewayPoolProgress'

vi.mock('@/api/admin/accounts', () => ({ getGatewayPoolProgress: vi.fn() }))

describe('gateway progress polling', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('polls visible IDs without overlap and clears stale progress on failure/unmount', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    const ids = ref([1])
    const progress = { phase: 'verifying', attempt: 2, limit: 5 }
    vi.mocked(getGatewayPoolProgress).mockResolvedValueOnce({ 1: progress } as any)
      .mockRejectedValueOnce(new Error('offline'))
    let state: ReturnType<typeof useGatewayPoolProgress>
    const wrapper = mount(defineComponent({
      setup() { state = useGatewayPoolProgress(computed(() => ids.value)); return () => null }
    }))
    await flushPromises()
    expect(state!.progress.value[1]).toEqual(progress)
    expect(getGatewayPoolProgress).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(2000)
    await flushPromises()
    expect(state!.progress.value).toEqual({})
    expect(state!.unavailable.value).toBe(true)
    wrapper.unmount()
    await vi.advanceTimersByTimeAsync(10000)
    expect(getGatewayPoolProgress).toHaveBeenCalledTimes(2)
  })

  it('does not poll a hidden page or an empty target set', async () => {
    vi.useFakeTimers()
    vi.mocked(getGatewayPoolProgress).mockClear()
    const hidden = vi.spyOn(document, 'hidden', 'get').mockReturnValue(true)
    const ids = ref([1])
    const wrapper = mount(defineComponent({
      setup() { useGatewayPoolProgress(computed(() => ids.value)); return () => null }
    }))
    await flushPromises()
    expect(getGatewayPoolProgress).not.toHaveBeenCalled()
    ids.value = []
    hidden.mockReturnValue(false)
    document.dispatchEvent(new Event('visibilitychange'))
    await flushPromises()
    expect(getGatewayPoolProgress).not.toHaveBeenCalled()
    wrapper.unmount()
  })
})
