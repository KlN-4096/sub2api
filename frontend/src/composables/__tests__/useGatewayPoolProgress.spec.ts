import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { computed, defineComponent, ref } from 'vue'
import { flushPromises, mount } from '@vue/test-utils'
import { getGatewayPoolProgress, type GatewayPoolProgress } from '@/api/admin/accounts'
import { useGatewayPoolProgress } from '../useGatewayPoolProgress'

vi.mock('@/api/admin/accounts', () => ({ getGatewayPoolProgress: vi.fn() }))

const snapshot = (values: Partial<GatewayPoolProgress> = {}): GatewayPoolProgress => ({
  phase: 'idle', attempt: 0, limit: 0, rejected: 0, elapsed_ms: 0, active_requests: 0,
  started_at: '', updated_at: '', runtime: {
    observed_at: new Date().toISOString(), tickets: [], rounds: [], archived: null, rest: { active: false }
  }, ...values
})

describe('gateway progress polling', () => {
  beforeEach(() => {
    vi.mocked(getGatewayPoolProgress).mockReset()
    vi.spyOn(document, 'hasFocus').mockReturnValue(true)
  })
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('polls every second and retains the last snapshot unchanged on failure', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    const ids = ref([1])
    const progress = snapshot({ phase: 'verifying', attempt: 2, limit: 5 })
    vi.mocked(getGatewayPoolProgress).mockResolvedValueOnce({ 1: progress } as any)
      .mockRejectedValueOnce(new Error('offline'))
    let state: ReturnType<typeof useGatewayPoolProgress>
    const wrapper = mount(defineComponent({
      setup() { state = useGatewayPoolProgress(computed(() => ids.value)); return () => null }
    }))
    await flushPromises()
    expect(state!.progress.value[1]).toEqual(progress)
    expect(getGatewayPoolProgress).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1000)
    await flushPromises()
    expect(state!.progress.value[1]).toEqual(progress)
    expect(state!.paused.value).toBe(false)
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

  it('pauses on blur without fabricating a failure and resumes with one complete snapshot', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    const focus = vi.mocked(document.hasFocus)
    const first = snapshot({ phase: 'ready', attempt: 2 })
    let resolveNext!: (value: any) => void
    vi.mocked(getGatewayPoolProgress).mockResolvedValueOnce({ 1: first } as any)
      .mockImplementationOnce(() => new Promise(resolve => { resolveNext = resolve }))
    let state: ReturnType<typeof useGatewayPoolProgress>
    const wrapper = mount(defineComponent({
      setup() { state = useGatewayPoolProgress(computed(() => [1])); return () => null }
    }))
    await flushPromises()
    expect(state!.progress.value[1]).toEqual(first)
    focus.mockReturnValue(false)
    window.dispatchEvent(new Event('blur'))
    await flushPromises()
    expect(state!.paused.value).toBe(true)
    expect(state!.progress.value[1]).toEqual(first)
    await vi.advanceTimersByTimeAsync(5000)
    expect(getGatewayPoolProgress).toHaveBeenCalledTimes(1)
    focus.mockReturnValue(true)
    window.dispatchEvent(new Event('focus'))
    await flushPromises()
    expect(state!.progress.value[1]).toEqual(first)
    expect(state!.paused.value).toBe(true)
    resolveNext({ 1: { ...first, attempt: 3 } })
    await flushPromises()
    expect(state!.progress.value[1].attempt).toBe(3)
    expect(state!.paused.value).toBe(false)
    wrapper.unmount()
  })

  it('waits for an aborted request to settle and rejects its late previous-page snapshot', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    const ids = ref([1])
    let resolveOld!: (value: any) => void
    vi.mocked(getGatewayPoolProgress).mockImplementationOnce(() => new Promise(resolve => { resolveOld = resolve }))
      .mockResolvedValueOnce({ 2: snapshot() })
    let state: ReturnType<typeof useGatewayPoolProgress>
    const wrapper = mount(defineComponent({
      setup() { state = useGatewayPoolProgress(computed(() => ids.value)); return () => null }
    }))
    await flushPromises()
    ids.value = [2]
    await flushPromises()
    expect(getGatewayPoolProgress).toHaveBeenCalledTimes(1)
    expect(vi.mocked(getGatewayPoolProgress).mock.calls[0][1]?.aborted).toBe(true)
    resolveOld({ 1: snapshot({ phase: 'ready', attempt: 9 }) })
    await flushPromises()
    expect(getGatewayPoolProgress).toHaveBeenCalledTimes(2)
    expect(state!.progress.value[1]).toBeUndefined()
    expect(state!.progress.value[2].phase).toBe('idle')
    wrapper.unmount()
  })

  it('keeps the paused frame after a failed focus refresh until a successful replacement', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    const focus = vi.mocked(document.hasFocus)
    const first = snapshot({ phase: 'ready', attempt: 1 })
    vi.mocked(getGatewayPoolProgress)
      .mockResolvedValueOnce({ 1: first } as any)
      .mockRejectedValueOnce(new Error('offline'))
      .mockResolvedValueOnce({ 1: { ...first, attempt: 2 } } as any)
    let state: ReturnType<typeof useGatewayPoolProgress>
    const wrapper = mount(defineComponent({
      setup() { state = useGatewayPoolProgress(computed(() => [1])); return () => null }
    }))
    try {
      await flushPromises()
      focus.mockReturnValue(false)
      window.dispatchEvent(new Event('blur'))
      await flushPromises()
      focus.mockReturnValue(true)
      window.dispatchEvent(new Event('focus'))
      await flushPromises()
      expect(state!.paused.value).toBe(true)
      expect(state!.progress.value[1]).toEqual(first)
      await vi.advanceTimersByTimeAsync(1000)
      await flushPromises()
      expect(state!.paused.value).toBe(false)
      expect(state!.progress.value[1].attempt).toBe(2)
    } finally {
      wrapper.unmount()
    }
  })

  it('keeps all rows paused through empty or incomplete successful replies, then swaps a complete batch', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    const focus = vi.mocked(document.hasFocus)
    const first = { 1: snapshot({ attempt: 1 }), 2: snapshot({ attempt: 2 }) }
    const next = { 1: snapshot({ attempt: 3 }), 2: snapshot({ attempt: 4 }) }
    const incomplete = [{}, { 1: next[1] }, { 1: next[1], 2: { ...next[2], runtime: undefined } }]
    vi.mocked(getGatewayPoolProgress).mockResolvedValueOnce(first)
    for (const batch of incomplete) vi.mocked(getGatewayPoolProgress).mockResolvedValueOnce(batch as any)
    vi.mocked(getGatewayPoolProgress).mockResolvedValueOnce(next)
    let state!: ReturnType<typeof useGatewayPoolProgress>
    const wrapper = mount(defineComponent({
      setup() { state = useGatewayPoolProgress(computed(() => [1, 2])); return () => null }
    }))
    try {
      await flushPromises()
      focus.mockReturnValue(false)
      window.dispatchEvent(new Event('blur'))
      await flushPromises()
      focus.mockReturnValue(true)
      window.dispatchEvent(new Event('focus'))
      await flushPromises()
      for (let index = 0; index < incomplete.length; index++) {
        expect(state.paused.value).toBe(true)
        expect(state.progress.value).toEqual(first)
        await vi.advanceTimersByTimeAsync(1000)
        await flushPromises()
      }
      expect(state.paused.value).toBe(false)
      expect(state.progress.value).toEqual(next)
    } finally { wrapper.unmount() }
  })
})
