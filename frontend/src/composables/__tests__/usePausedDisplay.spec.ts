import { describe, expect, it } from 'vitest'
import { defineComponent, nextTick, ref } from 'vue'
import { mount } from '@vue/test-utils'
import { usePausedDisplay } from '../usePausedDisplay'

describe('paused JSON display', () => {
  it('freezes nested input, keeps repeated pauses stable, and resumes the latest complete value', async () => {
    const paused = ref(false)
    const source = ref({ id: 1, nested: { count: 7 }, now: 100 })
    let display!: ReturnType<typeof usePausedDisplay<typeof source.value>>
    const wrapper = mount(defineComponent({
      setup() {
        display = usePausedDisplay(() => source.value, () => paused.value, () => source.value.id)
        return () => null
      }
    }))
    try {
      paused.value = true
      await nextTick()
      source.value.nested.count = 9
      source.value.now = 200
      paused.value = true
      await nextTick()
      expect(display.value).toEqual({ id: 1, nested: { count: 7 }, now: 100 })
      paused.value = false
      await nextTick()
      expect(display.value).toEqual(source.value)
    } finally { wrapper.unmount() }
  })

  it('never retains another account when a paused virtual row is recycled', async () => {
    const source = ref({ id: 1, name: 'old' })
    let display!: ReturnType<typeof usePausedDisplay<typeof source.value>>
    const wrapper = mount(defineComponent({
      setup() {
        display = usePausedDisplay(() => source.value, () => true, () => source.value.id)
        return () => null
      }
    }))
    try {
      source.value = { id: 2, name: 'new' }
      await nextTick()
      expect(display.value).toEqual({ id: 2, name: 'new' })
    } finally { wrapper.unmount() }
  })
})
