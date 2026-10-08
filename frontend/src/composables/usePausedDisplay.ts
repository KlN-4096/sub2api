import { computed, shallowRef, watch } from 'vue'

// The inputs are JSON API/display data, not editable state. Snapshot only when
// pausing; a recycled row must never retain the previous account's frame.
export function usePausedDisplay<T extends object>(
  read: () => T,
  paused: () => boolean,
  identity: () => unknown
) {
  const frozen = shallowRef<T>()
  watch([identity, paused], () => {
    frozen.value = paused() ? JSON.parse(JSON.stringify(read())) : undefined
  }, { immediate: true })
  return computed(() => frozen.value ?? read())
}
