import { onMounted, onUnmounted, ref, watch, type ComputedRef } from 'vue'
import { getGatewayPoolProgress, type GatewayPoolProgress } from '@/api/admin/accounts'

const POLL_MS = 2000
const REQUEST_TIMEOUT_MS = 5000
const MAX_ACCOUNTS = 200

// One lightweight batch poll for the visible page. No full account refresh,
// persistence, or upstream verification is triggered by this endpoint.
export function useGatewayPoolProgress(ids: ComputedRef<number[]>) {
  const progress = ref<Record<number, GatewayPoolProgress>>({})
  const unavailable = ref(false)
  let timer: ReturnType<typeof setTimeout> | undefined
  let controller: AbortController | undefined
  let mounted = false
  let generation = 0

  function stop() {
    generation++
    clearTimeout(timer)
    controller?.abort()
  }
  async function poll() {
    stop()
    const current = generation
    if (!mounted || document.hidden || ids.value.length === 0) {
      progress.value = {}
      return
    }
    const request = new AbortController()
    controller = request
    const timeout = setTimeout(() => request.abort(), REQUEST_TIMEOUT_MS)
    try {
      const snapshots = await getGatewayPoolProgress(ids.value.slice(0, MAX_ACCOUNTS), request.signal)
      if (current === generation) {
        progress.value = snapshots
        unavailable.value = false
      }
    } catch {
      if (current === generation) {
        unavailable.value = true
      }
    } finally {
      clearTimeout(timeout)
      if (current === generation && mounted) timer = setTimeout(poll, POLL_MS)
    }
  }
  watch(() => ids.value.join(','), poll)
  onMounted(() => {
    mounted = true
    document.addEventListener('visibilitychange', poll)
    void poll()
  })
  onUnmounted(() => {
    mounted = false
    stop()
    document.removeEventListener('visibilitychange', poll)
  })
  return { progress, unavailable }
}
