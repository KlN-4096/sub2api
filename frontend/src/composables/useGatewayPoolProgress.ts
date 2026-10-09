import { onMounted, onUnmounted, ref, watch, type ComputedRef } from 'vue'
import { getGatewayPoolProgress, type GatewayPoolProgress } from '@/api/admin/accounts'

const POLL_MS = 1000
const REQUEST_TIMEOUT_MS = 5000
const MAX_ACCOUNTS = 200

// One lightweight batch poll for the visible page. No full account refresh,
// persistence, or upstream verification is triggered by this endpoint.
export function useGatewayPoolProgress(ids: ComputedRef<number[]>) {
  const progress = ref<Record<number, GatewayPoolProgress>>({})
  const paused = ref(true)
  let timer: ReturnType<typeof setTimeout> | undefined
  let controller: AbortController | undefined
  let mounted = false
  let generation = 0
  let inFlight = false
  let refreshPending = false

  const targets = () => [...new Set(ids.value)].slice(0, MAX_ACCOUNTS)
  const active = () => mounted && !document.hidden && document.hasFocus() && targets().length > 0

  function stop() {
    generation++
    clearTimeout(timer)
    controller?.abort()
  }
  function refresh() {
    stop()
    // Blur is an intentional pause, not a failed snapshot. Keep the display
    // frozen through focus until a complete replacement arrives.
    paused.value = true
    const visible = new Set(targets())
    // Keep the same cells during blur/focus, but never carry a previous page's
    // rows into a different target set.
    progress.value = Object.fromEntries(Object.entries(progress.value).filter(([id]) => visible.has(Number(id))))
    refreshPending = active()
    if (!inFlight && refreshPending) void poll()
  }
  const refreshVisibility = () => refresh()
  async function poll() {
    if (inFlight || !active()) return
    clearTimeout(timer)
    inFlight = true
    refreshPending = false
    const current = generation
    const requested = targets()
    const request = new AbortController()
    controller = request
    const timeout = setTimeout(() => request.abort(), REQUEST_TIMEOUT_MS)
    try {
      const snapshots = await getGatewayPoolProgress(requested, request.signal)
      const complete = requested.every(id => {
        const runtime = snapshots?.[id]?.runtime
        return runtime && Number.isFinite(Date.parse(runtime.observed_at)) &&
          Array.isArray(runtime.tickets) && Array.isArray(runtime.rounds) &&
          typeof runtime.rest?.active === 'boolean'
      })
      if (current === generation && active() && complete) {
        progress.value = Object.fromEntries(Object.entries(snapshots).filter(([id]) => requested.includes(Number(id))))
        paused.value = false
      }
    } catch {
      // A transport failure says nothing about ticket/capacity/rest state.
      // Retain the last snapshot; only successful reads resume a paused view.
    } finally {
      clearTimeout(timeout)
      controller = undefined
      inFlight = false
      if (active()) {
        if (refreshPending) void poll()
        else timer = setTimeout(poll, POLL_MS)
      }
    }
  }
  watch(() => ids.value.join(','), () => refresh())
  onMounted(() => {
    mounted = true
    document.addEventListener('visibilitychange', refreshVisibility)
    window.addEventListener('focus', refreshVisibility)
    window.addEventListener('blur', refreshVisibility)
    refresh()
  })
  onUnmounted(() => {
    mounted = false
    stop()
    document.removeEventListener('visibilitychange', refreshVisibility)
    window.removeEventListener('focus', refreshVisibility)
    window.removeEventListener('blur', refreshVisibility)
  })
  return { progress, paused, refresh }
}
