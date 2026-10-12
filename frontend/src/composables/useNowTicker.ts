import { onUnmounted, ref, type Ref } from 'vue'

/**
 * 会走的「现在」。倒计时必须真的走，否则页面一挂就是一张冻结的快照：过期的票不消失、
 * 进度条永远停在打开时的比例（账号页的自动刷新默认是关的，props 不会自己变）。
 *
 * 同一行里判同一条到期时间的地方都要用它。各自写裸 `new Date()` 的话，那处 computed
 * 没有响应式依赖、永不重算。
 *
 * 同一周期全页共用一只表（useSharedNowTicker），各处同相位刷新；新使用者加入时表若已旧满 1s 就拨到
 * 当前时间，换页/筛选新挂载的行不会读到最多一个周期前的旧时间（不满 1s 不拨：拨一次同周期全部重渲染）。
 * 共享表在首个使用者挂载时才建、最后一个卸载即清：只要先前挂载的组件都已卸载（spec
 * 里用 enableAutoUnmount），先装 fake timer 再挂载就推得动，被推到未来的 ref 也不会漏给后续用例。
 *
 * 30s 粒度：UsageProgressBar 自己那只表是 60s，比它快一档就不会出现「条子还是绿的、
 * 右边已经写着待刷新」这种自相矛盾。
 */
export const NOW_TICKER_INTERVAL_MS = 30_000

interface SharedTicker {
  now: Ref<number>
  users: number
  timer: ReturnType<typeof setInterval>
}
const sharedTickers = new Map<number, SharedTicker>()

// One timer per cadence for all users. Start lazily and dispose on the last unmount:
// no module-load timer, and nothing outlives the components (pages, test cases) using it.
export function useSharedNowTicker(intervalMs = NOW_TICKER_INTERVAL_MS) {
  let ticker = sharedTickers.get(intervalMs)
  if (!ticker) {
    const now = ref(Date.now())
    ticker = { now, users: 0, timer: setInterval(() => { now.value = Date.now() }, intervalMs) }
    sharedTickers.set(intervalMs, ticker)
  } else if (Date.now() - ticker.now.value >= 1000) {
    // Every subscriber of this cadence re-renders on a bump; under a second is
    // invisible on second-granular countdowns, and virtualized rows mount on scroll.
    ticker.now.value = Date.now()
  }
  ticker.users++
  const current = ticker
  onUnmounted(() => {
    if (--current.users === 0) {
      clearInterval(current.timer)
      sharedTickers.delete(intervalMs)
    }
  })
  return current.now
}
