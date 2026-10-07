import { onUnmounted, ref } from 'vue'

/**
 * 会走的「现在」。倒计时必须真的走，否则页面一挂就是一张冻结的快照：过期的票不消失、
 * 进度条永远停在打开时的比例（账号页的自动刷新默认是关的，props 不会自己变）。
 *
 * 同一行里判同一条到期时间的地方都要用它。各自写裸 `new Date()` 的话，那处 computed
 * 没有响应式依赖、永不重算，于是暂停到期后猎手行翻成「无流量·暂停」而状态列徽标还冻结
 * 在「降智」——两行各说各话。
 *
 * 每个实例一只表、一个 ref。这是从 AccountTurnStateCell 里原样抽出来的行为——那边的注释
 * 写着「模块级共享时钟」，但 `<script setup>` 顶层声明本来就是每实例的，它从来没共享过。
 * 真要改成模块级单例得先解决测试隔离：单例定时器会在 fake timer 安装之前就建好（推不动），
 * 而被 `advanceTimersByTime` 推到未来的共享 ref 会泄漏给同文件的后续用例。
 * 代价是账号列表每行两只表（几十行即上百只 30s 定时器），写的是各自的 ref，可接受。
 *
 * 30s 粒度：UsageProgressBar 自己那只表是 60s，比它快一档就不会出现「条子还是绿的、
 * 右边已经写着待刷新」这种自相矛盾。
 */
export const NOW_TICKER_INTERVAL_MS = 30_000

export function useNowTicker() {
  const now = ref(Date.now())
  const timer = setInterval(() => (now.value = Date.now()), NOW_TICKER_INTERVAL_MS)
  onUnmounted(() => clearInterval(timer))
  return now
}
