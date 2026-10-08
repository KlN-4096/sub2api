<template>
  <div v-if="isCodexAccount" class="w-[260px] min-w-[260px] max-w-[260px] space-y-2 overflow-hidden text-[11px] tabular-nums" data-testid="account-gateway-cell">
    <div v-if="usesPool" class="min-h-[60px] whitespace-normal break-words rounded-md bg-primary-50 px-2.5 py-2 text-[11px] text-primary-700 dark:bg-primary-900/20 dark:text-primary-300"
      role="status" aria-live="polite" data-testid="account-gateway-progress">
      <p>
        <span v-if="displayProgress?.sequence">{{ t('admin.accounts.openai.gatewayProgress.run', { id: displayProgress.sequence }) }} · </span>
        {{ t(`admin.accounts.openai.gatewayProgress.${displayProgress?.phase || 'idle'}`) }}
      </p>
      <p v-if="displayProgress && displayProgress.phase !== 'idle'" class="mt-1 text-[11px] text-gray-500 dark:text-gray-400">
        {{ t('admin.accounts.openai.gatewayProgress.count', {
          attempt: displayProgress.attempt, seconds: Math.floor(displayProgress.elapsed_ms / 1000)
        }) }}
        <span v-if="displayProgress.rejected"> · {{ t('admin.accounts.openai.gatewayProgress.rejected', { count: displayProgress.rejected }) }}</span>
        <span v-if="displayProgress.active_requests > 1"> · {{ t('admin.accounts.openai.gatewayProgress.concurrent', { count: displayProgress.active_requests }) }}</span>
      </p>
    </div>
    <!-- 没有读数也要占位：整块消失时，「没接网关池」「接了还没跑过流量」「落点读不出来」
         在页面上长得一模一样。非 Codex 上游的账号根本没有落点这回事，那才该整块消失。 -->
    <p v-if="!preparing && !current && !items.length" class="text-[10px] text-gray-400" data-testid="account-gateway-empty">
      {{ t('admin.accounts.openai.gatewayHistory.empty') }}
    </p>
      <!-- 第一行是「当前大区 · 当前网关」：整块里最要紧的一个事实。 -->
      <div v-if="current || preparing" class="flex items-center gap-1" data-testid="account-gateway-current">
        <span class="shrink-0 text-[10px] text-gray-400">
          {{ t('admin.accounts.openai.gatewayHistory.current') }}
        </span>
        <!-- 当前票按真实验满/降级证据着色；下方尚未验满的候选始终保持中性。 -->
        <span
          v-if="current"
          class="truncate rounded px-1 text-[10px] font-medium leading-4"
          :class="TONE_CLASS[preparing ? 'idle' : toneOf(current)]"
          :data-tone="preparing ? 'idle' : toneOf(current)"
          data-testid="account-gateway-current-ticket"
          :title="preparing ? t('admin.accounts.openai.gatewayProgress.verifying') : titleOf(current)"
        >
          {{ preparing ? '' : verdictMark(current) }}{{ regionLabel(current.region) }} · {{ current.name }}
        </span>
        <span v-else class="truncate text-[10px] text-gray-400">{{ t(`admin.accounts.openai.gatewayProgress.${displayProgress?.phase}`) }}</span>
        <span v-if="current" class="ml-auto shrink-0 text-[10px] text-gray-400" data-testid="account-gateway-current-time">{{ safeRelativeTime(currentAt) }}</span>
      </div>
      <div v-for="round in (usesPool ? activeRounds : [])" :key="round.id" class="space-y-1 text-[11px] text-gray-500 dark:text-gray-400" data-testid="account-gateway-usage-round">
        <span>{{ t('admin.accounts.openai.gatewayRuntime.counts', { full: round.full, attempted: round.attempted }) }}</span>
        <p v-if="round.full_usage_mode === ACTIVE_USAGE_MODE" :title="t('admin.accounts.openai.gatewayRuntime.activeHint')">
          {{ t('admin.accounts.openai.gatewayRuntime.active', { duration: fullUseTime(round) }) }}<span v-if="round.duration_incomplete">{{ t('admin.accounts.openai.gatewayRuntime.durationIncomplete') }}</span>
        </p>
      </div>
      <div v-if="usesPool && runtime && !activeRounds.length" class="space-y-1 text-[11px] text-gray-500 dark:text-gray-400" data-testid="account-gateway-usage-idle">
        <span>{{ t('admin.accounts.openai.gatewayRuntime.counts', { full: 0, attempted: 0 }) }}</span>
        <p>{{ t('admin.accounts.openai.gatewayRuntime.active', { duration: formatUseTime(0) }) }}</p>
      </div>
      <GatewayQueueCards v-if="usesPool" :snapshot="runtime?.queues" :now="now" />
      <!-- 只展示本地冷却，不把过期库存快照当成剩余候选。 -->
    <div v-if="usesPool" class="flex items-center justify-end gap-2" data-testid="account-gateway-cooldown-row">
      <p
        v-if="items.length"
        class="mr-auto min-w-0 text-[9px] leading-3 text-gray-500 dark:text-gray-400"
        :title="t('admin.accounts.openai.gatewayHistory.windowUsageHint')"
        data-testid="account-gateway-window-usage"
      >
        {{
          t('admin.accounts.openai.gatewayHistory.windowUsage', {
            used: windowUsage.used,
            cooled: windowUsage.cooled
          })
        }}
      </p>
      <button type="button"
        class="inline-flex min-w-[92px] shrink-0 items-center justify-center gap-1.5 rounded-md border border-gray-200 px-2.5 py-1.5 text-[11px] text-gray-500 transition-colors hover:bg-gray-50 hover:text-gray-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gray-400 disabled:cursor-wait disabled:opacity-50 dark:border-dark-600 dark:text-gray-400 dark:hover:bg-dark-700 dark:hover:text-gray-200"
        data-testid="account-gateway-retry" :disabled="display.retryPending" :aria-busy="display.retryPending || undefined"
        :title="t('admin.accounts.openai.gwpoolManualRetryHint')" @click="emit('retry', account.id)">
        <Icon name="refresh" size="xs" class="shrink-0" :class="{ 'motion-safe:animate-spin': display.retryPending }"
          :style="display.retryPending && progressPaused ? { animationPlayState: 'paused' } : undefined" aria-hidden="true" />
        <span>{{ t(`admin.accounts.openai.${display.retryPending ? 'gwpoolManualRetryPending' : 'gwpoolManualRetry'}`) }}</span>
      </button>
    </div>
    <p v-if="usesPool && runtime" class="truncate border-t border-gray-100 pt-2 text-[10px] text-gray-500 dark:border-gray-700 dark:text-gray-400" data-testid="account-gateway-usage-history">
      {{ historyUsage.hasDuration
        ? t('admin.accounts.openai.gatewayRuntime.archived', { count: historyUsage.rounds, duration: formatUseTime(historyUsage.durationMS) })
        : t('admin.accounts.openai.gatewayRuntime.archivedCounts', { count: historyUsage.rounds }) }}
      <span v-if="historyUsage.hasDuration && historyUsage.incomplete">{{ t('admin.accounts.openai.gatewayRuntime.durationIncomplete') }}</span>
    </p>
  </div>
</template>

<script setup lang="ts">
/**
 * 当前落点、真实满血使用与后端同源的优质/普通候选摘要。
 *
 * 跟随已有运行态快照，不额外调接口；队列缺快照时不从历史自行推测。
 *
 * **只是展示**：这条记录挂在账号行上，而真正被烧掉的单位是上游账号（同一份凭据可能挂在
 * 多个行上），各行只看得见自己发出去的那些。拿它判「这个网关还能不能用」会低估烧掉的
 * 范围，那个判定在后端 gatewayPoolUsedRecently。
 *
 * 大区同样是**池子口径**（铸这张票的出口在哪儿），不是「这一发实际落在哪个大区」：
 * 注入时两件 cookie 齐送 ⇒ 上游不回新 __oailb ⇒ 真实落点读不出来（docs 的 S1/S2）。
 */
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { Account } from '@/types'
import type { GatewayPoolProgress, GatewayPoolUsageRound } from '@/api/admin/accounts'
import { targetsCodexUpstream } from '@/utils/turnState'
import { formatRelativeTime } from '@/utils/format'
import { useSharedNowTicker } from '@/composables/useNowTicker'
import { usePausedDisplay } from '@/composables/usePausedDisplay'
import Icon from '@/components/icons/Icon.vue'
import GatewayQueueCards from './GatewayQueueCards.vue'
import { gatewayRegionDisplayKey } from '@/utils/gatewayRegionDisplay'

/** 初始冷却默认 1 小时；有学习状态时优先使用每个网关自己的截止时间。 */
const DEFAULT_WINDOW_MS = 60 * 60 * 1000
const MAX_WINDOW_MS = 24 * 60 * 60 * 1000

const props = defineProps<{ account: Account; progress?: GatewayPoolProgress; progressPaused?: boolean; retryPending?: boolean }>()
const emit = defineEmits<{ retry: [id: number] }>()
const { t } = useI18n()
const wallTime = useSharedNowTicker(1000)
const display = usePausedDisplay(
  () => ({ ...props, now: wallTime.value }),
  () => props.progressPaused === true,
  () => props.account.id
)
const displayProgress = computed(() => display.value.progress)
const now = computed(() => display.value.now)
const runtime = computed(() => displayProgress.value?.runtime)
function ticketExpired(ticket: NonNullable<GatewayPoolProgress['runtime']>['tickets'][number]): boolean {
  return validTimestamp(ticket.expires_at) && Date.parse(ticket.expires_at!) <= now.value
}
const liveTickets = computed(() => {
  const snapshot = runtime.value
  if (!snapshot) return []
  // The API only lists backend-live tickets. Missing expiry means no explicit
  // cookie deadline, not an invented TTL; a present malformed value is unknown.
  return snapshot.tickets.filter((ticket) => ticket.expires_at === undefined ||
    (validTimestamp(ticket.expires_at) && !ticketExpired(ticket)))
})
const activeRounds = computed(() => runtime.value?.rounds.filter((round) => round.model === 'all' && !round.ended_at) || [])
const ACTIVE_USAGE_MODE = 'business_active_v1'
const historyUsage = computed(() => {
  const snapshot = runtime.value
  const total = {
    rounds: snapshot?.archived?.all?.rounds || 0,
    durationMS: Math.max(0, snapshot?.archived?.all?.active_duration_ms || 0),
    hasDuration: snapshot?.archived?.all?.active_duration_ms !== undefined,
    incomplete: snapshot?.archived?.all?.active_duration_incomplete || false
  }
  // Retained closed rounds and compressed archives are disjoint. Use measured
  // full-use duration, never wall time; ongoing and legacy model rounds stay out.
  for (const round of snapshot?.rounds || []) {
    if (round.model !== 'all' || !validTimestamp(round.ended_at)) continue
    total.rounds++
    if (round.full_usage_mode === ACTIVE_USAGE_MODE) {
      total.durationMS += Math.max(0, round.full_duration_ms || 0)
      total.hasDuration = true
      total.incomplete ||= round.duration_incomplete || false
    }
  }
  return total
})
function validTimestamp(value?: string): boolean {
  return !!value && Number.isFinite(Date.parse(value)) && Date.parse(value) > 0
}
function safeRelativeTime(value?: string): string {
  return validTimestamp(value) ? formatRelativeTime(value!, now.value) : '—'
}
function formatUseTime(ms: number): string {
  const seconds = Math.max(0, Math.floor(ms / 1000))
  if (seconds >= 3600) {
    return t('admin.accounts.openai.gatewayRuntime.durationHours', {
      hours: Math.floor(seconds / 3600), minutes: Math.floor(seconds / 60) % 60, seconds: seconds % 60
    })
  }
  return t('admin.accounts.openai.gatewayRuntime.duration', { minutes: Math.floor(seconds / 60), seconds: seconds % 60 })
}
function fullUseTime(round: GatewayPoolUsageRound): string {
  // The one-second backend snapshot already projects live observed intervals.
  // Do not invent additional usage while no fresh observation has arrived.
  return formatUseTime(Math.max(0, round.full_duration_ms || 0))
}

interface GatewaySeen {
  at?: string
  region?: string
  verdict?: string
  full_at?: string
  /** 后端在判降智那一刻量到的满血时长（毫秒）。缺省 / 0 = 没量到。 */
  full_held_ms?: number
  cooldown?: { until?: string; window_seconds?: number; fixed_seconds?: number; recommended_seconds?: number; cleared?: boolean }
}

interface GatewayHistory {
  current?: string
  current_region?: string
  seen?: Record<string, GatewaySeen>
  /** 池子最近一次报的可交付网关数。缺省 / 0 = 没问到清单。 */
  pool_live?: number
  /** 上面那些里这个号还没烧过的个数（后端当场数的，不许在前端用减法算，见 windowUsage）。 */
  pool_free?: number
  updated_at?: string
}

interface GatewayItem {
  name: string
  at: string
  region: string
  verdict: string
  fullAt: string
  /** 后端量到的满血时长（毫秒）。0 = 没量到，见 fullHeldOf。 */
  fullHeldMs: number
  cooldownUntil: string
  cooldownCleared: boolean
  cooldownWindowMs: number
  fixedSeconds: number
  recommendedSeconds: number
}

// Green requires a verified, unexpired ticket. Red requires an expired ticket
// or an explicit degraded verdict still in cooldown; mere contact is grey.
const TONE_CLASS = {
  full: 'bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300',
  degraded: 'bg-rose-50 text-rose-700 dark:bg-rose-900/40 dark:text-rose-300',
  idle: 'bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-400'
} as const

type GatewayTone = keyof typeof TONE_CLASS

const isCodexAccount = computed(() => targetsCodexUpstream(display.value.account))

const extra = computed(() => {
  const base = (display.value.account.extra as Record<string, unknown> | undefined) ?? {}
  const snapshot = runtime.value
  if (base.openai_gwpool !== true || !snapshot) return base
  // Display-only projection: never replace the account object used by editing.
  return {
    ...base,
    openai_gwpool_gateways: snapshot.history ?? base.openai_gwpool_gateways,
    openai_gwpool_contacts: snapshot.contacts ?? base.openai_gwpool_contacts,
    openai_gwpool_ledger_tag: snapshot.ledger_tag ?? base.openai_gwpool_ledger_tag,
    openai_gwpool_gateway_window_s: snapshot.gateway_window_seconds ?? base.openai_gwpool_gateway_window_s
  }
})

/**
 * 这个号的路由 cookie 是不是由网关池下发（账号上的 `openai_gwpool` 开关）。
 *
 * 没开的号只显示落点本身；池专属的验满颜色、候选队列与冷却统计均不适用。
 */
const usesPool = computed(() => extra.value.openai_gwpool === true)

const history = computed<GatewayHistory>(() => {
  const raw = extra.value.openai_gwpool_gateways
  return raw && typeof raw === 'object' ? (raw as GatewayHistory) : {}
})


/**
 * 判「还烧着」的窗口。账号自己配了本地账本窗口就按它 —— 后端拿同一个数判「这个网关
 * 最近烧过没有」，页面按另一个数会和它对不上。
 */
const windowMs = computed(() => {
  const raw = extra.value.openai_gwpool_gateway_window_s
  const seconds = typeof raw === 'number' ? raw : Number.NaN
  return Number.isFinite(seconds) && seconds * 1000 >= DEFAULT_WINDOW_MS && seconds * 1000 <= MAX_WINDOW_MS
    ? seconds * 1000 : DEFAULT_WINDOW_MS
})

/** 按最近用过的在前排。后端存的是 map，顺序在这里定。 */
const items = computed<GatewayItem[]>(() => {
  const seen = history.value.seen
  if (!seen || typeof seen !== 'object') return []
  return Object.entries(seen)
    .filter(([name, row]) => typeof name === 'string' && name !== '' && !!row && typeof row === 'object')
    .map(([name, row]) => ({
      name,
      at: typeof row.at === 'string' ? row.at : '',
      region: typeof row.region === 'string' ? row.region : '',
      verdict: row.verdict === 'full' || row.verdict === 'degraded' ? row.verdict : '',
      fullAt: typeof row.full_at === 'string' ? row.full_at : '',
      fullHeldMs: typeof row.full_held_ms === 'number' ? row.full_held_ms : 0,
      cooldownUntil: typeof row.cooldown?.until === 'string' ? row.cooldown.until : '',
      cooldownCleared: row.cooldown?.cleared === true,
      cooldownWindowMs: typeof row.cooldown?.window_seconds === 'number'
        && row.cooldown.window_seconds > 0 && row.cooldown.window_seconds * 1000 <= MAX_WINDOW_MS
        ? row.cooldown.window_seconds * 1000 : windowMs.value,
      fixedSeconds: typeof row.cooldown?.fixed_seconds === 'number' ? row.cooldown.fixed_seconds : 0,
      recommendedSeconds: typeof row.cooldown?.recommended_seconds === 'number'
        ? row.cooldown.recommended_seconds : 0
    }))
    .sort((a, b) => Date.parse(b.at) - Date.parse(a.at))
})

const preparing = computed(() => usesPool.value && !!displayProgress.value &&
  ['pending', 'fetching', 'verifying', 'waiting'].includes(displayProgress.value.phase))
const currentTicket = computed(() => preparing.value
  ? runtime.value?.tickets.find(ticket => ticket.gateway === displayProgress.value?.gateway)
  : runtime.value?.tickets[0])
const current = computed<GatewayItem | null>(() => {
  const live = currentTicket.value
  // A rejected candidate disappears during acquisition; it must not expose
  // history.current again between two verification attempts.
  const name = preparing.value
    ? (displayProgress.value?.phase === 'verifying' ? displayProgress.value.gateway : '')
    : live?.gateway || history.value.current
  if (!name) return null
  // 时间和大区取 seen 里那条；没有就退回记录自己的那两个字段（老记录、或被裁过）。
  return (
    items.value.find((i) => i.name === name) ?? {
      name,
      at: history.value.updated_at ?? '',
      region: live?.region || (preparing.value ? '' : history.value.current_region) || '',
      verdict: '',
      fullAt: '',
      fullHeldMs: 0,
      cooldownUntil: '',
      cooldownCleared: false,
      cooldownWindowMs: windowMs.value,
      fixedSeconds: 0,
      recommendedSeconds: 0
    }
  )
})
const currentAt = computed(() => preparing.value
  ? displayProgress.value?.updated_at
  : currentTicket.value?.verified_at || current.value?.at)

/**
 * 本地冷却计数与候选队列各自独立，不能用库存减历史接触数来推算候选。
 * 冷却到期不保证仍有票或恢复满血；两类候选只认后端有效目录投影。
 */
const windowUsage = computed(() => {
  let used = 0
  let cooled = 0
  for (const item of items.value) {
    const deadline = cooldownDeadline(item)
    if (!Number.isFinite(deadline)) continue
    if (deadline > now.value) used += 1
    else cooled += 1
  }
  return { used, cooled }
})

function cooldownDeadline(item: GatewayItem): number {
  if (item.cooldownCleared) return 0
  const legacy = Date.parse(item.at) + item.cooldownWindowMs
  const learned = Date.parse(item.cooldownUntil)
  if (!Number.isFinite(learned)) return legacy
  return Number.isFinite(legacy) ? Math.max(learned, legacy) : learned
}

function isHot(item: GatewayItem): boolean {
  return cooldownDeadline(item) > now.value
}

function toneOf(item: GatewayItem | null | undefined): GatewayTone {
  // 没开网关池的号一律中性：见 usesPool 的注释，它的 verdict 恒为空，不拦的话下面那条
  // 兜底会把每个最近用过的落点都染红。
  if (!usesPool.value) return 'idle'
  if (item && liveTickets.value.some((ticket) => ticket.gateway === item.name && ticket.verified_models.length > 0)) return 'full'
  if (!item) return 'idle'
  const tickets = runtime.value?.tickets.filter((ticket) => ticket.gateway === item.name) ?? []
  // A replacement awaiting verification must not inherit the previous ticket's
  // degraded verdict just because both came from the same gateway.
  if (tickets.length) return tickets.every(ticketExpired) ? 'degraded' : 'idle'
  return item.verdict === 'degraded' && isHot(item) ? 'degraded' : 'idle'
}

/**
 * 判定的字符前缀：颜色退化成装饰之后，信息仍然读得出来。
 * 带一个空格 —— 不带的话渲染成 `✓US · unified-107`，前缀和大区名糊在一起。
 */
function verdictMark(item: GatewayItem | null | undefined): string {
  switch (toneOf(item)) {
    case 'full':
      return '✓ '
    case 'degraded':
      return '! '
    default:
      return ''
  }
}

function shortName(name: string): string {
  return name.startsWith('unified-') ? name.slice('unified-'.length) : name
}

function regionLabel(key: string): string {
  return t(`admin.accounts.openai.gatewayHistory.regions.${gatewayRegionDisplayKey(key) || 'unknown'}`)
}

/**
 * tooltip 固定五段：区域-网关名-满血时间-状态-判定（例：`美东-149-45 分钟前-冷却中-降智`）。
 *
 * 段位固定（没有就写「未满血 / 没判过」而不是整段省掉）是刻意的：运营方是竖着扫一列格子
 * 看的，段数会变的话每一行都得重新找「满血时间」在哪儿。同理每段只放值、不带标签——
 * 一列里每行都重复一遍「满血于」「上次判定：」，真正要比的那几个值反而被推到行尾对不齐。
 *
 * 满血时间是**这一格的满血窗口持续了多久**，不是它发生在什么时候：窗口的长度才是运营方
 * 要横着比的那个量（「这个落点只给了我 40 秒」对「给了 180 秒」）。
 */
const TITLE_SEP = '-'

/**
 * 满血时长：这一格的满血窗口持续了多久，直接读后端量好的 full_held_ms。
 *
 * **不要在这里用 `at - fullAt` 算。** 第一版就是那么写的，现网渲染出 22655s / 21738s ——
 * fullAt 是粘滞的，而 degraded 判定要等**下一次真的打到这个网关**才会写，中间空了几小时
 * 就白算几小时。真正的窗口长度由后端在判降智那一刻量（从这张票验出满血算起，两头都在
 * 同一张票的生命里，有界），见 openAIGatewaySeen.FullHeldMs。
 *
 * 没有读数就写「未计时」：窗口还在跑、从没验出过满血、或者这一格的降智不是从本进程这条
 * 路判出来的 —— 对读者都是同一件事，这一格没有时长可报。
 */
function fullHeldOf(item: GatewayItem): string {
  const base = 'admin.accounts.openai.gatewayHistory'
  if (!(item.fullHeldMs > 0)) return t(`${base}.fullUntimed`)
  return `${Math.round(item.fullHeldMs / 1000)}s`
}

/**
 * 第四段：还烧着就报**还剩多少分钟出冷却**，出了就是「可再用」。
 *
 * 向上取整并兜到 1：这一段只在 isHot 为真时出现，而「剩余 0 分钟」会被读成「已经好了」——
 * 正好和它要表达的相反。不足一分钟报「1 分钟」，宁可催早一点。
 */
function cooldownOf(item: GatewayItem): string {
  const base = 'admin.accounts.openai.gatewayHistory'
  const left = cooldownDeadline(item) - now.value
  const remaining = isHot(item)
    ? t(`${base}.regionHot`, { minutes: Math.max(1, Math.ceil(left / 60_000)) })
    : t(`${base}.regionCooled`)
  if (item.fixedSeconds > 0) {
    return `${remaining} ${t(`${base}.cooldownFixed`, { minutes: Math.ceil(item.fixedSeconds / 60) })}`
  }
  return item.recommendedSeconds > 0
    ? `${remaining} ${t(`${base}.cooldownRecommended`, { minutes: Math.ceil(item.recommendedSeconds / 60) })}`
    : remaining
}

function titleOf(item: GatewayItem): string {
  const base = 'admin.accounts.openai.gatewayHistory'
  const live = liveTickets.value.find((ticket) => ticket.gateway === item.name && ticket.verified_models.length > 0)
  if (live) {
    return t('admin.accounts.openai.gatewayRuntime.live', { gateway: item.name, models: live.verified_models.join(', ') })
  }
  const state = cooldownOf(item)
  const verdict = item.verdict === 'full'
    ? t(`${base}.verdicts.fullExpired`)
    : item.verdict
    ? t(`${base}.verdicts.${item.verdict}`)
    : t(`${base}.verdicts.none`)
  const summary = [regionLabel(item.region), shortName(item.name), fullHeldOf(item), state, verdict].join(
    TITLE_SEP
  )
  return gatewayRegionDisplayKey(item.region) === item.region ? summary : `${summary} · ${item.region || '—'}`
}
</script>
