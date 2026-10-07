<template>
  <div v-if="isCodexAccount" class="space-y-1" data-testid="account-gateway-cell">
    <p v-if="usesPool && progress && progress.phase !== 'idle'" class="text-[10px] text-primary-600 dark:text-primary-400"
      role="status" aria-live="polite" data-testid="account-gateway-progress">
      <span v-if="progress.run_id">{{ t('admin.accounts.openai.gatewayProgress.run', { id: progress.run_id }) }} · </span>
      {{ t(`admin.accounts.openai.gatewayProgress.${progress.phase}`, {
        attempt: progress.attempt, limit: progress.limit, seconds: Math.floor(progress.elapsed_ms / 1000)
      }) }}
      <span v-if="progress.rejected"> · {{ t('admin.accounts.openai.gatewayProgress.rejected', { count: progress.rejected }) }}</span>
      <span v-if="progress.active_requests > 1"> · {{ t('admin.accounts.openai.gatewayProgress.concurrent', { count: progress.active_requests }) }}</span>
    </p>
    <template v-if="usesPool && runtime">
      <p v-for="ticket in liveTickets" :key="ticket.gateway" class="text-[10px] text-gray-500" data-testid="account-gateway-live">
        {{ t('admin.accounts.openai.gatewayRuntime.live', { gateway: ticket.gateway, seconds: Math.max(0, Math.floor((Date.parse(ticket.expires_at) - now) / 1000)), models: ticket.verified_models.join(', ') || t('admin.accounts.openai.gatewayRuntime.unverified') }) }}
      </p>
      <p v-for="round in activeRounds" :key="round.id" class="text-[10px] text-gray-500" data-testid="account-gateway-usage-round">
        {{ t('admin.accounts.openai.gatewayRuntime.active', { model: round.model, full: round.full, attempted: round.attempted, seconds: roundSeconds(round) }) }}
        <span v-if="round.incomplete">{{ t('admin.accounts.openai.gatewayRuntime.incomplete') }}</span>
      </p>
      <details v-if="runtime.rounds.length || Object.keys(runtime.archived || {}).length" class="text-[10px] text-gray-500" data-testid="account-gateway-usage-history">
        <summary>{{ t('admin.accounts.openai.gatewayRuntime.history') }}</summary>
        <p>{{ t('admin.accounts.openai.gatewayRuntime.hint') }}</p>
        <p v-for="round in endedRounds" :key="round.id">
          {{ t('admin.accounts.openai.gatewayRuntime.ended', { model: round.model, full: round.full, attempted: round.attempted, seconds: roundSeconds(round), start: formatRelativeTime(round.started_at), end: formatRelativeTime(round.ended_at!) }) }}
          <span v-if="round.incomplete">{{ t('admin.accounts.openai.gatewayRuntime.incomplete') }}</span>
        </p>
        <p v-for="(total, model) in runtime.archived" :key="model">
          {{ t('admin.accounts.openai.gatewayRuntime.archived', { model, count: total.rounds, full: total.full, attempted: total.attempted, seconds: Math.floor(total.duration_ms / 1000) }) }}
          <span v-if="total.incomplete">{{ t('admin.accounts.openai.gatewayRuntime.incomplete') }}</span>
        </p>
        <p v-if="runtime.incomplete">{{ t('admin.accounts.openai.gatewayRuntime.incomplete') }}</p>
      </details>
    </template>
    <p v-if="usesPool && progressUnavailable" class="text-[10px] text-gray-400" data-testid="account-gateway-progress-unavailable">
      {{ t('admin.accounts.openai.gatewayProgress.unavailable') }}
    </p>
    <!-- 没有读数也要占位：整块消失时，「没接网关池」「接了还没跑过流量」「落点读不出来」
         在页面上长得一模一样。非 Codex 上游的账号根本没有落点这回事，那才该整块消失。 -->
    <p v-if="!current && !cells.length" class="text-[10px] text-gray-400" data-testid="account-gateway-empty">
      {{ t('admin.accounts.openai.gatewayHistory.empty') }}
    </p>
    <template v-else>
      <!-- 第一行是「当前大区 · 当前网关」：整块里最要紧的一个事实。 -->
      <div v-if="current" class="flex items-center gap-1" data-testid="account-gateway-current">
        <span class="shrink-0 text-[10px] text-gray-400">
          {{ t('admin.accounts.openai.gatewayHistory.current') }}
        </span>
        <!-- 色和下面九宫格同一把尺子：这个 chip 原来恒为绿，而同一个落点在格子里可能是红的，
             同一张卡上一绿一红指着同一件事。 -->
        <span
          class="truncate rounded px-1 text-[10px] font-medium leading-4"
          :class="TONE_CLASS[toneOf(current)]"
          :title="titleOf(current)"
        >
          {{ verdictMark(current) }}{{ regionLabel(current.region) }} · {{ current.name }}
        </span>
        <span class="shrink-0 text-[10px] text-gray-400">{{ formatRelativeTime(current.at) }}</span>
      </div>
      <!-- 九个大区各自落在哪个网关。满血窗口的单位是 (账号 × 网关)，而网关 = (大区 × 账号)
           ⇒ 这张格子回答的是「这个号现在还能去哪个大区铸没烧过的票」：窗口内打过的高亮
           （还烧着），窗口外的淡显（那个大区又能用了）。和网关池页面那张九宫格同一把尺子。
           窗口内再按 state-echo 判定分色：绿=验过满血、红=判过降智、黄=碰过但没判据。 -->
      <div v-if="usesPool && cells.length" class="grid grid-cols-3 gap-x-1" data-testid="account-gateway-regions">
        <span
          v-for="cell in cells"
          :key="cell.key"
          class="flex items-center gap-0.5 truncate text-[9px] leading-4"
          :class="
            cell.hot
              ? 'font-medium text-gray-600 dark:text-gray-300'
              : 'text-gray-400 dark:text-gray-500'
          "
          :title="cell.title"
          :data-testid="`account-gateway-region-${cell.key}`"
          :data-tone="cell.tone"
        >
          <span class="shrink-0">{{ cell.label }}</span>
          <!-- 判定用**字符**打头而不是只靠颜色：这一列是 9px 字号，emerald/rose 同明度，
               红绿色盲分不出来；而 tooltip 是 title 属性，触屏上摸不到。 -->
          <span v-if="cell.name" class="truncate rounded px-0.5" :class="TONE_CLASS[cell.tone]">
            {{ cell.mark }}{{ cell.name }}<template v-if="cell.extra">+{{ cell.extra }}</template>
          </span>
          <span v-else class="text-gray-300 dark:text-gray-600">-</span>
        </span>
      </div>
      <!-- 一小时满血分钟预测。单位是 (账号 × 网关)，算法和口径见 forecastGatewayMinutes。
           上行空间（本行没碰过的网关）只在 tooltip 里定性说一句：这一行不知道池子一共有
           多少网关，给不出数。 -->
      <!-- 本地冷却随时钟更新；库存是旧查询快照，两者分行避免把快照读成实时余额。 -->
      <p
        v-if="usesPool && cells.length"
        class="text-[9px] leading-3 text-gray-500 dark:text-gray-400"
        :title="t('admin.accounts.openai.gatewayHistory.windowUsageHint')"
        data-testid="account-gateway-window-usage"
      >
        {{
          t('admin.accounts.openai.gatewayHistory.windowUsage', {
            hours: windowHours,
            used: windowUsage.used,
            cooled: windowUsage.cooled
          })
        }}
      </p>
      <p
        v-if="usesPool && cells.length && windowUsage.measured"
        class="text-[9px] leading-3 text-gray-400"
        :title="t('admin.accounts.openai.gatewayHistory.windowUsageHint')"
        data-testid="account-gateway-pool-snapshot"
      >
        {{ t('admin.accounts.openai.gatewayHistory.poolSnapshot', { free: windowUsage.free }) }}
      </p>
      <p
        v-if="usesPool && cells.length"
        class="text-[9px] leading-3 text-gray-500 dark:text-gray-400"
        :title="forecastTitle"
        data-testid="account-gateway-forecast"
      >
        {{
          forecast.minutes === null
            ? t('admin.accounts.openai.gatewayHistory.forecastPending')
            : forecast.units
              ? t('admin.accounts.openai.gatewayHistory.forecast', { minutes: forecast.minutes })
              : t('admin.accounts.openai.gatewayHistory.forecastNone')
        }}
      </p>
      <!-- 图例：四种色的语义原来只写在这个文件的注释里，页面上没有任何地方说，而 tooltip
           是 title 属性、触屏摸不到。 -->
      <p
        v-if="usesPool && cells.length"
        class="text-[9px] leading-3 text-gray-400"
        data-testid="account-gateway-legend"
      >
        {{ t('admin.accounts.openai.gatewayHistory.legend') }}
      </p>
    </template>
  </div>
</template>

<script setup lang="ts">
/**
 * 账号条目的网关落点列：当前落在哪个大区的哪个网关、九个大区各自打过哪个。
 *
 * 读数来自 account.extra.openai_gwpool_gateways（后端
 * openai_gwpool_gateway_history.go，用量路径上带节流地写），跟着账号列表一起下发，
 * 不额外调接口 —— 和原来那块 turn-state 读数同一条管线。
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
import { useNowTicker } from '@/composables/useNowTicker'
import { GATEWAY_REGION_KEYS, gatewayRegionDisplayKey } from '@/utils/gatewayRegionDisplay'
import {
  CONTACT_MIN_RESULTS, CONTACT_MIN_WINDOWS,
  readGatewayContacts, forecastGatewayMinutes
} from '@/utils/gatewayContactStats'

/**
 * 九个大区，顺序照 gwpool 的 types.Regions（页面之间对着看的时候格子位置要一致）。
 * 最后那格 `''` 是「未归类」：老记录没带大区，以及被上游改派走的那些发（池子说的大区
 * 讲的是另一个网关的事，后端刻意不记）。
 */
const REGION_KEYS = GATEWAY_REGION_KEYS

/** 一个大区里列几个网关名，超出的折成 +N。正常情况恒为 1（网关 = 大区 × 账号）。 */
const MAX_PER_REGION = 1

/** 初始冷却默认 1 小时；有学习状态时优先使用每个网关自己的截止时间。 */
const DEFAULT_WINDOW_MS = 60 * 60 * 1000
const MAX_WINDOW_MS = 24 * 60 * 60 * 1000

// Three missed 2s polls invalidate the live snapshot; history never turns green.
const LIVE_SNAPSHOT_MAX_AGE_MS = 6_000

const props = defineProps<{ account: Account; progress?: GatewayPoolProgress; progressUnavailable?: boolean }>()
const { t } = useI18n()
const now = useNowTicker()
const runtime = computed(() => props.progressUnavailable ? undefined : props.progress?.runtime)
const liveTickets = computed(() => {
  const snapshot = runtime.value
  if (!snapshot || now.value - Date.parse(snapshot.observed_at) > LIVE_SNAPSHOT_MAX_AGE_MS) return []
  return snapshot.tickets.filter((ticket) => Date.parse(ticket.expires_at) > now.value)
})
const activeRounds = computed(() => runtime.value?.rounds.filter((round) => !round.ended_at) || [])
const endedRounds = computed(() => runtime.value?.rounds.filter((round) => !!round.ended_at) || [])
function roundSeconds(round: GatewayPoolUsageRound): number {
  return Math.max(0, Math.floor(((round.ended_at ? Date.parse(round.ended_at) : now.value) - Date.parse(round.started_at)) / 1000))
}

interface GatewaySeen {
  at?: string
  region?: string
  verdict?: string
  full_at?: string
  /** 后端在判降智那一刻量到的满血时长（毫秒）。缺省 / 0 = 没量到。 */
  full_held_ms?: number
  cooldown?: { until?: string; window_seconds?: number; fixed_seconds?: number; recommended_seconds?: number }
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
  cooldownWindowMs: number
  fixedSeconds: number
  recommendedSeconds: number
}

// Green requires a current leased ticket with explicit model proofs. Red means
// local cooldown, not a new upstream judgment; grey only permits another attempt.
const TONE_CLASS = {
  full: 'bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300',
  degraded: 'bg-rose-50 text-rose-700 dark:bg-rose-900/40 dark:text-rose-300',
  idle: 'bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-400'
} as const

type GatewayTone = keyof typeof TONE_CLASS

const isCodexAccount = computed(() => targetsCodexUpstream(props.account))

const extra = computed(() => (props.account.extra as Record<string, unknown> | undefined) ?? {})

/**
 * 这个号的路由 cookie 是不是由网关池下发（账号上的 `openai_gwpool` 开关）。
 *
 * **没开的号只显示落点本身，不套烧灼那一套。** 烧灼模型的三件东西对它全都不成立：
 *   - 颜色：红的含义是「窗口内碰过 ⇒ 现在打过去就是降智」，而那是针对**取票轮换**说的。
 *     没开池子的号根本不选落点，上游把它路由到哪儿就是哪儿，红色读起来像「这个号废了」。
 *     而且 state-echo 判据只在池子那条传输路径上跑 ⇒ 它的 verdict 恒为空 ⇒ toneOf 的兜底
 *     把**每一个**最近用过的落点都染成红的。这正是误导的来源。
 *   - 一小时满血预测：分子是「冷却到期的落点数」，而它没有冷却这回事。
 *   - 九宫格：它回答「这个号还能去哪个大区铸没烧过的票」，而它不铸票。
 */
const usesPool = computed(() => extra.value.openai_gwpool === true)

const history = computed<GatewayHistory>(() => {
  const raw = extra.value.openai_gwpool_gateways
  return raw && typeof raw === 'object' ? (raw as GatewayHistory) : {}
})

const contacts = computed(() => readGatewayContacts(
  extra.value.openai_gwpool_contacts, extra.value.openai_gwpool_ledger_tag, now.value
))
const contactSource = (value: string) => t(`admin.accounts.openai.gatewayHistory.contactSources.${value}`)

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
      cooldownWindowMs: typeof row.cooldown?.window_seconds === 'number'
        && row.cooldown.window_seconds > 0 && row.cooldown.window_seconds * 1000 <= MAX_WINDOW_MS
        ? row.cooldown.window_seconds * 1000 : windowMs.value,
      fixedSeconds: typeof row.cooldown?.fixed_seconds === 'number' ? row.cooldown.fixed_seconds : 0,
      recommendedSeconds: typeof row.cooldown?.recommended_seconds === 'number'
        ? row.cooldown.recommended_seconds : 0
    }))
    .sort((a, b) => Date.parse(b.at) - Date.parse(a.at))
})

const current = computed<GatewayItem | null>(() => {
  const live = liveTickets.value[0]
  const name = live?.gateway || history.value.current
  if (!name) return null
  // 时间和大区取 seen 里那条；没有就退回记录自己的那两个字段（老记录、或被裁过）。
  return (
    items.value.find((i) => i.name === name) ?? {
      name,
      at: history.value.updated_at ?? '',
      region: live?.region || history.value.current_region || '',
      verdict: '',
      fullAt: '',
      fullHeldMs: 0,
      cooldownUntil: '',
      cooldownWindowMs: windowMs.value,
      fixedSeconds: 0,
      recommendedSeconds: 0
    }
  )
})

interface RegionCell {
  key: string
  label: string
  name: string
  extra: number
  hot: boolean
  tone: GatewayTone
  mark: string
  title: string
}

/**
 * 九个大区（外加「未归类」）各自的格子。一个大区里只该有一个网关，多出来的折成 +N 并
 * 进 tooltip —— 真出现那是漂移的证据，不该被悄悄吞掉。
 */
const cells = computed<RegionCell[]>(() => {
  if (!items.value.length) return []
  const byRegion = new Map<string, GatewayItem[]>()
  for (const item of items.value) {
    const key = gatewayRegionDisplayKey(item.region)
    const bucket = byRegion.get(key)
    if (bucket) bucket.push(item)
    else byRegion.set(key, [item])
  }
  return REGION_KEYS
    .filter((key) => key !== '' || byRegion.has(key))
    .map((key) => {
      const bucket = byRegion.get(key) ?? []
      const shown = bucket.slice(0, MAX_PER_REGION)
      return {
        key: key || 'unknown',
        label: regionLabel(key),
        name: shown.map((i) => shortName(i.name)).join(' '),
        extra: bucket.length - shown.length,
        // 外层高亮和内层的色**必须看同一条记录**：原来 hot 用 some()、tone 用 bucket[0]，
        // 一个大区里最新那个已出窗口而旧的还在窗口内时，外层按「烧着」渲染、内层按淡显渲染。
        // 统一按最近那一条（bucket 已按时间倒排）：一个大区正常只有一个网关，真出现多个时
        // 最新那条才是当前状态，老的在 tooltip 里。
        hot: !!bucket.length && isHot(bucket[0]),
        tone: toneOf(bucket[0]),
        mark: verdictMark(bucket[0]),
        title: bucket.length
          ? bucket.map(titleOf).join('\n')
          : `${regionLabel(key)}${TITLE_SEP}${t('admin.accounts.openai.gatewayHistory.regionIdle')}`
      }
    })
})

// Each gateway is a separate unit. Model/source/criterion and actual resting
// interval must match observed repeat contacts; no fallback to a fixed lifetime.
const forecast = computed(() => forecastGatewayMinutes(
  contacts.value, items.value.map((item) => ({ name: item.name, retryAt: cooldownDeadline(item) })), now.value
))

/**
 * 本地冷却实时计数；池子数字只是最近一次成功保存的查询快照，不随本地到期递增。
 *
 * **free 直接读后端的 pool_free，不在这里减。** 2026-10-03 第一版写的是
 * `pool_live - used`：账本装的是过去一个窗口里碰过的网关名（票早过期的也在里面），而
 * pool_live 是此刻还有活票的，两个集合不是包含关系 ⇒ 相减能出负数，夹到 0 就渲染成
 * 「池子里还剩 0 个没用」。现网当场撞上：账本 67、可交付 62，卡片报成 0，而池子好好的。
 * 正确的数由后端 gatewayPoolPick 在遍历清单时当场数出来（那一遍本来就逐个问过本地账本）。
 *
 * pool_live=0 = 没问到清单（关了 steering、或者清单一直打不开）⇒ 只报已用，不编分母。
 * pool_live>0 时 pool_free=0 是**真的 0**（可交付的全烧过了），照报。
 *
 * 冷却到期不保证仍有票，也不保证已恢复满血。
 */
const windowHours = computed(() => +(windowMs.value / 3_600_000).toFixed(1))

const windowUsage = computed(() => {
  let used = 0
  let cooled = 0
  for (const item of items.value) {
    const deadline = cooldownDeadline(item)
    if (!Number.isFinite(deadline)) continue
    if (deadline > now.value) used += 1
    else cooled += 1
  }
  const live = typeof history.value.pool_live === 'number' ? history.value.pool_live : 0
  const free = history.value.pool_free
  // **两个字段都在**才算测到。只看 live 的话，klno.3 及更早写下的记录（有 live、没有
  // pool_free）会把缺字段当成 0，渲染出「可交付 61 个，其中 0 个没烧过」—— 正是这次要修
  // 的那句假话，换了个来源。后端那边 pool_free 刻意不带 omitempty，所以真的 0 会出现在
  // JSON 里，缺席只可能是老记录。
  const measured = live > 0 && typeof free === 'number'
  return { used, cooled, live, free: measured ? (free as number) : 0, measured }
})

const forecastTitle = computed(() => {
  const base = 'admin.accounts.openai.gatewayHistory'
  return t(`${base}.forecastHint`, {
    units: forecast.value.units, model: forecast.value.model || '—',
    source: forecast.value.source ? contactSource(forecast.value.source) : '—',
    results: CONTACT_MIN_RESULTS, windows: CONTACT_MIN_WINDOWS
  })
})

function cooldownDeadline(item: GatewayItem): number {
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
  if (!item || !isHot(item)) return 'idle'
  return 'degraded'
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

/** `unified-` 是恒定前缀，这一列按格子排，省掉它才塞得下大区名。 */
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
