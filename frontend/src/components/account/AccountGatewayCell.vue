<template>
  <div v-if="isCodexAccount" class="space-y-1" data-testid="account-gateway-cell">
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
      <div v-if="cells.length" class="grid grid-cols-3 gap-x-1" data-testid="account-gateway-regions">
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
      <!-- 一小时满血分钟预测。单位是 (账号 × 大区)，算法和口径见 forecastUnits。 -->
      <p
        v-if="cells.length"
        class="text-[9px] leading-3 text-gray-500 dark:text-gray-400"
        :title="forecastTitle"
        data-testid="account-gateway-forecast"
      >
        {{ t('admin.accounts.openai.gatewayHistory.forecast', { minutes: forecastMinutes }) }}
        <span v-if="forecastBlind" class="text-amber-600 dark:text-amber-400">
          {{ t('admin.accounts.openai.gatewayHistory.forecastBlind', { count: forecastBlind }) }}
        </span>
      </p>
      <!-- 图例：四种色的语义原来只写在这个文件的注释里，页面上没有任何地方说，而 tooltip
           是 title 属性、触屏摸不到。 -->
      <p v-if="cells.length" class="text-[9px] leading-3 text-gray-400" data-testid="account-gateway-legend">
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
import { targetsCodexUpstream } from '@/utils/turnState'
import { formatDateTime, formatRelativeTime } from '@/utils/format'
import { useNowTicker } from '@/composables/useNowTicker'

/**
 * 九个大区，顺序照 gwpool 的 types.Regions（页面之间对着看的时候格子位置要一致）。
 * 最后那格 `''` 是「未归类」：老记录没带大区，以及被上游改派走的那些发（池子说的大区
 * 讲的是另一个网关的事，后端刻意不记）。
 */
const REGION_KEYS = [
  'us-east',
  'us-west',
  'south-america',
  'west-europe',
  'europe',
  'east-asia',
  'oceania',
  'south-asia',
  'middle-east',
  ''
] as const

/** 一个大区里列几个网关名，超出的折成 +N。正常情况恒为 1（网关 = 大区 × 账号）。 */
const MAX_PER_REGION = 1

/** 本地账本窗口默认 4 小时 = 槽位冷却，和后端 openai_gwpool_gateway_window_s 的默认值同一个数。 */
const DEFAULT_WINDOW_MS = 4 * 60 * 60 * 1000

/**
 * 满血窗口 183 秒，**必须和后端 openAIGatewayFullWindow 同值**（跨语言，只能靠这条注释）。
 *
 * 183 不是我们测出来的，是取两边最保守的那个：实测窗口是 200–300 秒，而池子自己的
 * types.FullWindow 就是 183 秒、DeliverTTL 只有 150 秒。取大的会让这一格在池子和后端都认为
 * 窗口已关之后还绿着 —— 而运营方正照着它挑落点。
 *
 * 「验过满血」这一格**必须按它判，不能按本地账本那 4 小时**：后端的 verdict 是粘滞的
 * （没判据的那些发只刷新 at、判定原样留着，见 openai_gwpool_gateway_history.go），
 * 按 4 小时着色的话「3 小时 59 分前判过满血、1 分钟前又用过」会和「刚刚验出满血」长得一样 ——
 * 运营方照着那一格去挑落点，挑中的是一个烧了三个多小时的网关。
 *
 * 过期就回落「碰过」（琥珀），不是「没碰过」（淡显）：窗口过了不代表那次接触没发生。
 * 后端对 `full` 判定有一条节流穿透就是为了这个：持续被验成满血的落点，它的 FullAt 至少每
 * 183 秒刷新一次，否则格子会在写节流（5 分钟）的空档里掉成琥珀。
 */
const FULL_WINDOW_MS = 183 * 1000

/** 满血分钟预测往前看多久。一小时同时是预测值的天花板：一小时里最多只能用一小时的满血。 */
const FORECAST_HORIZON_MS = 60 * 60 * 1000

const props = defineProps<{ account: Account }>()
const { t } = useI18n()
const now = useNowTicker()

interface GatewaySeen {
  at?: string
  region?: string
  verdict?: string
  full_at?: string
}

interface GatewayHistory {
  current?: string
  current_region?: string
  seen?: Record<string, GatewaySeen>
  updated_at?: string
}

interface GatewayItem {
  name: string
  at: string
  region: string
  verdict: string
  fullAt: string
}

/**
 * 格子的三种色：绿 = 此刻真的在满血窗口里；红 = 窗口内碰过、现在打过去就是降智；灰 = 已过
 * 本地账本窗口，可以再用。
 *
 * **原来还有一档琥珀**（「碰过没判据，或曾判满血但 183 秒窗口已过」），2026-10-02 并进红色：
 * 那两种情况在「现在能不能用」这个问题上和降智完全等价 —— 满血窗口是 (账号 × 网关) 首次接触
 * 那一下给的，过了就没了，判没判过不改变这个事实。分成两色只会让人以为琥珀比红安全。
 * 历史判定仍然在 tooltip 里（verdict 粘滞保存）。
 *
 * **窗口外不着色是刻意的**：回归的触发变量未知（后端 openAIGatewaySeen.FullAt 的注释），
 * 过了本地账本窗口那条读数就只是历史，不该再当成当前状态渲染。
 */
const TONE_CLASS = {
  full: 'bg-emerald-50 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300',
  degraded: 'bg-rose-50 text-rose-700 dark:bg-rose-900/40 dark:text-rose-300',
  idle: 'bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-400'
} as const

type GatewayTone = keyof typeof TONE_CLASS

const isCodexAccount = computed(() => targetsCodexUpstream(props.account))

const extra = computed(() => (props.account.extra as Record<string, unknown> | undefined) ?? {})

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
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : DEFAULT_WINDOW_MS
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
      fullAt: typeof row.full_at === 'string' ? row.full_at : ''
    }))
    .sort((a, b) => Date.parse(b.at) - Date.parse(a.at))
})

const current = computed<GatewayItem | null>(() => {
  const name = history.value.current
  if (!name) return null
  // 时间和大区取 seen 里那条；没有就退回记录自己的那两个字段（老记录、或被裁过）。
  return (
    items.value.find((i) => i.name === name) ?? {
      name,
      at: history.value.updated_at ?? '',
      region: history.value.current_region ?? '',
      verdict: '',
      fullAt: ''
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
    const key = (REGION_KEYS as readonly string[]).includes(item.region) ? item.region : ''
    const bucket = byRegion.get(key)
    if (bucket) bucket.push(item)
    else byRegion.set(key, [item])
  }
  return REGION_KEYS.filter((key) => key !== '' || byRegion.has(''))
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
        hot: !!bucket.length && isHot(bucket[0].at),
        tone: toneOf(bucket[0]),
        mark: verdictMark(bucket[0]),
        title: bucket.length
          ? bucket.map(titleOf).join('\n')
          : `${regionLabel(key)} · ${t('admin.accounts.openai.gatewayHistory.regionIdle')}`
      }
    })
})

/**
 * 「接下来一小时最多能用到几分钟满血」。
 *
 * 单位是 **(账号 × 大区)**，不是 (账号 × 网关)：一个号在一个大区同一时间只有一个网关，
 * 所以同一个大区下的多个网关名是**同一个**单位（现网 us-west 一个大区就有 20 个不同
 * 网关名，按名字数会把它数 20 遍）。每个大区取它**最近**一次被碰的时刻当冷却起点。
 *
 *	可用单位 = 冷却剩余 ≤ 1 小时的大区 + 从没碰过的大区
 *	预测     = min(可用单位 × 183 秒, 1 小时)
 *
 * 算在前端而不是后端：这是个随时间衰减的值，而后端那条记录有 5 分钟写节流 ——
 * 存进去的预测立刻就过期了。前端这里 now 是跟着 ticker 走的，读数永远是当下的。
 */
const forecastUnits = computed(() => {
  const latest = new Map<string, number>()
  for (const item of items.value) {
    // 未归类（region 为空）的落点归不到单位上，猜一个会把别的单位算重。
    if (!item.region || !(REGION_KEYS as readonly string[]).includes(item.region)) continue
    const ts = Date.parse(item.at)
    if (!Number.isFinite(ts)) continue
    const prev = latest.get(item.region)
    if (prev === undefined || ts > prev) latest.set(item.region, ts)
  }
  // 从没碰过的大区天然可用。REGION_KEYS 末尾那个空串是「未归类」那一格，不算大区。
  let units = REGION_KEYS.filter((key) => key !== '').length - latest.size
  if (units < 0) units = 0
  for (const ts of latest.values()) {
    if (windowMs.value - (now.value - ts) <= FORECAST_HORIZON_MS) units += 1
  }
  return units
})

const forecastMinutes = computed(() =>
  Math.round(Math.min(forecastUnits.value * FULL_WINDOW_MS, FORECAST_HORIZON_MS) / 60_000)
)

/**
 * 还在窗口里、却归不到大区上的落点数。
 *
 * 它是预测**偏乐观多少**的度量：这些落点确实烧掉了某个大区的单位，但记录里没有 region
 * （池子侧续期型铸票的出口反查不出九个代表出口之一时 Pair.RegionKey 会落空，已修，
 * 但**旧记录不会自己补上**）⇒ 预测把那些单位当成了「没碰过」。非零时要在 tooltip 里说。
 */
const forecastBlind = computed(
  () =>
    items.value.filter(
      (item) =>
        (!item.region || !(REGION_KEYS as readonly string[]).includes(item.region)) && isHot(item.at)
    ).length
)

/**
 * 预测的 tooltip。三条**必须**写出来，否则这个数会被当成承诺：
 *
 *  1. 它是**上界**不是承诺 —— 这本账挂在账号行上，而烧灼的单位是上游账号的，
 *     同一份凭据的克隆行/影子行各自只看得见自己发出去的那些 ⇒「烧过」记少了。
 *  2. 4 小时冷却本身**没测准**（静置 30 分钟到 4 小时，满血率恒定、零相关），是工程保守取值。
 *  3. 有未归类落点时数字还会再偏大一截（见 forecastBlind）。
 */
const forecastTitle = computed(() => {
  const base = 'admin.accounts.openai.gatewayHistory'
  const lines = [
    t(`${base}.forecastHint`, { units: forecastUnits.value, window: FULL_WINDOW_MS / 1000 })
  ]
  if (forecastBlind.value) {
    lines.push(t(`${base}.forecastBlindHint`, { count: forecastBlind.value }))
  }
  return lines.join('\n')
})

function within(at: string, span: number): boolean {
  const ts = Date.parse(at)
  return Number.isFinite(ts) && now.value - ts < span
}

function isHot(at: string): boolean {
  return within(at, windowMs.value)
}

function toneOf(item: GatewayItem | null | undefined): GatewayTone {
  if (!item || !isHot(item.at)) return 'idle'
  // 满血只在真实的满血窗口内才算（见 FULL_WINDOW_MS）。过了它、或者压根没判过，都是红：
  // 窗口内碰过 ⇒ 这一刻打过去就是降智，这三种情况对使用者是同一件事。
  if (item.verdict === 'full' && within(item.fullAt, FULL_WINDOW_MS)) return 'full'
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
  return t(`admin.accounts.openai.gatewayHistory.regions.${key || 'unknown'}`)
}

/**
 * tooltip 固定五段：区域 · 网关名 · 满血时刻 · 状态 · 上次判定。
 *
 * 段位固定（没有就写「从未 / 没判过」而不是整段省掉）是刻意的：运营方是竖着扫一列格子看的，
 * 段数会变的话每一行都得重新找「满血时刻」在哪儿。
 *
 * 满血时刻是**判成满血的那一刻**，不是「最近用过」那一刻 —— verdict 在后端是粘滞的
 * （没判据的那些发只刷新 at、判定原样留着），不写时刻的话一条 3 小时前的满血判定读起来
 * 和刚验出来的一样。
 */
function titleOf(item: GatewayItem): string {
  const base = 'admin.accounts.openai.gatewayHistory'
  const full = item.fullAt
    ? t(`${base}.fullAt`, { when: formatDateTime(item.fullAt) })
    : t(`${base}.fullNever`)
  const state = t(isHot(item.at) ? `${base}.regionHot` : `${base}.regionCooled`)
  const verdict = item.verdict
    ? t(`${base}.verdicts.${item.verdict}`)
    : t(`${base}.verdicts.none`)
  return [regionLabel(item.region), item.name, full, state, verdict].join(' · ')
}
</script>
