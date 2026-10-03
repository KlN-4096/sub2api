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
      <!-- 一小时满血分钟预测。单位是 (账号 × 网关)，算法和口径见 forecastUnits。
           上行空间（本行没碰过的网关）只在 tooltip 里定性说一句：这一行不知道池子一共有
           多少网关，给不出数。 -->
      <!-- 窗口用量：这一条回答「现在手上还有几个落点能用」，和下面那条「能打多少分钟」
           分开两行 —— 合成一句的话「0 个落点」和「0 分钟」会被读成同一件事。 -->
      <p
        v-if="usesPool && cells.length"
        class="text-[9px] leading-3 text-gray-500 dark:text-gray-400"
        :title="t('admin.accounts.openai.gatewayHistory.windowUsageHint')"
        data-testid="account-gateway-window-usage"
      >
        {{
          windowUsage.measured
            ? t('admin.accounts.openai.gatewayHistory.windowUsage', {
                hours: windowHours,
                used: windowUsage.used,
                live: windowUsage.live,
                free: windowUsage.free
              })
            : t('admin.accounts.openai.gatewayHistory.windowUsageUsedOnly', {
                hours: windowHours,
                used: windowUsage.used
              })
        }}
      </p>
      <p
        v-if="usesPool && cells.length"
        class="text-[9px] leading-3 text-gray-500 dark:text-gray-400"
        :title="forecastTitle"
        data-testid="account-gateway-forecast"
      >
        {{
          forecastMinutes > 0
            ? t('admin.accounts.openai.gatewayHistory.forecast', { minutes: forecastMinutes })
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
import { targetsCodexUpstream } from '@/utils/turnState'
import { formatRelativeTime } from '@/utils/format'
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
          : `${regionLabel(key)}${TITLE_SEP}${t('admin.accounts.openai.gatewayHistory.regionIdle')}`
      }
    })
})

/**
 * 「接下来一小时**至少**能用到几分钟满血」—— 刻意算**下界**。
 *
 * **单位是 (账号 × 网关)，一个网关名就是一个单位。**（2026-10-03 用户纠正：「时间还是
 * 按网关来的，相同区域不同网关同一个号还是有不同的满血期的」。第一版按大区去重，把
 * us-west 那 20 个网关名算成 1 个单位 ⇒ 预测值低一个数量级。大区只适合当分组展示用，
 * 就是上面那个九宫格，不能当去重键。）
 *
 * items 本身已经是一行一个网关名（后端 seen 是以网关名为键的 map），所以这里不用再去重
 * —— 加一层去重就是上面那个错。region 在这个计算里完全不参与：没带 region 的落点一样是
 * 一个有名有姓的网关，照数。
 *
 * 只数有**正面证据**的单位：账本里有这个网关的记录，而且它的冷却在一小时内结束。
 * 两处刻意悲观：
 *
 *  1. **从没碰过的网关不计入数字**。这本账只看得见**本行**发出去的那些（克隆行/影子行
 *     各看各的，见 openai_gwpool_gateway_history.go 开头那段口径），而且
 *     openAIGatewayHistoryMax 从 24 提到 201 之前写下的行被按时间裁过 ⇒「本行没碰过」
 *     完全可能是「别的行烧过了」或「记录被裁了」。更要紧的是这里**算不出**那个上行空间：
 *     池子一共有多少网关这一行不知道（现网 105 个，但那是服务端的事）⇒ 宁可只在 tooltip
 *     里定性说一句，不编一个数。
 *  2. 一个单位只按一个满血窗口算（183 秒，取的是实测 200–300 秒里最保守的那个）。
 *
 * 算在前端而不是后端：这是个随时间衰减的值，而后端那条记录有 5 分钟写节流 ——
 * 存进去的预测立刻就过期了。前端这里 now 是跟着 ticker 走的，读数永远是当下的。
 */
const forecastUnits = computed(() => {
  let units = 0
  for (const item of items.value) {
    const ts = Date.parse(item.at)
    if (!Number.isFinite(ts)) continue
    if (windowMs.value - (now.value - ts) <= FORECAST_HORIZON_MS) units += 1
  }
  return units
})

const forecastMinutes = computed(() =>
  Math.round(Math.min(forecastUnits.value * FULL_WINDOW_MS, FORECAST_HORIZON_MS) / 60_000)
)

/**
 * 窗口用量：本地账本窗口里烧掉了几个落点，以及池子此刻能交付几个、其中还有几个没烧过。
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
 * 和 forecast 分开一行：这条回答「现在还有几个落点能用」，forecast 回答「接下来一小时能
 * 打多少分钟」。合成一句的话「0 个落点」和「0 分钟」会被读成同一件事。
 */
const windowHours = computed(() => +(windowMs.value / 3_600_000).toFixed(1))

const windowUsage = computed(() => {
  let used = 0
  for (const item of items.value) {
    if (isHot(item.at)) used += 1
  }
  const live = typeof history.value.pool_live === 'number' ? history.value.pool_live : 0
  const free = typeof history.value.pool_free === 'number' ? history.value.pool_free : 0
  return { used, live, free, measured: live > 0 }
})

/**
 * 预测的 tooltip。要交代清楚这个数是**下界**，以及它往哪两个方向偏：
 *
 *  - 往大偏（上行空间）：本行没碰过的网关不计入，实际可能更多。定性说，不给数 ——
 *    这一行不知道池子一共有多少网关（见 forecastUnits 第 1 条）。
 *  - 往小偏（仍然乐观的那一处）：4 小时冷却本身**没测准**（静置 30 分钟到 4 小时，
 *    满血率恒定、零相关），真实恢复时间比它长的话这个数还是会偏大。
 */
const forecastTitle = computed(() => {
  const base = 'admin.accounts.openai.gatewayHistory'
  return t(`${base}.forecastHint`, {
    units: forecastUnits.value,
    window: FULL_WINDOW_MS / 1000
  })
})

function within(at: string, span: number): boolean {
  const ts = Date.parse(at)
  return Number.isFinite(ts) && now.value - ts < span
}

function isHot(at: string): boolean {
  return within(at, windowMs.value)
}

function toneOf(item: GatewayItem | null | undefined): GatewayTone {
  // 没开网关池的号一律中性：见 usesPool 的注释，它的 verdict 恒为空，不拦的话下面那条
  // 兜底会把每个最近用过的落点都染红。
  if (!usesPool.value) return 'idle'
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
 * 满血时长：从判成满血（fullAt）到判成降智（at）的那一段，只有**窗口已经结束**才有数。
 *
 * 两头都是后端已经落下的读数，不用新字段：verdict 变成 degraded 必须穿过 5 分钟节流
 * （noteOpenAIGatewayUse 的注释），所以降智那一刻的 at 就是窗口的收尾时刻。
 *
 * 回「未计时」的三种情况合成一个标签，因为它们对读者是同一件事——**这一格没有时长读数**：
 *   - verdict 还是 full：窗口正在跑，这时候报的任何数都只是「到目前为止」，会被当成结果；
 *   - 从没验出过满血：窗口压根没开过，没有起点；
 *   - 算出来不是正数：两条读数来自同一次写入（判满血和判降智挤在一次节流里），测不出长度。
 */
function fullHeldOf(item: GatewayItem): string {
  const base = 'admin.accounts.openai.gatewayHistory'
  if (item.verdict !== 'degraded' || !item.fullAt || !item.at) return t(`${base}.fullUntimed`)
  const held = new Date(item.at).getTime() - new Date(item.fullAt).getTime()
  if (!(held > 0)) return t(`${base}.fullUntimed`)
  return `${Math.round(held / 1000)}s`
}

function titleOf(item: GatewayItem): string {
  const base = 'admin.accounts.openai.gatewayHistory'
  const state = t(isHot(item.at) ? `${base}.regionHot` : `${base}.regionCooled`)
  const verdict = item.verdict
    ? t(`${base}.verdicts.${item.verdict}`)
    : t(`${base}.verdicts.none`)
  return [regionLabel(item.region), shortName(item.name), fullHeldOf(item), state, verdict].join(
    TITLE_SEP
  )
}
</script>
