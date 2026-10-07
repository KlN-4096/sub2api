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
        <span
          class="truncate rounded bg-emerald-50 px-1 text-[10px] font-medium leading-4 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300"
          :title="titleOf(current)"
        >
          {{ regionLabel(current.region) }} · {{ current.name }}
        </span>
        <span class="shrink-0 text-[10px] text-gray-400">{{ formatRelativeTime(current.at) }}</span>
      </div>
      <!-- 九个大区各自落在哪个网关。满血窗口的单位是 (账号 × 网关)，而网关 = (大区 × 账号)
           ⇒ 这张格子回答的是「这个号现在还能去哪个大区铸没烧过的票」：窗口内打过的高亮
           （还烧着），窗口外的淡显（那个大区又能用了）。和网关池页面那张九宫格同一把尺子。 -->
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
        >
          <span class="shrink-0">{{ cell.label }}</span>
          <span
            v-if="cell.name"
            class="truncate rounded px-0.5"
            :class="
              cell.hot
                ? 'bg-amber-50 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300'
                : 'bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-400'
            "
          >
            {{ cell.name }}<template v-if="cell.extra">+{{ cell.extra }}</template>
          </span>
          <span v-else class="text-gray-300 dark:text-gray-600">-</span>
        </span>
      </div>
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

const props = defineProps<{ account: Account }>()
const { t } = useI18n()
const now = useNowTicker()

interface GatewaySeen {
  at?: string
  region?: string
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
}

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
      region: typeof row.region === 'string' ? row.region : ''
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
      region: history.value.current_region ?? ''
    }
  )
})

interface RegionCell {
  key: string
  label: string
  name: string
  extra: number
  hot: boolean
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
        // 窗口内打过 ⇒ 这个大区的落点还烧着。空格子永远不是「烧着」。
        hot: bucket.some((i) => isHot(i.at)),
        title: bucket.length
          ? bucket.map(titleOf).join('\n')
          : `${regionLabel(key)} · ${t('admin.accounts.openai.gatewayHistory.regionIdle')}`
      }
    })
})

function isHot(at: string): boolean {
  const ts = Date.parse(at)
  return Number.isFinite(ts) && now.value - ts < windowMs.value
}

/** `unified-` 是恒定前缀，这一列按格子排，省掉它才塞得下大区名。 */
function shortName(name: string): string {
  return name.startsWith('unified-') ? name.slice('unified-'.length) : name
}

function regionLabel(key: string): string {
  return t(`admin.accounts.openai.gatewayHistory.regions.${key || 'unknown'}`)
}

function titleOf(item: GatewayItem): string {
  const head = `${regionLabel(item.region)} · ${item.name}`
  if (!item.at) return head
  const when = `${t('admin.accounts.openai.gatewayHistory.lastUsed')} ${formatDateTime(item.at)}`
  const state = t(
    isHot(item.at)
      ? 'admin.accounts.openai.gatewayHistory.regionHot'
      : 'admin.accounts.openai.gatewayHistory.regionCooled'
  )
  return `${head} · ${when} · ${state}`
}
</script>
