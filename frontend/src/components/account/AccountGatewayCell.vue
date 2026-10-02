<template>
  <div v-if="isCodexAccount" class="mt-1 space-y-1" data-testid="account-gateway-cell">
    <!-- 没有读数也要占位：整块消失时，「没接网关池」「接了还没跑过流量」「落点读不出来」
         在页面上长得一模一样。非 Codex 上游的账号根本没有落点这回事，那才该整块消失。 -->
    <p v-if="!current && !others.length" class="text-[10px] text-gray-400" data-testid="account-gateway-empty">
      {{ t('admin.accounts.openai.gatewayHistory.empty') }}
    </p>
    <template v-else>
      <!-- 当前落点单独一行、带色：这是整块里最要紧的一个事实。 -->
      <div v-if="current" class="flex items-center gap-1" data-testid="account-gateway-current">
        <span class="shrink-0 text-[10px] text-gray-400">
          {{ t('admin.accounts.openai.gatewayHistory.current') }}
        </span>
        <span
          class="truncate rounded bg-emerald-50 px-1 text-[10px] font-medium leading-4 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300"
          :title="titleOf(current)"
        >
          {{ current.name }}
        </span>
        <span class="shrink-0 text-[10px] text-gray-400">{{ formatRelativeTime(current.at) }}</span>
      </div>
      <!-- 打过的其余网关。(账号 × 网关) 是烧窗口的单位，所以这一串回答的是
           「这个号还剩哪些没碰过的落点」——名字必须列出来，只给个数等于没说。 -->
      <div v-if="others.length" class="flex flex-wrap items-center gap-1" data-testid="account-gateway-seen">
        <span class="shrink-0 text-[10px] text-gray-400">
          {{ t('admin.accounts.openai.gatewayHistory.seen', { n: others.length }) }}
        </span>
        <span
          v-for="item in visibleOthers"
          :key="item.name"
          class="rounded bg-gray-100 px-1 text-[9px] leading-4 text-gray-500 dark:bg-gray-800 dark:text-gray-400"
          :title="titleOf(item)"
        >
          {{ item.name }}
        </span>
        <!-- 超出的那些折进 tooltip，不省略不提示的话这一列会把整行撑开。 -->
        <span
          v-if="hiddenOthers.length"
          class="rounded bg-gray-100 px-1 text-[9px] leading-4 text-gray-500 dark:bg-gray-800 dark:text-gray-400"
          :title="hiddenTitle"
          data-testid="account-gateway-more"
        >
          +{{ hiddenOthers.length }}
        </span>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
/**
 * 账号条目下的网关落点：当前落在哪个网关、历史上打过哪些。
 *
 * 读数来自 account.extra.openai_gwpool_gateways（后端
 * openai_gwpool_gateway_history.go，用量路径上带节流地写），跟着账号列表一起下发，
 * 不额外调接口 —— 和原来那块 turn-state 读数同一条管线。
 *
 * **只是展示**：这条记录挂在账号行上，而真正被烧掉的单位是上游账号（同一份凭据可能挂在
 * 多个行上），各行只看得见自己发出去的那些。拿它判「这个网关还能不能用」会低估烧掉的
 * 范围，那个判定在后端 gatewayPoolUsedRecently。
 */
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { Account } from '@/types'
import { targetsCodexUpstream } from '@/utils/turnState'
import { formatDateTime, formatRelativeTime } from '@/utils/format'

/** 列出来的其余网关最多几个，超出的折进 +N 的 tooltip。 */
const MAX_VISIBLE_OTHERS = 6

const props = defineProps<{ account: Account }>()
const { t } = useI18n()

interface GatewayHistory {
  current?: string
  seen?: Record<string, string>
  updated_at?: string
}

interface GatewayItem {
  name: string
  at: string
}

const isCodexAccount = computed(() => targetsCodexUpstream(props.account))

const history = computed<GatewayHistory>(() => {
  const raw = (props.account.extra as Record<string, unknown> | undefined)?.openai_gwpool_gateways
  return raw && typeof raw === 'object' ? (raw as GatewayHistory) : {}
})

/** 按最近用过的在前排。后端存的是 map，顺序在这里定。 */
const items = computed<GatewayItem[]>(() => {
  const seen = history.value.seen
  if (!seen || typeof seen !== 'object') return []
  return Object.entries(seen)
    .filter(([name, at]) => typeof name === 'string' && name !== '' && typeof at === 'string')
    .map(([name, at]) => ({ name, at }))
    .sort((a, b) => Date.parse(b.at) - Date.parse(a.at))
})

const current = computed<GatewayItem | null>(() => {
  const name = history.value.current
  if (!name) return null
  // 时间取 seen 里那条；没有就退回记录自己的更新时刻（老记录、或被裁过）。
  return items.value.find((i) => i.name === name) ?? { name, at: history.value.updated_at ?? '' }
})

const others = computed(() => items.value.filter((i) => i.name !== current.value?.name))
const visibleOthers = computed(() => others.value.slice(0, MAX_VISIBLE_OTHERS))
const hiddenOthers = computed(() => others.value.slice(MAX_VISIBLE_OTHERS))

function titleOf(item: GatewayItem): string {
  if (!item.at) return item.name
  return `${item.name} · ${t('admin.accounts.openai.gatewayHistory.lastUsed')} ${formatDateTime(item.at)}`
}

const hiddenTitle = computed(() => hiddenOthers.value.map(titleOf).join('\n'))
</script>
