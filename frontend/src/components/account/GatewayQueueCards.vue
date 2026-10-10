<template>
  <section class="space-y-1.5 border-y border-gray-100 py-1.5 dark:border-gray-700" data-testid="account-gateway-queues">
    <div class="flex min-w-0 flex-wrap items-center justify-between gap-x-2 gap-y-1 text-[10px] text-gray-500 dark:text-gray-400">
      <span v-if="known && snapshot?.next_gateway" :title="t('admin.accounts.openai.gatewayQueues.orderHint')" data-testid="gateway-queue-next">
        {{ t('admin.accounts.openai.gatewayQueues.next', { name: snapshot.next_gateway.replace(/^unified-/, '') }) }}
      </span>
      <span v-else :title="t('admin.accounts.openai.gatewayQueues.hint')">{{ t('admin.accounts.openai.gatewayQueues.title') }}</span>
      <span v-if="stale" data-testid="gateway-queue-stale" :title="t('admin.accounts.openai.gatewayQueues.staleHint')">
        {{ t('admin.accounts.openai.gatewayQueues.stale') }}
      </span>
      <span v-if="known && snapshot?.observed_at" :title="`${model} · ${snapshot.observed_at}`" data-testid="gateway-queue-age">
        {{ formatRelativeTime(snapshot.observed_at, now) }}
      </span>
    </div>
    <div v-for="group in groups" :key="group.key"
      class="grid grid-cols-[76px_minmax(0,1fr)] items-center gap-2 rounded-md bg-gray-100 p-1.5 text-left text-[11px] dark:bg-gray-800"
      data-testid="gateway-queue-card">
      <div class="flex items-center justify-between gap-1">
        <span class="min-w-0 truncate font-medium text-gray-700 dark:text-gray-300" :title="t(`admin.accounts.openai.gatewayQueues.${group.key}Hint`)">
          {{ t(`admin.accounts.openai.gatewayQueues.${group.key}`) }}
        </span>
        <span class="shrink-0 tabular-nums text-gray-500 dark:text-gray-400" data-testid="gateway-queue-count">{{ group.value?.count ?? '—' }}</span>
      </div>
      <div class="flex min-w-0 flex-wrap items-center gap-1">
        <template v-if="group.value">
          <span v-for="(name, index) in group.value.gateways" :key="name"
            class="inline-flex min-w-[29px] max-w-[60px] items-baseline gap-1 rounded bg-white px-1 py-0.5 text-center tabular-nums text-gray-700 dark:bg-dark-900 dark:text-gray-300"
            :class="name === snapshot?.next_gateway ? 'ring-1 ring-primary-400/60' : ''"
            :data-position="group.value.positions?.[index]" data-testid="gateway-queue-entry"
            :title="t('admin.accounts.openai.gatewayQueues.candidateHint', { name })">
            <span v-if="group.value.positions" class="shrink-0 text-[9px] text-gray-400">{{ group.value.positions[index] }}.</span>
            <span class="truncate" data-testid="gateway-queue-name"
              :title="t('admin.accounts.openai.gatewayQueues.candidateHint', { name })">{{ name.replace(/^unified-/, '') }}</span>
          </span>
          <span v-if="group.value.count > group.value.gateways.length" class="text-gray-500 dark:text-gray-400" data-testid="gateway-queue-more"
            :title="t('admin.accounts.openai.gatewayQueues.more', { count: group.value.count - group.value.gateways.length })">
            +{{ group.value.count - group.value.gateways.length }}
          </span>
          <span v-if="group.value.count === 0" class="text-gray-500 dark:text-gray-400">{{ t('admin.accounts.openai.gatewayQueues.empty') }}</span>
        </template>
        <span v-else class="text-gray-500 dark:text-gray-400">{{ t('admin.accounts.openai.gatewayQueues.unknown') }}</span>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { GatewayPoolQueueGroup, GatewayPoolQueueView } from '@/api/admin/accounts'
import { formatRelativeTime } from '@/utils/format'

const props = defineProps<{ snapshot?: GatewayPoolQueueView | null; now: number }>()
const { t } = useI18n()
const MAX_PREVIEW_GATEWAYS = 3
function validGroup(group: GatewayPoolQueueGroup | undefined): group is GatewayPoolQueueGroup {
  return !!group && Number.isSafeInteger(group.count) && group.count >= 0 &&
    Array.isArray(group.gateways) && group.gateways.length === Math.min(group.count, MAX_PREVIEW_GATEWAYS) &&
    group.gateways.every(name => typeof name === 'string' && name.trim().length > 0) &&
    new Set(group.gateways).size === group.gateways.length &&
    (group.positions === undefined || (Array.isArray(group.positions) &&
      group.positions.length === group.gateways.length &&
      group.positions.every((position, index) => Number.isSafeInteger(position) && position > 0 &&
        (index === 0 || position > group.positions![index - 1]))))
}
const known = computed(() => {
  const value = props.snapshot
  if (!value || typeof value.model !== 'string' || value.model.trim() === '' ||
    !Number.isFinite(Date.parse(value.valid_until)) ||
    (value.observed_at !== undefined && !Number.isFinite(Date.parse(value.observed_at))) ||
    !validGroup(value.quality) || !validGroup(value.ordinary)) return false
  const names = [...value.quality.gateways, ...value.ordinary.gateways]
  const positions = [...(value.quality.positions ?? []), ...(value.ordinary.positions ?? [])]
  const nextPosition = [value.quality, value.ordinary].flatMap(group =>
    group.gateways.map((name, index) => ({ name, position: group.positions?.[index] }))
  ).find(entry => entry.name === value.next_gateway)?.position
  return new Set(names).size === names.length && new Set(positions).size === positions.length &&
    positions.every(position => position <= value.quality.count + value.ordinary.count) &&
    (value.next_gateway === undefined || value.next_gateway === '' ||
      (typeof value.next_gateway === 'string' && names.includes(value.next_gateway))) &&
    (!value.next_gateway || positions.length === 0 || nextPosition === 1) &&
    (value.stale === undefined || typeof value.stale === 'boolean')
})
const stale = computed(() => known.value && (props.snapshot!.stale === true || Date.parse(props.snapshot!.valid_until) <= props.now))
const model = computed(() => known.value ? props.snapshot!.model : '—')
const groups = computed(() => (['quality', 'ordinary'] as const).map(key => ({
  key, value: known.value ? props.snapshot![key] : null
})))
</script>
