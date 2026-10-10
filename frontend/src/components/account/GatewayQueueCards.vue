<template>
  <!-- One light card: global forecast positions | short gateway names; the next attempt gets the only accent border. -->
  <section class="grid grid-cols-[auto_minmax(0,1fr)] items-center gap-x-2 gap-y-1 rounded-md bg-gray-50 px-2 py-1.5 text-[11px] dark:bg-dark-800/70"
    data-testid="account-gateway-queues">
    <div class="col-span-2 flex min-w-0 items-center gap-1 text-[10px] text-gray-400 dark:text-gray-500">
      <span :title="t('admin.accounts.openai.gatewayQueues.hint')">{{ t('admin.accounts.openai.gatewayQueues.title') }}</span>
      <span v-if="known && snapshot?.observed_at" :title="`${model} · ${snapshot.observed_at}`" data-testid="gateway-queue-age">· {{ formatRelativeTime(snapshot.observed_at, now) }}</span>
      <span v-if="stale" class="ml-auto rounded bg-amber-50 px-1 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300"
        data-testid="gateway-queue-stale" :title="t('admin.accounts.openai.gatewayQueues.staleHint')">{{ t('admin.accounts.openai.gatewayQueues.stale') }}</span>
      <span v-else-if="!known" class="ml-auto">{{ t('admin.accounts.openai.gatewayQueues.unknown') }}</span>
    </div>
    <template v-for="group in groups" :key="group.key">
      <span class="text-[10px] text-gray-400 dark:text-gray-500" :title="t(`admin.accounts.openai.gatewayQueues.${group.key}Hint`)">{{ t(`admin.accounts.openai.gatewayQueues.${group.key}`) }}</span>
      <div class="flex min-w-0 items-center gap-1" data-testid="gateway-queue-card">
        <span class="w-6 shrink-0 font-medium text-gray-700 dark:text-gray-200" data-testid="gateway-queue-count">{{ group.value?.count ?? '—' }}</span>
        <template v-if="group.value">
          <span v-for="(name, index) in group.value.gateways" :key="name"
            class="inline-flex h-4 min-w-0 items-stretch overflow-hidden rounded border bg-white text-[10px] leading-[14px] dark:bg-dark-900"
            :class="name === snapshot?.next_gateway ? 'border-primary-400 dark:border-primary-500' : 'border-gray-200 dark:border-dark-600'"
            :data-position="group.value.positions?.[index]" :data-next="name === snapshot?.next_gateway || undefined"
            data-testid="gateway-queue-entry" :title="entryTitle(name)">
            <span v-if="group.value.positions" class="border-r bg-gray-100 px-[3px] text-[9px] dark:bg-dark-700"
              :class="name === snapshot?.next_gateway ? 'border-primary-400 text-primary-600 dark:border-primary-500 dark:text-primary-300' : 'border-gray-200 text-gray-400 dark:border-dark-600 dark:text-gray-500'">{{ group.value.positions[index] }}</span>
            <span class="truncate px-1 text-gray-700 dark:text-gray-200" data-testid="gateway-queue-name">{{ name.replace(/^unified-/, '') }}</span>
          </span>
          <span v-if="group.value.count > group.value.gateways.length" class="text-[10px] text-gray-400" data-testid="gateway-queue-more"
            :title="t('admin.accounts.openai.gatewayQueues.more', { count: group.value.count - group.value.gateways.length })">
            +{{ group.value.count - group.value.gateways.length }}
          </span>
          <span v-if="group.value.count === 0" class="text-[10px] text-gray-400">{{ t('admin.accounts.openai.gatewayQueues.empty') }}</span>
        </template>
      </div>
    </template>
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
function entryTitle(name: string): string {
  const candidate = t('admin.accounts.openai.gatewayQueues.candidateHint', { name })
  return name === props.snapshot?.next_gateway
    ? `${t('admin.accounts.openai.gatewayQueues.next', { name })} · ${candidate}`
    : candidate
}
</script>
