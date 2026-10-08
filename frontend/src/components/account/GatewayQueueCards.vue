<template>
  <section class="space-y-2 border-y border-gray-100 py-2 dark:border-gray-700" data-testid="account-gateway-queues">
    <div class="flex items-center justify-between gap-2 text-[11px] text-gray-500 dark:text-gray-400">
      <span :title="t('admin.accounts.openai.gatewayQueues.hint')">{{ t('admin.accounts.openai.gatewayQueues.title') }}</span>
      <span :title="model">{{ model === 'gpt-6-luna' ? 'Luna' : model }}</span>
    </div>
    <div v-for="group in groups" :key="group.key"
      class="grid grid-cols-[76px_minmax(0,1fr)] items-center gap-2 rounded-md bg-gray-100 p-2 text-left text-[11px] dark:bg-gray-800"
      data-testid="gateway-queue-card">
      <div class="flex items-center justify-between gap-1">
        <span class="min-w-0 truncate font-medium text-gray-700 dark:text-gray-300" :title="t(`admin.accounts.openai.gatewayQueues.${group.key}Hint`)">
          {{ t(`admin.accounts.openai.gatewayQueues.${group.key}`) }}
        </span>
        <span class="shrink-0 tabular-nums text-gray-500 dark:text-gray-400" data-testid="gateway-queue-count">{{ group.value?.count ?? '—' }}</span>
      </div>
      <div class="flex min-w-0 flex-wrap items-center gap-1">
        <template v-if="group.value">
          <span v-for="name in group.value.gateways" :key="name"
            class="min-w-[29px] max-w-[48px] truncate rounded bg-white px-1 py-0.5 text-center tabular-nums text-gray-700 dark:bg-dark-900 dark:text-gray-300"
            :title="t('admin.accounts.openai.gatewayQueues.candidateHint', { name })" data-testid="gateway-queue-name">
            {{ name.replace(/^unified-/, '') }}
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

const props = defineProps<{ snapshot?: GatewayPoolQueueView | null; now: number }>()
const { t } = useI18n()
const MAX_PREVIEW_GATEWAYS = 3
function validGroup(group: GatewayPoolQueueGroup | undefined): group is GatewayPoolQueueGroup {
  return !!group && Number.isSafeInteger(group.count) && group.count >= 0 &&
    Array.isArray(group.gateways) && group.gateways.length === Math.min(group.count, MAX_PREVIEW_GATEWAYS) &&
    group.gateways.every(name => typeof name === 'string' && name.trim().length > 0) &&
    new Set(group.gateways).size === group.gateways.length
}
const known = computed(() => {
  const value = props.snapshot
  return !!value && typeof value.model === 'string' && value.model.trim() !== '' &&
    Number.isFinite(Date.parse(value.valid_until)) && Date.parse(value.valid_until) > props.now &&
    validGroup(value.quality) && validGroup(value.ordinary) &&
    !value.quality.gateways.some(name => value.ordinary.gateways.includes(name))
})
const model = computed(() => known.value ? props.snapshot!.model : '—')
const groups = computed(() => (['quality', 'ordinary'] as const).map(key => ({
  key, value: known.value ? props.snapshot![key] : null
})))
</script>
