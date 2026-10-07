<template>
  <section class="space-y-3 border-t border-gray-200 pt-4 dark:border-dark-600" data-testid="bulk-gwpool">
    <h3 class="font-medium">{{ t('admin.accounts.openai.gwpool') }}</h3>
    <p class="input-hint">{{ t('admin.accounts.openai.gwpoolBulkHint') }}</p>
    <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
    <div v-for="field in fields" :key="field.key" class="grid grid-cols-[auto_1fr] items-start gap-3" :class="{ 'sm:col-span-2': field.wide }">
      <input
        type="checkbox"
        :data-testid="`bulk-gwpool-apply-${field.key}`"
        :aria-label="t('admin.accounts.openai.gwpoolBulkApply', { field: t(`admin.accounts.openai.${field.label}`) })"
        :checked="selected(field.key)"
        class="mt-2 rounded border-gray-300 text-primary-600"
        @change="toggle(field, ($event.target as HTMLInputElement).checked)"
      />
      <label class="space-y-1 text-xs">
        <span>{{ t(`admin.accounts.openai.${field.label}`) }}</span>
        <input
          v-if="field.type === 'boolean'"
          type="checkbox"
          :data-testid="`bulk-gwpool-value-${field.key}`"
          :disabled="!selected(field.key)"
          :checked="modelValue[field.key] === true"
          class="ml-3 rounded border-gray-300 text-primary-600"
          @change="set(field.key, ($event.target as HTMLInputElement).checked)"
        />
        <input
          v-else
          class="input text-xs"
          :data-testid="`bulk-gwpool-value-${field.key}`"
          :type="field.type"
          :disabled="!selected(field.key)"
          :value="modelValue[field.key] ?? ''"
          :min="field.type === 'number' ? (field.min ?? 1) : undefined"
          :max="field.max"
          :step="field.type === 'number' ? 1 : undefined"
          :placeholder="String(field.default)"
          autocomplete="off"
          @input="input(field, ($event.target as HTMLInputElement).value)"
        />
        <p v-if="field.hint" class="input-hint">{{ t(`admin.accounts.openai.${field.hint}`) }}</p>
      </label>
    </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { useI18n } from 'vue-i18n'

type Value = string | number | boolean | null
type Field = { key: string; label: string; type: string; default: Value; min?: number; max?: number; wide?: boolean; hint?: string }
const props = defineProps<{ modelValue: Record<string, Value> }>()
const emit = defineEmits<{ 'update:modelValue': [Record<string, Value>] }>()
const { t } = useI18n()
const cooldownResetMaxHours = 365 * 24
const fields: Field[] = [
  { key: 'openai_gwpool', label: 'gwpool', type: 'boolean', default: true, wide: true },
  { key: 'openai_gwpool_base_url', label: 'gwpoolBaseUrl', type: 'url', default: '', wide: true },
  { key: 'openai_gwpool_consumer_key', label: 'gwpoolConsumerKey', type: 'password', default: '', wide: true },
  { key: 'openai_gwpool_max_wait_s', label: 'gwpoolMaxWait', hint: 'gwpoolMaxWaitDesc', type: 'number', default: 120, max: 3600 },
  { key: 'openai_gwpool_probe_timeout_s', label: 'gwpoolProbeTimeout', hint: 'gwpoolProbeTimeoutDesc', type: 'number', default: 35, max: 120 },
  { key: 'openai_gwpool_prepare_retries', label: 'gwpoolPrepareRetries', hint: 'gwpoolPrepareRetriesDesc', type: 'number', default: 0, min: 0, max: 10 },
  { key: 'openai_gwpool_rotation_min_gateways', label: 'gwpoolRotationMinGateways', hint: 'gwpoolRotationMinGatewaysDesc', type: 'number', default: 1, max: 512 },
  { key: 'openai_gwpool_resume_gateways', label: 'gwpoolResumeGateways', hint: 'gwpoolResumeGatewaysDesc', type: 'number', default: 50, max: 512 },
  { key: 'openai_gwpool_gateway_window_s', label: 'gwpoolGatewayWindow', type: 'number', default: 3600, max: 86400 },
  { key: 'openai_gwpool_cooldown_reset_hours', label: 'gwpoolCooldownResetHours', hint: 'gwpoolCooldownResetDesc', type: 'number', default: 24, min: 0, max: cooldownResetMaxHours },
  { key: 'openai_gwpool_member_isolation', label: 'gwpoolMemberIsolation', hint: 'gwpoolMemberIsolationDesc', type: 'boolean', default: false, wide: true },
  { key: 'openai_gwpool_early_probe_enabled', label: 'gwpoolEarlyProbe', hint: 'gwpoolEarlyProbeDesc', type: 'boolean', default: false, wide: true },
  { key: 'openai_gwpool_use_recommended_cooldown', label: 'gwpoolUseRecommendation', hint: 'gwpoolUseRecommendationDesc', type: 'boolean', default: false, wide: true },
  { key: 'openai_gwpool_fetch_timeout_s', label: 'gwpoolFetchTimeout', type: 'number', default: 25, max: 86400 },
  { key: 'openai_gwpool_list_timeout_s', label: 'gwpoolListTimeout', type: 'number', default: 2, max: 86400 }
]
const selected = (key: string) => Object.prototype.hasOwnProperty.call(props.modelValue, key)
function set(key: string, value: Value) {
  emit('update:modelValue', { ...props.modelValue, [key]: value })
}
function toggle(field: Field, checked: boolean) {
  const next = { ...props.modelValue }
  if (checked) next[field.key] = field.default
  else delete next[field.key]
  emit('update:modelValue', next)
}
function input(field: Field, value: string) {
  set(field.key, field.type === 'number' ? (value === '' ? null : Number(value)) : value)
}
</script>
