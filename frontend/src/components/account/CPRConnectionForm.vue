<template>
  <div class="space-y-3" data-testid="cpr-connection">
    <div>
      <label class="input-label">{{ t('admin.accounts.cpr.baseUrl') }}</label>
      <input :value="baseUrl" type="text" class="input" placeholder="http://127.0.0.1:18081" @input="emit('update:baseUrl', inputValue($event))" />
    </div>
    <div>
      <label class="input-label">{{ t('admin.accounts.cpr.adminApiKey') }}</label>
      <input :value="adminKey" type="password" autocomplete="off" class="input" :placeholder="existingAccountId ? t('admin.accounts.leaveEmptyToKeep') : 'Admin Key'" @input="emit('update:adminKey', inputValue($event))" />
    </div>
    <details>
      <summary class="cursor-pointer text-xs text-gray-500">{{ t('admin.accounts.cpr.adminBaseUrl') }}</summary>
      <input :value="adminBaseUrl" type="text" class="input mt-1" :placeholder="baseUrl" @input="emit('update:adminBaseUrl', inputValue($event))" />
    </details>
    <button type="button" class="btn btn-secondary" :disabled="loading || !baseUrl.trim() || (!adminKey.trim() && !existingAccountId)" @click="connect">
      {{ loading ? t('admin.accounts.cpr.connecting') : t('admin.accounts.cpr.connect') }}
    </button>
    <p v-if="error" role="alert" class="text-xs text-red-500">{{ error }}</p>
    <div>
      <label class="input-label">{{ t('admin.accounts.cpr.selectAccount') }}</label>
      <select :value="accountId" class="input" :disabled="!connected" @change="selectAccount(inputValue($event))">
        <option value="">{{ t('admin.accounts.cpr.selectAccount') }}</option>
        <option v-if="accountId && !accounts.some(a => a.id === accountId)" :value="accountId">{{ accountId }}</option>
        <option v-for="account in accounts" :key="account.id" :value="account.id">
          {{ account.name || account.email || account.id }} · {{ account.plan_type || '—' }} · {{ account.status }}
        </option>
      </select>
    </div>
    <p v-if="selected" class="text-xs text-gray-500">
      {{ selected.email || '—' }} · {{ selected.plan_type || '—' }} · {{ selected.status }} ·
      {{ t('admin.accounts.cpr.concurrency') }}: {{ selected.concurrency_limit ?? '∞' }}
    </p>
    <p class="text-xs text-gray-500">{{ t('admin.accounts.cpr.autoSetupHint') }}</p>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { discoverCPR } from '@/api/admin/accounts'
import type { CPRConnectAccount } from '@/types'

const props = withDefaults(defineProps<{
  baseUrl: string
  adminKey: string
  adminBaseUrl?: string
  accountId: string
  existingAccountId?: number
}>(), { adminBaseUrl: '', existingAccountId: undefined })
const emit = defineEmits<{
  'update:baseUrl': [value: string]
  'update:adminKey': [value: string]
  'update:adminBaseUrl': [value: string]
  'update:accountId': [value: string]
  selected: [account: CPRConnectAccount]
  invalidated: []
}>()
const { t } = useI18n()
const accounts = ref<CPRConnectAccount[]>([])
const connected = ref(false)
const loading = ref(false)
const error = ref('')
let generation = 0
const selected = computed(() => accounts.value.find(a => a.id === props.accountId))
const inputValue = (event: Event) => (event.target as HTMLInputElement).value
watch(() => [props.baseUrl, props.adminKey, props.adminBaseUrl], () => {
  generation++
  accounts.value = []
  connected.value = false
  loading.value = false
  error.value = ''
  emit('update:accountId', '')
  emit('invalidated')
})
onBeforeUnmount(() => { generation++ })
async function connect() {
  const current = ++generation
  loading.value = true
  error.value = ''
  try {
    const result = await discoverCPR({
      base_url: props.baseUrl.trim(),
      admin_base_url: props.adminBaseUrl.trim(),
      admin_api_key: props.adminKey.trim(),
      existing_account_id: props.existingAccountId
    })
    if (current !== generation) return
    accounts.value = result
    connected.value = true
    if (!result.some(a => a.id === props.accountId)) emit('update:accountId', '')
    if (result.length === 0) error.value = t('admin.accounts.cpr.noAccounts')
  } catch {
    if (current === generation) {
      connected.value = false
      error.value = t('admin.accounts.cpr.connectFailed')
      emit('invalidated')
    }
  } finally {
    if (current === generation) loading.value = false
  }
}
function selectAccount(id: string) {
  emit('update:accountId', id)
  const account = accounts.value.find(a => a.id === id)
  if (account) emit('selected', account)
  else emit('invalidated')
}
</script>
