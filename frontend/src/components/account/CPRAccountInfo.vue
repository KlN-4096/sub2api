<template>
  <div
    v-if="snapshot?.query_error"
    class="text-[10px] text-amber-600 dark:text-amber-400"
    :title="t(`admin.accounts.cpr.errors.${snapshot.query_error}`)"
    role="status"
  >
    {{ t('admin.accounts.cpr.queryFailed') }}
  </div>
</template>

<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import type { CPRAccountSnapshot } from '@/types'
defineProps<{ snapshot?: CPRAccountSnapshot }>()
const { t } = useI18n()
</script>
