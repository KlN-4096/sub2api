<template>
  <!-- 卡片在设置页的大表单里：输入框回车会提交整页设置，这里挡掉，预设只由本卡片的保存按钮提交 -->
  <div
    v-if="view"
    class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-dark-600"
    data-testid="codex-client-profile-settings"
    @keydown.enter="blockImplicitSubmit"
  >
    <CodexClientProfileFields
      v-model="profile"
      :presets="view.presets"
      :max-version-lag="view.max_version_lag"
      :title="t('admin.codexClientProfile.globalTitle')"
      :description="t('admin.codexClientProfile.globalDescription')"
      :inherit-label="t('admin.codexClientProfile.globalInherit')"
      :inherit-preview="inheritPreview"
    />
    <div class="flex items-center justify-between gap-3">
      <p class="min-w-0 break-all text-xs text-gray-500 dark:text-gray-400">
        {{ t('admin.codexClientProfile.effective', { source: t(`admin.codexClientProfile.source.${view.effective.source || 'builtin'}`) }) }}
        <span class="font-mono">{{ view.effective.user_agent }}</span>
      </p>
      <button type="button" class="btn btn-primary btn-sm flex-shrink-0" :disabled="saving" data-testid="codex-profile-save" @click="save">
        {{ t('admin.codexClientProfile.save') }}
      </button>
    </div>

    <div class="border-t border-gray-200 pt-3 dark:border-dark-600">
      <div class="flex items-center justify-between gap-3">
        <div>
          <p class="text-sm font-medium text-gray-700 dark:text-gray-300">{{ t('admin.codexClientProfile.releases') }}</p>
          <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">{{ t('admin.codexClientProfile.releasesDescription') }}</p>
        </div>
        <button type="button" class="btn btn-secondary btn-sm flex-shrink-0" :disabled="view.refreshing" data-testid="codex-release-refresh" @click="refresh">
          {{ view.refreshing ? t('admin.codexClientProfile.refreshing') : t('admin.codexClientProfile.refresh') }}
        </button>
      </div>
      <ul class="mt-2 space-y-1 text-xs">
        <li v-for="platform in platforms" :key="platform" class="flex flex-wrap gap-x-2" :data-testid="`codex-release-${platform}`">
          <span class="w-16 font-medium text-gray-700 dark:text-gray-300">{{ t(`admin.codexClientProfile.platform.${platform}`) }}</span>
          <span class="font-mono">{{ view.desktop[platform].latest.desktop_version }} · core {{ view.desktop[platform].latest.core_version }}</span>
          <span v-if="!view.desktop[platform].verified" class="text-amber-600 dark:text-amber-400">{{ t('admin.codexClientProfile.seed') }}</span>
          <span v-if="view.desktop[platform].checked_at" class="text-gray-500 dark:text-gray-400">
            {{ t('admin.codexClientProfile.checkedAt', { time: formatTime(view.desktop[platform].checked_at) }) }}
          </span>
          <span v-if="view.desktop[platform].error" class="break-all text-red-600 dark:text-red-400">{{ view.desktop[platform].error }}</span>
        </li>
        <li class="flex flex-wrap gap-x-2">
          <span class="w-16 font-medium text-gray-700 dark:text-gray-300">{{ t('admin.codexClientProfile.cliHistory') }}</span>
          <span class="font-mono">{{ view.cli.join(' · ') }}</span>
        </li>
      </ul>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import CodexClientProfileFields from '@/components/account/CodexClientProfileFields.vue'
import {
  getCodexClientProfile,
  refreshCodexReleases,
  updateCodexClientProfile,
  type CodexClientProfile,
  type CodexClientProfileView,
} from '@/api/admin/codexClientProfile'
import { useAppStore } from '@/stores'
import { extractApiErrorMessage } from '@/utils/apiError'

const { t } = useI18n()
const appStore = useAppStore()
const platforms = ['macos', 'windows', 'linux'] as const
const view = ref<CodexClientProfileView | null>(null)
const profile = ref<CodexClientProfile | null>(null)
const saving = ref(false)
let pollTimer: ReturnType<typeof setTimeout> | undefined
let unmounted = false

// 不设全局预设时生效的是全局自定义 UA 或内置默认。
const inheritPreview = computed(() =>
  view.value
    ? t('admin.codexClientProfile.inheritPreview', {
        userAgent: view.value.fallback.user_agent,
        source: t(`admin.codexClientProfile.source.${view.value.fallback.source || 'builtin'}`),
      })
    : '',
)

function apply(next: CodexClientProfileView) {
  if (unmounted) return
  view.value = next
  clearTimeout(pollTimer)
  if (next.refreshing) pollTimer = setTimeout(load, 5000)
}

function blockImplicitSubmit(event: KeyboardEvent) {
  if ((event.target as HTMLElement | null)?.tagName === 'INPUT') event.preventDefault()
}

async function load() {
  try {
    const next = await getCodexClientProfile()
    if (!view.value) profile.value = next.global
    apply(next)
  } catch (error: unknown) {
    appStore.showError(extractApiErrorMessage(error, t('admin.codexClientProfile.loadFailed')))
  }
}

async function save() {
  saving.value = true
  try {
    const next = await updateCodexClientProfile(profile.value)
    profile.value = next.global
    apply(next)
    appStore.showSuccess(t('admin.codexClientProfile.saved'))
  } catch (error: unknown) {
    appStore.showError(extractApiErrorMessage(error, t('admin.codexClientProfile.saveFailed')))
  } finally {
    saving.value = false
  }
}

async function refresh() {
  try {
    apply(await refreshCodexReleases())
  } catch (error: unknown) {
    appStore.showError(extractApiErrorMessage(error, t('admin.codexClientProfile.refreshFailed')))
  }
}

function formatTime(value: string) {
  return new Date(value).toLocaleString()
}

onMounted(load)
onBeforeUnmount(() => {
  unmounted = true
  clearTimeout(pollTimer)
})
</script>
