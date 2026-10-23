<template>
  <div class="space-y-3">
    <div class="flex items-center justify-between gap-4">
      <div>
        <label class="input-label">{{ title }}</label>
        <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">{{ description }}</p>
      </div>
      <div class="w-56 flex-shrink-0">
        <Select v-model="presetKey" data-testid="codex-profile-preset" :options="presetOptions" />
      </div>
    </div>

    <template v-if="form.client">
      <div class="grid grid-cols-2 gap-3">
        <div v-if="form.client === 'cli'">
          <label class="input-label">{{ t('admin.codexClientProfile.cliEntry') }}</label>
          <Select v-model="form.cliEntry" data-testid="codex-profile-cli-entry" :options="cliEntryOptions" />
        </div>
        <div>
          <label class="input-label">{{ t('admin.codexClientProfile.versionMode') }}</label>
          <Select v-model="form.versionMode" data-testid="codex-profile-version-mode" :options="versionModeOptions" />
        </div>
        <div v-if="form.versionMode === 'latest'">
          <label class="input-label">{{ t('admin.codexClientProfile.versionLag', { max: maxVersionLag }) }}</label>
          <input v-model.number="form.versionLag" type="number" min="0" :max="maxVersionLag" class="input" data-testid="codex-profile-version-lag" />
        </div>
        <template v-else>
          <div>
            <label class="input-label">{{ t('admin.codexClientProfile.codexVersion') }}</label>
            <input v-model.trim="form.codexVersion" type="text" class="input" placeholder="0.163.0-alpha.11.2" data-testid="codex-profile-codex-version" />
          </div>
          <div v-if="form.client === 'desktop'">
            <label class="input-label">{{ t('admin.codexClientProfile.desktopVersion') }}</label>
            <input v-model.trim="form.desktopVersion" type="text" class="input" placeholder="26.1016.62342" data-testid="codex-profile-desktop-version" />
          </div>
        </template>
      </div>
      <details class="text-sm">
        <summary class="cursor-pointer text-xs text-gray-500 dark:text-gray-400">{{ t('admin.codexClientProfile.overrides') }}</summary>
        <div class="mt-2 grid grid-cols-2 gap-3">
          <div v-for="field in overrideFields" :key="field.key">
            <label class="input-label">{{ field.label }}</label>
            <input v-model.trim="form[field.key]" type="text" class="input" :placeholder="field.placeholder" :data-testid="`codex-profile-${field.key}`" />
          </div>
        </div>
      </details>
    </template>

    <p class="break-all font-mono text-xs" :class="previewError ? 'text-red-600 dark:text-red-400' : 'text-gray-600 dark:text-gray-300'" data-testid="codex-profile-preview">
      {{ previewError || previewText }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import Select from '@/components/common/Select.vue'
import {
  previewCodexClientProfile,
  type CodexClientProfile,
  type CodexClientProfilePreset,
} from '@/api/admin/codexClientProfile'

const props = defineProps<{
  modelValue: CodexClientProfile | null
  presets: CodexClientProfilePreset[]
  maxVersionLag: number
  title: string
  description: string
  /** 未选预设时生效的身份（账号：全局身份；全局：自定义 UA 或内置默认）。 */
  inheritLabel: string
  inheritPreview: string
  /** 传了才在下拉末尾加「自定义 UA」项；选中时 modelValue 为 null、custom 为 true，输入框由调用方渲染。 */
  customLabel?: string
  custom?: boolean
}>()
const emit = defineEmits<{
  (e: 'update:modelValue', value: CodexClientProfile | null): void
  (e: 'update:custom', value: boolean): void
}>()
const { t } = useI18n()

type OverrideKey = 'osType' | 'osVersion' | 'arch' | 'terminal'
interface Form {
  client: '' | CodexClientProfile['client']
  platform: '' | CodexClientProfile['platform']
  cliEntry: '' | 'exec'
  versionMode: CodexClientProfile['versionMode']
  versionLag: number | ''
  codexVersion: string
  desktopVersion: string
  osType: string
  osVersion: string
  arch: string
  terminal: string
}

const emptyForm = (): Form => ({
  client: '', platform: '', cliEntry: '', versionMode: 'latest', versionLag: 0,
  codexVersion: '', desktopVersion: '', osType: '', osVersion: '', arch: '', terminal: '',
})
const form = reactive<Form>(emptyForm())

function toProfile(): CodexClientProfile | null {
  if (!form.client || !form.platform) return null
  const profile: CodexClientProfile = { client: form.client, platform: form.platform, versionMode: form.versionMode }
  if (form.client === 'cli' && form.cliEntry) profile.cliEntry = form.cliEntry
  if (form.versionMode === 'latest' && typeof form.versionLag === 'number' && form.versionLag > 0) profile.versionLag = form.versionLag
  if (form.versionMode === 'fixed') {
    if (form.codexVersion) profile.codexVersion = form.codexVersion
    if (form.client === 'desktop' && form.desktopVersion) profile.desktopVersion = form.desktopVersion
  }
  for (const key of ['osType', 'osVersion', 'arch', 'terminal'] as OverrideKey[]) {
    if (form[key]) profile[key] = form[key]
  }
  return profile
}

watch(
  () => props.modelValue,
  (value) => {
    if (JSON.stringify(value ?? null) === JSON.stringify(toProfile())) return
    Object.assign(form, emptyForm(), value ? { ...value, cliEntry: value.cliEntry ?? '', versionLag: value.versionLag ?? 0 } : {})
  },
  { immediate: true },
)
watch(form, () => emit('update:modelValue', toProfile()), { deep: true })

const CUSTOM_KEY = 'custom'
const presetKey = computed<string>({
  get: () => (form.client && form.platform ? `${form.client}:${form.platform}` : props.custom ? CUSTOM_KEY : ''),
  set: (key) => {
    const custom = key === CUSTOM_KEY
    if (custom !== Boolean(props.custom)) emit('update:custom', custom)
    const [client, platform] = (custom ? '' : key || '').split(':') as [Form['client'], Form['platform']]
    if (client === form.client && platform === form.platform) return
    Object.assign(form, emptyForm(), { client: client || '', platform: platform || '' })
  },
})

const presetOptions = computed(() => [
  { value: '', label: props.inheritLabel },
  ...props.presets.map(({ profile }) => ({
    value: `${profile.client}:${profile.platform}`,
    label: `${profile.client === 'desktop' ? 'Desktop' : 'CLI'} · ${t(`admin.codexClientProfile.platform.${profile.platform}`)}`,
  })),
  ...(props.customLabel ? [{ value: CUSTOM_KEY, label: props.customLabel }] : []),
])
const cliEntryOptions = computed(() => [
  { value: '', label: 'codex_cli_rs' },
  { value: 'exec', label: 'codex_exec (exec)' },
])
const versionModeOptions = computed(() => [
  { value: 'latest', label: t('admin.codexClientProfile.versionLatest') },
  { value: 'fixed', label: t('admin.codexClientProfile.versionFixed') },
])

const activePreset = computed(() =>
  props.presets.find(({ profile }) => profile.client === form.client && profile.platform === form.platform),
)
const overrideFields = computed(() => {
  const preset = activePreset.value
  return [
    { key: 'osType' as OverrideKey, label: t('admin.codexClientProfile.osType'), placeholder: preset?.os_type ?? '' },
    { key: 'osVersion' as OverrideKey, label: t('admin.codexClientProfile.osVersion'), placeholder: preset?.os_version ?? '' },
    { key: 'arch' as OverrideKey, label: t('admin.codexClientProfile.arch'), placeholder: preset?.arch ?? '' },
    { key: 'terminal' as OverrideKey, label: t('admin.codexClientProfile.terminal'), placeholder: preset?.terminal ?? '' },
  ]
})

const previewText = ref('')
const previewError = ref('')
let previewTimer: ReturnType<typeof setTimeout> | undefined
let previewSeq = 0
watch(
  [() => JSON.stringify(toProfile()), () => props.inheritPreview],
  () => {
    clearTimeout(previewTimer)
    const profile = toProfile()
    previewError.value = ''
    // 先作废在途预览：切回继承后迟到的旧结果不能盖掉继承文案。
    const seq = ++previewSeq
    if (!profile) {
      previewText.value = props.inheritPreview
      return
    }
    previewTimer = setTimeout(async () => {
      try {
        const preview = await previewCodexClientProfile(profile)
        if (seq === previewSeq) previewText.value = preview.user_agent
      } catch (error: unknown) {
        if (seq !== previewSeq) return
        const message = (error as { message?: string })?.message
        previewError.value = message || t('admin.codexClientProfile.previewFailed')
      }
    }, 300)
  },
  { immediate: true },
)
onBeforeUnmount(() => {
  clearTimeout(previewTimer)
  previewSeq++
})
</script>
