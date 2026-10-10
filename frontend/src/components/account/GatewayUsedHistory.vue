<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, useId, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { GATEWAY_REGION_KEYS } from '@/utils/gatewayRegionDisplay'
import Icon from '@/components/icons/Icon.vue'

const props = defineProps<{ items: Array<{
  name: string; region: string; title: string
  duration: string; status: string; tone: 'full' | 'degraded' | 'idle'
}> }>()
const { t } = useI18n()
const PAGE_SIZE = 4
const EDGE_GAP = 8
const HOVER_CLOSE_DELAY_MS = 180
const id = useId()
const trigger = ref<HTMLButtonElement>()
const panel = ref<HTMLElement>()
const open = ref(false)
const region = ref('all')
const page = ref(0)
const position = ref({ top: '0px', left: '0px' })
let pinned = false
let closeTimer: ReturnType<typeof setTimeout> | undefined

const regions = computed(() => GATEWAY_REGION_KEYS.map(key => ({
  key, count: props.items.filter(item => item.region === key).length
})).filter(entry => entry.count))
const filtered = computed(() => props.items.filter(item => region.value === 'all' || item.region === region.value))
const pageCount = computed(() => Math.max(1, Math.ceil(filtered.value.length / PAGE_SIZE)))
const visible = computed(() => filtered.value.slice(page.value * PAGE_SIZE, (page.value + 1) * PAGE_SIZE))
const regionLabel = (key: string) => t(`admin.accounts.openai.gatewayHistory.regions.${key || 'unknown'}`)

function positionPanel() {
  if (!open.value || !panel.value || !trigger.value) return
  const anchor = trigger.value.getBoundingClientRect()
  const box = panel.value.getBoundingClientRect()
  const above = anchor.top - box.height - EDGE_GAP
  position.value = {
    left: `${Math.max(EDGE_GAP, Math.min(anchor.right - box.width, window.innerWidth - box.width - EDGE_GAP))}px`,
    top: `${Math.max(EDGE_GAP, Math.min(above >= EDGE_GAP ? above : anchor.bottom + EDGE_GAP, window.innerHeight - box.height - EDGE_GAP))}px`
  }
}
function cancelClose() {
  clearTimeout(closeTimer)
}
function pinPanel() {
  cancelClose()
  pinned = true
}
function show(pin = false) {
  cancelClose()
  pinned ||= pin
  open.value = true
  nextTick(() => {
    positionPanel()
    if (pin) panel.value?.querySelector('select')?.focus()
  })
}
function close() {
  cancelClose()
  open.value = false
  pinned = false
}
function leave() {
  if (!pinned) closeTimer = setTimeout(close, HOVER_CLOSE_DELAY_MS)
}
function onOutside(event: Event) {
  const target = event.target
  if (target instanceof Node && !trigger.value?.contains(target) && !panel.value?.contains(target)) close()
}
function dismiss() {
  close()
  trigger.value?.focus()
}
function onKey(event: KeyboardEvent) {
  if (open.value && event.key === 'Escape') dismiss()
}
watch(region, () => { page.value = 0 })
watch([() => props.items, region], () => {
  if (region.value !== 'all' && !regions.value.some(entry => entry.key === region.value)) region.value = 'all'
  page.value = Math.min(page.value, pageCount.value - 1)
  nextTick(positionPanel)
})
watch(page, () => nextTick(positionPanel))
onMounted(() => {
  document.addEventListener('pointerdown', onOutside)
  document.addEventListener('keydown', onKey)
  window.addEventListener('scroll', positionPanel, true)
  window.addEventListener('resize', positionPanel)
})
onBeforeUnmount(() => {
  cancelClose()
  document.removeEventListener('pointerdown', onOutside)
  document.removeEventListener('keydown', onKey)
  window.removeEventListener('scroll', positionPanel, true)
  window.removeEventListener('resize', positionPanel)
})
</script>

<template>
  <!-- Same quiet bordered button as clear-cooldown; stays pressed while the panel is open. -->
  <button ref="trigger" type="button"
    class="inline-flex h-5 shrink-0 items-center gap-1 whitespace-nowrap rounded border border-gray-200 px-1.5 text-[10px] text-gray-500 transition-colors hover:border-gray-300 hover:bg-gray-50 hover:text-gray-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gray-400 aria-expanded:border-gray-300 aria-expanded:bg-gray-50 aria-expanded:text-gray-700 dark:border-dark-600 dark:text-gray-400 dark:hover:bg-dark-700 dark:hover:text-gray-200 dark:aria-expanded:bg-dark-700 dark:aria-expanded:text-gray-200"
    :aria-expanded="open" :aria-controls="id" aria-haspopup="dialog" data-testid="gateway-used-history-trigger"
    @mouseenter="show()" @mouseleave="leave" @click="show(true)">
    <Icon name="clock" size="xs" class="h-3 w-3 shrink-0" aria-hidden="true" />
    <span>{{ t('admin.accounts.openai.gatewayUsed.allHistory') }}</span>
  </button>
  <Teleport to="body">
    <section v-if="open" :id="id" ref="panel" role="dialog" :aria-label="t('admin.accounts.openai.gatewayUsed.allHistory')"
      class="fixed z-[99999] w-[300px] max-w-[calc(100vw-16px)] max-h-[calc(100vh-16px)] overflow-auto rounded-lg border border-gray-200 bg-white p-2.5 text-[10px] tabular-nums text-gray-700 shadow-xl dark:border-gray-600 dark:bg-dark-900 dark:text-gray-200"
      :style="position" data-testid="gateway-used-history-panel" @mouseenter="cancelClose" @mouseleave="leave" @pointerdown="pinPanel" @focusin="pinPanel">
      <div class="mb-2 flex items-center justify-between gap-2">
        <span class="text-[11px] font-medium">{{ t('admin.accounts.openai.gatewayUsed.title', { count: items.length }) }}</span>
        <button type="button" class="text-gray-400 hover:text-primary-500" data-testid="gateway-used-history-close" @click="dismiss">{{ t('common.close') }}</button>
      </div>
      <div class="mb-1 flex items-center justify-between gap-2 text-[10px] text-gray-400">
        <span>{{ t('admin.accounts.openai.gatewayUsed.order') }}</span>
        <select v-model="region" :aria-label="t('admin.accounts.openai.gatewayUsed.allRegions')" class="min-w-0 max-w-[130px] rounded border border-gray-200 bg-white px-1 py-0.5 text-[10px] text-gray-600 dark:border-gray-600 dark:bg-dark-900 dark:text-gray-300" data-testid="gateway-used-history-region">
          <option value="all">{{ t('admin.accounts.openai.gatewayUsed.allRegions') }}</option>
          <option v-for="entry in regions" :key="entry.key" :value="entry.key">{{ regionLabel(entry.key) }} · {{ entry.count }}</option>
        </select>
      </div>
      <div v-for="item in visible" :key="item.name" class="grid grid-cols-[minmax(0,1fr)_auto_auto] items-center gap-2 border-t border-gray-100 py-1 dark:border-gray-700"
        :title="item.title" :data-gateway="item.name" data-testid="gateway-used-history-entry">
        <span class="min-w-0 truncate">{{ item.name }} <span class="text-[10px] text-gray-400">{{ regionLabel(item.region) }}</span></span>
        <span>{{ item.duration }}</span>
        <span :class="item.tone === 'full' ? 'text-emerald-600 dark:text-emerald-300' : item.tone === 'degraded' ? 'text-rose-600 dark:text-rose-300' : 'text-gray-400'">{{ item.status }}</span>
      </div>
      <div class="mt-1 flex items-center justify-between gap-2 border-t border-gray-100 pt-2 text-[10px] dark:border-gray-700">
        <button type="button" class="rounded border border-gray-200 px-1.5 py-0.5 disabled:opacity-40 dark:border-gray-600" :disabled="page === 0" data-testid="gateway-used-history-prev" @click="page--">{{ t('admin.accounts.openai.gatewayUsed.pagePrevious') }}</button>
        <span aria-live="polite" data-testid="gateway-used-history-page">{{ page + 1 }}/{{ pageCount }} · {{ filtered.length }}</span>
        <button type="button" class="rounded border border-gray-200 px-1.5 py-0.5 disabled:opacity-40 dark:border-gray-600" :disabled="page >= pageCount - 1" data-testid="gateway-used-history-next" @click="page++">{{ t('admin.accounts.openai.gatewayUsed.pageNext') }}</button>
      </div>
    </section>
  </Teleport>
</template>
