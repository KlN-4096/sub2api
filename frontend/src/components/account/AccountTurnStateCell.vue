<template>
  <div v-if="isCodexAccount" class="mt-1 space-y-1" data-testid="account-turn-state-cell">
    <!-- 没有生效的票也要占位：整块消失时，「没开接管」「开了但池空」「票全过期了」
         在页面上长得一模一样，运维只能靠猜。非 Codex 上游的账号根本没有这个头，
         那才是真该整块消失的情况。
         占位必须带上「Turn-State」这几个字：它挤在额度列下方，一个裸的 - 谁也认不出
         是什么，等于没显示。 -->
    <p v-if="!entries.length" class="text-[10px] text-gray-400" data-testid="account-turn-state-empty">
      {{ t('admin.accounts.openai.turnStatePool.empty') }}
    </p>
    <UsageProgressBar
      v-for="entry in entries"
      :key="entry.model"
      :label="entry.label"
      label-width="auto"
      :utilization="entry.remainingPercent"
      :resets-at="entry.expiresAt"
      remaining-capacity
      :color="entry.healthy ? 'purple' : 'amber'"
      :data-testid="`account-turn-state-${entry.model}`"
    />
    <p v-if="entries.length" class="text-[10px] text-gray-400" :title="detailTitle">
      {{ t('admin.accounts.openai.turnStatePool.summary', { n: entries.length }) }}
    </p>
  </div>
</template>

<script setup lang="ts">
/**
 * 账号页的 turn-state 实时池：每个模型一行，用与用量窗口同一个 UsageProgressBar
 * 展示「这张票还剩多久到期」。
 *
 * 数据直接来自 account.extra.openai_turn_state_pool（网关维护的运行态键），
 * 不额外调接口——池子本来就跟着账号列表一起下发了。
 */
import { computed, onUnmounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import UsageProgressBar from './UsageProgressBar.vue'
import type { Account } from '@/types'
import {
  decodeTurnState,
  isTurnStateHealthy,
  targetsCodexUpstream,
  TURN_STATE_DEFAULT_TTL_MINUTES
} from '@/utils/turnState'
import { formatDateTime } from '@/utils/format'

/**
 * 模块级共享时钟。倒计时必须真的走，否则页面一挂就是一张冻结的快照：过期的票不消失、
 * 进度条永远停在打开时的比例（账号页的自动刷新默认是关的，props 不会自己变）。
 * 共享而不是每个实例一只表——账号列表一行一个实例，几十只定时器没必要。
 * 30s 粒度：UsageProgressBar 自己那只表是 60s，比它快一档就不会出现「条子还是绿的、
 * 右边已经写着待刷新」这种自相矛盾。
 */
const sharedNow = ref(Date.now())
let clockUsers = 0
let clockTimer: ReturnType<typeof setInterval> | null = null

const acquireClock = () => {
  if (++clockUsers === 1) {
    clockTimer = setInterval(() => (sharedNow.value = Date.now()), 30_000)
  }
}
const releaseClock = () => {
  if (--clockUsers === 0 && clockTimer) {
    clearInterval(clockTimer)
    clockTimer = null
  }
}

const props = defineProps<{ account: Account }>()
const { t } = useI18n()

acquireClock()
onUnmounted(releaseClock)

interface PoolCandidate {
  blob?: string
  model?: string
  minted_at?: string
  failed?: boolean
  fail_streak?: number
}

const extra = computed(
  () => (props.account.extra as Record<string, unknown> | undefined) ?? {}
)

const ttlMs = computed(() => {
  const raw = extra.value['openai_turn_state_stale_after_minutes']
  const minutes = typeof raw === 'number' && raw > 0 ? raw : TURN_STATE_DEFAULT_TTL_MINUTES
  return minutes * 60_000
})

/**
 * 展示的是「当前真的会被注入的票」，不是「池子里还躺着什么」。所以除了 Codex 上游、
 * 未失效、未过期，还要满足：自动接管开着（关了之后池子还会留最多 1 小时，那段时间
 * 里一条都不会被注入）。
 */
// 只有最终落到 ChatGPT Codex 后端的账号才有这个头（oauth / setup-token / cpr）。
const isCodexAccount = computed(() => targetsCodexUpstream(props.account))

/**
 * 自动接管关着时生效的是手填覆写表——后端的分支正好相反（开了自动就完全忽略手填）。
 * 不展示它的话，「票正在注入」的账号格子上会写着「没有生效的票」，那比整块不渲染更糟：
 * 歧义换成了错误断言。
 *
 * 铸造时刻只能从信封自己解：手填票没有后端写的 minted_at。解不出就不展示这一条，
 * 与候选池「没有 minted_at 就跳过」同一套降级——算不出到期时间的进度条是假的。
 */
const manualOverrides = computed<PoolCandidate[]>(() => {
  const raw = extra.value['openai_turn_state_override']
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return []
  const out: PoolCandidate[] = []
  for (const [model, blob] of Object.entries(raw as Record<string, unknown>)) {
    if (typeof blob !== 'string' || !blob.trim() || !model.trim()) continue
    const env = decodeTurnState(blob.trim())
    if (!env) continue
    out.push({ model: model.trim(), blob: blob.trim(), minted_at: env.mintedAt.toISOString() })
  }
  return out
})

const isManualMode = computed(
  () => isCodexAccount.value && extra.value['openai_turn_state_auto'] !== true
)

const pool = computed<PoolCandidate[]>(() => {
  if (!isCodexAccount.value) return []
  if (isManualMode.value) return manualOverrides.value
  const raw = extra.value['openai_turn_state_pool']
  return Array.isArray(raw) ? (raw as PoolCandidate[]) : []
})

interface PoolEntry {
  model: string
  label: string
  blob: string
  healthy: boolean
  mintedAt: Date
  expiresAt: string
  remainingPercent: number
}

/**
 * 每个模型只展示当前生效的那一条：池子是新在前，取第一条未失效、未过期的。
 * 取不到就整个模型不展示——「没有可用票」和「有一张过期票」对运维是同一件事。
 *
 * 铸造时刻以后端写的 minted_at 为准：那是过期判定真正用的值，且后端对信封里的
 * 时间戳做了合理性校验（解不出或明显离谱时会退回观测时刻）。前端自己解出来的
 * 时间戳没有那道闸，拿它当权威会出现「页面显示还剩 365 天、后端 1 小时后就不注入了」。
 */
const entries = computed<PoolEntry[]>(() => {
  const now = sharedNow.value
  const seen = new Set<string>()
  const out: PoolEntry[] = []
  for (const c of pool.value) {
    const model = String(c?.model ?? '').trim()
    const blob = String(c?.blob ?? '').trim()
    if (!model || !blob || c?.failed || seen.has(model)) continue
    const minted = c?.minted_at ? new Date(c.minted_at) : null
    if (!minted || Number.isNaN(minted.getTime())) continue
    const expires = minted.getTime() + ttlMs.value
    if (expires <= now) continue
    seen.add(model)
    out.push({
      model,
      label: isManualMode.value
        ? t('admin.accounts.openai.turnStatePool.manualLabel', { model })
        : model,
      blob,
      healthy: isTurnStateHealthy(blob),
      mintedAt: minted,
      expiresAt: new Date(expires).toISOString(),
      remainingPercent: Math.round(((expires - now) / ttlMs.value) * 100)
    })
  }
  return out
})

const detailTitle = computed(() =>
  entries.value
    .map((e) =>
      t('admin.accounts.openai.turnStatePool.detail', {
        model: e.label,
        shape: `${e.blob.length}c`,
        health: t(
          e.healthy
            ? 'admin.accounts.openai.turnStatePool.healthy'
            : 'admin.accounts.openai.turnStatePool.suspect'
        ),
        minted: formatDateTime(e.mintedAt),
        expires: formatDateTime(new Date(e.expiresAt))
      })
    )
    .join('\n')
)
</script>
