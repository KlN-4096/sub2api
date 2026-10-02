<template>
  <div v-if="isCodexAccount" class="mt-1 space-y-1" data-testid="account-turn-state-cell">
    <!-- 降智恢复探测：连胜进度 / 下次窗口 / 冷却；判定恢复后显示绿色的「已恢复」。 -->
    <p
      v-if="recoveryLine"
      class="text-[10px]"
      :class="
        recovered
          ? 'text-emerald-600 dark:text-emerald-400'
          : recoveryErrored
            ? 'text-amber-600 dark:text-amber-400'
            : 'text-gray-500 dark:text-gray-400'
      "
      :title="recoveryTitle"
      data-testid="account-turn-state-recovery"
    >
      {{ recoveryLine }}
    </p>
  </div>
</template>

<script setup lang="ts">
/**
 * 账号条目下的降智恢复探测：答对进度 / 下次窗口 / 冷却，判定恢复后转绿。
 *
 * 读数在 account.extra.openai_turn_state_recovery_state 里，跟着账号列表一起下发，
 * 不额外调接口。关着（enabled 不为 true）时整行不渲染。
 *
 * 2026-10-02 清掉了这里原先的另外四行——候选池 / 手填 / 形态观测 / 292 猎手。它们都
 * 建立在「注入 292 能换回正常服务」上，而这个前提 2026-09-21 就失效了，从那天起它们
 * 只是一堆没人能据以行动的数字。恢复探测不在此列：它做的是糖果题，判据今天仍然有效。
 * 腾出来的位置给了网关落点（AccountGatewayCell），那才是当前判「这个号还能往哪儿打」
 * 的依据；形态读数本身后端照常采集，用量表每行都写着。
 */
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useNowTicker } from '@/composables/useNowTicker'
import type { Account } from '@/types'
import { targetsCodexUpstream } from '@/utils/turnState'
import { formatDateTime, formatTime } from '@/utils/format'

const props = defineProps<{ account: Account }>()
const { t } = useI18n()

// 与 AccountStatusIndicator 用同一个 ticker：那边原来是裸 new Date()，不会重算，
// 暂停到期后两处会各说各话（见 useNowTicker 的注释）。
const sharedNow = useNowTicker()

const extra = computed(
  () => (props.account.extra as Record<string, unknown> | undefined) ?? {}
)

const isCodexAccount = computed(() => targetsCodexUpstream(props.account))
// cpr 走原样中继，只观测不替换（2026-09-23）：恢复探测对它不适用，
// extra 里残留的旧配置一律不展示。
const replacesTurnState = computed(() => props.account.type !== 'cpr')






interface HuntAttempt {
  at?: string
  model?: string
  proxy?: string
  status?: number
  chars?: number
  healthy?: boolean
  latency_ms?: number
  exit?: string
  error?: string
  /** 恢复探测与 pair 模式的猎手探测都有：糖果题的回答（归一化后）。 */
  answer?: string
  /** pair 模式独有：随票收到的路由 cookie 条数。 */
  cookies?: number
}



const parseTime = (raw: unknown): Date | null => {
  if (typeof raw !== 'string' || !raw) return null
  const d = new Date(raw)
  return Number.isNaN(d.getTime()) ? null : d
}







interface RecoveryState {
  /** 判定窗口，新的在前；true = 答对。 */
  results?: boolean[]
  fail_streak?: number
  next_at?: string
  recovered_at?: string
  cooling_until?: string
  last?: HuntAttempt[]
  last_error?: string
}
const RECOVERY_DEFAULT_WINDOW = 5
const RECOVERY_DEFAULT_SUCCESS = 4

// 与后端 applyDefaults 同一套兜底：窗口默认 5，成功次数默认 4 且不超过窗口。
const recoveryTargets = computed<{ window: number; success: number } | null>(() => {
  const raw = extra.value['openai_turn_state_recovery']
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null
  const cfg = raw as { enabled?: unknown; streak_target?: unknown; success_target?: unknown }
  if (cfg.enabled !== true) return null
  const positive = (v: unknown, fallback: number) => (typeof v === 'number' && v > 0 ? v : fallback)
  const window = positive(cfg.streak_target, RECOVERY_DEFAULT_WINDOW)
  return { window, success: Math.min(positive(cfg.success_target, RECOVERY_DEFAULT_SUCCESS), window) }
})

const recoveryState = computed<RecoveryState>(() => {
  const raw = extra.value['openai_turn_state_recovery_state']
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {}
  return raw as RecoveryState
})

// 零值时间要当成「没有」：后端现在用 omitzero 不再落 "0001-01-01T00:00:00Z"，但老账号行里
// 可能还留着，而 JS 的 Date 认这个字符串——不挡的话那些行会一直写着「已恢复」。
const parsePresentTime = (raw: unknown): Date | null => {
  const d = parseTime(raw)
  return d && d.getTime() > 0 ? d : null
}

const recovered = computed(() => !!parsePresentTime(recoveryState.value.recovered_at))

const recoveryLine = computed(() => {
  const targets = recoveryTargets.value
  if (targets === null || !replacesTurnState.value) return ''
  const st = recoveryState.value
  const recoveredAt = parsePresentTime(st.recovered_at)
  if (recoveredAt) {
    return t('admin.accounts.openai.turnStatePool.recoveryDone', { time: formatTime(recoveredAt) })
  }
  const now = sharedNow.value
  const cooling = parsePresentTime(st.cooling_until)
  const nextAt = parsePresentTime(st.next_at)
  let next: string
  if (cooling && cooling.getTime() > now) {
    next = t('admin.accounts.openai.turnStatePool.recoveryCooling', { time: formatTime(cooling) })
  } else if (nextAt && nextAt.getTime() > now) {
    next = t('admin.accounts.openai.turnStatePool.hunterNext', { time: formatTime(nextAt) })
  } else {
    next = t('admin.accounts.openai.turnStatePool.hunterProbing')
  }
  // 「开着但探不了」（出口不通、上游一直报错）要看得见，否则这行永远是中性的
  // 「恢复探测 0/5 · 下次 12:34」，原因只在 tooltip 里（第一轮评审 S6）。
  if (st.last_error && !st.last?.length) {
    next = t('admin.accounts.openai.turnStatePool.hunterResultError', { status: '-', error: st.last_error })
  }
  return t('admin.accounts.openai.turnStatePool.recoverySummary', {
    successes: (st.results ?? []).filter(Boolean).length,
    success: targets.success,
    window: targets.window,
    next
  })
})

// 与猎手行同一套：探不出来时用告警色，别让一行中性文字长期挂着。
const recoveryErrored = computed(() => {
  const st = recoveryState.value
  return !recovered.value && (!!st.last?.[0]?.error || (!!st.last_error && !st.last?.length))
})

// 恢复探测按回答判，不按票长：tooltip 里写答了什么。
const recoveryAttemptResult = (a: HuntAttempt) => {
  if (a.error) return t('admin.accounts.openai.turnStatePool.hunterResultError', { status: a.status || '-', error: a.error })
  return t(
    a.healthy
      ? 'admin.accounts.openai.turnStatePool.recoveryResultHit'
      : 'admin.accounts.openai.turnStatePool.recoveryResultMiss',
    { answer: a.answer || '-' }
  )
}

const recoveryTitle = computed(() => {
  const attempts = Array.isArray(recoveryState.value.last) ? recoveryState.value.last : []
  if (!attempts.length) return recoveryState.value.last_error ?? ''
  return attempts
    .map((a) =>
      t('admin.accounts.openai.turnStatePool.recoveryDetail', {
        time: formatDateTime(parseTime(a.at) ?? new Date(NaN)),
        model: a.model || '-',
        proxy: a.proxy || '-',
        exit: a.exit ? ` (${a.exit})` : '',
        result: recoveryAttemptResult(a),
        latency: typeof a.latency_ms === 'number' ? `${(a.latency_ms / 1000).toFixed(1)}s` : '-'
      })
    )
    .join('\n')
})
</script>
