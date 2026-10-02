<template>
  <div v-if="isCodexAccount" class="mt-1 space-y-1" data-testid="account-turn-state-cell">
    <!-- 猎手状态：本小时用了几次、下次开窗、上次摇到什么。最近 10 次在 tooltip 里。
         只在猎手开着时渲染——没开的账号多一行「猎手 0/30」只是噪音。 -->
    <p
      v-if="hunterLine"
      class="text-[10px]"
      :class="hunterErrored ? 'text-amber-600 dark:text-amber-400' : 'text-gray-500 dark:text-gray-400'"
      :title="hunterTitle"
      data-testid="account-turn-state-hunter"
    >
      {{ hunterLine }}
    </p>
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
 * 账号条目下的 turn-state 运行态：猎手窗口、降智恢复探测。
 *
 * 读数都在 account.extra 里，跟着账号列表一起下发，不额外调接口。
 *
 * 2026-10-02 起这里**不再展示票**（候选池 / 手填 / 形态观测那三行连同裸奔告警一起删了）：
 * 注入 292 换回正常服务这个前提 2026-09-21 就失效了，那几行从那天起只是一堆没人能据以
 * 行动的数字。同一个格子现在给的是网关落点（AccountGatewayCell），那才是当前判「这个号
 * 还能往哪儿打」的依据。形态读数本身仍在后端采集，用量表每行都写着。
 */
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useNowTicker } from '@/composables/useNowTicker'
import type { Account } from '@/types'
import { targetsCodexUpstream, TURN_STATE_HOLD_REASON } from '@/utils/turnState'
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
// cpr 走原样中继，只观测不替换（2026-09-23）：手填 / 候选池 / 裸奔告警 / 猎手 / 恢复探测都不适用，
// extra 里残留的旧配置一律不展示。
const replacesTurnState = computed(() => props.account.type !== 'cpr')


const isManualMode = computed(
  () => isCodexAccount.value && extra.value['openai_turn_state_auto'] !== true
)




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
interface HuntState {
  next_at?: string
  hour_start?: string
  hour_count?: number
  last?: HuntAttempt[]
  last_error?: string
  /** 上次被门槛挡住的原因：idle（无真实流量）/ fresh（票未到期）；正在猎时为空。 */
  gate?: string
}
const TURN_STATE_HUNT_DEFAULT_MAX_PER_HOUR = 30

const hunterMaxPerHour = computed<number | null>(() => {
  const raw = extra.value['openai_turn_state_hunter']
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null
  const cfg = raw as { enabled?: unknown; max_per_hour?: unknown }
  if (cfg.enabled !== true) return null
  return typeof cfg.max_per_hour === 'number' && cfg.max_per_hour > 0
    ? cfg.max_per_hour
    : TURN_STATE_HUNT_DEFAULT_MAX_PER_HOUR
})

const huntState = computed<HuntState>(() => {
  const raw = extra.value['openai_turn_state_hunt']
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {}
  return raw as HuntState
})

const parseTime = (raw: unknown): Date | null => {
  if (typeof raw !== 'string' || !raw) return null
  const d = new Date(raw)
  return Number.isNaN(d.getTime()) ? null : d
}

const huntAttempts = computed<HuntAttempt[]>(() =>
  Array.isArray(huntState.value.last) ? huntState.value.last : []
)

/**
 * 后端 runOnce 要求猎手开关与自动接管**同时**开着才跑（票靠接管注入，只猎不注是白烧
 * 额度）。只看猎手开关的话，接管关着时这行会写着「待命」，而猎手一次都不会运行。
 */
const hunterNeedsAuto = computed(() => hunterMaxPerHour.value !== null && isManualMode.value)

// last_error 也算：「没有可用代理」这类错误不产生探测记录，只写 last_error。
const hunterErrored = computed(
  () => hunterNeedsAuto.value || !!huntAttempts.value[0]?.error || !!huntState.value.last_error
)

const hunterAttemptResult = (a: HuntAttempt) => {
  if (a.error) return t('admin.accounts.openai.turnStatePool.hunterResultError', { status: a.status || '-', error: a.error })
  // pair 模式按做题判，票长只是读数：不写出答案的话，「答对入池」和「答错丢掉」在页面上
  // 长得一模一样（两次的票都是 780 字符）。
  if (a.answer) {
    return t(
      a.healthy
        ? 'admin.accounts.openai.turnStatePool.hunterResultAnswerHit'
        : 'admin.accounts.openai.turnStatePool.hunterResultAnswerMiss',
      { chars: a.chars ?? 0, answer: a.answer }
    )
  }
  return t(
    a.healthy
      ? 'admin.accounts.openai.turnStatePool.hunterResultHit'
      : 'admin.accounts.openai.turnStatePool.hunterResultMiss',
    { chars: a.chars ?? '-' }
  )
}

/**
 * 有模型正因降智被停着。后端给被停的模型开了空闲门槛的后门（停着就说明刚有人请求过），
 * 而 gate 是上一个 tick 的快照，于是「刚发完请求被停」那几十秒里这行会写着「无流量 · 暂停」，
 * 和状态列的「降智暂停」直接打架（2026-09-19 用户反馈）。读同一份 model_rate_limits 覆盖它。
 */
const heldByTurnState = computed(() => {
  const limits = extra.value['model_rate_limits']
  if (!limits || typeof limits !== 'object') return false
  return Object.values(limits as Record<string, unknown>).some((raw) => {
    const entry = raw as { reason?: unknown; rate_limit_reset_at?: unknown } | null
    if (!entry || typeof entry !== 'object' || entry.reason !== TURN_STATE_HOLD_REASON) return false
    const resetAt = parseTime(entry.rate_limit_reset_at)
    return !!resetAt && resetAt.getTime() > sharedNow.value
  })
})

const hunterLine = computed(() => {
  const max = hunterMaxPerHour.value
  if (max === null || !replacesTurnState.value) return ''
  if (hunterNeedsAuto.value) return t('admin.accounts.openai.turnStatePool.hunterNeedsAuto')
  const now = sharedNow.value
  const st = huntState.value
  // 小时窗过了就是 0：后端只在下一次探测时才把计数归零，页面不能拿旧计数吓人。
  const hourStart = parseTime(st.hour_start)
  const count = hourStart && hourStart.getTime() + 3_600_000 > now ? st.hour_count ?? 0 : 0
  const nextAt = parseTime(st.next_at)
  const latest = huntAttempts.value[0]
  // 没在等窗时说清楚为什么没在猎：「待命」盖不住「票还新鲜」和「无流量暂停」的区别。
  const gateKey =
    st.gate === 'idle'
      ? heldByTurnState.value
        ? 'hunterGateHeld'
        : 'hunterGateIdle'
      : st.gate === 'fresh'
        ? 'hunterGateFresh'
        : 'hunterReady'
  // 没在等窗、没被门槛挡、最近一次又没命中：这轮还在猎（或下个 tick 接着猎）。多账号交错后
  // 排队最多一个 gap，「排队中」和「探测中」不再区分（2026-09-19 反馈：排队时页面写着待命）。
  const probing = !st.gate && !!latest && !latest.healthy
  const next =
    nextAt && nextAt.getTime() > now
      ? t('admin.accounts.openai.turnStatePool.hunterNext', { time: formatTime(nextAt) })
      : probing
        ? t('admin.accounts.openai.turnStatePool.hunterProbing')
        : t(`admin.accounts.openai.turnStatePool.${gateKey}`)
  const last = latest
    ? t('admin.accounts.openai.turnStatePool.hunterLast', {
        result: hunterAttemptResult(latest),
        proxy: latest.proxy || '-',
        time: formatTime(parseTime(latest.at) ?? new Date(NaN))
      })
    : st.last_error
      ? t('admin.accounts.openai.turnStatePool.hunterResultError', { status: '-', error: st.last_error })
      : t('admin.accounts.openai.turnStatePool.hunterLastNone')
  return t('admin.accounts.openai.turnStatePool.hunterSummary', { count, max, next, last })
})

const hunterTitle = computed(() =>
  huntAttempts.value
    .map((a) =>
      t('admin.accounts.openai.turnStatePool.hunterDetail', {
        time: formatDateTime(parseTime(a.at) ?? new Date(NaN)),
        model: a.model || '-',
        proxy: a.proxy || '-',
        // 固定出口探测前解析过出口 IP 才有；轮换端点由供应商选出口，这里为空。
        exit: a.exit ? ` (${a.exit})` : '',
        result: hunterAttemptResult(a),
        latency: typeof a.latency_ms === 'number' ? `${(a.latency_ms / 1000).toFixed(1)}s` : '-'
      }) +
      // pair 模式才有：随票收到几个路由 cookie。0 个说明那张票只能裸回放，是用户要看的读数之一。
      (a.cookies ? t('admin.accounts.openai.turnStatePool.detailPair', { n: a.cookies }) : '')
    )
    .join('\n')
)

/**
 * 降智恢复探测（extra.openai_turn_state_recovery / _state）：走账号自己的出口、间隔随机，
 * 每次出一道糖果题，最近 window 次里答对 success 次判定恢复。判定后后端停止探测，
 * 所以这行改说「已恢复」而不是下次窗口。
 */
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
