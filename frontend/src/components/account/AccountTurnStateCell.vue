<template>
  <div v-if="isCodexAccount" class="mt-1 space-y-1" data-testid="account-turn-state-cell">
    <!-- 没有票也要占位：整块消失时，「没开接管」「开了但池空」「票全过期了」在页面上
         长得一模一样，运维只能靠猜。非 Codex 上游的账号根本没有这个头，那才是真该
         整块消失的情况。
         占位必须带上「Turn-State」这几个字：它挤在额度列下方，一个裸的 - 谁也认不出
         是什么，等于没显示。
         接管开着却拿不出票是另一回事（starved），要用告警色单独说——那是正在裸奔，
         而且此时观测行多半还在（后端对所有 Codex 账号采集形态），所以它不能只在
         「一行都没有」时才出现，否则最该报警的场景恰好被观测行挡住。 -->
    <p
      v-if="starved || !entries.length"
      class="text-[10px]"
      :class="starved ? 'text-amber-600 dark:text-amber-400' : 'text-gray-400'"
      :data-starved="starved ? 'true' : 'false'"
      :role="starved ? 'status' : undefined"
      data-testid="account-turn-state-empty"
    >
      {{ emptyLabel }}
    </p>
    <!-- key 必须带上 active：同一个模型现在可能同时有「手填」和「最近铸出」两行，
         只用 model 会撞 key，Vue 会告警并错误复用节点。 -->
    <div
      v-for="entry in visibleEntries"
      :key="`${entry.model}-${entry.active}`"
      class="flex items-center gap-1"
      data-testid="account-turn-state-row"
    >
      <!-- 「手填」「最近铸出」这两个标记必须待在进度条外面。UsageProgressBar 的 label
           徽章是 max-w-[72px] truncate + 10px 字号（约 12 个字符），而模型名本身就有
           12 个字符（gpt-5.6-luna），写成 `{model}(最近铸出)` 的话后缀会整个落进省略号
           里——而那是唯一说明「这一行不会被注入」的文字，剩下的区分就只有 opacity-60，
           形态相同时（手填 292 + 观测 292）两行同色同文，等于分不出来。 -->
      <span
        v-if="entry.tag"
        class="shrink-0 rounded bg-gray-100 px-1 text-[9px] leading-4 text-gray-500 dark:bg-gray-800 dark:text-gray-400"
        data-testid="account-turn-state-tag"
      >
        {{ entry.tag }}
      </span>
      <!-- 形态数字。整行最要紧的一个事实就是它（292 还是 312），而 UsageProgressBar 里
           进度条和百分比的颜色来自**剩余时间**（还剩 44 分钟 → 74% → 绿），与健康度
           无关；`color` 属性只染那个会被截断的模型名药丸。不把数字摆出来的话，一条
           312 的读数在账号页上就是一条绿条，而用量表里同一条记录明晃晃写着红色 312
           ——2026-09-18 用户正是这么读错的。绿/红与用量表 turnStateBadgeClass 同口径，
           两个页面共用一套词汇。 -->
      <span
        class="shrink-0 rounded px-1 text-[9px] font-medium leading-4"
        :class="
          entry.healthy
            ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300'
            : 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300'
        "
        data-testid="account-turn-state-shape"
      >
        {{ entry.chars }}
      </span>
      <UsageProgressBar
        class="min-w-0 flex-1"
        :class="entry.active ? undefined : 'opacity-60'"
        :label="entry.label"
        label-width="auto"
        :utilization="entry.remainingPercent"
        :resets-at="entry.expiresAt"
        remaining-capacity
        :color="entry.healthy ? 'purple' : 'amber'"
        :data-testid="`account-turn-state-${entry.model}-${entry.active}`"
      />
    </div>
    <!-- 报的是「真的会被注入」的条数，不是行数。接管关着、一条手填都没有、池子里却
         有几条观测票时，实际注入数就是 0，此时换一句话说清楚，别报成「有生效的」。 -->
    <p
      v-if="entries.length"
      class="text-[10px] text-gray-400"
      :title="detailTitle"
      data-testid="account-turn-state-summary"
    >
      {{
        activeCount
          ? t('admin.accounts.openai.turnStatePool.summary', { n: activeCount })
          : t('admin.accounts.openai.turnStatePool.summaryObservedOnly', { n: entries.length })
      }}
    </p>
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
 * 账号页的 turn-state 实时池：每个模型一行，用与用量窗口同一个 UsageProgressBar
 * 展示「这张票还剩多久到期」。
 *
 * 三个数据源都在 account.extra 里，跟着账号列表一起下发，不额外调接口：
 *
 *  - openai_turn_state_pool：候选池，只在自动接管开着时由网关维护，带 blob。
 *  - openai_turn_state_override：手填覆写，只在自动接管关着时生效，带 blob。
 *  - openai_turn_state_observed：形态观测，所有 Codex 账号都记，**不带 blob**
 *    （blob 是上游令牌，后端刻意只存块数/字符数）。接管关着时它是唯一有数据的源。
 *    **只有未降智的进展示**：一条 312 永远注不出去，摆在票旁边只会被读成票，而
 *    「这个号在铸 312」用量表每行都写着。降智那条仍参与 starved 判定，见下。
 */
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import UsageProgressBar from './UsageProgressBar.vue'
import { useNowTicker } from '@/composables/useNowTicker'
import type { Account } from '@/types'
import {
  decodeTurnState,
  isTurnStateHealthy,
  targetsCodexUpstream,
  TURN_STATE_DEFAULT_TTL_MINUTES,
  TURN_STATE_HOLD_REASON,
  TURN_STATE_SHAPES
} from '@/utils/turnState'
import { formatDateTime, formatTime } from '@/utils/format'

const props = defineProps<{ account: Account }>()
const { t } = useI18n()

// 与 AccountStatusIndicator 用同一个 ticker：那边原来是裸 new Date()，不会重算，
// 暂停到期后两处会各说各话（见 useNowTicker 的注释）。
const sharedNow = useNowTicker()

interface PoolCandidate {
  blob?: string
  model?: string
  minted_at?: string
  failed?: boolean
  fail_streak?: number
}

/**
 * openai_turn_state_observed：账号最近一次**自然铸造**（本次没注入）的形态。
 * 后端只存形态，没有 blob，而且一个账号只存一条 —— 铸什么由账号当时的权重决定，
 * 与模型无关，按模型建表既会丢失更新又会无界增长。model 只说明这个读数是哪个模型的
 * 请求带回来的。
 *
 * 后端也下发算好的 healthy，这里刻意不读它：另外两个源（候选池 / 手填）只有 blob，
 * 健康与否必须前端自己判，读了后端的就等于同一列用两套判据。前后端的形态表是两份手抄
 * （TURN_STATE_SHAPES / openAITurnStateShapes），哪天漂移了，同一个形态会在同一个格子里
 * 一行紫一行琥珀，而页面上没有任何东西提示这是判据分歧。统一由前端这张表说了算。
 */
interface ShapeObservation {
  model?: string
  blocks?: number
  chars?: number
  healthy?: boolean
  minted_at?: string
  observed_at?: string
}

/**
 * 三个源归一后的一行。带 blob 的两个源（候选池 / 手填）在这里就折成 chars + healthy，
 * 与只有形态的观测源对齐——下游只用得着这两个值，留着 blob 只会让渲染路径多一条
 * 「这一行有没有 blob」的分支。
 *
 * active = 「这条票现在真的会被注入」，由本组件按后端的注入分支推出来，不是后端下发的
 * 字段。必填而不是可选：可选 + `!== false` 的默认方向是「没打标就当正在注入」，错在
 * 危险的那一侧——将来多一条供数路径忘了打标，一条只是观测到的记录就会被渲染成正在注入。
 */
interface PoolTicket {
  model: string
  chars: number
  healthy: boolean
  mintedAt: Date
  active: boolean
}

const extra = computed(
  () => (props.account.extra as Record<string, unknown> | undefined) ?? {}
)

const ttlMs = computed(() => {
  const raw = extra.value['openai_turn_state_stale_after_minutes']
  // 与后端 getExtraInt 同口径：数字串也收、小数截断、非正数回落默认。另夹一年上限——后端
  // 没有上限，但天文 TTL 会让 toISOString 抛 RangeError，页面先保住自己。
  const minutes = typeof raw === 'number' || typeof raw === 'string' ? Math.trunc(Number(raw)) : NaN
  const valid = Number.isFinite(minutes) && minutes > 0
  return (valid ? Math.min(minutes, 525_600) : TURN_STATE_DEFAULT_TTL_MINUTES) * 60_000
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
const manualOverrides = computed<PoolTicket[]>(() => {
  const raw = extra.value['openai_turn_state_override']
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return []
  const out: PoolTicket[] = []
  for (const [model, blob] of Object.entries(raw as Record<string, unknown>)) {
    if (typeof blob !== 'string' || !blob.trim() || !model.trim()) continue
    const trimmed = blob.trim()
    const env = decodeTurnState(trimmed)
    if (!env) continue
    out.push({
      model: model.trim(),
      chars: trimmed.length,
      healthy: isTurnStateHealthy(trimmed),
      mintedAt: env.mintedAt,
      active: true
    })
  }
  return out
})

const isManualMode = computed(
  () => isCodexAccount.value && extra.value['openai_turn_state_auto'] !== true
)

/** 自动接管开着时真正会被注入的那些票。关着时后端不写这个键，自然就是空的。 */
const candidatePool = computed<PoolTicket[]>(() => {
  const raw = extra.value['openai_turn_state_pool']
  if (!Array.isArray(raw)) return []
  const out: PoolTicket[] = []
  for (const c of raw as PoolCandidate[]) {
    const model = String(c?.model ?? '').trim()
    const blob = String(c?.blob ?? '').trim()
    if (!model || !blob || c?.failed) continue
    const minted = c?.minted_at ? new Date(c.minted_at) : null
    if (!minted || Number.isNaN(minted.getTime())) continue
    out.push({
      model,
      chars: blob.length,
      healthy: isTurnStateHealthy(blob),
      mintedAt: minted,
      active: true
    })
  }
  return out
})

/**
 * 网关对所有 Codex 账号采集的形态观测，与接管开关无关，最多一条。
 *
 * 健康与否都解出来，但**只有健康的进展示**（见 poolGroups）。降智那条留着是因为
 * starved 要靠它回答「这个号在不在跑流量」——展示上没意义（一条永远注不出去的 312
 * 摆在票旁边只会被读成票），判定上不可替代。
 */
const observedShapes = computed<PoolTicket[]>(() => {
  const raw = extra.value['openai_turn_state_observed']
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return []
  const { model, blocks, chars, minted_at: mintedAtRaw } = raw as ShapeObservation
  if (typeof chars !== 'number' || chars <= 0) return []
  if (typeof blocks !== 'number' || blocks <= 0) return []
  const minted = mintedAtRaw ? new Date(mintedAtRaw) : null
  if (!minted || Number.isNaN(minted.getTime())) return []
  return [
    {
      // 后端不要求能归到模型（归不到也照样是个有效的形态读数），徽章上留空比整条不
      // 展示好：这一行要说的是「最近铸出来的是这个形态」，模型名只是附注。
      model: typeof model === 'string' ? model.trim() : '',
      chars,
      // 走块数而不是 chars：块数是真判据，字符长度受 base64 padding 影响。
      healthy: TURN_STATE_SHAPES.some((shape) => shape.blocks === blocks),
      mintedAt: minted,
      active: false
    }
  ]
})

/**
 * 两件事要同时说清楚：现在有哪些票，以及其中哪些**真的会被注入**。后端的分支是
 * 自动接管开着就只认候选池、完全忽略手填；关着就只认手填。
 *
 * 观测行两种模式下都展示（标成「最近铸出」），但**只展示未降智的**：一条 312 永远
 * 注不出去，摆在票旁边只会被读成票（2026-09-18 用户就是这么读的），而「这个号在铸
 * 312」用量表每行都写着，不需要账号页再说一遍。降智那条仍参与 starved 判定。
 *
 * 返回的是**分组**而不是拼好的一串：entries 要在每组内部各自按模型去重。合成一组
 * 去重的话，同一个模型下生效票会把观测行整个吃掉——而运维盯着的恰恰是那个模型。
 */
const poolGroups = computed<PoolTicket[][]>(() => {
  if (!isCodexAccount.value) return []
  return [
    isManualMode.value ? manualOverrides.value : candidatePool.value,
    observedShapes.value.filter((o) => o.healthy)
  ]
})

interface PoolEntry {
  model: string
  label: string
  /** 「手填」/「最近铸出」标记；空串表示这行就是当前生效的自动注入票。 */
  tag: string
  chars: number
  healthy: boolean
  active: boolean
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
  const out: PoolEntry[] = []
  // 每组各自去重：同一个模型在「生效」和「最近铸出」下各留一条，互不吞没。
  for (const group of poolGroups.value) {
    const seen = new Set<string>()
    for (const c of group) {
      if (seen.has(c.model)) continue
      const expires = c.mintedAt.getTime() + ttlMs.value
      // 票过期就整个不展示：「没有可用票」和「有一张过期票」对运维是同一件事。
      //
      // 观测行不适用这条。它不是票，没有「到期」这回事，后端也永不删这条记录（只按
      // stale_after 节流重写）。按票的口径滤掉的话，超过一个 TTL 没铸过票的账号——闲置
      // 号、刚接手的号、正要决定该不该开接管的号——页面一个字都答不出来，而那恰恰是这
      // 一行存在的全部理由。过期的观测行照常渲染，进度条自然归零，铸造时刻在 tooltip 里。
      if (c.active && expires <= now) continue
      seen.add(c.model)
      out.push({
        model: c.model,
        label: c.model,
        // 三种标法互斥：接管开着 → 候选池生效，不标；接管关着 → 手填生效标「手填」；
        // 形态观测一律标「最近铸出」。不标的话「正在注入」和「只是看到过」在页面上长得
        // 一模一样。
        tag: c.active
          ? isManualMode.value
            ? t('admin.accounts.openai.turnStatePool.manualTag')
            : ''
          : t('admin.accounts.openai.turnStatePool.observedTag'),
        chars: c.chars,
        healthy: c.healthy,
        active: c.active,
        mintedAt: c.mintedAt,
        expiresAt: new Date(expires).toISOString(),
        remainingPercent: Math.round(((expires - now) / ttlMs.value) * 100)
      })
    }
  }
  return out
})

/**
 * 真正会被注入的条数。summary 只能报这个数——把观测行算进去，就会出现「接管关着、
 * 一条手填都没有、池子里 3 条观测」却写着「3 个模型有生效的 Turn-State」，而实际注入
 * 数是 0。那正是这个组件早先犯过的错：歧义换成了错误断言。
 */
const activeCount = computed(() => entries.value.filter((e) => e.active).length)

/**
 * 真正渲染成行的条目。接管开着时，某模型已经有生效票，就不再摆它的「最近铸出」行：池里
 * 的票全是这个号自己铸的（探测或真实流量），生效行本身就证明了「最近铸出 292」，再列一行
 * 只是同一张票重复出现（2026-09-19 用户截图：两行 292 / 68% / 40m 一模一样）。观测行只在
 * 池里拿不出该模型的票时才有信息量：「票已过期，但这个号铸的是 292」。手填模式照旧并列：
 * 手填票与自然铸造是两回事，观测行在说「可能不需要手填了」。
 *
 * 只收敛渲染，不动 entries：tooltip（detailTitle）仍列全部条目，观测行的铸造时刻在那里
 * 还能看到——生效票是注入下重铸的、观测是最后一次自然铸造，两个时刻本来就可能不同。
 */
const visibleEntries = computed<PoolEntry[]>(() => {
  if (isManualMode.value) return entries.value
  const activeModels = new Set(entries.value.filter((e) => e.active).map((e) => e.model))
  return entries.value.filter((e) => e.active || !activeModels.has(e.model))
})

/**
 * 自动接管开着、却一条可用票都拿不出来 = 注入停摆：客户端回带什么就原样发什么，
 * 降智会话下就是 312 直接出站。这跟「没开接管」是两件事，页面上必须分得出来——
 * 2026-09-18 就是因为两者长得一样，用户只能翻 usage 表才发现在裸奔。
 *
 * 判据是 activeCount 而不是 entries.length：形态观测对所有 Codex 账号都采集，池子空到
 * 底时观测行照样在，拿总行数判的话这条告警永远不会亮。
 *
 * 还要求「一个 TTL 内铸过票」（hasRecentMint），否则接管开着的账号只要一小时没流量就
 * 永久挂着琥珀色告警，刚打开开关、还没跑过一次请求的账号也立刻报警——在最常见的状态下
 * 恒亮的告警等于没有告警。裸奔说的是「在跑，而且注不出去」，不是「没在跑」。
 */
const hasRecentMint = computed(() =>
  observedShapes.value.some((o) => o.mintedAt.getTime() + ttlMs.value > sharedNow.value)
)

const starved = computed(
  () =>
    isCodexAccount.value &&
    !isManualMode.value &&
    activeCount.value === 0 &&
    hasRecentMint.value
)

const emptyLabel = computed(() =>
  t(
    starved.value
      ? 'admin.accounts.openai.turnStatePool.starved'
      : 'admin.accounts.openai.turnStatePool.empty'
  )
)

/**
 * 292 猎手（extra.openai_turn_state_hunter 是配置，openai_turn_state_hunt 是运行态）。
 * 配置只读 enabled 与每小时上限；运行态是猎手每次探测后写的：下次窗口、小时计数、
 * 最近 10 次。时间戳都来自后端，页面只做展示。
 */
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
  if (max === null) return ''
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
      })
    )
    .join('\n')
)

/**
 * 降智恢复探测（extra.openai_turn_state_recovery / _state）：走账号自己的出口、间隔随机，
 * 连续若干次 292 判定恢复。判定后后端停止探测，所以这行改说「已恢复」而不是下次窗口。
 */
interface RecoveryState {
  streak?: number
  fail_streak?: number
  next_at?: string
  recovered_at?: string
  cooling_until?: string
  last?: HuntAttempt[]
  last_error?: string
}
const RECOVERY_DEFAULT_STREAK = 5

const recoveryStreakTarget = computed<number | null>(() => {
  const raw = extra.value['openai_turn_state_recovery']
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null
  const cfg = raw as { enabled?: unknown; streak_target?: unknown }
  if (cfg.enabled !== true) return null
  return typeof cfg.streak_target === 'number' && cfg.streak_target > 0 ? cfg.streak_target : RECOVERY_DEFAULT_STREAK
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
  const target = recoveryStreakTarget.value
  if (target === null) return ''
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
  // 「开着但探不了」（模型名配错、账号没流量也没观测过）要看得见，否则这行永远是中性的
  // 「恢复探测 0/5 · 下次 12:34」，原因只在 tooltip 里（第一轮评审 S6）。
  if (st.last_error && !st.last?.length) {
    next = t('admin.accounts.openai.turnStatePool.hunterResultError', { status: '-', error: st.last_error })
  }
  return t('admin.accounts.openai.turnStatePool.recoverySummary', {
    streak: st.streak ?? 0,
    target,
    next
  })
})

// 与猎手行同一套：探不出来时用告警色，别让一行中性文字长期挂着。
const recoveryErrored = computed(() => {
  const st = recoveryState.value
  return !recovered.value && (!!st.last?.[0]?.error || (!!st.last_error && !st.last?.length))
})

const recoveryTitle = computed(() => {
  const attempts = Array.isArray(recoveryState.value.last) ? recoveryState.value.last : []
  if (!attempts.length) return recoveryState.value.last_error ?? ''
  return attempts
    .map((a) =>
      t('admin.accounts.openai.turnStatePool.hunterDetail', {
        time: formatDateTime(parseTime(a.at) ?? new Date(NaN)),
        model: a.model || '-',
        proxy: a.proxy || '-',
        exit: a.exit ? ` (${a.exit})` : '',
        result: hunterAttemptResult(a),
        latency: typeof a.latency_ms === 'number' ? `${(a.latency_ms / 1000).toFixed(1)}s` : '-'
      })
    )
    .join('\n')
})

const detailTitle = computed(() =>
  entries.value
    .map((e) =>
      t('admin.accounts.openai.turnStatePool.detail', {
        // 标记在 tooltip 里要跟回来：徽章上的文字现在只有裸模型名了。
        model: e.tag ? `${e.model}(${e.tag})` : e.model,
        shape: `${e.chars}c`,
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
