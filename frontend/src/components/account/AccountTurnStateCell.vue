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
    <!-- key 必须带上 active：同一个模型现在可能同时有「手填」和「仅观测」两行，
         只用 model 会撞 key，Vue 会告警并错误复用节点。 -->
    <div
      v-for="entry in entries"
      :key="`${entry.model}-${entry.active}`"
      class="flex items-center gap-1"
      data-testid="account-turn-state-row"
    >
      <!-- 「手填」「仅观测」这两个标记必须待在进度条外面。UsageProgressBar 的 label
           徽章是 max-w-[72px] truncate + 10px 字号（约 12 个字符），而模型名本身就有
           12 个字符（gpt-5.6-luna），写成 `{model}(仅观测)` 的话后缀会整个落进省略号
           里——而那是唯一说明「这一行不会被注入」的文字，剩下的区分就只有 opacity-60，
           形态相同时（手填 292 + 观测 292）两行同色同文，等于分不出来。 -->
      <span
        v-if="entry.tag"
        class="shrink-0 rounded bg-gray-100 px-1 text-[9px] leading-4 text-gray-500 dark:bg-gray-800 dark:text-gray-400"
        data-testid="account-turn-state-tag"
      >
        {{ entry.tag }}
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
 *    （blob 是上游令牌，后端刻意只存块数/字符数）。它是「这个号现在铸的是 292 还是
 *    312」的唯一答案，也是唯一在接管关着时也有数据的源。
 */
import { computed, onUnmounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import UsageProgressBar from './UsageProgressBar.vue'
import type { Account } from '@/types'
import {
  decodeTurnState,
  isTurnStateHealthy,
  targetsCodexUpstream,
  TURN_STATE_DEFAULT_TTL_MINUTES,
  TURN_STATE_SHAPES
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
 * 危险的那一侧——将来多一条供数路径忘了打标，一条仅观测的记录就会被渲染成正在注入。
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
 * 网关对所有 Codex 账号采集的形态观测，与接管开关无关。健康与否都记——正在铸 312
 * 恰恰是最需要在页面上看到的那一半。最多一条。
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
      // 展示好：这一行要回答的是「铸的是 292 还是 312」，模型名只是附注。
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
 * 观测行两种模式下都展示（标成「仅观测」）：那是「这个账号现在铸出来的是 292 还是
 * 312」的唯一答案，而那正是判断该不该开接管的前提。接管关着时它更是唯一有数据的
 * 源——后端那时根本不写候选池。
 *
 * 返回的是**分组**而不是拼好的一串：entries 要在每组内部各自按模型去重。合成一组
 * 去重的话，同一个模型下生效票会把观测行整个吃掉——而运维盯着的恰恰是那个模型，
 * 于是最需要看到观测值的那一行反而没有，上面那条理由在它自己的动机场景里就失效了。
 */
const poolGroups = computed<PoolTicket[][]>(() => {
  if (!isCodexAccount.value) return []
  return [isManualMode.value ? manualOverrides.value : candidatePool.value, observedShapes.value]
})

interface PoolEntry {
  model: string
  label: string
  /** 「手填」/「仅观测」标记；空串表示这行就是当前生效的自动注入票。 */
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
  // 每组各自去重：同一个模型在「生效」和「仅观测」下各留一条，互不吞没。
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
        // 形态观测一律标「仅观测」。不标的话「正在注入」和「只是看到过」在页面上长得
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
 * 真正会被注入的条数。summary 只能报这个数——把仅观测的行算进去，就会出现「接管关着、
 * 一条手填都没有、池子里 3 条观测」却写着「3 个模型有生效的 Turn-State」，而实际注入
 * 数是 0。那正是这个组件早先犯过的错：歧义换成了错误断言。
 */
const activeCount = computed(() => entries.value.filter((e) => e.active).length)

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
