import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountTurnStateCell from '../AccountTurnStateCell.vue'
import { turnStateFixture } from './turnStateFixture'
import type { Account } from '@/types'

// 只替 useI18n，其余保留真实导出：src/utils/format.ts 会 import src/i18n/index.ts，
// 整个模块被 mock 掉的话 createI18n 就没了。
vi.mock('vue-i18n', async (importOriginal) => ({
  ...(await importOriginal<typeof import('vue-i18n')>()),
  useI18n: () => ({
    t: (key: string, params?: Record<string, unknown>) =>
      params ? `${key}:${JSON.stringify(params)}` : key
  })
}))

const UsageProgressBarStub = {
  name: 'UsageProgressBar',
  props: ['label', 'utilization', 'resetsAt', 'color', 'remainingCapacity', 'labelWidth'],
  template:
    '<div class="bar" :data-label="label" :data-color="color" :data-resets="resetsAt">{{ utilization }}</div>'
}

const nowSec = Math.floor(Date.now() / 1000)
const isoAgo = (sec: number) => new Date((nowSec - sec) * 1000).toISOString()

/** 一条候选：blob 走真 Fernet 信封，minted_at 是后端写的权威铸造时刻。 */
const cand = (model: string, agoSec: number, blocks = 10, rest: Record<string, unknown> = {}) => ({
  model,
  blob: turnStateFixture(nowSec - agoSec, blocks),
  minted_at: isoAgo(agoSec),
  ...rest
})

/**
 * openai_turn_state_observed 的值：后端刻意不存 blob，只有块数/字符数，而且一个账号
 * 只存**一条**（铸什么由账号权重定，与模型无关）。chars 按 base64 长度公式算，与后端
 * openAITurnStateShapes 同源：4*ceil((57+16*blocks)/3)。
 */
const obs = (model: string, agoSec: number, blocks = 10) => ({
  model,
  blocks,
  chars: 4 * Math.ceil((57 + 16 * blocks) / 3),
  healthy: blocks === 10 || blocks === 12,
  minted_at: isoAgo(agoSec),
  observed_at: isoAgo(agoSec)
})

const account = (pool: unknown, extra: Record<string, unknown> = {}): Account =>
  ({
    id: 1,
    platform: 'openai',
    type: 'cpr',
    extra: { openai_turn_state_auto: true, openai_turn_state_pool: pool, ...extra }
  }) as unknown as Account

const render = (acc: Account) =>
  mount(AccountTurnStateCell, {
    props: { account: acc },
    global: { stubs: { UsageProgressBar: UsageProgressBarStub } }
  })

/**
 * 每行折成 `标记|模型名`。标记是独立元素而不是 label 的后缀：label 徽章是 72px + truncate，
 * `{model}(最近铸出)` 里的后缀会整个落进省略号，而那是唯一说明这行不会被注入的文字。
 */
const rows = (w: ReturnType<typeof render>) =>
  w.findAll('[data-testid="account-turn-state-row"]').map((r) => {
    const tag = r.find('[data-testid="account-turn-state-tag"]')
    return `${tag.exists() ? tag.text() : ''}|${r.get('.bar').attributes('data-label')}`
  })

describe('AccountTurnStateCell', () => {
  it('每个模型一行，取数组里第一条未失效的', () => {
    const w = render(
      account([cand('gpt-5.6-luna', 300), cand('gpt-5.6-luna', 900), cand('gpt-6-astra', 60, 11)])
    )
    const bars = w.findAll('.bar')
    expect(bars).toHaveLength(2)
    expect(bars.map((b) => b.attributes('data-label'))).toEqual(['gpt-5.6-luna', 'gpt-6-astra'])
    // 剩余比例：luna 铸于 5 分钟前，1 小时有效 → 还剩约 92%
    expect(Number(bars[0].text())).toBeGreaterThan(88)
    expect(Number(bars[0].text())).toBeLessThanOrEqual(92)
  })

  it('疑似降智的票换色——功能存在的理由就是一眼挑出来', () => {
    const w = render(account([cand('healthy', 60, 10), cand('suspect', 60, 11)]))
    const byLabel = Object.fromEntries(
      w.findAll('.bar').map((b) => [b.attributes('data-label'), b.attributes('data-color')])
    )
    expect(byLabel.healthy).toBe('purple')
    expect(byLabel.suspect).toBe('amber')
  })

  it('过期与已失效的票都不展示；接管开着、在跑流量却没票要报「裸奔」', () => {
    // 观测行代表「这个号一分钟前还在铸票」＝确实在跑流量。现网每次铸票都会写一条，
    // 所以票失效时它一定在——没有它就不算裸奔，只是没流量。
    const w = render(
      account([cand('expired', 7200), cand('failed', 60, 10, { failed: true })], {
        openai_turn_state_observed: obs('failed', 60, 11)
      })
    )
    // 312 不进展示，所以一行都没有 —— 但它仍然证明了「一分钟前还在铸票」，
    // 裸奔告警靠的就是这个。
    expect(rows(w)).toEqual([])
    // 整块消失会让「没票」和「组件没渲染」长得一样，所以要占位。
    // 占位必须带标识：它挤在额度列下方，裸的 - 认不出是什么，等于没显示。
    //
    // 更要紧的是「接管开着却一条票都拿不出来」必须与「没开接管」分开说：那时客户端
    // 回带什么就原样发什么，降智会话下就是 312 直接出站。两者共用一个文案的那阵子，
    // 用户只能翻 usage 表才发现在裸奔。
    const empty = w.get('[data-testid="account-turn-state-empty"]')
    expect(empty.text()).toBe('admin.accounts.openai.turnStatePool.starved')
    expect(empty.attributes('data-starved')).toBe('true')
  })

  it('接管开着但一个 TTL 内没铸过票：是「没流量」不是「裸奔」', () => {
    // 恒亮的告警等于没有告警：接管开着的号只要一小时没请求就永久挂琥珀，刚打开开关
    // 还没跑过一次的号也立刻报警，运维很快就会学会无视它。
    const w = render(account([], { openai_turn_state_observed: obs('m', 7200) }))
    expect(w.find('[data-testid="account-turn-state-empty"]').exists()).toBe(false)
    // 过期的观测行照常渲染（它不是票，没有到期这回事，后端也永不删它）。闲置号、刚接手
    // 的号恰恰最需要页面回答「这个号最近铸出来的是什么」，按票的口径滤掉就等于在自己
    // 的动机场景里失效。
    expect(rows(w)).toEqual(['admin.accounts.openai.turnStatePool.observedTag|m'])
    // 此时一条都注不出去，summary 必须换口径说，不能报成「1 个模型有生效的」。
    expect(w.get('[data-testid="account-turn-state-summary"]').text()).toBe(
      'admin.accounts.openai.turnStatePool.summaryObservedOnly:{"n":1}'
    )
  })

  it('倒计时是活的：时间推进到过期后条目消失', async () => {
    vi.useFakeTimers()
    try {
      const w = render(account([cand('m', 60)]))
      expect(w.findAll('.bar')).toHaveLength(1)
      await vi.advanceTimersByTimeAsync(61 * 60 * 1000)
      expect(w.findAll('.bar')).toHaveLength(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it('有效期跟随账号配置的分钟数', () => {
    const w = render(
      account([cand('m', 5400)], { openai_turn_state_stale_after_minutes: 180 })
    )
    expect(w.findAll('.bar')).toHaveLength(1)
  })

  it('关掉自动接管：手填票标「手填」，未降智的观测仍展示但标「最近铸出」', () => {
    const w = render(
      account([cand('stale-pool', 60)], {
        openai_turn_state_auto: false,
        openai_turn_state_override: { other: turnStateFixture(nowSec - 60, 10) },
        openai_turn_state_observed: obs('other', 30, 10)
      })
    )
    // 整个顺序都钉住，三件事一次说清：
    //  - 手填那组整体排在观测之前：生效的票要先看见，不能被观测行挤到下面去。
    //  - 未降智的观测照样要展示：接管关着时它是唯一有数据的源，后端那时根本不写候选池。
    //  - 同一个模型（other）在两组里各留一条：合成一组去重的话，运维盯着的恰恰是那个
    //    模型，观测行会被手填行整个吃掉。
    //
    // 同时钉住候选池在接管关着时不出现：那是死数据（后端那时根本不写它，留下的最多
    // 再躺 1 小时），拿它冒充观测就是用一条注不出去的票充当现状读数。
    expect(rows(w)).toEqual([
      'admin.accounts.openai.turnStatePool.manualTag|other',
      'admin.accounts.openai.turnStatePool.observedTag|other'
    ])
  })

  it('接管开着：模型已有生效票就不再摆它的「最近铸出」行，别的模型的观测照常并列', () => {
    // 池里的票全是这个号自己铸的，生效行本身就证明了「最近铸出 292」；再列一行只是同一张票
    // 重复出现（2026-09-19 用户截图两行 292 / 68% / 40m 一模一样）。
    const same = render(account([cand('m', 60)], { openai_turn_state_observed: obs('m', 30, 12) }))
    expect(rows(same)).toEqual(['|m'])
    // summary 只能报真的会被注入的条数。
    expect(same.get('[data-testid="account-turn-state-summary"]').text()).toBe(
      'admin.accounts.openai.turnStatePool.summary:{"n":1}'
    )

    // 观测的是另一个模型：那个模型没有生效票，观测行才有信息量。team 基线的 332 也算健康。
    const other = render(account([cand('m', 60)], { openai_turn_state_observed: obs('n', 30, 12) }))
    expect(rows(other)).toEqual(['|m', 'admin.accounts.openai.turnStatePool.observedTag|n'])
    expect(other.findAll('.bar')[1].attributes('data-color')).toBe('purple')
  })

  it('接管开着、生效票过期：观测行回来，说明「票没了但这个号铸的是 292」', () => {
    const w = render(account([cand('m', 7200)], { openai_turn_state_observed: obs('m', 7200) }))
    expect(rows(w)).toEqual(['admin.accounts.openai.turnStatePool.observedTag|m'])
  })

  it('手填模式：同一模型的手填票与「最近铸出」并列', () => {
    const w = render(
      account([], {
        openai_turn_state_auto: false,
        openai_turn_state_override: { m: turnStateFixture(nowSec - 60, 10) },
        openai_turn_state_observed: obs('m', 30)
      })
    )
    expect(rows(w)).toEqual([
      'admin.accounts.openai.turnStatePool.manualTag|m',
      'admin.accounts.openai.turnStatePool.observedTag|m'
    ])
  })

  it('接管开着、池空但有观测行：仍要报「裸奔」', () => {
    // 降智的观测不进展示（用量表每行都写着，账号页再说一遍没意义），但它必须仍然
    // 参与 starved 判定——「接管开着、在铸 312、一张票都注不出去」正是最该报警的时刻，
    // 而这个账号一行都不渲染。judging 用 activeCount + hasRecentMint，不是行数。
    const w = render(account([], { openai_turn_state_observed: obs('m', 60, 11) }))
    expect(w.findAll('.bar')).toHaveLength(0)
    const empty = w.get('[data-testid="account-turn-state-empty"]')
    expect(empty.text()).toBe('admin.accounts.openai.turnStatePool.starved')
    expect(empty.attributes('data-starved')).toBe('true')
  })

  it('形态观测异常时安全降级为不展示', () => {
    const bad = (observed: unknown) =>
      render(account([], { openai_turn_state_auto: false, openai_turn_state_observed: observed }))
    expect(bad('{not json').find('.bar').exists()).toBe(false)
    expect(bad([{ chars: 292 }]).find('.bar').exists()).toBe(false) // 数组：旧的按模型建表形态
    expect(bad({ chars: 292, blocks: 10 }).find('.bar').exists()).toBe(false) // 没有 minted_at
    expect(bad({ blocks: 10, minted_at: isoAgo(60) }).find('.bar').exists()).toBe(false) // 没有 chars
    expect(bad({ chars: 292, minted_at: isoAgo(60) }).find('.bar').exists()).toBe(false) // 没有 blocks
  })

  it('归不到模型的观测照样展示，徽章留空', () => {
    // 这一行要回答的是「铸的是 292 还是 312」，模型名只是附注。因为归不到模型就整条
    // 不展示的话，最该看见的那个读数反而没有。
    const w = render(
      account([], { openai_turn_state_auto: false, openai_turn_state_observed: obs('', 60, 10) })
    )
    expect(rows(w)).toEqual(['admin.accounts.openai.turnStatePool.observedTag|'])
  })

  it.each([
    [10, false, true],
    [11, true, false],
    [12, false, true],
    [13, true, false]
  ])('观测行按块数 %i 判健康，不信后端下发的 healthy=%s', (blocks, backendHealthy, shown) => {
    // 前后端的形态表是两份手抄。读后端的 healthy 就等于同一列用两套判据：哪天漂移了，
    // 同一个形态的展示与否会前后端打架，而页面上没有任何东西提示这是判据分歧。
    // 这里刻意把后端的 healthy 填成与块数相反的值 —— 展示与否必须只由块数说了算。
    const w = render(
      account([], {
        openai_turn_state_auto: false,
        openai_turn_state_observed: { ...obs('m', 60, blocks), healthy: backendHealthy }
      })
    )
    expect(w.find('.bar').exists()).toBe(shown)
  })

  it('每行都把形态数字摆出来，绿/红与用量表同口径', () => {
    // 进度条和百分比的颜色来自剩余时间（74% → 绿），与健康度无关；不摆数字的话，
    // 一条票在账号页上只有一条绿条，而用量表里同一条记录写着 292/312。
    // 手填是唯一可能出现降智值的来源（人手填错），所以用它来取红色那一档。
    const w = render(
      account([cand('m', 60, 10)], {
        openai_turn_state_auto: false,
        openai_turn_state_override: { bad: turnStateFixture(nowSec - 60, 11) },
        openai_turn_state_observed: obs('m', 30, 12)
      })
    )
    const chips = w.findAll('[data-testid="account-turn-state-shape"]')
    expect(chips.map((c) => c.text())).toEqual(['312', '332'])
    expect(chips[0].classes().join(' ')).toContain('red')
    expect(chips[1].classes().join(' ')).toContain('green')
  })

  it('接管关着且一条票都没有：是「没开」不是「裸奔」', () => {
    const w = render(account([], { openai_turn_state_auto: false }))
    const empty = w.get('[data-testid="account-turn-state-empty"]')
    expect(empty.text()).toBe('admin.accounts.openai.turnStatePool.empty')
    expect(empty.attributes('data-starved')).toBe('false')
  })

  it('team 形态的 12 块也算健康——个人号与 team 号各有各的基线', () => {
    // 只认 individual 的 10 块时，team 号铸出来的每一条都会被判降智：接管会一直注入、
    // 一直判失效，最后把一个从头到尾正常的号停掉。
    const w = render(
      account([cand('individual', 60, 10), cand('team', 60, 12), cand('degraded', 60, 11)])
    )
    const byLabel = Object.fromEntries(
      w.findAll('.bar').map((b) => [b.attributes('data-label'), b.attributes('data-color')])
    )
    expect(byLabel.individual).toBe('purple')
    expect(byLabel.team).toBe('purple')
    expect(byLabel.degraded).toBe('amber')
  })

  it('非 Codex 上游的账号整块不展示', () => {
    const acc = account([cand('m', 60)])
    ;(acc as unknown as Record<string, unknown>).type = 'apikey'
    expect(render(acc).find('[data-testid="account-turn-state-cell"]').exists()).toBe(false)
  })

  it('池子形态异常时安全降级为不展示', () => {
    expect(render(account('{not json')).find('.bar').exists()).toBe(false)
    expect(render(account(undefined)).find('.bar').exists()).toBe(false)
    expect(render(account([{ model: 'm', blob: 'x' }])).find('.bar').exists()).toBe(false)
  })

  // 猎手行：本小时次数 / 下次窗口 / 上次结果，最近 10 次在 tooltip。没开猎手不渲染。
  it('开了猎手时多一行猎手状态，最近几次在 tooltip 里', () => {
    const w = render(
      account([], {
        openai_turn_state_hunter: { enabled: true, max_per_hour: 30 },
        openai_turn_state_hunt: {
          next_at: new Date(Date.now() + 600_000).toISOString(),
          hour_start: isoAgo(600),
          hour_count: 3,
          last: [
            { at: isoAgo(60), model: 'gpt-6-astra', proxy: 'webshare', status: 200, chars: 312, healthy: false },
            { at: isoAgo(120), model: 'gpt-6-astra', proxy: 'cox', status: 0, error: 'proxy refused', exit: '203.0.113.7' }
          ]
        }
      })
    )
    const line = w.get('[data-testid="account-turn-state-hunter"]')
    expect(line.text()).toContain('turnStatePool.hunterSummary')
    expect(line.text()).toContain('"count":3')
    expect(line.text()).toContain('"max":30')
    expect(line.text()).toContain('hunterNext')
    expect(line.text()).toContain('hunterResultMiss')
    const title = line.attributes('title') ?? ''
    expect(title.split('\n')).toHaveLength(2)
    // t 的 mock 会把嵌套的参数再 JSON.stringify 一次，引号被转义，只认键名和数值。
    expect(title).toContain('hunterResultMiss')
    expect(title).toContain('312')
    expect(title).toContain('proxy refused')
    expect(title).toContain('(203.0.113.7)')
    expect(title.split('\n')[0]).toContain('"exit":""')
  })

  it('小时窗过了计数归零、退避到期显示待命；没开猎手整行不渲染', () => {
    const w = render(
      account([], {
        openai_turn_state_hunter: { enabled: true },
        openai_turn_state_hunt: { next_at: isoAgo(1), hour_start: isoAgo(7200), hour_count: 9, last: [] }
      })
    )
    const line = w.get('[data-testid="account-turn-state-hunter"]')
    expect(line.text()).toContain('"count":0')
    expect(line.text()).toContain('"max":30')
    expect(line.text()).toContain('hunterReady')
    expect(line.text()).toContain('hunterLastNone')

    expect(render(account([], { openai_turn_state_hunt: { hour_count: 9 } })).find('[data-testid="account-turn-state-hunter"]').exists()).toBe(false)
    expect(render(account([], { openai_turn_state_hunter: { enabled: false } })).find('[data-testid="account-turn-state-hunter"]').exists()).toBe(false)
  })

  // 没在等窗也没被门槛挡、最近一次是 312：这轮还在猎，显示「探测中」而不是「待命」（2026-09-19
  // 反馈：多账号排队时页面写着待命，看不出它其实在等轮次）。命中后 NextAt 留在过去才是「待命」。
  it('最近一次未命中且没有下次/门槛时显示探测中', () => {
    const hunt = (healthy: boolean) => ({
      openai_turn_state_hunter: { enabled: true },
      openai_turn_state_hunt: {
        next_at: isoAgo(1),
        hour_start: isoAgo(60),
        hour_count: 4,
        last: [{ at: isoAgo(30), model: 'gpt-6-astra', proxy: 'webshare', status: 200, chars: healthy ? 292 : 312, healthy }]
      }
    })
    const probing = render(account([], hunt(false))).get('[data-testid="account-turn-state-hunter"]')
    expect(probing.text()).toContain('hunterProbing')
    expect(probing.text()).not.toContain('hunterReady')
    const done = render(account([], hunt(true))).get('[data-testid="account-turn-state-hunter"]')
    expect(done.text()).toContain('hunterReady')
    expect(done.text()).not.toContain('hunterProbing')
  })

  // 后端被门槛挡住时会记原因（gate）：「无流量暂停」和「票未到期」都不能显示成「待命」。
  it.each([
    ['idle', 'hunterGateIdle'],
    ['fresh', 'hunterGateFresh']
  ])('gate=%s 显示原因而不是待命', (gate, key) => {
    const w = render(
      account([], {
        openai_turn_state_hunter: { enabled: true },
        openai_turn_state_hunt: { next_at: isoAgo(1), hour_start: isoAgo(60), hour_count: 0, last: [], gate }
      })
    )
    const line = w.get('[data-testid="account-turn-state-hunter"]')
    expect(line.text()).toContain(key)
    expect(line.text()).not.toContain('hunterReady')
    expect(line.classes()).not.toContain('text-amber-600')
  })

  // 被停的模型走的是空闲门槛的后门（停着就说明刚有人请求过），gate 又是上一个 tick 的快照：
  // 「刚发完请求被停」那几十秒里这行写着「无流量·暂停」，和状态列的降智暂停直接打架。
  it('gate=idle 但有活着的降智暂停：写暂停补票，不写无流量', () => {
    const held = (resetAt: string) => ({
      openai_turn_state_hunter: { enabled: true },
      openai_turn_state_hunt: { next_at: isoAgo(1), hour_start: isoAgo(60), hour_count: 0, last: [], gate: 'idle' },
      model_rate_limits: { 'gpt-6-astra': { rate_limit_reset_at: resetAt, reason: 'turn_state_hold' } }
    })
    const line = render(account([], held(isoAgo(-600)))).get('[data-testid="account-turn-state-hunter"]')
    expect(line.text()).toContain('hunterGateHeld')
    expect(line.text()).not.toContain('hunterGateIdle')

    // 过期的条目不算：放回后这行要回到「无流量·暂停」。
    const expired = render(account([], held(isoAgo(600)))).get('[data-testid="account-turn-state-hunter"]')
    expect(expired.text()).toContain('hunterGateIdle')
    expect(expired.text()).not.toContain('hunterGateHeld')
  })

  // 暂停到期这一行要自己翻回去。没有这条的话，把 useNowTicker 换成不带定时器的 ref
  // 也照样绿——「页面挂着不刷新会不会冻结」正是这次改动要修的东西。
  it('降智暂停到期后猎手行自己翻回无流量', async () => {
    vi.useFakeTimers()
    try {
      const w = render(
        account([], {
          openai_turn_state_hunter: { enabled: true },
          openai_turn_state_hunt: { next_at: isoAgo(1), hour_start: isoAgo(60), hour_count: 0, last: [], gate: 'idle' },
          model_rate_limits: {
            'gpt-6-astra': { rate_limit_reset_at: new Date(Date.now() + 40_000).toISOString(), reason: 'turn_state_hold' }
          }
        })
      )
      expect(w.get('[data-testid="account-turn-state-hunter"]').text()).toContain('hunterGateHeld')
      await vi.advanceTimersByTimeAsync(60_000)
      expect(w.get('[data-testid="account-turn-state-hunter"]').text()).toContain('hunterGateIdle')
    } finally {
      vi.useRealTimers()
    }
  })

  // 别的原因写的 model_rate_limits（真限流、管理员操作）不能借降智暂停的壳。
  it('gate=idle 且限流不是降智暂停写的：仍写无流量', () => {
    const w = render(
      account([], {
        openai_turn_state_hunter: { enabled: true },
        openai_turn_state_hunt: { next_at: isoAgo(1), hour_start: isoAgo(60), hour_count: 0, last: [], gate: 'idle' },
        model_rate_limits: { 'gpt-6-astra': { rate_limit_reset_at: isoAgo(-600) } }
      })
    )
    expect(w.get('[data-testid="account-turn-state-hunter"]').text()).toContain('hunterGateIdle')
  })

  // 后端要猎手开关与自动接管同时开着才跑：接管关着时不能写「待命」，那是在说一个永远
  // 不会发生的事。
  it('猎手开着、自动接管关着：标成未生效并用告警色', () => {
    const w = render(
      account([], {
        openai_turn_state_auto: false,
        openai_turn_state_hunter: { enabled: true, max_per_hour: 30 },
        openai_turn_state_hunt: { next_at: isoAgo(1), hour_start: isoAgo(60), hour_count: 2, last: [] }
      })
    )
    const line = w.get('[data-testid="account-turn-state-hunter"]')
    expect(line.text()).toContain('turnStatePool.hunterNeedsAuto')
    expect(line.text()).not.toContain('hunterReady')
    expect(line.classes()).toContain('text-amber-600')
  })

  // 后端 parseExtraFloat64 收数字串：TTL 口径要一样，否则条子画 60 分钟、后端 10 分钟就不注入。
  it('stale_after_minutes 是数字串也按它算 TTL', () => {
    const w = render(account([cand('m', 300)], { openai_turn_state_stale_after_minutes: '10' }))
    const bar = w.get('.bar')
    expect(bar.text()).toBe('50')
    expect(bar.attributes('data-resets')).toBe(new Date((nowSec - 300 + 600) * 1000).toISOString())
  })
  // 降智恢复探测：独立于猎手的一行。连胜进度 / 冷却 / 已恢复三态，关着时整行不渲染。
  describe('降智恢复探测行', () => {
    const isoIn = (sec: number) => new Date((nowSec + sec) * 1000).toISOString()

    it('攒连胜时显示进度与下次窗口', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true },
          openai_turn_state_recovery_state: {
            streak: 3,
            next_at: isoIn(1800),
            last: [{ at: isoAgo(60), model: 'gpt-6-astra', proxy: 'cox', status: 200, chars: 292, healthy: true }]
          }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('recoverySummary')
      expect(line.text()).toContain('"streak":3')
      expect(line.text()).toContain('"target":5')
      expect(line.text()).toContain('hunterNext')
      expect(line.classes()).not.toContain('text-emerald-600')
      expect(line.attributes('title')).toContain('hunterResultHit')
    })

    it('冷却中显示冷却到点，而不是下次窗口', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true, streak_target: 3 },
          openai_turn_state_recovery_state: { streak: 0, fail_streak: 0, cooling_until: isoIn(3600), next_at: isoIn(3600) }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('recoveryCooling')
      expect(line.text()).toContain('"target":3')
      expect(line.text()).not.toContain('hunterNext')
    })

    it('判定恢复后显示已恢复并用绿色，不再说下次窗口', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true },
          openai_turn_state_recovery_state: { streak: 5, recovered_at: isoAgo(120), next_at: isoIn(1800) }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('recoveryDone')
      expect(line.text()).not.toContain('recoverySummary')
      expect(line.classes()).toContain('text-emerald-600')
    })

    // 后端曾把零值时间落成 "0001-01-01T00:00:00Z"（omitempty 对 struct 不生效），而 JS 的 Date
    // 认这个字符串——老账号行里还留着这种值，不挡就会从第一次探测起一直写着「已恢复」。
    it('零值时间不算已恢复，也不算冷却', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true },
          openai_turn_state_recovery_state: {
            streak: 1,
            next_at: isoIn(1800),
            recovered_at: '0001-01-01T00:00:00Z',
            cooling_until: '0001-01-01T00:00:00Z'
          }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('recoverySummary')
      expect(line.text()).not.toContain('recoveryDone')
      expect(line.text()).not.toContain('recoveryCooling')
      expect(line.classes()).not.toContain('text-emerald-600')
    })

    // 「开着但探不了」（模型名配错、没流量也没观测过）要看得见：一行中性的 0/5 会被无视。
    it('探不出模型时显示错误并用告警色', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true },
          openai_turn_state_recovery_state: { streak: 0, next_at: isoIn(1800), last_error: 'no model to probe' }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('hunterResultError')
      expect(line.text()).toContain('no model to probe')
      expect(line.classes()).toContain('text-amber-600')
    })

    it('没开恢复探测就不渲染这一行', () => {
      expect(
        render(account([], { openai_turn_state_recovery_state: { streak: 2 } }))
          .find('[data-testid="account-turn-state-recovery"]').exists()
      ).toBe(false)
      expect(
        render(account([], { openai_turn_state_recovery: { enabled: false } }))
          .find('[data-testid="account-turn-state-recovery"]').exists()
      ).toBe(false)
    })
  })
})
