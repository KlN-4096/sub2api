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

  it('接管开着：候选池是生效行，形态观测是并列的「最近铸出」行，且不计进生效数', () => {
    const w = render(
      account([cand('m', 60)], { openai_turn_state_observed: obs('m', 30, 12) })
    )
    // 同一个模型两行并存：生效的那条不带标记，观测那条标「最近铸出」。共用一个 seen
    // 去重的话，运维正盯着的那个模型恰好只剩一行，观测值被吃掉。
    expect(rows(w)).toEqual(['|m', 'admin.accounts.openai.turnStatePool.observedTag|m'])
    // team 基线的 332 也算健康，照样展示。
    expect(w.findAll('.bar')[1].attributes('data-color')).toBe('purple')
    // summary 只能报真的会被注入的条数，否则「3 行」会被读成「3 个模型在注入」。
    expect(w.get('[data-testid="account-turn-state-summary"]').text()).toBe(
      'admin.accounts.openai.turnStatePool.summary:{"n":1}'
    )
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
})
