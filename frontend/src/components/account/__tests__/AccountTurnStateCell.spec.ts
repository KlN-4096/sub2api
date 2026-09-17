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

  it('过期与已失效的票都不展示——运维要看的是「现在能用的」', () => {
    const w = render(account([cand('expired', 7200), cand('failed', 60, 10, { failed: true })]))
    expect(w.find('[data-testid="account-turn-state-cell"]').exists()).toBe(false)
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

  it('关掉自动接管就不展示——池子还在，但一条都不会被注入', () => {
    const w = render(account([cand('m', 60)], { openai_turn_state_auto: false }))
    expect(w.find('[data-testid="account-turn-state-cell"]').exists()).toBe(false)
  })

  it('生效模型名单外的票不展示', () => {
    const acc = account([cand('gpt-5.6-luna', 60), cand('gpt-6-astra', 60)], {
      openai_turn_state_models: 'gpt-6*'
    })
    const bars = render(acc).findAll('.bar')
    expect(bars.map((b) => b.attributes('data-label'))).toEqual(['gpt-6-astra'])
  })

  it('非 Codex 上游的账号不展示', () => {
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
