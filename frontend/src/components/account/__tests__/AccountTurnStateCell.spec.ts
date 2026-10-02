import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountTurnStateCell from '../AccountTurnStateCell.vue'
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

const account = (pool: unknown, extra: Record<string, unknown> = {}): Account =>
  ({
    id: 1,
    platform: 'openai',
    type: 'oauth',
    extra: { openai_turn_state_auto: true, openai_turn_state_pool: pool, ...extra }
  }) as unknown as Account

const render = (acc: Account) =>
  mount(AccountTurnStateCell, {
    props: { account: acc },
    global: { stubs: { UsageProgressBar: UsageProgressBarStub } }
  })

describe('AccountTurnStateCell', () => {
  // 下面只剩猎手与恢复探测：票那几行（候选池 / 手填 / 形态观测 / 裸奔告警）
  // 2026-10-02 从组件里删了，位置让给网关落点（AccountGatewayCell）。

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

  // 降智恢复探测：独立于猎手的一行。答题进度 / 冷却 / 已恢复三态，关着时整行不渲染。
  describe('降智恢复探测行', () => {
    const isoIn = (sec: number) => new Date((nowSec + sec) * 1000).toISOString()

    it('攒答对次数时显示 答对/窗口（需几次）与下次窗口', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true },
          openai_turn_state_recovery_state: {
            results: [true, false, true, true],
            next_at: isoIn(1800),
            last: [{ at: isoAgo(60), model: 'gpt-5.6-sol', proxy: 'cox', status: 200, chars: 292, healthy: true, answer: '21' }]
          }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('recoverySummary')
      expect(line.text()).toContain('"successes":3')
      expect(line.text()).toContain('"success":4')
      expect(line.text()).toContain('"window":5')
      expect(line.text()).toContain('hunterNext')
      expect(line.classes()).not.toContain('text-emerald-600')
      expect(line.attributes('title')).toContain('recoveryDetail')
      expect(line.attributes('title')).toContain('recoveryResultHit')
      // t 的 mock 会把嵌套的参数再 JSON.stringify 一次，引号被转义。
      expect(line.attributes('title')).toContain('answer')
      expect(line.attributes('title')).toContain('21')
    })

    it('答错的探测在 tooltip 里写出回答，票长不再是判据', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true },
          openai_turn_state_recovery_state: {
            results: [false],
            next_at: isoIn(1800),
            last: [{ at: isoAgo(60), model: 'gpt-5.6-sol', proxy: 'cox', status: 200, chars: 292, healthy: false, answer: '29' }]
          }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('"successes":0')
      expect(line.attributes('title')).toContain('recoveryResultMiss')
      expect(line.attributes('title')).toContain('answer')
      expect(line.attributes('title')).toContain('29')
      expect(line.attributes('title')).not.toContain('hunterResult')
    })

    it('冷却中显示冷却到点，而不是下次窗口', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true, streak_target: 3 },
          openai_turn_state_recovery_state: { fail_streak: 0, cooling_until: isoIn(3600), next_at: isoIn(3600) }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('recoveryCooling')
      expect(line.text()).toContain('"window":3')
      expect(line.text()).toContain('"success":3')
      expect(line.text()).not.toContain('hunterNext')
    })

    it('判定恢复后显示已恢复并用绿色，不再说下次窗口', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true },
          openai_turn_state_recovery_state: { results: [true, true, false, true, true], recovered_at: isoAgo(120), next_at: isoIn(1800) }
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
            results: [true],
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

    // 「开着但探不了」（出口不通）要看得见：一行中性的 0/5 会被无视。
    it('探测出错时显示错误并用告警色', () => {
      const w = render(
        account([], {
          openai_turn_state_recovery: { enabled: true },
          openai_turn_state_recovery_state: { next_at: isoIn(1800), last_error: 'dial tcp: proxy refused' }
        })
      )
      const line = w.get('[data-testid="account-turn-state-recovery"]')
      expect(line.text()).toContain('hunterResultError')
      expect(line.text()).toContain('dial tcp: proxy refused')
      expect(line.classes()).toContain('text-amber-600')
    })

    it('没开恢复探测就不渲染这一行', () => {
      expect(
        render(account([], { openai_turn_state_recovery_state: { results: [true, true] } }))
          .find('[data-testid="account-turn-state-recovery"]').exists()
      ).toBe(false)
      expect(
        render(account([], { openai_turn_state_recovery: { enabled: false } }))
          .find('[data-testid="account-turn-state-recovery"]').exists()
      ).toBe(false)
    })
  })

  // 猎手行写出模型的回答：答对入池与答错丢掉的票一样长。
  it('猎手行写出模型的回答：答对入池与答错丢掉的票一样长', () => {
    const hunt = (answer: string, healthy: boolean) => ({
      openai_turn_state_hunter: { enabled: true, max_per_hour: 30, pair_mode: true },
      openai_turn_state_hunt: {
        hour_start: isoAgo(600),
        hour_count: 1,
        last: [{ at: isoAgo(60), model: 'gpt-6-astra', proxy: 'webshare', status: 200, chars: 780, healthy, answer, cookies: 2 }]
      }
    })
    const hit = render(account([], hunt('21', true))).get('[data-testid="account-turn-state-hunter"]')
    expect(hit.text()).toContain('hunterResultAnswerHit')
    expect(hit.text()).toContain('21')
    // 随票收到几个 pair cookie 也要能看见：0 个说明那张票只能裸回放。
    expect(hit.attributes('title') ?? '').toContain('detailPair')
    const miss = render(account([], hunt('29', false))).get('[data-testid="account-turn-state-hunter"]')
    expect(miss.text()).toContain('hunterResultAnswerMiss')
    expect(miss.text()).toContain('29')
    // 没有回答的探测（老路径头到手即断）仍按票长显示。
    const legacy = render(
      account([], {
        openai_turn_state_hunter: { enabled: true, max_per_hour: 30 },
        openai_turn_state_hunt: {
          hour_start: isoAgo(600),
          hour_count: 1,
          last: [{ at: isoAgo(60), model: 'gpt-6-astra', proxy: 'webshare', status: 200, chars: 292, healthy: true }]
        }
      })
    ).get('[data-testid="account-turn-state-hunter"]')
    expect(legacy.text()).toContain('hunterResultHit')
    expect(legacy.text()).not.toContain('hunterResultAnswerHit')
  })

  // cpr 走原样中继：猎手 / 恢复探测都不适用，extra 里残留的旧配置一律不展示。
  it('cpr 账号不展示猎手与恢复探测', () => {
    const w = render({
      id: 2,
      platform: 'openai',
      type: 'cpr',
      extra: {
        openai_turn_state_hunter: { enabled: true, max_per_hour: 30 },
        openai_turn_state_recovery: { enabled: true }
      }
    } as unknown as Account)
    expect(w.find('[data-testid="account-turn-state-hunter"]').exists()).toBe(false)
    expect(w.find('[data-testid="account-turn-state-recovery"]').exists()).toBe(false)
  })
})
