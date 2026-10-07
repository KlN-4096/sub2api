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

const nowSec = Math.floor(Date.now() / 1000)
const isoAgo = (sec: number) => new Date((nowSec - sec) * 1000).toISOString()

// 第一个参数是历史遗留的候选池（组件已经不读它了），留着是因为每个用例都这么调；
// 真正有用的是第二个：恢复探测的配置与运行态。
const account = (_pool: unknown, extra: Record<string, unknown> = {}): Account =>
  ({
    id: 1,
    platform: 'openai',
    type: 'oauth',
    extra: { ...extra }
  }) as unknown as Account

const render = (acc: Account) => mount(AccountTurnStateCell, { props: { account: acc } })

describe('AccountTurnStateCell', () => {
  // 降智恢复探测：答题进度 / 冷却 / 已恢复三态，关着时整行不渲染。
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
