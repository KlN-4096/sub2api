import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { afterEach } from 'vitest'
import { enableAutoUnmount } from '@vue/test-utils'
import AccountStatusIndicator from '../AccountStatusIndicator.vue'
import type { Account } from '@/types'

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      // 带参数的键要把参数拼出来：只回显键名的话，「文案里有没有把模型名放进去」这种断言恒绿。
      t: (key: string, params?: Record<string, unknown>) => (params ? `${key}:${JSON.stringify(params)}` : key)
    })
  }
})

vi.mock('@/utils/format', async () => {
  const actual = await vi.importActual<typeof import('@/utils/format')>('@/utils/format')
  return {
    ...actual,
    formatCountdown: () => '1h'
  }
})

// Badges share one clock that lives while any badge is mounted. Unmount every badge after
// each case, or a leaked one keeps a real-timer clock for the fake-timer cases after it.
enableAutoUnmount(afterEach)

it('网关池休息不把复评期限写成恢复承诺，普通封禁保持原文案', async () => {
  const wrapper = mount(AccountStatusIndicator, {
    props: { account: makeAccount({
      temp_unschedulable_until: '2099-01-01T00:00:00Z',
      temp_unschedulable_reason: '网关候选低于10，休息后达到50才恢复'
    }) },
    global: { stubs: { Icon: true } }
  })
  expect(wrapper.text()).toContain('admin.accounts.tempUnschedulable.poolRestPending')
  expect(wrapper.text()).not.toContain('admin.accounts.status.tempUnschedulableUntil')
  expect(wrapper.find('span.whitespace-normal').exists()).toBe(true)
  await wrapper.setProps({ account: makeAccount({
    temp_unschedulable_until: '2099-01-01T00:00:00Z', temp_unschedulable_reason: 'ordinary-error'
  }) })
  expect(wrapper.text()).toContain('admin.accounts.status.tempUnschedulableUntil')
})

function makeAccount(overrides: Partial<Account>): Account {
  return {
    id: 1,
    name: 'account',
    platform: 'antigravity',
    type: 'oauth',
    proxy_id: null,
    concurrency: 1,
    priority: 1,
    status: 'active',
    error_message: null,
    last_used_at: null,
    expires_at: null,
    auto_pause_on_expired: true,
    created_at: '2026-03-15T00:00:00Z',
    updated_at: '2026-03-15T00:00:00Z',
    schedulable: true,
    rate_limited_at: null,
    rate_limit_reset_at: null,
    overload_until: null,
    temp_unschedulable_until: null,
    temp_unschedulable_reason: null,
    session_window_start: null,
    session_window_end: null,
    session_window_status: null,
    ...overrides,
  }
}

describe('AccountStatusIndicator', () => {
  it('freezes status and deadlines during blur, including parent updates', async () => {
    vi.useFakeTimers()
    const account = makeAccount({ platform: 'openai', extra: { openai_gwpool: true },
      overload_until: new Date(Date.now() + 60_000).toISOString() })
    const wrapper = mount(AccountStatusIndicator, { props: { account, gatewayPoolRest: { active: false } } })
    try {
      const frame = wrapper.html()
      await wrapper.setProps({ progressPaused: true })
      await vi.advanceTimersByTimeAsync(90_000)
      await wrapper.setProps({ account: { ...account, overload_until: null }, gatewayPoolRest: { active: true } })
      expect(wrapper.html()).toBe(frame)
      await wrapper.setProps({ progressPaused: false })
      expect(wrapper.text()).toContain('admin.accounts.tempUnschedulable.poolRestPending')
      expect(wrapper.text()).not.toContain('admin.accounts.status.overloaded')
    } finally { vi.useRealTimers() }
  })

  it('keeps real pool rest visible after recheck expiry or generic block clearing', async () => {
    const account = makeAccount({ platform: 'openai', extra: { openai_gwpool: true },
      temp_unschedulable_until: '2020-01-01T00:00:00Z',
      temp_unschedulable_reason: '网关候选低于10，休息后达到40才恢复' })
    const wrapper = mount(AccountStatusIndicator, {
      props: { account, gatewayPoolRest: { active: true } },
      global: { stubs: { Icon: true } }
    })
    expect(wrapper.text()).toContain('admin.accounts.status.tempUnschedulable')
    await wrapper.setProps({ account: { ...account, temp_unschedulable_until: null, temp_unschedulable_reason: null } })
    expect(wrapper.text()).toContain('admin.accounts.tempUnschedulable.poolRestPending')
    await wrapper.setProps({ gatewayPoolRest: { active: false } })
    expect(wrapper.text()).not.toContain('admin.accounts.status.tempUnschedulable')
    await wrapper.setProps({ gatewayPoolRest: undefined, gatewayPoolRestPending: true })
    expect(wrapper.text()).toContain('common.unknown')
    expect(wrapper.text()).not.toContain('admin.accounts.status.active')
  })

  it('expires a deadline on the next 30s clock tick', async () => {
    vi.useFakeTimers()
    const account = makeAccount({ overload_until: new Date(Date.now() + 40_000).toISOString() })
    const wrapper = mount(AccountStatusIndicator, { props: { account }, global: { stubs: { Icon: true } } })
    try {
      await vi.advanceTimersByTimeAsync(30_000)
      expect(wrapper.text()).toContain('admin.accounts.status.overloaded')
      await vi.advanceTimersByTimeAsync(30_000)
      expect(wrapper.text()).not.toContain('admin.accounts.status.overloaded')
    } finally { vi.useRealTimers() }
  })

  it('drives every badge from one shared 30s timer, so rows expire on the same tick', async () => {
    vi.useFakeTimers()
    const until = new Date(Date.now() + 40_000).toISOString()
    const row = (id: number) => mount(AccountStatusIndicator, {
      props: { account: makeAccount({ id, overload_until: until }) }, global: { stubs: { Icon: true } }
    })
    const baseline = vi.getTimerCount()
    const first = row(1)
    await vi.advanceTimersByTimeAsync(15_000)
    const second = row(2)
    try {
      expect(vi.getTimerCount()).toBe(baseline + 1)
      await vi.advanceTimersByTimeAsync(30_000)
      for (const wrapper of [first, second]) expect(wrapper.text()).toContain('admin.accounts.status.overloaded')
      await vi.advanceTimersByTimeAsync(15_000)
      for (const wrapper of [first, second]) expect(wrapper.text()).not.toContain('admin.accounts.status.overloaded')
    } finally { vi.useRealTimers() }
  })

  // Each case changes one rendered input: it must reach the live badge and stay out of a paused frame.
  it('renders every status input and keeps each one out of a paused frame', async () => {
    const future = '2099-01-01T00:00:00Z'
    const quotas = { quota_limit: 10, quota_used: 5, quota_daily_limit: 10, quota_daily_used: 5, quota_weekly_limit: 10, quota_weekly_used: 5 }
    const limited = { model_rate_limits: { 'claude-sonnet-4-5': { rate_limited_at: '2026-03-15T00:00:00Z', rate_limit_reset_at: future } } }
    const field = (label: string, base: Partial<Account>, next: Partial<Account>): [string, Record<string, unknown>, Record<string, unknown>] =>
      [label, { account: makeAccount(base) }, { account: makeAccount({ ...base, ...next }) }]
    const cases = [
      field('status', quotas, { status: 'inactive' }),
      field('schedulable', quotas, { schedulable: false }),
      field('quota_used', quotas, { quota_used: 10 }),
      field('quota_limit', quotas, { quota_limit: 5 }),
      field('quota_daily_used', quotas, { quota_daily_used: 10 }),
      field('quota_daily_limit', quotas, { quota_daily_limit: 5 }),
      field('quota_weekly_used', quotas, { quota_weekly_used: 10 }),
      field('quota_weekly_limit', quotas, { quota_weekly_limit: 5 }),
      field('error_message', { status: 'error', error_message: 'boom' }, { error_message: 'bang' }),
      field('rate_limit_reset_at', {}, { rate_limit_reset_at: future }),
      field('overload_until', {}, { overload_until: future }),
      field('temp_unschedulable_until', {}, { temp_unschedulable_until: future }),
      field('temp_unschedulable_reason', { temp_unschedulable_until: future, temp_unschedulable_reason: 'ordinary-error' },
        { temp_unschedulable_reason: '网关候选低于10，休息后达到50才恢复' }),
      field('model_rate_limits', {}, { extra: limited }),
      field('allow_overages', { extra: limited }, { extra: { ...limited, allow_overages: true } }),
      ['gatewayPoolRest', { account: makeAccount({}) }, { account: makeAccount({}), gatewayPoolRest: { active: true } }],
      ['gatewayPoolRestPending', { account: makeAccount({}) }, { account: makeAccount({}), gatewayPoolRestPending: true }]
    ] as Array<[string, Record<string, unknown>, Record<string, unknown>]>
    for (const [label, base, next] of cases) {
      const wrapper = mount(AccountStatusIndicator, { props: base as any, global: { stubs: { Icon: true } } })
      const frame = wrapper.html()
      await wrapper.setProps({ progressPaused: true })
      await wrapper.setProps(next)
      expect(wrapper.html(), `${label} leaked into the paused frame`).toBe(frame)
      await wrapper.setProps({ progressPaused: false })
      expect(wrapper.html(), `${label} is not rendered`).not.toBe(frame)
    }
  })
  it('Claude 5 系列模型限流时显示 Opus 和 Sonnet 的短别名', () => {
    const wrapper = mount(AccountStatusIndicator, {
      props: {
        account: makeAccount({
          extra: {
            model_rate_limits: {
              'claude-opus-5': {
                rate_limited_at: '2026-07-28T00:00:00Z',
                rate_limit_reset_at: '2099-07-28T00:00:00Z'
              },
              'claude-sonnet-5': {
                rate_limited_at: '2026-07-28T00:00:00Z',
                rate_limit_reset_at: '2099-07-28T00:00:00Z'
              },
              'claude-sonnet-5-5': {
                rate_limited_at: '2026-09-28T00:00:00Z',
                rate_limit_reset_at: '2099-09-28T00:00:00Z'
              }
            }
          }
        })
      },
      global: {
        stubs: {
          Icon: true
        }
      }
    })

    expect(wrapper.text()).toContain('COpus5')
    expect(wrapper.text()).toContain('CSon5')
    expect(wrapper.text()).toContain('CSon55')
    expect(wrapper.text()).not.toContain('claude-sonnet-5')
  })

  it('Grok 账号额度限流时显示自动恢复时间而非临时不可调度', () => {
    const wrapper = mount(AccountStatusIndicator, {
      props: {
        account: makeAccount({
          id: 5,
          name: 'grok-free-1',
          platform: 'grok',
          rate_limited_at: '2026-07-11T12:00:00Z',
          rate_limit_reset_at: '2099-07-11T13:00:00Z',
          temp_unschedulable_until: '2099-07-11T12:30:00Z',
          temp_unschedulable_reason: 'legacy grok rate limited'
        })
      },
      global: {
        stubs: {
          Icon: true
        }
      }
    })

    expect(wrapper.find('.badge-warning').text()).toBe('admin.accounts.status.rateLimited')
    expect(wrapper.text()).toContain('admin.accounts.status.rateLimitedAutoResume')
    expect(wrapper.text()).not.toContain('admin.accounts.status.tempUnschedulable')
  })

  it('模型限流 + overages 启用 + 无 AICredits key → 显示 ⚡ (credits_active)', () => {
    const wrapper = mount(AccountStatusIndicator, {
      props: {
        account: makeAccount({
          id: 1,
          name: 'ag-1',
          extra: {
            allow_overages: true,
            model_rate_limits: {
              'claude-sonnet-4-5': {
                rate_limited_at: '2026-03-15T00:00:00Z',
                rate_limit_reset_at: '2099-03-15T00:00:00Z'
              }
            }
          }
        })
      },
      global: {
        stubs: {
          Icon: true
        }
      }
    })

    expect(wrapper.text()).toContain('⚡')
    expect(wrapper.text()).toContain('CSon45')
  })

  it('模型限流 + overages 未启用 → 普通限流样式（无 ⚡）', () => {
    const wrapper = mount(AccountStatusIndicator, {
      props: {
        account: makeAccount({
          id: 2,
          name: 'ag-2',
          extra: {
            model_rate_limits: {
              'claude-sonnet-4-5': {
                rate_limited_at: '2026-03-15T00:00:00Z',
                rate_limit_reset_at: '2099-03-15T00:00:00Z'
              }
            }
          }
        })
      },
      global: {
        stubs: {
          Icon: true
        }
      }
    })

    expect(wrapper.text()).toContain('CSon45')
    expect(wrapper.text()).not.toContain('⚡')
  })

  it('AICredits key 生效 → 显示积分已用尽 (credits_exhausted)', () => {
    const wrapper = mount(AccountStatusIndicator, {
      props: {
        account: makeAccount({
          id: 3,
          name: 'ag-3',
          extra: {
            allow_overages: true,
            model_rate_limits: {
              'AICredits': {
                rate_limited_at: '2026-03-15T00:00:00Z',
                rate_limit_reset_at: '2099-03-15T00:00:00Z'
              }
            }
          }
        })
      },
      global: {
        stubs: {
          Icon: true
        }
      }
    })

    expect(wrapper.text()).toContain('admin.accounts.status.creditsExhausted')
  })

  it('模型限流 + overages 启用 + AICredits key 生效 → 普通限流样式（积分耗尽，无 ⚡）', () => {
    const wrapper = mount(AccountStatusIndicator, {
      props: {
        account: makeAccount({
          id: 4,
          name: 'ag-4',
          extra: {
            allow_overages: true,
            model_rate_limits: {
              'claude-sonnet-4-5': {
                rate_limited_at: '2026-03-15T00:00:00Z',
                rate_limit_reset_at: '2099-03-15T00:00:00Z'
              },
              'AICredits': {
                rate_limited_at: '2026-03-15T00:00:00Z',
                rate_limit_reset_at: '2099-03-15T00:00:00Z'
              }
            }
          }
        })
      },
      global: {
        stubs: {
          Icon: true
        }
      }
    })

    // 模型限流 + 积分耗尽 → 不应显示 ⚡
    expect(wrapper.text()).toContain('CSon45')
    expect(wrapper.text()).not.toContain('⚡')
    // AICredits 积分耗尽状态应显示
    expect(wrapper.text()).toContain('admin.accounts.status.creditsExhausted')
  })
})
