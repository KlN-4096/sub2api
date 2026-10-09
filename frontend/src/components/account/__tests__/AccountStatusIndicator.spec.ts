import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
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
  wrapper.unmount()
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
    } finally { wrapper.unmount(); vi.useRealTimers() }
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
    wrapper.unmount()
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

  // 降智暂停是模型级的（model_rate_limits + reason=turn_state_hold）：显示成「寻票中」而不是
  // 普通模型限流，不报倒计时；账号本身不算临时不可调度。
  it('降智暂停：按模型显示寻票中，不是普通限流也不是账号级暂停', () => {
    const wrapper = mount(AccountStatusIndicator, {
      props: {
        account: makeAccount({
          platform: 'openai',
          extra: {
            model_rate_limits: {
              'gpt-6-astra': {
                rate_limited_at: '2026-03-15T00:00:00Z',
                rate_limit_reset_at: '2099-03-15T00:00:00Z',
                reason: 'turn_state_hold'
              }
            }
          }
        })
      },
      global: { stubs: { Icon: true } }
    })

    expect(wrapper.text()).toContain('admin.accounts.status.turnStateHoldShort')
    expect(wrapper.text()).toContain('gpt-6-astra')
    expect(wrapper.text()).toContain('admin.accounts.status.turnStateHold:{"model":"gpt-6-astra"}')
    expect(wrapper.text()).not.toContain('admin.accounts.status.modelRateLimitedUntil')
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
