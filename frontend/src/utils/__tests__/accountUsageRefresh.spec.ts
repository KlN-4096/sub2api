import { describe, expect, it } from 'vitest'
import { buildGrokUsageRefreshKey, buildOpenAIUsageRefreshKey, isOpenAICodexUsageAccount } from '../accountUsageRefresh'

describe('buildOpenAIUsageRefreshKey', () => {
  it('会在 codex 快照变化时生成不同 key', () => {
    const base = {
      id: 1,
      platform: 'openai',
      type: 'oauth',
      updated_at: '2026-03-07T10:00:00Z',
      last_used_at: '2026-03-07T09:59:00Z',
      extra: {
        codex_usage_updated_at: '2026-03-07T10:00:00Z',
        codex_5h_used_percent: 0,
        codex_7d_used_percent: 0
      }
    } as any

    const next = {
      ...base,
      extra: {
        ...base.extra,
        codex_usage_updated_at: '2026-03-07T10:01:00Z',
        codex_5h_used_percent: 100
      }
    }

    expect(buildOpenAIUsageRefreshKey(base)).not.toBe(buildOpenAIUsageRefreshKey(next))
  })

  it('会在 last_used_at 变化时生成不同 key', () => {
    const base = {
      id: 3,
      platform: 'openai',
      type: 'oauth',
      updated_at: '2026-03-07T10:00:00Z',
      last_used_at: '2026-03-07T10:00:00Z',
      extra: {
        codex_usage_updated_at: '2026-03-07T10:00:00Z',
        codex_5h_used_percent: 12,
        codex_7d_used_percent: 24
      }
    } as any

    const next = {
      ...base,
      last_used_at: '2026-03-07T10:02:00Z'
    }

    expect(buildOpenAIUsageRefreshKey(base)).not.toBe(buildOpenAIUsageRefreshKey(next))
  })

  it('非 OpenAI OAuth 账号返回空 key', () => {
    expect(buildOpenAIUsageRefreshKey({
      id: 2,
      platform: 'anthropic',
      type: 'oauth',
      updated_at: '2026-03-07T10:00:00Z',
      last_used_at: '2026-03-07T10:00:00Z',
      extra: {}
    } as any)).toBe('')
  })
})

describe('buildGrokUsageRefreshKey', () => {
  it('changes when a canonical Grok billing or usage snapshot changes', () => {
    const base = {
      platform: 'grok',
      extra: {
        grok_billing_snapshot: { plan: 'Free', usage_percent: 0 },
        grok_usage_snapshot: { subscription_tier: 'Free', status_code: 200 }
      }
    } as any

    expect(buildGrokUsageRefreshKey(base)).not.toBe(buildGrokUsageRefreshKey({
      ...base,
      extra: {
        ...base.extra,
        grok_billing_snapshot: { plan: 'SuperGrok', usage_percent: 0 }
      }
    }))
    expect(buildGrokUsageRefreshKey(base)).not.toBe(buildGrokUsageRefreshKey({
      ...base,
      extra: {
        ...base.extra,
        grok_usage_snapshot: { subscription_tier: 'SuperGrok', status_code: 200 }
      }
    }))
  })

  it('ignores object key order and a legacy alias shadowed by canonical usage', () => {
    const first = {
      platform: 'grok',
      extra: {
        grok_billing_snapshot: {
          plan: 'SuperGrok',
          limits: { monthly: 100, weekly: 25 }
        },
        grok_usage_snapshot: { status_code: 200, subscription_tier: 'SuperGrok' },
        grok_quota_snapshot: { subscription_tier: 'Free' }
      }
    } as any
    const reordered = {
      platform: 'grok',
      extra: {
        grok_quota_snapshot: { subscription_tier: 'SuperGrok Heavy' },
        grok_usage_snapshot: { subscription_tier: 'SuperGrok', status_code: 200 },
        grok_billing_snapshot: {
          limits: { weekly: 25, monthly: 100 },
          plan: 'SuperGrok'
        }
      }
    } as any

    expect(buildGrokUsageRefreshKey(first)).toBe(buildGrokUsageRefreshKey(reordered))
  })

  it('uses the legacy quota alias only when the canonical snapshot is absent', () => {
    const base = {
      platform: 'grok',
      extra: { grok_quota_snapshot: { subscription_tier: 'Free' } }
    } as any
    const next = {
      platform: 'grok',
      extra: { grok_quota_snapshot: { subscription_tier: 'SuperGrok' } }
    } as any

    expect(buildGrokUsageRefreshKey(base)).not.toBe(buildGrokUsageRefreshKey(next))
  })

  it('tracks the legacy tier when the canonical snapshot has no usable tier', () => {
    for (const canonicalSnapshot of [
      { status_code: 200 },
      { status_code: 200, subscription_tier: '   ' },
    ]) {
      const base = {
        platform: 'grok',
        extra: {
          grok_usage_snapshot: canonicalSnapshot,
          grok_quota_snapshot: { subscription_tier: 'Free' },
        },
      } as any
      const next = {
        ...base,
        extra: {
          ...base.extra,
          grok_quota_snapshot: { subscription_tier: 'SuperGrok' },
        },
      }

      expect(buildGrokUsageRefreshKey(base)).not.toBe(buildGrokUsageRefreshKey(next))
    }
  })

  it('returns an empty key for non-Grok accounts', () => {
    expect(buildGrokUsageRefreshKey({
      platform: 'openai',
      extra: { grok_usage_snapshot: { subscription_tier: 'SuperGrok' } }
    } as any)).toBe('')
  })
})

describe('buildOpenAIUsageRefreshKey - cpr', () => {
  // 回归：cpr 与 oauth 共用 codex_5h_* / codex_7d_*，只是数据源不同。
  // 之前这里返回空串，导致 AccountUsageCell 的 watch 因 !prevKey 恒直接 return。
  it('cpr 账号不返回空串，且额度变化时 key 变化', () => {
    const base = {
      id: 4,
      platform: 'openai',
      type: 'cpr',
      updated_at: '2026-09-16T10:00:00Z',
      last_used_at: '2026-09-16T09:59:00Z',
      extra: {
        codex_usage_updated_at: '2026-09-16T10:00:00Z',
        codex_5h_used_percent: 0,
        codex_7d_used_percent: 14
      }
    } as any

    expect(buildOpenAIUsageRefreshKey(base)).not.toBe('')
    expect(buildOpenAIUsageRefreshKey(base)).not.toBe(
      buildOpenAIUsageRefreshKey({ ...base, extra: { ...base.extra, codex_7d_used_percent: 15 } })
    )
  })

  it('仍然只对 openai 平台生效', () => {
    expect(buildOpenAIUsageRefreshKey({ id: 5, platform: 'openai', type: 'apikey', extra: {} } as any)).toBe('')
    expect(buildOpenAIUsageRefreshKey({ id: 6, platform: 'grok', type: 'cpr', extra: {} } as any)).toBe('')
  })
})

describe('isOpenAICodexUsageAccount', () => {
  it('oauth 与 cpr 都算 codex 额度账号，其余不算', () => {
    expect(isOpenAICodexUsageAccount({ platform: 'openai', type: 'oauth' } as any)).toBe(true)
    expect(isOpenAICodexUsageAccount({ platform: 'openai', type: 'cpr' } as any)).toBe(true)
    expect(isOpenAICodexUsageAccount({ platform: 'openai', type: 'apikey' } as any)).toBe(false)
    expect(isOpenAICodexUsageAccount({ platform: 'openai', type: 'setup-token' } as any)).toBe(false)
    expect(isOpenAICodexUsageAccount({ platform: 'grok', type: 'oauth' } as any)).toBe(false)
  })
})
