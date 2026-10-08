import { beforeEach, describe, expect, it, vi } from 'vitest'

const post = vi.hoisted(() => vi.fn())
vi.mock('@/api/client', () => ({ apiClient: { post } }))

import { importData, refreshOpenAIToken } from '@/api/admin/accounts'

describe('account import request budgets', () => {
  beforeEach(() => post.mockReset().mockResolvedValue({ data: {
    access_token: 'offline-at', proxy_created: 0, proxy_reused: 0, proxy_failed: 0, account_created: 0, account_failed: 0
  } }))

  it('keeps RT requests alive beyond the backend 120 second budget', async () => {
    await refreshOpenAIToken('offline-rt', 7, '/admin/openai/refresh-token', 'offline-client')
    expect(post).toHaveBeenCalledWith('/admin/openai/refresh-token', {
      refresh_token: 'offline-rt', proxy_id: 7, client_id: 'offline-client'
    }, { timeout: 150000 })
  })

  it('does not use the 30 second global timeout for bulk JSON imports', async () => {
    await importData({ data: { accounts: [], proxies: [], exported_at: '' }, skip_default_group_bind: true })
    expect(post.mock.calls[0]?.[2]).toEqual({ timeout: 150000 })
  })

  it('does not treat a proxy HTML response as successful import or valid credentials', async () => {
    post.mockResolvedValue({ data: '<html>offline proxy page</html>' })
    await expect(importData({ data: { accounts: [], proxies: [], exported_at: '' } })).rejects.toThrow('response is invalid')
    await expect(refreshOpenAIToken('offline-rt')).rejects.toThrow('response is invalid')
  })
})
