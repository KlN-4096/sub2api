import { beforeEach, describe, expect, it, vi } from 'vitest'

const { post } = vi.hoisted(() => ({ post: vi.fn() }))
vi.mock('@/api/client', () => ({ apiClient: { post } }))
import { refreshCPRQuota, resetCPRQuota } from '@/api/admin/accounts'

describe('CPR quota management API', () => {
  beforeEach(() => post.mockReset())

  it('uses a dedicated CPR refresh endpoint with a sufficient management timeout', async () => {
    post.mockResolvedValueOnce({ data: { fetched_at: 1 } })
    await expect(refreshCPRQuota(56)).resolves.toEqual({ fetched_at: 1 })
    expect(post).toHaveBeenCalledWith('/admin/accounts/56/cpr/quota/refresh', undefined, { timeout: 90_000 })
  })

  it('forwards the caller UUID unchanged and never targets the OAuth reset route', async () => {
    const id = '8fbf302d-11df-4bd5-82e4-08e4b3df7874'
    post.mockResolvedValueOnce({ data: { code: 'reset' } })
    await expect(resetCPRQuota(56, id)).resolves.toEqual({ code: 'reset' })
    expect(post).toHaveBeenCalledWith('/admin/accounts/56/cpr/reset-quota', { redeem_request_id: id }, { timeout: 90_000 })
  })
})
