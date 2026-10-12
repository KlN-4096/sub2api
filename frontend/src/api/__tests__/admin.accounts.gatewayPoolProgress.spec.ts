import { beforeEach, describe, expect, it, vi } from 'vitest'

const { get } = vi.hoisted(() => ({ get: vi.fn() }))

vi.mock('@/api/client', () => ({ apiClient: { get } }))

import { getGatewayPoolProgress } from '@/api/admin/accounts'

describe('admin gateway pool progress API', () => {
  beforeEach(() => {
    get.mockReset()
    get.mockResolvedValue({ data: {} })
  })

  it('asks for the cooldown estimate unless the caller opts out', async () => {
    const signal = new AbortController().signal
    await getGatewayPoolProgress([1, 2])
    await getGatewayPoolProgress([3], signal, false)
    expect(get).toHaveBeenNthCalledWith(1, '/admin/accounts/gateway-pool-progress', {
      params: { ids: '1,2', estimate: 1 }, signal: undefined
    })
    expect(get).toHaveBeenNthCalledWith(2, '/admin/accounts/gateway-pool-progress', {
      params: { ids: '3' }, signal
    })
  })
})
