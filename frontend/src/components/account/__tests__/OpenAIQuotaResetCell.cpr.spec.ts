import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import type { Account, CPRAccountSnapshot } from '@/types'
import ConfirmDialog from '@/components/common/ConfirmDialog.vue'
import OpenAIQuotaResetCell from '../OpenAIQuotaResetCell.vue'
import {
  refreshCPRQuota, resetCPRQuota, refreshOpenAIQuota, resetOpenAIQuota,
  type OpenAIQuotaRefreshResult
} from '@/api/admin/accounts'

vi.mock('@/api/admin/accounts', () => ({
  refreshCPRQuota: vi.fn(),
  resetCPRQuota: vi.fn(),
  refreshOpenAIQuota: vi.fn(),
  resetOpenAIQuota: vi.fn(),
  refreshOpenAIReferrals: vi.fn(),
  sendOpenAIReferralInvite: vi.fn()
}))
vi.mock('vue-i18n', async () => ({
  ...await vi.importActual<typeof import('vue-i18n')>('vue-i18n'),
  useI18n: () => ({ t: (key: string) => key })
}))

const requestID = '8fbf302d-11df-4bd5-82e4-08e4b3df7874'
const balance = { has_credits: true, unlimited: false, balance: '74080.1165380000' }
const cards = { available_count: 1, credits: [{ expires_at: '2099-10-30T03:01:00+08:00' }] }
const snapshot = (overrides: Partial<CPRAccountSnapshot> = {}): CPRAccountSnapshot => ({
  account_id: 'acct', enabled: true, checked_at: '2026-10-21T08:00:00Z',
  capabilities: { quota_refresh: true, reset_credits: true, consume_reset_credit: true },
  credits: balance, reset_credits: cards, ...overrides
})
const account = (overrides: Partial<Account> = {}): Account => ({
  id: 56, platform: 'openai', type: 'cpr', name: 'cpr',
  credentials: { base_url: 'http://127.0.0.1:18081', cpr_account_id: 'acct' },
  extra: { cpr_account_snapshot: snapshot() }, ...overrides
} as Account)

const mountCard = (value = account()) => mount(OpenAIQuotaResetCell, {
  props: { account: value },
  global: { stubs: { OpenAIReferralCell: { template: '<div data-test="referrals" />' } } }
})
const confirmReset = async (wrapper: ReturnType<typeof mount>) => {
  await wrapper.findAll('button')[1].trigger('click')
  wrapper.findComponent(ConfirmDialog).vm.$emit('confirm')
  await flushPromises()
}
const refreshResult = (): OpenAIQuotaRefreshResult => ({
  credits: balance, rate_limit_reset_credits: cards, fetched_at: 1792569600,
  cache_persisted: true, credits_cache_persisted: true,
  usage: { cpr: snapshot(), seven_day: { utilization: 2 } }
})

beforeEach(() => {
  vi.clearAllMocks()
  sessionStorage.clear()
  vi.spyOn(crypto, 'randomUUID').mockReturnValue(requestID)
})
afterEach(() => vi.restoreAllMocks())

describe('OpenAIQuotaResetCell — CPR', () => {
  it('rehydrates points and reset-card expiry but never renders referrals or auto-reset state', () => {
    const wrapper = mountCard(account({
      extra: {
        cpr_account_snapshot: snapshot(),
        auto_reset_credit_enabled: true,
        codex_auto_reset_credit_state: { status: 'success' }
      }
    }))
    expect(wrapper.get('[data-testid="codex-credits"]').text()).toContain(balance.balance)
    expect(wrapper.findAll('button')[0]!.text()).toContain('admin.accounts.openaiQuotaReset.count1')
    expect(wrapper.text()).toContain('admin.accounts.openaiQuotaReset.expiresAt')
    expect(wrapper.find('[data-test="referrals"]').exists()).toBe(false)
    expect(wrapper.find('[data-testid="auto-reset-credit-state"]').exists()).toBe(false)
    expect(refreshCPRQuota).not.toHaveBeenCalled()
    expect(refreshOpenAIQuota).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('queries the CPR endpoint and emits the live quota windows to the parent', async () => {
    vi.mocked(refreshCPRQuota).mockResolvedValue(refreshResult())
    const wrapper = mountCard()
    await wrapper.findAll('button')[0].trigger('click')
    await flushPromises()
    expect(refreshCPRQuota).toHaveBeenCalledWith(56)
    expect(refreshOpenAIQuota).not.toHaveBeenCalled()
    expect(wrapper.emitted('usage-updated')?.[0]?.[0]).toMatchObject({ seven_day: { utilization: 2 } })
    expect(wrapper.get('[data-testid="codex-credits"]').text()).toContain(balance.balance)
    wrapper.unmount()
  })

  it('updates a mounted card from a new usage snapshot without querying again', async () => {
    const wrapper = mountCard()
    await wrapper.setProps({ cprSnapshot: snapshot({ credits: { ...balance, balance: '90000.001' }, reset_credits: { available_count: 0, credits: [] } }) })
    expect(wrapper.get('[data-testid="codex-credits"]').text()).toContain('90000.001')
    expect(wrapper.findAll('button')[1].attributes('disabled')).toBeDefined()
    expect(refreshCPRQuota).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('hides unsupported reset controls and does not borrow OAuth snapshots', () => {
    const wrapper = mountCard(account({
      extra: {
        cpr_account_snapshot: snapshot({ capabilities: { quota_refresh: false, reset_credits: false, consume_reset_credit: false }, credits: undefined, reset_credits: undefined }),
        codex_reset_credit_snapshot: cards,
        codex_credits_snapshot: { credits: balance, fetched_at: 1792569600 }
      }
    }))
    expect(wrapper.findAll('button')).toHaveLength(1)
    expect(wrapper.text()).not.toContain('admin.accounts.openaiQuotaReset.count')
    expect(wrapper.get('[data-testid="codex-credits"]').text()).toContain('—')
    expect(wrapper.text()).not.toContain(balance.balance)
    wrapper.unmount()
  })

  it.each([undefined, { available_count: 0, credits: [] }])('retries an unknown operation after remount with inventory %s, never calling the OAuth reset', async (inventory) => {
    vi.mocked(resetCPRQuota).mockRejectedValueOnce(new Error('unknown result'))
    let wrapper = mountCard()
    await confirmReset(wrapper)
    expect(resetCPRQuota).toHaveBeenCalledWith(56, requestID)
    expect(wrapper.text()).toContain('admin.accounts.cpr.resetUnconfirmed')
    wrapper.unmount()
    vi.mocked(resetCPRQuota).mockResolvedValueOnce({
      code: 'already_redeemed', windows_reset: 0, cache_refreshed: true, account_state_recovered: false,
      quota: { ...refreshResult(), rate_limit_reset_credits: { available_count: 0, credits: [] } }
    })
    wrapper = mountCard(account({ extra: { cpr_account_snapshot: snapshot({ reset_credits: inventory }) } }))
    expect(wrapper.findAll('button')[1].attributes('disabled')).toBeUndefined()
    expect(wrapper.findAll('button')[1].text()).toContain('admin.accounts.cpr.retryReset')
    await confirmReset(wrapper)
    expect(resetCPRQuota).toHaveBeenNthCalledWith(2, 56, requestID)
    expect(crypto.randomUUID).toHaveBeenCalledTimes(1)
    expect(resetOpenAIQuota).not.toHaveBeenCalled()
    expect(wrapper.text()).toContain('admin.accounts.cpr.resetAlreadyApplied')
    expect(wrapper.findAll('button')[1].attributes('disabled')).toBeDefined()
    expect(sessionStorage.length).toBe(0)
    wrapper.unmount()
  })

  it('does not create a new operation if the pending UUID disappears before retry confirmation', async () => {
    vi.mocked(resetCPRQuota).mockRejectedValueOnce(new Error('unknown result'))
    let wrapper = mountCard()
    await confirmReset(wrapper)
    wrapper.unmount()
    wrapper = mountCard(account({ extra: { cpr_account_snapshot: snapshot({ reset_credits: undefined }) } }))
    sessionStorage.clear()
    await confirmReset(wrapper)
    expect(resetCPRQuota).toHaveBeenCalledTimes(1)
    expect(crypto.randomUUID).toHaveBeenCalledTimes(1)
    expect(wrapper.text()).toContain('admin.accounts.cpr.resetRetryUnavailable')
    wrapper.unmount()
  })

  it('shows a confirmed-reset readback failure without allowing a second cached consumption', async () => {
    vi.mocked(resetCPRQuota).mockResolvedValueOnce({
      code: 'reset', windows_reset: 0, cache_refreshed: false, account_state_recovered: false,
      warning_code: 'cpr_reset_refresh_failed'
    })
    const wrapper = mountCard()
    await confirmReset(wrapper)
    expect(wrapper.text()).toContain('admin.accounts.cpr.cpr_reset_refresh_failed')
    expect(wrapper.findAll('button')[1].attributes('disabled')).toBeDefined()
    expect(wrapper.text()).not.toContain('admin.accounts.cpr.resetSuccess')
    wrapper.unmount()
  })

  it.each([
    {
      before: { base_url: 'http://gateway.invalid', admin_base_url: ' http://admin.invalid/// ', cpr_account_id: 'acct' },
      after: { base_url: 'http://new-gateway.invalid', admin_base_url: 'http://admin.invalid', cpr_account_id: 'acct' }
    },
    {
      before: { base_url: 'http://admin.invalid/', cpr_account_id: 'acct' },
      after: { base_url: 'http://new-gateway.invalid', admin_base_url: 'http://admin.invalid', cpr_account_id: 'acct' }
    }
  ])('keeps the pending UUID when only the gateway entry changes: $before', async ({ before, after }) => {
    vi.mocked(resetCPRQuota).mockRejectedValue(new Error('unknown result'))
    let wrapper = mountCard(account({ credentials: before }))
    await confirmReset(wrapper)
    wrapper.unmount()
    wrapper = mountCard(account({ credentials: after }))
    await confirmReset(wrapper)
    expect(resetCPRQuota).toHaveBeenNthCalledWith(2, 56, requestID)
    expect(crypto.randomUUID).toHaveBeenCalledTimes(1)
    wrapper.unmount()
  })

  it('does not apply a late query response after the CPR binding changes on the same row', async () => {
    let resolve!: (result: OpenAIQuotaRefreshResult) => void
    vi.mocked(refreshCPRQuota).mockReturnValueOnce(new Promise((done) => { resolve = done }))
    const wrapper = mountCard()
    await wrapper.findAll('button')[0].trigger('click')
    await wrapper.setProps({ account: account({ credentials: { base_url: 'http://other.invalid', cpr_account_id: 'other-account' }, extra: {} }) })
    resolve(refreshResult())
    await flushPromises()
    expect(wrapper.emitted('usage-updated')).toBeUndefined()
    expect(wrapper.text()).not.toContain(balance.balance)
    wrapper.unmount()
  })

  it('does not send a reset when its retry identifier cannot be saved', async () => {
    vi.spyOn(Storage.prototype, 'setItem').mockImplementationOnce(() => { throw new Error('storage blocked') })
    const wrapper = mountCard()
    await confirmReset(wrapper)
    expect(resetCPRQuota).not.toHaveBeenCalled()
    expect(resetOpenAIQuota).not.toHaveBeenCalled()
    wrapper.unmount()
  })
})
