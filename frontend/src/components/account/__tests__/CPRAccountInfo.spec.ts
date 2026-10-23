import { mount } from '@vue/test-utils'
import { describe, expect, it, vi } from 'vitest'
import CPRAccountInfo from '../CPRAccountInfo.vue'

vi.mock('vue-i18n', async () => ({
  ...await vi.importActual<typeof import('vue-i18n')>('vue-i18n'),
  useI18n: () => ({ t: (key: string) => key })
}))

describe('CPRAccountInfo', () => {
  it('does not display normal status, internal metadata or account details', () => {
    const wrapper = mount(CPRAccountInfo, {
      props: { snapshot: { account_id: 'acct', enabled: true, checked_at: '2026-10-21T08:00:00Z', email: 'one@example.invalid', status: 'normal', quota_updated_display: '26 天前' } }
    })
    expect(wrapper.text()).toBe('')
    expect(wrapper.find('details').exists()).toBe(false)
    expect(wrapper.find('button').exists()).toBe(false)
  })

  it('only shows a compact query-failure hint, with the cause in its tooltip', () => {
    const wrapper = mount(CPRAccountInfo, {
      props: { snapshot: { account_id: 'acct', enabled: true, status: 'error', error_reason: 'reauthorize', checked_at: '2026-10-20T08:00:00Z', query_error: 'admin_key_invalid', five_hour: { utilization: 42 }, access_token_expires_at: '2026-10-31T08:00:00Z' } }
    })
    expect(wrapper.get('[role=status]').text()).toBe('admin.accounts.cpr.queryFailed')
    expect(wrapper.get('[role=status]').attributes('title')).toContain('admin_key_invalid')
    expect(wrapper.text()).not.toContain('reauthorize')
    expect(wrapper.text()).not.toContain('accessExpires')
  })
})
