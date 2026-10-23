import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import CPRConnectionForm from '../CPRConnectionForm.vue'

const { discoverCPR } = vi.hoisted(() => ({ discoverCPR: vi.fn() }))
vi.mock('@/api/admin/accounts', () => ({ discoverCPR }))
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key }) }))

const account = { id: 'acct_1', name: 'CPR One', email: 'one@example.invalid', plan_type: 'pro', status: 'normal', enabled: true, concurrency_limit: 16 }
const props = { baseUrl: 'http://127.0.0.1:18081', adminKey: 'fake-admin', accountId: '' }

describe('CPRConnectionForm', () => {
  beforeEach(() => { discoverCPR.mockReset() })

  it('only discovers on connect, selects metadata, and exposes no client key input', async () => {
    discoverCPR.mockResolvedValue([account])
    const wrapper = mount(CPRConnectionForm, { props })
    expect(discoverCPR).not.toHaveBeenCalled()
    expect(wrapper.findAll('input[type=password]')).toHaveLength(1)
    await wrapper.get('button').trigger('click')
    await flushPromises()
    expect(discoverCPR).toHaveBeenCalledOnce()
    expect(wrapper.get('select').text()).toContain('CPR One')
    await wrapper.get('select').setValue('acct_1')
    expect(wrapper.emitted('update:accountId')?.at(-1)).toEqual(['acct_1'])
    expect(wrapper.emitted('selected')?.at(-1)).toEqual([account])
    wrapper.unmount()
  })

  it('invalidates selection and ignores stale responses when destination changes', async () => {
    let resolve!: (value: typeof account[]) => void
    discoverCPR.mockReturnValue(new Promise(r => { resolve = r }))
    const wrapper = mount(CPRConnectionForm, { props })
    await wrapper.get('button').trigger('click')
    await wrapper.setProps({ baseUrl: 'https://different.invalid' })
    resolve([account])
    await flushPromises()
    expect(wrapper.get('select').attributes('disabled')).toBeDefined()
    expect(wrapper.get('select').text()).not.toContain('CPR One')
    expect(wrapper.emitted('invalidated')).toBeTruthy()
    expect(wrapper.emitted('update:accountId')?.at(-1)).toEqual([''])
    wrapper.unmount()
  })

  it('allows server-side saved key reuse without receiving the secret', async () => {
    discoverCPR.mockResolvedValue([account])
    const wrapper = mount(CPRConnectionForm, { props: { ...props, adminKey: '', existingAccountId: 56, accountId: 'acct_1' } })
    await wrapper.get('button').trigger('click')
    await flushPromises()
    expect(discoverCPR).toHaveBeenCalledWith(expect.objectContaining({ admin_api_key: '', existing_account_id: 56 }))
    wrapper.unmount()
  })

  it('shows failures without reflecting response secrets', async () => {
    discoverCPR.mockRejectedValue(new Error('secret-reflection'))
    const wrapper = mount(CPRConnectionForm, { props })
    await wrapper.get('button').trigger('click')
    await flushPromises()
    expect(wrapper.get('[role=alert]').text()).toContain('connectFailed')
    expect(wrapper.text()).not.toContain('secret-reflection')
    wrapper.unmount()
  })
})
