import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { ref, type ComputedRef } from 'vue'
import { useGatewayPoolProgress } from '@/composables/useGatewayPoolProgress'
import type { GatewayPoolProgress } from '@/api/admin/accounts'
import AccountStatusIndicator from '@/components/account/AccountStatusIndicator.vue'
import AccountGatewayCell from '@/components/account/AccountGatewayCell.vue'
import AccountCapacityCell from '@/components/account/AccountCapacityCell.vue'

import AccountsView from '../AccountsView.vue'

vi.mock('@/composables/useGatewayPoolProgress', () => ({ useGatewayPoolProgress: vi.fn() }))
const progress = ref<Record<number, GatewayPoolProgress>>({})
const paused = ref(false)
let pollIDs: ComputedRef<number[]>

const { listAccounts } = vi.hoisted(() => ({
  listAccounts: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      list: listAccounts,
      listWithEtag: vi.fn(),
      getBatchTodayStats: vi.fn().mockResolvedValue({ stats: {} }),
      getUpstreamBillingProbeSettings: vi.fn().mockResolvedValue({ enabled: true, interval_minutes: 30 }),
      delete: vi.fn(),
      batchClearError: vi.fn(),
      batchRefresh: vi.fn(),
      toggleSchedulable: vi.fn()
    },
    proxies: { getAll: vi.fn().mockResolvedValue([]) },
    groups: { getAll: vi.fn().mockResolvedValue([]) }
  }
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showError: vi.fn(), showSuccess: vi.fn(), showInfo: vi.fn() })
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => ({ token: 'test-token', isSimpleMode: false })
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({ t: (key: string) => key })
  }
})

const DataTableStub = {
  props: ['columns', 'data'],
  emits: ['sort'],
  template: `
    <div data-test="data-table">
      <span v-for="column in columns" :key="column.key" :data-column="column.key">
        {{ column.sortable ? 'sortable' : 'fixed' }}
      </span>
      <button data-test="sort-priority" @click="$emit('sort', 'priority', 'desc')" />
      <slot v-for="row in data" name="cell-status" :row="row" />
      <slot v-for="row in data" name="cell-gateway" :row="row" />
      <slot v-for="row in data" name="cell-capacity" :row="row" />
    </div>
  `
}

function mountView() {
  return mount(AccountsView, {
    global: {
      stubs: {
        AppLayout: { template: '<div><slot /></div>' },
        TablePageLayout: {
          template: '<div><slot name="filters" /><slot name="table" /><slot name="pagination" /></div>'
        },
        DataTable: DataTableStub,
        AccountTableActions: { template: '<div><slot name="after" /></div>' },
        AccountTableFilters: true,
        AccountBulkActionsBar: true,
        Pagination: true,
        ConfirmDialog: true,
        AccountActionMenu: true,
        ImportDataModal: true,
        ReAuthAccountModal: true,
        AccountTestModal: true,
        AccountStatsModal: true,
        ScheduledTestsPanel: true,
        SyncFromCrsModal: true,
        TempUnschedStatusModal: true,
        ErrorPassthroughRulesModal: true,
        TLSFingerprintProfilesModal: true,
        CreateAccountModal: true,
        EditAccountModal: true,
        BulkEditAccountModal: true,
        PlatformTypeBadge: true,
        AccountCapacityCell: true,
        AccountGatewayCell: true,
        AccountStatusIndicator: true,
        AccountTodayStatsCell: true,
        AccountGroupsCell: true,
        AccountUsageCell: true,
        HelpTooltip: true,
        Icon: true,
        Teleport: true
      }
    }
  })
}

describe('admin AccountsView priority column preferences', () => {
  beforeEach(() => {
    progress.value = {}
    paused.value = false
    vi.mocked(useGatewayPoolProgress).mockImplementation(ids => {
      pollIDs = ids
      return { progress, paused, refresh: vi.fn() }
    })
    localStorage.clear()
    listAccounts.mockReset().mockResolvedValue({
      items: [],
      total: 0,
      page: 1,
      page_size: 20,
      pages: 0
    })
  })

  it('polls rest for the status column without treating pause or read failure as a state change', async () => {
    localStorage.setItem('account-hidden-columns', JSON.stringify(['gateway', 'capacity']))
    localStorage.setItem('account-hidden-columns-version', 'scheduler-score-hidden-by-default')
    listAccounts.mockResolvedValueOnce({ items: [{
      id: 1, name: 'pool', platform: 'openai', type: 'oauth', status: 'active', schedulable: true,
      extra: { openai_gwpool: true }
    }], total: 1, page: 1, page_size: 20, pages: 1 })
    progress.value = { 1: { runtime: { rest: { active: false } } } as GatewayPoolProgress }
    const wrapper = mountView()
    await flushPromises()
    expect(pollIDs.value).toEqual([1])
    const status = wrapper.findComponent(AccountStatusIndicator)
    expect(status.props('gatewayPoolRest')).toEqual({ active: false })
    paused.value = true
    await flushPromises()
    expect(wrapper.findComponent(AccountGatewayCell).props()).toMatchObject({
      progressPaused: true
    })
    expect(wrapper.findComponent(AccountCapacityCell).props('progressPaused')).toBe(true)
    expect(status.props('progressPaused')).toBe(true)
    expect(status.props('gatewayPoolRest')).toEqual({ active: false })
    expect(status.props('gatewayPoolRestPending')).toBe(false)
    paused.value = false
    await flushPromises()
    expect(status.props('gatewayPoolRest')).toEqual({ active: false })
    expect(status.props('gatewayPoolRestPending')).toBe(false)
    wrapper.unmount()
  })

  it('does not freeze non-pool rows when gateway polling is paused or has no targets', async () => {
    paused.value = true
    listAccounts.mockResolvedValueOnce({ items: [{
      id: 2, name: 'ordinary', platform: 'openai', type: 'oauth', status: 'active', schedulable: true,
      extra: {}
    }], total: 1, page: 1, page_size: 20, pages: 1 })
    const wrapper = mountView()
    try {
      await flushPromises()
      expect(pollIDs.value).toEqual([])
      for (const component of [AccountGatewayCell, AccountCapacityCell, AccountStatusIndicator]) {
        expect(wrapper.findComponent(component).props('progressPaused')).toBe(false)
      }
    } finally { wrapper.unmount() }
  })

  it('shows priority as a sortable column for fresh preferences', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.get('[data-column="priority"]').text()).toBe('sortable')

    await wrapper.get('[data-test="sort-priority"]').trigger('click')
    await flushPromises()

    expect(listAccounts).toHaveBeenLastCalledWith(
      1,
      20,
      expect.objectContaining({ sort_by: 'priority', sort_order: 'desc' }),
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    )
  })

  it('preserves an existing preference that explicitly hides priority', async () => {
    localStorage.setItem('account-hidden-columns', JSON.stringify(['priority', 'today_stats']))
    localStorage.setItem('account-hidden-columns-version', 'scheduler-score-hidden-by-default')

    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.find('[data-column="priority"]').exists()).toBe(false)
    expect(JSON.parse(localStorage.getItem('account-hidden-columns') || '[]')).toEqual([
      'priority',
      'today_stats'
    ])
  })

  it('keeps priority visible while migrating older saved preferences', async () => {
    localStorage.setItem('account-hidden-columns', JSON.stringify(['today_stats']))

    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.get('[data-column="priority"]').text()).toBe('sortable')
    expect(JSON.parse(localStorage.getItem('account-hidden-columns') || '[]')).toEqual(
      expect.arrayContaining(['today_stats', 'scheduler_score'])
    )
    expect(JSON.parse(localStorage.getItem('account-hidden-columns') || '[]')).not.toContain('priority')
  })
})
