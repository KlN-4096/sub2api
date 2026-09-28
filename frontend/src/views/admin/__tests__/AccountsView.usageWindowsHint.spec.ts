import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'

import AccountsView from '../AccountsView.vue'

const {
  listAccounts,
  listWithEtag,
  getBatchTodayStats,
  getAllProxies,
  getAllGroups
} = vi.hoisted(() => ({
  listAccounts: vi.fn(),
  listWithEtag: vi.fn(),
  getBatchTodayStats: vi.fn(),
  getAllProxies: vi.fn(),
  getAllGroups: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      list: listAccounts,
      listWithEtag,
      getBatchTodayStats,
      getUpstreamBillingProbeSettings: vi.fn().mockResolvedValue({ enabled: true, interval_minutes: 30 }),
      delete: vi.fn(),
      batchClearError: vi.fn(),
      batchRefresh: vi.fn(),
      toggleSchedulable: vi.fn()
    },
    proxies: {
      getAll: getAllProxies
    },
    groups: {
      getAll: getAllGroups
    }
  }
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({
    showError: vi.fn(),
    showSuccess: vi.fn(),
    showInfo: vi.fn()
  })
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => ({
    token: 'test-token'
  })
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string) => key
    })
  }
})

// Render the per-column header slots so we can assert the usage-window header hint.
const DataTableStub = {
  props: ['columns', 'data'],
  template: `
    <div data-test="data-table">
      <template v-for="column in columns" :key="column.key">
        <div v-if="column.key === 'usage'" data-test="usage-header">
          <slot :name="'header-' + column.key" :column="column" />
        </div>
        <div v-if="column.key === 'upstream_billing_rate'" data-test="upstream-billing-header">
          <slot :name="'header-' + column.key" :column="column" />
        </div>
      </template>
      <div v-for="row in data" :key="row.id" data-test="account-rate">
        <slot name="cell-rate_multiplier" :row="row" />
      </div>
      <div v-for="row in data" :key="'proxy-' + row.id" data-test="account-proxy">
        <slot name="cell-proxy" :row="row" />
      </div>
    </div>
  `
}

// Expose the content passed to HelpTooltip without dealing with its <Teleport>.
const HelpTooltipStub = {
  props: ['content', 'widthClass'],
  template: '<span data-test="usage-windows-hint">{{ content }}</span>'
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
        HelpTooltip: HelpTooltipStub,
        Pagination: true,
        ConfirmDialog: true,
        AccountTableActions: { template: '<div><slot name="beforeCreate" /><slot name="after" /></div>' },
        AccountTableFilters: {
          props: ['groups'],
          template: '<div data-test="account-filters" :data-group-count="groups.length"></div>'
        },
        AccountBulkActionsBar: true,
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
        AccountStatusIndicator: true,
        AccountTodayStatsCell: true,
        AccountGroupsCell: true,
        AccountUsageCell: true,
        Icon: true
      }
    }
  })
}

describe('admin AccountsView usage windows hint', () => {
  beforeEach(() => {
    localStorage.clear()

    listAccounts.mockReset()
    listWithEtag.mockReset()
    getBatchTodayStats.mockReset()
    getAllProxies.mockReset()
    getAllGroups.mockReset()

    listAccounts.mockResolvedValue({
      items: [],
      total: 0,
      page: 1,
      page_size: 20,
      pages: 0
    })
    listWithEtag.mockResolvedValue({
      notModified: true,
      etag: null,
      data: null
    })
    getBatchTodayStats.mockResolvedValue({ stats: {} })
    getAllProxies.mockResolvedValue([])
    getAllGroups.mockResolvedValue([])
  })

  it('keeps groups available when loading proxies fails', async () => {
    getAllProxies.mockRejectedValue(new Error('proxy service unavailable'))
    getAllGroups.mockResolvedValue([{ id: 7, name: 'production' }])

    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.get('[data-test="account-filters"]').attributes('data-group-count')).toBe('1')
  })

  it('renders an explanatory tooltip next to the usage windows column header', async () => {
    const wrapper = mountView()
    await flushPromises()

    const header = wrapper.find('[data-test="usage-header"]')
    expect(header.exists()).toBe(true)
    // Column label is still shown alongside the help icon.
    expect(header.text()).toContain('admin.accounts.columns.usageWindows')

    const hint = wrapper.find('[data-test="usage-windows-hint"]')
    expect(hint.exists()).toBe(true)
    expect(hint.text()).toBe('admin.accounts.usageWindowsHint')
  })

  it('keeps Ollama Cloud in the single usage column and ignores legacy column preferences', async () => {
    localStorage.setItem('account-hidden-columns', JSON.stringify(['ollama_cloud_usage']))
    const wrapper = mountView()
    await flushPromises()

    const columns = wrapper.getComponent(DataTableStub).props('columns') as Array<{ key: string }>
    expect(columns.filter(column => column.key === 'usage')).toHaveLength(1)
    expect(columns.some(column => column.key === 'ollama_cloud_usage')).toBe(false)
  })

  it('renders the upstream billing trust warning next to the declared-rate column', async () => {
    const wrapper = mountView()
    await flushPromises()

    const header = wrapper.find('[data-test="upstream-billing-header"]')
    expect(header.exists()).toBe(true)
    expect(header.text()).toContain('admin.accounts.columns.upstreamBillingRate')
    expect(wrapper.findAll('[data-test="usage-windows-hint"]').some(node =>
      node.text() === 'admin.accounts.upstreamBilling.trustWarning'
    )).toBe(true)
    const columns = wrapper.getComponent(DataTableStub).props('columns') as Array<{ key: string; sortable: boolean }>
    expect(columns.find(column => column.key === 'upstream_billing_rate')?.sortable).toBe(true)
  })

  // cpr 账号真正的出口在 CPR 那一层；账号自己绑的 proxy 只作用于 sub2api → CPR 这一跳
  // (现网是 127.0.0.1)。照代理列判断"这个号从哪出去"会得到完全错误的答案，所以出口要
  // 跟在代理列里一起显示。凭据必须在渲染前剥掉 —— CPR 是独立仓库的上游服务,"它只返回
  // 脱敏值"是观察不是不变量。
  it('shows the CPR exit next to the account proxy, with credentials stripped', async () => {
    listAccounts.mockResolvedValueOnce({
      items: [{
        id: 11,
        name: 'cpr-account',
        platform: 'openai',
        type: 'cpr',
        status: 'active',
        schedulable: true,
        extra: { cpr_outbound_proxy: 'socks5h://user:p@ss@198.51.100.7:1080' },
        created_at: '2026-09-17T00:00:00Z',
        updated_at: '2026-09-17T00:00:00Z'
      }],
      total: 1,
      page: 1,
      page_size: 20,
      pages: 1
    })

    const wrapper = mountView()
    await flushPromises()

    const exit = wrapper.get('[data-testid="account-cpr-outbound"]')
    expect(exit.text()).toContain('socks5h://198.51.100.7:1080')
    expect(exit.text()).not.toContain('p@ss')
    expect(exit.text()).not.toContain('user')
  })

  it('omits the CPR exit row for accounts without one', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.find('[data-testid="account-cpr-outbound"]').exists()).toBe(false)
  })

  it('shows account multipliers with enough precision to match declared rates', async () => {
    listAccounts.mockResolvedValueOnce({
      items: [{
        id: 7,
        name: 'precision-account',
        platform: 'gemini',
        type: 'apikey',
        status: 'active',
        schedulable: true,
        rate_multiplier: 0.065,
        extra: {
          upstream_billing_probe_enabled: true,
          upstream_billing_rate_sync_enabled: true
        },
        created_at: '2026-07-13T00:00:00Z',
        updated_at: '2026-07-13T00:00:00Z'
      }],
      total: 1,
      page: 1,
      page_size: 20,
      pages: 1
    })

    const wrapper = mountView()
    await flushPromises()

    expect(wrapper.get('[data-test="account-rate"]').text()).toBe('0.065x')
    const indicator = wrapper.get('[data-testid="account-rate-sync-indicator"]')
    expect(indicator.attributes('title')).toBe('admin.accounts.upstreamBilling.syncedRateTooltip')
  })
})
