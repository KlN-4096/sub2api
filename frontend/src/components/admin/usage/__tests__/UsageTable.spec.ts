const ipGeoMocks = vi.hoisted(() => ({
  getEntry: vi.fn(() => ({ status: 'idle' as const })),
  fetchOne: vi.fn(),
  fetchBatch: vi.fn(),
}))

const appStoreMocks = vi.hoisted(() => ({
  showSuccess: vi.fn(),
  showError: vi.fn(),
}))

vi.mock('@/utils/ipGeoLookup', () => ipGeoMocks)
vi.mock('@/stores/app', () => ({ useAppStore: () => appStoreMocks }))

import { describe, expect, it, vi, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { turnStateFixture } from '@/components/account/__tests__/turnStateFixture'
import { nextTick } from 'vue'

import en from '@/i18n/locales/en/admin/resources'
import zh from '@/i18n/locales/zh/admin/resources'
import UsageTable from '../UsageTable.vue'

let locale: 'en' | 'zh' = 'en'
const localizedMessages: Record<'en' | 'zh', Record<string, string>> = {
  en: {
    'admin.usage.longContext': en.usage.longContext,
    'admin.usage.longContextPricingTooltip': en.usage.longContextPricingTooltip,
  },
  zh: {
    'admin.usage.longContext': zh.usage.longContext,
    'admin.usage.longContextPricingTooltip': zh.usage.longContextPricingTooltip,
  },
}

const messages: Record<string, string> = {
  'usage.outputTps': 'Output TPS',
  'admin.usage.turnStateBlocks': 'blocks:{n}',
  'admin.usage.turnStateSourceShort.manual': 'BADGE-MAN',
  'admin.usage.turnStateSourceShort.auto': 'BADGE-AUTO',
  'admin.usage.turnStateSourceShort.auto_stale': 'BADGE-AUTOSTALE',
  'admin.usage.turnStateSourceLong.manual': 'long.manual',
  'admin.usage.turnStateSourceLong.auto': 'long.auto',
  'admin.usage.turnStateSourceLong.auto_stale': 'long.auto_stale',
  'admin.usage.userDeletedBadge': 'Deleted',
  'admin.usage.turnStateOverriddenShort': 'OVR',
  'admin.usage.routePairOverriddenShort': 'POOL',
  'admin.usage.routePairOverridden': 'Pair came from the gateway pool',
  'admin.usage.routePairReroutedShort': 'REROUTED',
  'admin.usage.routePairRerouted': 'Pool promised {promised}, upstream landed {landed}',
  'admin.usage.routePairPoolVersion': 'Pool ticket id',
  'admin.usage.turnStateOverridden': 'Override active',
  'admin.usage.turnStateCopied': 'Turn-state copied',
  'usage.costDetails': 'Cost Breakdown',
  'admin.usage.inputCost': 'Input Cost',
  'admin.usage.outputCost': 'Output Cost',
  'admin.usage.cacheCreationCost': 'Cache Creation Cost',
  'admin.usage.cacheReadCost': 'Cache Read Cost',
  'usage.inputTokenPrice': 'Input price',
  'usage.outputTokenPrice': 'Output price',
  'usage.perMillionTokens': '/ 1M tokens',
  'usage.serviceTier': 'Service tier',
  'usage.serviceTierPriority': 'Fast',
  'usage.serviceTierUltrafast': 'Ultrafast',
  'usage.serviceTierFlex': 'Flex',
  'usage.serviceTierStandard': 'Standard',
  'usage.rate': 'Rate',
  'usage.accountMultiplier': 'Account rate',
  'usage.original': 'Original',
  'usage.userBilled': 'User billed',
  'usage.accountBilled': 'Account billed',
  'usage.imageUnit': ' images',
  'usage.imageCount': 'Image count',
  'usage.imageBillingSize': 'Billing size',
  'usage.imageInputSize': 'Input size',
  'usage.imageOutputSize': 'Output size',
  'usage.imageSizeSource': 'Size source',
  'usage.imageSizeBreakdown': 'Size breakdown',
  'usage.imageSizeSourceOutput': 'Upstream output',
  'usage.imageSizeSourceInput': 'Request input',
  'usage.imageSizeSourceDefault': 'Default billing tier',
  'usage.imageSizeSourceLegacy': 'Legacy record',
  'usage.imageSizeSourceMissing': 'Not recorded',
  'usage.imageSizeNotRecorded': 'not recorded',
  'usage.imageSizeLegacyUnstandardized': 'legacy unstandardized',
  'usage.imageSizeUnknown': 'unknown',
  'usage.imageUnitPrice': 'Per-image price',
  'usage.imageTotalPrice': 'Image total price',
  'usage.stream': 'Stream',
  'usage.sync': 'Sync',
  'usage.nativeCompactionV2': 'Compaction',
  'admin.usage.billingModeToken': 'Token',
  'admin.usage.billingModePerRequest': 'Per request',
  'admin.usage.billingModeImage': 'Image',
	'admin.usage.requestIdCopied': 'Request ID copied',
	'admin.usage.upstreamRequestIdCopied': 'Upstream ID copied',
	'keys.copied': 'Copied',
	'keys.copyToClipboard': 'Copy to clipboard',
	'common.copyFailed': 'Copy failed',
	'usage.requestedModel': 'Requested',
	'usage.sentUpstreamModel': 'Sent upstream',
	'usage.upstreamResponseModel': 'Upstream response',
	'usage.modelVariant': 'Possible version variant',
	'usage.modelMismatch': 'Different model',
}

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string, params?: Record<string, unknown>) => {
        const raw = localizedMessages[locale][key] ?? messages[key] ?? key
        if (!params) return raw
        return raw.replace(/\{(\w+)\}/g, (m, k) => (k in params ? String(params[k]) : m))
      },
    }),
  }
})

const DataTableStub = {
  props: ['data'],
  template: `
    <div>
      <div v-for="row in data" :key="row.request_id" :data-request-id="row.request_id">
        <slot name="cell-model" :row="row" :value="row.model" />
        <slot name="cell-turn_state_sent" :row="row" :value="row.turn_state_sent" />
        <slot name="cell-reasoning_effort" :row="row" :value="row.reasoning_effort" />
        <slot name="cell-billing_mode" :row="row" />
        <slot name="cell-tokens" :row="row" />
        <slot name="cell-latency" :row="row" />
        <slot name="cell-cost" :row="row" />
        <slot name="cell-request_id" :row="row" />
        <slot name="cell-upstream_request_id" :row="row" />
        <slot name="cell-turn_state" :row="row" />
        <slot name="cell-route_gateway" :row="row" />
      </div>
    </div>
  `,
}

const baseImageRow = {
  request_id: 'req-admin-image',
  model: 'gpt-image-2',
  actual_cost: 0.4,
  total_cost: 0.4,
  account_rate_multiplier: 1,
  rate_multiplier: 1,
  service_tier: null,
  input_cost: 0,
  output_cost: 0,
  cache_creation_cost: 0,
  cache_read_cost: 0,
  input_tokens: 0,
  output_tokens: 0,
  cache_creation_tokens: 0,
  cache_read_tokens: 0,
  cache_creation_5m_tokens: 0,
  cache_creation_1h_tokens: 0,
  cache_ttl_overridden: false,
  billing_mode: 'image',
  image_count: 2,
  image_size: '2K',
  image_input_size: null,
  image_output_size: null,
  image_size_source: null,
  image_size_breakdown: null,
}

describe('admin UsageTable tooltip', () => {
  it('shows sync TPS using total duration, with missing and image data unavailable', () => {
    const row = { ...baseImageRow, request_type: 'sync', stream: false, image_count: 0, billing_mode: 'token', output_tokens: 1000, duration_ms: 20_000, first_token_ms: 10_000 }
    const wrapper = mount(UsageTable, {
      props: {
        data: [
          row,
          { ...row, request_id: 'no-duration', duration_ms: null },
          { ...row, request_id: 'image', image_count: 1 },
          { ...row, request_id: 'image-tokens', image_output_tokens: 10 },
        ],
        loading: false,
        columns: [{ key: 'latency', label: 'Latency' }],
      },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })
    expect(wrapper.findAll('[data-testid="usage-tps"]').map(cell => cell.text())).toEqual(['50.00', '—', '—', '—'])
    expect(wrapper.text()).toContain('TPS')
    wrapper.unmount()
  })

  it('shows aligned TPS as the third latency row in both usage views', () => {
    for (const showAccountBilling of [true, false]) {
      const wrapper = mount(UsageTable, {
        props: {
          data: [{
            ...baseImageRow,
            billing_mode: 'token',
            image_count: 0,
            output_tokens: 100,
            request_type: 'stream',
            stream: true,
            duration_ms: 7000,
            first_token_ms: 2000,
          }],
          columns: [],
          showAccountBilling,
        },
        global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
      })
      const tps = wrapper.get('[data-testid="usage-tps"]')
      expect(tps.text()).toBe('20.00')
      expect(tps.attributes('title')).toBe('usage.tpsDescription')
      expect(tps.element.parentElement!.classList.contains('text-left')).toBe(true)
      const cells = Array.from(tps.element.parentElement!.children)
      expect(cells).toHaveLength(6)
      expect(cells[4].textContent).toBe('TPS')
      expect(cells[4].getAttribute('title')).toBe('usage.tpsDescription')
      for (const index of [1, 3, 5]) {
        expect(cells[index].classList.contains('text-right')).toBe(true)
        expect(cells[index].classList.contains('tabular-nums')).toBe(true)
      }
      wrapper.unmount()
    }
  })

  beforeEach(() => {
    locale = 'en'
    vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockReturnValue({
      x: 0,
      y: 0,
      top: 20,
      left: 20,
      right: 120,
      bottom: 40,
      width: 100,
      height: 20,
      toJSON: () => ({}),
    } as DOMRect)
  })

  it.each([
    ['en', 'Long context', 'Long-context pricing was applied. Input and output rates depend on the pricing tier, not a uniform multiplier.'],
    ['zh', '长上下文', '已应用长上下文计费。输入和输出费率取决于定价档位，并非统一倍率。'],
  ] as const)('marks only applied long-context billing with localized text in %s', (language, label, tooltip) => {
    locale = language
    const wrapper = mount(UsageTable, {
      props: {
        data: [
          {
            ...baseImageRow,
            request_id: 'req-long-context-enabled',
            long_context_billing_applied: true,
          },
          {
            ...baseImageRow,
            request_id: 'req-long-context-disabled',
            long_context_billing_applied: false,
          },
          {
            ...baseImageRow,
            request_id: 'req-long-context-absent',
          },
        ],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    expect(wrapper.findAll('[data-testid="long-context-billing-marker"]')).toHaveLength(1)
    const marker = wrapper.get('[data-request-id="req-long-context-enabled"] [data-testid="long-context-billing-marker"]')
    expect(marker.text()).toBe(label)
    expect(marker.attributes('title')).toBe(tooltip)
    expect(wrapper.find('[data-request-id="req-long-context-disabled"] [data-testid="long-context-billing-marker"]').exists()).toBe(false)
    expect(wrapper.find('[data-request-id="req-long-context-absent"] [data-testid="long-context-billing-marker"]').exists()).toBe(false)
    expect(wrapper.text()).not.toContain('x2')
  })

  it('keeps the request type badge and adds a separate badge only for native compaction rows', () => {
    const DataTableStreamStub = {
      props: ['data'],
      template: `
        <div>
          <div v-for="row in data" :key="row.request_id">
            <slot name="cell-stream" :row="row" />
          </div>
        </div>
      `,
    }
    const wrapper = mount(UsageTable, {
      props: {
        data: [
          {
            ...baseImageRow,
            request_id: 'req-compaction-stream',
            request_type: 'stream',
            stream: true,
            native_compaction_v2: true,
          },
          {
            ...baseImageRow,
            request_id: 'req-historical-sync',
            request_type: 'sync',
            stream: false,
            native_compaction_v2: false,
          },
        ],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStreamStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    const requestBadges = wrapper.findAll('[data-testid="request-type-badge"]')
    expect(requestBadges).toHaveLength(2)
    expect(requestBadges[0].text()).toBe('Stream')
    expect(requestBadges[1].text()).toBe('Sync')
    expect(wrapper.findAll('[data-testid="native-compaction-badge"]')).toHaveLength(1)
    expect(wrapper.get('[data-testid="native-compaction-badge"]').text()).toBe('Compaction')
  })

  it.each([
    [0, '0.0x'],
    [0.5, '0.50x'],
    [undefined, '1.00x'],
  ])('shows the stored user rate %s in cost details', async (rate, expected) => {
    const wrapper = mount(UsageTable, {
      props: { data: [{ ...baseImageRow, rate_multiplier: rate }], loading: false, columns: [] },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })
    const triggers = wrapper.findAll('.group.relative')
    await triggers[triggers.length - 1].trigger('mouseenter')
    const rateLabel = wrapper.get('.fixed').findAll('span').find(span => span.text() === 'Rate')!
    expect(rateLabel.element.parentElement?.textContent).toContain(expected)
    wrapper.unmount()
  })

  it('shows service tier and billing breakdown in cost tooltip', async () => {
    const row = {
      request_id: 'req-admin-1',
      actual_cost: 0.092883,
      total_cost: 0.092883,
      account_rate_multiplier: 1,
      rate_multiplier: 1,
      service_tier: 'priority',
      input_cost: 0.020285,
      output_cost: 0.00303,
      cache_creation_cost: 0,
      cache_read_cost: 0.069568,
      input_tokens: 4057,
      output_tokens: 101,
    }

    const wrapper = mount(UsageTable, {
      props: {
        data: [row],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    const tooltipTriggers = wrapper.findAll('.group.relative')
    await tooltipTriggers[tooltipTriggers.length - 1].trigger('mouseenter')
    await nextTick()

    const text = wrapper.text()
    expect(text).toContain('Service tier')
    expect(text).toContain('Fast')
    expect(text).toContain('Rate')
    expect(text).toContain('1.00x')
    expect(text).toContain('Account rate')
    expect(text).toContain('User billed')
    expect(text).toContain('Account billed')
    expect(text).toContain('$0.092883')
    expect(text).toContain('$5.0000 / 1M tokens')
    expect(text).toContain('$30.0000 / 1M tokens')
    expect(text).toContain('$0.069568')
  })

  it.each(['token', 'image', 'per_request'])('keeps eight decimal places in %s cost details', async (billingMode) => {
    const row = {
      ...baseImageRow,
      billing_mode: billingMode,
      image_count: billingMode === 'image' ? 2 : 0,
      input_cost: 0.00000001,
      image_input_cost: 0.00000002,
      output_cost: 0.00000003,
      image_output_cost: 0.00000004,
      cache_creation_cost: 0.00000005,
      cache_read_cost: 0.00000006,
      total_cost: 0.00000022,
      actual_cost: 0.00000042,
      account_stats_cost: 0.00000012,
      account_rate_multiplier: 1.5,
    }
    const wrapper = mount(UsageTable, {
      props: { data: [row], loading: false, columns: [] },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })
    const triggers = wrapper.findAll('.group.relative')
    await triggers[triggers.length - 1].trigger('mouseenter')
    const amounts = wrapper.get('.fixed').findAll('span').map(span => span.text())
    expect(amounts).toEqual(expect.arrayContaining([
      '$0.00000001', '$0.00000002', '$0.00000003', '$0.00000004',
      '$0.00000005', '$0.00000006', '$0.00000022', '$0.00000042', '$0.00000018',
    ]))
    if (billingMode === 'image') expect(amounts).toContain('$0.00000011')
    wrapper.unmount()
  })

  it('uses eight decimal places for missing cost values', async () => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [{ ...baseImageRow, billing_mode: 'per_request', image_count: 0, total_cost: undefined, actual_cost: undefined }],
        loading: false,
        columns: [],
      },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })
    const triggers = wrapper.findAll('.group.relative')
    await triggers[triggers.length - 1].trigger('mouseenter')
    const amounts = wrapper.get('.fixed').findAll('span').map(span => span.text()).filter(text => text.startsWith('$'))
    expect(amounts).toEqual(['$0.00000000', '$0.00000000', '$0.00000000', '$0.00000000'])
    wrapper.unmount()
  })

  it('shows requested and upstream models separately for admin rows', () => {
    const row = {
      request_id: 'req-admin-model-1',
      model: 'claude-sonnet-4',
      upstream_model: 'claude-sonnet-4-20250514',
      actual_cost: 0,
      total_cost: 0,
      account_rate_multiplier: 1,
      rate_multiplier: 1,
      input_cost: 0,
      output_cost: 0,
      cache_creation_cost: 0,
      cache_read_cost: 0,
      input_tokens: 0,
      output_tokens: 0,
    }

    const wrapper = mount(UsageTable, {
      props: {
        data: [row],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    const text = wrapper.text()
    expect(text).toContain('claude-sonnet-4')
    expect(text).toContain('claude-sonnet-4-20250514')
  })

  it('shows requested and forwarded reasoning effort separately when they differ', () => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [{
          request_id: 'req-admin-effort-1',
          model: 'gpt-5.4',
          reasoning_effort: 'max',
          upstream_reasoning_effort: 'xhigh',
        }],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    const text = wrapper.text()
    expect(text).toContain('Max')
    expect(text).toContain('XHigh')
    expect(text).toContain('↳')
  })

  it('shows a single reasoning effort when requested matches forwarded', () => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [{
          request_id: 'req-admin-effort-2',
          model: 'gpt-5.6-sol',
          reasoning_effort: 'max',
        }],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    const text = wrapper.text()
    expect(text).toContain('Max')
    expect(text).not.toContain('↳')
  })

  it('hides mapped reasoning effort for user rows that only have the requested value', () => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [{
          request_id: 'req-user-effort-1',
          model: 'gpt-5.4',
          reasoning_effort: 'max',
        }],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    expect(wrapper.text()).toContain('Max')
    expect(wrapper.text()).not.toContain('XHigh')
    expect(wrapper.text()).not.toContain('↳')
  })

	it.each([
		{
			name: 'possible version variant',
			responseModel: 'gpt-5.5-2026-08-01',
			expectedBadge: 'Possible version variant',
		},
		{
			name: 'different upstream model',
			responseModel: 'gpt-5.4',
			expectedBadge: 'Different model',
		},
	])('shows a compact upstream response audit marker for $name', ({ responseModel, expectedBadge }) => {
		const wrapper = mount(UsageTable, {
			props: {
				data: [{
					request_id: `req-${responseModel}`,
					model: 'gpt-5.6-sol',
					upstream_model: 'gpt-5.5',
					model_mapping_chain: 'gpt-5.6-sol→gpt-5.5',
					upstream_response_model: responseModel,
					upstream_model_mismatch: true,
				}],
				loading: false,
				columns: [],
			},
			global: {
				stubs: {
					DataTable: DataTableStub,
					EmptyState: true,
					Icon: true,
					Teleport: true,
				},
			},
		})

		const text = wrapper.text()
		expect(text).toContain('gpt-5.6-sol')
		expect(text).toContain('gpt-5.5')
		expect(text).toContain(responseModel)
		expect(text).toContain(expectedBadge)
	})

  it.each([
    {
      name: 'defaulted row',
      row: {
        ...baseImageRow,
        request_id: 'req-admin-default-image',
        image_size: '2K',
        image_input_size: 'auto',
        image_output_size: null,
        image_size_source: 'default',
      },
      expected: ['2K', 'Default billing tier', 'auto', 'unknown'],
    },
    {
      name: 'output-sourced row',
      row: {
        ...baseImageRow,
        request_id: 'req-admin-output-image',
        image_size: '4K',
        image_input_size: '1024x1024',
        image_output_size: '3840x2160',
        image_size_source: 'output',
        image_size_breakdown: { '4K': 1 },
      },
      expected: ['4K', 'Upstream output', '1024x1024', '3840x2160', '4K x 1'],
    },
    {
      name: 'input-sourced row',
      row: {
        ...baseImageRow,
        request_id: 'req-admin-input-image',
        image_size: '1K',
        image_input_size: '1024x1024',
        image_output_size: null,
        image_size_source: 'input',
      },
      expected: ['1K', 'Request input', '1024x1024', 'unknown'],
    },
    {
      name: 'legacy unstandardized row',
      row: {
        ...baseImageRow,
        request_id: 'req-admin-legacy-unstandardized-image',
        image_size: '512x512',
        image_input_size: null,
        image_output_size: null,
        image_size_source: null,
      },
      expected: ['legacy unstandardized: 512x512', 'Legacy record', 'unknown'],
    },
  ])('shows image usage metadata for $name', async ({ row, expected }) => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [row],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    await wrapper.find('.group.relative').trigger('mouseenter')
    await nextTick()

    const text = wrapper.text()
    expect(text).toContain('Image count')
    expect(text).toContain('Billing size')
    expect(text).toContain('Size source')
    expect(text).toContain('Input size')
    expect(text).toContain('Output size')
    expect(text).toContain('Per-image price')
    expect(text).toContain('Image total price')
    for (const value of expected) {
      expect(text).toContain(value)
    }
  })

  it('displays historical image rows with missing billing_mode as image usage without a 2K fallback', async () => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [
          {
            ...baseImageRow,
            request_id: 'req-admin-legacy-missing-image',
            billing_mode: null,
            image_size: null,
            image_input_size: null,
            image_output_size: null,
            image_size_source: null,
            image_size_breakdown: null,
          },
        ],
        loading: false,
        columns: [],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    await wrapper.find('.group.relative').trigger('mouseenter')
    await nextTick()

    const text = wrapper.text()
    expect(text).toContain('Image')
    expect(text).toContain('Image count')
    expect(text).toContain('Per-image price')
    expect(text).toContain('not recorded')
    expect(text).not.toContain('(2K)')
  })
})

describe('admin UsageTable request ID column', () => {
  beforeEach(() => {
    appStoreMocks.showSuccess.mockReset()
    appStoreMocks.showError.mockReset()
  })

  it('renders and copies the request ID', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', { clipboard: { writeText } })

    const wrapper = mount(UsageTable, {
      props: {
        data: [{ ...baseImageRow, request_id: 'req-admin-visible-id' }],
        loading: false,
        columns: [{ key: 'request_id', label: 'Request ID' }],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    expect(wrapper.text()).toContain('req-admin-visible-id')
    await wrapper.get('button[title="Copy to clipboard"]').trigger('click')

    expect(writeText).toHaveBeenCalledWith('req-admin-visible-id')
    expect(appStoreMocks.showSuccess).toHaveBeenCalledWith('Request ID copied')
  })

  it('renders and copies the upstream ID', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', { clipboard: { writeText } })

    const wrapper = mount(UsageTable, {
      props: {
        data: [{ ...baseImageRow, request_id: '', upstream_request_id: '20260903082826779695' }],
        loading: false,
        columns: [{ key: 'upstream_request_id', label: 'Upstream ID' }],
      },
      global: {
        stubs: {
          DataTable: DataTableStub,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    expect(wrapper.text()).toContain('20260903082826779695')
    const copyButtons = wrapper.findAll('button[title="Copy to clipboard"]')
    expect(copyButtons).toHaveLength(1)
    await copyButtons[0].trigger('click')

    expect(writeText).toHaveBeenCalledWith('20260903082826779695')
    expect(appStoreMocks.showSuccess).toHaveBeenCalledWith('Upstream ID copied')
  })

  // 判据是密文块数，不是字符长度。样本必须是真 Fernet 信封——假字符串首字节不是
  // 0x80，decodeTurnState 会返回 null，就只测到了「退回字符长度」的兜底分支。
  const mountTurnState = (row: Record<string, unknown>) =>
    mount(UsageTable, {
      props: {
        data: [{ ...baseImageRow, request_id: '', upstream_request_id: '', ...row }],
        loading: false,
        columns: [{ key: 'turn_state', label: 'Turn-state' }],
      },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })

  it.each([
    [10, 292, true],
    [11, 312, false],
  ])('badges a %i-block (%i-char) turn-state as green=%s', async (blocks, len, green) => {
    const blob = turnStateFixture(1789651097, blocks)
    expect(blob).toHaveLength(len)

    const wrapper = mountTurnState({ turn_state: blob })
    const badge = wrapper.findAll('span').find((n) => n.text() === String(len))
    expect(badge, '徽标必须显示字符长度（292/312），块数只作判据').toBeTruthy()
    expect(badge!.classes().some((c) => c.includes('green'))).toBe(green)
    // 非健康必须是红色告警，不能退回中性灰——徽标语义就是「一眼看出降智」。
    expect(badge!.classes().some((c) => c.includes('red'))).toBe(!green)
    // 徽标文本现在只是字符长度，真信封与 'g'.repeat(292) 产出完全一样的文本和颜色。
    // 这道断言是全套测试里唯一还依赖 decodeTurnState 返回值的闸：把它改成 return null
    // 整套就会退回长度兜底而照样绿——这个仓库以前正是这么踩过的。
    expect(badge!.attributes('title')).toContain('turnStateHint')
  })

  it('表外形态（780 / 33 块）标黄，不冒充降智的红', async () => {
    const blob = turnStateFixture(1789651097, 33)
    expect(blob).toHaveLength(780)

    const wrapper = mountTurnState({ turn_state: blob })
    const badge = wrapper.findAll('span').find((n) => n.text() === '780')
    expect(badge).toBeTruthy()
    expect(badge!.classes()).toContain('bg-yellow-100')
    expect(badge!.classes().some((c) => c.includes('red') || c.includes('green'))).toBe(false)
    expect(badge!.attributes('title')).toContain('turnStateUnknownShape')
  })

  it('解不出信封时退回字符长度（老判据）', async () => {
    const wrapper = mountTurnState({ turn_state: 'g'.repeat(292) })
    const badge = wrapper.findAll('span').find((n) => n.text() === '292')
    expect(badge).toBeTruthy()
    expect(badge!.classes().some((c) => c.includes('green'))).toBe(true)
    expect(badge!.attributes('title')).toContain('turnStateUndecodable')
  })

  it.each([
    ['manual', 'BADGE-MAN'],
    ['auto', 'BADGE-AUTO'],
    ['auto_stale', 'BADGE-AUTOSTALE'],
  ])('覆写来源 %s 渲染成本地化徽标而不是后端枚举', async (source, label) => {
    // 来源徽标贴在出站列：它说的是出站那张票从哪来。贴在铸出列时「注入 292 后上游仍铸
    // 312」会显示成「312 手填」，像是手填的就是 312。
    const sent = turnStateFixture(1789651097, 10)
    const minted = turnStateFixture(1789651098, 11)
    const wrapper = mount(UsageTable, {
      props: {
        data: [{
          ...baseImageRow,
          request_id: '',
          upstream_request_id: '',
          turn_state: minted,
          turn_state_sent: sent,
          turn_state_overridden: true,
          turn_state_source: source,
        }],
        loading: false,
        columns: [{ key: 'turn_state', label: 'Turn-state' }, { key: 'turn_state_sent', label: 'Sent' }],
      },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })
    expect(wrapper.text()).toContain(label)
    expect(wrapper.text()).not.toContain(source)
    const badge = wrapper.findAll('span').find((n) => n.text() === label)!
    expect(badge.element.parentElement?.textContent).toContain(sent)
    expect(badge.element.parentElement?.textContent).not.toContain(minted)
  })

  it('copies the turn-state with its own toast', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', { clipboard: { writeText } })
    const blob = 'gAAAAAB-turn-state'

    const wrapper = mount(UsageTable, {
      props: {
        data: [{ ...baseImageRow, request_id: '', upstream_request_id: '', turn_state: blob }],
        loading: false,
        columns: [{ key: 'turn_state', label: 'Turn-state' }],
      },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })

    const copyButtons = wrapper.findAll('button[title="Copy to clipboard"]')
    expect(copyButtons).toHaveLength(1)
    await copyButtons[0].trigger('click')

    expect(writeText).toHaveBeenCalledWith(blob)
    // 复用上游ID的文案会弹错提示，这里钉住专属文案
    expect(appStoreMocks.showSuccess).toHaveBeenCalledWith('Turn-state copied')
  })
})

describe('admin UsageTable IP geolocation batch toolbar', () => {
  const DataTableStubWithIp = {
    props: ['data'],
    template: `
      <div>
        <div v-for="row in data" :key="row.request_id">
          <slot name="cell-ip_address" :row="row" />
        </div>
      </div>
    `,
  }

  beforeEach(() => {
    ipGeoMocks.getEntry.mockReset()
    ipGeoMocks.fetchOne.mockReset()
    ipGeoMocks.fetchBatch.mockReset()
    ipGeoMocks.getEntry.mockReturnValue({ status: 'idle' })
  })

  it('does not render the batch toolbar when the ip_address column is not visible', () => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [{ request_id: 'r1', ip_address: '8.8.8.8' }],
        loading: false,
        columns: [],
      },
      global: { stubs: { DataTable: DataTableStubWithIp, EmptyState: true, Teleport: true } },
    })
    expect(wrapper.text()).not.toContain('usage.ipGeo.batchFetch')
  })

  it('renders the batch toolbar with a pending count when the ip_address column is visible', () => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [
          { request_id: 'r1', ip_address: '8.8.8.8' },
          { request_id: 'r2', ip_address: '8.8.8.8' },
          { request_id: 'r3', ip_address: '1.1.1.1' },
        ],
        loading: false,
        columns: [{ key: 'ip_address', label: 'IP' }],
      },
      global: { stubs: { DataTable: DataTableStubWithIp, EmptyState: true, Teleport: true } },
    })
    expect(wrapper.text()).toContain('usage.ipGeo.pending')
    const button = wrapper.find('button')
    expect(button.exists()).toBe(true)
    expect((button.element as HTMLButtonElement).disabled).toBe(false)
  })

  it('fetches deduplicated IPs from the current page when the batch button is clicked', async () => {
    ipGeoMocks.fetchBatch.mockResolvedValue(true)
    const wrapper = mount(UsageTable, {
      props: {
        data: [
          { request_id: 'r1', ip_address: '8.8.8.8' },
          { request_id: 'r2', ip_address: '8.8.8.8' },
          { request_id: 'r3', ip_address: '1.1.1.1' },
        ],
        loading: false,
        columns: [{ key: 'ip_address', label: 'IP' }],
      },
      global: { stubs: { DataTable: DataTableStubWithIp, EmptyState: true, Teleport: true } },
    })
    await wrapper.find('button').trigger('click')
    expect(ipGeoMocks.fetchBatch).toHaveBeenCalledWith(['8.8.8.8', '1.1.1.1'])
    expect(wrapper.emitted('ipGeoBatchFailed')).toBeUndefined()
  })

  it('emits ipGeoBatchFailed when the batch request reports a network-level failure', async () => {
    ipGeoMocks.fetchBatch.mockResolvedValue(false)
    const wrapper = mount(UsageTable, {
      props: {
        data: [{ request_id: 'r1', ip_address: '8.8.8.8' }],
        loading: false,
        columns: [{ key: 'ip_address', label: 'IP' }],
      },
      global: { stubs: { DataTable: DataTableStubWithIp, EmptyState: true, Teleport: true } },
    })
    await wrapper.find('button').trigger('click')
    expect(wrapper.emitted('ipGeoBatchFailed')).toHaveLength(1)
  })

  it('renders IpGeoCell content for ip_address cells', () => {
    ipGeoMocks.getEntry.mockReturnValue({ status: 'success', label: 'CN · Guangdong · Shenzhen', detail: {} })
    const wrapper = mount(UsageTable, {
      props: {
        data: [{ request_id: 'r1', ip_address: '121.35.47.43' }],
        loading: false,
        columns: [{ key: 'ip_address', label: 'IP' }],
      },
      global: { stubs: { DataTable: DataTableStubWithIp, EmptyState: true, Teleport: true } },
    })
    expect(wrapper.text()).toContain('121.35.47.43')
    expect(wrapper.text()).toContain('CN · Guangdong · Shenzhen')
  })
})

// A DataTable stub that also renders cell-user, so the deleted badge can be asserted.
const DataTableStubWithUser = {
  props: ['data'],
  template: `
    <div>
      <div v-for="row in data" :key="row.request_id">
        <slot name="cell-user" :row="row" />
        <slot name="cell-model" :row="row" :value="row.model" />
        <slot name="cell-reasoning_effort" :row="row" :value="row.reasoning_effort" />
        <slot name="cell-billing_mode" :row="row" />
        <slot name="cell-tokens" :row="row" />
        <slot name="cell-cost" :row="row" />
      </div>
    </div>
  `,
}

describe('admin UsageTable deleted-user badge', () => {
  it('renders deleted badge for a soft-deleted user row', () => {
    const row = {
      request_id: 'req-deleted-user-1',
      model: 'claude-3',
      user_id: 2,
      user: { id: 2, email: 'd@test.com', deleted_at: '2026-05-28T00:00:00Z' },
      actual_cost: 0,
      total_cost: 0,
      input_cost: 0,
      output_cost: 0,
      rate_multiplier: 1,
      input_tokens: 1,
      output_tokens: 1,
    }

    const wrapper = mount(UsageTable, {
      props: {
        data: [row],
        loading: false,
        columns: [{ key: 'user', label: 'User' }],
      },
      global: {
        stubs: {
          DataTable: DataTableStubWithUser,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    expect(wrapper.text()).toContain('Deleted')
    expect(wrapper.text()).toContain('d@test.com')
  })

  it('does NOT render deleted badge for an active user row', () => {
    const row = {
      request_id: 'req-active-user-1',
      model: 'claude-3',
      user_id: 3,
      user: { id: 3, email: 'active@test.com', deleted_at: null },
      actual_cost: 0,
      total_cost: 0,
      input_cost: 0,
      output_cost: 0,
      rate_multiplier: 1,
      input_tokens: 1,
      output_tokens: 1,
    }

    const wrapper = mount(UsageTable, {
      props: {
        data: [row],
        loading: false,
        columns: [{ key: 'user', label: 'User' }],
      },
      global: {
        stubs: {
          DataTable: DataTableStubWithUser,
          EmptyState: true,
          Icon: true,
          Teleport: true,
        },
      },
    })

    expect(wrapper.text()).not.toContain('Deleted')
    expect(wrapper.text()).toContain('active@test.com')
  })

  // safety_buffering_* 两列只落库不展示（用户 2026-09-25 定）：faster-model 是客户端「换更快模型重试」
  // 的备选，不是路由结果，显示出来会被读成错误路由。
  it('does not render the upstream safety-buffering headers under the model cell', () => {
    const wrapper = mount(UsageTable, {
      props: {
        data: [{
          request_id: 'req-safety-buffering',
          model: 'gpt-6-astra',
          safety_buffering_enabled: true,
          safety_buffering_faster_model: 'gpt-5.6-luna',
        }],
        loading: false,
        columns: [],
      },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })
    expect(wrapper.find('[data-testid="safety-buffering-marker"]').exists()).toBe(false)
    expect(wrapper.text()).not.toContain('gpt-5.6-luna')
    expect(wrapper.text()).not.toContain('enabled=')
  })
  // 网关池覆写徽标：挂在默认可见的模型列（route_gateway 列默认隐藏），只给网关名与状态，
  // cookie 本体仍只在 route_gateway 列的复制按钮里。
  const mountGwpoolRow = (row: Record<string, unknown>) =>
    mount(UsageTable, {
      props: {
        data: [{ request_id: 'req-gwpool', model: 'gpt-6-astra', ...row }],
        loading: false,
        columns: [{ key: 'model', label: 'Model' }],
      },
      global: { stubs: { DataTable: DataTableStub, EmptyState: true, Icon: true, Teleport: true } },
    })

  it('marks the row as applied when the pool gateway equals the landing gateway', () => {
    const wrapper = mountGwpoolRow({
      route_gateway: 'unified-142',
      route_pair: '__cflb=lb-pool; __oailb=jwt-pool',
      route_pair_overridden: true,
      route_pair_pool_gateway: 'unified-142',
      route_pair_pool_version: 'tkt-7',
    })
    const badge = wrapper.get('[data-testid="route-pair-overridden-marker"]')
    expect(badge.attributes('data-state')).toBe('applied')
    expect(badge.text()).toContain('POOL')
    expect(badge.text()).toContain('unified-142')
    expect(wrapper.text()).not.toContain('__oailb')
    // 票号只进 tooltip：它是和池子日志对账的键，不是读表时要扫的列。
    const title = badge.get('span').attributes('title') || ''
    expect(title).toContain('Pool ticket id: tkt-7')
    expect(badge.text()).not.toContain('tkt-7')
  })

  // 注入 142 落 84：上游下发了新的 __oailb，徽标必须说「被改派」而不是「已覆写」。
  it('marks the row as rerouted when the upstream landed it on another gateway', () => {
    const wrapper = mountGwpoolRow({
      route_gateway: 'unified-84',
      route_pair: '__cflb=lb-fresh; __oailb=jwt-fresh',
      route_pair_overridden: true,
      route_pair_pool_gateway: 'unified-142',
    })
    const badge = wrapper.get('[data-testid="route-pair-overridden-marker"]')
    expect(badge.attributes('data-state')).toBe('rerouted')
    expect(badge.text()).toContain('REROUTED')
    expect(badge.text()).toContain('unified-142 → unified-84')
    expect(badge.html()).toContain('rose')
  })

  it('leaves the override badge off when the pair came from the account cookie jar', () => {
    const wrapper = mountGwpoolRow({
      route_gateway: 'unified-126',
      route_pair: '__cflb=lb-jar; __oailb=jwt-jar',
      route_pair_overridden: false,
    })
    expect(wrapper.find('[data-testid="route-pair-overridden-marker"]').exists()).toBe(false)
  })
})
