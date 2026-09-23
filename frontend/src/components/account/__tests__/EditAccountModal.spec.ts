import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { defineComponent } from 'vue'
import { flushPromises, mount } from '@vue/test-utils'

const { updateAccountMock, checkMixedChannelRiskMock, authIsSimpleMode } = vi.hoisted(() => ({
  updateAccountMock: vi.fn(),
  checkMixedChannelRiskMock: vi.fn(),
  authIsSimpleMode: { value: true }
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
    get isSimpleMode() {
      return authIsSimpleMode.value
    }
  })
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      update: updateAccountMock,
      checkMixedChannelRisk: checkMixedChannelRiskMock,
      // turn-state 覆写表的模型下拉走这个接口（AccountTestModal 同款）。
      getAvailableModels: vi.fn().mockResolvedValue([{ id: 'gpt-5.6-luna' }, { id: 'gpt-6-astra' }])
    },
    proxies: {
      // 292 猎手的代理多选走这个接口，懒加载。
      getAll: vi.fn().mockResolvedValue([
        { id: 20, name: 'webshare', protocol: 'socks5', host: 'p.webshare.io', port: 1080 },
        { id: 21, name: 'b2proxy', protocol: 'http', host: 'gw.b2proxy.example', port: 8000 }
      ])
    },
    settings: {
      getWebSearchEmulationConfig: vi.fn().mockResolvedValue({ enabled: false, providers: [] }),
      getSettings: vi.fn().mockResolvedValue({})
    },
    tlsFingerprintProfiles: {
      list: vi.fn().mockResolvedValue([])
    }
  }
}))

vi.mock('@/api/admin/accounts', () => ({
  getAntigravityDefaultModelMapping: vi.fn()
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

import EditAccountModal from '../EditAccountModal.vue'

const BaseDialogStub = defineComponent({
  name: 'BaseDialog',
  props: {
    show: {
      type: Boolean,
      default: false
    }
  },
  template: '<div v-if="show"><slot /><slot name="footer" /></div>'
})

const ModelWhitelistSelectorStub = defineComponent({
  name: 'ModelWhitelistSelector',
  props: {
    modelValue: {
      type: Array,
      default: () => []
    }
  },
  emits: ['update:modelValue'],
  template: `
    <div>
      <button
        type="button"
        data-testid="rewrite-to-snapshot"
        @click="$emit('update:modelValue', ['gpt-5.2-2025-12-11'])"
      >
        rewrite
      </button>
      <span data-testid="model-whitelist-value">
        {{ Array.isArray(modelValue) ? modelValue.join(',') : '' }}
      </span>
    </div>
  `
})

const SelectStub = defineComponent({
  name: 'SelectStub',
  props: {
    modelValue: {
      type: [String, Number, Boolean, null],
      default: ''
    },
    options: {
      type: Array,
      default: () => []
    }
  },
  emits: ['update:modelValue'],
  template: `
    <select
      v-bind="$attrs"
      :value="modelValue"
      @change="$emit('update:modelValue', $event.target.value)"
    >
      <option v-for="option in options" :key="option.value" :value="option.value">
        {{ option.label }}
      </option>
    </select>
  `
})

const GroupSelectorStub = defineComponent({
  name: 'GroupSelector',
  props: {
    modelValue: {
      type: Array,
      default: () => []
    }
  },
  emits: ['update:modelValue'],
  template: `
    <div data-testid="group-selector">
      <button
        type="button"
        data-testid="set-shadow-group"
        @click="$emit('update:modelValue', [7])"
      >
        group
      </button>
    </div>
  `
})

function buildAccount() {
  return {
    id: 1,
    name: 'OpenAI Key',
    notes: '',
    platform: 'openai',
    type: 'apikey',
    credentials: {
      api_key: 'sk-test',
      base_url: 'https://api.openai.com',
      model_mapping: {
        'gpt-5.2': 'gpt-5.2'
      }
    },
    extra: {},
    proxy_id: null,
    concurrency: 1,
    priority: 1,
    rate_multiplier: 1,
    status: 'active',
    group_ids: [],
    expires_at: null,
    auto_pause_on_expired: false
  } as any
}

function buildOpenAISparkShadowAccount() {
  const account = buildAccount()
  return {
    ...account,
    id: 4,
    name: 'OpenAI Spark Shadow',
    type: 'oauth',
    parent_account_id: 1,
    credentials: {
      access_token: 'parent-access-token',
      refresh_token: 'parent-refresh-token',
      api_key: 'sk-parent',
      base_url: 'https://api.openai.com',
      model_mapping: {
        'gpt-5.3-codex-spark': 'gpt-5.3-codex-spark'
      },
      compact_model_mapping: {
        'gpt-5.3-codex-spark': 'gpt-5.3-codex-spark-compact'
      }
    },
    group_ids: []
  } as any
}

function buildVertexAccount() {
  return {
    id: 2,
    name: 'Vertex SA',
    notes: '',
    platform: 'gemini',
    type: 'service_account',
    credentials: {
      service_account_json: '{"type":"service_account","client_email":"sa@example.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\\nMIIE\\n-----END PRIVATE KEY-----\\n"}',
      project_id: 'demo-project',
      client_email: 'sa@example.iam.gserviceaccount.com',
      location: 'us-central1',
      tier_id: 'vertex'
    },
    extra: {},
    proxy_id: null,
    concurrency: 1,
    priority: 1,
    rate_multiplier: 1,
    status: 'active',
    group_ids: [],
    expires_at: null,
    auto_pause_on_expired: false
  } as any
}

function buildAntigravityAccount(projectId = 'configured-project') {
  return {
    id: 3,
    name: 'Antigravity OAuth',
    notes: '',
    platform: 'antigravity',
    type: 'oauth',
    credentials: {
      antigravity_project_id: projectId,
      model_mapping: {
        'gemini-2.5-flash': 'gemini-2.5-flash'
      }
    },
    extra: {},
    proxy_id: null,
    concurrency: 1,
    priority: 1,
    rate_multiplier: 1,
    status: 'active',
    group_ids: [],
    expires_at: null,
    auto_pause_on_expired: false
  } as any
}

function buildGrokOAuthAccount() {
  return {
    id: 5,
    name: 'Grok OAuth',
    notes: '',
    platform: 'grok',
    type: 'oauth',
    credentials: {
      refresh_token: 'grok-rt',
      base_url: 'https://api.x.ai/v1',
      model_mapping: {
        'grok-latest': 'grok-4.3'
      }
    },
    extra: {},
    proxy_id: null,
    concurrency: 1,
    priority: 1,
    rate_multiplier: 1,
    status: 'active',
    group_ids: [],
    expires_at: null,
    auto_pause_on_expired: false
  } as any
}

function buildGrokAPIKeyAccount() {
  return {
    ...buildAccount(),
    id: 6,
    name: 'Grok API Key',
    platform: 'grok',
    credentials: {},
    credentials_status: { has_api_key: true },
    concurrency: 2
  } as any
}

function buildOpenAISetupTokenAccount() {
  return {
    ...buildAccount(),
    type: 'setup-token',
    extra: {
      openai_oauth_responses_websockets_v2_mode: 'ctx_pool',
      openai_oauth_responses_websockets_v2_enabled: true
    }
  } as any
}

function buildOpenAIOAuthParentAccount() {
  return {
    ...buildAccount(),
    id: 7,
    name: 'OpenAI OAuth Parent',
    type: 'oauth',
    parent_account_id: null,
    credentials: { access_token: 'oauth-token' },
    extra: {}
  } as any
}

function mountModal(account = buildAccount(), renderGroupSelector = false) {
  return mount(EditAccountModal, {
    props: {
      show: true,
      account,
      proxies: [],
      groups: []
    },
    global: {
      stubs: {
        BaseDialog: BaseDialogStub,
        Select: SelectStub,
        Icon: true,
        ProxySelector: true,
        GroupSelector: renderGroupSelector ? false : GroupSelectorStub,
        ModelWhitelistSelector: ModelWhitelistSelectorStub
      }
    }
  })
}

describe('EditAccountModal', () => {
  beforeEach(() => {
    authIsSimpleMode.value = true
  })

  afterEach(() => vi.useRealTimers())

  it('sets expiry presets from now instead of extending the saved expiry', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(new Date('2028-02-29T12:34:00'))
    const account = buildAccount()
    account.expires_at = new Date('2030-06-15T09:00:00').getTime() / 1000
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })
    const wrapper = mountModal(account)
    const input = wrapper.get<HTMLInputElement>('input[type="datetime-local"]')

    for (const [label, expected] of [
      ['payment.oneMonth', '2028-03-29T12:34'],
      ['payment.oneYear', '2029-02-28T12:34'],
    ]) {
      const button = wrapper.findAll('button').find((candidate) => candidate.text() === label)!
      expect(button.attributes('type')).toBe('button')
      await button.trigger('click')
      expect(input.element.value).toBe(expected)
      expect(updateAccountMock).not.toHaveBeenCalled()
    }

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.expires_at).toBe(new Date('2029-02-28T12:34:00').getTime() / 1000)
    wrapper.unmount()
  })

  it('can clear a selected expiry preset before saving the account', async () => {
    const account = buildAccount()
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })
    const wrapper = mountModal(account)
    const button = wrapper.findAll('button').find((candidate) => candidate.text() === 'payment.oneYear')!
    await button.trigger('click')
    const input = wrapper.get<HTMLInputElement>('input[type="datetime-local"]')
    expect(input.element.value).not.toBe('')
    await input.setValue('')

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.expires_at).toBe(0)
    wrapper.unmount()
  })

  it('allows removing assigned inactive groups and undoing the selection before saving', async () => {
    authIsSimpleMode.value = false
    const account = buildAccount()
    const activeGroup = {
      id: 1,
      name: 'Active group',
      platform: 'openai',
      status: 'active',
      subscription_type: 'standard',
      rate_multiplier: 1
    }
    const inactiveGroup = { ...activeGroup, id: 2, name: 'Paused group', status: 'inactive' }
    account.group_ids = [1, 2]
    account.groups = [
      { ...activeGroup, name: 'Outdated name' },
      inactiveGroup,
      inactiveGroup,
      { ...inactiveGroup, id: 3, name: 'Unassigned paused group' }
    ]
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account, true)
    await wrapper.setProps({ groups: [activeGroup] as any })
    const selector = wrapper.get('[data-tour="account-form-groups"]')
    expect(selector.findAll('input[type="checkbox"]').map(input => input.attributes('value')))
      .toEqual(['1', '2'])
    expect(selector.text()).toContain('Active group')
    expect(selector.text()).not.toContain('Outdated name')
    const pausedCheckbox = selector.get<HTMLInputElement>('input[value="2"]')
    expect(pausedCheckbox.element.checked).toBe(true)

    await pausedCheckbox.setValue(false)
    expect(selector.get<HTMLInputElement>('input[value="2"]').element.checked).toBe(false)
    await pausedCheckbox.setValue(true)
    expect(pausedCheckbox.element.checked).toBe(true)
    await pausedCheckbox.setValue(false)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.group_ids).toEqual([1])
    expect(account.group_ids).toEqual([1, 2])
  })

  it('reopening the same account rehydrates the OpenAI whitelist from props', async () => {
    const account = buildAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    expect(wrapper.get('[data-testid="model-whitelist-value"]').text()).toBe('gpt-5.2')

    await wrapper.get('[data-testid="rewrite-to-snapshot"]').trigger('click')
    expect(wrapper.get('[data-testid="model-whitelist-value"]').text()).toBe('gpt-5.2-2025-12-11')

    await wrapper.setProps({ show: false })
    await wrapper.setProps({ show: true })

    expect(wrapper.get('[data-testid="model-whitelist-value"]').text()).toBe('gpt-5.2')

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.model_mapping).toEqual({
      'gpt-5.2': 'gpt-5.2'
    })
  })

  it('preserves OpenCode Zen account type and endpoints on submit', async () => {
    const account = buildAccount()
    account.platform = 'opencode_go'
    account.credentials = {
      api_key: 'sk-opencode',
      account_mode: 'zen',
      api_protocol: 'adaptive',
      base_url: 'https://opencode.ai/zen/v1',
      api_base_urls: {
        chat_completions: 'https://opencode.ai/zen/v1',
        anthropic: 'https://opencode.ai/zen',
        responses: 'https://opencode.ai/zen/v1'
      },
      protocol_rules: [
        { pattern: 'grok-*', protocol: 'responses' },
        { pattern: 'gpt-*', protocol: 'responses' },
        { pattern: 'muse-spark-*', protocol: 'responses' },
        { pattern: 'claude-*', protocol: 'anthropic' },
        { pattern: 'qwen*', protocol: 'anthropic' }
      ]
    }
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).toMatchObject({
      account_mode: 'zen',
      api_protocol: 'adaptive',
      base_url: 'https://opencode.ai/zen/v1',
      api_base_urls: {
        chat_completions: 'https://opencode.ai/zen/v1',
        anthropic: 'https://opencode.ai/zen',
        responses: 'https://opencode.ai/zen/v1'
      },
      protocol_rules: [
        { pattern: 'grok-*', protocol: 'responses' },
        { pattern: 'gpt-*', protocol: 'responses' },
        { pattern: 'muse-spark-*', protocol: 'responses' },
        { pattern: 'claude-*', protocol: 'anthropic' },
        { pattern: 'qwen*', protocol: 'anthropic' }
      ]
    })
  })

  it('treats a legacy OpenCode account without account_mode as GO', async () => {
    const account = buildAccount()
    account.platform = 'opencode_go'
    account.credentials = {
      api_key: 'sk-opencode',
      api_protocol: 'adaptive',
      base_url: 'https://opencode.ai/zen/go/v1',
      api_base_urls: {
        chat_completions: 'https://opencode.ai/zen/go/v1',
        anthropic: 'https://opencode.ai/zen/go',
        responses: 'https://opencode.ai/zen/go/v1'
      }
    }
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).toMatchObject({
      account_mode: 'go',
      api_protocol: 'adaptive',
      base_url: 'https://opencode.ai/zen/go/v1'
    })
  })

  it('preserves adaptive Kimi Responses endpoint on submit', async () => {
    const account = buildAccount()
    account.platform = 'kimi'
    account.credentials = {
      api_key: 'sk-kimi',
      account_mode: 'payg',
      api_protocol: 'adaptive',
      base_url: 'https://api.moonshot.cn/v1',
      api_base_urls: {
        chat_completions: 'https://api.moonshot.cn/v1',
        anthropic: 'https://api.moonshot.cn/anthropic',
        responses: 'https://api.moonshot.cn/v1'
      }
    }
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).toMatchObject({
      account_mode: 'payg',
      api_protocol: 'adaptive',
      base_url: 'https://api.moonshot.cn/v1',
      api_base_urls: {
        chat_completions: 'https://api.moonshot.cn/v1',
        anthropic: 'https://api.moonshot.cn/anthropic',
        responses: 'https://api.moonshot.cn/v1'
      }
    })
  })

  it('preserves adaptive GLM endpoints on submit', async () => {
    const account = buildAccount()
    account.platform = 'zhipu'
    account.credentials = {
      api_key: 'sk-glm',
      account_mode: 'coding',
      api_protocol: 'adaptive',
      base_url: 'https://open.bigmodel.cn/api/coding/paas/v4',
      api_base_urls: {
        chat_completions: 'https://open.bigmodel.cn/api/coding/paas/v4',
        anthropic: 'https://open.bigmodel.cn/api/anthropic'
      }
    }
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).toMatchObject({
      account_mode: 'coding',
      api_protocol: 'adaptive',
      base_url: 'https://open.bigmodel.cn/api/coding/paas/v4',
      api_base_urls: {
        chat_completions: 'https://open.bigmodel.cn/api/coding/paas/v4',
        anthropic: 'https://open.bigmodel.cn/api/anthropic'
      }
    })
  })

  it.each([
    ['explicit Chat Completions', 'chat_completions'],
    ['legacy missing protocol', undefined]
  ])('preserves a custom CN relay for %s accounts', async (_name, storedProtocol) => {
    const account = buildAccount()
    account.platform = 'zhipu'
    account.credentials = {
      api_key: 'sk-glm',
      account_mode: 'payg',
      base_url: 'https://relay.example.com/v1'
    }
    if (storedProtocol) {
      account.credentials.api_protocol = storedProtocol
    }
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    const submittedCredentials = updateAccountMock.mock.calls[0]?.[1]?.credentials
    expect(submittedCredentials).toMatchObject({
      account_mode: 'payg',
      api_protocol: 'chat_completions',
      base_url: 'https://relay.example.com/v1'
    })
    expect(submittedCredentials).not.toHaveProperty('api_base_urls')
  })

  it('uses the legacy base_url when adaptive endpoints are missing', async () => {
    const account = buildAccount()
    account.platform = 'zhipu'
    account.credentials = {
      api_key: 'sk-glm',
      account_mode: 'payg',
      api_protocol: 'adaptive',
      base_url: 'https://relay.example.com/v1',
      api_base_urls: {
        chat_completions: '   '
      }
    }
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).toMatchObject({
      api_protocol: 'adaptive',
      base_url: 'https://relay.example.com/v1',
      api_base_urls: {
        chat_completions: 'https://relay.example.com/v1',
        anthropic: 'https://open.bigmodel.cn/api/anthropic'
      }
    })
  })

  it('carries a fixed Chat relay into Adaptive when the user switches protocols', async () => {
    const account = buildAccount()
    account.platform = 'zhipu'
    account.credentials = {
      api_key: 'sk-glm',
      account_mode: 'payg',
      api_protocol: 'chat_completions',
      base_url: 'https://relay.example.com/v1'
    }
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)
    const adaptiveButton = wrapper
      .findAll('button')
      .find(button => button.text().includes('admin.accounts.cnProviders.apiProtocol.adaptive'))
    expect(adaptiveButton).toBeDefined()
    await adaptiveButton!.trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).toMatchObject({
      api_protocol: 'adaptive',
      base_url: 'https://relay.example.com/v1',
      api_base_urls: {
        chat_completions: 'https://relay.example.com/v1'
      }
    })
  })

  it.each([
    {
      name: 'Anthropic',
      platform: 'zhipu',
      protocol: 'anthropic',
      baseUrl: 'https://relay.example.com/anthropic',
      expectedBaseUrl: 'https://open.bigmodel.cn/api/paas/v4',
      expectedProtocolUrls: {
        chat_completions: 'https://open.bigmodel.cn/api/paas/v4',
        anthropic: 'https://relay.example.com/anthropic'
      }
    },
    {
      name: 'Responses',
      platform: 'deepseek',
      protocol: 'responses',
      baseUrl: 'https://relay.example.com/responses',
      expectedBaseUrl: 'https://api.deepseek.com',
      expectedProtocolUrls: {
        chat_completions: 'https://api.deepseek.com',
        anthropic: 'https://api.deepseek.com/anthropic',
        responses: 'https://relay.example.com/responses'
      }
    }
  ])('keeps a fixed $name relay in its protocol slot when switching to Adaptive', async (testCase) => {
    const account = buildAccount()
    account.platform = testCase.platform
    account.credentials = {
      api_key: 'sk-cn',
      account_mode: 'payg',
      api_protocol: testCase.protocol,
      base_url: testCase.baseUrl
    }
    updateAccountMock.mockReset().mockResolvedValue(account)
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)
    const adaptiveButton = wrapper
      .findAll('button')
      .find(button => button.text().includes('admin.accounts.cnProviders.apiProtocol.adaptive'))
    expect(adaptiveButton).toBeDefined()
    await adaptiveButton!.trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).toMatchObject({
      api_protocol: 'adaptive',
      base_url: testCase.expectedBaseUrl,
      api_base_urls: testCase.expectedProtocolUrls
    })
  })

  it('preserves model mappings when editing the whitelist', async () => {
    const account = buildAccount()
    account.credentials.model_mapping = {
      'gpt-5.2': 'gpt-5.2',
      'gpt-latest': 'gpt-5.2'
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    expect(wrapper.get('[data-testid="model-whitelist-value"]').text()).toBe('gpt-5.2')

    await wrapper.get('[data-testid="rewrite-to-snapshot"]').trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.model_mapping).toEqual({
      'gpt-5.2-2025-12-11': 'gpt-5.2-2025-12-11',
      'gpt-latest': 'gpt-5.2'
    })
  })

  it('submits OpenAI compact mode and compact-only model mapping', async () => {
    const account = buildAccount()
    account.extra = {
      openai_compact_mode: 'force_on'
    }
    account.credentials = {
      ...account.credentials,
      compact_model_mapping: {
        'gpt-5.4': 'gpt-5.4-openai-compact'
      }
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_compact_mode).toBe('force_on')
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.compact_model_mapping).toEqual({
      'gpt-5.4': 'gpt-5.4-openai-compact'
    })
  })

  it('loads and submits the per-account OpenAI long-context billing toggle', async () => {
    const account = buildAccount()
    account.extra = {
      openai_long_context_billing_enabled: true
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const toggle = wrapper.get('[data-testid="openai-long-context-billing-toggle"]')
    expect(toggle.attributes('aria-checked')).toBe('true')

    await toggle.trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_long_context_billing_enabled).toBe(false)
  })

  it('loads and clears the OAuth-only Codex namespace flatten toggle', async () => {
    const account = buildAccount()
    account.type = 'oauth'
    account.extra = {
      openai_responses_flatten_namespaces: true
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const toggle = wrapper.get('[data-testid="edit-openai-flatten-namespaces-toggle"]')

    // 关闭后应从 extra 中删除该键，而不是写入 false
    await toggle.trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty(
      'openai_responses_flatten_namespaces'
    )
  })

  it('submits the Codex namespace flatten toggle when switched on', async () => {
    const account = buildAccount()
    account.type = 'oauth'
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    await wrapper.get('[data-testid="edit-openai-flatten-namespaces-toggle"]').trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_responses_flatten_namespaces).toBe(
      true
    )
  })

  it('toggles the API-key-only raw relay switch in extra', async () => {
    const account = buildAccount()
    account.type = 'apikey'
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    await wrapper.get('[data-testid="edit-openai-raw-relay-toggle"]').trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_raw_relay).toBe(true)

    account.extra = { openai_raw_relay: true }
    updateAccountMock.mockReset()
    updateAccountMock.mockResolvedValue(account)
    const enabled = mountModal(account)
    // 关闭后删键，不写 false
    await enabled.get('[data-testid="edit-openai-raw-relay-toggle"]').trigger('click')
    await enabled.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('openai_raw_relay')

    const oauth = buildAccount()
    oauth.type = 'oauth'
    expect(mountModal(oauth).find('[data-testid="edit-openai-raw-relay-toggle"]').exists()).toBe(false)
  })

  it('writes the upstream request id header into extra only when it changes', async () => {
    const account = buildAccount()
    account.extra = { openai_compact_mode: 'force_on' }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const untouched = mountModal(account)
    await untouched.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.upstream_request_id_header).toBeUndefined()

    updateAccountMock.mockClear()
    const wrapper = mountModal(account)
    await wrapper.get('[data-testid="upstream-request-id-header"]').setValue(' X-Oneapi-Request-Id ')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).toMatchObject({
      openai_compact_mode: 'force_on',
      upstream_request_id_header: 'X-Oneapi-Request-Id'
    })
  })

  it('removes the upstream request id header from extra when cleared', async () => {
    const account = buildAccount()
    account.extra = { upstream_request_id_header: 'X-Request-ID' }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    expect((wrapper.get('[data-testid="upstream-request-id-header"]').element as HTMLInputElement).value).toBe('X-Request-ID')
    await wrapper.get('[data-testid="upstream-request-id-header"]').setValue('')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).toBeDefined()
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('upstream_request_id_header')
  })

  it('writes images_url_to_b64_json into extra when toggled on', async () => {
    const account = buildAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const toggle = wrapper.get('[data-testid="openai-images-url-to-b64-json-toggle"]')
    expect(toggle.attributes('aria-checked')).toBe('false')
    await toggle.trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.images_url_to_b64_json).toBe(true)
  })

  it('removes images_url_to_b64_json from extra when toggled off', async () => {
    const account = buildAccount()
    account.extra = { images_url_to_b64_json: true }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const toggle = wrapper.get('[data-testid="openai-images-url-to-b64-json-toggle"]')
    expect(toggle.attributes('aria-checked')).toBe('true')
    await toggle.trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).toBeDefined()
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('images_url_to_b64_json')
  })

  it('hides the Codex namespace flatten toggle for non-OAuth OpenAI accounts', async () => {
    const account = buildAccount()
    const wrapper = mountModal(account)

    expect(wrapper.find('[data-testid="edit-openai-flatten-namespaces-toggle"]').exists()).toBe(
      false
    )
  })

  it('defaults legacy OpenAI accounts to long-context billing disabled', async () => {
    const account = buildAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const toggle = wrapper.get('[data-testid="openai-long-context-billing-toggle"]')
    expect(toggle.attributes('aria-checked')).toBe('false')

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_long_context_billing_enabled).toBe(false)
  })

  it('does not render or submit the long-context billing toggle for Spark shadow accounts', async () => {
    const account = buildOpenAISparkShadowAccount()
    account.extra = {
      openai_long_context_billing_enabled: false
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)
    const wrapper = mountModal(account)

    expect(wrapper.find('[data-testid="openai-long-context-billing-toggle"]').exists()).toBe(false)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty(
      'openai_long_context_billing_enabled'
    )
  })

  it('preserves an explicit OpenAI long-context billing opt-out', async () => {
    const account = buildAccount()
    account.extra = {
      openai_long_context_billing_enabled: false
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const toggle = wrapper.get('[data-testid="openai-long-context-billing-toggle"]')
    expect(toggle.attributes('aria-checked')).toBe('false')

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_long_context_billing_enabled).toBe(false)
  })

  it('fails closed for malformed OpenAI long-context billing values', async () => {
    const account = buildAccount()
    account.extra = {
      openai_long_context_billing_enabled: 'false'
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    expect(wrapper.get('[data-testid="openai-long-context-billing-toggle"]').attributes('aria-checked')).toBe('false')

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_long_context_billing_enabled).toBe(false)
  })

  it('loads and submits Grok OAuth model mapping edits', async () => {
    const account = buildGrokOAuthAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    expect(wrapper.text()).toContain('Imagine Image')
    expect(wrapper.text()).toContain('Imagine Video')

    const inputWithValue = (value: string) => {
      const input = wrapper
        .findAll('input')
        .find((input) => (input.element as HTMLInputElement).value === value)
      expect(input).toBeTruthy()
      return input!
    }

    await inputWithValue('grok-latest').setValue('grok')
    await inputWithValue('grok-4.3').setValue('grok-build-0.1')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.model_mapping).toEqual({
      grok: 'grok-build-0.1'
    })
  })

  it('uses the official xAI base URL when a Grok API-key account omits base_url', async () => {
    const account = buildGrokAPIKeyAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    expect((wrapper.get('input[placeholder="https://api.x.ai/v1"]').element as HTMLInputElement).value)
      .toBe('https://api.x.ai/v1')

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.base_url).toBe('https://api.x.ai/v1')
  })

  it('only submits model mapping credentials when saving an OpenAI spark shadow account', async () => {
    authIsSimpleMode.value = false
    const account = buildOpenAISparkShadowAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('[data-testid="set-shadow-group"]').trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    const payload = updateAccountMock.mock.calls[0]?.[1]
    expect(payload?.group_ids).toEqual([7])
    expect(payload?.credentials).toEqual({
      model_mapping: {
        'gpt-5.3-codex-spark': 'gpt-5.3-codex-spark'
      },
      compact_model_mapping: {
        'gpt-5.3-codex-spark': 'gpt-5.3-codex-spark-compact'
      }
    })
  })

  it('submits OpenAI APIKey Responses support override mode', async () => {
    const account = buildAccount()
    account.extra = {
      openai_responses_mode: 'force_chat_completions',
      openai_responses_supported: false
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('[data-testid="openai-responses-mode-select"]').setValue('force_responses')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_responses_mode).toBe('force_responses')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_responses_supported).toBe(false)
  })

  it('submits the account upstream billing auto-probe setting', async () => {
    const account = buildAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const toggle = wrapper.get('[data-testid="upstream-billing-auto-probe"]')
    expect(toggle.attributes('aria-checked')).toBe('false')

    await toggle.trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.upstream_billing_probe_enabled).toBe(true)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty(
      'upstream_billing_probe_enabled'
    )
  })

  it('exposes the upstream billing auto-probe toggle for non-OpenAI API-key accounts', async () => {
    // 探测已放宽到全部 API-key 平台：grok 账号同样能开启并保存。
    const account = buildAccount()
    account.platform = 'grok'
    account.name = 'grok-relay'
    account.credentials = { api_key: 'sk-grok', base_url: 'https://relay.example/v1' }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const toggle = wrapper.get('[data-testid="upstream-billing-auto-probe"]')
    expect(toggle.attributes('aria-checked')).toBe('false')

    await toggle.trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.upstream_billing_probe_enabled).toBe(true)
  })

  it('enabling rate sync also enables probing and stops submitting a manual rate', async () => {
    const account = buildAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const syncToggle = wrapper.get('[data-testid="upstream-billing-rate-sync"]')
    const probeToggle = wrapper.get('[data-testid="upstream-billing-auto-probe"]')
    const rateInput = wrapper.get<HTMLInputElement>('[data-testid="account-rate-multiplier"]')
    expect(syncToggle.attributes('aria-checked')).toBe('false')
    expect(probeToggle.attributes('aria-checked')).toBe('false')
    expect(rateInput.element.disabled).toBe(false)
    expect(wrapper.text()).toContain('admin.accounts.billingRateMultiplierHint')
    expect(wrapper.text()).not.toContain('admin.accounts.upstreamBilling.syncRateManagedHint')

    await syncToggle.trigger('click')
    expect(syncToggle.attributes('aria-checked')).toBe('true')
    expect(probeToggle.attributes('aria-checked')).toBe('true')
    expect(rateInput.element.disabled).toBe(true)
    expect(wrapper.text()).toContain('admin.accounts.upstreamBilling.syncRateManagedHint')
    expect(wrapper.text()).not.toContain('admin.accounts.billingRateMultiplierHint')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    const payload = updateAccountMock.mock.calls[0]?.[1]
    expect(payload?.upstream_billing_probe_enabled).toBe(true)
    expect(payload?.upstream_billing_rate_sync_enabled).toBe(true)
    expect(payload).not.toHaveProperty('rate_multiplier')
  })

  it('disabling probing also disables rate sync and restores manual rate editing', async () => {
    const account = buildAccount()
    account.extra = {
      upstream_billing_probe_enabled: true,
      upstream_billing_rate_sync_enabled: true
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const syncToggle = wrapper.get('[data-testid="upstream-billing-rate-sync"]')
    const probeToggle = wrapper.get('[data-testid="upstream-billing-auto-probe"]')
    const rateInput = wrapper.get<HTMLInputElement>('[data-testid="account-rate-multiplier"]')
    expect(syncToggle.attributes('aria-checked')).toBe('true')
    expect(rateInput.element.disabled).toBe(true)

    await probeToggle.trigger('click')
    expect(probeToggle.attributes('aria-checked')).toBe('false')
    expect(syncToggle.attributes('aria-checked')).toBe('false')
    expect(rateInput.element.disabled).toBe(false)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    const payload = updateAccountMock.mock.calls[0]?.[1]
    expect(payload?.upstream_billing_probe_enabled).toBe(false)
    expect(payload?.upstream_billing_rate_sync_enabled).toBe(false)
    expect(payload?.rate_multiplier).toBe(1)
  })

  it('disabling only rate sync keeps automatic probing enabled', async () => {
    const account = buildAccount()
    account.extra = {
      upstream_billing_probe_enabled: true,
      upstream_billing_rate_sync_enabled: true
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    await wrapper.get('[data-testid="upstream-billing-rate-sync"]').trigger('click')
    expect(wrapper.get('[data-testid="upstream-billing-auto-probe"]').attributes('aria-checked')).toBe(
      'true'
    )
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    const payload = updateAccountMock.mock.calls[0]?.[1]
    expect(payload?.upstream_billing_probe_enabled).toBe(true)
    expect(payload?.upstream_billing_rate_sync_enabled).toBe(false)
    expect(payload?.rate_multiplier).toBe(1)
  })

  it('clears OpenAI APIKey Responses override when set back to auto', async () => {
    const account = buildAccount()
    account.extra = {
      openai_responses_mode: 'force_chat_completions',
      openai_responses_supported: true
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('[data-testid="openai-responses-mode-select"]').setValue('auto')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('openai_responses_mode')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_responses_supported).toBe(true)
  })

  it('submits OpenAI APIKey endpoint capabilities from credentials', async () => {
    const account = buildAccount()
    account.credentials.openai_capabilities = ['chat_completions']
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    expect(wrapper.findAll('input[type="checkbox"]').some((input) => (input.element as HTMLInputElement).checked)).toBe(true)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.openai_capabilities).toEqual([
      'chat_completions'
    ])
  })

	it('submits OpenAI quota auto-pause thresholds in extra', async () => {
	  const account = buildAccount()
	  account.extra = {
		auto_pause_5h_threshold: 0.9,
		auto_pause_7d_threshold: 0.8
	  }
	  updateAccountMock.mockReset()
	  checkMixedChannelRiskMock.mockReset()
	  checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
	  updateAccountMock.mockResolvedValue(account)

	  const wrapper = mountModal(account)

	  await wrapper.get('[data-testid="auto-pause-5h-threshold"]').setValue('95')
	  await wrapper.get('[data-testid="auto-pause-7d-threshold"]').setValue('96')
	  await wrapper.get('form#edit-account-form').trigger('submit.prevent')

	  expect(updateAccountMock).toHaveBeenCalledTimes(1)
	  expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.auto_pause_5h_threshold).toBe(0.95)
	  expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.auto_pause_7d_threshold).toBe(0.96)
	})

	it('submits OpenAI extra settings for cpr accounts', async () => {
	  // 回归：cpr 走不进那个写 updatePayload.extra 的分支时，自动暂停阈值的控件
	  // 照常显示、能改、保存提示成功，但 extra 从头到尾没被赋值——重开弹窗全空，
	  // 额度打满后账号继续被调度直到上游 429。回填与写入必须共用同一个判定。
	  const account = buildAccount()
	  account.type = 'cpr'
	  account.credentials = {
	    base_url: 'http://127.0.0.1:18081',
	    api_key: 'sk_cpr',
	    admin_base_url: 'http://127.0.0.1:18081',
	    admin_api_key: 'admin_cpr',
	    cpr_account_id: 'acct_0199c0ffee'
	  }
	  account.extra = { auto_pause_5h_threshold: 0.9 }
	  updateAccountMock.mockReset()
	  checkMixedChannelRiskMock.mockReset()
	  checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
	  updateAccountMock.mockResolvedValue(account)

	  const wrapper = mountModal(account)

	  // 回填：cpr 也要读得出已有的阈值，否则会出现「保存后一刷新变默认值」。
	  expect(
	    wrapper.get<HTMLInputElement>('[data-testid="auto-pause-5h-threshold"]').element.value
	  ).toBe('90')

	  await wrapper.get('[data-testid="auto-pause-5h-threshold"]').setValue('95')
	  await wrapper.get('[data-testid="auto-pause-7d-threshold"]').setValue('80')
	  await wrapper.get('form#edit-account-form').trigger('submit.prevent')

	  expect(updateAccountMock).toHaveBeenCalledTimes(1)
	  const extra = updateAccountMock.mock.calls[0]?.[1]?.extra
	  expect(extra?.auto_pause_5h_threshold).toBe(0.95)
	  expect(extra?.auto_pause_7d_threshold).toBe(0.8)
	  // 同一个门控放开的另外两项也必须落库。
	  expect(extra).toHaveProperty('openai_long_context_billing_enabled')
	  // 自动透传与 WS 模式对 cpr 后端硬返回 false，不得因此被写进来。
	  expect(extra).not.toHaveProperty('openai_passthrough')
	  expect(extra).not.toHaveProperty('openai_oauth_responses_websockets_v2_mode')
	  expect(extra).not.toHaveProperty('openai_apikey_responses_websockets_v2_mode')
	})

	it('submits OpenAI quota auto-pause disable flag in extra', async () => {
	  // Toggling the per-account disable flag must persist as auto_pause_5h_disabled
	  // so an admin can exempt one account from auto-pause even when a global default
	  // threshold is configured (otherwise leaving the threshold blank would silently
	  // fall back to the global default).
	  const account = buildAccount()
	  updateAccountMock.mockReset()
	  checkMixedChannelRiskMock.mockReset()
	  checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
	  updateAccountMock.mockResolvedValue(account)

	  const wrapper = mountModal(account)

	  await wrapper.get('[data-testid="auto-pause-5h-disabled"]').trigger('click')
	  await wrapper.get('form#edit-account-form').trigger('submit.prevent')

	  expect(updateAccountMock).toHaveBeenCalledTimes(1)
	  expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.auto_pause_5h_disabled).toBe(true)
	  expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.auto_pause_7d_disabled).toBeUndefined()
	})

  it('preserves Seedance when exactly two endpoint capabilities are selected', async () => {
    const account = buildAccount()
    account.credentials.openai_capabilities = ['chat_completions', 'seedance']
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)
    const wrapper = mountModal(account)
    expect(wrapper.get<HTMLInputElement>('[data-testid="openai-endpoint-capability-seedance"]').element.checked).toBe(true)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.openai_capabilities).toEqual(['chat_completions', 'seedance'])
  })

  it('keeps at least one OpenAI APIKey endpoint capability selected', async () => {
    const account = buildAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    const chatCheckbox = wrapper.get<HTMLInputElement>(
      '[data-testid="openai-endpoint-capability-chat_completions"]'
    )
    const embeddingsCheckbox = wrapper.get<HTMLInputElement>(
      '[data-testid="openai-endpoint-capability-embeddings"]'
    )

    expect(chatCheckbox.element.checked).toBe(true)
    expect(embeddingsCheckbox.element.checked).toBe(true)

    await embeddingsCheckbox.setValue(false)

    expect(chatCheckbox.element.checked).toBe(true)
    expect(embeddingsCheckbox.element.checked).toBe(false)

    await chatCheckbox.setValue(false)

    expect(chatCheckbox.element.checked).toBe(true)
    expect(embeddingsCheckbox.element.checked).toBe(false)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.openai_capabilities).toEqual([
      'chat_completions'
    ])
  })

  it('disables text generation protocol when only embeddings requests are accepted', async () => {
    const account = buildAccount()
    account.credentials.openai_capabilities = ['embeddings']
    account.extra = {
      openai_responses_mode: 'force_responses',
      openai_responses_supported: true
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    const responsesModeSelect = wrapper.get<HTMLSelectElement>(
      '[data-testid="openai-responses-mode-select"]'
    )

    expect(responsesModeSelect.element.disabled).toBe(true)
    expect(wrapper.find('[data-testid="openai-responses-mode-not-applicable"]').exists()).toBe(true)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.openai_capabilities).toEqual([
      'embeddings'
    ])
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('openai_responses_mode')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_responses_supported).toBe(true)
  })

  it('submits Codex image tool force-inject mode as bridge override', async () => {
    const account = buildAccount()
    account.extra = {
      codex_image_generation_bridge: false,
      codex_image_generation_bridge_enabled: true
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    expect(wrapper.text()).toContain('admin.accounts.openai.codexImageTool')
    expect(wrapper.text()).toContain('admin.accounts.openai.codexImageToolDesc')
    expect(wrapper.text()).toContain('admin.accounts.openai.codexImageToolEnabledDesc')

    await wrapper.get('button[data-testid="codex-image-tool-enabled"]').trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.codex_image_generation_bridge).toBe(true)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('codex_image_generation_bridge_enabled')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('codex_image_generation_explicit_tool_policy')
  })

  it('submits Codex image tool no-injection mode without strip policy', async () => {
    const account = buildAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('button[data-testid="codex-image-tool-disabled"]').trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.codex_image_generation_bridge).toBe(false)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('codex_image_generation_explicit_tool_policy')
  })

  it('submits Codex image tool block mode as strip policy and clears bridge override', async () => {
    const account = buildAccount()
    account.extra = {
      codex_image_generation_bridge: true
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    expect(wrapper.text()).toContain('admin.accounts.openai.codexImageToolBlock')
    expect(wrapper.text()).toContain('admin.accounts.openai.codexImageToolBlockDesc')

    await wrapper.get('button[data-testid="codex-image-tool-block"]').trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.codex_image_generation_explicit_tool_policy).toBe('strip')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('codex_image_generation_bridge')
  })

  it('loads strip policy as block mode and clears both keys when reset to inherit', async () => {
    const account = buildAccount()
    account.extra = {
      codex_image_generation_explicit_tool_policy: 'strip'
    }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('button[data-testid="codex-image-tool-inherit"]').trigger('click')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('codex_image_generation_explicit_tool_policy')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra).not.toHaveProperty('codex_image_generation_bridge')
  })

  it('setup-token account can select and submit OAuth WS mode', async () => {
    const account = buildOpenAISetupTokenAccount()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('[data-testid="edit-openai-ws-mode-select"]').setValue('http_bridge')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_oauth_responses_websockets_v2_mode).toBe('http_bridge')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_oauth_responses_websockets_v2_enabled).toBe(true)
  })

  it('allows saving apikey account when backend redacted api_key but credentials_status reports it exists', async () => {
    // 新前端 + 新后端：响应已脱敏，credentials 里没有 api_key，credentials_status.has_api_key=true
    const account = buildAccount()
    account.credentials = {
      base_url: 'https://api.openai.com',
      model_mapping: { 'gpt-5.2': 'gpt-5.2' }
    }
    account.credentials_status = { has_api_key: true }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    // 用户未输入新 key 时，payload 不应带 api_key，由后端合并保留旧值
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).not.toHaveProperty('api_key')
  })

  it('allows saving apikey account against legacy backend without credentials_status', async () => {
    // 新前端 + 旧后端：credentials_status 缺失，但 credentials.api_key 仍是明文，应允许保存
    const account = buildAccount()
    // 显式确保没有 credentials_status
    expect(account.credentials_status).toBeUndefined()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    // 旧后端响应未脱敏，原 api_key 会随 currentCredentials 一起传回去（旧行为，等价于无操作）
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.api_key).toBe('sk-test')
  })

  it('blocks apikey save when neither credentials_status nor legacy api_key indicates existence', async () => {
    const account = buildAccount()
    account.credentials = {
      base_url: 'https://api.openai.com'
    }
    // 既没有 credentials_status 也没有旧的 api_key
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).not.toHaveBeenCalled()
  })

  it('allows saving Vertex SA account when backend redacted service_account_json but credentials_status reports it exists', async () => {
    // 新前端 + 新后端：响应已脱敏，credentials 里没有 service_account_json，credentials_status.has_service_account_json=true
    const account = buildVertexAccount()
    account.credentials = {
      project_id: 'demo-project',
      client_email: 'sa@example.iam.gserviceaccount.com',
      location: 'us-central1',
      tier_id: 'vertex'
    }
    account.credentials_status = { has_service_account_json: true }
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.project_id).toBe('demo-project')
  })

  it('allows saving Vertex SA account against legacy backend without credentials_status', async () => {
    // 新前端 + 旧后端：credentials_status 缺失，但 credentials.service_account_json 仍是明文，应允许保存
    const account = buildVertexAccount()
    expect(account.credentials_status).toBeUndefined()
    expect(account.credentials.service_account_json).toBeTruthy()
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
  })

  it('blocks Vertex SA save when neither credentials_status nor legacy json indicates existence', async () => {
    const account = buildVertexAccount()
    account.credentials = {
      project_id: 'demo-project',
      client_email: 'sa@example.iam.gserviceaccount.com',
      location: 'us-central1',
      tier_id: 'vertex'
    }
    // 既没有 credentials_status 也没有旧的 service_account_json
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })

    const wrapper = mountModal(account)

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).not.toHaveBeenCalled()
  })

  it('loads and submits Antigravity configured project fallback', async () => {
    const account = buildAntigravityAccount('configured-project')
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const input = wrapper.get<HTMLInputElement>('[data-testid="antigravity-project-id-input"]')
    expect(input.element.value).toBe('configured-project')

    await input.setValue('  updated-project  ')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials?.antigravity_project_id).toBe(
      'updated-project'
    )
  })

  it('clears Antigravity configured project fallback when input is empty', async () => {
    const account = buildAntigravityAccount('configured-project')
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset()
    checkMixedChannelRiskMock.mockResolvedValue({ has_risk: false })
    updateAccountMock.mockResolvedValue(account)

    const wrapper = mountModal(account)
    const input = wrapper.get<HTMLInputElement>('[data-testid="antigravity-project-id-input"]')

    await input.setValue('')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    expect(updateAccountMock.mock.calls[0]?.[1]?.credentials).not.toHaveProperty(
      'antigravity_project_id'
    )
  })
})

describe('EditAccountModal OpenAI 自动使用重置卡', () => {
  beforeEach(() => {
    authIsSimpleMode.value = true
    updateAccountMock.mockReset()
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })
  })

  it('仅对 OpenAI OAuth 母账号显示，默认关闭且阈值为 100/100', () => {
    const parent = mountModal(buildOpenAIOAuthParentAccount())
    expect(parent.find('[data-testid="auto-reset-credit-settings"]').exists()).toBe(true)
    expect((parent.get('[data-testid="auto-reset-credit-5h-threshold"]').element as HTMLInputElement).value).toBe('100')
    expect((parent.get('[data-testid="auto-reset-credit-7d-threshold"]').element as HTMLInputElement).value).toBe('100')
    expect(parent.get('[data-testid="auto-reset-credit-5h-threshold"]').attributes('disabled')).toBeDefined()
    parent.unmount()

    for (const account of [buildAccount(), buildOpenAISetupTokenAccount(), buildOpenAISparkShadowAccount()]) {
      const wrapper = mountModal(account)
      expect(wrapper.find('[data-testid="auto-reset-credit-settings"]').exists()).toBe(false)
      wrapper.unmount()
    }
  })

  it('独立保存两个阈值，并禁止把运行态回写到管理请求', async () => {
    const account = buildOpenAIOAuthParentAccount()
    account.extra = {
      codex_auto_reset_credit_state: {
        status: 'success',
        trigger_window: '5h',
        available_count: 1
      }
    }
    updateAccountMock.mockResolvedValue(account)
    const wrapper = mountModal(account)

    await wrapper.get('[data-testid="auto-reset-credit-enabled"]').trigger('click')
    await wrapper.get('[data-testid="auto-reset-credit-5h-threshold"]').setValue('75.5')
    await wrapper.get('[data-testid="auto-reset-credit-7d-threshold"]').setValue('92')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    const extra = updateAccountMock.mock.calls[0]?.[1]?.extra
    expect(extra).toMatchObject({
      auto_reset_credit_enabled: true,
      auto_reset_credit_5h_threshold: 0.755,
      auto_reset_credit_7d_threshold: 0.92
    })
    expect(extra).not.toHaveProperty('codex_auto_reset_credit_state')
    wrapper.unmount()
  })

  it('开启后拒绝超出 0.1–100 范围的任一阈值', async () => {
    const wrapper = mountModal(buildOpenAIOAuthParentAccount())
    await wrapper.get('[data-testid="auto-reset-credit-enabled"]').trigger('click')
    await wrapper.get('[data-testid="auto-reset-credit-5h-threshold"]').setValue('0')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock).not.toHaveBeenCalled()
    wrapper.unmount()
  })
})

describe('EditAccountModal turn-state 自动接管', () => {
  const buildCodexAccount = (extra: Record<string, unknown> = {}) =>
    ({
      ...buildOpenAIOAuthParentAccount(),
      extra
    }) as any

  beforeEach(() => {
    updateAccountMock.mockReset().mockResolvedValue({})
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })
  })

  /**
   * CPR 账号的 ProxySelector 只作用于 sub2api → CPR 这一跳，真正出上游的是 CPR 自己绑的
   * 代理。没有这句提示的话，运维在这里换个代理会以为出口跟着变了，而实际一点没变——
   * 这正是 292 探测要按 IP 轮换时第一个会踩的坑。
   */
  it('有 CPR 出口信息时在代理选择器下方提示真实出口', () => {
    const wrapper = mountModal(
      buildCodexAccount({ cpr_outbound_proxy: 'socks5h://198.51.100.7:1080' })
    )
    expect(wrapper.get('[data-testid="edit-account-cpr-outbound"]').text()).toBe(
      'admin.accounts.cprOutboundHint'
    )
    wrapper.unmount()
  })

  it('没有 CPR 出口信息时不占位', () => {
    const wrapper = mountModal(buildCodexAccount())
    expect(wrapper.find('[data-testid="edit-account-cpr-outbound"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('开着开关时手填框置灰并显示「已由自动接管」', async () => {
    const wrapper = mountModal(buildCodexAccount({ openai_turn_state_auto: true }))

    const textarea = wrapper.get<HTMLTextAreaElement>(
      'textarea[placeholder="admin.accounts.openai.turnStateOverridePlaceholder"]'
    )
    expect(textarea.element.disabled).toBe(true)
    expect(wrapper.find('[data-testid="edit-openai-turn-state-auto-banner"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('关着开关时手填框可用且无横幅', async () => {
    const wrapper = mountModal(buildCodexAccount())
    // 模型下拉是聚焦才拉的（那条接口对 oauth 有置错误的副作用），且是异步的。
    await wrapper.get('[data-testid="edit-openai-turn-state-model"]').trigger('focus')
    await flushPromises()

    const textarea = wrapper.get<HTMLTextAreaElement>(
      'textarea[placeholder="admin.accounts.openai.turnStateOverridePlaceholder"]'
    )
    expect(textarea.element.disabled).toBe(false)
    expect(wrapper.find('[data-testid="edit-openai-turn-state-auto-banner"]').exists()).toBe(false)
    wrapper.unmount()
  })

  // 候选池由后端在保存时强制还原（admin_account.go 的保留清单），前端只负责别把它弄丢。
  it('打开开关只写 openai_turn_state_auto，候选池原样带回', async () => {
    const pool = [{ blob: 'gAAAAAB...', minted_at: '2026-09-17T00:00:00Z' }]
    const wrapper = mountModal(buildCodexAccount({ openai_turn_state_pool: pool }))

    await wrapper.get('[data-testid="edit-openai-turn-state-auto"]').setValue(true)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    const extra = updateAccountMock.mock.calls[0]?.[1]?.extra
    expect(extra).toMatchObject({ openai_turn_state_auto: true })
    expect(extra?.openai_turn_state_pool).toEqual(pool)
    wrapper.unmount()
  })

  it('关掉开关时把键删掉而不是写 false', async () => {
    const wrapper = mountModal(buildCodexAccount({ openai_turn_state_auto: true }))

    await wrapper.get('[data-testid="edit-openai-turn-state-auto"]').setValue(false)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    const extra = updateAccountMock.mock.calls[0]?.[1]?.extra
    expect(extra).toBeDefined()
    expect(extra).not.toHaveProperty('openai_turn_state_auto')
    wrapper.unmount()
  })

  // 覆写表是 {模型: blob}：turn-state 绑死在铸它的那个模型上，blob 本身是密文，
  // 系统无从得知它来自哪个模型，只能由管理员在下拉里指定。
  it('手填覆写按模型写回，切模型互不覆盖', async () => {
    const wrapper = mountModal(buildCodexAccount())
    const select = wrapper.get('[data-testid="edit-openai-turn-state-model"]')
    // 懒加载：下拉要先聚焦才会去拉模型列表。
    await select.trigger('focus')
    await flushPromises()

    const textarea = () =>
      wrapper.get<HTMLTextAreaElement>(
        'textarea[placeholder="admin.accounts.openai.turnStateOverridePlaceholder"]'
      )

    await select.setValue('gpt-5.6-luna')
    await textarea().setValue('gAAAAAB-luna')
    await select.setValue('gpt-6-astra')
    // 切过去是空的：另一个模型的票不该串过来。
    expect(textarea().element.value).toBe('')
    await textarea().setValue('gAAAAAB-astra')

    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_turn_state_override).toEqual({
      'gpt-5.6-luna': 'gAAAAAB-luna',
      'gpt-6-astra': 'gAAAAAB-astra'
    })
    wrapper.unmount()
  })

  it('清空某个模型的票就从表里删掉该模型，而不是留个空串', async () => {
    const wrapper = mountModal(
      buildCodexAccount({
        openai_turn_state_override: { 'gpt-5.6-luna': 'gAAAAAB-luna', 'gpt-6-astra': 'gAAAAAB-astra' }
      })
    )
    await wrapper.get('[data-testid="edit-openai-turn-state-model"]').trigger('focus')
    await flushPromises()

    await wrapper.get('[data-testid="edit-openai-turn-state-model"]').setValue('gpt-5.6-luna')
    await wrapper
      .get('textarea[placeholder="admin.accounts.openai.turnStateOverridePlaceholder"]')
      .setValue('')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_turn_state_override).toEqual({
      'gpt-6-astra': 'gAAAAAB-astra'
    })
    wrapper.unmount()
  })
})

describe('EditAccountModal 292 猎手', () => {
  const buildCodexAccount = (extra: Record<string, unknown> = {}) =>
    ({
      ...buildOpenAIOAuthParentAccount(),
      extra
    }) as any

  beforeEach(() => {
    updateAccountMock.mockReset().mockResolvedValue({})
    checkMixedChannelRiskMock.mockReset().mockResolvedValue({ has_risk: false })
  })

  // 猎到的票靠自动接管注入：接管关着时开猎手等于白烧额度，开关直接置灰并说明原因。
  it('自动接管关着时猎手开关置灰并提示', () => {
    const wrapper = mountModal(buildCodexAccount())
    const toggle = wrapper.get<HTMLInputElement>('[data-testid="edit-openai-turn-state-hunter"]')
    expect(toggle.element.disabled).toBe(true)
    expect(wrapper.find('[data-testid="edit-openai-turn-state-hunter-needs-auto"]').exists()).toBe(true)
    wrapper.unmount()
  })

  it('cpr 账号不显示 turn-state 替换设置：原样中继，只观测不替换', () => {
    const wrapper = mountModal({ ...buildCodexAccount({ openai_turn_state_auto: true }), type: 'cpr' })
    expect(wrapper.find('[data-testid="edit-openai-turn-state-hunter-section"]').exists()).toBe(false)
    expect(wrapper.find('[data-testid="edit-openai-turn-state-auto"]').exists()).toBe(false)
    expect(wrapper.find('[data-testid="edit-openai-turn-state-deprecated"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('开猎手、选模型和代理后只写 openai_turn_state_hunter，留空的数值不写', async () => {
    const wrapper = mountModal(buildCodexAccount({ openai_turn_state_auto: true }))

    await wrapper.get('[data-testid="edit-openai-turn-state-hunter"]').setValue(true)
    await flushPromises() // 模型列表与代理列表都是开关打开时才拉的
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter-models"]').setValue(['gpt-6-astra'])
    // setValue 按 DOM 的 option.value（字符串）匹配；v-model 再按 :value 绑定回数字。
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter-proxies"]').setValue(['20'])
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter-max"]').setValue('40')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    expect(updateAccountMock).toHaveBeenCalledTimes(1)
    const extra = updateAccountMock.mock.calls[0]?.[1]?.extra
    expect(extra?.openai_turn_state_hunter).toEqual({
      enabled: true,
      models: ['gpt-6-astra'],
      proxy_ids: [20],
      max_per_hour: 40
    })
    expect(extra).not.toHaveProperty('openai_turn_state_hunt')
    wrapper.unmount()
  })

  // 关掉开关保留已选的模型/代理，再开时不用重选；运行态键由猎手维护，前端原样带回。
  it('关掉猎手保留选择，运行态原样带回', async () => {
    const hunt = { hour_count: 3, last: [] }
    const wrapper = mountModal(
      buildCodexAccount({
        openai_turn_state_auto: true,
        openai_turn_state_hunter: { enabled: true, models: ['gpt-6-astra'], proxy_ids: [20] },
        openai_turn_state_hunt: hunt
      })
    )

    await wrapper.get('[data-testid="edit-openai-turn-state-hunter"]').setValue(false)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    const extra = updateAccountMock.mock.calls[0]?.[1]?.extra
    expect(extra?.openai_turn_state_hunter).toEqual({ enabled: false, models: ['gpt-6-astra'], proxy_ids: [20] })
    expect(extra?.openai_turn_state_hunt).toEqual(hunt)
    wrapper.unmount()
  })

  // retry_minutes 没有 UI 字段（只经 API 写入），改区块里别的项时整个对象会被替换，它得跟着回去。
  it('改猎手别的项时 retry_minutes 不丢', async () => {
    const wrapper = mountModal(
      buildCodexAccount({
        openai_turn_state_auto: true,
        openai_turn_state_hunter: { enabled: true, models: ['gpt-6-astra'], proxy_ids: [20], retry_minutes: 30 }
      })
    )
    await flushPromises()
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter-max"]').setValue('40')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')

    const extra = updateAccountMock.mock.calls[0]?.[1]?.extra
    expect(extra?.openai_turn_state_hunter).toEqual({
      enabled: true,
      models: ['gpt-6-astra'],
      proxy_ids: [20],
      max_per_hour: 40,
      retry_minutes: 30
    })
    wrapper.unmount()
  })

  // 降智暂停是猎手区块里的一个开关：勾上写 hold_when_degraded: true；没勾不写键（后端零值同义）。
  it('勾选降智暂停写入 hold_when_degraded，改别的项时不丢', async () => {
    const wrapper = mountModal(
      buildCodexAccount({
        openai_turn_state_auto: true,
        openai_turn_state_hunter: { enabled: true, models: ['gpt-6-astra'], proxy_ids: [20] }
      })
    )
    await flushPromises()
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter-hold"]').setValue(true)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_turn_state_hunter).toEqual({
      enabled: true,
      models: ['gpt-6-astra'],
      proxy_ids: [20],
      hold_when_degraded: true
    })
    wrapper.unmount()

    const kept = mountModal(
      buildCodexAccount({
        openai_turn_state_auto: true,
        openai_turn_state_hunter: { enabled: true, models: ['gpt-6-astra'], proxy_ids: [20], hold_when_degraded: true }
      })
    )
    await flushPromises()
    expect((kept.get('[data-testid="edit-openai-turn-state-hunter-hold"]').element as HTMLInputElement).checked).toBe(true)
    await kept.get('[data-testid="edit-openai-turn-state-hunter-max"]').setValue('40')
    await kept.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[1]?.[1]?.extra?.openai_turn_state_hunter).toEqual({
      enabled: true,
      models: ['gpt-6-astra'],
      proxy_ids: [20],
      max_per_hour: 40,
      hold_when_degraded: true
    })
    kept.unmount()
  })

  // 轮换标记按已选代理逐个勾；取消勾选代理后它的标记不留残余。
  it('轮换代理勾选写入 rotating_proxy_ids，且只保留仍在探测列表里的', async () => {
    const wrapper = mountModal(
      buildCodexAccount({
        openai_turn_state_auto: true,
        openai_turn_state_hunter: { enabled: true, models: ['gpt-6-astra'], proxy_ids: [20, 21], rotating_proxy_ids: [21] }
      })
    )
    await flushPromises()
    expect((wrapper.get('[data-testid="edit-openai-turn-state-hunter-rotating-21"]').element as HTMLInputElement).checked).toBe(true)
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter-rotating-20"]').setValue(true)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_turn_state_hunter).toEqual({
      enabled: true,
      models: ['gpt-6-astra'],
      proxy_ids: [20, 21],
      rotating_proxy_ids: [21, 20]
    })
    wrapper.unmount()

    const pruned = mountModal(
      buildCodexAccount({
        openai_turn_state_auto: true,
        openai_turn_state_hunter: { enabled: true, models: ['gpt-6-astra'], proxy_ids: [20, 21], rotating_proxy_ids: [21] }
      })
    )
    await flushPromises()
    await pruned.get('[data-testid="edit-openai-turn-state-hunter-proxies"]').setValue(['20'])
    await pruned.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[1]?.[1]?.extra?.openai_turn_state_hunter).toEqual({
      enabled: true,
      models: ['gpt-6-astra'],
      proxy_ids: [20]
    })
    pruned.unmount()
  })

  // 自动定模型：勾上后手选列表置灰、可以为空也能提交，写 auto_models: true。
  it('按真实请求自动定模型：不选模型也能提交', async () => {
    const wrapper = mountModal(buildCodexAccount({ openai_turn_state_auto: true }))
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter"]').setValue(true)
    await flushPromises()
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter-auto-models"]').setValue(true)
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter-proxies"]').setValue(['20'])
    expect((wrapper.get('[data-testid="edit-openai-turn-state-hunter-models"]').element as HTMLSelectElement).disabled).toBe(true)
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    await flushPromises()
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_turn_state_hunter).toEqual({
      enabled: true,
      models: [],
      proxy_ids: [20],
      auto_models: true
    })
    wrapper.unmount()
  })

  // 记账 key 是数字字段：填了写 usage_api_key_id，清空就不写键（后端 0 同义于不记）。
  it('记账 API Key ID 往返', async () => {
    const wrapper = mountModal(
      buildCodexAccount({
        openai_turn_state_auto: true,
        openai_turn_state_hunter: { enabled: true, models: ['gpt-6-astra'], proxy_ids: [20], usage_api_key_id: 7 }
      })
    )
    await flushPromises()
    const field = wrapper.get('[data-testid="edit-openai-turn-state-hunter-usage-key"]')
    expect((field.element as HTMLInputElement).value).toBe('7')
    await field.setValue('12')
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    expect(updateAccountMock.mock.calls[0]?.[1]?.extra?.openai_turn_state_hunter).toEqual({
      enabled: true,
      models: ['gpt-6-astra'],
      proxy_ids: [20],
      usage_api_key_id: 12
    })
    wrapper.unmount()
  })

  // 与后端 ValidateOpenAITurnStateHunterExtra 同口径：开着没选模型/代理直接拦下，不发请求。
  it('猎手开着但没选模型或代理：不提交', async () => {
    const wrapper = mountModal(buildCodexAccount({ openai_turn_state_auto: true }))
    await wrapper.get('[data-testid="edit-openai-turn-state-hunter"]').setValue(true)
    await flushPromises()
    await wrapper.get('form#edit-account-form').trigger('submit.prevent')
    await flushPromises()

    expect(updateAccountMock).not.toHaveBeenCalled()
    wrapper.unmount()
  })
})
