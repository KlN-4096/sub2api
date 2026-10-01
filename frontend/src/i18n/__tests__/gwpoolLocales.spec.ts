import { describe, expect, it } from 'vitest'

import { extractI18nErrorMessage } from '@/utils/apiError'
import en from '../locales/en'
import zh from '../locales/zh'

// 后端只给稳定的 reason code（GWPOOL_*），文案在这里（ops_user_error.go 的同口径：后端给码、
// 前端做 i18n）。这个 spec 钉两件事：键在两种语言下都在，以及 extractI18nErrorMessage 真的能
// 按 code 取到译文——生产里靠「t 返回值 !== key」判有没有映射，组件 spec 的 t 桩钉不住这一步。
const GWPOOL_ERROR_NAMESPACE = 'admin.accounts.openai.gwpoolErrors'

type Messages = Record<string, unknown>

// translatorFor 模拟 vue-i18n 的行为：命中就回译文，没命中原样回 key。
function translatorFor(messages: Messages) {
  return (key: string): string => {
    let node: unknown = messages
    for (const segment of key.split('.')) {
      if (!node || typeof node !== 'object') return key
      node = (node as Messages)[segment]
    }
    return typeof node === 'string' ? node : key
  }
}

describe('gateway pool locale keys', () => {
  it.each([
    ['zh', zh],
    ['en', en]
  ] as const)('%s exposes every account-level gateway pool knob', (_locale, messages) => {
    const openai = (messages as any).admin.accounts.openai
    for (const key of [
      'gwpool',
      'gwpoolDesc',
      'gwpoolBaseUrl',
      'gwpoolBaseUrlDesc',
      'gwpoolConsumerKey',
      'gwpoolConsumerKeyDesc',
      'gwpoolAllModels',
      'gwpoolAllModelsDesc',
      'gwpoolAdvanced',
      'gwpoolGatewayWindow',
      'gwpoolGatewayWindowDesc',
      'gwpoolFetchTimeout',
      'gwpoolFetchTimeoutDesc',
      'gwpoolListTimeout',
      'gwpoolListTimeoutDesc',
      'gwpoolSteering',
      'gwpoolSteeringDesc'
    ]) {
      expect(typeof openai[key], key).toBe('string')
    }
    // 地址提示必须点出「根地址」这个坑（用户填过 /a/xxxx 个人页面）。
    expect(openai.gwpoolBaseUrlDesc).toContain('pool.0102400.xyz')
    expect(openai.gwpoolBaseUrlDesc).toContain('/a/xxxx')
  })

  it.each([
    ['zh', zh],
    ['en', en]
  ] as const)('%s localizes the backend GWPOOL_* reason codes', (_locale, messages) => {
    const t = translatorFor(messages as Messages)
    for (const reason of ['GWPOOL_BASE_URL_INVALID', 'GWPOOL_CONSUMER_KEY_REQUIRED']) {
      const localized = extractI18nErrorMessage(
        { reason, message: 'account 1 enables openai_gwpool without openai_gwpool_base_url' },
        t,
        GWPOOL_ERROR_NAMESPACE,
        'fallback'
      )
      expect(localized, reason).not.toBe('fallback')
      expect(localized, reason).not.toContain('openai_gwpool_base_url')
      expect(localized, reason).toBe(t(`${GWPOOL_ERROR_NAMESPACE}.${reason}`))
    }
    // 没有映射的 code 仍然回落后端原串，不能被吞掉。
    expect(
      extractI18nErrorMessage({ reason: 'SOMETHING_ELSE', message: 'backend says no' }, t, GWPOOL_ERROR_NAMESPACE, 'fallback')
    ).toBe('backend says no')
  })

  it.each([
    ['zh', zh],
    ['en', en]
  ] as const)('%s labels the usage-log route pair override badge', (_locale, messages) => {
    const usage = (messages as any).admin.usage
    expect(typeof usage.routePairOverriddenShort).toBe('string')
    expect(typeof usage.routePairOverridden).toBe('string')
    expect(typeof usage.routePairReroutedShort).toBe('string')
    expect(typeof usage.routePairPoolVersion).toBe('string')
    // 被改派的文案要把两个网关都点出来，否则读的人不知道比对的是什么。
    expect(usage.routePairRerouted).toContain('{promised}')
    expect(usage.routePairRerouted).toContain('{landed}')
    // 卡片只给网关名与状态，不许把 cookie 本体写进文案。
    expect(usage.routePairOverridden).not.toContain('=')
    expect(usage.routePairRerouted).not.toContain('=')
  })
})
