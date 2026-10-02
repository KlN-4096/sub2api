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
      'gwpoolAdvanced',
      'gwpoolGatewayWindow',
      'gwpoolGatewayWindowDesc',
      'gwpoolFetchTimeout',
      'gwpoolFetchTimeoutDesc',
      'gwpoolListTimeout',
      'gwpoolListTimeoutDesc',
      'gwpoolSteering',
      'gwpoolSteeringDesc',
      'gwpoolGuard'
    ]) {
      expect(typeof openai[key], key).toBe('string')
    }
    // 降智防护的四档：每档都要有标签和说明，少一条下拉里就会出现一个空选项。
    for (const mode of ['off', 'cut', 'retry', 'queue']) {
      expect(typeof openai.gwpoolGuardModes[mode], mode).toBe('string')
      expect(typeof openai.gwpoolGuardDescs[mode], mode).toBe('string')
      expect(openai.gwpoolGuardDescs[mode].length, mode).toBeGreaterThan(40)
    }
    // queue 档会花掉上游配额（平均约 6 发垫话换一个窗口），说明里必须写清成本 —— 它是这一档
    // 唯一的代价，运营方不该靠读源码才知道。
    expect(openai.gwpoolGuardDescs.queue).toContain('gwpool_warm_probe')
    // 判不出来那条路的**行为**必须点名，而且必须点对：代码在那条路上放行业务请求
    // （openai_gwpool_warm.go 的 `case !conclusive`），第一版文案写的是「这一发直接失败、
    // 上游限流期间这一档会挡掉每个请求」—— 正好相反。运营方选这一档就是为了「上游不正常时
    // 宁可失败也别放降智出去」，而这里恰好是它做不到的那一格，说反了比不说更坏。
    // 长度/关键词断言抓不到语义反转，所以钉死这两个判别词。
    expect(openai.gwpoolGuardDescs.queue).toContain('gwpool_warm_inconclusive')
    // 和首输出超时共享墙上时间这件事也要点名：配到 30 秒以下整档静默失效。
    expect(openai.gwpoolGuardDescs.queue).toContain('gwpool_warm_no_budget')
    // 地址提示必须点出「根地址」这个坑（用户填过 /a/xxxx 个人页面）。
    expect(openai.gwpoolBaseUrlDesc).toContain('pool.0102400.xyz')
    expect(openai.gwpoolBaseUrlDesc).toContain('/a/xxxx')
    // 九宫格的四种色只靠颜色传达不行（9px 字号、emerald/rose 同明度、title 触屏摸不到），
    // 图例必须在页面上，而且要带那两个字符前缀。
    for (const mark of ['✓', '!']) {
      expect(openai.gatewayHistory.legend, mark).toContain(mark)
    }
  })

  // 判不出来那条路的文案在中英两边都必须说「放行」，不能说「失败」。
  // 分开一条用例是因为判别词按语言不同，塞进上面那个 it.each 会变成一堆 if。
  it('describes the inconclusive filler shot as letting the request through', () => {
    expect(zh.admin.accounts.openai.gwpoolGuardDescs.queue).toContain('不拦这一发')
    expect(zh.admin.accounts.openai.gwpoolGuardDescs.queue).not.toContain('这一发直接失败')
    expect(en.admin.accounts.openai.gwpoolGuardDescs.queue).toContain('is not blocked')
    expect(en.admin.accounts.openai.gwpoolGuardDescs.queue).not.toContain('the request simply fails')
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

  it.each([
    ['zh', zh],
    ['en', en]
  ] as const)('%s labels the state-echo degraded request type', (_locale, messages) => {
    // request_type=gwpool_degraded 的用量行（被判降智后整发丢掉的那一次上游尝试）要有标签，
    // 否则筛选下拉里会出现一个空选项。
    expect(typeof (messages as any).usage.gwpoolDegraded).toBe('string')
    expect((messages as any).usage.gwpoolDegraded.length).toBeGreaterThan(0)
  })
})
