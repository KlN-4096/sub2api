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
      'gwpoolWarmTickets',
      'gwpoolWarmTicketsDesc',
      'gwpoolPrewarm',
      'gwpoolPrewarmDesc',
      'gwpoolAutoWait',
      'gwpoolAutoWaitDesc',
      'gwpoolMaxWait',
      'gwpoolMaxWaitDesc',
      'gwpoolGuard',
      'gwpoolGuardDesc',
      'gwpoolDetails',
      'gwpoolCooldownDetails',
      'gwpoolPrewarmDetails',
      'gwpoolGuardDetails',
      'gwpoolWarmDetails'
    ]) {
      expect(typeof openai[key], key).toBe('string')
    }
    // 降智防护 2026-10-03 删成零档：页面上只剩一段常驻说明，下拉和三个档的标签全删了。
    // 这三条钉住「别把档位接回来」—— 留着任何一个标签，页面就会重新长出一个选不中的选项。
    expect(openai.gwpoolGuardModes, '档位已删').toBeUndefined()
    for (const mode of ['off', 'cut', 'retry']) {
      expect(openai.gwpoolGuardDescs[mode], `${mode} 档已删`).toBeUndefined()
    }
    expect(typeof openai.gwpoolGuardDescs.queue).toBe('string')
    expect(openai.gwpoolGuardDescs.queue.length).toBeGreaterThan(40)
    // 排障细节移到展开说明，正常配置区只显示用途、默认值和代价。
    for (const log of ['gwpool_warm_probe', 'gwpool_warm_inconclusive', 'gwpool_warm_no_budget']) {
      expect(openai.gwpoolWarmDetails).toContain(log)
    }
    expect(openai.gwpoolWarmTicketsDesc).toContain('5')
    expect(openai.gwpoolWarmTicketsDesc).toContain('8')
    expect(openai.gwpoolWarmTicketsDesc).not.toMatch(/4 小时|4-hour|82%|25.*hour/)
    expect(openai.gwpoolPrewarmDesc).toContain('1')
    expect(openai.gwpoolPrewarmDesc).not.toMatch(/一秒空档|no gap at all|p95.*15/)
    expect(openai.gwpoolPrewarmDetails).toContain('90')
    expect(openai.gwpoolPrewarmDetails).toContain('60')
    expect(openai.gwpoolGatewayWindowDesc).toContain('3600')
    expect(openai.gwpoolCooldownDetails).toContain('1/2/4/6/8/10')
    // 地址提示必须点出「根地址」这个坑（用户填过 /a/xxxx 个人页面）。
    expect(openai.gwpoolBaseUrlDesc).toContain('pool.0102400.xyz')
    expect(openai.gwpoolBaseUrlDesc).toContain('/a/xxxx')
    // 九宫格的四种色只靠颜色传达不行（9px 字号、emerald/rose 同明度、title 触屏摸不到），
    // 图例必须在页面上，而且要带那两个字符前缀。
    for (const mark of ['✓', '!']) {
      expect(openai.gatewayHistory.legend, mark).toContain(mark)
    }
    // 自适应冷却到期只是允许重试，不能把预测写成满血保底或配额。
    expect(openai.gatewayHistory.forecast).toMatch(/可重试|retry-eligible/)
    expect(openai.gatewayHistory.forecast).not.toMatch(/至少|at least|最多|at most/)
    expect(openai.gatewayHistory.forecastHint).toMatch(/不是满血保底|not guaranteed/)
    expect(openai.gatewayHistory.forecastHint).toMatch(/其他行|Other rows/)
    expect(openai.gatewayHistory.forecastHint).toMatch(/确认恢复|confirm recovery/)
    // **单位是 (账号 × 网关)，不是大区。** 2026-10-03 用户纠正：「时间还是按网关来的，
    // 相同区域不同网关同一个号还是有不同的满血期的」。第一版按大区数，把 us-west 那 20 个
    // 网关名算成 1 个单位 ⇒ 预测值低一个数量级。这条断言钉住口径，别再被「九宫格是按大区
    // 画的」带回去。
    expect(openai.gatewayHistory.forecastHint).toMatch(/账号 × 网关|account × gateway/)
    expect(openai.gatewayHistory.forecastHint).not.toMatch(/账号 × 大区|account × region/)
    // 而且要明说「别按大区并」：那正是会被顺手改回去的那一步。
    expect(openai.gatewayHistory.forecastHint).toMatch(/别按大区并|do not collapse by region/)
    // 按大区的那三条键删了（上行空间改成在 tooltip 里定性说一句：这一行不知道池子一共有
    // 多少网关，给不出数）。留着会让下一个人以为还有个数字该显示。
    expect(openai.gatewayHistory.forecastUntouched, '按大区的上行空间文案已删').toBeUndefined()
    expect(openai.gatewayHistory.forecastUntouchedHint, '按大区的上行空间文案已删').toBeUndefined()
    expect(openai.gatewayHistory.forecastBlindHint, '未归类扣减文案已删').toBeUndefined()
    // 预测的插值与样本门槛须匹配组件，不能重新引入固定窗口。
    // 改了名字 vue-i18n 会静默渲染成字面量。
    expect(openai.gatewayHistory.forecast).toContain('{minutes}')
    expect(openai.gatewayHistory.forecastHint).toContain('{units}')
    for (const key of ['model', 'source', 'results', 'windows']) {
      expect(openai.gatewayHistory.forecastHint).toContain(`{${key}}`)
    }
    expect(openai.gatewayHistory.forecastHint).not.toContain('{window}')
    expect(openai.gatewayHistory.forecastPending).toMatch(/待统计|awaiting/)
  })

  // 判不出来那条路的文案在中英两边都必须说「放行」，不能说「失败」。
  // 分开一条用例是因为判别词按语言不同，塞进上面那个 it.each 会变成一堆 if。
  it('describes strict blocking and the explicit off switch', () => {
    expect(zh.admin.accounts.openai.gwpoolGuardDesc).toContain('阻止业务请求')
    expect(zh.admin.accounts.openai.gwpoolGuardDesc).toContain('关闭后')
    expect(zh.admin.accounts.openai.gwpoolGuardDesc).not.toContain('这一发直接失败')
    expect(en.admin.accounts.openai.gwpoolGuardDesc).toContain('blocks the business request')
    expect(en.admin.accounts.openai.gwpoolGuardDesc).toContain('When off')
    expect(en.admin.accounts.openai.gwpoolGuardDesc).not.toContain('the request simply fails')
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
    // 「已覆写」那一档必须说清落点**没观测到**。它读的是我们自己发出去那张 cookie，而上游
    // 只在改派时才下发新 __oailb ⇒「池子说的和 route_gateway 一样」恒为真、无法证伪。
    // 第一版写的是「落点就是池子给的那个网关」，那是拿零证据当确认 —— 而「消费者回放之后
    // 到底落在哪」恰好是 (账号 × 网关) 这个单位唯一还没实证的一环，这条文案是页面上唯一的提示。
    expect(usage.routePairOverridden).toMatch(/没有观测到|never observed/)
    expect(usage.routePairOverridden).not.toMatch(/落点就是|landed on the gateway/)
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
