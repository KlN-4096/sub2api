import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountGatewayCell from '../AccountGatewayCell.vue'
import type { Account } from '@/types'

// 只替 useI18n，其余保留真实导出：src/utils/format.ts 会 import src/i18n/index.ts，
// 整个模块被 mock 掉的话 createI18n 就没了。
vi.mock('vue-i18n', async (importOriginal) => ({
  ...(await importOriginal<typeof import('vue-i18n')>()),
  useI18n: () => ({
    t: (key: string, params?: Record<string, unknown>) =>
      params ? `${key}:${JSON.stringify(params)}` : key
  })
}))

const nowSec = Math.floor(Date.now() / 1000)
const isoAgo = (sec: number) => new Date((nowSec - sec) * 1000).toISOString()

const account = (gateways: unknown, extra: Record<string, unknown> = {}): Account =>
  ({
    id: 1,
    platform: 'openai',
    type: 'oauth',
    extra: { openai_gwpool_gateways: gateways, ...extra }
  }) as unknown as Account

const render = (acc: Account) => mount(AccountGatewayCell, { props: { account: acc } })

const cell = (w: ReturnType<typeof render>, region: string) =>
  w.get(`[data-testid="account-gateway-region-${region}"]`)

/**
 * 格子里是「大区名 + 判定字符 + 网关号」，断言只看网关号那一截（大区名是 i18n key 桩）。
 * 判定字符单独由 markOf 看 —— 两件事分开断言，改一个不会连带改另一个的期望值。
 */
const gatewayOf = (w: ReturnType<typeof render>, region: string) =>
  markedOf(w, region)?.replace(/^[✓!] /, '')

const markedOf = (w: ReturnType<typeof render>, region: string) =>
  cell(w, region)
    .findAll('span')
    .map((s) => s.text())
    .at(-1)

/** 判定字符：'✓ ' = 验过满血，'! ' = 窗口内碰过（现在打就是降智），'' = 已过窗口。 */
const markOf = (w: ReturnType<typeof render>, region: string) =>
  (markedOf(w, region) ?? '').match(/^[✓!] /)?.[0] ?? ''

/** 「还烧着」= 本地账本窗口内碰过 = 格子不是淡显的那一档。 */
const isHot = (w: ReturnType<typeof render>, region: string) => tone(w, region) !== 'idle'

/** 格子的状态色：full / degraded / idle（见 AccountGatewayCell 的 TONE_CLASS）。 */
const tone = (w: ReturnType<typeof render>, region: string) => cell(w, region).attributes('data-tone')

describe('AccountGatewayCell', () => {
  it('第一行是当前大区 · 当前网关', () => {
    const w = render(
      account({
        current: 'unified-73',
        current_region: 'east-asia',
        seen: { 'unified-73': { at: isoAgo(60), region: 'east-asia' } },
        updated_at: isoAgo(60)
      })
    )
    const text = w.get('[data-testid="account-gateway-current"]').text()
    expect(text).toContain('73')
    expect(text).toContain('gatewayHistory.regions.east-asia')
  })

  // 这张格子的全部意义：按大区摊开，才看得出「这个号还能去哪个大区铸没烧过的票」。
  it('按大区摊开打过的网关，没打过的大区留空位', () => {
    const w = render(
      account({
        current: 'unified-73',
        current_region: 'east-asia',
        seen: {
          'unified-73': { at: isoAgo(60), region: 'east-asia' },
          'unified-142': { at: isoAgo(600), region: 'us-east' }
        },
        updated_at: isoAgo(60)
      })
    )
    expect(gatewayOf(w, 'east-asia')).toBe('73')
    expect(gatewayOf(w, 'us-east')).toBe('142')
    // 没打过的大区照样有格子（它才是「还能去哪儿」的答案），但没有网关名。
    expect(gatewayOf(w, 'europe')).toBe('-')
    // 「未归类」只在真有读不出大区的落点时才出现，平时不占位。
    expect(w.find('[data-testid="account-gateway-region-unknown"]').exists()).toBe(false)
  })

  // 4 小时（= 槽位冷却 = 本地账本窗口默认值）内打过的才算还烧着。窗口外的要淡下去，
  // 否则这一列永远全亮，答不出「现在能去哪个大区」。
  it('按窗口判冷热：窗口内打过算烧着，窗口外的冷却完了', () => {
    const w = render(
      account({
        current: 'unified-73',
        current_region: 'east-asia',
        seen: {
          'unified-73': { at: isoAgo(600), region: 'east-asia' },
          'unified-142': { at: isoAgo(5 * 3600), region: 'us-east' }
        },
        updated_at: isoAgo(600)
      })
    )
    expect(isHot(w, 'east-asia')).toBe(true)
    expect(isHot(w, 'us-east')).toBe(false)
  })

  // 窗口是账号自己配的那个旋钮（后端拿同一个数判「这个网关最近烧过没有」）。
  it('窗口跟着账号的本地账本旋钮走', () => {
    const seen = {
      current: 'unified-73',
      current_region: 'east-asia',
      seen: { 'unified-73': { at: isoAgo(3 * 3600), region: 'east-asia' } },
      updated_at: isoAgo(3 * 3600)
    }
    // 默认 4 小时：3 小时前打的还算烧着。
    expect(isHot(render(account(seen)), 'east-asia')).toBe(true)
    // 旋钮调到 1 小时：同一条读数就该冷了。
    expect(isHot(render(account(seen, { openai_gwpool_gateway_window_s: 3600 })), 'east-asia')).toBe(false)
  })

  // 大区读不出来的落点（老记录、或这一发被上游改派走了）要能看见，不能悄悄消失。
  it('没有大区的落点归到未归类', () => {
    const w = render(
      account({
        current: 'unified-73',
        seen: { 'unified-73': { at: isoAgo(60) } },
        updated_at: isoAgo(60)
      })
    )
    expect(gatewayOf(w, 'unknown')).toBe('73')
  })

  // 一个大区只该有一个网关（网关 = 大区 × 账号）。真多出来说明漂移了，
  // 折成 +N 并进 tooltip，不许吞掉。
  it('同一个大区有多个落点时折成 +N', () => {
    const w = render(
      account({
        current: 'unified-73',
        current_region: 'east-asia',
        seen: {
          'unified-73': { at: isoAgo(60), region: 'east-asia' },
          'unified-99': { at: isoAgo(600), region: 'east-asia' }
        },
        updated_at: isoAgo(60)
      })
    )
    expect(gatewayOf(w, 'east-asia')).toBe('73+1')
    expect(cell(w, 'east-asia').attributes('title') ?? '').toContain('unified-99')
  })

  it('没有读数时给占位，不是整块消失', () => {
    const w = render(account(undefined))
    expect(w.find('[data-testid="account-gateway-empty"]').exists()).toBe(true)
    expect(w.find('[data-testid="account-gateway-current"]').exists()).toBe(false)
  })

  // 当前网关可能已经被裁出 seen（条目有上限），那时也得照常显示。
  it('当前网关不在 seen 里也照常显示', () => {
    const w = render(
      account({ current: 'unified-200', current_region: 'oceania', seen: {}, updated_at: isoAgo(30) })
    )
    const text = w.get('[data-testid="account-gateway-current"]').text()
    expect(text).toContain('200')
    expect(text).toContain('gatewayHistory.regions.oceania')
    expect(w.find('[data-testid="account-gateway-regions"]').exists()).toBe(false)
  })

  it('读数形状不对时安全降级为占位', () => {
    for (const bad of ['', 42, [], { seen: 'nope' }, { seen: { 'unified-1': 7 } }]) {
      const w = render(account(bad))
      expect(w.find('[data-testid="account-gateway-empty"]').exists()).toBe(true)
    }
  })

  // 状态色只有三种（2026-10-02 从四种并成三种）：绿 = 此刻真的在 183 秒满血窗口里；
  // 红 = 窗口内碰过、现在打过去就是降智；灰 = 已过本地账本窗口，可以再用。
  //
  // 原来那档琥珀（「碰过没判据」/「曾判满血但 183 秒窗口已过」）并进红色：它们在
  // 「现在能不能用」这个问题上和降智完全等价，分两色只会让人以为琥珀比红安全。
  it('窗口内只分满血与降智，窗口外一律淡显', () => {
    const w = render(
      account({
        current: 'unified-73',
        current_region: 'east-asia',
        seen: {
          'unified-73': { at: isoAgo(60), region: 'east-asia', verdict: 'full', full_at: isoAgo(60) },
          'unified-84': { at: isoAgo(120), region: 'us-west', verdict: 'degraded' },
          'unified-95': { at: isoAgo(180), region: 'us-east' },
          // 窗口外（默认 4 小时）：判定过期了，不该再按它渲染当前状态。
          'unified-200': { at: isoAgo(5 * 3600), region: 'oceania', verdict: 'full' }
        },
        updated_at: isoAgo(60)
      })
    )
    expect(tone(w, 'east-asia')).toBe('full')
    expect(tone(w, 'us-west')).toBe('degraded')
    expect(tone(w, 'us-east')).toBe('degraded') // 碰过没判据 = 窗口已经烧了
    expect(tone(w, 'oceania')).toBe('idle')
    expect(tone(w, 'europe')).toBe('idle') // 空格子

    // 色退化成装饰（9px 字号 + 红绿色盲 + title 在触屏上摸不到）时信息仍然读得出来。
    expect(markOf(w, 'east-asia')).toBe('✓ ')
    expect(markOf(w, 'us-west')).toBe('! ')
    expect(markOf(w, 'us-east')).toBe('! ')
    expect(markOf(w, 'oceania')).toBe('')

    // 判定不管窗口内外都进 tooltip：它是「验出过满血没有」唯一的记录。
    expect(cell(w, 'east-asia').attributes('title')).toContain('gatewayHistory.verdicts.full')
    expect(cell(w, 'us-west').attributes('title')).toContain('gatewayHistory.verdicts.degraded')
    expect(cell(w, 'oceania').attributes('title')).toContain('gatewayHistory.verdicts.full')
    expect(cell(w, 'us-east').attributes('title')).toContain('gatewayHistory.verdicts.none')
  })

  // 判过满血、但 183 秒窗口已经过去 ⇒ 红，不是绿也不是琥珀：窗口是 (账号 × 网关) 首次接触
  // 那一下给的，过了就没了。
  it('满血判定过了 183 秒窗口就变红', () => {
    const w = render(
      account({
        current: 'unified-73',
        seen: {
          'unified-73': { at: isoAgo(60), region: 'east-asia', verdict: 'full', full_at: isoAgo(400) }
        },
        updated_at: isoAgo(60)
      })
    )
    expect(tone(w, 'east-asia')).toBe('degraded')
    expect(cell(w, 'east-asia').attributes('title')).toContain('gatewayHistory.verdicts.full')
  })

  it('认不出的判定值按「没判过」处理', () => {
    const w = render(
      account({
        current: 'unified-73',
        seen: { 'unified-73': { at: isoAgo(60), region: 'east-asia', verdict: 'FULL' } },
        updated_at: isoAgo(60)
      })
    )
    expect(tone(w, 'east-asia')).toBe('degraded')
    expect(cell(w, 'east-asia').attributes('title')).toContain('gatewayHistory.verdicts.none')
  })

  // tooltip 固定五段：区域 · 网关名 · 满血时刻 · 状态 · 上次判定。
  // 段位固定（没有就写「从未 / 没判过」）是刻意的 —— 运营方竖着扫一列格子看，段数会变的话
  // 每一行都得重新找「满血时刻」在哪儿。
  it('tooltip 恒为五段：区域·网关·满血时刻·状态·判定', () => {
    const w = render(
      account({
        current: 'unified-73',
        seen: {
          'unified-73': { at: isoAgo(60), region: 'east-asia', verdict: 'full', full_at: isoAgo(60) },
          'unified-95': { at: isoAgo(180), region: 'us-east' }
        },
        updated_at: isoAgo(60)
      })
    )
    const judged = (cell(w, 'east-asia').attributes('title') ?? '').split(' · ')
    expect(judged).toHaveLength(5)
    expect(judged[0]).toContain('gatewayHistory.regions.east-asia')
    expect(judged[1]).toBe('unified-73')
    expect(judged[2]).toContain('gatewayHistory.fullAt')
    expect(judged[3]).toContain('gatewayHistory.regionHot')
    expect(judged[4]).toContain('gatewayHistory.verdicts.full')

    // 从没判过满血的那一格段数一样，第三段写「从未」而不是整段消失。
    const never = (cell(w, 'us-east').attributes('title') ?? '').split(' · ')
    expect(never).toHaveLength(5)
    expect(never[2]).toContain('gatewayHistory.fullNever')
    expect(never[4]).toContain('gatewayHistory.verdicts.none')
  })

  it('非 Codex 上游的账号整块不展示', () => {
    const acc = {
      id: 1,
      platform: 'anthropic',
      type: 'oauth',
      extra: {
        openai_gwpool_gateways: {
          current: 'unified-73',
          seen: { 'unified-73': { at: isoAgo(60), region: 'east-asia' } }
        }
      }
    } as unknown as Account
    expect(render(acc).find('[data-testid="account-gateway-cell"]').exists()).toBe(false)
  })
})
