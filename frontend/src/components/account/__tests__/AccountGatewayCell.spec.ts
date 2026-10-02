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

// openai_gwpool 默认开：整块烧灼读数（颜色、九宫格、预测）只对走网关池的号成立，关着的号
// 另有一条用例。放在 spread 前面，用例可以传 false 覆盖。
const account = (gateways: unknown, extra: Record<string, unknown> = {}): Account =>
  ({
    id: 1,
    platform: 'openai',
    type: 'oauth',
    extra: { openai_gwpool: true, openai_gwpool_gateways: gateways, ...extra }
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

/** 预测那行里的分钟数。t() 是桩，渲染出来是 `key:{"minutes":N}`。 */
const minutesOf = (w: ReturnType<typeof render>) => {
  const text = w.get('[data-testid="account-gateway-forecast"]').text()
  // 0 分钟那一档换成了另一句话（forecastNone，不带插值）—— 没有 `{` 就是那一档。
  // 措辞由专门那条用例钉，这里只把它折回 0，免得每个算单位数的断言都要分两种写法。
  if (!text.includes('{')) return 0
  return JSON.parse(text.slice(text.indexOf('{'), text.indexOf('}') + 1)).minutes as number
}

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
    // 被折进 +1 的那个必须在 tooltip 里单独占一行。名字和格子里一样去掉 `unified-` 前缀，
    // 所以这里认的是行首那个 `-99-`：`toContain('99')` 会被 `unified-199` 之类蒙混过去。
    expect((cell(w, 'east-asia').attributes('title') ?? '').split('\n')).toHaveLength(2)
    expect(cell(w, 'east-asia').attributes('title') ?? '').toContain('-99-')
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

  // tooltip 固定五段：区域-网关名-满血时长-状态-判定，例 `美东-149-180s-冷却中-降智`。
  //
  // 段位固定（没有就写「未计时 / 没判过」）是刻意的 —— 运营方竖着扫一列格子看，段数会变
  // 的话每一行都得重新找「满血时长」在哪儿。每段只放**值**、不带标签，同一个理由。
  //
  // 这里断言整串 toBe 而不是切开数段数：分隔符是 `-`，而 i18n 桩回的是带 `-` 的 key
  // （`regions.east-asia`），切出来的段数没有意义。整串比对连「段里混进标签」也一起钉住。
  it('tooltip 恒为五段：区域-网关-满血时长-状态-判定', () => {
    const base = 'admin.accounts.openai.gatewayHistory'
    const w = render(
      account({
        current: 'unified-73',
        seen: {
          // 窗口已经收尾（判成降智）：满血从 240 秒前持续到 60 秒前 ⇒ 180s。
          'unified-73': {
            at: isoAgo(60),
            region: 'east-asia',
            verdict: 'degraded',
            full_at: isoAgo(240)
          },
          'unified-95': { at: isoAgo(180), region: 'us-east' }
        },
        updated_at: isoAgo(60)
      })
    )
    expect(cell(w, 'east-asia').attributes('title')).toBe(
      [
        `${base}.regions.east-asia`,
        '73', // `unified-` 前缀在这一列里是恒定的，省掉才塞得下
        '180s', // 满血持续了多久，不是它发生在什么时候
        `${base}.regionHot`,
        `${base}.verdicts.degraded`
      ].join('-')
    )

    // 从没验出过满血的那一格段数一样，第三段写「未计时」而不是整段消失。
    expect(cell(w, 'us-east').attributes('title')).toBe(
      [
        `${base}.regions.us-east`,
        '95',
        `${base}.fullUntimed`,
        `${base}.regionHot`,
        `${base}.verdicts.none`
      ].join('-')
    )
  })

  // 没开「Codex 路由 cookie 由网关池下发」的号：只显示落点本身，整块烧灼读数都不出现。
  //
  // 这条是用户 2026-10-03 当场指出来的误导：那种号的 state-echo 判据压根不跑 ⇒ verdict 恒为空
  // ⇒ toneOf 的兜底把**每一个**最近用过的落点都染成红的（「现在打就是降智」），而它根本不选
  // 落点、也没有冷却这回事。红色读起来像「这个号废了」。
  it('没开网关池的号不套烧灼读数：不染色、没有九宫格/预测/图例', () => {
    const w = render(
      account(
        {
          current: 'unified-121',
          current_region: 'east-asia',
          seen: { 'unified-121': { at: isoAgo(60), region: 'east-asia' } },
          updated_at: isoAgo(60)
        },
        { openai_gwpool: false }
      )
    )
    // 落点本身仍然要显示 —— 它是真的，只是不该按烧灼去读。
    const current = w.get('[data-testid="account-gateway-current"]')
    expect(current.text()).toContain('unified-121')
    // 判定字符和红色都不许出现。
    expect(current.text()).not.toContain('!')
    expect(current.text()).not.toContain('✓')
    for (const id of ['regions', 'forecast', 'legend', 'window-usage']) {
      expect(w.find(`[data-testid="account-gateway-${id}"]`).exists(), id).toBe(false)
    }
  })

  // 窗口用量：已用来自本地账本（窗口内碰过的），**分母是池子报的可交付网关数**。
  // 拿账本条目数当分母是错的：账本只装碰过的，那样算出来的「还剩多少没用」恒等于
  // 「我碰过但已经凉了的」，答的是另一个问题。
  it('窗口用量用池子的可交付网关数当分母', () => {
    const w = render(
      account({
        current: 'unified-1',
        seen: {
          'unified-1': { at: isoAgo(60), region: 'us-east' }, // 窗口内 ⇒ 已用
          'unified-2': { at: isoAgo(120), region: 'us-west' }, // 窗口内 ⇒ 已用
          'unified-3': { at: isoAgo(5 * 3600), region: 'europe' } // 4h 窗口外 ⇒ 不算已用
        },
        pool_live: 50,
        updated_at: isoAgo(60)
      })
    )
    const text = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(text).toContain('gatewayHistory.windowUsage:')
    expect(JSON.parse(text.slice(text.indexOf('{')))).toEqual({ hours: 4, used: 2, free: 48 })
  })

  // 问不到池子清单（没开 steering / 列表打不开 ⇒ pool_live 缺省）时只报已用那一半。
  // 退回「账本条目数」当分母是编数据，比不报更坏。
  it('拿不到池子清单时只报已用，不编一个分母', () => {
    const w = render(
      account({
        current: 'unified-1',
        seen: { 'unified-1': { at: isoAgo(60), region: 'us-east' } },
        updated_at: isoAgo(60)
      })
    )
    const text = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(text).toContain('gatewayHistory.windowUsageUsedOnly:')
    expect(JSON.parse(text.slice(text.indexOf('{')))).toEqual({ hours: 4, used: 1 })
  })

  // 账本是窗口期的、清单是此刻的 ⇒ 账本里的落点可能已经不在清单上（票过期）⇒ 差值可能
  // 为负。夹到 0：报一个负数等于说谎。
  it('已用多于池子清单长度时剩余夹到 0，不报负数', () => {
    const seen = Object.fromEntries(
      Array.from({ length: 6 }, (_, i) => [`unified-${i}`, { at: isoAgo(60), region: 'us-east' }])
    )
    const w = render(account({ current: 'unified-0', seen, pool_live: 2, updated_at: isoAgo(60) }))
    const text = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(JSON.parse(text.slice(text.indexOf('{')))).toEqual({ hours: 4, used: 6, free: 0 })
  })

  // 0 的时候不能渲染成「至少 0 分钟满血」：那读起来像对这个号的判决，而它说的是
  // 「账本里每个落点的冷却都要一小时之后才结束」——一个关于时间的事实。
  it('一小时内没有落点出冷却时换一句话，不写「至少 0 分钟」', () => {
    const w = render(
      account({
        current: 'unified-1',
        // 刚碰过 ⇒ 冷却还剩约 4 小时 ⇒ 一小时内出不了冷却 ⇒ 预测为 0。
        seen: { 'unified-1': { at: isoAgo(30), region: 'us-east' } },
        updated_at: isoAgo(30)
      })
    )
    const text = w.get('[data-testid="account-gateway-forecast"]').text()
    expect(text).toContain('gatewayHistory.forecastNone')
    expect(text).not.toContain('gatewayHistory.forecast:')
  })

  // 窗口还在跑的时候**不许**报时长：这时候算出来的是「到目前为止」，而它会被当成
  // 「这个落点只给了这么多」。判成降智那一刻才有收尾时刻，才算得出长度。
  it('满血窗口没收尾时第三段写「未计时」，不报一个半截的数', () => {
    const base = 'admin.accounts.openai.gatewayHistory'
    const w = render(
      account({
        current: 'unified-73',
        seen: {
          // 仍判满血 = 窗口正在跑。
          'unified-73': { at: isoAgo(10), region: 'east-asia', verdict: 'full', full_at: isoAgo(70) },
          // 判了降智但从没验出过满血：没有起点，同样算不出长度。
          'unified-95': { at: isoAgo(30), region: 'us-east', verdict: 'degraded' }
        },
        updated_at: isoAgo(10)
      })
    )
    expect(cell(w, 'east-asia').attributes('title')).toContain(`-${base}.fullUntimed-`)
    expect(cell(w, 'us-east').attributes('title')).toContain(`-${base}.fullUntimed-`)
  })

  // 满血分钟预测：单位是 (账号 × 网关)，**一个网关名就是一个单位**，而且算**下界**。
  //
  // 第一版按大区去重（「一个号在一个大区同一时间只有一个网关」），2026-10-03 用户否了：
  // 「时间还是按网关来的，相同区域不同网关同一个号还是有不同的满血期的」。按大区数会把
  // us-west 那 20 个网关名算成 1 个单位，预测值低一个数量级。
  it('满血分钟预测按网关名数单位，同一大区的多个网关各算一个', () => {
    // 三个网关名同属 us-west，全部已出冷却 ⇒ **3** 个单位，不是 1 个。
    // 这一条就是那次纠正本身，按大区并会让它掉回 1。
    const oneRegion = render(
      account({
        current: 'unified-1',
        seen: {
          'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' },
          'unified-2': { at: isoAgo(6 * 3600), region: 'us-west' },
          'unified-3': { at: isoAgo(7 * 3600), region: 'us-west' }
        }
      })
    )
    expect(minutesOf(oneRegion)).toBe(Math.round((3 * 183) / 60))

    // 同一大区里新旧混着时**各算各的**：旧的那个已恢复、新的那个还在烧 ⇒ 1 个单位。
    // 按大区取「最近那次」当起点会让它变成 0。
    const staleAndFresh = render(
      account({
        current: 'unified-2',
        seen: {
          'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' },
          'unified-2': { at: isoAgo(60), region: 'us-west' }
        }
      })
    )
    expect(minutesOf(staleAndFresh)).toBe(Math.round(183 / 60))

    // 全部刚烧过 ⇒ 一小时内一个都出不来 ⇒ 0 分钟。
    const allBurned = Object.fromEntries(
      Array.from({ length: 9 }, (_, i) => [`unified-${i}`, { at: isoAgo(60), region: 'us-west' }])
    )
    expect(minutesOf(render(account({ current: 'unified-0', seen: allBurned })))).toBe(0)

    // 封顶一小时：一小时里最多只能用一小时的满血，25 个单位 × 183 秒远超它。
    const many = Object.fromEntries(
      Array.from({ length: 25 }, (_, i) => [
        `unified-${i}`,
        { at: isoAgo(5 * 3600), region: 'us-west' }
      ])
    )
    expect(minutesOf(render(account({ current: 'unified-0', seen: many })))).toBe(60)

    // 冷却剩余 ≤ 1 小时就算可用：4 小时窗口下，3.5 小时前烧的那个算回来。
    const recovering = render(
      account({
        current: 'unified-1',
        seen: { 'unified-1': { at: isoAgo(3.5 * 3600), region: 'us-west' } }
      })
    )
    expect(minutesOf(recovering)).toBe(Math.round(183 / 60))
  })

  // region 完全不参与计数：没带 region 的落点一样是一个有名有姓的网关，照数。
  //
  // 老版本有两条按大区的规则，都跟着删了：「没摸过的大区当上行空间提示」（给不出数 ——
  // 这一行不知道池子一共有多少网关）和「未归类落点从可用数里扣掉」（它本来是为了补
  // 按大区归类的漏，按网关数之后没有漏可补）。
  it('没带大区的落点照样算一个单位，不扣减也不另算', () => {
    const w = render(
      account({
        current: 'unified-1',
        seen: {
          'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' },
          'unified-2': { at: isoAgo(5 * 3600), region: 'europe' },
          'unified-9': { at: isoAgo(5 * 3600) } // 读不出大区，但出了冷却
        }
      })
    )
    expect(minutesOf(w)).toBe(Math.round((3 * 183) / 60))

    // 它还在窗口里的时候只是「这一个单位不可用」，不该再去扣别人。
    const hotBlind = render(
      account({
        current: 'unified-1',
        seen: {
          'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' },
          'unified-2': { at: isoAgo(5 * 3600), region: 'europe' },
          'unified-9': { at: isoAgo(60) }
        }
      })
    )
    expect(minutesOf(hotBlind)).toBe(Math.round((2 * 183) / 60))

    // 那两条按大区的提示文案已经没了，页面上不该再出现它们。
    const text = w.get('[data-testid="account-gateway-forecast"]').text()
    expect(text).not.toContain('forecastUntouched')
    expect(text).not.toContain('forecastBlind')
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
