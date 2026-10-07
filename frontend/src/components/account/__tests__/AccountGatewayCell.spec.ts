import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountGatewayCell from '../AccountGatewayCell.vue'
import type { Account } from '@/types'
import { gatewayRegionDisplayKey } from '@/utils/gatewayRegionDisplay'

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
    // 既有展示回归显式使用 4h；缺省 1h 与逐网关学习由独立用例验证。
    extra: { openai_gwpool: true, openai_gwpool_gateway_window_s: 14400, openai_gwpool_gateways: gateways, ...extra }
  }) as unknown as Account

const render = (acc: Account) => mount(AccountGatewayCell, { props: { account: acc } })

it('手动重试只发出账号动作，清冷却墓碑不被最近历史染红', async () => {
  const w = render(account({ current: 'g', seen: {
    g: { at: isoAgo(5), region: 'east-asia', cooldown: { cleared: true, until: isoAgo(2), window_seconds: 3600 } }
  } }))
  expect(w.get('[data-testid="account-gateway-region-east-asia"]').attributes('data-tone')).toBe('idle')
  const retry = w.get('[data-testid="account-gateway-retry"]')
  expect(retry.element.parentElement?.classList.contains('justify-end')).toBe(true)
  expect(retry.element.parentElement).toBe(w.get('[data-testid="account-gateway-window-usage"]').element.parentElement)
  expect(retry.classes()).toContain('border')
  expect(retry.get('svg').attributes('aria-hidden')).toBe('true')
  expect(retry.text()).toBe('admin.accounts.openai.gwpoolManualRetry')
  await retry.trigger('click')
  expect(w.emitted('retry')).toEqual([[1]])
  await w.setProps({ retryPending: true })
  expect(w.get('[data-testid="account-gateway-retry"]').attributes('disabled')).toBeDefined()
  expect(retry.attributes('aria-busy')).toBe('true')
  expect(retry.text()).toBe('admin.accounts.openai.gwpoolManualRetryPending')
})

it('完整快照覆盖陈旧落点但不修改编辑账号，失焦保留数字且撤绿冻结', async () => {
  vi.useFakeTimers()
  const at = new Date().toISOString()
  const acc = account({ current: 'old', seen: { old: { at, region: 'east-asia' } } })
  const wrapper = mount(AccountGatewayCell, { props: {
    account: acc,
    progress: {
      phase: 'ready', attempt: 1, limit: 0, rejected: 0, elapsed_ms: 1000,
      active_requests: 0, started_at: at, updated_at: at,
      runtime: {
        observed_at: at,
        history: { current: 'new', seen: { new: { at, region: 'east-asia' } } },
        tickets: [{ gateway: 'new', region: 'east-asia', verified_at: at, verified_models: ['gpt-6-luna'] }],
        rounds: [{ id: 'round', model: 'all', started_at: at, full: 1, attempted: 1, full_duration_ms: 61000 }],
        archived: null
      }
    }
  } })
  expect(wrapper.get('[data-testid="account-gateway-current"]').text()).toContain('new')
  expect((acc.extra?.openai_gwpool_gateways as { current: string }).current).toBe('old')
  expect(wrapper.get('[data-testid="account-gateway-region-east-asia"]').attributes('data-tone')).toBe('full')
  const duration = wrapper.get('[data-testid="account-gateway-usage-round"]').text()
  await wrapper.setProps({ progressUnavailable: true })
  await vi.advanceTimersByTimeAsync(20_000)
  expect(wrapper.get('[data-testid="account-gateway-usage-round"]').text()).toBe(duration)
  expect(wrapper.get('[data-testid="account-gateway-region-east-asia"]').attributes('data-tone')).toBe('idle')
  wrapper.unmount()
  vi.useRealTimers()
})

it('网关落点在没有历史时也显示真实验证进度，失败不继续假装寻找', async () => {
  const wrapper = mount(AccountGatewayCell, { props: { account: account(undefined), progress: {
    phase: 'verifying', attempt: 2, limit: 5, rejected: 1, elapsed_ms: 8200,
    started_at: new Date().toISOString(), updated_at: new Date().toISOString(), active_requests: 1
  } } })
  expect(wrapper.get('[data-testid="account-gateway-progress"]').text()).toContain('"attempt":2,"seconds":8')
  expect(wrapper.get('[data-testid="account-gateway-progress"]').classes()).toContain('whitespace-normal')
  expect(wrapper.get('[data-testid="account-gateway-progress"]').classes()).toContain('break-words')
  await wrapper.setProps({ progress: undefined, progressUnavailable: true })
  expect(wrapper.find('[data-testid="account-gateway-progress"]').exists()).toBe(true)
  expect(wrapper.find('[data-testid="account-gateway-progress-unavailable"]').exists()).toBe(false)
  expect(wrapper.get('[data-testid="account-gateway-cell"]').classes()).toContain('w-[260px]')
  wrapper.unmount()
})

it('验证编号使用周期内sequence，结束隐藏旧进度，新周期从1开始', async () => {
  const progress = {
    run_id: '150', sequence: 3, phase: 'verifying' as const,
    attempt: 2, limit: 8, rejected: 1, elapsed_ms: 8200,
    started_at: new Date().toISOString(), updated_at: new Date().toISOString(), active_requests: 1
  }
  const wrapper = mount(AccountGatewayCell, { props: { account: account(undefined), progress } })
  expect(wrapper.get('[data-testid="account-gateway-progress"]').text()).toContain('run:{"id":3}')
  expect(wrapper.get('[data-testid="account-gateway-progress"]').text()).not.toContain('150')
  await wrapper.setProps({ progress: {
    ...progress, run_id: '', sequence: 0, phase: 'idle',
    attempt: 0, limit: 0, rejected: 0, elapsed_ms: 0, active_requests: 0
  } })
  const idle = wrapper.get('[data-testid="account-gateway-progress"]').text()
  expect(idle).not.toContain('run:')
  expect(idle).not.toContain('"attempt"')
  expect(idle).not.toContain('rejected')
  await wrapper.setProps({ progress: { ...progress, run_id: '151', sequence: 1 } })
  expect(wrapper.get('[data-testid="account-gateway-progress"]').text()).toContain('run:{"id":1}')
  wrapper.unmount()
})

it('失联仍保留已观测时长，归档同排展示且不混入旧模型墙钟', () => {
  const wrapper = mount(AccountGatewayCell, { props: {
    account: account(undefined), progressUnavailable: true,
    progress: {
      phase: 'idle', attempt: 0, limit: 8, rejected: 0, elapsed_ms: 0,
      started_at: '', updated_at: '', active_requests: 0,
      runtime: {
        observed_at: new Date().toISOString(), tickets: [],
        rounds: [
          { id: 'active', model: 'all', started_at: isoAgo(1000), full: 7, attempted: 9, full_duration_ms: 61000, full_active_until: ['0001-01-01T00:00:00Z'] },
          { id: 'ended', model: 'all', started_at: isoAgo(3000), ended_at: isoAgo(2000), full: 2, attempted: 5, incomplete: true }
        ],
        archived: {
          all: { rounds: 1, full: 3, attempted: 6, duration_ms: 1000, incomplete: true },
          'gpt-6-luna': { rounds: 3, full: 19, attempted: 71, duration_ms: 3418000 }
        },
        incomplete: true
      }
    }
  } })
  const current = wrapper.get('[data-testid="account-gateway-usage-round"]').text()
  expect(current).toContain('"full":7,"attempted":9')
  expect(current).toContain('\\"minutes\\":1,\\"seconds\\":1')
  const history = wrapper.get('[data-testid="account-gateway-usage-history"]').text()
  expect(history).toContain('"count":2')
  expect(history).not.toContain('legacyArchived')
  expect(history).not.toContain('3418')
  expect(history).not.toContain('gatewayRuntime.incomplete')
  expect(history).not.toContain('gatewayRuntime.durationIncomplete')
  wrapper.unmount()
})

// Deliberately not 183s or 100%: 3/5 full × 120s = 72 expected seconds per gateway.
function accountWithSamples(history: { current: string; seen: Record<string, { at: string; region?: string }> }): Account {
  const rounds = [5 * 3600, 7 * 3600].flatMap((gap, bucket) => Array.from({ length: 5 }, (_, index) => ({
    report: {
      id: `sample-${bucket}-${index}`, gateway: 'unified-sample', model: 'gpt-6-astra',
      criterion: 'state-echo-v1', source: 'foreground', first: 'repeat', at: isoAgo(8 * 3600 + index),
      gap_known: true, elapsed_seconds: gap, outcome: index < 3 ? 'full' : 'refreshed',
      window_final: index < 3, full_window_ms: index < 3 ? 120000 : 0
    }
  })))
  return account(history, {
    openai_gwpool_ledger_tag: 'same-ledger',
    openai_gwpool_contacts: {
      ledger_tag: 'same-ledger',
      seen: Object.fromEntries(Object.entries(history.seen).map(([name, row]) =>
        [name, { first_at: isoAgo(24 * 3600), last_at: row.at }])),
      rounds
    }
  })
}

const cell = (w: ReturnType<typeof render>, region: string) =>
  w.get(`[data-testid="account-gateway-region-${gatewayRegionDisplayKey(region) || 'unknown'}"]`)

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
  it('诊断不展示，国家城市合入固定九大区', () => {
    const w = render(account({
      seen: {
        'unified-141': { at: isoAgo(30), region: 'de-central' },
        'unified-42': { at: isoAgo(60), region: 'southeast-asia-sg' },
        'unified-43': { at: isoAgo(90), region: 'country-fr' }
      }
    }, { openai_gwpool_metrics: { foreground: { requests: 10 } } }))
    expect(w.findAll('[data-testid^="account-gateway-region-"]')).toHaveLength(9)
    expect(cell(w, 'europe').text()).toContain('gatewayHistory.regions.europe')
    expect(cell(w, 'southeast-asia').text()).toContain('gatewayHistory.regions.southeast-asia')
    expect(cell(w, 'west-europe').text()).toContain('gatewayHistory.regions.europe')
    expect(w.find('[data-testid="account-gateway-diagnostics"]').exists()).toBe(false)
    expect(w.find('[data-testid="account-gateway-runtime"]').exists()).toBe(false)
  })
  it('无样本时不展示满血预测、图例、候选快照或详情按钮', () => {
    const w = render(account({
      current: 'unified-1',
      seen: { 'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' } }
    }))
    for (const id of ['forecast', 'legend', 'pool-snapshot']) {
      expect(w.find(`[data-testid="account-gateway-${id}"]`).exists()).toBe(false)
    }
    expect(w.find('details').exists()).toBe(false)
  })
  it('冷却完毕独立于旧库存快照，非法时间不冒充冷却完成', () => {
    const w = render(account({
      seen: {
        'unified-1': { at: isoAgo(60), region: 'us-east' },
        'unified-2': { at: isoAgo(5 * 3600), region: 'us-west' },
        'unified-3': { at: isoAgo(6 * 3600), region: 'europe' },
        'unified-4': { at: 'invalid', region: 'east-asia' }
      },
      pool_live: 98,
      pool_free: 1
    }))
    const summary = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(JSON.parse(summary.slice(summary.indexOf('{')))).toEqual({ used: 1, cooled: 2 })
    expect(w.find('[data-testid="account-gateway-pool-snapshot"]').exists()).toBe(false)
  })
  it('验证统计与反馈保留在数据中但常规界面不展示', () => {
    const w = render(account(null, {
      openai_gwpool_metrics: {
        foreground: { rounds: 3, requests: 6, full: 1, degraded: 1, inconclusive: 1, duration_ms: 1500 },
        background: { rounds: 1, requests: 1, inconclusive: 1, duration_ms: 300 }
      },
      openai_gwpool_feedback_outbox: {
        sent: 4, discarded: 2, pending: [{ report: { account_tag: 'must-not-render' } }, { permanent: true }]
      }
    }))
    expect(w.find('[data-testid="account-gateway-runtime"]').exists()).toBe(false)
    expect(w.find('[data-testid="account-gateway-feedback"]').exists()).toBe(false)
    expect(w.text()).not.toContain('must-not-render')
  })
  it('新出口只在提示里保留稳定ID，不扩增国家城市格子', () => {
    const w = render(account({ seen: { 'unified-99': { at: isoAgo(30), region: 'africa-south' } } }))
    expect(cell(w, 'africa').attributes('title')).toContain('africa-south')
    expect(gatewayOf(w, 'africa')).toBe('99')
    expect(w.find('[data-testid="account-gateway-region-unknown"]').exists()).toBe(false)
  })
  it('同大区多国合并网关格，但冷却数量仍按网关独立统计', () => {
    const w = render(account({
      seen: {
        'unified-141': { at: isoAgo(10), region: 'de-central' },
        'unified-142': { at: isoAgo(5 * 3600), region: 'country-at' },
        'unified-143': { at: isoAgo(6 * 3600), region: 'europe' },
        'unified-144': { at: isoAgo(7 * 3600), region: 'custom-unknown-country' }
      }
    }))
    expect(w.findAll('[data-testid^="account-gateway-region-"]')).toHaveLength(10)
    expect(gatewayOf(w, 'europe')).toBe('141+2')
    expect(cell(w, 'europe').attributes('title')).toContain('de-central')
    expect(cell(w, 'europe').attributes('title')).toContain('country-at')
    const summary = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(JSON.parse(summary.slice(summary.indexOf('{')))).toEqual({ used: 1, cooled: 3 })
  })
  it('美国与西雅图共用北美格，当前落点和区域提示都保留具体出口', () => {
    const w = render(account({
      current: 'unified-141',
      seen: {
        'unified-141': { at: isoAgo(10), region: 'us-west' },
        'unified-142': { at: isoAgo(60), region: 'us-east' },
        'unified-143': { at: isoAgo(90), region: 'country-us' }
      }
    }))
    expect(w.findAll('[data-testid^="account-gateway-region-"]')).toHaveLength(9)
    expect(w.get('[data-testid="account-gateway-region-north-america"]').text()).toContain('141+2')
    expect(w.get('[data-testid="account-gateway-current"] span[title]').attributes('title')).toContain('us-west')
    expect(cell(w, 'north-america').attributes('title')).toContain('country-us')
  })
  it('缺省冷却为1小时，学习档位和固定状态按网关分别显示', () => {
    const w = render(account({
      current: 'unified-73',
      seen: {
        'unified-73': { at: isoAgo(61 * 60), region: 'east-asia' },
        'unified-142': {
          at: isoAgo(60), region: 'us-east', full_held_ms: 75000,
          cooldown: { until: isoAgo(-119 * 60), window_seconds: 7200, fixed_seconds: 7200 }
        }
      }
    }, { openai_gwpool_gateway_window_s: undefined }))
    expect(tone(w, 'east-asia')).toBe('idle')
    expect(tone(w, 'us-east')).toBe('degraded')
    const title = cell(w, 'us-east').attributes('title') ?? ''
    expect(title).toContain('-75s-')
    expect(title).toContain('regionHot:{"minutes":119}')
    expect(title).toContain('cooldownFixed:{"minutes":120}')
  })
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
  it('按九大区摊开，未接触大区不冒充有可用网关', () => {
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
    expect(cell(w, 'europe').text()).toContain('-')
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

  it('没有真实活票快照时历史满血也不染绿，冷却外淡显', () => {
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
    expect(tone(w, 'east-asia')).toBe('degraded')
    expect(tone(w, 'us-west')).toBe('degraded')
    expect(tone(w, 'us-east')).toBe('degraded') // 碰过没判据 = 窗口已经烧了
    expect(tone(w, 'oceania')).toBe('idle')
    expect(cell(w, 'europe').text()).toContain('-')

    // 色退化成装饰（9px 字号 + 红绿色盲 + title 在触屏上摸不到）时信息仍然读得出来。
    expect(markOf(w, 'east-asia')).toBe('! ')
    expect(markOf(w, 'us-west')).toBe('! ')
    expect(markOf(w, 'us-east')).toBe('! ')
    expect(markOf(w, 'oceania')).toBe('')

    // 判定不管窗口内外都进 tooltip：它是「验出过满血没有」唯一的记录。
    expect(cell(w, 'east-asia').attributes('title')).toContain('gatewayHistory.verdicts.full')
    expect(cell(w, 'us-west').attributes('title')).toContain('gatewayHistory.verdicts.degraded')
    expect(cell(w, 'oceania').attributes('title')).toContain('gatewayHistory.verdicts.full')
    expect(cell(w, 'us-east').attributes('title')).toContain('gatewayHistory.verdicts.none')
  })

  it('旧满血只保留为历史，不用固定183秒推算当前票', () => {
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
    expect(cell(w, 'east-asia').attributes('title')).toContain('gatewayHistory.verdicts.fullExpired')
    expect(w.get('[data-testid="account-gateway-current"] span[title]').attributes('title')).toContain('gatewayHistory.verdicts.fullExpired')
  })

  it('活票按真实租约及已验模型显示，快照失效即撤绿', async () => {
    const progress = {
      phase: 'idle' as const, attempt: 0, limit: 0, rejected: 0, elapsed_ms: 0,
      started_at: '', updated_at: '', active_requests: 0,
      runtime: {
        observed_at: new Date().toISOString(),
        tickets: [{ gateway: 'unified-73', region: 'east-asia', expires_at: new Date(Date.now() + 240_000).toISOString(), verified_models: ['gpt-6-luna'] }],
        rounds: [{ id: 'round', model: 'all', started_at: isoAgo(240), attempted: 8, full: 3, full_duration_ms: 65000 }],
        archived: { 'gpt-6-luna': { rounds: 2, attempted: 20, full: 5, duration_ms: 200_000 } }
      }
    }
    const w = mount(AccountGatewayCell, { props: {
      account: account({ current: 'unified-73', seen: { 'unified-73': { at: isoAgo(300), region: 'east-asia', verdict: 'full', full_at: isoAgo(300) } } }),
      progress
    } })
    expect(tone(w, 'east-asia')).toBe('full')
    expect(w.get('[data-testid="account-gateway-current"]').text()).not.toContain('gpt-6-luna')
    expect(w.get('[data-testid="account-gateway-usage-round"]').text()).toContain('"full":3,"attempted":8')
    expect(w.find('[data-testid="account-gateway-progress"]').exists()).toBe(true)
    await w.setProps({ progress: { ...progress, runtime: { ...progress.runtime, observed_at: isoAgo(10) } } })
    expect(tone(w, 'east-asia')).toBe('idle')
    await w.setProps({ progress, progressUnavailable: true })
    expect(w.find('[data-testid="account-gateway-live"]').exists()).toBe(false)
    expect(tone(w, 'east-asia')).toBe('idle')
    w.unmount()
  })

  it('零时间不显示远古天数，满血使用读逐票累计而非轮次墙钟', () => {
    const w = mount(AccountGatewayCell, { props: {
      account: account({ current: 'unified-178', updated_at: '0001-01-01T00:00:00Z' }),
      progress: {
        phase: 'idle', attempt: 0, limit: 8, rejected: 0, elapsed_ms: 0,
        started_at: '', updated_at: '', active_requests: 0,
        runtime: { observed_at: new Date().toISOString(), tickets: [],
          rounds: [{ id: 'r', model: 'all', started_at: isoAgo(3418), attempted: 71, full: 20, full_duration_ms: 1202000 }],
          archived: null }
      }
    } })
    expect(w.get('[data-testid="account-gateway-current-time"]').text()).toBe('—')
    const usage = w.get('[data-testid="account-gateway-usage-round"]').text()
    expect(usage).toContain('\\"minutes\\":20,\\"seconds\\":2')
    expect(usage).not.toContain('3418')
    expect(w.text()).not.toContain('gatewayRuntime.hint')
    w.unmount()
  })

  it('周期结束后当前计数时长归零，新周期不累计上一周期', async () => {
    const runtime = { observed_at: new Date().toISOString(), tickets: [],
      rounds: [{ id: 'old', model: 'all', started_at: isoAgo(3600), ended_at: isoAgo(1800),
        attempted: 8, full: 3, full_duration_ms: 65000 }], archived: null }
    const progress = { phase: 'idle' as const, attempt: 0, limit: 5, rejected: 0, elapsed_ms: 0,
      started_at: '', updated_at: '', active_requests: 0, runtime }
    const w = mount(AccountGatewayCell, { props: { account: account({}), progress } })
    expect(w.find('[data-testid="account-gateway-usage-round"]').exists()).toBe(false)
    const empty = w.get('[data-testid="account-gateway-usage-idle"]').text()
    expect(empty).toContain('"full":0,"attempted":0')
    expect(empty).toContain('\\"minutes\\":0,\\"seconds\\":0')
    const history = w.get('[data-testid="account-gateway-usage-history"]').text()
    expect(history).toContain('"count":1')
    expect(history).toContain('\\"minutes\\":1,\\"seconds\\":5')
    await w.setProps({ progress: { ...progress, runtime: { ...runtime,
      rounds: [...runtime.rounds, { id: 'new', model: 'all', started_at: isoAgo(30), ended_at: '',
        attempted: 1, full: 1, full_duration_ms: 5000 }] } } })
    expect(w.find('[data-testid="account-gateway-usage-idle"]').exists()).toBe(false)
    const next = w.get('[data-testid="account-gateway-usage-round"]').text()
    expect(next).toContain('"full":1,"attempted":1')
    expect(next).toContain('\\"minutes\\":0,\\"seconds\\":5')
    expect(w.get('[data-testid="account-gateway-usage-history"]').text()).toBe(history)
    w.unmount()
  })

  it('历史累计包含已结束明细，排除当前轮，压缩归档后保持总数不变', async () => {
    const active = { id: 'active', model: 'all', started_at: isoAgo(300), attempted: 1,
      full: 1, full_duration_ms: 5000 }
    const ended = { id: 'ended', model: 'all', started_at: isoAgo(3600), ended_at: isoAgo(1800),
      attempted: 8, full: 3, full_duration_ms: 65000 }
    const runtime = { observed_at: new Date().toISOString(), tickets: [],
      rounds: [active, ended],
      archived: { all: { rounds: 2, attempted: 10, full: 4, duration_ms: 95000 } } }
    const progress = { phase: 'idle' as const, attempt: 0, limit: 5, rejected: 0, elapsed_ms: 0,
      started_at: '', updated_at: '', active_requests: 0, runtime }
    const w = mount(AccountGatewayCell, { props: { account: account({}), progress } })
    const history = () => w.get('[data-testid="account-gateway-usage-history"]').text()
    expect(history()).toContain('"count":3')
    expect(history()).toContain('\\"minutes\\":2,\\"seconds\\":40')
    const before = history()
    await w.setProps({ progress: { ...progress, runtime: { ...runtime, rounds: [active],
      archived: { all: { rounds: 3, attempted: 18, full: 7, duration_ms: 160000 } } } } })
    expect(history()).toBe(before)
    expect(w.get('[data-testid="account-gateway-usage-round"]').text()).toContain('\\"seconds\\":5')
    w.unmount()
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
          // 后端量到的满血时长照原样显示，不在前端拿时刻相减。
          'unified-73': {
            at: isoAgo(60),
            region: 'east-asia',
            verdict: 'degraded',
            full_at: isoAgo(240),
            full_held_ms: 180_000
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
        // 4 小时窗口减掉「1 分钟前用过」再向上取整 ⇒ 239。钉死这个数同时钉住两件事：
        // 报 240 是忘了减，报 238 是向下取整（那会让「还剩 0 分钟」读成「已经好了」）。
        `${base}.regionHot:{"minutes":239}`,
        `${base}.verdicts.degraded`
      ].join('-')
    )

    // 从没验出过满血的那一格段数一样，第三段写「未计时」而不是整段消失。
    expect(cell(w, 'us-east').attributes('title')).toBe(
      [
        `${base}.regions.north-america`,
        '95',
        `${base}.fullUntimed`,
        `${base}.regionHot:{"minutes":237}`,
        `${base}.verdicts.none`
      ].join('-') + ' · us-east'
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

  // 窗口用量三个数**各报各的，不做减法**。
  //
  // 第一版写的是 `pool_live - used` 再夹到 0：账本装的是过去一个窗口碰过的网关名（票早
  // 过期的也在），pool_live 是此刻还有活票的，两个集合不是包含关系。现网当场撞上 ——
  // 账本 67、可交付 62 ⇒ 渲染成「池子里还剩 0 个没用」，而池子好好的有 62 个。
  it('窗口用量三个数各报各的，不在前端做减法', () => {
    const w = render(
      account({
        current: 'unified-1',
        seen: {
          'unified-1': { at: isoAgo(60), region: 'us-east' }, // 窗口内 ⇒ 已用
          'unified-2': { at: isoAgo(120), region: 'us-west' }, // 窗口内 ⇒ 已用
          'unified-3': { at: isoAgo(5 * 3600), region: 'europe' } // 4h 窗口外 ⇒ 不算已用
        },
        pool_live: 62,
        pool_free: 5,
        updated_at: isoAgo(60)
      })
    )
    const text = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(text).toContain('gatewayHistory.windowUsage:')
    expect(JSON.parse(text.slice(text.indexOf('{')))).toEqual({ used: 2, cooled: 1 })
    expect(w.find('[data-testid="account-gateway-pool-snapshot"]').exists()).toBe(false)
  })

  // 已用多于可交付是**正常的**（账本跨一个窗口、清单是此刻的快照），不许因此把「没烧过」
  // 算成 0 —— 那正是现网报错的那一幕。free 来自后端，照原样显示。
  it('历史多于可交付时仍独立统计冷却，不展示旧候选数', () => {
    const seen = Object.fromEntries(
      Array.from({ length: 67 }, (_, i) => [`unified-${i}`, { at: isoAgo(60), region: 'us-east' }])
    )
    const w = render(
      account({ current: 'unified-0', seen, pool_live: 62, pool_free: 7, updated_at: isoAgo(60) })
    )
    const text = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(JSON.parse(text.slice(text.indexOf('{')))).toEqual({ used: 67, cooled: 0 })
    expect(w.find('[data-testid="account-gateway-pool-snapshot"]').exists()).toBe(false)
  })

  // 问不到池子清单（没开 steering / 列表打不开 ⇒ pool_live 缺省）时只报已用那一半。
  it('拿不到池子清单时只报已用，不编一个分母', () => {
    const w = render(
      account({
        current: 'unified-1',
        seen: { 'unified-1': { at: isoAgo(60), region: 'us-east' } },
        updated_at: isoAgo(60)
      })
    )
    const text = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(text).toContain('gatewayHistory.windowUsage:')
    expect(JSON.parse(text.slice(text.indexOf('{')))).toEqual({ used: 1, cooled: 0 })
    expect(w.find('[data-testid="account-gateway-pool-snapshot"]').exists()).toBe(false)
  })

  // 旧版本（klno.3 及更早）写下的记录有 pool_live、没有 pool_free。两个字段都在才算测到 ——
  // 只看 live 的话缺席会被当成 0，渲染出「可交付 61 个，其中 0 个没烧过」，正是这次要修的
  // 那句假话换了个来源。后端那边 pool_free 刻意不带 omitempty，真的 0 一定在 JSON 里。
  it('旧记录只有 pool_live 没有 pool_free 时退回「只报已用」', () => {
    const w = render(
      account({
        current: 'unified-1',
        seen: { 'unified-1': { at: isoAgo(60), region: 'us-east' } },
        pool_live: 61,
        updated_at: isoAgo(60)
      })
    )
    const text = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(text).toContain('gatewayHistory.windowUsage:')
    expect(w.find('[data-testid="account-gateway-pool-snapshot"]').exists()).toBe(false)
  })

  // pool_live>0 时 free=0 是**真的 0**（可交付的全烧过了），要和「没问到清单」分开。
  it('可交付的全烧过时报 0，不退回「只报已用」', () => {
    const w = render(
      account({
        current: 'unified-1',
        seen: { 'unified-1': { at: isoAgo(60), region: 'us-east' } },
        pool_live: 62,
        pool_free: 0,
        updated_at: isoAgo(60)
      })
    )
    const text = w.get('[data-testid="account-gateway-window-usage"]').text()
    expect(text).toContain('gatewayHistory.windowUsage:')
    expect(w.find('[data-testid="account-gateway-pool-snapshot"]').exists()).toBe(false)
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
    expect(w.find('[data-testid="account-gateway-forecast"]').exists()).toBe(false)
  })

  // 没有 full_held_ms 就写「未计时」，**绝不拿 at − full_at 顶上**。
  //
  // 第一版就是那个减法：full_at 是粘滞的，而 degraded 判定要等下一次真的打到这个网关才会
  // 写，中间空几个小时就白算几个小时 —— 现网渲染出了 22655s / 21738s（满血窗口才 183 秒）。
  // 这条用例铺的正是那个形状：满血判定在 6 小时前、降智判定在 30 秒前，减出来是 21570s，
  // 而正确答案是「未计时」。
  it('没有后端量到的时长就写「未计时」，不拿两个时刻相减', () => {
    const base = 'admin.accounts.openai.gatewayHistory'
    const w = render(
      account({
        current: 'unified-73',
        seen: {
          // 仍判满血 = 窗口正在跑，没有收尾读数。
          'unified-73': { at: isoAgo(10), region: 'east-asia', verdict: 'full', full_at: isoAgo(70) },
          // 判了降智、也有满血时刻，但两者隔了 6 小时 —— 相减是 21570s，必须是「未计时」。
          'unified-95': {
            at: isoAgo(30),
            region: 'us-east',
            verdict: 'degraded',
            full_at: isoAgo(6 * 3600)
          }
        },
        updated_at: isoAgo(10)
      })
    )
    expect(cell(w, 'east-asia').attributes('title')).toContain(`-${base}.fullUntimed-`)
    const stale = cell(w, 'us-east').attributes('title') ?? ''
    expect(stale).toContain(`-${base}.fullUntimed-`)
    expect(stale).not.toMatch(/-\d{4,}s-/)
  })

  // 满血分钟预测：每个网关各算一个单位，使用同层实测率和时长，不是保底。
  //
  // 第一版按大区去重（「一个号在一个大区同一时间只有一个网关」），2026-10-03 用户否了：
  // 「时间还是按网关来的，相同区域不同网关同一个号还是有不同的满血期的」。按大区数会把
  // us-west 那 20 个网关名算成 1 个单位，预测值低一个数量级。
  it('冷却统计按网关计数，即使有预测样本也不展示预测', () => {
    // 三个网关名同属 us-west，全部已出冷却 ⇒ **3** 个单位，不是 1 个。
    // 这一条就是那次纠正本身，按大区并会让它掉回 1。
    const oneRegion = render(
      accountWithSamples({
        current: 'unified-1',
        seen: {
          'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' },
          'unified-2': { at: isoAgo(6 * 3600), region: 'us-west' },
          'unified-3': { at: isoAgo(7 * 3600), region: 'us-west' }
        }
      })
    )
    expect(oneRegion.get('[data-testid="account-gateway-window-usage"]').text()).toContain('"used":0,"cooled":3')
    expect(oneRegion.find('[data-testid="account-gateway-forecast"]').exists()).toBe(false)

    // 同一大区里新旧混着时**各算各的**：旧的那个已恢复、新的那个还在烧 ⇒ 1 个单位。
    // 按大区取「最近那次」当起点会让它变成 0。
    const staleAndFresh = render(
      accountWithSamples({
        current: 'unified-2',
        seen: {
          'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' },
          'unified-2': { at: isoAgo(60), region: 'us-west' }
        }
      })
    )
    expect(staleAndFresh.get('[data-testid="account-gateway-window-usage"]').text()).toContain('"used":1,"cooled":1')

    // 全部刚烧过 ⇒ 一小时内一个都出不来 ⇒ 0 分钟。
    const allBurned = Object.fromEntries(
      Array.from({ length: 9 }, (_, i) => [`unified-${i}`, { at: isoAgo(60), region: 'us-west' }])
    )
    expect(render(account({ current: 'unified-0', seen: allBurned })).get('[data-testid="account-gateway-window-usage"]').text()).toContain('"used":9,"cooled":0')

    // 足够多的可重试网关仍封顶一小时。
    const many = Object.fromEntries(
      Array.from({ length: 100 }, (_, i) => [
        `unified-${i}`,
        { at: isoAgo(5 * 3600), region: 'us-west' }
      ])
    )
    expect(render(accountWithSamples({ current: 'unified-0', seen: many })).get('[data-testid="account-gateway-window-usage"]').text()).toContain('"used":0,"cooled":100')

    // 冷却剩余 ≤ 1 小时就算可用：4 小时窗口下，3.5 小时前烧的那个算回来。
    const recovering = render(
      accountWithSamples({
        current: 'unified-1',
        seen: { 'unified-1': { at: isoAgo(3.5 * 3600), region: 'us-west' } }
      })
    )
    expect(recovering.get('[data-testid="account-gateway-window-usage"]').text()).toContain('"used":1,"cooled":0')
  })

  // region 完全不参与计数：没带 region 的落点一样是一个有名有姓的网关，照数。
  //
  // 老版本有两条按大区的规则，都跟着删了：「没摸过的大区当上行空间提示」（给不出数 ——
  // 这一行不知道池子一共有多少网关）和「未归类落点从可用数里扣掉」（它本来是为了补
  // 按大区归类的漏，按网关数之后没有漏可补）。
  it('没带大区的落点照样算一个单位，不扣减也不另算', () => {
    const w = render(
      accountWithSamples({
        current: 'unified-1',
        seen: {
          'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' },
          'unified-2': { at: isoAgo(5 * 3600), region: 'europe' },
          'unified-9': { at: isoAgo(5 * 3600) } // 读不出大区，但出了冷却
        }
      })
    )
    expect(w.get('[data-testid="account-gateway-window-usage"]').text()).toContain('"used":0,"cooled":3')

    // 它还在窗口里的时候只是「这一个单位不可用」，不该再去扣别人。
    const hotBlind = render(
      accountWithSamples({
        current: 'unified-1',
        seen: {
          'unified-1': { at: isoAgo(5 * 3600), region: 'us-west' },
          'unified-2': { at: isoAgo(5 * 3600), region: 'europe' },
          'unified-9': { at: isoAgo(60) }
        }
      })
    )
    expect(hotBlind.get('[data-testid="account-gateway-window-usage"]').text()).toContain('"used":1,"cooled":2')

    // 那两条按大区的提示文案已经没了，页面上不该再出现它们。
    const text = w.text()
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
