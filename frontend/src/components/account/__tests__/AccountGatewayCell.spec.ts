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

const account = (gateways: unknown, type = 'oauth'): Account =>
  ({
    id: 1,
    platform: 'openai',
    type,
    extra: { openai_gwpool_gateways: gateways }
  }) as unknown as Account

const render = (acc: Account) => mount(AccountGatewayCell, { props: { account: acc } })

const seenChips = (w: ReturnType<typeof render>) =>
  w
    .get('[data-testid="account-gateway-seen"]')
    .findAll('span')
    .map((s) => s.text())
    .filter((text) => text.startsWith('unified-'))

describe('AccountGatewayCell', () => {
  it('当前落点单独一行，其余按最近用过的在前排', () => {
    const w = render(
      account({
        current: 'unified-73',
        seen: {
          'unified-167': isoAgo(3600),
          'unified-73': isoAgo(60),
          'unified-126': isoAgo(600)
        },
        updated_at: isoAgo(60)
      })
    )
    expect(w.get('[data-testid="account-gateway-current"]').text()).toContain('unified-73')
    // 当前那个不在「打过的」里重复一遍，其余按时间倒序。
    expect(seenChips(w)).toEqual(['unified-126', 'unified-167'])
  })

  // (账号 × 网关) 是烧窗口的单位，这一串回答的是「还剩哪些没碰过的落点」——
  // 只给个数等于没说，所以名字必须真的落在 DOM 上。
  it('超出上限的网关折进 +N，名字进 tooltip', () => {
    const seen: Record<string, string> = {}
    for (let i = 0; i < 10; i++) seen[`unified-${i}`] = isoAgo(i * 60)
    const w = render(account({ current: 'unified-0', seen, updated_at: isoAgo(0) }))

    // 当前那个排掉之后还剩 9 个，露出 6 个 + 一个 +3。
    expect(seenChips(w)).toHaveLength(6)
    const more = w.get('[data-testid="account-gateway-more"]')
    expect(more.text()).toBe('+3')
    expect(more.attributes('title') ?? '').toContain('unified-9')
  })

  it('没有读数时给占位，不是整块消失', () => {
    const w = render(account(undefined))
    expect(w.find('[data-testid="account-gateway-empty"]').exists()).toBe(true)
    expect(w.find('[data-testid="account-gateway-current"]').exists()).toBe(false)
  })

  // 当前网关可能已经被裁出 seen（条目有上限），那时也得照常显示。
  it('当前网关不在 seen 里也照常显示', () => {
    const w = render(account({ current: 'unified-200', seen: {}, updated_at: isoAgo(30) }))
    expect(w.get('[data-testid="account-gateway-current"]').text()).toContain('unified-200')
    expect(w.find('[data-testid="account-gateway-seen"]').exists()).toBe(false)
  })

  it('读数形状不对时安全降级为占位', () => {
    for (const bad of ['', 42, [], { seen: 'nope' }, { seen: { 'unified-1': 7 } }]) {
      const w = render(account(bad))
      expect(w.find('[data-testid="account-gateway-empty"]').exists()).toBe(true)
    }
  })

  it('非 Codex 上游的账号整块不展示', () => {
    const acc = {
      id: 1,
      platform: 'anthropic',
      type: 'oauth',
      extra: { openai_gwpool_gateways: { current: 'unified-73', seen: { 'unified-73': isoAgo(60) } } }
    } as unknown as Account
    expect(render(acc).find('[data-testid="account-gateway-cell"]').exists()).toBe(false)
  })
})
