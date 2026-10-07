import { describe, expect, it } from 'vitest'

import { cprOutboundProxy, isTurnStateHealthy, TURN_STATE_SHAPES, turnStateVerdict } from '@/utils/turnState'

import { turnStateFixture } from '@/components/account/__tests__/turnStateFixture'

const nowSec = Math.floor(Date.now() / 1000)

describe('isTurnStateHealthy', () => {
  // 只认 individual 的 10 块时，team 号铸出来的每一条都会被判降智：接管会一直注入、
  // 一直判失效，最后把一个从头到尾正常的号停掉。
  it.each(TURN_STATE_SHAPES.map((s) => s.blocks))('正常块数 %i 判健康', (blocks) => {
    expect(isTurnStateHealthy(turnStateFixture(nowSec, blocks))).toBe(true)
  })

  it.each([11, 13])('降智块数 %i 判不健康', (blocks) => {
    expect(isTurnStateHealthy(turnStateFixture(nowSec, blocks))).toBe(false)
  })

  // 信封解不开时退回字符长度，口径要与后端 openAITurnStateHealthy 的兜底一致。
  it('解不出信封时按字符长度兜底，且先 trim', () => {
    expect(isTurnStateHealthy('x'.repeat(292))).toBe(true)
    expect(isTurnStateHealthy(` ${'x'.repeat(332)}\n`)).toBe(true)
    expect(isTurnStateHealthy('x'.repeat(312))).toBe(false)
  })
})

describe('turnStateVerdict', () => {
  // 红色只给认得出的降智形态；表外形态（2026-09-23 起 pro2 铸出 780 / 33 块）判不了，标黄。
  it.each([
    [10, 'healthy'],
    [12, 'healthy'],
    [11, 'degraded'],
    [13, 'degraded'],
    [33, 'unknown'],
    [9, 'unknown']
  ])('%i 块判 %s', (blocks, want) => {
    expect(turnStateVerdict(turnStateFixture(nowSec, blocks))).toBe(want)
  })

  it('780 字符就是 33 块的真信封', () => {
    expect(turnStateFixture(nowSec, 33)).toHaveLength(780)
  })

  it('解不出信封时按字符长度兜底', () => {
    expect(turnStateVerdict('x'.repeat(292))).toBe('healthy')
    expect(turnStateVerdict(` ${'x'.repeat(356)}\n`)).toBe('degraded')
    expect(turnStateVerdict('x'.repeat(780))).toBe('unknown')
  })
})

describe('cprOutboundProxy', () => {
  const withExtra = (v: unknown) => ({ extra: { cpr_outbound_proxy: v } })

  it('脱敏值原样返回', () => {
    expect(cprOutboundProxy(withExtra('socks5h://198.51.100.7:1080'))).toBe(
      'socks5h://198.51.100.7:1080'
    )
  })

  /**
   * CPR 是独立仓库的服务，「它只返回脱敏值」是观察不是不变量。真带上凭据时这个值会
   * 一路写进 account.extra、随账号列表下发，在代理列和编辑弹窗里明文显示密码——渲染点
   * 是最后一道闸。后端 sanitizeCPROutboundProxy 是第一道，两道都要有。
   */
  it.each([
    ['socks5h://user:pass@198.51.100.7:1080', 'socks5h://198.51.100.7:1080'],
    ['http://user@proxy.example:8080', 'http://proxy.example:8080'],
    ['socks5://p%40ss:w%3Ard@[2001:db8::1]:1080', 'socks5://[2001:db8::1]:1080'],
    // 密码里带**字面** @（代理密码里很常见）：停在第一个 @ 的写法会砍掉密码前半段、
    // 把后半段连同残渣留在页面上（→ socks5h://ss@198.51.100.7:1080）。必须贪婪到
    // 最后一个 @，与后端 proxyurl.Parse（Go 按最后一个 @ 切）同口径。
    ['socks5h://user:p@ss@198.51.100.7:1080', 'socks5h://198.51.100.7:1080'],
    ['http://admin:se@cr@et@proxy.example:8080', 'http://proxy.example:8080']
  ])('剥掉 userinfo：%s', (raw, want) => {
    expect(cprOutboundProxy(withExtra(raw))).toBe(want)
  })

  it('路径里的 @ 不该被当成 userinfo 分隔符砍掉前面那段', () => {
    expect(cprOutboundProxy(withExtra('http://proxy.example:8080/a@b'))).toBe(
      'http://proxy.example:8080/a@b'
    )
  })

  it('缺键、非字符串、空账号都返回空串（调用点用它做 v-if）', () => {
    expect(cprOutboundProxy(null)).toBe('')
    expect(cprOutboundProxy(undefined)).toBe('')
    expect(cprOutboundProxy({})).toBe('')
    expect(cprOutboundProxy(withExtra(undefined))).toBe('')
    expect(cprOutboundProxy(withExtra(123))).toBe('')
    expect(cprOutboundProxy(withExtra('   '))).toBe('')
  })
})
