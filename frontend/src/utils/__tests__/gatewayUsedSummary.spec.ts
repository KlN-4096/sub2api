import { describe, expect, it } from 'vitest'
import { readGatewayContacts } from '../gatewayContactStats'
import { gatewayUsedSummary } from '../gatewayUsedSummary'

const now = Date.parse('2026-10-10T03:00:00Z')
const iso = (ago: number) => new Date(now - ago * 1000).toISOString()
function round(id: string, ago: number, outcome = 'refreshed', gateway = 'bad', version = id) {
  return {
    version_hash: version,
    report: { id, gateway, model: 'luna', source: 'foreground', at: iso(ago), outcome,
      window_final: outcome === 'full', full_window_ms: outcome === 'full' ? 124000 : 0 }
  }
}
function contacts(rounds: ReturnType<typeof round>[], trackingAgo = 300) {
  return readGatewayContacts({ ledger_tag: 'member', tracking_since: iso(trackingAgo), rounds }, 'member', now)
}
const history = [
  { name: 'good', at: iso(176), fullAt: iso(300), fullHeldMs: 124000 },
  { name: 'bad', at: iso(1), fullAt: '', fullHeldMs: 0 }
]

describe('gateway used summary', () => {
  it('counts tickets rather than distinct gateways and deduplicates reused versions', () => {
    const rows = gatewayUsedSummary(contacts([
      round('full', 300, 'full', 'good'), round('a', 100), round('b', 90),
      round('duplicate-b', 80, 'refreshed', 'bad', 'b')
    ]), history, new Set())
    expect(rows).toHaveLength(2)
    expect(rows[0]).toMatchObject({ kind: 'degraded', count: 2, atLeast: false })
    expect(rows[1]).toMatchObject({ kind: 'full', item: { name: 'good', fullHeldMs: 124000 } })
  })

  it('does not count unknown or unmeasured full tickets as degraded, or join runs across them', () => {
    for (const outcome of ['unknown', 'full']) {
      const middle = round('unknown', 50, outcome)
      middle.report.window_final = false
      const rows = gatewayUsedSummary(contacts([
        round('full', 300, 'full', 'good'), round('older-bad', 100), middle, round('latest-bad', 10)
      ]), history, new Set())
      expect(rows[0]).toMatchObject({ kind: 'degraded', count: 1 })
      expect(rows[1]).toMatchObject({ kind: 'full', item: { name: 'good' } })
    }
  })

  it('marks a truncated tail as at least and keeps older per-gateway full timing', () => {
    const rows = gatewayUsedSummary(contacts([round('a', 100), round('b', 10)]), history, new Set())
    expect(rows[0]).toMatchObject({ kind: 'degraded', count: 2, atLeast: true })
    expect(rows[1]).toMatchObject({ kind: 'full', at: now - 300000 })
  })

  it('keeps the lower-bound marker when the oldest raw contact duplicates a newer ticket', () => {
    const rows = gatewayUsedSummary(contacts([
      round('old-a', 150, 'refreshed', 'bad', 'a'), round('b', 100),
      round('new-a', 10, 'refreshed', 'bad', 'a')
    ]), history, new Set())
    expect(rows[0]).toMatchObject({ kind: 'degraded', count: 2, atLeast: true })
  })

  it('does not mark a bounded run as incomplete just because older history was dropped', () => {
    const rows = gatewayUsedSummary(contacts([
      round('boundary', 150, 'unknown'), round('a', 100), round('b', 10)
    ], 600), history, new Set())
    expect(rows[0]).toMatchObject({ count: 2, atLeast: false })
  })

  it('excludes all current gateways and keeps untimed entries only in full details', () => {
    const rows = gatewayUsedSummary(contacts([
      round('full', 300, 'full', 'good'), round('a', 100), round('b', 10)
    ]), history, new Set(['bad']))
    expect(rows).toHaveLength(1)
    expect(rows[0].kind).toBe('full')
    expect(gatewayUsedSummary(contacts([], 0), [history[1]], new Set())).toEqual([])
  })

  it('ignores cross-member contacts without inventing a degraded count from gateway history', () => {
    const wrong = readGatewayContacts({ ledger_tag: 'other', rounds: [round('a', 10)] }, 'member', now)
    expect(gatewayUsedSummary(wrong, history, new Set()).map(row => row.kind)).toEqual(['full'])
  })

  it('keeps 300 ticket records in the browser too, preserving the seven-day cutoff', () => {
    const raw = Array.from({ length: 305 }, (_, i) => round(`ticket-${i}`, 305 - i))
    const state = contacts(raw, 305)
    expect(state.rounds).toHaveLength(300)
    expect(state.rounds[0].id).toBe('ticket-5')
    expect(gatewayUsedSummary(state, history.slice(1), new Set())[0]).toMatchObject({
      kind: 'degraded', count: 300, atLeast: true
    })
    expect(contacts([round('expired', 8 * 24 * 3600)]).rounds).toEqual([])
  })
})
