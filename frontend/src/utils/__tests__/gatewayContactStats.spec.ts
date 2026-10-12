import { describe, expect, it } from 'vitest'
import { readGatewayContacts } from '../gatewayContactStats'

const now = Date.parse('2026-10-03T12:00:00Z')
const hour = 3_600_000
function rawContacts() {
  return {
    ledger_tag: 'tag',
    rounds: Array.from({ length: 6 }, (_, i) => ({
      report: {
        id: `id-${i}`, gateway: 'one', model: 'astra', source: 'foreground',
        at: new Date(now - (10 - i) * 60_000).toISOString(),
        outcome: i < 3 ? 'full' : i < 5 ? 'refreshed' : 'unknown',
        window_final: i < 3, full_window_ms: i < 3 ? 100000 : 0
      }
    }))
  }
}

describe('readGatewayContacts', () => {
  it('reads outcomes and only measured, ended full windows', () => {
    const rounds = readGatewayContacts(rawContacts(), 'tag', now).rounds
    expect(rounds.map((r) => r.outcome)).toEqual(['full', 'full', 'full', 'refreshed', 'refreshed', 'unknown'])
    expect(rounds.map((r) => r.windowMs)).toEqual([100000, 100000, 100000, null, null, null])
  })

  it('accepts only the current ledger tag', () => {
    expect(readGatewayContacts(rawContacts(), 'different-identity', now).rounds).toEqual([])
  })

  it('deduplicates IDs and rejects stale/future observations', () => {
    const raw = rawContacts()
    raw.rounds.push(raw.rounds[0])
    raw.rounds[1].report.at = new Date(now - 8 * 24 * hour).toISOString()
    raw.rounds[2].report.at = new Date(now + hour).toISOString()
    expect(readGatewayContacts(raw, 'tag', now).rounds).toHaveLength(4)
  })
})
