import { describe, expect, it } from 'vitest'
import { contactInterval, forecastGatewayMinutes, groupGatewayContacts, readGatewayContacts } from '../gatewayContactStats'

const now = Date.parse('2026-10-03T12:00:00Z')
const hour = 3_600_000
function rawContacts() {
  return {
    ledger_tag: 'tag',
    seen: { one: { first_at: new Date(now - 8 * hour).toISOString(), last_at: new Date(now - 90 * 60_000).toISOString() } },
    rounds: Array.from({ length: 6 }, (_, i) => ({
      report: {
        id: `id-${i}`, gateway: 'one', model: 'astra', criterion: 'state-echo-v1',
        source: 'foreground', first: 'repeat', at: new Date(now - (10 - i) * 60_000).toISOString(),
        gap_known: true, elapsed_seconds: 90 * 60,
        outcome: i < 3 ? 'full' : i < 5 ? 'refreshed' : 'unknown',
        window_final: i < 3, full_window_ms: i < 3 ? 100000 : 0
      }
    }))
  }
}

describe('gateway contact estimates', () => {
  it('uses conclusive outcomes and measured ended windows, without a 183s fallback', () => {
    const contacts = readGatewayContacts(rawContacts(), 'tag', now)
    expect(forecastGatewayMinutes(contacts, [{ name: 'one', retryAt: now }], now).minutes).toBe(1)
    expect(groupGatewayContacts(contacts.rounds)[0]).toMatchObject({ full: 3, refreshed: 2, unknown: 1, windowSamples: 3 })
  })

  it.each(['model', 'criterion', 'source', 'first', 'elapsed_seconds'] as const)('does not mix strata: %s', (field) => {
    const raw = rawContacts()
    const values = { model: 'luna', criterion: 'different', source: 'background', first: 'tracked_first', elapsed_seconds: 4 * 3600 }
    Object.assign(raw.rounds[0].report, { [field]: values[field] })
    const contacts = readGatewayContacts(raw, 'tag', now)
    expect(forecastGatewayMinutes(contacts, [{ name: 'one', retryAt: now }], now).minutes).toBeNull()
  })

  it('requires a measured last send and enough ended windows, preserves unknowns', () => {
    const raw = rawContacts()
    raw.rounds[0].report.window_final = false
    const contacts = readGatewayContacts(raw, 'tag', now)
    expect(forecastGatewayMinutes(contacts, [{ name: 'one', retryAt: now }], now).minutes).toBeNull()
    expect(forecastGatewayMinutes(contacts, [{ name: 'unseen', retryAt: now }], now).minutes).toBeNull()
    expect(forecastGatewayMinutes(contacts, [{ name: 'one', retryAt: now + 2 * hour }], now).minutes).toBe(0)
    expect(readGatewayContacts(raw, 'different-identity', now).rounds).toEqual([])
  })

  it('deduplicates IDs and rejects stale/future observations', () => {
    const raw = rawContacts()
    raw.rounds.push(raw.rounds[0])
    raw.rounds[1].report.at = new Date(now - 8 * 24 * hour).toISOString()
    raw.rounds[2].report.at = new Date(now + hour).toISOString()
    expect(readGatewayContacts(raw, 'tag', now).rounds).toHaveLength(4)
  })

  it('matches interval boundaries and clamps estimates to one hour', () => {
    expect([null, 0, 3599, 3600, 7200, 14400, 21600, 28800, 36000].map(contactInterval))
      .toEqual(['unknown', '<1h', '<1h', '1-2h', '2-4h', '4-6h', '6-8h', '8-10h', '>=10h'])
    const contacts = readGatewayContacts(rawContacts(), 'tag', now)
    const candidates = Array.from({ length: 100 }, (_, i) => {
      const name = `gateway-${i}`
      contacts.seen[name] = contacts.seen.one
      return { name, retryAt: now }
    })
    expect(forecastGatewayMinutes(contacts, candidates, now).minutes).toBe(60)
  })
})
