import { expect, it } from 'vitest'
import { forecastGatewayMinutes, readGatewayContacts } from '../gatewayContactStats'

it('selects latest actual probe context independently of frozen first report and business lastAt', () => {
  const now = Date.parse('2026-10-03T12:00:00Z')
  const iso = (minutes: number) => new Date(now - minutes * 60_000).toISOString()
  const report = (id: string, minutes: number) => ({
    id, gateway: 'one', model: 'astra', criterion: 'state-echo-v1', source: 'foreground',
    first: 'repeat', outcome: 'full', at: iso(minutes)
  })
  const raw = {
    ledger_tag: 'tag',
    rounds: [
      { report: report('old-round-latest-probe', 10), last_at: iso(2),
        last_probe_at: iso(2), last_probe_model: 'luna', last_probe_source: 'background' },
      { report: report('new-round-old-probe', 5), last_at: iso(0),
        last_probe_at: iso(5), last_probe_model: 'astra', last_probe_source: 'foreground' }
    ]
  }
  const contacts = readGatewayContacts(raw, 'tag', now)
  const forecast = forecastGatewayMinutes(contacts, [{ name: 'one', retryAt: now }], now)
  expect(forecast.model).toBe('luna')
  expect(forecast.source).toBe('background')
  expect(forecast.minutes).toBeNull()
})
