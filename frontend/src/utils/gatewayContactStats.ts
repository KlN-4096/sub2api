const HOUR_MS = 3_600_000
const RETENTION_MS = 7 * 24 * HOUR_MS
const ROUND_LIMIT = 300

type Outcome = 'full' | 'refreshed' | 'unknown'
type Source = 'foreground' | 'background' | 'business'

export interface ContactRound {
  id: string
  versionHash: string
  gateway: string
  source: Source
  at: number
  outcome: Outcome
  /** Business time at full strength this ticket had in its cycle, measured when it degraded. */
  fullUseMs: number | null
}

export interface Contacts {
  trackingSince: number
  truncated: boolean
  rounds: ContactRound[]
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown> : {}
}

function text(value: unknown): string {
  return typeof value === 'string' ? value.slice(0, 128) : ''
}

function timestamp(value: unknown): number {
  return typeof value === 'string' ? Date.parse(value) : Number.NaN
}

function nonnegative(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : 0
}

/** Only the persisted credential-domain ledger is accepted, never a different account's import. */
export function readGatewayContacts(value: unknown, ledgerTag: unknown, now: number): Contacts {
  const state = record(value)
  const result: Contacts = { trackingSince: Number.NaN, truncated: false, rounds: [] }
  if (typeof ledgerTag !== 'string' || !ledgerTag || state.ledger_tag !== ledgerTag) return result
  result.trackingSince = timestamp(state.tracking_since)
  result.truncated = state.history_truncated === true
  const ids = new Set<string>()
  for (const value of Array.isArray(state.rounds) ? state.rounds.slice(-ROUND_LIMIT) : []) {
    const round = record(value)
    const report = record(round.report)
    const at = timestamp(report.at)
    const id = text(report.id)
    if (!id || ids.has(id) || !Number.isFinite(at) || at < now - RETENTION_MS || at > now + 60_000) continue
    const source = report.source
    if (source !== 'foreground' && source !== 'background' && source !== 'business') continue
    const gateway = text(report.gateway)
    if (!text(report.model) || !gateway) continue
    ids.add(id)
    const fullUseMs = nonnegative(round.full_use_ms)
    result.rounds.push({
      id, versionHash: text(round.version_hash), gateway, source, at,
      outcome: report.outcome === 'full' || report.outcome === 'refreshed' ? report.outcome : 'unknown',
      fullUseMs: fullUseMs > 0 ? fullUseMs : null
    })
  }
  result.rounds.sort((a, b) => a.at - b.at)
  return result
}
