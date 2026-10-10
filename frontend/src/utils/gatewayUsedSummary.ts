import type { Contacts } from './gatewayContactStats'

interface UsedGateway {
  name: string
  at: string
  fullAt: string
  fullHeldMs: number
}

export type GatewayUsedSummary<T> =
  | { kind: 'full'; key: string; at: number; item: T }
  | { kind: 'degraded'; key: string; at: number; count: number; atLeast: boolean }

const SUMMARY_ROW_LIMIT = 2

/** A ticket count comes from contact records, never the gateway-deduplicated history. */
export function gatewayUsedSummary<T extends UsedGateway>(
  contacts: Contacts, history: T[], excluded: Set<string>
): GatewayUsedSummary<T>[] {
  type Event = GatewayUsedSummary<T> | { kind: 'boundary'; key: string; at: number }
  const events: Event[] = []
  const items = new Map(history.map(item => [item.name, item]))
  const measuredGateways = new Set<string>()
  const tickets = new Set<string>()
  const oldestAt = contacts.rounds[0]?.at ?? Number.NaN
  const historyIncomplete = contacts.truncated || !Number.isFinite(contacts.trackingSince)
    || contacts.trackingSince < oldestAt
  for (const round of [...contacts.rounds].reverse()) {
    const ticket = JSON.stringify([round.gateway, round.versionHash || round.id])
    if (tickets.has(ticket)) continue
    tickets.add(ticket)
    const base = { key: round.id, at: round.at }
    const item = items.get(round.gateway)
    if (excluded.has(round.gateway)) {
      events.push({ ...base, kind: 'boundary' })
    } else if (round.windowMs !== null && item) {
      measuredGateways.add(round.gateway)
      events.push({ ...base, kind: 'full', item: { ...item, fullHeldMs: round.windowMs } })
    } else if (round.outcome === 'refreshed' && round.source !== 'business') {
      events.push({ ...base, kind: 'degraded', count: 1, atLeast: false })
    } else {
      // Unknown, failed verification and unmeasured full tickets break a run.
      events.push({ ...base, kind: 'boundary' })
    }
  }
  // Deduplication may remove the oldest raw record. The oldest surviving
  // event, rather than one exact timestamp, determines whether a run is open.
  const oldestEvent = events.at(-1)
  if (oldestEvent?.kind === 'degraded') oldestEvent.atLeast = historyIncomplete
  // Retained per-gateway timing can outlive the bounded ticket ledger. Its
  // full_at, not a later failed touch, positions the old full window.
  for (const item of history) {
    if (excluded.has(item.name) || measuredGateways.has(item.name)
      || !Number.isFinite(item.fullHeldMs) || item.fullHeldMs <= 0) continue
    const fullAt = Date.parse(item.fullAt)
    const at = Number.isFinite(fullAt) && fullAt > 0 ? fullAt : Date.parse(item.at)
    if (Number.isFinite(at) && at > 0) events.push({ kind: 'full', key: `history:${item.name}`, at, item })
  }
  events.sort((a, b) => b.at - a.at || a.key.localeCompare(b.key))
  const rows: GatewayUsedSummary<T>[] = []
  let run: Extract<GatewayUsedSummary<T>, { kind: 'degraded' }> | undefined
  for (const event of events) {
    if (event.kind === 'degraded') {
      if (run) {
        run.count += event.count
        run.atLeast ||= event.atLeast
      } else {
        run = { ...event }
        rows.push(run)
      }
    } else {
      run = undefined
      if (event.kind === 'full') rows.push(event)
    }
  }
  // Do not let several unknown-separated rejection runs hide the last useful
  // full window. Without any timing, one rejection summary is sufficient.
  if (rows[0]?.kind === 'degraded') {
    const full = rows.find(row => row.kind === 'full')
    return full ? [rows[0], full] : [rows[0]]
  }
  return rows.slice(0, SUMMARY_ROW_LIMIT)
}
