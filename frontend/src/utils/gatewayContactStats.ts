// Keep the interval boundaries aligned with pkg/gwpool.ContactInterval.
const HOUR_MS = 3_600_000
const RETENTION_MS = 7 * 24 * HOUR_MS
const ROUND_LIMIT = 64
export const CONTACT_MIN_RESULTS = 5
export const CONTACT_MIN_WINDOWS = 3
export const CONTACT_CRITERION = 'state-echo-v1'

type Outcome = 'full' | 'refreshed' | 'unknown'
type Source = 'foreground' | 'background' | 'business'
type First = 'tracked_first' | 'repeat' | 'unknown'

export interface ContactStep {
  shot: string
  sent: boolean
  status: number
  gotState: boolean
  echoAccepted: boolean
  actualGateway: string
  durationMs: number
}

export interface ContactRound {
  id: string
  gateway: string
  model: string
  criterion: string
  source: Source
  first: First
  at: number
  lastAt: number
  probeAt: number
  probeModel: string
  probeSource: string
  gapSeconds: number | null
  outcome: Outcome
  windowMs: number | null
  lastOutcome: Outcome
  steps: ContactStep[]
}

export interface Contacts {
  trackingSince: number
  truncated: boolean
  seen: Record<string, { firstAt: number; lastAt: number }>
  rounds: ContactRound[]
}

export interface ContactGroup {
  key: string
  model: string
  criterion: string
  source: Source
  first: First
  interval: string
  full: number
  refreshed: number
  unknown: number
  windowSamples: number
  windowTotalMs: number
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

function outcome(value: unknown): Outcome {
  return value === 'full' || value === 'refreshed' ? value : 'unknown'
}

export function contactInterval(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds) || seconds < 0) return 'unknown'
  if (seconds < 3600) return '<1h'
  if (seconds < 7200) return '1-2h'
  if (seconds < 14400) return '2-4h'
  if (seconds < 21600) return '4-6h'
  if (seconds < 28800) return '6-8h'
  if (seconds < 36000) return '8-10h'
  return '>=10h'
}

/** Only the persisted credential-domain ledger is accepted, never a different account's import. */
export function readGatewayContacts(value: unknown, ledgerTag: unknown, now: number): Contacts {
  const state = record(value)
  const result: Contacts = { trackingSince: Number.NaN, truncated: false, seen: {}, rounds: [] }
  if (typeof ledgerTag !== 'string' || !ledgerTag || state.ledger_tag !== ledgerTag) return result
  result.trackingSince = timestamp(state.tracking_since)
  result.truncated = state.history_truncated === true
  for (const [name, value] of Object.entries(record(state.seen)).slice(0, 512)) {
    const seen = record(value)
    const lastAt = timestamp(seen.last_at)
    if (Number.isFinite(lastAt) && lastAt <= now + 60_000) {
      result.seen[name] = { firstAt: timestamp(seen.first_at), lastAt }
    }
  }
  const ids = new Set<string>()
  for (const value of Array.isArray(state.rounds) ? state.rounds.slice(-ROUND_LIMIT) : []) {
    const round = record(value)
    const report = record(round.report)
    const at = timestamp(report.at)
    const id = text(report.id)
    if (!id || ids.has(id) || !Number.isFinite(at) || at < now - RETENTION_MS || at > now + 60_000) continue
    const source = report.source
    if (source !== 'foreground' && source !== 'background' && source !== 'business') continue
    const model = text(report.model)
    const gateway = text(report.gateway)
    if (!model || !gateway) continue
    ids.add(id)
    const gap = report.elapsed_seconds
    const windowMs = nonnegative(report.full_window_ms)
    const explicitProbeAt = timestamp(round.last_probe_at)
    const hasProbeContext = Number.isFinite(explicitProbeAt) && explicitProbeAt <= now + 60_000 &&
      (round.last_probe_source === 'foreground' || round.last_probe_source === 'background')
    result.rounds.push({
      id, gateway, model, criterion: text(report.criterion), source, at,
      first: report.first === 'tracked_first' || report.first === 'repeat' ? report.first : 'unknown',
      lastAt: timestamp(round.last_at),
      probeAt: hasProbeContext ? explicitProbeAt : source === 'business' ? Number.NaN : at,
      probeModel: hasProbeContext ? text(round.last_probe_model) : model,
      probeSource: hasProbeContext ? text(round.last_probe_source) : source,
      gapSeconds: report.gap_known === true && typeof gap === 'number' && Number.isFinite(gap) && gap >= 0 && gap <= 30 * 24 * 3600 ? gap : null,
      outcome: outcome(report.outcome), lastOutcome: outcome(round.last_probe_outcome),
      windowMs: report.outcome === 'full' && report.window_final === true && windowMs > 0 && windowMs <= 10 * HOUR_MS ? windowMs : null,
      steps: (Array.isArray(round.steps) ? round.steps.slice(0, 2) : []).map((value) => {
        const step = record(value)
        return {
          shot: text(step.shot), sent: step.sent === true, status: nonnegative(step.status),
          gotState: step.got_state === true, echoAccepted: step.echo_accepted === true,
          actualGateway: text(step.actual_gateway), durationMs: nonnegative(step.duration_ms)
        }
      })
    })
  }
  result.rounds.sort((a, b) => a.at - b.at)
  return result
}

export function groupGatewayContacts(rounds: ContactRound[]): ContactGroup[] {
  const groups = new Map<string, ContactGroup>()
  for (const round of rounds) {
    const interval = contactInterval(round.gapSeconds)
    const key = JSON.stringify([round.model, round.criterion, round.source, round.first, interval])
    const group = groups.get(key) ?? {
      key, model: round.model, criterion: round.criterion, source: round.source, first: round.first,
      interval, full: 0, refreshed: 0, unknown: 0, windowSamples: 0, windowTotalMs: 0
    }
    group[round.outcome] += 1
    if (round.windowMs !== null) {
      group.windowSamples += 1
      group.windowTotalMs += round.windowMs
    }
    groups.set(key, group)
  }
  return [...groups.values()]
}

/** Empirical estimate, not a lower bound, lease lifetime, or recovery guarantee. */
export function forecastGatewayMinutes(
  contacts: Contacts, candidates: { name: string; retryAt: number }[], now: number
): { minutes: number | null; units: number; model: string; source: string } {
  const eligible = candidates.filter((candidate) => Number.isFinite(candidate.retryAt) && candidate.retryAt <= now + HOUR_MS)
  const latest = contacts.rounds.filter((r) => Number.isFinite(r.probeAt) && r.probeSource !== 'business' &&
    r.criterion === CONTACT_CRITERION && r.probeModel && r.probeModel !== 'unknown').sort((a, b) => a.probeAt - b.probeAt).at(-1)
  const result = { minutes: eligible.length ? null : 0, units: eligible.length, model: latest?.probeModel ?? '', source: latest?.probeSource ?? '' }
  if (!eligible.length || !latest) return result
  const groups = groupGatewayContacts(contacts.rounds).filter((g) =>
    g.model === latest.probeModel && g.criterion === latest.criterion && g.source === latest.probeSource && g.first === 'repeat')
  let expectedMs = 0
  for (const candidate of eligible) {
    const lastAt = contacts.seen[candidate.name]?.lastAt
    if (!Number.isFinite(lastAt)) return result
    const gap = (Math.max(now, candidate.retryAt) - lastAt) / 1000
    if (gap < 0 || gap > 30 * 24 * 3600) return result
    const group = groups.find((g) => g.interval === contactInterval(gap))
    if (!group || group.full + group.refreshed < CONTACT_MIN_RESULTS || group.windowSamples < CONTACT_MIN_WINDOWS) return result
    expectedMs += group.full / (group.full + group.refreshed) * group.windowTotalMs / group.windowSamples
  }
  return { ...result, minutes: Math.round(Math.min(expectedMs, HOUR_MS) / 60_000) }
}
