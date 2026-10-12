import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountGatewayCell from '../AccountGatewayCell.vue'
import GatewayUsedHistory from '../GatewayUsedHistory.vue'
import type { Account } from '@/types'
import type { GatewayPoolProgress } from '@/api/admin/accounts'
import { readGatewayContacts } from '@/utils/gatewayContactStats'
// Written by the backend TestGatewayPoolRuntimeDisplayFixtureInSync: one merged
// history and contact ledger as stored, and as the progress API sends it.
import fixture from './gatewayRuntimeDisplay.fixture.json'

vi.mock('vue-i18n', async (importOriginal) => ({
  ...(await importOriginal<typeof import('vue-i18n')>()),
  useI18n: () => ({
    t: (key: string, params?: Record<string, unknown>) =>
      params ? `${key}:${JSON.stringify(params)}` : key
  })
}))

type Json = Record<string, unknown>
type Snapshot = { history: Json; contacts: Json }

// Every field the API sends; '*' stands for any seen gateway or contact round.
const SENT = [
  'history.current', 'history.current_region', 'history.updated_at',
  'history.seen.*.at', 'history.seen.*.region', 'history.seen.*.verdict', 'history.seen.*.full_at',
  'history.seen.*.full_held_ms', 'history.seen.*.cooldown.cleared', 'history.seen.*.cooldown.window_seconds',
  'history.seen.*.cooldown.fixed_seconds', 'history.seen.*.cooldown.until', 'history.seen.*.cooldown.recommended_seconds',
  'contacts.ledger_tag', 'contacts.tracking_since', 'contacts.history_truncated', 'contacts.rounds.*.version_hash',
  'contacts.rounds.*.report.id', 'contacts.rounds.*.report.gateway', 'contacts.rounds.*.report.model',
  'contacts.rounds.*.report.source', 'contacts.rounds.*.report.at', 'contacts.rounds.*.report.outcome',
  'contacts.rounds.*.report.full_window_ms', 'contacts.rounds.*.report.window_final'
]

const isObject = (value: unknown): value is Json => typeof value === 'object' && value !== null

function fields(value: unknown, path: string[] = []): string[] {
  if (!isObject(value)) return [path.join('.')]
  const any = Array.isArray(value) || path.at(-1) === 'seen'
  return [...new Set(Object.entries(value).flatMap(([key, child]) => fields(child, [...path, any ? '*' : key])))]
}

function without(value: unknown, [key, ...rest]: string[]): unknown {
  if (!isObject(value)) return value
  const copy = (Array.isArray(value) ? [...value] : { ...value }) as Json
  for (const name of key === '*' ? Object.keys(copy) : [key]) {
    if (!(name in copy)) continue
    if (rest.length) copy[name] = without(copy[name], rest)
    else delete copy[name]
  }
  return copy
}

// What the cell shows for one runtime snapshot, with the full history panel
// open so every entry's title, duration and status is built.
async function observe(snapshot: Snapshot) {
  const account = { id: 1, platform: 'openai', type: 'oauth', extra: { openai_gwpool: true } } as unknown as Account
  const progress: GatewayPoolProgress = {
    phase: 'idle', attempt: 0, limit: 0, rejected: 0, elapsed_ms: 0, active_requests: 0,
    started_at: fixture.now, updated_at: fixture.now,
    runtime: {
      observed_at: fixture.now, tickets: [], rounds: [], archived: null,
      ledger_tag: fixture.ledger_tag, gateway_window_seconds: fixture.gateway_window_seconds, ...snapshot
    }
  }
  const w = mount(AccountGatewayCell, { props: { account, progress }, global: { stubs: { teleport: true } } })
  try {
    const trigger = w.find('[data-testid="gateway-used-history-trigger"]')
    if (trigger.exists()) await trigger.trigger('click')
    const panel = w.findComponent(GatewayUsedHistory)
    return {
      html: w.html(),
      entries: panel.exists() ? panel.props('items') : null,
      contacts: readGatewayContacts(snapshot.contacts, fixture.ledger_tag, Date.now())
    }
  } finally {
    w.unmount()
  }
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date(fixture.now))
})
afterEach(() => {
  vi.useRealTimers()
})

it('the slimmed runtime history and contacts show exactly what the stored records show', async () => {
  const stored = await observe(fixture.stored)
  expect(stored.entries).toHaveLength(5)
  expect(stored.contacts.rounds).toHaveLength(3)
  expect(await observe(fixture.displayed)).toEqual(stored)
})

it('every sent field changes the cell, so the comparison above notices any of them missing', async () => {
  expect(fields(fixture.displayed).sort()).toEqual([...SENT].sort())
  const stored = await observe(fixture.stored)
  for (const field of SENT) {
    const dropped = without(fixture.displayed, field.split('.')) as Snapshot
    expect(await observe(dropped), `dropping ${field} must change the cell`).not.toEqual(stored)
  }
})
