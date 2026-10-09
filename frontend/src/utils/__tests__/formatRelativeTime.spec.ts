import { afterEach, describe, expect, it, vi } from 'vitest'
import { formatCountdown, formatCountdownWithSuffix, formatRelativeTime } from '../format'

vi.mock('@/i18n', () => ({
  i18n: { global: { t: (key: string, params?: { n: number }) => `${key}:${params?.n ?? ''}` } },
  getLocale: () => 'en'
}))

describe('formatRelativeTime display clock', () => {
  afterEach(() => vi.useRealTimers())

  it('keeps a supplied display time fixed while the real clock advances', () => {
    vi.useFakeTimers()
    const now = Date.now()
    const at = new Date(now - 30_000)
    expect(formatRelativeTime(at, now)).toBe('common.time.justNow:')
    vi.advanceTimersByTime(60_000)
    expect(formatRelativeTime(at, now)).toBe('common.time.justNow:')
    expect(formatRelativeTime(at)).toBe('common.time.minutesAgo:1')
  })

  it('preserves empty, invalid, future, and elapsed-time boundaries', () => {
    const now = Date.UTC(2026, 9, 9)
    for (const value of [undefined, null, '', 'invalid', new Date(now + 1000)]) {
      expect(formatRelativeTime(value, now)).toBe('common.time.never:')
    }
    expect(formatRelativeTime(new Date(now - 60_000), now)).toBe('common.time.minutesAgo:1')
    expect(formatRelativeTime(new Date(now - 3_600_000), now)).toBe('common.time.hoursAgo:1')
    expect(formatRelativeTime(new Date(now - 86_400_000), now)).toBe('common.time.daysAgo:1')
  })

  it('freezes countdowns on the same display clock, including the suffixed form', () => {
    vi.useFakeTimers()
    const now = Date.now()
    const target = new Date(now + 60_000)
    const plain = formatCountdown(target, now)
    const suffixed = formatCountdownWithSuffix(target, now)
    vi.advanceTimersByTime(90_000)
    expect(formatCountdown(target, now)).toBe(plain)
    expect(formatCountdownWithSuffix(target, now)).toBe(suffixed)
    expect(formatCountdown(target)).toBeNull()
    expect(formatCountdownWithSuffix(target)).toBeNull()
  })
})
