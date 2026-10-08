import { describe, expect, it } from 'vitest'
import { formatUsageTps, usageTps } from '../usageTps'

const row = { output_tokens: 100, duration_ms: 7000, first_token_ms: 2000, stream: true }

describe('usage TPS', () => {
  it('uses generation time for streaming and full duration for sync', () => {
    expect(formatUsageTps(row)).toBe('20.00')
    expect(formatUsageTps({ ...row, request_type: 'ws_v2', stream: false })).toBe('20.00')
    expect(formatUsageTps({ ...row, request_type: 'sync' })).toBe('14.29')
    expect(formatUsageTps({ ...row, stream: false, first_token_ms: null })).toBe('14.29')
    expect(formatUsageTps({ ...row, first_token_ms: 0 })).toBe('14.29')
    expect(formatUsageTps({ ...row, output_tokens: 0 })).toBe('0.00')
  })

  it.each(['cyber', 'live', 'unknown'])('uses legacy stream for orthogonal type %s', request_type => {
    expect(formatUsageTps({ ...row, request_type })).toBe('20.00')
    expect(formatUsageTps({ ...row, request_type, stream: false })).toBe('14.29')
  })

  it.each([
    { output_tokens: null }, { output_tokens: -1 }, { output_tokens: NaN }, { output_tokens: Infinity },
    { duration_ms: null }, { duration_ms: 0 }, { duration_ms: -1 }, { duration_ms: NaN }, { duration_ms: Infinity },
    { first_token_ms: null }, { first_token_ms: -1 }, { first_token_ms: 7000 }, { first_token_ms: 8000 },
    { first_token_ms: NaN }, { first_token_ms: Infinity },
    { request_type: 'probe' }, { request_type: 'gwpool_degraded' },
    { billing_mode: 'image' }, { billing_mode: 'video' }, { image_count: 1 },
    { billing_mode: 'token', image_count: 1 }, { image_output_tokens: 100 },
  ])('does not invent a rate from missing or inapplicable data: %j', patch => {
    expect(usageTps({ ...row, ...patch })).toBeNull()
    expect(formatUsageTps({ ...row, ...patch })).toBe('—')
  })
})
