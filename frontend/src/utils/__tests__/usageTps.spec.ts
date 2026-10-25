import { describe, expect, it } from 'vitest'
import { formatUsageTps, usageTps } from '../usageTps'

const row = { output_tokens: 100, duration_ms: 7000, first_token_ms: 2000, stream: true }

describe('usage TPS', () => {
  it('falls back to total duration for every request type without answer timing', () => {
    for (const request_type of [undefined, 'stream', 'ws_v2', 'sync', 'cyber', 'live', 'unknown']) {
      expect(formatUsageTps({ ...row, request_type })).toBe('14.29')
      expect(formatUsageTps({ ...row, request_type, stream: false })).toBe('14.29')
    }
    expect(formatUsageTps({ ...row, output_tokens: 0 })).toBe('0.00')
    expect(formatUsageTps({ ...row, duration_ms: 1500 })).toBe('66.67')
    expect(formatUsageTps({ ...row, output_tokens: 17, duration_ms: 5440 })).toBe('3.13')
    expect(formatUsageTps({ ...row, reasoning_tokens: 40 })).toBe('14.29')
    expect(formatUsageTps({ ...row, first_answer_ms: 3000 })).toBe('14.29')
    expect(formatUsageTps({ ...row, reasoning_tokens: null, first_answer_ms: 3000 })).toBe('14.29')
  })

  it('rates answer tokens over the time after the first answer delta', () => {
    expect(formatUsageTps({ ...row, reasoning_tokens: 40, first_answer_ms: 3000 })).toBe('15.00')
    expect(formatUsageTps({ ...row, reasoning_tokens: 0, first_answer_ms: 2000 })).toBe('20.00')
    // Codex 隐藏推理：900 输出里 640 是推理，首个答案 delta 在 4.2s
    expect(formatUsageTps({ output_tokens: 900, reasoning_tokens: 640, duration_ms: 9000, first_answer_ms: 4200 })).toBe('54.17')
  })

  it.each([
    { reasoning_tokens: 100, first_answer_ms: 3000 }, { reasoning_tokens: 120, first_answer_ms: 3000 },
    { reasoning_tokens: 40, first_answer_ms: 7000 }, { reasoning_tokens: 40, first_answer_ms: 8000 },
    { reasoning_tokens: NaN, first_answer_ms: 3000 }, { reasoning_tokens: 40, first_answer_ms: NaN },
  ])('shows no rate when the answer has no tokens or no time: %j', patch => {
    expect(usageTps({ ...row, ...patch })).toBeNull()
    expect(formatUsageTps({ ...row, ...patch })).toBe('—')
  })

  it.each([null, -1, 0, 7000, 8000, NaN, Infinity])('ignores time to first token %s', first_token_ms => {
    const input = { ...row, first_token_ms }
    expect(formatUsageTps(input)).toBe('14.29')
    expect(formatUsageTps({ ...input, reasoning_tokens: 40, first_answer_ms: 3000 })).toBe('15.00')
  })

  it.each([
    { output_tokens: null }, { output_tokens: -1 }, { output_tokens: NaN }, { output_tokens: Infinity },
    { duration_ms: null }, { duration_ms: 0 }, { duration_ms: -1 }, { duration_ms: NaN }, { duration_ms: Infinity },
    { request_type: 'probe' }, { request_type: 'gwpool_degraded' },
    { billing_mode: 'image' }, { billing_mode: 'video' }, { image_count: 1 },
    { billing_mode: 'token', image_count: 1 }, { image_output_tokens: 100 },
  ])('does not invent a rate from missing or inapplicable data: %j', patch => {
    for (const input of [{ ...row, ...patch }, { ...row, reasoning_tokens: 40, first_answer_ms: 3000, ...patch }]) {
      expect(usageTps(input)).toBeNull()
      expect(formatUsageTps(input)).toBe('—')
    }
  })
})
