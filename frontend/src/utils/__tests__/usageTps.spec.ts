import { describe, expect, it } from 'vitest'
import { formatUsageTps, usageTps } from '../usageTps'

const row = { output_tokens: 100, duration_ms: 7000, first_token_ms: 2000, stream: true }

describe('usage TPS', () => {
  it('subtracts the first response frame for every applicable request type', () => {
    for (const request_type of [undefined, 'stream', 'ws_v2', 'sync', 'cyber', 'live', 'unknown']) {
      expect(formatUsageTps({ ...row, request_type })).toBe('20.00')
      expect(formatUsageTps({ ...row, request_type, stream: false })).toBe('20.00')
    }
    expect(formatUsageTps({ ...row, output_tokens: 0 })).toBe('0.00')
    expect(formatUsageTps({ ...row, first_token_ms: 0 })).toBe('14.29')
    expect(formatUsageTps({ ...row, output_tokens: 17, duration_ms: 7440 })).toBe('3.13')
  })

  it('keeps reasoning and uses the first response frame instead of the first answer', () => {
    const hiddenReasoning = {
      output_tokens: 1984, reasoning_tokens: 516, duration_ms: 40928,
      first_token_ms: 349, first_answer_ms: 26153,
    }
    expect(formatUsageTps(hiddenReasoning)).toBe('48.89')

    const bufferedAnswer = {
      output_tokens: 158, reasoning_tokens: 0, duration_ms: 7933, first_token_ms: 400, first_answer_ms: 7556,
    }
    expect(formatUsageTps(bufferedAnswer)).toBe('20.97')
  })

  it.each([
    { reasoning_tokens: 40 }, { first_answer_ms: 3000 },
    { reasoning_tokens: null, first_answer_ms: 3000 },
    { reasoning_tokens: 40, first_answer_ms: 3000 }, { reasoning_tokens: 0, first_answer_ms: 2000 },
    { reasoning_tokens: 100, first_answer_ms: 3000 }, { reasoning_tokens: 120, first_answer_ms: 3000 },
    { reasoning_tokens: 40, first_answer_ms: 7000 }, { reasoning_tokens: 40, first_answer_ms: 8000 },
    { reasoning_tokens: NaN, first_answer_ms: 3000 }, { reasoning_tokens: 40, first_answer_ms: NaN },
  ])('does not change the rate based on reasoning counts or first-answer timing: %j', patch => {
    expect(formatUsageTps({ ...row, ...patch })).toBe('20.00')
  })

  it.each([undefined, null, -1, 7000, 8000, NaN, Infinity])('does not fall back with invalid or missing time to first token %s', first_token_ms => {
    const input = { ...row, first_token_ms }
    expect(formatUsageTps(input)).toBe('—')
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
