import { getDisplayBillingMode } from '@/utils/billingMode'
import { resolveUsageRequestType, type UsageRequestTypeLike } from '@/utils/usageRequestType'

interface UsageTpsRow extends UsageRequestTypeLike {
  output_tokens?: number | null
  reasoning_tokens?: number | null
  duration_ms?: number | null
  first_answer_ms?: number | null
  billing_mode?: string | null
  image_count?: number
  image_output_tokens?: number | null
}

/**
 * Answer tokens (output − reasoning) ÷ time from the first answer delta to the end. Reasoning text is usually not
 * sent, and reasoning or summary deltas that are sent are not answer, so much reasoning can precede the first answer
 * delta: reasoning tokens are subtracted and the wait before the first answer is not timed. Rows missing either input (older rows, non-streaming, WebSocket,
 * safety-buffered, upstreams that do not report reasoning, etc.) fall back to output tokens ÷ total duration, the
 * formula of ops queryOutputTPS (its sample filters differ). first_token_ms is never subtracted: the event it marks
 * varies by path and TTFT mode.
 */
export function usageTps(row: UsageTpsRow): number | null {
  const type = resolveUsageRequestType(row)
  const mode = getDisplayBillingMode({ billing_mode: row.billing_mode, image_count: row.image_count ?? 0 })
  if (type === 'probe' || type === 'gwpool_degraded' || mode === 'image' || mode === 'video' ||
      (row.image_count ?? 0) > 0 || (row.image_output_tokens ?? 0) > 0) return null
  const tokens = row.output_tokens
  const duration = row.duration_ms
  if (tokens == null || !Number.isFinite(tokens) || tokens < 0 ||
      duration == null || !Number.isFinite(duration) || duration <= 0) return null
  let rate = tokens * 1000 / duration
  if (row.reasoning_tokens != null && row.first_answer_ms != null) {
    const answerTokens = tokens - row.reasoning_tokens
    const answerMs = duration - row.first_answer_ms
    if (!(answerTokens > 0 && answerMs > 0)) return null
    rate = answerTokens * 1000 / answerMs
  }
  return Number.isFinite(rate) ? rate : null
}

export function formatUsageTps(row: UsageTpsRow): string {
  return usageTps(row)?.toFixed(2) ?? '—'
}
