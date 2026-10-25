import { getDisplayBillingMode } from '@/utils/billingMode'
import { resolveUsageRequestType, type UsageRequestTypeLike } from '@/utils/usageRequestType'

interface UsageTpsRow extends UsageRequestTypeLike {
  output_tokens?: number | null
  duration_ms?: number | null
  billing_mode?: string | null
  image_count?: number
  image_output_tokens?: number | null
}

/**
 * Output tokens ÷ total duration for every request type; same formula as ops queryOutputTPS (its sample filters differ).
 * first_token_ms is not subtracted: the event it marks varies by path and TTFT mode, and tokens generated before it
 * would count without their time.
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
  const rate = tokens * 1000 / duration
  return Number.isFinite(rate) ? rate : null
}

export function formatUsageTps(row: UsageTpsRow): string {
  return usageTps(row)?.toFixed(2) ?? '—'
}
