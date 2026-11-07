import { getDisplayBillingMode } from '@/utils/billingMode'
import { resolveUsageRequestType, type UsageRequestTypeLike } from '@/utils/usageRequestType'

interface UsageTpsRow extends UsageRequestTypeLike {
  output_tokens?: number | null
  duration_ms?: number | null
  first_token_ms?: number | null
  billing_mode?: string | null
  image_count?: number
  image_output_tokens?: number | null
}

/**
 * Average output tokens per second after the first response frame.
 * The formula matches ops queryOutputTPS (its sample filters differ).
 * First-answer timing and reasoning counts do not change the rate.
 */
export function usageTps(row: UsageTpsRow): number | null {
  const type = resolveUsageRequestType(row)
  const mode = getDisplayBillingMode({ billing_mode: row.billing_mode, image_count: row.image_count ?? 0 })
  if (type === 'probe' || type === 'gwpool_degraded' || mode === 'image' || mode === 'video' ||
      (row.image_count ?? 0) > 0 || (row.image_output_tokens ?? 0) > 0) return null
  const tokens = row.output_tokens
  const duration = row.duration_ms
  const firstToken = row.first_token_ms
  if (tokens == null || !Number.isFinite(tokens) || tokens < 0 ||
      duration == null || !Number.isFinite(duration) ||
      firstToken == null || !Number.isFinite(firstToken) || firstToken < 0 || firstToken >= duration) return null
  const rate = tokens * 1000 / (duration - firstToken)
  return Number.isFinite(rate) ? rate : null
}

export function formatUsageTps(row: UsageTpsRow): string {
  return usageTps(row)?.toFixed(2) ?? '—'
}
