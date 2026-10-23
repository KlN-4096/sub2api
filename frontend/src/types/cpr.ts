import type { UsageProgress } from './index'
import type { OpenAICredits, OpenAIRateLimitResetCredits } from '@/api/admin/accounts'

export interface CPRConnectAccount {
  id: string
  name: string
  email: string
  plan_type: string
  status: string
  enabled: boolean
  concurrency_limit: number | null
}

export interface CPRAccountSnapshot {
  account_id: string
  name?: string
  email?: string
  plan_type?: string
  status?: string
  enabled: boolean
  error_reason?: string
  access_token_expires_at?: string
  refresh_token_expires_at?: string
  rate_limited_until?: string
  outbound_proxy?: string
  quota_updated_display?: string
  checked_at?: string
  query_attempted_at?: string
  query_error?: string
  five_hour?: UsageProgress
  seven_day?: UsageProgress
  credits?: OpenAICredits
  reset_credits?: OpenAIRateLimitResetCredits
  capabilities?: {
    quota_refresh: boolean
    reset_credits: boolean
    consume_reset_credit: boolean
  }
}
