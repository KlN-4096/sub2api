import type { UsageRequestType } from '@/types'

export interface UsageRequestTypeLike {
  request_type?: string | null
  stream?: boolean | null
  openai_ws_mode?: boolean | null
}

const VALID_REQUEST_TYPES = new Set<UsageRequestType>(['unknown', 'sync', 'stream', 'ws_v2', 'cyber', 'live', 'probe'])

export const isUsageRequestType = (value: unknown): value is UsageRequestType => {
  return typeof value === 'string' && VALID_REQUEST_TYPES.has(value as UsageRequestType)
}

export const resolveUsageRequestType = (value: UsageRequestTypeLike): UsageRequestType => {
  if (isUsageRequestType(value.request_type)) {
    return value.request_type
  }
  if (value.openai_ws_mode) {
    return 'ws_v2'
  }
  return value.stream ? 'stream' : 'sync'
}

export const requestTypeToLegacyStream = (requestType?: UsageRequestType | null): boolean | null | undefined => {
  // cyber 与 stream 正交（cyber 可发生在 stream 或非 stream 请求），不映射到 legacy stream 维度。
  // probe（292 猎手探测）同理：它总是流式发出但头到手即断，按 legacy stream 筛没有意义。
  if (!requestType || requestType === 'unknown' || requestType === 'cyber' || requestType === 'live' || requestType === 'probe') {
    return null
  }
  if (requestType === 'sync') {
    return false
  }
  return true
}
