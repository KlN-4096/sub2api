/**
 * X-Codex-Turn-State 是标准 Fernet 令牌：
 *   0x80 版本 | 8 字节大端铸造时间戳（明文） | 16 字节 IV | AES-CBC 密文 | 32 字节 HMAC
 * 57 字节固定开销，其余是 16 的整数倍。292 字符 = 10 块，312 字符 = 11 块，差一个块。
 *
 * 判据用密文块数而不是字符长度：块数只把明文框进 16 字节的窗口，所以这是疑似判据；
 * 但它比字符长度稳，信封解不开时才退回长度。
 */
export const TURN_STATE_FERNET_OVERHEAD = 1 + 8 + 16 + 32
export const TURN_STATE_BLOCK_BYTES = 16
export const TURN_STATE_HEALTHY_BLOCKS = 10
export const TURN_STATE_HEALTHY_CHARS = 292
/** 票自铸造起 1 小时有效（对家实时池六张卡的「到期」都精确等于 Fernet 戳 + 1h）。 */
export const TURN_STATE_DEFAULT_TTL_MINUTES = 60

export interface TurnStateEnvelope {
  blocks: number
  mintedAt: Date
}

export const decodeTurnState = (blob?: string | null): TurnStateEnvelope | null => {
  if (!blob) return null
  try {
    const padded = blob.replace(/-/g, '+').replace(/_/g, '/')
    const bin = atob(padded + '='.repeat((4 - (padded.length % 4)) % 4))
    if (bin.length <= TURN_STATE_FERNET_OVERHEAD || bin.charCodeAt(0) !== 0x80) return null
    const cipher = bin.length - TURN_STATE_FERNET_OVERHEAD
    if (cipher % TURN_STATE_BLOCK_BYTES !== 0) return null
    let ts = 0
    for (let i = 1; i <= 8; i++) ts = ts * 256 + bin.charCodeAt(i)
    const mintedAt = new Date(ts * 1000)
    // 与后端 parseOpenAITurnStateEnvelope 同一道合理性闸：解出 1970 或 2999 年的
    // 时间戳说明这根本不是 turn-state，宁可判解析失败，也不要展示一个假的到期时刻。
    if (
      Number.isNaN(mintedAt.getTime()) ||
      mintedAt.getUTCFullYear() < 2020 ||
      mintedAt.getTime() > Date.now() + 24 * 3600_000
    ) {
      return null
    }
    return { blocks: cipher / TURN_STATE_BLOCK_BYTES, mintedAt }
  } catch {
    return null
  }
}

/** 解不出信封时退回字符长度（老判据），别把解不开的当健康。 */
export const isTurnStateHealthy = (blob: string): boolean => {
  const env = decodeTurnState(blob)
  return env ? env.blocks === TURN_STATE_HEALTHY_BLOCKS : blob.length === TURN_STATE_HEALTHY_CHARS
}

/**
 * targetsCodexUpstream 与后端 TargetsChatGPTCodexUpstream() 对齐：
 * IsOpenAIOAuthLike()（openai + oauth/setup-token）∪ IsCPR()（openai + cpr）。
 * 只有这些账号的出站才带 x-codex-turn-state。
 */
export const targetsCodexUpstream = (account: {
  platform?: string | null
  type?: string | null
}): boolean =>
  account?.platform === 'openai' &&
  ['oauth', 'setup-token', 'cpr'].includes(String(account?.type ?? ''))

/**
 * turnStateModelAllowed 与后端 openAITurnStateModelAllowed 对齐：
 * 逗号分隔、大小写不敏感、结尾 * 前缀匹配；名单留空或模型识别不出来时放行。
 */
export const turnStateModelAllowed = (raw: unknown, model: string): boolean => {
  const list = typeof raw === 'string' ? raw.trim() : ''
  if (!list) return true
  const target = model.trim().toLowerCase()
  if (!target) return true
  return list.split(',').some((entry) => {
    const e = entry.trim().toLowerCase()
    if (!e) return false
    return e.endsWith('*') ? target.startsWith(e.slice(0, -1)) : e === target
  })
}
