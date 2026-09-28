/**
 * X-Codex-Turn-State 是标准 Fernet 令牌：
 *   0x80 版本 | 8 字节大端铸造时间戳（明文） | 16 字节 IV | AES-CBC 密文 | 32 字节 HMAC
 * 57 字节固定开销，其余是 16 的整数倍，所以块数与字符数一一对应（见 TURN_STATE_SHAPES）。
 *
 * 判据用密文块数而不是字符长度：块数只把明文框进 16 字节的窗口，所以这是疑似判据；
 * 但它比字符长度稳，信封解不开时才退回长度。
 */
export const TURN_STATE_FERNET_OVERHEAD = 1 + 8 + 16 + 32
export const TURN_STATE_BLOCK_BYTES = 16

/**
 * 正常形态表：individual 10 块 / 292 字符，team 12 块 / 332。降智一律是各自基线上
 * 多出恰好一块（11 块 / 312、13 块 / 356），两种形态各有各的基线，不能拿一个阈值切
 * ——只认 individual 的话，team 号铸出来的每一条都会被判降智。
 *
 * 两种正常块数（10 / 12）互不冲突，两个降智值（11 / 13）也都不在正常集合里，所以
 * 不需要知道账号是哪种类型。与后端 openAITurnStateShapes 一一对应，改一边要改两边。
 */
export const TURN_STATE_SHAPES = [
  { blocks: 10, chars: 292 }, // individual
  { blocks: 12, chars: 332 } // team
] as const

/** 票自铸造起 1 小时有效（对家实时池六张卡的「到期」都精确等于 Fernet 戳 + 1h）。 */
export const TURN_STATE_DEFAULT_TTL_MINUTES = 60

/**
 * 降智暂停（后端 openai_turn_state_hold.go）借 model_rate_limits 存，reason 标本功能。
 * 状态列徽标和猎手行都要按它判，两边必须是同一个串——各抄一份就会在漂移时悄悄错位。
 */
export const TURN_STATE_HOLD_REASON = 'turn_state_hold'

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
  if (env) return TURN_STATE_SHAPES.some((shape) => shape.blocks === env.blocks)
  // trim 与后端 openAITurnStateHealthy 的兜底口径对齐：调用方不保证传进来的值已 trim
  // （UsageTable 就直接传 row.turn_state），差一个空格就会被判成非正常形态。
  return TURN_STATE_SHAPES.some((shape) => shape.chars === blob.trim().length)
}

/**
 * cprOutboundProxy 读 cpr 账号在 CPR 侧绑的出站代理（extra.cpr_outbound_proxy）。
 *
 * **渲染前必须剥掉 userinfo。** CPR 现在返回的是脱敏值（实测只有 scheme://host:port），
 * 但那是上游的行为、不是我们能保证的不变量：CPR 是独立仓库的服务，哪天返回
 * `socks5h://user:pass@host:port`，这个值会一路写进 account.extra、随账号列表下发、
 * 在代理列和编辑弹窗里明文显示密码。渲染点是最后一道闸，不该依赖上游自觉。
 *
 * 这也是本仓库既有的口径：/admin/proxies 对代理密码默认打码 + 显式 reveal 开关，
 * 代理凭据从来不裸奔。
 *
 * 用正则而不是 new URL()：后者对 socks5h:// 这类非特殊 scheme 的 username/password
 * 解析行为不一致，而这里只需要砍掉 `//` 与 `@` 之间的那一段。
 *
 * `[^/]*` 必须贪婪到**最后一个** @，不能写成 `[^/@]*`：密码里带字面 @ 很常见，
 * 停在第一个 @ 只会砍掉密码的前半段，把后半段连同用户名残渣留在页面上
 * （`socks5h://user:p@ss@host` → `socks5h://ss@host`）。后端 sanitizeCPROutboundProxy
 * 走 proxyurl.Parse，Go 按 authority 里最后一个 @ 切——两道闸的口径必须一致，
 * 否则这道「最后一道闸」反而比它要兜底的那道弱。`[^/]` 保证不跨过路径分隔符，
 * 路径里的 @（`http://host:8080/a@b`）不会被误伤。
 */
export const cprOutboundProxy = (account: { extra?: unknown } | null | undefined): string => {
  const raw = (account?.extra as Record<string, unknown> | undefined)?.cpr_outbound_proxy
  if (typeof raw !== 'string') return ''
  return raw.trim().replace(/\/\/[^/]*@/, '//')
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
