/**
 * 造一条真的 Fernet 信封：0x80 | 8B 大端铸造戳 | 16B IV | blocks × 16B 密文 | 32B HMAC。
 *
 * 必须用真信封：`'g'.repeat(292)` 这种假样本首字节是 0x82，decodeTurnState 直接返回
 * null，测试就只会走「退回字符长度」的兜底分支，块数逻辑一行都碰不到。
 */
export const turnStateFixture = (mintedSec: number, blocks: number): string => {
  const bytes = new Uint8Array(1 + 8 + 16 + blocks * 16 + 32)
  bytes[0] = 0x80
  let ts = mintedSec
  for (let i = 8; i >= 1; i--) {
    bytes[i] = ts % 256
    ts = Math.floor(ts / 256)
  }
  let bin = ''
  bytes.forEach((b) => (bin += String.fromCharCode(b)))
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_')
}
