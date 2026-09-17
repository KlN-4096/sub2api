package service

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
	"time"
)

// turn-state 是 Fernet 信封，结构已用 1300 条现网样本核实（1300/1300 首字节 0x80）：
//
//	0x80 | 8B 大端铸造时间戳（明文） | 16B IV | AES-CBC 密文 | 32B HMAC-SHA256
//
// 两个可用信息：
//
//  1. 时间戳是**铸造时刻**，不是过期时刻。实测「观测时刻 − 时间戳」全为正，
//     中位 10 秒、最长 367 秒；同一条 blob 的时间戳恒定（1300 个唯一 blob，0 漂移）。
//     过期由验证方自己定 TTL，信封里没有这个值——别把它当过期时间用。
//
//  2. 密文块数把明文框进一个 16 字节的窗口。实测只见过两种：
//     292 字符 → 密文 160B = 10 块 → 明文 144–159B（1245 条）
//     312 字符 → 密文 176B = 11 块 → 明文 160–175B（55 条）
//     运维口径「292 = 不降智」等价于「密文 10 块」。用块数而不是字符长度做判据：
//     字符长度受 base64 padding 影响，块数还能如实报出第三种取值。
//
// **块数只把明文框进 16 字节的窗口，所以这是疑似判据，不是确证。**
const (
	// openAITurnStateFernetOverhead = 版本 1 + 时间戳 8 + IV 16 + HMAC 32。
	openAITurnStateFernetOverhead = 1 + 8 + 16 + 32
	openAITurnStateFernetVersion  = 0x80
	openAITurnStateAESBlockBytes  = 16
	// openAIHealthyTurnStateBlocks 是「不降智」的密文块数基线。
	openAIHealthyTurnStateBlocks = 10
)

// openAITurnStateEnvelope 是解出来的信封头部信息。
type openAITurnStateEnvelope struct {
	MintedAt time.Time
	// CipherBlocks 是密文的 AES 块数；0 表示解不出来。
	CipherBlocks int
}

// parseOpenAITurnStateEnvelope 解 turn-state 的明文头部。
//
// 只读版本字节、时间戳和长度——不解密、不校验 HMAC（我们没有密钥，也不需要）。
// 解不出来时返回零值和 false：宁可退回调用方的兜底，也不要拿半个结构当判据。
func parseOpenAITurnStateEnvelope(blob string) (openAITurnStateEnvelope, bool) {
	blob = strings.TrimSpace(blob)
	if blob == "" {
		return openAITurnStateEnvelope{}, false
	}
	// 上游发的是 urlsafe base64；padding 有没有都接着。
	if pad := len(blob) % 4; pad != 0 {
		blob += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(blob)
	if err != nil {
		return openAITurnStateEnvelope{}, false
	}
	if len(raw) <= openAITurnStateFernetOverhead || raw[0] != openAITurnStateFernetVersion {
		return openAITurnStateEnvelope{}, false
	}
	cipherLen := len(raw) - openAITurnStateFernetOverhead
	if cipherLen%openAITurnStateAESBlockBytes != 0 {
		return openAITurnStateEnvelope{}, false
	}
	ts := int64(binary.BigEndian.Uint64(raw[1:9]))
	minted := time.Unix(ts, 0).UTC()
	// 兜一道合理性：时间戳字段解错位时会得到天文数字，拿它算年龄会让所有候选
	// 要么永远新鲜要么永远过期。窗口取宽一点，只拦明显离谱的值。
	if minted.Year() < 2020 || minted.After(time.Now().Add(24*time.Hour)) {
		return openAITurnStateEnvelope{}, false
	}
	return openAITurnStateEnvelope{
		MintedAt:     minted,
		CipherBlocks: cipherLen / openAITurnStateAESBlockBytes,
	}, true
}

// openAITurnStateHealthy 判断一条 turn-state 是否落在「不降智」基线上。
//
// 解不出信封时退回字符长度（老判据）：宁可按旧口径判，也不要把一条解不开的
// blob 默认当健康——那会让它进候选池，之后每次注入都白费一轮观测。
func openAITurnStateHealthy(blob string) bool {
	if env, ok := parseOpenAITurnStateEnvelope(blob); ok {
		return env.CipherBlocks == openAIHealthyTurnStateBlocks
	}
	return len(strings.TrimSpace(blob)) == openAIHealthyTurnStateLen
}

// openAITurnStateMintedAt 返回铸造时刻；解不出来时用 fallback（通常是观测时刻）。
func openAITurnStateMintedAt(blob string, fallback time.Time) time.Time {
	if env, ok := parseOpenAITurnStateEnvelope(blob); ok {
		return env.MintedAt
	}
	return fallback
}
