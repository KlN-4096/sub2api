package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/gjson"
)

// 真客户端默认开启 enable_request_compression（16ff14c: features/src/lib.rs:1221-1224，Stable、
// default_enabled=true）：ChatGPT 登录态对 OpenAI provider 的每条 /responses 请求体做 zstd 压缩并带
// Content-Encoding: zstd（core/src/client.rs:1534-1541 responses_request_compression →
// http-client/src/request.rs:192-222，zstd::stream::encode_all(body, 3)）。compact
// （codex-api/src/endpoint/compact.rs 没有 compression）、WS 帧、/models GET 都不压。
//
// 帧头对齐 libzstd 流式编码的默认形态：encode_all 走流式（io::copy 先 ZSTD_e_continue，再 finish），
// 没有 pledged size → 不写内容长度、非 single segment；ZSTD_c_checksumFlag 默认 0 → 无校验和；
// level 3 的 windowLog 21 → 2MB 窗口。即前六个字节固定为 28 b5 2f fd 00 58。klauspost 会按输入大小
// 自选 single segment / 内容长度 / 窗口描述，所以帧头由 normalizeOwnedCodexZstdFrameHeader 统一改写；块内容
// （偏移不超过 2MB 窗口）不动，由各自实现决定，不作对齐。
const (
	codexRequestZstdContentEncoding = "zstd"
	codexRequestZstdWindowSize      = 1 << 21
)

var codexRequestZstdEncoders = sync.Pool{New: func() any {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(false),
		zstd.WithWindowSize(codexRequestZstdWindowSize),
		zstd.WithSingleSegment(false),
		zstd.WithEncoderConcurrency(1),
		zstd.WithLowerEncoderMem(true),
	)
	if err != nil {
		panic(fmt.Sprintf("codex request zstd encoder: %v", err))
	}
	return enc
}}

// codexRequestBodyCompressionEnabled：双开账号（只可能是 OAuth 类凭据，对应真客户端的 ChatGPT 登录态）
// 且出站 URL 是 /responses。按出站 URL 判定，与顶层字段序同一条规则；/responses/compact 不匹配。
func codexRequestBodyCompressionEnabled(c *gin.Context, account *Account, targetURL string) bool {
	if !codexDeviceWireProfileEnabled(c, account) {
		return false
	}
	parsed, err := url.Parse(targetURL)
	if err != nil {
		return false
	}
	return strings.HasSuffix(strings.TrimRight(parsed.Path, "/"), "/responses")
}

// finalizeCodexWireBody 是两个 HTTP 构造器（buildUpstreamRequest / buildUpstreamRequestOpenAIPassthrough）
// 共用的请求体定稿链，步骤顺序承重。targetURL 是按路径分流用的出站 URL（自建图片请求是覆盖后的
// wireTargetURL）。返回的 ctx 可能挂上网关池确认体，调用方必须拿它构造请求；body 是定稿后的明文，
// 供路由提示与诊断日志读取；wire / enc 是上线字节与 Content-Encoding。
func (s *OpenAIGatewayService) finalizeCodexWireBody(ctx context.Context, c *gin.Context, account *Account, targetURL string, body []byte) (context.Context, []byte, []byte, string, error) {
	body, err := filterCodexCompactAccessPrograms(c, account, body)
	if err != nil {
		return ctx, nil, nil, "", fmt.Errorf("filter compact access programs: %w", err)
	}

	// codex 0.156 的两处体内形态：guardian 计费标记、turn-metadata 的 model 跟随出站体。
	body = applyCodexGuardianCreditsRequested(c, account, targetURL, body)
	body = alignCodexTurnMetadataExecutionBody(c, account, targetURL, body)

	// 顶层键序：只重排，不改任何值（下面的 user_location 改写与压缩仍会动体）。
	body = applyCodexBodyFieldOrder(c, account, targetURL, body)

	// environment_context 的时区已在各入口归一化之前投影过（见 Forward 开头的注释）；同一份出口
	// 解析结果在这里投影到 web_search 的 user_location，否则出站是"出口时区 + 客户端本机城市"
	// （openai_codex_wire_user_location.go）。
	body = rewriteCodexWebSearchUserLocation(c, account, body)

	// 趁 body 还是明文，把出站模型名记进本次请求的网关池 sink：queue 档的预热垫话必须用同一个
	// 模型（后置回声复验沿用本次业务模型），而它跑在传输层、只拿到 *http.Request ——
	// 到那时双开账号的体已经是 zstd 了，解不出来（openai_gwpool_warm.go 的 gatewayPoolWarmModel）。
	// 没挂 sink 的路径这一行是空操作。
	openAIGatewayPoolSinkFrom(ctx).noteModel(gjson.GetBytes(body, "model").String())
	if account.UsesGatewayPool() || account.qualityGuardEnabled() {
		ctx = context.WithValue(ctx, gatewayPoolConfirmBodyKey{}, gatewayPoolConfirmBody(body))
	}
	if account.qualityGuardEnabled() {
		ctx = withQualityGuardRequest(ctx, c, account, body)
	}

	// 上线字节：双开 /responses 的请求体按真客户端默认做 zstd 压缩。
	wire, enc, err := compressCodexRequestBody(c, account, targetURL, body)
	return ctx, body, wire, enc, err
}

// compressCodexRequestBody 返回上线的请求体字节与 Content-Encoding。不压缩时原样返回、编码为空。
// 调用方继续持有明文 body 供路由提示与诊断日志读取；每次构造（含重试与 failover）独立压缩。
func compressCodexRequestBody(c *gin.Context, account *Account, targetURL string, body []byte) ([]byte, string, error) {
	if len(body) == 0 || !codexRequestBodyCompressionEnabled(c, account, targetURL) {
		return body, "", nil // 空体不压：编码器对空输入返回零字节，真 /responses 也不存在空体
	}
	wire, err := encodeCodexZstdRequestBody(body)
	if err != nil {
		return nil, "", err
	}
	return wire, codexRequestZstdContentEncoding, nil
}

// encodeCodexZstdRequestBody 按真客户端的 zstd 帧形态压缩请求体。
func encodeCodexZstdRequestBody(body []byte) ([]byte, error) {
	enc, ok := codexRequestZstdEncoders.Get().(*zstd.Encoder)
	if !ok {
		return nil, errors.New("codex request zstd encoder pool returned unexpected type")
	}
	defer codexRequestZstdEncoders.Put(enc)
	frame := enc.EncodeAll(body, make([]byte, 0, len(body)/3+64))
	wire, err := normalizeOwnedCodexZstdFrameHeader(frame)
	if err != nil {
		return nil, fmt.Errorf("zstd compress codex request body: %w", err)
	}
	return wire, nil
}

var (
	codexZstdFrameMagic        = []byte{0x28, 0xb5, 0x2f, 0xfd}
	codexZstdStreamFrameHeader = []byte{0x00, 0x58} // FHD：无 FCS / 非 single segment / 无校验和 / 无字典；窗口描述 2^21
)

// normalizeOwnedCodexZstdFrameHeader 把编码器写出的帧头原地换成 libzstd 流式默认帧头，块内容原样保留。
// 编码器窗口固定 2MB，块内偏移不会超过它，所以声明 2MB 窗口且不写内容长度对任何解码器都成立。
// Only EncodeAll's newly allocated output is owned. Never rewrite a caller's
// plaintext body or a shared frame in place.
func normalizeOwnedCodexZstdFrameHeader(frame []byte) ([]byte, error) {
	magic := len(codexZstdFrameMagic)
	if len(frame) < magic+1 || !bytes.Equal(frame[:magic], codexZstdFrameMagic) {
		return nil, errors.New("unexpected zstd frame magic")
	}
	fhd := frame[magic]
	if fhd&0x1f != 0 { // 字典 ID / 校验和 / 保留位 / 未用位都不是 libzstd 默认形态
		return nil, errors.New("unexpected zstd frame header descriptor flags")
	}
	single := fhd&(1<<5) != 0
	headerLen := 1
	if !single {
		headerLen++ // Window_Descriptor
	}
	switch fhd >> 6 { // Frame_Content_Size_flag
	case 0:
		if single {
			headerLen++
		}
	case 1:
		headerLen += 2
	case 2:
		headerLen += 4
	default:
		headerLen += 8
	}
	if len(frame) < magic+headerLen {
		return nil, errors.New("short zstd frame header")
	}
	if headerLen == len(codexZstdStreamFrameHeader) && bytes.Equal(frame[magic:magic+headerLen], codexZstdStreamFrameHeader) {
		return frame, nil
	}
	// Every accepted frame has at least a two-byte header. The normalized
	// header is never longer, so the block payload can move left in place.
	length := magic + len(codexZstdStreamFrameHeader) + copy(frame[magic+len(codexZstdStreamFrameHeader):], frame[magic+headerLen:])
	copy(frame[magic:], codexZstdStreamFrameHeader)
	return frame[:length], nil
}
