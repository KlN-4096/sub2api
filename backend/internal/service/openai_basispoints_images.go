package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// BPS 不收用户消息里内嵌的 data: 图片，但收自己附件接口的 file id：这里照 Excel 加载项
// 「上传文件」按钮的做法，用同一套鉴权头把图片 POST 到 /basispoints/api/attachments，
// 请求里改按返回的 openai_file_id 引用。同一张图（按账号 × sha256）只传一次。
// 图片和请求其它内容一样只发往 bps.openai.com，不经任何第三方，也不需要公网可达的图床。

const (
	openAIBasisPointsMaxImageBytes = 20 << 20
	openAIBasisPointsImageCacheCap = 256
)

var openAIBasisPointsImageExtensions = map[string]string{
	"image/png": "png", "image/jpeg": "jpg", "image/gif": "gif", "image/webp": "webp",
}

// 附件在服务端能活多久不清楚：缓存 30 分钟，过期就重传。
const openAIBasisPointsImageTTL = 30 * time.Minute

type openAIBasisPointsImageEntry struct {
	fileID string
	at     time.Time
}

var openAIBasisPointsImages = newBPSLRU(openAIBasisPointsImageCacheCap, 0)

func openAIBasisPointsCachedImage(key string, now time.Time) (string, bool) {
	value, ok := openAIBasisPointsImages.get(key)
	if !ok {
		return "", false
	}
	entry, _ := value.(openAIBasisPointsImageEntry)
	if now.Sub(entry.at) > openAIBasisPointsImageTTL {
		return "", false
	}
	return entry.fileID, true
}

// decodeOpenAIBasisPointsDataURL 解 data:<media>;base64,<payload>。
func decodeOpenAIBasisPointsDataURL(url string) (string, []byte, bool) {
	header, payload, found := strings.Cut(url, ",")
	if !found || !strings.Contains(header, ";base64") {
		return "", nil, false
	}
	// 客户端声明的 media type 只用来先筛掉「压根不是图」的形态；真正的类型按字节定（见下）。
	// 不能把客户端的串带进出站：它会被原样写进 multipart 的 Content-Type，而
	// multipart.CreatePart 对头值不转义也不校验 —— 等于让持有本站 API key 的人在带着账号主人
	// bearer token 的上传请求里注入任意 MIME 部件头。
	// 形状校验仍然留着（mime.FormatMediaType 会拒掉含 CR/LF、非 token 字符的串）：类型按字节定
	// 之后注入面确实没了，但一个畸形的 data URL 本来就不该当合法输入收下。
	declared, _, _ := strings.Cut(strings.TrimPrefix(header, "data:"), ";")
	declared = strings.ToLower(strings.TrimSpace(declared))
	if !strings.HasPrefix(declared, "image/") || mime.FormatMediaType(declared, nil) != declared {
		return "", nil, false
	}
	// 先按 base64 长度预判，别为一张注定被拒的图先分配几十 MB：4 个字符最多解出 3 字节。
	// 减 2 是因为 padding 最多让这个上界高估 2 字节 —— 不减就会把上限悄悄收紧到比常量小 3 字节，
	// 让常量名不再等于实际行为。真正的判定仍在下面解码之后。
	//
	// 排在 strings.Map 之前：剥空白只会让长度变短，所以对原文成立的上界对剥完更成立，
	// 而放在后面等于「为了省一次分配，先整份拷贝一遍」—— 预判的目的就没了。
	if len(payload)/4*3-2 > openAIBasisPointsMaxImageBytes {
		return "", nil, false
	}
	payload = strings.Map(func(r rune) rune {
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' {
			return -1
		}
		return r
	}, payload)
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		if data, err = base64.RawStdEncoding.DecodeString(strings.TrimRight(payload, "=")); err != nil {
			return "", nil, false
		}
	}
	if len(data) == 0 || len(data) > openAIBasisPointsMaxImageBytes {
		return "", nil, false
	}
	// **类型按字节定，不按客户端声明的那个串定，也绝不默认 png。**
	//
	// BPS 只接受 .jpeg/.jpg/.png/.gif/.webp，别的格式（或者扩展名对不上字节，例如 .jfif、
	// 无后缀）会让**整单** 400 `Expected image type to be a supported format … but got none`；
	// 而历史里一旦带上这张图，这个会话每一轮回放都失败
	// （JaxsonWang/cpa-plugin-oai-basispoints#15 的现场，维护者 v0.2.4 也改成了按字节识别）。
	// 猜成 png 的代价是一样的整单 400，而且客户端拿不到任何定位信息 —— 所以识别不出就在这里判死，
	// 让它变成一条带明确原因的硬报错。
	mediaType := bpsSniffImageMediaType(data)
	if mediaType == "" {
		return "", nil, false
	}
	return mediaType, data, true
}

// bpsSniffImageMediaType 按字节识别，只认 BPS 支持的四种，其余（含识别不出）返回 ""。
// 用 http.DetectContentType 而不是自己写魔数表：它已经覆盖 PNG / JPEG / GIF / WebP 的签名。
// 白名单直接查上面那张扩展名表，不再另写一份 switch —— 两份会各自漂，而
// uploadOpenAIBasisPointsImage 的「这张表必中」恰恰依赖它们同步。
func bpsSniffImageMediaType(data []byte) string {
	mediaType, _, _ := strings.Cut(http.DetectContentType(data), ";")
	if _, ok := openAIBasisPointsImageExtensions[mediaType]; !ok {
		return ""
	}
	return mediaType
}

// openAIBasisPointsUploader 返回本请求用的上传函数：走账号自己的出口，复用 BPS 鉴权头。
func (s *OpenAIGatewayService) openAIBasisPointsUploader(ctx context.Context, c *gin.Context, account *Account, proxyURL string, headers http.Header, secrets ...string) func(string, []byte) (string, error) {
	return func(mediaType string, data []byte) (string, error) {
		digest := sha256.Sum256(data)
		// 键按本站账号：Team 工作区多名成员共用一个 chatgpt_account_id，各自的附件不互认。
		key := fmt.Sprintf("%d\x00%s", account.ID, hex.EncodeToString(digest[:]))
		if fileID, ok := openAIBasisPointsCachedImage(key, time.Now()); ok {
			return fileID, nil
		}
		fileID, err := s.uploadOpenAIBasisPointsImage(ctx, c, account, proxyURL, headers, mediaType, data, hex.EncodeToString(digest[:6]), secrets...)
		if err != nil {
			return "", err
		}
		openAIBasisPointsImages.put(key, openAIBasisPointsImageEntry{fileID: fileID, at: time.Now()}, 0)
		return fileID, nil
	}
}

func (s *OpenAIGatewayService) uploadOpenAIBasisPointsImage(ctx context.Context, c *gin.Context, account *Account, proxyURL string, headers http.Header, mediaType string, data []byte, digest string, secrets ...string) (string, error) {
	// mediaType 只会是 bpsSniffImageMediaType 认过的四种之一（按字节识别），所以这张表必中，
	// 出站的 Content-Type 与 filename= 都是本层的字面量、没有一个字节来自客户端 —— 原来那段
	// 「从客户端子类型推扩展名再消毒」连带它防的注入面一起没有了。
	extension := openAIBasisPointsImageExtensions[mediaType]
	if extension == "" {
		return "", fmt.Errorf("basispoints: unsupported image media type %q", mediaType)
	}
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	partHeader := textproto.MIMEHeader{}
	partHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="picture-%s.%s"`, digest, extension))
	partHeader.Set("Content-Type", mediaType)
	part, err := form.CreatePart(partHeader)
	if err != nil {
		return "", err
	}
	if _, err := part.Write(data); err != nil {
		return "", err
	}
	if err := form.Close(); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, openAIBasisPointsAttachmentsURL, bytes.NewReader(body.Bytes()))
	if err != nil {
		return "", err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))
	req.Header = headers.Clone()
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("Accept", "application/json")
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		// 附件上传排在主 /responses **之前**，所以带内联图片的请求上代理死亡只会经过这里。
		// 三件事与主请求出口逐条对齐：记 ops 事件（少了它账号被摘池 10 分钟而 ops 里查不到任何
		// 上游错误）、持久错误摘池、reason 用专用的 image_upload_transport（在
		// openAIBasisPointsReasonIsAccountFault 表里 → 罚分）而不是形态类的 image_upload。
		//
		// 这里的 ctx 是**客户端 ctx**（uploader 跑在 bridge.prepare 里，早于主请求那次
		// detachUpstreamContext），所以 isClientCanceledTransportError 在这条路上是真守卫，
		// 不是主请求注释里说的那种便宜保险。
		// client-cancel 那道守卫排在**记 ops 之前**：与 handleOpenAIUpstreamTransportError 同口径
		// （它的文档原话是「记进 ops，**除客户端断开外**」）。客户端自己走了不是上游错误，记了就是
		// 一条假的 failover 行。
		if isClientCanceledTransportError(ctx, err) {
			return "", err
		}
		safeErr := sanitizeUpstreamErrorMessage(err.Error())
		s.appendOpenAIBasisPointsUpstreamError(c, account, 0, "", safeErr, secrets...)
		if !classifyUpstreamTransportError(err).Persistent {
			// **非持久（超时这类）不罚分**，与 upstream_silent / stream_eof 同一条理由：那更可能是
			// BPS 通道自己超时，罚分会把流量推给没开开关的账号 = 掺杂，正好违背这个功能的口径。
			// 只有持久错误（代理死了）才既摘池又罚分 —— 那时候确实是这个账号的出口发不出去。
			return "", bpsNative("image_upload_timeout")
		}
		s.tempUnscheduleOpenAITransportError(ctx, account, safeErr)
		return "", bpsNative("image_upload_transport")
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("basispoints attachment upload status %d", resp.StatusCode)
	}
	fileID := strings.TrimSpace(gjson.GetBytes(raw, "openai_file_id").String())
	if fileID == "" {
		return "", errors.New("basispoints attachment upload returned no file id")
	}
	return fileID, nil
}
