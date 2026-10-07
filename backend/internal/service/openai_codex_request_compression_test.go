//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// 真客户端默认压缩 /responses 请求体（features/src/lib.rs:1221-1224 enable_request_compression Stable +
// default_enabled；core/src/client.rs:1534-1541；http-client/src/request.rs:192-222 encode_all level 3）。
// libzstd 流式 level 3 的帧头：magic 28 b5 2f fd、FHD 00（无内容长度、无校验和、非 single segment）、
// 窗口描述 58（2MB）。期望值写成独立字面量，不引用生产常量。
var wantCodexZstdFrameHead = []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58}

const codexTestResponsesURL = "https://chatgpt.com/backend-api/codex/responses"

func TestCodexDeviceWireProfileResponsesRequestBodyZstd(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		name := "map"
		if passthrough {
			name = "raw"
		}
		t.Run(name, func(t *testing.T) {
			body := wireProfileTestBody(t)
			c := newConvTestContext(t, body)
			account := wireProfileTestAccount(true)
			account.Extra["openai_passthrough"] = passthrough
			svc, up := wireProfileTestService()
			_, _ = svc.Forward(context.Background(), c, account, body)
			require.NotNil(t, up.lastReq)
			require.Equal(t, "zstd", up.lastReq.Header.Get("Content-Encoding"))
			require.Equal(t, "application/json", up.lastReq.Header.Get("content-type"))
			require.Greater(t, len(up.lastRawBody), len(wantCodexZstdFrameHead))
			require.Equal(t, wantCodexZstdFrameHead, up.lastRawBody[:len(wantCodexZstdFrameHead)],
				"帧头必须与 libzstd 流式 level 3 默认形态一致（无 FCS、无校验和、2MB 窗口）")
			require.Equal(t, int64(len(up.lastRawBody)), up.lastReq.ContentLength, "Content-Length 是压缩后的长度")
			require.False(t, bytes.HasPrefix(bytes.TrimSpace(up.lastRawBody), []byte("{")), "明文不得直接上线")
			require.True(t, gjson.ValidBytes(up.lastBody), "解压后是明文 JSON")
			require.NotEmpty(t, gjson.GetBytes(up.lastBody, "model").String())
			require.Equal(t, up.lastReq.Header.Get("session-id"), gjson.GetBytes(up.lastBody, "prompt_cache_key").String(),
				"解压后的体与出站头仍同源")
		})
	}
}

func TestCodexDeviceWireProfileCompactRequestBodyPlain(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		name := "map"
		if passthrough {
			name = "raw"
		}
		t.Run(name, func(t *testing.T) {
			body := wireProfileTestBody(t)
			c := newConvTestContext(t, body)
			c.Request.URL.Path = "/v1/responses/compact"
			account := wireProfileTestAccount(true)
			account.Extra["openai_passthrough"] = passthrough
			svc, up := wireProfileTestService()
			_, _ = svc.Forward(context.Background(), c, account, body)
			require.NotNil(t, up.lastReq)
			require.True(t, strings.HasSuffix(up.lastReq.URL.Path, "/responses/compact"))
			require.Empty(t, up.lastReq.Header.Get("Content-Encoding"), "compact 不压（endpoint/compact.rs 无 compression）")
			require.True(t, bytes.HasPrefix(bytes.TrimSpace(up.lastRawBody), []byte("{")))
			require.Equal(t, up.lastRawBody, up.lastBody)
		})
	}
}

func TestCodexRequestBodyCompressionOnlyWhenWireProfileEnabled(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		name := "map"
		if passthrough {
			name = "raw"
		}
		t.Run(name, func(t *testing.T) {
			body := wireProfileTestBody(t)
			c := newConvTestContext(t, body)
			account := wireProfileTestAccount(false)
			account.Extra["openai_passthrough"] = passthrough
			svc, up := wireProfileTestService()
			_, _ = svc.Forward(context.Background(), c, account, body)
			require.NotNil(t, up.lastReq)
			require.Empty(t, up.lastReq.Header.Get("Content-Encoding"), "非双开逐字节不变：不压")
			require.True(t, bytes.HasPrefix(bytes.TrimSpace(up.lastRawBody), []byte("{")))
			require.Equal(t, up.lastRawBody, up.lastBody)
		})
	}
}

func TestCompressCodexRequestBodyRoundTripAndScope(t *testing.T) {
	c := newConvTestContext(t, []byte(`{}`))
	account := wireProfileTestAccount(true)
	body := []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":[{"type":"input_text","text":"hello hello hello hello"}]}],"client_metadata":{"session_id":"s"}}`)

	first, encoding, err := compressCodexRequestBody(c, account, codexTestResponsesURL, body)
	require.NoError(t, err)
	require.Equal(t, "zstd", encoding)
	require.Equal(t, wantCodexZstdFrameHead, first[:len(wantCodexZstdFrameHead)])
	dec, err := zstd.NewReader(bytes.NewReader(first))
	require.NoError(t, err)
	plain, err := io.ReadAll(dec)
	dec.Close()
	require.NoError(t, err)
	require.Equal(t, body, plain, "解压后逐字节等于明文")

	// 各种大小（klauspost 会按大小切换 single segment / 内容长度字段 / 窗口描述）帧头都必须被统一，且可解压。
	for _, size := range []int{100, 5000, 70000, 300000} {
		large := []byte(`{"model":"gpt-5.4","input":"` + strings.Repeat("abc ", size/4) + `"}`)
		frame, encoding, err := compressCodexRequestBody(c, account, codexTestResponsesURL, large)
		require.NoError(t, err, size)
		require.Equal(t, "zstd", encoding, size)
		require.Equal(t, wantCodexZstdFrameHead, frame[:len(wantCodexZstdFrameHead)], "size %d", size)
		dec, err := zstd.NewReader(bytes.NewReader(frame))
		require.NoError(t, err, size)
		plain, err := io.ReadAll(dec)
		dec.Close()
		require.NoError(t, err, size)
		require.Equal(t, large, plain, "size %d", size)
	}

	// 每次构造独立压缩（重试 / failover 各自一份），且结果确定。
	second, _, err := compressCodexRequestBody(c, account, codexTestResponsesURL, body)
	require.NoError(t, err)
	require.Equal(t, first, second)

	for _, target := range []string{
		"https://chatgpt.com/backend-api/codex/responses/compact",
		"https://chatgpt.com/backend-api/codex/alpha/search",
		"https://chatgpt.com/backend-api/codex/models",
	} {
		out, encoding, err := compressCodexRequestBody(c, account, target, body)
		require.NoError(t, err, target)
		require.Empty(t, encoding, target)
		require.Equal(t, body, out, target)
	}
	out, encoding, err := compressCodexRequestBody(c, wireProfileTestAccount(false), codexTestResponsesURL, body)
	require.NoError(t, err)
	require.Empty(t, encoding, "非双开不压")
	require.Equal(t, body, out)
}

// 双开 + PAT（at-…）账号的 /alpha/search 走 hosted web_search 的 /responses 兜底
// （openai_alpha_search.go forwardAlphaSearchViaResponsesWebSearch）。真客户端的 PersonalAccessToken
// 同样 uses_codex_backend（protocol/src/auth.rs:54-61），这条 /responses 也压；同一账号不得压缩与明文混发。
func TestCodexDeviceWireProfilePATAlphaSearchFallbackBodyZstd(t *testing.T) {
	for name, enabled := range map[string]bool{"wire_profile_off": false, "wire_profile_on": true} {
		t.Run(name, func(t *testing.T) {
			body := []byte(`{"id":"search-session","model":"gpt-5.4","commands":{"search_query":[{"q":"OpenAI news"}]}}`)
			c := newConvTestContext(t, body)
			c.Request.URL.Path = "/v1/alpha/search"
			account := wireProfileTestAccount(enabled)
			account.Credentials["auth_mode"] = OpenAIAuthModePersonalAccessToken
			account.Credentials["access_token"] = "at-offline-token"
			svc, up := wireProfileTestService()
			up.resp = &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(alphaSearchResponsesSSE("offline result"))),
			}
			_, err := svc.ForwardAlphaSearch(context.Background(), c, account, body)
			require.NoError(t, err)
			require.NotNil(t, up.lastReq)
			require.Equal(t, codexTestResponsesURL, up.lastReq.URL.String())
			require.Equal(t, "web_search", gjson.GetBytes(up.lastBody, "tools.0.type").String())
			if !enabled {
				require.Empty(t, up.lastReq.Header.Get("Content-Encoding"))
				require.Equal(t, up.lastRawBody, up.lastBody)
				return
			}
			require.Equal(t, "zstd", up.lastReq.Header.Get("Content-Encoding"))
			require.Equal(t, wantCodexZstdFrameHead, up.lastRawBody[:len(wantCodexZstdFrameHead)])
			require.Equal(t, int64(len(up.lastRawBody)), up.lastReq.ContentLength)
			require.False(t, bytes.HasPrefix(bytes.TrimSpace(up.lastRawBody), []byte("{")))
		})
	}
}

// 空体不压：klauspost 对空输入返回零字节，帧头改写会报错把请求变成 500；真 /responses 不存在空体，原样透传即可。
func TestCompressCodexRequestBodyEmptyBodyStaysPlain(t *testing.T) {
	c := newConvTestContext(t, nil)
	account := wireProfileTestAccount(true)
	wire, encoding, err := compressCodexRequestBody(c, account, codexTestResponsesURL, nil)
	require.NoError(t, err)
	require.Empty(t, encoding)
	require.Empty(t, wire)
}

// 帧头描述符里字典 ID / 校验和 / 保留位 / 未用位任一置位都不是 libzstd 默认形态，改写前必须拒绝而不是静默抹掉。
func TestNormalizeCodexZstdFrameHeaderRejectsFlagBits(t *testing.T) {
	for name, fhd := range map[string]byte{"dict_id": 0x01, "checksum": 1 << 2, "reserved": 1 << 3, "unused": 1 << 4} {
		t.Run(name, func(t *testing.T) {
			frame := []byte{0x28, 0xb5, 0x2f, 0xfd, fhd, 0x58, 0x01, 0x00, 0x00}
			_, err := normalizeCodexZstdFrameHeader(frame)
			require.Error(t, err)
		})
	}
}
