package repository

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestDecompressResponseBodyZstdUsage(t *testing.T) {
	payload := []byte(`{"usage":{"input_tokens":123,"output_tokens":45,"cache_read_input_tokens":67}}`)
	compressed := compressZstd(t, payload)
	resp := newEncodedResponse("zstd", compressed)

	decompressResponseBody(resp)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, payload, body)
	require.Equal(t, int64(123), gjson.GetBytes(body, "usage.input_tokens").Int())
	require.Equal(t, int64(45), gjson.GetBytes(body, "usage.output_tokens").Int())
	require.Equal(t, int64(67), gjson.GetBytes(body, "usage.cache_read_input_tokens").Int())
	require.Empty(t, resp.Header.Get("Content-Encoding"))
	require.Empty(t, resp.Header.Get("Content-Length"))
	require.Equal(t, int64(-1), resp.ContentLength)
	require.NoError(t, resp.Body.Close())
}

func TestDecompressResponseBodyExistingEncodings(t *testing.T) {
	payload := []byte(`{"ok":true}`)
	tests := []struct {
		name     string
		encoding string
		compress func(*testing.T, []byte) []byte
	}{
		{name: "gzip", encoding: "gzip", compress: compressGzip},
		{name: "brotli", encoding: "br", compress: compressBrotli},
		{name: "deflate", encoding: "deflate", compress: compressDeflate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := newEncodedResponse(tt.encoding, tt.compress(t, payload))

			decompressResponseBody(resp)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, payload, body)
			require.Empty(t, resp.Header.Get("Content-Encoding"))
			require.Empty(t, resp.Header.Get("Content-Length"))
			require.Equal(t, int64(-1), resp.ContentLength)
			require.NoError(t, resp.Body.Close())
		})
	}
}

func TestDecompressResponseBodyWithoutEncodingLeavesBodyUntouched(t *testing.T) {
	originalBody := &responseTestBody{Reader: bytes.NewReader([]byte("plain"))}
	resp := &http.Response{
		Header:        make(http.Header),
		Body:          originalBody,
		ContentLength: 5,
	}

	decompressResponseBody(resp)

	require.Same(t, originalBody, resp.Body)
	require.Equal(t, int64(5), resp.ContentLength)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "plain", string(body))
	require.NoError(t, resp.Body.Close())
}

func TestDecompressResponseBodyInvalidZstdWarnsAndPreservesBody(t *testing.T) {
	previousLogger := slog.Default()
	var logOutput bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logOutput, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	payload := []byte("not a zstd response")
	resp := newEncodedResponse("zstd", payload)

	require.NotPanics(t, func() {
		decompressResponseBody(resp)
	})

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, payload, body)
	require.Equal(t, "zstd", resp.Header.Get("Content-Encoding"))
	require.Equal(t, int64(len(payload)), resp.ContentLength)
	require.Contains(t, logOutput.String(), "msg=zstd_decompress_failed")
	require.NoError(t, resp.Body.Close())
}

func TestDecompressResponseBodyEmptyZstdWarnsAndPreservesBody(t *testing.T) {
	previousLogger := slog.Default()
	var logOutput bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logOutput, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	resp := newEncodedResponse("zstd", nil)

	require.NotPanics(t, func() {
		decompressResponseBody(resp)
	})

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Empty(t, body)
	require.Equal(t, "zstd", resp.Header.Get("Content-Encoding"))
	require.Equal(t, int64(0), resp.ContentLength)
	require.Contains(t, logOutput.String(), "msg=zstd_decompress_failed")
	require.NoError(t, resp.Body.Close())
}

type responseTestBody struct {
	io.Reader
}

func (b *responseTestBody) Close() error {
	return nil
}

func newEncodedResponse(encoding string, body []byte) *http.Response {
	header := make(http.Header)
	header.Set("Content-Encoding", encoding)
	header.Set("Content-Length", "123")
	return &http.Response{
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func compressZstd(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = zw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func compressGzip(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func compressBrotli(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := brotli.NewWriter(&buf)
	_, err := zw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func compressDeflate(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := flate.NewWriter(&buf, flate.DefaultCompression)
	require.NoError(t, err)
	_, err = zw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// 上游声称 gzip 但体不是 gzip 时，网关不能把体吃掉一截再原样标着 Content-Encoding: gzip
// 发给下游——客户端拿到的是"声称 gzip 的残缺流"，比原样透传更糟。
// 失败路径必须零消费：体逐字节保持原样，头也保持原样（那才是上游真正发来的东西）。
func TestDecompressResponseBodyInvalidGzipPreservesBodyBytes(t *testing.T) {
	previousLogger := slog.Default()
	var logOutput bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logOutput, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	payload := []byte(`{"error":{"message":"upstream said gzip but sent plain json"}}`)
	resp := newEncodedResponse("gzip", payload)

	require.NotPanics(t, func() { decompressResponseBody(resp) })

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, payload, body, "解压失败时不能吞掉任何字节")
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	require.Equal(t, int64(len(payload)), resp.ContentLength)
	require.Contains(t, logOutput.String(), "msg=gzip_decompress_failed")
	require.NoError(t, resp.Body.Close())
}

// 初始化已消费部分 gzip 头后，不能把剩余字节当作完整原文返回，也不能吞掉读取错误。
// 错误留在 Body.Read，响应头与关闭链不变，不升级成 HTTPUpstream.Do 的请求错误。
func TestDecompressResponseBodyInvalidGzipHeaderReturnsReadError(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		readErr error
		wantErr error
	}{
		{name: "short_fixed_header", payload: []byte{0x1f, 0x8b, 8}, wantErr: io.ErrUnexpectedEOF},
		{name: "truncated_extra", payload: []byte{0x1f, 0x8b, 8, 4, 0, 0, 0, 0, 0, 255, 2, 0, 'x'}, wantErr: io.ErrUnexpectedEOF},
		{name: "unterminated_name", payload: []byte{0x1f, 0x8b, 8, 8, 0, 0, 0, 0, 0, 255, 'x'}, wantErr: io.ErrUnexpectedEOF},
		{name: "invalid_header_checksum", payload: []byte{0x1f, 0x8b, 8, 2, 0, 0, 0, 0, 0, 255, 0, 0, 'x'}, wantErr: gzip.ErrHeader},
		{name: "canceled_header_read", payload: []byte{0x1f, 0x8b, 8}, readErr: context.Canceled, wantErr: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var source io.Reader = bytes.NewReader(tt.payload)
			if tt.readErr != nil {
				source = io.MultiReader(source, readerFunc(func([]byte) (int, error) { return 0, tt.readErr }))
			}
			sourceCloses, released := 0, 0
			resp := newEncodedResponse("gzip", tt.payload)
			resp.Body = wrapTrackedBody(io.NopCloser(source), func() { sourceCloses++ })

			decompressResponseBody(resp)
			resp.Body = wrapTrackedBody(resp.Body, func() { released++ })
			t.Cleanup(func() { _ = resp.Body.Close() })

			body, err := io.ReadAll(resp.Body)
			require.ErrorIs(t, err, tt.wantErr)
			require.Empty(t, body, "坏 gzip 头之后的残余字节不得伪装成完整响应")
			n, err := resp.Body.Read(make([]byte, 1))
			require.Zero(t, n)
			require.ErrorIs(t, err, tt.wantErr, "失败后的后续读取不得退化成正常 EOF")
			require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
			require.Equal(t, "123", resp.Header.Get("Content-Length"))
			require.Equal(t, int64(len(tt.payload)), resp.ContentLength)
			require.Zero(t, sourceCloses)
			require.Zero(t, released)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, 1, sourceCloses)
			require.Equal(t, 1, released, "解码错误仍须释放在途请求计数")
		})
	}
}

// 带 Content-Encoding 的空体（204/304、空错误体）是完全正常的场景，不该刷 WARN。
func TestDecompressResponseBodyEmptyGzipDoesNotWarn(t *testing.T) {
	previousLogger := slog.Default()
	var logOutput bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logOutput, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	resp := newEncodedResponse("gzip", nil)

	require.NotPanics(t, func() { decompressResponseBody(resp) })

	require.NotContains(t, logOutput.String(), "gzip_decompress_failed")
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	require.NoError(t, resp.Body.Close())
}

// 流式回归：探测头部时读取的字节数不能超过旧实现的 10 字节。gzip 的 SSE 流首个 flush
// 可能只够一个 gzip 头就停下等下一个事件，多探一个字节就会把流式拖成卡住。
func TestDecompressResponseBodyGzipDoesNotOverReadOnStreamingBody(t *testing.T) {
	full := compressGzip(t, []byte(`{"ok":true}`))
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	resp := newEncodedResponse("gzip", nil)
	resp.Body = &responseTestBody{Reader: io.MultiReader(
		bytes.NewReader(full[:10]), // 恰好一个 gzip 头
		readerFunc(func([]byte) (int, error) { <-blocked; return 0, io.EOF }),
	)}

	done := make(chan struct{})
	go func() { defer close(done); decompressResponseBody(resp) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("探测头部时读过了头：流式响应会被拖住")
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
