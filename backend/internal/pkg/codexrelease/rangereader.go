package codexrelease

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const (
	rangeChunkBytes = 1 << 20
	// maxArtifactDownload 是一次制品检查最多下载的字节数（大头是 core 二进制；安装包本身几百 MB）。
	maxArtifactDownload = 512 << 20
	// maxArtifactSize 是声明大小的上限（官方包几百 MiB），超过视为异常。
	maxArtifactSize = 2 << 30
)

var errDownloadBudget = errors.New("codexrelease: artifact download budget exceeded")

// Identity 是服务端对制品的标识；标识没变即沿用上次结果，什么都不下载。
type Identity struct {
	ETag string `json:"etag,omitempty"`
	Size int64  `json:"size,omitempty"`
}

// rangeReader 是钉在一个 ETag 上的 HTTP Range io.ReaderAt：按 1MiB 对齐分块读、缓存最近
// 一块（zip 读取器会发很多小的顺序读）。
type rangeReader struct {
	ctx      context.Context
	client   *http.Client
	url      string
	identity Identity
	budget   int64
	chunkAt  int64
	chunk    []byte
}

// openRange 用一字节的 Range 请求拿到制品大小与 ETag。
func openRange(ctx context.Context, client *http.Client, url string) (*rangeReader, error) {
	r := &rangeReader{ctx: ctx, client: client, url: url, budget: maxArtifactDownload, chunkAt: -1}
	resp, err := r.get(0, 0, "")
	if err != nil {
		return nil, err
	}
	_ = resp.Body.Close()
	etag := resp.Header.Get("ETag")
	// If-Match 只做强比较，弱 ETag（W/ 前缀）钉不住版本。
	if etag == "" || len(etag) > 128 || strings.HasPrefix(etag, "W/") {
		return nil, fmt.Errorf("codexrelease: %s has no usable ETag", url)
	}
	if r.identity.Size > maxArtifactSize {
		return nil, ErrInvalidArtifact
	}
	r.identity = Identity{ETag: etag, Size: r.identity.Size}
	return r, nil
}

func (r *rangeReader) get(start, end int64, ifMatch string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("Accept-Encoding", "identity")
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	gotStart, gotEnd, size, ok := parseContentRange(resp.Header.Get("Content-Range"))
	if resp.StatusCode != http.StatusPartialContent || !ok || gotStart != start || gotEnd != end ||
		resp.ContentLength != end-start+1 || r.identity.Size != 0 && size != r.identity.Size ||
		ifMatch != "" && resp.Header.Get("ETag") != ifMatch {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("codexrelease: %s range %d-%d: status %d %q", r.url, start, end, resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	r.identity.Size = size
	return resp, nil
}

func parseContentRange(value string) (start, end, size int64, ok bool) {
	rest, found := strings.CutPrefix(value, "bytes ")
	if !found {
		return 0, 0, 0, false
	}
	span, total, found := strings.Cut(rest, "/")
	from, to, found2 := strings.Cut(span, "-")
	if !found || !found2 {
		return 0, 0, 0, false
	}
	var err1, err2, err3 error
	start, err1 = strconv.ParseInt(from, 10, 64)
	end, err2 = strconv.ParseInt(to, 10, 64)
	size, err3 = strconv.ParseInt(total, 10, 64)
	return start, end, size, err1 == nil && err2 == nil && err3 == nil && start <= end && end < size
}

func (r *rangeReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, ErrInvalidArtifact
	}
	n := 0
	for n < len(p) {
		at := off + int64(n)
		if at >= r.identity.Size {
			return n, io.EOF
		}
		chunkAt := at - at%rangeChunkBytes
		if chunkAt != r.chunkAt {
			if err := r.fetch(chunkAt); err != nil {
				return n, err
			}
		}
		n += copy(p[n:], r.chunk[at-chunkAt:])
	}
	return n, nil
}

func (r *rangeReader) fetch(chunkAt int64) error {
	end := min(chunkAt+rangeChunkBytes, r.identity.Size) - 1
	if r.budget -= end - chunkAt + 1; r.budget < 0 {
		return errDownloadBudget
	}
	resp, err := r.get(chunkAt, end, r.identity.ETag)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	chunk := r.chunk[:0]
	if cap(chunk) < rangeChunkBytes {
		chunk = make([]byte, 0, rangeChunkBytes)
	}
	chunk = chunk[:end-chunkAt+1]
	if _, err := io.ReadFull(resp.Body, chunk); err != nil {
		r.chunkAt = -1
		return err
	}
	r.chunk, r.chunkAt = chunk, chunkAt
	return nil
}
