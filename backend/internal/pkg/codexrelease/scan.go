// Package codexrelease 照 codex-proxy-rs 的做法，从官方 Codex Desktop 安装包（macOS zip、
// Windows msix、Linux deb）读出 core 版本与应用版本：只按需取用到的字节，内置 core 二进制
// 边下边扫，不落盘。
package codexrelease

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// ErrInvalidArtifact 表示制品不是预期的官方布局。
var ErrInvalidArtifact = errors.New("codexrelease: unexpected artifact layout")

// coreVersionAnchor 在每个内置 codex 二进制里紧挨着 core 版本（MCP 客户端的 User-Agent，
// codex-rs mcp-client）。
var coreVersionAnchor = []byte("codex-mcp-client/")

const (
	maxVersionBytes = 64
	coreHeaderBytes = 4096
	maxCoreBytes    = 512 << 20
)

// coreVersionScanner 跨分块查找 "codex-mcp-client/<version>"。
type coreVersionScanner struct{ carry []byte }

func (s *coreVersionScanner) push(chunk []byte) string {
	window := make([]byte, 0, len(s.carry)+len(chunk))
	window = append(append(window, s.carry...), chunk...)
	if version := scanCoreVersion(window, false); version != "" {
		return version
	}
	keep := min(len(window), len(coreVersionAnchor)+maxVersionBytes)
	s.carry = append(s.carry[:0], window[len(window)-keep:]...)
	return ""
}

func (s *coreVersionScanner) finish() string { return scanCoreVersion(s.carry, true) }

func scanCoreVersion(window []byte, atEnd bool) string {
	for start := 0; ; {
		i := bytes.Index(window[start:], coreVersionAnchor)
		if i < 0 {
			return ""
		}
		start += i + len(coreVersionAnchor)
		if version, ok := parseCoreVersion(window[start:], atEnd); ok {
			return version
		}
	}
}

// parseCoreVersion 接受 X.Y.Z 加可选的 -alpha|beta|rc(.N)+ 后缀（Desktop 带的是 alpha core），
// 后面还接着版本字符的（+build、nightly）一律拒绝。atEnd 为假时，顶到缓冲区末尾的版本不可信：
// 它可能延续到下一块。
func parseCoreVersion(b []byte, atEnd bool) (string, bool) {
	cur := 0
	number := func() bool {
		start := cur
		for cur < len(b) && isDigit(b[cur]) {
			cur++
		}
		return cur > start && (cur-start == 1 || b[start] != '0')
	}
	for part := 0; part < 3; part++ {
		if !number() {
			return "", false
		}
		if part < 2 {
			if cur >= len(b) || b[cur] != '.' {
				return "", false
			}
			cur++
		}
	}
	if cur < len(b) && b[cur] == '-' {
		cur++
		channel := ""
		for _, name := range []string{"alpha", "beta", "rc"} {
			if bytes.HasPrefix(b[cur:], []byte(name)) {
				channel = name
				break
			}
		}
		if channel == "" {
			return "", false
		}
		cur += len(channel)
		parts := 0
		for cur < len(b) && b[cur] == '.' {
			cur++
			if !number() {
				return "", false
			}
			parts++
		}
		if parts == 0 {
			return "", false
		}
	}
	if cur > maxVersionBytes || cur == len(b) && !atEnd {
		return "", false
	}
	if cur < len(b) && (isDigit(b[cur]) || b[cur] == '.' || b[cur] == '+' || b[cur] == '-') {
		return "", false
	}
	return string(b[:cur]), true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// binaryKind 是各平台内置 core 的可执行格式（只跟默认架构：macOS arm64，Windows 与 Linux x86_64）。
type binaryKind int

const (
	machOArm64 binaryKind = iota
	peAMD64
	elfAMD64
)

func (k binaryKind) matches(header []byte) bool {
	switch k {
	case machOArm64:
		return bytes.HasPrefix(header, []byte{0xcf, 0xfa, 0xed, 0xfe}) && binary.LittleEndian.Uint32(header[4:8]) == 0x0100000c
	case peAMD64:
		if !bytes.HasPrefix(header, []byte("MZ")) {
			return false
		}
		offset := int(binary.LittleEndian.Uint32(header[60:64]))
		return offset+6 <= len(header) && bytes.Equal(header[offset:offset+4], []byte("PE\x00\x00")) &&
			binary.LittleEndian.Uint16(header[offset+4:offset+6]) == 0x8664
	case elfAMD64:
		return bytes.HasPrefix(header, []byte("\x7fELF\x02\x01")) && binary.LittleEndian.Uint16(header[18:20]) == 62
	}
	return false
}

// readCoreVersion 校验可执行文件头，再扫出 core 版本。
func readCoreVersion(r io.Reader, size int64, kind binaryKind) (string, error) {
	if size < coreHeaderBytes || size > maxCoreBytes {
		return "", ErrInvalidArtifact
	}
	header := make([]byte, coreHeaderBytes)
	if _, err := io.ReadFull(r, header); err != nil {
		return "", err
	}
	if !kind.matches(header) {
		return "", ErrInvalidArtifact
	}
	var scanner coreVersionScanner
	if version := scanner.push(header); version != "" {
		return version, nil
	}
	rest := io.LimitReader(r, size-coreHeaderBytes)
	buf := make([]byte, 64<<10)
	for {
		n, err := rest.Read(buf)
		if version := scanner.push(buf[:n]); version != "" {
			return version, nil
		}
		if err == io.EOF {
			if version := scanner.finish(); version != "" {
				return version, nil
			}
			return "", ErrInvalidArtifact
		}
		if err != nil {
			return "", err
		}
	}
}
