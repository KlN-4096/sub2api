//go:build unit

package codexrelease

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ulikunitz/xz"
)

func TestParseCoreVersion(t *testing.T) {
	cases := []struct {
		in    string
		atEnd bool
		want  string
	}{
		{"0.163.0-alpha.11\x00rest", false, "0.163.0-alpha.11"},
		{"0.162.0-alpha.17.2 (", false, "0.162.0-alpha.17.2"},
		{"0.164.0\"", false, "0.164.0"},
		{"0.164.0-rc.1\x00", false, "0.164.0-rc.1"},
		{"0.164.0+build\x00", false, ""},
		{"0.164.0-nightly.1\x00", false, ""},
		{"0.164.0-alpha\x00", false, ""},
		{"0.164.0-alpha.01\x00", false, ""},
		{"01.2.3\x00", false, ""},
		{"0.164\x00", false, ""},
		{"0.164.0", false, ""},
		{"0.164.0", true, "0.164.0"},
		{"0.164.0.1\x00", false, ""},
	}
	for _, tc := range cases {
		got, ok := parseCoreVersion([]byte(tc.in), tc.atEnd)
		require.Equal(t, tc.want, got, "%q", tc.in)
		require.Equal(t, tc.want != "", ok, "%q", tc.in)
	}
}

func TestCoreVersionScannerAcrossChunks(t *testing.T) {
	data := []byte("xxcodex-mcp-client/notaversion codex-mcp-client/0.163.0-alpha.11\x00tail")
	for split := 1; split < len(data); split++ {
		var scanner coreVersionScanner
		got := scanner.push(data[:split])
		if got == "" {
			got = scanner.push(data[split:])
		}
		require.Equal(t, "0.163.0-alpha.11", got, "split at %d", split)
	}
	var scanner coreVersionScanner
	require.Empty(t, scanner.push([]byte("codex-mcp-client/0.164.0")))
	require.Equal(t, "0.164.0", scanner.finish())
}

func TestReadCoreVersionChecksExecutableKind(t *testing.T) {
	for _, kind := range []binaryKind{machOArm64, peAMD64, elfAMD64} {
		exe := fakeCore(kind, "0.163.0-alpha.11")
		got, err := readCoreVersion(bytes.NewReader(exe), int64(len(exe)), kind)
		require.NoError(t, err)
		require.Equal(t, "0.163.0-alpha.11", got)

		other := (kind + 1) % 3
		_, err = readCoreVersion(bytes.NewReader(exe), int64(len(exe)), other)
		require.ErrorIs(t, err, ErrInvalidArtifact)
	}
	noMarker := fakeCore(elfAMD64, "")
	_, err := readCoreVersion(bytes.NewReader(noMarker), int64(len(noMarker)), elfAMD64)
	require.ErrorIs(t, err, ErrInvalidArtifact)
}

func TestCheckReadsOfficialArtifactsOnceUntilTheyChange(t *testing.T) {
	macZip := buildZip(t, map[string][]byte{
		"Codex.app/Contents/Resources/bin/codex":                                   []byte("#!/bin/sh\nexec codex-mcp-client/9.9.9 \"$@\"\n"),
		"Codex.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex": fakeCore(machOArm64, "0.162.0-alpha.17.2"),
	})
	msix := buildZip(t, map[string][]byte{
		"AppxManifest.xml":        []byte("\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><Package xmlns=\"http://schemas.microsoft.com/appx/manifest/foundation/windows10\"><Identity Name=\"OpenAI.Codex\" Publisher=\"CN=x\" Version=\"26.1016.51333.0\" ProcessorArchitecture=\"x64\"/></Package>"),
		"app/resources/app.asar":  fakeASAR(t, "26.1016.51333", "9910"),
		"app/resources/codex.exe": fakeCore(peAMD64, "0.163.0-alpha.11"),
	})
	deb := buildDeb(t, fakeASAR(t, "26.1015.40000", "9900"), fakeCore(elfAMD64, "0.163.0-alpha.10.2"), 0)
	appcast := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:sparkle="http://www.andymatuschak.org/xml-namespaces/sparkle"><channel>
<item><title>notes only</title></item>
<item><title>26.1007.71326</title><sparkle:version>9812</sparkle:version><sparkle:shortVersionString>26.1007.71326</sparkle:shortVersionString>
<enclosure url="https://persistent.oaistatic.com/codex-app-prod/Codex-darwin-arm64-26.1007.71326.zip" length="%d" type="application/octet-stream" sparkle:edSignature="c2ln"/></item>
</channel></rss>`, len(macZip))

	server := newArtifactServer(map[string][]byte{
		"/codex-app-prod/appcast.xml":                          []byte(appcast),
		"/codex-app-prod/Codex-darwin-arm64-26.1007.71326.zip": macZip,
		"/codex-app-prod/ChatGPT-x64.msix":                     msix,
		"/codex-app-prod/linux/deb/latest/chatgpt_amd64.deb":   deb,
	})
	defer server.Close()
	client := server.officialClient()

	want := map[Platform]Release{
		MacOS:   {CoreVersion: "0.162.0-alpha.17.2", DesktopVersion: "26.1007.71326", DesktopBuild: "9812"},
		Windows: {CoreVersion: "0.163.0-alpha.11", DesktopVersion: "26.1016.51333", DesktopBuild: "9910"},
		Linux:   {CoreVersion: "0.163.0-alpha.10.2", DesktopVersion: "26.1015.40000", DesktopBuild: "9900"},
	}
	for platform, expected := range want {
		got, err := Check(context.Background(), client, platform, Release{})
		require.NoError(t, err, platform)
		require.NotEmpty(t, got.Artifact.ETag, platform)
		require.Positive(t, got.Artifact.Size, platform)
		got.Artifact = Identity{}
		require.Equal(t, expected, got, platform)
	}

	for platform := range want {
		previous, err := Check(context.Background(), client, platform, Release{})
		require.NoError(t, err)
		server.resetCounts()
		again, err := Check(context.Background(), client, platform, previous)
		require.NoError(t, err)
		require.Equal(t, previous, again)
		if platform == MacOS {
			require.Equal(t, map[string]int{"/codex-app-prod/appcast.xml": 1}, server.counts(), "unchanged appcast build reads no zip")
		} else {
			require.Len(t, server.counts(), 1, "unchanged artifact identity only costs the one-byte probe")
			for _, n := range server.counts() {
				require.Equal(t, 1, n)
			}
		}
	}
}

func TestRangeReaderFailsWhenArtifactChangesMidRead(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 3*rangeChunkBytes)
	server := newArtifactServer(map[string][]byte{"/codex-app-prod/ChatGPT-x64.msix": payload})
	defer server.Close()

	reader, err := openRange(context.Background(), server.officialClient(), artifactURLs[Windows])
	require.NoError(t, err)
	buf := make([]byte, 10)
	_, err = reader.ReadAt(buf, 0)
	require.NoError(t, err)

	server.setETag(`"v2"`)
	_, err = reader.ReadAt(buf, 2*rangeChunkBytes)
	require.Error(t, err, "If-Match pins every chunk to the probed ETag")
}

func TestReadLinuxOnlyDecompressesTouchedBlocks(t *testing.T) {
	deb := buildDeb(t, fakeASAR(t, "26.1015.40000", "9900"), fakeCore(elfAMD64, "0.163.0-alpha.10.2"), 2<<20)
	source := &countingReaderAt{r: bytes.NewReader(deb)}
	release, err := readLinux(source, int64(len(deb)))
	require.NoError(t, err)
	require.Equal(t, "0.163.0-alpha.10.2", release.CoreVersion)
	require.Less(t, source.n.Load(), int64(len(deb))/2, "the filler between the two files is skipped via the xz index")
}

func TestReadWindowsRequiresTheX64CodexPackage(t *testing.T) {
	for _, identity := range []string{
		`Name="OpenAI.Codex" ProcessorArchitecture="arm64"`,
		`Name="OpenAI.ChatGPT-Desktop" ProcessorArchitecture="x64"`,
	} {
		msix := buildZip(t, map[string][]byte{
			"AppxManifest.xml":        []byte(`<Package><Identity ` + identity + `/></Package>`),
			"app/resources/app.asar":  fakeASAR(t, "26.1016.51333", "9910"),
			"app/resources/codex.exe": fakeCore(peAMD64, "0.163.0-alpha.11"),
		})
		_, err := readWindows(bytes.NewReader(msix), int64(len(msix)))
		require.ErrorIs(t, err, ErrInvalidArtifact, identity)
	}
}

func TestParseAppcastRequiresSignedOfficialZip(t *testing.T) {
	item := func(enclosure string) []byte {
		return []byte(`<rss xmlns:sparkle="s"><channel><item><sparkle:version>9812</sparkle:version><sparkle:shortVersionString>26.1007.71326</sparkle:shortVersionString>` + enclosure + `</item></channel></rss>`)
	}
	_, err := parseAppcast(item(`<enclosure url="https://persistent.oaistatic.com/a.zip" length="10" sparkle:edSignature="x"/>`))
	require.NoError(t, err)
	for _, enclosure := range []string{
		`<enclosure url="https://persistent.oaistatic.com/a.zip" length="10"/>`,
		`<enclosure url="https://evil.example/a.zip" length="10" sparkle:edSignature="x"/>`,
		`<enclosure url="http://persistent.oaistatic.com/a.zip" length="10" sparkle:edSignature="x"/>`,
		`<enclosure url="https://persistent.oaistatic.com/a.dmg" length="10" sparkle:edSignature="x"/>`,
		`<enclosure url="https://persistent.oaistatic.com/a.zip" length="0" sparkle:edSignature="x"/>`,
		`<enclosure url="https://persistent.oaistatic.com/a.zip" length="4294967296" sparkle:edSignature="x"/>`,
	} {
		_, err := parseAppcast(item(enclosure))
		require.ErrorIs(t, err, ErrInvalidArtifact, enclosure)
	}
	for _, versions := range []string{
		`<sparkle:version>abc</sparkle:version><sparkle:shortVersionString>26.1007.71326</sparkle:shortVersionString>`,
		`<sparkle:version>9812</sparkle:version><sparkle:shortVersionString>26.x</sparkle:shortVersionString>`,
	} {
		_, err := parseAppcast([]byte(`<rss xmlns:sparkle="s"><channel><item>` + versions +
			`<enclosure url="https://persistent.oaistatic.com/a.zip" length="10" sparkle:edSignature="x"/></item></channel></rss>`))
		require.ErrorIs(t, err, ErrInvalidArtifact, versions)
	}
}

// fakeCore is an executable header of the given kind followed by the MCP client marker.
func fakeCore(kind binaryKind, version string) []byte {
	out := make([]byte, 8192)
	switch kind {
	case machOArm64:
		copy(out, []byte{0xcf, 0xfa, 0xed, 0xfe})
		binary.LittleEndian.PutUint32(out[4:], 0x0100000c)
	case peAMD64:
		copy(out, "MZ")
		binary.LittleEndian.PutUint32(out[60:], 0x80)
		copy(out[0x80:], "PE\x00\x00")
		binary.LittleEndian.PutUint16(out[0x84:], 0x8664)
	case elfAMD64:
		copy(out, "\x7fELF\x02\x01")
		binary.LittleEndian.PutUint16(out[18:], 62)
	}
	if version != "" {
		copy(out[6000:], "codex-mcp-client/"+version+"\x00")
	}
	return out
}

// fakeASAR packs index.js and package.json the way @electron/asar lays out its header pickle.
func fakeASAR(t *testing.T, version, build string) []byte {
	t.Helper()
	index := []byte(strings.Repeat("console.log(1);\n", 64))
	pkg := []byte(fmt.Sprintf(`{"name":"openai-codex-electron","version":%q,"codexBuildNumber":%q,"codexBuildFlavor":"prod"}`, version, build))
	header := []byte(fmt.Sprintf(`{"files":{"index.js":{"size":%d,"offset":"0"},"package.json":{"size":%d,"offset":"%d"}}}`, len(index), len(pkg), len(index)))
	padded := (len(header) + 3) &^ 3
	out := binary.LittleEndian.AppendUint32(nil, 4)
	out = binary.LittleEndian.AppendUint32(out, uint32(8+padded))
	out = binary.LittleEndian.AppendUint32(out, uint32(4+padded))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(header)))
	out = append(out, header...)
	out = append(out, make([]byte, padded-len(header))...)
	return append(append(out, index...), pkg...)
}

func buildZip(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, data := range files {
		f, err := w.Create(name)
		require.NoError(t, err)
		_, err = f.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// buildDeb writes an ar archive whose data.tar.xz (64 KiB blocks) holds app.asar, filler random
// bytes, then codex.
func buildDeb(t *testing.T, asar, core []byte, filler int) []byte {
	t.Helper()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	noise := make([]byte, filler)
	rand.New(rand.NewSource(1)).Read(noise)
	for _, file := range []struct {
		name string
		data []byte
	}{
		{"./usr/lib/chatgpt/resources/app.asar", asar},
		{"./usr/lib/chatgpt/libfiller.so", noise},
		{"./usr/lib/chatgpt/resources/codex", core},
	} {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: file.name, Mode: 0o755, Size: int64(len(file.data)), Typeflag: tar.TypeReg}))
		_, err := tw.Write(file.data)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())

	var xzBuf bytes.Buffer
	xw, err := xz.WriterConfig{BlockSize: 64 << 10}.NewWriter(&xzBuf)
	require.NoError(t, err)
	_, err = xw.Write(tarBuf.Bytes())
	require.NoError(t, err)
	require.NoError(t, xw.Close())

	out := []byte("!<arch>\n")
	for _, member := range []struct {
		name string
		data []byte
	}{
		{"debian-binary", []byte("2.0\n")},
		{"control.tar.xz", []byte("not read")},
		{"data.tar.xz", xzBuf.Bytes()},
	} {
		out = append(out, fmt.Sprintf("%-16s%-12d%-6d%-6d%-8s%-10d`\n", member.name, 0, 0, 0, "100644", len(member.data))...)
		out = append(out, member.data...)
		if len(member.data)%2 == 1 {
			out = append(out, '\n')
		}
	}
	return out
}

type countingReaderAt struct {
	r io.ReaderAt
	n atomic.Int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.r.ReadAt(p, off)
	c.n.Add(int64(n))
	return n, err
}

// artifactServer stands in for persistent.oaistatic.com; officialClient routes that host to it.
type artifactServer struct {
	*httptest.Server
	mu            sync.Mutex
	etag          string
	files         map[string][]byte
	hits          map[string]int
	ignoreIfMatch bool   // 模拟不认 If-Match 的 CDN
	lastIfMatch   string // 最近一次请求带的 If-Match
}

func newArtifactServer(files map[string][]byte) *artifactServer {
	s := &artifactServer{etag: `"v1"`, files: files, hits: map[string]int{}}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.hits[r.URL.Path]++
		etag := s.etag
		s.lastIfMatch = r.Header.Get("If-Match")
		if s.ignoreIfMatch {
			r.Header.Del("If-Match")
		}
		s.mu.Unlock()
		data, ok := s.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", etag)
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	}))
	return s
}

func (s *artifactServer) officialClient() *http.Client {
	target, _ := url.Parse(s.URL)
	transport := s.Client().Transport
	return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != officialHost || r.URL.Scheme != "https" {
			return nil, errors.New("unexpected destination " + r.URL.String())
		}
		r = r.Clone(r.Context())
		r.URL.Scheme, r.URL.Host = target.Scheme, target.Host
		return transport.RoundTrip(r)
	})}
}

func (s *artifactServer) setETag(etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.etag = etag
}

func (s *artifactServer) resetCounts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits = map[string]int{}
}

func (s *artifactServer) counts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for path, n := range s.hits {
		out[path] = n
	}
	return out
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// 32 字节构造输入：footer 的 backward size 为 0（索引 4 字节），全零索引能过标识与 CRC
// （空串 CRC 为 0），之前在读块数时切片越界 panic（评审 B2）。
func TestNewIndexedXZRejectsAnIndexTooShortForItsBlockCount(t *testing.T) {
	flags := []byte{0, 1}
	data := binary.LittleEndian.AppendUint32(append([]byte("\xfd7zXZ\x00"), flags...), crc32.ChecksumIEEE(flags))
	data = append(data, make([]byte, 8)...)
	footer := append(binary.LittleEndian.AppendUint32(nil, 0), flags...)
	data = binary.LittleEndian.AppendUint32(data, crc32.ChecksumIEEE(footer))
	data = append(append(data, footer...), 'Y', 'Z')
	require.Len(t, data, 32)
	_, err := newIndexedXZ(bytes.NewReader(data), int64(len(data)))
	require.ErrorIs(t, err, ErrInvalidArtifact)
}

// 块头声明的字典大小决定解码器的一次性分配：改成 512MiB（属性 34）且重算块头 CRC 的块必须拒绝
// （评审 B3）；未篡改的同一流照常解码。
func TestIndexedXZRejectsAnOversizedDictionary(t *testing.T) {
	var buf bytes.Buffer
	w, err := xz.WriterConfig{BlockSize: 64 << 10}.NewWriter(&buf)
	require.NoError(t, err)
	_, err = w.Write(bytes.Repeat([]byte("codex"), 10000))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	data := buf.Bytes()

	read := func() error {
		x, err := newIndexedXZ(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)
		_, err = x.ReadAt(make([]byte, 1), 0)
		return err
	}
	require.NoError(t, read())

	block := data[12:]
	size := (int(block[0]) + 1) * 4
	at := bytes.Index(block[2:size-4], []byte{0x21, 0x01}) + 2
	require.Greater(t, at, 1, "the LZMA2 filter flags are in the block header")
	block[at+2] = 34
	binary.LittleEndian.PutUint32(block[size-4:size], crc32.ChecksumIEEE(block[:size-4]))
	require.ErrorIs(t, read(), ErrInvalidArtifact)
}

// zip.NewReader 逐条分配中央目录项：超过 maxZipDirectoryBytes 的目录在解析中途就停（评审 S9）。
func TestOpenZipBoundsTheCentralDirectory(t *testing.T) {
	name := strings.Repeat("n", 200)
	record := make([]byte, 46, 46+len(name))
	binary.LittleEndian.PutUint32(record, 0x02014b50)
	binary.LittleEndian.PutUint16(record[28:], uint16(len(name)))
	record = append(record, name...)
	count := maxZipDirectoryBytes/len(record) + 16
	data := bytes.Repeat(record, count)
	end := make([]byte, 22)
	binary.LittleEndian.PutUint32(end, 0x06054b50)
	binary.LittleEndian.PutUint16(end[8:], uint16(count))
	binary.LittleEndian.PutUint16(end[10:], uint16(count))
	binary.LittleEndian.PutUint32(end[12:], uint32(len(data)))
	data = append(data, end...)
	_, err := openZip(bytes.NewReader(data), int64(len(data)))
	require.ErrorIs(t, err, ErrInvalidArtifact)
}

func TestReadWindowsRejectsDuplicateEntries(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for _, file := range []struct {
		name string
		data []byte
	}{
		{"AppxManifest.xml", []byte(`<Package><Identity Name="OpenAI.Codex" ProcessorArchitecture="x64"/></Package>`)},
		{"app/resources/app.asar", fakeASAR(t, "26.1016.51333", "9910")},
		{"app/resources/codex.exe", fakeCore(peAMD64, "0.163.0-alpha.11")},
		{"app/resources/codex.exe", fakeCore(peAMD64, "0.170.0")},
	} {
		f, err := w.Create(file.name)
		require.NoError(t, err)
		_, err = f.Write(file.data)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	_, err := readWindows(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.ErrorIs(t, err, ErrInvalidArtifact)
}

func TestReadASARPackageRequiresTheProdCodexApp(t *testing.T) {
	asar := fakeASAR(t, "26.1016.51333", "9910")
	release, err := readASARPackage(bytes.NewReader(asar), int64(len(asar)))
	require.NoError(t, err)
	require.Equal(t, "26.1016.51333", release.DesktopVersion)
	for _, swap := range [][2]string{{`"prod"`, `"beta"`}, {"openai-codex-electron", "openai-chatgpt-electr"}} {
		tampered := bytes.Replace(asar, []byte(swap[0]), []byte(swap[1]), 1)
		_, err := readASARPackage(bytes.NewReader(tampered), int64(len(tampered)))
		require.ErrorIs(t, err, ErrInvalidArtifact, swap[1])
	}
}

func TestOpenRangeRejectsWeakETags(t *testing.T) {
	server := newArtifactServer(map[string][]byte{"/codex-app-prod/ChatGPT-x64.msix": []byte("payload")})
	defer server.Close()
	server.setETag(`W/"v1"`)
	_, err := openRange(context.Background(), server.officialClient(), artifactURLs[Windows])
	require.Error(t, err, "If-Match compares strongly, so a weak ETag cannot pin the artifact")
}

// If-Match 与响应 ETag 两道守卫各自独立：CDN 不认 If-Match 时靠响应 ETag 发现换包，且每个分块都带 If-Match。
func TestRangeReaderGuardsEachChunk(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 3*rangeChunkBytes)
	server := newArtifactServer(map[string][]byte{"/codex-app-prod/ChatGPT-x64.msix": payload})
	defer server.Close()
	server.mu.Lock()
	server.ignoreIfMatch = true
	server.mu.Unlock()

	reader, err := openRange(context.Background(), server.officialClient(), artifactURLs[Windows])
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 10), 0)
	require.NoError(t, err)
	server.mu.Lock()
	require.Equal(t, `"v1"`, server.lastIfMatch, "every chunk request is pinned with If-Match")
	server.mu.Unlock()

	server.setETag(`"v2"`)
	_, err = reader.ReadAt(make([]byte, 10), 2*rangeChunkBytes)
	require.Error(t, err, "a CDN ignoring If-Match is caught by the response ETag")
}

func TestRangeReaderStopsAtTheDownloadBudget(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 3*rangeChunkBytes)
	server := newArtifactServer(map[string][]byte{"/codex-app-prod/ChatGPT-x64.msix": payload})
	defer server.Close()
	reader, err := openRange(context.Background(), server.officialClient(), artifactURLs[Windows])
	require.NoError(t, err)
	reader.budget = rangeChunkBytes
	_, err = reader.ReadAt(make([]byte, 10), 0)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 10), 2*rangeChunkBytes)
	require.ErrorIs(t, err, errDownloadBudget)
}

// appcast 声明的 zip 大小必须等于服务端给的大小：签名只覆盖 appcast 里的条目，换了包要能发现。
func TestCheckRejectsAMacOSZipOfTheWrongSize(t *testing.T) {
	macZip := buildZip(t, map[string][]byte{
		"Codex.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex": fakeCore(machOArm64, "0.162.0-alpha.17.2"),
	})
	appcast := fmt.Sprintf(`<rss xmlns:sparkle="http://www.andymatuschak.org/xml-namespaces/sparkle"><channel><item>`+
		`<sparkle:version>9812</sparkle:version><sparkle:shortVersionString>26.1007.71326</sparkle:shortVersionString>`+
		`<enclosure url="https://persistent.oaistatic.com/codex-app-prod/Codex.zip" length="%d" sparkle:edSignature="c2ln"/></item></channel></rss>`, len(macZip)+1)
	server := newArtifactServer(map[string][]byte{
		"/codex-app-prod/appcast.xml": []byte(appcast),
		"/codex-app-prod/Codex.zip":   macZip,
	})
	defer server.Close()
	_, err := Check(context.Background(), server.officialClient(), MacOS, Release{})
	require.Error(t, err)
}
