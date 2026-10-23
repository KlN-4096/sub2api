package codexrelease

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"io"
	"math"
	"strconv"
	"strings"
)

const (
	macOSCorePathSuffix = "/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex"
	maxMSIXEntries      = 20000
	maxManifestBytes    = 128 << 10
	maxASARHeaderBytes  = 16 << 20
	maxASARPrefix       = 128 << 20
	maxPackageJSONBytes = 256 << 10
	maxTarEntries       = 30000
	// maxZipDirectoryBytes 限制 zip.NewReader 读中央目录的字节数：它逐条分配目录项，不限的话
	// 声称几十万条的目录能把内存放大到下载预算的数倍（官方 zip/msix 的目录在 1MiB 量级）。
	maxZipDirectoryBytes = 16 << 20
)

// openZip 只在解析中央目录期间限读，之后读条目不受限。
func openZip(ra io.ReaderAt, size int64) (*zip.Reader, error) {
	limited := &limitedReaderAt{ReaderAt: ra, left: maxZipDirectoryBytes}
	zr, err := zip.NewReader(limited, size)
	limited.left = math.MaxInt64
	return zr, err
}

type limitedReaderAt struct {
	io.ReaderAt
	left int64
}

func (l *limitedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if int64(len(p)) > l.left {
		return 0, ErrInvalidArtifact
	}
	l.left -= int64(len(p))
	return l.ReaderAt.ReadAt(p, off)
}

// readMacOSCore 扫 macOS 应用 zip 里内置的 core（bin/codex 只是启动脚本，core 在嵌套的
// CodexCLI.app 里）。
func readMacOSCore(ra io.ReaderAt, size int64) (string, error) {
	zr, err := openZip(ra, size)
	if err != nil {
		return "", err
	}
	core, err := zipEntry(zr, func(name string) bool { return strings.HasSuffix(name, macOSCorePathSuffix) })
	if err != nil {
		return "", err
	}
	return readZipCore(core, machOArm64)
}

// readWindows 读 x64 msix：AppxManifest 身份、app.asar 里的 package.json、codex.exe。
func readWindows(ra io.ReaderAt, size int64) (Release, error) {
	zr, err := openZip(ra, size)
	if err != nil {
		return Release{}, err
	}
	if len(zr.File) > maxMSIXEntries {
		return Release{}, ErrInvalidArtifact
	}
	named := func(want string) (*zip.File, error) {
		return zipEntry(zr, func(name string) bool { return name == want })
	}
	manifest, err := named("AppxManifest.xml")
	if err != nil {
		return Release{}, err
	}
	if err := checkAppxManifest(manifest); err != nil {
		return Release{}, err
	}
	asar, err := named("app/resources/app.asar")
	if err != nil {
		return Release{}, err
	}
	asarReader, err := asar.Open()
	if err != nil {
		return Release{}, err
	}
	release, err := readASARPackage(asarReader, int64(asar.UncompressedSize64))
	_ = asarReader.Close()
	if err != nil {
		return Release{}, err
	}
	core, err := named("app/resources/codex.exe")
	if err != nil {
		return Release{}, err
	}
	if release.CoreVersion, err = readZipCore(core, peAMD64); err != nil {
		return Release{}, err
	}
	return release, nil
}

// readLinux 读 amd64 deb：ar 成员 data.tar.xz，再读 tar 里的 app.asar 与 codex。
// 借 xz 索引，tar 读取器跳过文件正文时不必解压。
func readLinux(ra io.ReaderAt, size int64) (Release, error) {
	dataOffset, dataSize, err := debDataMember(ra, size)
	if err != nil {
		return Release{}, err
	}
	data, err := newIndexedXZ(io.NewSectionReader(ra, dataOffset, dataSize), dataSize)
	if err != nil {
		return Release{}, err
	}
	type span struct{ offset, size int64 }
	var asar, core *span
	stream := io.NewSectionReader(data, 0, data.size)
	tr := tar.NewReader(stream)
	for i := 0; i < maxTarEntries && (asar == nil || core == nil); i++ {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Release{}, err
		}
		var target **span
		switch strings.TrimPrefix(header.Name, "./") {
		case "usr/lib/chatgpt/resources/app.asar":
			target = &asar
		case "usr/lib/chatgpt/resources/codex":
			target = &core
		default:
			continue
		}
		if header.Typeflag != tar.TypeReg || *target != nil {
			return Release{}, ErrInvalidArtifact
		}
		// tar.Reader 停在头块之后，此时流位置就是正文开头。
		offset, _ := stream.Seek(0, io.SeekCurrent)
		*target = &span{offset: offset, size: header.Size}
	}
	if asar == nil || core == nil {
		return Release{}, ErrInvalidArtifact
	}
	release, err := readASARPackage(io.NewSectionReader(data, asar.offset, asar.size), asar.size)
	if err != nil {
		return Release{}, err
	}
	if release.CoreVersion, err = readCoreVersion(io.NewSectionReader(data, core.offset, core.size), core.size, elfAMD64); err != nil {
		return Release{}, err
	}
	return release, nil
}

func zipEntry(zr *zip.Reader, match func(name string) bool) (*zip.File, error) {
	var found *zip.File
	for _, file := range zr.File {
		if match(file.Name) {
			if found != nil {
				return nil, ErrInvalidArtifact
			}
			found = file
		}
	}
	if found == nil {
		return nil, ErrInvalidArtifact
	}
	return found, nil
}

func readZipCore(file *zip.File, kind binaryKind) (string, error) {
	r, err := file.Open()
	if err != nil {
		return "", err
	}
	defer func() { _ = r.Close() }()
	return readCoreVersion(r, int64(file.UncompressedSize64), kind)
}

func checkAppxManifest(file *zip.File) error {
	r, err := file.Open()
	if err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(r, maxManifestBytes+1))
	_ = r.Close()
	if err != nil {
		return err
	}
	if len(data) > maxManifestBytes {
		return ErrInvalidArtifact
	}
	decoder := xml.NewDecoder(bytes.NewReader(bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))))
	for {
		token, err := decoder.Token()
		if err != nil {
			return ErrInvalidArtifact
		}
		element, ok := token.(xml.StartElement)
		if !ok || element.Name.Local != "Identity" {
			continue
		}
		attrs := map[string]string{}
		for _, attr := range element.Attr {
			if attr.Name.Space == "" {
				attrs[attr.Name.Local] = attr.Value
			}
		}
		if attrs["Name"] != "OpenAI.Codex" || attrs["ProcessorArchitecture"] != "x64" {
			return ErrInvalidArtifact
		}
		return nil
	}
}

// readASARPackage 从首字节起流式读 asar，取出 Electron 的 package.json；它必须是打包在内的
// （不是 unpacked 或链接）且靠前。
func readASARPackage(r io.Reader, size int64) (Release, error) {
	var prefix [16]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return Release{}, err
	}
	headerSize := int64(binary.LittleEndian.Uint32(prefix[4:8]))
	jsonSize := int64(binary.LittleEndian.Uint32(prefix[12:16]))
	if binary.LittleEndian.Uint32(prefix[0:4]) != 4 || headerSize < 8 || headerSize > maxASARHeaderBytes ||
		jsonSize > headerSize-8 || headerSize+8 > size {
		return Release{}, ErrInvalidArtifact
	}
	headerJSON := make([]byte, jsonSize)
	if _, err := io.ReadFull(r, headerJSON); err != nil {
		return Release{}, err
	}
	var header struct {
		Files struct {
			Package *struct {
				Offset   string          `json:"offset"`
				Size     int64           `json:"size"`
				Unpacked json.RawMessage `json:"unpacked"`
				Link     json.RawMessage `json:"link"`
			} `json:"package.json"`
		} `json:"files"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return Release{}, ErrInvalidArtifact
	}
	pkg := header.Files.Package
	if pkg == nil || pkg.Unpacked != nil || pkg.Link != nil || pkg.Size <= 0 || pkg.Size > maxPackageJSONBytes {
		return Release{}, ErrInvalidArtifact
	}
	offset, err := strconv.ParseInt(pkg.Offset, 10, 64)
	if err != nil || offset < 0 || offset > maxASARPrefix { // 先比上限再相加，防溢出成负数
		return Release{}, ErrInvalidArtifact
	}
	offset += headerSize + 8
	if offset > maxASARPrefix || offset+pkg.Size > size {
		return Release{}, ErrInvalidArtifact
	}
	// deflate 流（msix）只能读过去；deb 的带索引 xz 可以 seek。
	if seeker, ok := r.(io.Seeker); ok {
		_, err = seeker.Seek(offset-16-jsonSize, io.SeekCurrent)
	} else {
		_, err = io.CopyN(io.Discard, r, offset-16-jsonSize)
	}
	if err != nil {
		return Release{}, err
	}
	body := make([]byte, pkg.Size)
	if _, err := io.ReadFull(r, body); err != nil {
		return Release{}, err
	}
	var app struct {
		Name             string `json:"name"`
		Version          string `json:"version"`
		CodexBuildNumber string `json:"codexBuildNumber"`
		CodexBuildFlavor string `json:"codexBuildFlavor"`
	}
	if err := json.Unmarshal(body, &app); err != nil {
		return Release{}, ErrInvalidArtifact
	}
	if app.Name != "openai-codex-electron" || app.CodexBuildFlavor != "prod" ||
		!IsDesktopVersion(app.Version) || BuildSequence(app.CodexBuildNumber) == 0 {
		return Release{}, ErrInvalidArtifact
	}
	return Release{DesktopVersion: app.Version, DesktopBuild: app.CodexBuildNumber}, nil
}

// debDataMember 在 deb 开头几个 ar 成员里找 data.tar.xz。
func debDataMember(ra io.ReaderAt, size int64) (int64, int64, error) {
	magic := make([]byte, 8)
	if _, err := ra.ReadAt(magic, 0); err != nil {
		return 0, 0, err
	}
	if string(magic) != "!<arch>\n" {
		return 0, 0, ErrInvalidArtifact
	}
	header := make([]byte, 60)
	for offset, i := int64(8), 0; i < 16 && offset+60 <= size; i++ {
		if _, err := ra.ReadAt(header, offset); err != nil {
			return 0, 0, err
		}
		memberSize, err := strconv.ParseInt(strings.TrimSpace(string(header[48:58])), 10, 64)
		if string(header[58:]) != "`\n" || err != nil || memberSize < 0 || offset+60+memberSize > size {
			return 0, 0, ErrInvalidArtifact
		}
		if strings.TrimSpace(string(header[:16])) == "data.tar.xz" {
			return offset + 60, memberSize, nil
		}
		offset += 60 + memberSize + memberSize%2
	}
	return 0, 0, ErrInvalidArtifact
}

// IsDesktopVersion 判断 Desktop 应用版本形状：两段及以上、点分隔的纯数字。
func IsDesktopVersion(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) < 2 || len(value) > maxVersionBytes {
		return false
	}
	for _, part := range parts {
		if part == "" || strings.Trim(part, "0123456789") != "" {
			return false
		}
	}
	return true
}

// BuildSequence 解析 Desktop 构建号；0 表示非法（构建号从 1 开始）。
func BuildSequence(build string) uint64 {
	if build == "" || len(build) > 19 || strings.Trim(build, "0123456789") != "" {
		return 0
	}
	sequence, _ := strconv.ParseUint(build, 10, 64)
	return sequence
}
