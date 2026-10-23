package codexrelease

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sort"

	"github.com/ulikunitz/xz"
)

const (
	maxXZIndexBytes = 1 << 20
	maxXZBlocks     = 4096
	maxXZBlockBytes = 64 << 20
	maxXZDictBytes  = 64 << 20
	maxXZTarBytes   = 4 << 30
)

type xzBlock struct {
	offset   int64 // block start inside the .xz member
	unpadded int64 // index "unpadded size": block header + data + check
	size     int64 // uncompressed bytes
	start    int64 // uncompressed position
}

// indexedXZ 是多块 xz 流上的 io.ReaderAt：先读流索引，只下载、解压读到的块，同时只缓存一块。
type indexedXZ struct {
	src    io.ReaderAt
	header []byte
	blocks []xzBlock
	size   int64
	cached int
	buf    []byte
}

func newIndexedXZ(src io.ReaderAt, length int64) (*indexedXZ, error) {
	if length < 32 {
		return nil, ErrInvalidArtifact
	}
	header := make([]byte, 12)
	footer := make([]byte, 12)
	if _, err := src.ReadAt(header, 0); err != nil {
		return nil, err
	}
	if _, err := src.ReadAt(footer, length-12); err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(header, []byte("\xfd7zXZ\x00")) || !crcMatches(header[6:8], header[8:12]) ||
		string(footer[10:]) != "YZ" || !bytes.Equal(footer[8:10], header[6:8]) || !crcMatches(footer[4:10], footer[:4]) {
		return nil, ErrInvalidArtifact
	}
	indexSize := (int64(binary.LittleEndian.Uint32(footer[4:8])) + 1) * 4
	// 最小索引 8 字节（标识、块数、填充、CRC）；更短的连块数都放不下。
	if indexSize < 8 || indexSize > maxXZIndexBytes || indexSize > length-24 {
		return nil, ErrInvalidArtifact
	}
	index := make([]byte, indexSize)
	if _, err := src.ReadAt(index, length-12-indexSize); err != nil {
		return nil, err
	}
	if index[0] != 0 || !crcMatches(index[:indexSize-4], index[indexSize-4:]) {
		return nil, ErrInvalidArtifact
	}
	cursor := 1
	vli := func() int64 {
		value, n := binary.Uvarint(index[cursor : indexSize-4])
		if n <= 0 || value > maxXZTarBytes {
			return -1
		}
		cursor += n
		return int64(value)
	}
	count := vli()
	if count < 1 || count > maxXZBlocks {
		return nil, ErrInvalidArtifact
	}
	x := &indexedXZ{src: src, header: header, cached: -1}
	offset := int64(12)
	for i := int64(0); i < count; i++ {
		block := xzBlock{offset: offset, unpadded: vli(), size: vli(), start: x.size}
		if block.unpadded < 1 || block.unpadded > maxXZBlockBytes || block.size < 1 || block.size > maxXZBlockBytes {
			return nil, ErrInvalidArtifact
		}
		x.blocks = append(x.blocks, block)
		offset += (block.unpadded + 3) &^ 3
		if x.size += block.size; x.size > maxXZTarBytes {
			return nil, ErrInvalidArtifact
		}
	}
	if offset != length-12-indexSize || len(bytes.Trim(index[cursor:indexSize-4], "\x00")) != 0 {
		return nil, ErrInvalidArtifact
	}
	return x, nil
}

func (x *indexedXZ) ReadAt(p []byte, off int64) (int, error) {
	n := 0
	for n < len(p) {
		at := off + int64(n)
		if at >= x.size {
			return n, io.EOF
		}
		i := sort.Search(len(x.blocks), func(i int) bool { return x.blocks[i].start > at }) - 1
		if i != x.cached {
			if err := x.load(i); err != nil {
				return n, err
			}
		}
		n += copy(p[n:], x.buf[at-x.blocks[i].start:])
	}
	return n, nil
}

// load 把第 i 块包成单块 xz 流（流头、该块、重建的索引与流尾）再解码，解码器照样校验块的
// 校验和与两个大小。
func (x *indexedXZ) load(i int) error {
	block := x.blocks[i]
	padded := (block.unpadded + 3) &^ 3
	stream := make([]byte, 12+padded, 12+padded+64)
	copy(stream, x.header)
	if _, err := x.src.ReadAt(stream[12:], block.offset); err != nil {
		return err
	}
	if err := checkXZBlockFilter(stream[12:]); err != nil {
		return err
	}
	index := binary.AppendUvarint([]byte{0, 1}, uint64(block.unpadded))
	index = binary.AppendUvarint(index, uint64(block.size))
	for len(index)%4 != 0 {
		index = append(index, 0)
	}
	index = binary.LittleEndian.AppendUint32(index, crc32.ChecksumIEEE(index))
	footer := append(binary.LittleEndian.AppendUint32(nil, uint32(len(index)/4-1)), x.header[6:8]...)
	stream = append(stream, index...)
	stream = binary.LittleEndian.AppendUint32(stream, crc32.ChecksumIEEE(footer))
	stream = append(append(stream, footer...), 'Y', 'Z')

	x.cached = -1
	reader, err := xz.ReaderConfig{SingleStream: true}.NewReader(bytes.NewReader(stream))
	if err != nil {
		return err
	}
	out := bytes.NewBuffer(x.buf[:0])
	out.Grow(int(block.size) + 1)
	if _, err := io.Copy(out, io.LimitReader(reader, block.size+1)); err != nil {
		return err
	}
	if int64(out.Len()) != block.size {
		return ErrInvalidArtifact
	}
	x.buf, x.cached = out.Bytes(), i
	return nil
}

// checkXZBlockFilter 只放行单个 LZMA2 过滤器且字典不超过 maxXZDictBytes 的块（官方 deb 是
// 8MiB 字典）：解码器按块头声明的字典大小一次性分配，篡改过的块头能要走 4GiB。块头 CRC 由解码器校验。
func checkXZBlockFilter(block []byte) error {
	// 标志低 2 位是过滤器数减一（必须 0：只一个），中间 4 位保留（必须 0）。
	if len(block) < 8 || block[0] == 0 || block[1]&0x3f != 0 {
		return ErrInvalidArtifact
	}
	size := (int(block[0]) + 1) * 4
	if size > len(block) {
		return ErrInvalidArtifact
	}
	header, cursor := block[:size-4], 2
	vli := func() uint64 {
		if cursor >= len(header) {
			cursor = len(header) + 1
			return 0
		}
		value, n := binary.Uvarint(header[cursor:])
		if n <= 0 {
			cursor = len(header) + 1
			return 0
		}
		cursor += n
		return value
	}
	if block[1]&0x40 != 0 { // 压缩大小
		vli()
	}
	if block[1]&0x80 != 0 { // 解压大小
		vli()
	}
	id, propsSize := vli(), vli()
	if id != 0x21 || propsSize != 1 || cursor >= len(header) {
		return ErrInvalidArtifact
	}
	if prop := header[cursor]; prop > 40 || lzma2DictBytes(prop) > maxXZDictBytes {
		return ErrInvalidArtifact
	}
	return nil
}

// lzma2DictBytes 按 xz 规范把 LZMA2 字典属性字节换成字节数（40 = 4GiB-1）。
func lzma2DictBytes(prop byte) uint64 {
	if prop >= 40 {
		return 1<<32 - 1
	}
	return uint64(2|prop&1) << (prop/2 + 11)
}

func crcMatches(data, sum []byte) bool {
	return crc32.ChecksumIEEE(data) == binary.LittleEndian.Uint32(sum)
}
