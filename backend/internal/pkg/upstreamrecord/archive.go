package upstreamrecord

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	archiveMagic          = "OAICAP01\n"
	archiveSuffix         = ".oaicapture"
	archiveSegmentBytes   = 64 << 20
	maxArchiveFrameBytes  = 2 << 20
	maxArchiveEventsBytes = 1 << 20
	maxArchiveFiles       = 100000
	keyBytes              = 32
)

var errArchiveFull = errors.New("recording capacity reached")

type archiveWriter struct {
	opts    Options
	aead    cipher.AEAD
	file    *os.File
	written int64
	total   int64
	buffer  bytes.Buffer
	zip     *gzip.Writer
}

// Each application instance must own its archive directory. Files are unique
// and never overwritten; the single Recorder is shared by its HTTP/WS paths.
func openArchive(opts Options) (*archiveWriter, error) {
	if opts.Directory == "" || opts.KeyFile == "" {
		return nil, errors.New("recording paths are not configured")
	}
	root, err := filepath.Abs(opts.Directory)
	if err != nil {
		return nil, err
	}
	keyPath, err := filepath.Abs(opts.KeyFile)
	if err != nil {
		return nil, err
	}
	relativeKey, err := filepath.Rel(root, keyPath)
	if err == nil && relativeKey != ".." && !strings.HasPrefix(relativeKey, ".."+string(filepath.Separator)) {
		return nil, errors.New("recording key must be outside the archive directory")
	}
	opts.Directory, opts.KeyFile = root, keyPath
	if err := privateDirectory(root); err != nil {
		return nil, err
	}
	dir, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dir.Close() }()
	var total int64
	count := 0
	for {
		entries, readErr := dir.ReadDir(256)
		for _, entry := range entries {
			count++
			if count > maxArchiveFiles {
				return nil, errors.New("too many recording files")
			}
			info, statErr := entry.Info()
			if statErr != nil {
				return nil, statErr
			}
			if !info.Mode().IsRegular() {
				return nil, errors.New("archive directory contains a non-regular file")
			}
			// Count all files, not just files whose name we recognize.
			total += info.Size()
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if total >= opts.MaxBytes {
		return nil, errArchiveFull
	}
	key, err := loadOrCreateKey(keyPath, total == 0)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &archiveWriter{opts: opts, aead: aead, total: total}, nil
}

func privateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("recording directory is not a real directory")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return errors.New("recording directory must have private permissions")
	}
	return nil
}

func loadOrCreateKey(path string, allowCreate bool) ([]byte, error) {
	if !allowCreate {
		return readPrivateKey(path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	key := make([]byte, keyBytes)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		_, writeErr := file.Write(key)
		if writeErr == nil {
			writeErr = file.Sync()
		}
		closeErr := file.Close()
		if writeErr != nil {
			return nil, writeErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	return readPrivateKey(path)
}

func readPrivateKey(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != keyBytes {
		return nil, errors.New("invalid recording key file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("recording key must have private permissions")
	}
	key, err := os.ReadFile(path)
	if err == nil && len(key) != keyBytes {
		return nil, errors.New("recording key changed size")
	}
	return key, err
}

func (w *archiveWriter) write(event Event) error {
	plain, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(plain) > maxArchiveEventsBytes {
		return errors.New("recording event is too large")
	}
	w.buffer.Reset()
	if w.zip == nil {
		w.zip, _ = gzip.NewWriterLevel(&w.buffer, gzip.BestSpeed)
	} else {
		w.zip.Reset(&w.buffer)
	}
	if _, err := w.zip.Write(plain); err != nil {
		return err
	}
	if err := w.zip.Close(); err != nil {
		return err
	}
	nonce := make([]byte, w.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	sealed := w.aead.Seal(nonce, nonce, w.buffer.Bytes(), []byte(archiveMagic))
	size := int64(4 + len(sealed))
	newSegment := w.file == nil || w.written+size > archiveSegmentBytes
	additional := size
	if newSegment {
		additional += int64(len(archiveMagic))
	}
	if additional > w.opts.MaxBytes-w.total {
		return errArchiveFull
	}
	if newSegment {
		if err := w.close(); err != nil {
			return err
		}
		var id [8]byte
		if _, err := rand.Read(id[:]); err != nil {
			return err
		}
		name := time.Now().UTC().Format("20060102T150405.000000000") + "-" + hex.EncodeToString(id[:]) + archiveSuffix
		file, err := os.OpenFile(filepath.Join(w.opts.Directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		w.file, w.written = file, 0
		if err := w.append([]byte(archiveMagic)); err != nil {
			return err
		}
	}
	frame := make([]byte, 4+len(sealed))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(sealed)))
	copy(frame[4:], sealed)
	return w.append(frame)
}

func (w *archiveWriter) append(data []byte) error {
	n, err := w.file.Write(data)
	w.total += int64(n)
	w.written += int64(n)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

func (w *archiveWriter) close() error {
	if w.file == nil {
		return nil
	}
	syncErr := w.file.Sync()
	closeErr := w.file.Close()
	w.file = nil
	return errors.Join(syncErr, closeErr)
}

// ReadArchive authenticates/decompresses bounded frames. io.EOF at a frame
// boundary ends the segment, NOT a capture: consumers must find capture_end
// (possibly in a later segment) and check recording_complete.
func ReadArchive(reader io.Reader, key []byte, visit func(Event) error) error {
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	magic := make([]byte, len(archiveMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return err
	}
	if string(magic) != archiveMagic {
		return errors.New("unsupported recording format")
	}
	for {
		var size [4]byte
		if _, err := io.ReadFull(reader, size[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		n := binary.BigEndian.Uint32(size[:])
		if n < uint32(aead.NonceSize()+aead.Overhead()) || n > maxArchiveFrameBytes {
			return errors.New("invalid recording frame length")
		}
		sealed := make([]byte, n)
		if _, err := io.ReadFull(reader, sealed); err != nil {
			return err
		}
		compressed, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], []byte(archiveMagic))
		if err != nil {
			return errors.New("recording authentication failed")
		}
		zip, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return err
		}
		plain, readErr := io.ReadAll(io.LimitReader(zip, maxArchiveEventsBytes+1))
		closeErr := zip.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		if len(plain) > maxArchiveEventsBytes {
			return errors.New("recording decompression limit exceeded")
		}
		var event Event
		if err := json.Unmarshal(plain, &event); err != nil {
			return err
		}
		if event.Schema != 1 || strings.TrimSpace(event.CaptureID) == "" {
			return fmt.Errorf("unsupported recording event schema %d", event.Schema)
		}
		if err := visit(event); err != nil {
			return err
		}
	}
}
