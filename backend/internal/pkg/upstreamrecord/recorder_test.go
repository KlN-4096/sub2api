package upstreamrecord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func recordingFixture(t *testing.T, limit int64) (*Recorder, Options) {
	t.Helper()
	root := t.TempDir()
	opts := Options{Directory: filepath.Join(root, "records"), KeyFile: filepath.Join(root, "key"), MaxBytes: limit}
	return New(opts), opts
}

func closeRecording(t *testing.T, r *Recorder) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func readRecording(t *testing.T, opts Options) []Event {
	t.Helper()
	key, err := os.ReadFile(opts.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(opts.Directory, "*"+archiveSuffix))
	if err != nil {
		t.Fatal(err)
	}
	var events []Event
	for _, name := range files {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("private-prompt")) {
			t.Fatal("archive contains plaintext")
		}
		if err := ReadArchive(bytes.NewReader(data), key, func(e Event) error {
			events = append(events, e)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return events
}

func TestRecorderDisabledCreatesNoFilesAndPreservesRequest(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	req, _ := http.NewRequest(http.MethodPost, "https://example.invalid/responses", strings.NewReader("unchanged"))
	resp, err := HTTP(nil, req, func(got *http.Request) (*http.Response, error) {
		if got != req {
			t.Fatal("disabled path changed request")
		}
		return &http.Response{Body: http.NoBody}, nil
	})
	if err != nil || resp == nil {
		t.Fatal(err)
	}
	closeRecording(t, r)
	if _, err := os.Stat(opts.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("disabled recorder initialized storage")
	}
}

func TestRecorderHTTPPreservesBodiesRetriesAndExcludesCredentials(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	payload := append([]byte{0x28, 0xb5, 0x2f, 0xfd}, []byte("private-prompt compressed-wire-bytes")...)
	req, _ := http.NewRequest(http.MethodPost, "https://user:password@example.invalid/responses?api_key=secret&model=test", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer secret-access-token")
	req.Header.Set("X-Custom-Auth-Token", "secret-custom-token")
	req.Header.Set("Cookie", "auth_session=secret-session; __cflb=route-a; __oailb=route-b")
	req.Header.Set("Content-Encoding", "zstd")
	c := r.Begin(1, map[string]string{"kind": "test"})
	want := []byte("data: private-prompt\n\ndata: [DONE]\n\n")
	response, err := HTTP(c, req, func(got *http.Request) (*http.Response, error) {
		actual, err := io.ReadAll(got.Body)
		if err != nil || !bytes.Equal(actual, payload) {
			t.Fatalf("body changed: %v", err)
		}
		_ = got.Body.Close()
		replay, err := got.GetBody()
		if err != nil {
			t.Fatal(err)
		}
		again, err := io.ReadAll(replay)
		_ = replay.Close()
		if err != nil || !bytes.Equal(again, payload) {
			t.Fatal("GetBody retry changed")
		}
		return &http.Response{
			StatusCode: 200, Header: http.Header{"Set-Cookie": {"auth_session=secret-response", "__oailb=route-c"}},
			Body: io.NopCloser(bytes.NewReader(want)),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(response.Body)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("response changed")
	}
	_ = response.Body.Close()
	closeRecording(t, r)
	events := readRecording(t, opts)
	var requestBody, replayBody, responseBody []byte
	var summary Summary
	for _, e := range events {
		switch e.Kind {
		case "request_body":
			requestBody = append(requestBody, e.Data...)
		case "request_replay_1":
			replayBody = append(replayBody, e.Data...)
		case "response_body":
			responseBody = append(responseBody, e.Data...)
		case "capture_end":
			_ = json.Unmarshal(e.Details, &summary)
		}
	}
	if !bytes.Equal(requestBody, payload) || !bytes.Equal(replayBody, payload) || !bytes.Equal(responseBody, want) {
		t.Fatal("recorded bytes differ")
	}
	encoded, _ := json.Marshal(events)
	for _, forbidden := range []string{"secret-access-token", "secret-custom-token", "secret-session", "secret-response", "user:password", "api_key=secret"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("credential retained: %s", forbidden)
		}
	}
	if !bytes.Contains(encoded, []byte("route-b")) || !bytes.Contains(encoded, []byte("route-c")) {
		t.Fatal("routing cookie omitted from encrypted data")
	}
	if !summary.RecordingComplete || summary.Reason != "eof" {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestRecorderHTTPWaitsForAsyncRequestAndKeepsCloseError(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	req, _ := http.NewRequest(http.MethodPost, "https://example.invalid", strings.NewReader("late-request-body"))
	var deferredRequest io.ReadCloser
	closeErr := errors.New("response close failed")
	responseBody := &recordingCloseErrorBody{Reader: strings.NewReader("response"), err: closeErr}
	response, err := HTTP(r.Begin(1, nil), req, func(got *http.Request) (*http.Response, error) {
		deferredRequest = got.Body
		return &http.Response{StatusCode: 200, Body: responseBody}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != closeErr {
		t.Fatal("close error changed")
	}
	// Models plugin/HTTP transports that finish reading the request after EOF.
	if _, err := io.ReadAll(deferredRequest); err != nil {
		t.Fatal(err)
	}
	_ = deferredRequest.Close()
	closeRecording(t, r)
	var body []byte
	var closeSeen, complete bool
	for _, event := range readRecording(t, opts) {
		switch event.Kind {
		case "request_body":
			body = append(body, event.Data...)
		case "response_body_closed":
			closeSeen = bytes.Contains(event.Details, []byte(`"close_error_kind":"io_error"`))
		case "capture_end":
			var summary Summary
			_ = json.Unmarshal(event.Details, &summary)
			complete = summary.RecordingComplete
		}
	}
	if string(body) != "late-request-body" || !closeSeen || !complete {
		t.Fatalf("late data/close lost: body=%q close=%t complete=%t", body, closeSeen, complete)
	}
}

type recordingCloseErrorBody struct {
	io.Reader
	err error
}

func (b *recordingCloseErrorBody) Close() error { return b.err }

type countingBody struct {
	reads  int
	closes int
	err    error
}

func (b *countingBody) Read(p []byte) (int, error) {
	b.reads++
	return copy(p, "unread-response"), nil
}
func (b *countingBody) Close() error { b.closes++; return b.err }

func TestRecorderEarlyCloseDoesNotDrainOrHideCancellation(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.invalid", nil)
	closeErr := errors.New("close sentinel")
	body := &countingBody{err: closeErr}
	resp, err := HTTP(r.Begin(1, nil), req, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: body}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := resp.Body.Close(); err != closeErr {
		t.Fatal("close error changed")
	}
	if body.reads != 0 || body.closes != 1 {
		t.Fatal("recorder changed body consumption")
	}
	closeRecording(t, r)
	for _, e := range readRecording(t, opts) {
		if e.Kind == "capture_end" {
			var summary Summary
			_ = json.Unmarshal(e.Details, &summary)
			if summary.Reason != "canceled" {
				t.Fatalf("early close presented as EOF: %+v", summary)
			}
			return
		}
	}
	t.Fatal("missing summary")
}

func TestRecorderQueueOverloadHasFinalLossSummary(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	// Hold the writer deterministically so the queue cannot drain during fill.
	r.once.Do(func() {})
	c := r.Begin(1, nil)
	for i := 0; i <= queueEntries; i++ {
		c.Data("response_body", []byte("x"))
	}
	c.Finish("eof")
	go r.run()
	closeRecording(t, r)
	for _, e := range readRecording(t, opts) {
		if e.Kind == "capture_end" {
			var summary Summary
			_ = json.Unmarshal(e.Details, &summary)
			if summary.RecordingComplete || summary.LostEvents == 0 || summary.ObservedBytes["response_body"] != queueEntries+1 {
				t.Fatalf("loss was hidden: %+v", summary)
			}
			return
		}
	}
	t.Fatal("overload dropped the final summary")
}

func TestRecorderCapacityAndRestartDoNotOverwrite(t *testing.T) {
	r, opts := recordingFixture(t, 350)
	c := r.Begin(1, nil)
	c.Data("response_body", bytes.Repeat([]byte("private-prompt"), 100))
	c.Finish("eof")
	closeRecording(t, r)
	files, _ := filepath.Glob(filepath.Join(opts.Directory, "*"+archiveSuffix))
	before := make(map[string][]byte)
	for _, file := range files {
		before[file], _ = os.ReadFile(file)
	}
	second := New(opts)
	second.Begin(2, nil).Finish("eof")
	closeRecording(t, second)
	var size int64
	files, _ = filepath.Glob(filepath.Join(opts.Directory, "*"+archiveSuffix))
	for _, file := range files {
		after, _ := os.ReadFile(file)
		size += int64(len(after))
		if old, ok := before[file]; ok && !bytes.Equal(old, after) {
			t.Fatal("existing archive overwritten")
		}
	}
	if size > opts.MaxBytes {
		t.Fatalf("capacity exceeded: %d", size)
	}
}

func TestRecorderArchiveRejectsTamperingAndTruncation(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	r.Begin(1, nil).Finish("eof")
	closeRecording(t, r)
	files, _ := filepath.Glob(filepath.Join(opts.Directory, "*"+archiveSuffix))
	data, _ := os.ReadFile(files[0])
	key, _ := os.ReadFile(opts.KeyFile)
	tampered := append([]byte(nil), data...)
	tampered[len(tampered)-1] ^= 1
	for _, bad := range [][]byte{tampered, data[:len(data)-1]} {
		if err := ReadArchive(bytes.NewReader(bad), key, func(Event) error { return nil }); err == nil {
			t.Fatal("invalid archive accepted")
		}
	}
}

func TestRecorderDoesNotReplaceLostKeyForExistingArchives(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	r.Begin(1, nil).Finish("eof")
	closeRecording(t, r)
	// No files need deleting: simulate a misconfigured new/missing key path.
	opts.KeyFile = filepath.Join(filepath.Dir(opts.KeyFile), "missing-key")
	next := New(opts)
	next.Begin(1, nil).Finish("eof")
	closeRecording(t, next)
	if _, err := os.Stat(opts.KeyFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("silently generated another key for existing archives")
	}
}

func TestRecorderHoldKeepsConcurrentWriteOutcomeAfterPeerClose(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	capture := r.Begin(1, nil)
	release := capture.Hold()
	capture.Note("ws_send_start", nil)
	capture.Finish("ws_read_ended")
	capture.Note("ws_send_end", map[string]string{"error_kind": "io_error"})
	release()
	closeRecording(t, r)
	events := readRecording(t, opts)
	if len(events) != 4 || events[2].Kind != "ws_send_end" || events[3].Kind != "capture_end" {
		t.Fatalf("completion event lost: %+v", events)
	}
}

func TestRecorderStorageFailureDoesNotAffectForwarding(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	if err := os.WriteFile(opts.Directory, []byte("not-a-directory"), 0600); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
	resp, err := HTTP(r.Begin(1, nil), req, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("unchanged"))}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "unchanged" {
		t.Fatal("forwarding changed")
	}
	_ = resp.Body.Close()
	closeRecording(t, r)
	if !r.failed.Load() {
		t.Fatal("storage failure was hidden")
	}
}

type recordingConcurrentBody struct {
	started    chan struct{}
	closed     chan struct{}
	returnRead chan struct{}
}

func (b *recordingConcurrentBody) Read(p []byte) (int, error) {
	close(b.started)
	<-b.closed
	<-b.returnRead
	return copy(p, "bytes-after-close"), io.EOF
}
func (b *recordingConcurrentBody) Close() error { close(b.closed); return nil }

func TestRecorderConcurrentReadCloseKeepsReturnedBytes(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	body := &recordingConcurrentBody{started: make(chan struct{}), closed: make(chan struct{}), returnRead: make(chan struct{})}
	req, _ := http.NewRequest(http.MethodPost, "https://example.invalid", body)
	var sentBody io.ReadCloser
	_, err := HTTP(r.Begin(1, nil), req, func(got *http.Request) (*http.Response, error) {
		sentBody = got.Body
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = io.ReadAll(sentBody)
	}()
	<-body.started
	// Close completes while Read has not returned, without a data race inside
	// the fixture. The response is already closed at this point.
	if err := sentBody.Close(); err != nil {
		t.Fatal(err)
	}
	close(body.returnRead)
	<-readDone
	closeRecording(t, r)
	var observed []byte
	var complete bool
	for _, event := range readRecording(t, opts) {
		if event.Kind == "request_body" {
			observed = append(observed, event.Data...)
		}
		if event.Kind == "capture_end" {
			var summary Summary
			_ = json.Unmarshal(event.Details, &summary)
			complete = summary.RecordingComplete
		}
	}
	if string(observed) != "bytes-after-close" || !complete {
		t.Fatalf("concurrent completion lost data: %q %t", observed, complete)
	}
}

func TestRecorderHTTPUnclosedBodyExpiresIncomplete(t *testing.T) {
	r, opts := recordingFixture(t, 0)
	req, _ := http.NewRequest(http.MethodPost, "https://example.invalid", strings.NewReader("not-consumed"))
	capture := r.Begin(1, nil)
	_, err := HTTP(capture, req, func(*http.Request) (*http.Response, error) {
		return &http.Response{Body: http.NoBody}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(httpObservationCloseGrace + time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !capture.isEnded() {
		select {
		case <-deadline.C:
			t.Fatal("unclosed body retained an unbounded capture")
		case <-tick.C:
		}
	}
	closeRecording(t, r)
	for _, event := range readRecording(t, opts) {
		if event.Kind != "capture_end" {
			continue
		}
		var summary Summary
		_ = json.Unmarshal(event.Details, &summary)
		if summary.RecordingComplete || summary.Reason != "body_lifecycle_not_closed" {
			t.Fatalf("unclosed body presented as complete: %+v", summary)
		}
		return
	}
	t.Fatal("missing bounded incomplete summary")
}
