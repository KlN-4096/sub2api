// Package upstreamrecord provides opt-in, bounded, encrypted application-level
// recordings. It never sends requests, drains bodies, or blocks on disk in a
// transport read/write callback. Missing data is explicit, not a successful dump.
package upstreamrecord

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultMaxBytes  int64 = 2 << 30
	chunkBytes             = 64 << 10
	queueEntries           = 128
	maxCaptures            = 256
	maxMetadataBytes       = 256 << 10
	flushInterval          = 100 * time.Millisecond
)

type Options struct {
	Directory string
	KeyFile   string
	MaxBytes  int64
}

type Event struct {
	Schema    int             `json:"schema"`
	CaptureID string          `json:"capture_id"`
	AccountID int64           `json:"account_id"`
	Sequence  uint64          `json:"sequence"`
	At        time.Time       `json:"at"`
	Kind      string          `json:"kind"`
	Details   json.RawMessage `json:"details,omitempty"`
	Data      []byte          `json:"data,omitempty"`
}

// Summary describes recording integrity, not model quality or wire delivery.
type Summary struct {
	Reason            string           `json:"reason"`
	RecordingComplete bool             `json:"recording_complete"`
	LostEvents        int64            `json:"lost_events"`
	ObservedBytes     map[string]int64 `json:"observed_bytes"`
}

type queuedEvent struct {
	capture *Capture
	event   Event
}

type Recorder struct {
	opts  Options
	once  sync.Once
	mu    sync.Mutex
	live  map[*Capture]struct{}
	queue chan queuedEvent
	wake  chan struct{}
	done  chan struct{}
	stop  bool
	// fault is a fixed category only. Transport errors/URLs must not enter logs.
	fault           string
	failed          atomic.Bool
	lastBusyWarning time.Time
}

func New(opts Options) *Recorder {
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = DefaultMaxBytes
	}
	return &Recorder{
		opts: opts, live: make(map[*Capture]struct{}),
		queue: make(chan queuedEvent, queueEntries),
		wake:  make(chan struct{}, 1), done: make(chan struct{}),
	}
}

// Begin does no filesystem IO. A nil capture means recording is unavailable;
// callers must continue the original transport operation unchanged.
func (r *Recorder) Begin(accountID int64, metadata any) *Capture {
	if r == nil || accountID <= 0 {
		return nil
	}
	details, err := json.Marshal(metadata)
	if err != nil || len(details) > maxMetadataBytes {
		slog.Warn("openai_recording_skipped", "account_id", accountID, "reason", "metadata_invalid")
		return nil
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		slog.Warn("openai_recording_skipped", "account_id", accountID, "reason", "random_unavailable")
		return nil
	}
	c := &Capture{
		recorder: r, id: hex.EncodeToString(id[:]), accountID: accountID,
		start: details, startedAt: time.Now().UTC(), observed: make(map[string]int64),
	}
	r.mu.Lock()
	if r.stop || r.fault != "" {
		// The transition already produced one explicit stop alert. Do not turn
		// a full archive into an unbounded ordinary-log flood.
		r.mu.Unlock()
		return nil
	}
	if len(r.live) >= maxCaptures {
		warn := time.Since(r.lastBusyWarning) >= time.Minute
		if warn {
			r.lastBusyWarning = time.Now()
		}
		r.mu.Unlock()
		if warn {
			slog.Warn("openai_recording_skipped", "reason", "active_capture_limit")
		}
		return nil
	}
	r.live[c] = struct{}{}
	r.mu.Unlock()
	r.once.Do(func() { go r.run() })
	r.signal()
	return c
}

type Capture struct {
	recorder  *Recorder
	id        string
	accountID int64
	start     json.RawMessage
	startedAt time.Time

	mu        sync.Mutex
	sequence  uint64
	pending   int
	lost      int64
	observed  map[string]int64
	ended     bool
	finishing bool
	holds     int
	reason    string
	// Only the writer goroutine uses written.
	written bool
}

func (c *Capture) ID() string {
	if c == nil {
		return ""
	}
	return c.id
}

func (c *Capture) Note(kind string, metadata any) {
	if c == nil {
		return
	}
	details, err := json.Marshal(metadata)
	if err != nil || len(details) > maxMetadataBytes {
		c.markLost()
		return
	}
	c.enqueue(kind, details, nil)
}

// Data copies only bounded chunks and never retains the caller's mutable buffer.
// Once this capture loses an event it retains no further payload; its final
// summary still states the loss and the total bytes observed.
func (c *Capture) Data(stream string, payload []byte) {
	c.data(stream, payload, nil)
}

// DataEvent keeps a frame/body-instance identifier on every bounded chunk,
// including when concurrent WebSocket read/write events interleave.
func (c *Capture) DataEvent(stream string, payload []byte, metadata any) {
	if c == nil {
		return
	}
	details, err := json.Marshal(metadata)
	if err != nil || len(details) > maxMetadataBytes {
		c.markLost()
		return
	}
	c.data(stream, payload, details)
}

func (c *Capture) data(stream string, payload []byte, details json.RawMessage) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.observed[stream] += int64(len(payload))
	c.mu.Unlock()
	for len(payload) > 0 {
		n := min(len(payload), chunkBytes)
		if !c.enqueue(stream, details, payload[:n]) {
			return
		}
		payload = payload[n:]
	}
}

func (c *Capture) markLost() {
	c.MarkIncomplete("queue_or_metadata_limit")
}

// MarkIncomplete is for bounded-observation failures such as an unclosed body.
// It does not change the transport; the final summary cannot claim completeness.
func (c *Capture) MarkIncomplete(reason string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	first := c.lost == 0
	c.lost++
	c.mu.Unlock()
	if first {
		slog.Warn("openai_recording_gap", "account_id", c.accountID, "capture_id", c.id, "reason", reason)
	}
}

func (c *Capture) enqueue(kind string, details json.RawMessage, payload []byte) bool {
	c.mu.Lock()
	if c.ended || c.lost > 0 || c.recorder.failed.Load() {
		c.mu.Unlock()
		return false
	}
	c.sequence++
	e := Event{
		Schema: 1, CaptureID: c.id, AccountID: c.accountID,
		Sequence: c.sequence, At: time.Now().UTC(), Kind: kind, Details: details,
		Data: append([]byte(nil), payload...),
	}
	c.pending++
	select {
	case c.recorder.queue <- queuedEvent{capture: c, event: e}:
		c.mu.Unlock()
		return true
	default:
		c.pending--
		c.lost++
		c.mu.Unlock()
		slog.Warn("openai_recording_gap", "account_id", c.accountID, "capture_id", c.id, "reason", "queue_full")
		return false
	}
}

func (c *Capture) Finish(reason string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if !c.ended && !c.finishing {
		c.finishing, c.reason = true, reason
		c.ended = c.holds == 0
	}
	c.mu.Unlock()
	c.recorder.signal()
}

// Hold keeps an already-started transport operation's completion observable
// when the peer closes concurrently. It never waits for the transport itself.
func (c *Capture) Hold() func() {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.ended || c.finishing {
		c.mu.Unlock()
		return nil
	}
	c.holds++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.holds--
			if c.finishing && c.holds == 0 {
				c.ended = true
			}
			c.mu.Unlock()
			c.recorder.signal()
		})
	}
}

func (c *Capture) isEnded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ended
}

// Abort bounds observation, not the transport. Late callbacks may still return
// to the caller, but this capture is explicitly incomplete before being closed.
func (c *Capture) Abort(reason string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.ended {
		c.mu.Unlock()
		return
	}
	c.lost++
	c.finishing, c.ended, c.reason = true, true, reason
	c.mu.Unlock()
	slog.Warn("openai_recording_gap", "account_id", c.accountID, "capture_id", c.id, "reason", reason)
	c.recorder.signal()
}

func (r *Recorder) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Recorder) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.stop = true
	for c := range r.live {
		c.mu.Lock()
		if !c.ended {
			c.ended, c.reason = true, "process_shutdown"
			c.lost++
		}
		c.mu.Unlock()
	}
	r.mu.Unlock()
	r.once.Do(func() { go r.run() })
	r.signal()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Recorder) run() {
	defer close(r.done)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	var archive *archiveWriter
	defer func() {
		if archive != nil {
			if err := archive.close(); err != nil {
				slog.Warn("openai_recording_stopped", "reason", "archive_close_failed")
			}
		}
	}()
	write := func(e Event) {
		r.mu.Lock()
		failed := r.fault != ""
		r.mu.Unlock()
		if failed {
			return
		}
		var err error
		if archive == nil {
			archive, err = openArchive(r.opts)
		}
		if err == nil {
			err = archive.write(e)
		}
		if err != nil {
			reason := "archive_write_failed"
			if errors.Is(err, errArchiveFull) {
				reason = "capacity_reached"
			}
			r.mu.Lock()
			r.fault = reason
			r.failed.Store(true)
			r.mu.Unlock()
			// All unfinished captures now lack a trustworthy final summary.
			// Never expose an IO error string, which can contain sensitive paths.
			slog.Warn("openai_recording_stopped", "reason", reason)
		}
	}
	start := func(c *Capture) {
		if !c.written {
			c.written = true
			write(Event{Schema: 1, CaptureID: c.id, AccountID: c.accountID, At: c.startedAt, Kind: "capture_start", Details: c.start})
		}
	}
	for {
		select {
		case item := <-r.queue:
			start(item.capture)
			write(item.event)
			item.capture.mu.Lock()
			item.capture.pending--
			item.capture.mu.Unlock()
		case <-r.wake:
		case <-ticker.C:
		}
		// Final summaries have reserved storage in the bounded live map, not the
		// data queue: overload cannot silently discard the loss report.
		r.mu.Lock()
		var finished []*Capture
		for c := range r.live {
			c.mu.Lock()
			ready := c.ended && c.pending == 0
			c.mu.Unlock()
			if ready {
				finished = append(finished, c)
				delete(r.live, c)
			}
		}
		stopping := r.stop && len(r.live) == 0
		r.mu.Unlock()
		for _, c := range finished {
			start(c)
			c.mu.Lock()
			summary, _ := json.Marshal(Summary{Reason: c.reason, RecordingComplete: c.lost == 0, LostEvents: c.lost, ObservedBytes: c.observed})
			end := Event{Schema: 1, CaptureID: c.id, AccountID: c.accountID, Sequence: c.sequence + 1, At: time.Now().UTC(), Kind: "capture_end", Details: summary}
			c.mu.Unlock()
			write(end)
		}
		if stopping {
			return
		}
	}
}
