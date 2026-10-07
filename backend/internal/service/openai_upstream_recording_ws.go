package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/Wei-Shaw/sub2api/internal/pkg/upstreamrecord"
	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

// The concrete connection is not replaced by an interface wrapper: raw relay
// intentionally requires *coderOpenAIWSClientConn, and pool capability checks
// must retain their original type/behavior.
type openAIWSRecording struct {
	gateway          *OpenAIGatewayService
	account          *Account // Policy identity only; credentials are not retained.
	meta             map[string]any
	mu               sync.Mutex
	capture          *upstreamrecord.Capture
	leaseCorrelation map[string]any
	frame            atomic.Uint64
	turn             atomic.Uint64
}

func (s *OpenAIGatewayService) dialRecordedOpenAIWS(ctx context.Context, account *Account, target string, headers http.Header, proxyURL string, dialer openAIWSClientDialer) (openAIWSClientConn, int, http.Header, error) {
	capture := s.beginOpenAIRecording(ctx, account, "websocket")
	if capture == nil {
		return dialer.Dial(ctx, target, headers, proxyURL)
	}
	parsed, _ := url.Parse(target)
	meta := map[string]any{"url": upstreamrecord.URL(parsed), "headers": upstreamrecord.Headers(headers), "view": "application_handshake"}
	capture.Note("ws_handshake_request", meta)
	conn, status, responseHeaders, err := dialer.Dial(ctx, target, headers, proxyURL)
	capture.Note("ws_handshake_response", map[string]any{
		"status": status, "connected": err == nil && conn != nil,
		"headers": upstreamrecord.Headers(responseHeaders), "error_kind": upstreamrecord.ErrorKind(err),
	})
	if err != nil || conn == nil {
		var handshakeErr *openAIWSHandshakeError
		if errors.As(err, &handshakeErr) {
			capture.Data("ws_handshake_error_body", handshakeErr.Body)
			capture.Note("ws_handshake_error_body_scope", map[string]any{"read_limit_bytes": 8 << 10, "complete": false})
		}
		capture.Finish("ws_handshake_failed")
		return conn, status, responseHeaders, err
	}
	raw, ok := conn.(*coderOpenAIWSClientConn)
	if !ok {
		capture.MarkIncomplete("unsupported_ws_transport")
		capture.Finish("unsupported_ws_transport")
		slog.Warn("openai_recording_gap", "account_id", account.ID, "reason", "unsupported_ws_transport")
	} else {
		meta["response_headers"] = upstreamrecord.Headers(responseHeaders)
		raw.recording.Store(&openAIWSRecording{
			gateway: s, account: &Account{ID: account.ID, Platform: account.Platform, Type: account.Type, Extra: map[string]any{openAIUpstreamRecordingExtraKey: true}},
			meta: meta, capture: capture,
		})
	}
	return conn, status, responseHeaders, err
}

// A pooled connection can predate the account opt-in. Start at this lease
// without reconnecting it and label the saved handshake as historical.
func (s *OpenAIGatewayService) recordOpenAIWSLease(ctx context.Context, account *Account, target string, headers http.Header, conn *openAIWSConn) {
	raw, ok := conn.ws.(*coderOpenAIWSClientConn)
	if !ok {
		return
	}
	if existing := raw.recording.Load(); existing != nil {
		existing.noteLease(ctx, account)
		return
	}
	if !openAIUpstreamRecordingEnabled(account) {
		return
	}
	capture := s.beginOpenAIRecording(ctx, account, "websocket_reused")
	if capture == nil {
		return
	}
	parsed, _ := url.Parse(target)
	meta := map[string]any{
		"url": upstreamrecord.URL(parsed), "lease_request_headers": upstreamrecord.Headers(headers),
		"response_headers": upstreamrecord.Headers(conn.handshakeHeaders), "historical_handshake": true,
		"connection_id": conn.id, "recording_started_after_handshake": true, "original_handshake_request_available": false,
	}
	capture.Note("ws_existing_connection", meta)
	recording := &openAIWSRecording{
		gateway: s, account: &Account{ID: account.ID, Platform: account.Platform, Type: account.Type, Extra: map[string]any{openAIUpstreamRecordingExtraKey: true}},
		meta: meta, capture: capture,
	}
	if !raw.recording.CompareAndSwap(nil, recording) {
		capture.Finish("already_recorded")
	} else {
		recording.noteLease(ctx, account)
	}
}

func (r *openAIWSRecording) noteLease(ctx context.Context, account *Account) {
	correlation := openAIRecordingCorrelation(ctx, "ws_pool_lease")
	r.mu.Lock()
	r.leaseCorrelation = correlation
	if !openAIUpstreamRecordingEnabled(account) {
		r.capture.Finish("account_disabled_at_lease")
		r.capture = nil
	}
	capture := r.capture
	r.mu.Unlock()
	capture.Note("ws_lease", map[string]any{"correlation": correlation})
}

func (r *openAIWSRecording) current() *upstreamrecord.Capture {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capture
}

func (r *openAIWSRecording) beforeWrite(ctx context.Context, msgType coderws.MessageType, payload []byte) (*upstreamrecord.Capture, uint64, func()) {
	if r == nil {
		return nil, 0, func() {}
	}
	if msgType == coderws.MessageText && gjson.GetBytes(payload, "type").String() == "response.create" {
		// New turns honor disabling even on an already-open recorded connection.
		// No DB lock is held while recording/reading the response.
		fresh := r.gateway.openAIRecordingAccount(ctx, r.account, true)
		r.mu.Lock()
		if fresh == nil {
			r.capture.Finish("account_disabled_or_policy_unavailable")
			r.capture = nil
		} else if r.capture == nil {
			r.capture = r.gateway.getOpenAIRecorder().Begin(fresh.ID, openAIRecordingMetadata(ctx, fresh, "websocket_resumed"))
			r.capture.Note("ws_existing_connection", r.meta)
		}
		capture := r.capture
		leaseCorrelation := r.leaseCorrelation
		r.mu.Unlock()
		wireModel := gjson.GetBytes(payload, "model")
		model, modelStatus := "", "absent"
		if wireModel.Type == gjson.String && len(wireModel.Str) <= 512 {
			model, modelStatus = wireModel.Str, "present"
		} else if wireModel.Exists() {
			modelStatus = "invalid_or_too_long"
		}
		capture.Note("ws_turn_start", map[string]any{
			"turn": r.turn.Add(1), "wire_model": model, "wire_model_status": modelStatus,
			"correlation": openAIRecordingCorrelation(ctx, "ws_turn"), "lease_correlation": leaseCorrelation,
		})
	}
	capture := r.current()
	release := capture.Hold()
	if release == nil {
		return nil, 0, func() {}
	}
	frame := r.frame.Add(1)
	capture.Note("ws_send_start", map[string]any{"frame": frame, "message_type": int(msgType), "bytes": len(payload), "turn": r.turn.Load()})
	capture.DataEvent("ws_send", payload, map[string]any{"frame": frame, "message_type": int(msgType), "turn": r.turn.Load()})
	return capture, frame, release
}

func (r *openAIWSRecording) read(msgType coderws.MessageType, payload []byte, err error) {
	if r == nil {
		return
	}
	capture := r.current()
	frame := r.frame.Add(1)
	capture.DataEvent("ws_receive", payload, map[string]any{"frame": frame, "message_type": int(msgType), "turn": r.turn.Load()})
	if err != nil {
		capture.Note("ws_read_end", map[string]any{"error_kind": upstreamrecord.ErrorKind(err), "close_status": int(coderws.CloseStatus(err))})
		capture.Finish("ws_read_ended")
	}
}

func (r *openAIWSRecording) finish(reason string) {
	if r != nil {
		r.current().Finish(reason)
	}
}

type openAIRecordingJSONWriter struct {
	ctx  context.Context
	conn *coderOpenAIWSClientConn
}

func (w openAIRecordingJSONWriter) Write(p []byte) (int, error) {
	if err := w.conn.WriteFrame(w.ctx, coderws.MessageText, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Mirrors the pinned coder/websocket wsjson.Write contract: one Encoder call,
// HTML escaping and final newline, the same message boundary and error wrapping.
// Do not Marshal again for logging: custom MarshalJSON methods can have effects.
func writeRecordedOpenAIWSJSON(ctx context.Context, conn *coderOpenAIWSClientConn, value any) error {
	if err := json.NewEncoder(openAIRecordingJSONWriter{ctx: ctx, conn: conn}).Encode(value); err != nil {
		return fmt.Errorf("failed to write JSON message: failed to marshal JSON: %w", err)
	}
	return nil
}
