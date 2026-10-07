package upstreamrecord

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Headers preserves routing material inside the encrypted archive, while
// excluding authentication and non-routing session cookies.
func Headers(source http.Header) http.Header {
	out := make(http.Header, len(source))
	for key, values := range source {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "authorization") || strings.Contains(lower, "api-key") ||
			strings.Contains(lower, "apikey") || strings.Contains(lower, "secret") ||
			strings.HasSuffix(lower, "-token") || strings.HasSuffix(lower, "_token") {
			out[key] = []string{"[redacted]"}
			continue
		}
		switch lower {
		case "authorization", "proxy-authorization", "x-api-key", "api-key",
			"x-auth-token", "x-access-token", "x-oai-attestation":
			out[key] = []string{"[redacted]"}
		case "cookie":
			var cookies []string
			for _, value := range values {
				for _, part := range strings.Split(value, ";") {
					name, _, ok := strings.Cut(strings.TrimSpace(part), "=")
					if ok && routingCookie(name) {
						cookies = append(cookies, strings.TrimSpace(part))
					}
				}
			}
			if len(cookies) > 0 {
				out[key] = []string{strings.Join(cookies, "; ")}
			}
		case "set-cookie":
			for _, value := range values {
				name, _, ok := strings.Cut(value, "=")
				if ok && routingCookie(strings.TrimSpace(name)) {
					out[key] = append(out[key], value)
				}
			}
		default:
			out[key] = append([]string(nil), values...)
		}
	}
	return out
}

func routingCookie(name string) bool { return name == "__cflb" || name == "__oailb" }

func URL(raw *url.URL) string {
	if raw == nil {
		return ""
	}
	out := *raw
	out.User, out.Fragment = nil, ""
	values := out.Query()
	for name := range values {
		lower := strings.ToLower(name)
		if strings.Contains(lower, "token") || strings.Contains(lower, "secret") ||
			strings.Contains(lower, "password") || strings.Contains(lower, "signature") ||
			lower == "key" || lower == "sig" || lower == "auth" ||
			strings.Contains(lower, "credential") || strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") ||
			strings.Contains(lower, "authorization") {
			values.Set(name, "[redacted]")
		}
	}
	out.RawQuery = values.Encode()
	return out.String()
}

// ErrorKind deliberately omits raw error text (which may echo credentials or a
// signed URL). The original error returned to the caller is never changed.
func ErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return "io_error"
	}
}

type observedBody struct {
	body      io.ReadCloser
	c         *Capture
	stream    string
	ctx       context.Context
	closeOnce sync.Once
	eof       atomic.Bool
	exchange  *httpObservation
	response  bool
	trailers  func() http.Header
}

const httpObservationCloseGrace = 5 * time.Second

// A RoundTripper may return a response before its request-body goroutine has
// stopped. Keep the capture alive until both sides close. Never wait in the
// caller: a bounded timer explicitly ends a non-conforming/unclosed exchange.
type httpObservation struct {
	capture        *Capture
	mu             sync.Mutex
	requests       int
	responseClosed bool
	reason         string
	finished       bool
	timer          *time.Timer
}

func (o *httpObservation) requestBody(body io.ReadCloser, stream string, ctx context.Context, trailers func() http.Header) io.ReadCloser {
	o.mu.Lock()
	o.requests++
	o.mu.Unlock()
	return &observedBody{body: body, c: o.capture, stream: stream, ctx: ctx, exchange: o, trailers: trailers}
}

func (o *httpObservation) endBody(response bool, reason string) {
	o.mu.Lock()
	if response {
		o.responseClosed, o.reason = true, reason
	} else {
		o.requests--
	}
	o.finishIfReadyLocked()
	if !o.finished && o.responseClosed {
		o.scheduleGraceLocked()
	}
	o.mu.Unlock()
}

func (o *httpObservation) finishIfReadyLocked() {
	if o.finished || !o.responseClosed || o.requests != 0 {
		return
	}
	o.capture.Note("http_observation_end", map[string]any{"request_bodies_pending": 0, "response_closed": true})
	o.capture.Finish(o.reason)
	if o.capture.isEnded() {
		o.finished = true
		if o.timer != nil {
			o.timer.Stop()
		}
	} else {
		o.scheduleGraceLocked()
	}
}

func (o *httpObservation) scheduleGraceLocked() {
	if o.timer != nil || o.finished {
		return
	}
	o.timer = time.AfterFunc(httpObservationCloseGrace, func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.finished || o.capture.isEnded() {
			o.finished = true
			return
		}
		o.finished = true
		o.capture.Note("http_observation_end", map[string]any{"request_bodies_pending": o.requests, "response_closed": o.responseClosed})
		o.capture.Abort("body_lifecycle_not_closed")
	})
}

func (b *observedBody) Read(p []byte) (int, error) {
	release := b.c.Hold()
	if release != nil {
		defer release()
	}
	n, err := b.body.Read(p)
	if n > 0 {
		b.c.Data(b.stream, p[:n])
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			b.eof.Store(true)
		}
		b.c.Note(b.stream+"_read_end", map[string]any{"error_kind": ErrorKind(err), "eof": b.eof.Load()})
		if b.eof.Load() && b.trailers != nil {
			b.c.Note(b.stream+"_trailers", Headers(b.trailers()))
		}
		if b.response {
			b.exchange.mu.Lock()
			b.exchange.scheduleGraceLocked()
			b.exchange.mu.Unlock()
		}
	}
	return n, err
}

func (b *observedBody) Close() error {
	err := b.body.Close()
	reason := "closed_before_eof"
	if b.eof.Load() {
		reason = "eof"
	} else if b.ctx != nil && b.ctx.Err() != nil {
		reason = ErrorKind(b.ctx.Err())
	}
	b.closeOnce.Do(func() {
		b.c.Note(b.stream+"_closed", map[string]any{"reason": reason, "close_error_kind": ErrorKind(err)})
		b.exchange.endBody(b.response, reason)
	})
	return err
}

// HTTP records the transport handoff and bytes consumed by that transport,
// rather than guessing what reached the peer. It does not pre-read, decompress,
// drain, change GetBody retry behavior, or alter an upstream error.
func HTTP(c *Capture, request *http.Request, send func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	if c == nil {
		return send(request)
	}
	out, observe := PrepareHTTP(c, request)
	response, err := send(out)
	return observe(response, err)
}

// PrepareHTTP supports send paths with early configuration failures while
// preserving their existing error handling.
func PrepareHTTP(c *Capture, request *http.Request) (*http.Request, func(*http.Response, error) (*http.Response, error)) {
	if c == nil {
		return request, func(response *http.Response, err error) (*http.Response, error) { return response, err }
	}
	c.Note("request_headers", map[string]any{
		"method": request.Method, "url": URL(request.URL), "headers": Headers(request.Header),
		"host": request.Host, "content_length": request.ContentLength, "transfer_encoding": request.TransferEncoding,
		"view": "application_transport_handoff",
	})
	observation := &httpObservation{capture: c}
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		c.Note("request_written", map[string]string{"error_kind": ErrorKind(info.Err)})
	}}
	out := request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	if request.Body != nil && request.Body != http.NoBody {
		out.Body = observation.requestBody(request.Body, "request_body", request.Context(), func() http.Header { return request.Trailer })
	}
	if request.GetBody != nil {
		var replays atomic.Uint64
		out.GetBody = func() (io.ReadCloser, error) {
			body, err := request.GetBody()
			if err != nil {
				return nil, err
			}
			name := "request_replay_" + strconv.FormatUint(replays.Add(1), 10)
			return observation.requestBody(body, name, request.Context(), nil), nil
		}
	}
	return out, func(response *http.Response, err error) (*http.Response, error) {
		if response != nil {
			c.Note("response_headers", map[string]any{
				"status": response.StatusCode, "headers": Headers(response.Header),
				"content_length": response.ContentLength, "uncompressed": response.Uncompressed,
				"transfer_encoding": response.TransferEncoding,
			})
		}
		if err != nil {
			c.Note("transport_error", map[string]string{"error_kind": ErrorKind(err)})
			if response == nil || response.Body == nil || response.Body == http.NoBody {
				observation.endBody(true, "transport_error")
				return response, err
			}
			observation.mu.Lock()
			observation.scheduleGraceLocked()
			observation.mu.Unlock()
		}
		if response == nil || response.Body == nil || response.Body == http.NoBody {
			observation.endBody(true, "no_response_body")
			return response, nil
		}
		response.Body = &observedBody{body: response.Body, c: c, stream: "response_body", ctx: request.Context(), exchange: observation, response: true, trailers: func() http.Header { return response.Trailer }}
		return response, err
	}
}
