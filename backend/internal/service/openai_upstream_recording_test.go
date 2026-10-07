package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/upstreamrecord"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

type openAIRecordingPolicyRepo struct {
	AccountRepository
	account *Account
	calls   atomic.Int64
}

func (r *openAIRecordingPolicyRepo) GetByID(context.Context, int64) (*Account, error) {
	r.calls.Add(1)
	return r.account, nil
}

func openAIRecordingFixture(t *testing.T) (*OpenAIGatewayService, upstreamrecord.Options, *Account) {
	t.Helper()
	dir := t.TempDir()
	opts := upstreamrecord.Options{Directory: filepath.Join(dir, "archives"), KeyFile: filepath.Join(dir, "key")}
	svc := &OpenAIGatewayService{openaiRecording: upstreamrecord.New(opts)}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{openAIUpstreamRecordingExtraKey: true}}
	return svc, opts, account
}

func readOpenAIRecordingEvents(t *testing.T, svc *OpenAIGatewayService, opts upstreamrecord.Options) []upstreamrecord.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, svc.StopOpenAIRecording(ctx))
	key, err := os.ReadFile(opts.KeyFile)
	require.NoError(t, err)
	files, err := filepath.Glob(filepath.Join(opts.Directory, "*.oaicapture"))
	require.NoError(t, err)
	var events []upstreamrecord.Event
	for _, path := range files {
		file, err := os.Open(path)
		require.NoError(t, err)
		err = upstreamrecord.ReadArchive(file, key, func(event upstreamrecord.Event) error {
			events = append(events, event)
			return nil
		})
		_ = file.Close()
		require.NoError(t, err)
	}
	return events
}

func TestOpenAIRecordingPolicyDefaultOffAndFreshDisable(t *testing.T) {
	for _, name := range []string{"missing", "malformed", "other-platform", "fresh-disabled"} {
		t.Run(name, func(t *testing.T) {
			svc, opts, account := openAIRecordingFixture(t)
			fresh := *account
			fresh.Extra = map[string]any{openAIUpstreamRecordingExtraKey: false}
			repo := &openAIRecordingPolicyRepo{account: &fresh}
			svc.accountRepo = repo
			switch name {
			case "missing":
				account.Extra = nil
			case "malformed":
				account.Extra[openAIUpstreamRecordingExtraKey] = "true"
			case "other-platform":
				account.Platform = PlatformAnthropic
			}
			req, _ := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
			_, err := svc.recordOpenAIHTTP(account, req, "inference", func(got *http.Request) (*http.Response, error) {
				require.Same(t, req, got)
				return &http.Response{Body: http.NoBody}, nil
			})
			require.NoError(t, err)
			require.NoError(t, svc.StopOpenAIRecording(context.Background()))
			_, err = os.Stat(opts.Directory)
			require.True(t, os.IsNotExist(err))
			if name == "fresh-disabled" {
				require.EqualValues(t, 1, repo.calls.Load())
			} else {
				require.Zero(t, repo.calls.Load(), "default off must not add policy reads")
			}
		})
	}
}

func TestOpenAIRecordingWorksWithoutGatewayPool(t *testing.T) {
	svc, opts, account := openAIRecordingFixture(t)
	account.Type = AccountTypeAPIKey
	req, _ := http.NewRequest(http.MethodPost, "https://example.invalid/v1/responses", strings.NewReader(`{"model":"test","input":"private"}`))
	resp, err := svc.recordOpenAIHTTP(account, req, "inference", func(got *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(got.Body)
		require.NoError(t, err)
		_ = got.Body.Close()
		require.Contains(t, string(body), `"input":"private"`)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))
	_ = resp.Body.Close()
	events := readOpenAIRecordingEvents(t, svc, opts)
	require.NotEmpty(t, events)
	require.Equal(t, "capture_start", events[0].Kind)
	require.Equal(t, "capture_end", events[len(events)-1].Kind)
}

func TestOpenAIRecordingCorrelationStatesMissingAndOversizedValues(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.RequestID, strings.Repeat("x", 513))
	ctx = context.WithValue(ctx, ctxkey.Model, 123)
	got := openAIRecordingCorrelation(ctx, "models")
	status, ok := got["availability"].(map[string]string)
	require.True(t, ok)
	require.Equal(t, "context_value_too_long", status["request_id"])
	require.Equal(t, "invalid_context_type", status["model"])
	require.Equal(t, "no_user_request_context", status["client_request_id"])
	require.NotContains(t, got, "request_id")
}

func TestOpenAIRecordingWSReusedLeaseHasCurrentRequestCorrelation(t *testing.T) {
	svc, opts, account := openAIRecordingFixture(t)
	first := context.WithValue(context.Background(), ctxkey.RequestID, "request-A")
	capture := svc.beginOpenAIRecording(first, account, "websocket")
	binding := &openAIWSRecording{gateway: svc, account: account, capture: capture}
	conn := &coderOpenAIWSClientConn{}
	conn.recording.Store(binding)
	second := context.WithValue(context.Background(), ctxkey.RequestID, "request-B")
	svc.recordOpenAIWSLease(second, account, "wss://example.invalid", nil, &openAIWSConn{ws: conn})
	c, frame, release := binding.beforeWrite(context.Background(), coderws.MessageText, []byte(`{"type":"response.create","model":"wire-model"}`))
	c.Note("ws_send_end", map[string]any{"frame": frame})
	release()
	binding.finish("test_finished")
	events := readOpenAIRecordingEvents(t, svc, opts)
	for _, event := range events {
		if event.Kind != "ws_turn_start" {
			continue
		}
		var meta map[string]any
		require.NoError(t, json.Unmarshal(event.Details, &meta))
		lease, ok := meta["lease_correlation"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "request-B", lease["request_id"])
		require.Equal(t, "wire-model", meta["wire_model"])
		correlation, ok := meta["correlation"].(map[string]any)
		require.True(t, ok)
		availability, ok := correlation["availability"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "context_absent", availability["request_id"])
		return
	}
	t.Fatal("missing WS turn association")
}

type recordingMarshalOnce struct{ calls *atomic.Int64 }

func (v recordingMarshalOnce) MarshalJSON() ([]byte, error) {
	v.calls.Add(1)
	return []byte(`{"type":"response.create","model":"test","input":"<private>&"}`), nil
}

func TestOpenAIRecordingWebSocketPreservesJSONAndBinaryFrames(t *testing.T) {
	svc, opts, account := openAIRecordingFixture(t)
	received := make(chan []byte, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add("Set-Cookie", "__oailb=route; Path=/")
		conn, err := coderws.Accept(w, req, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		for i := 0; i < 2; i++ {
			kind, payload, err := conn.Read(req.Context())
			if err != nil {
				return
			}
			received <- append([]byte(nil), payload...)
			if err := conn.Write(req.Context(), kind, payload); err != nil {
				return
			}
		}
		_ = conn.Close(coderws.StatusNormalClosure, "")
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, _, err := svc.dialRecordedOpenAIWS(ctx, account, "ws"+strings.TrimPrefix(server.URL, "http"),
		http.Header{"Authorization": {"Bearer never-save-this"}}, "", newDefaultOpenAIWSClientDialer())
	require.NoError(t, err)
	raw, ok := conn.(*coderOpenAIWSClientConn)
	require.True(t, ok, "recording must not replace the concrete connection type")
	var calls atomic.Int64
	require.NoError(t, conn.WriteJSON(ctx, recordingMarshalOnce{calls: &calls}))
	text, err := conn.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, <-received, text)
	require.Contains(t, string(text), `\u003cprivate\u003e\u0026`)
	require.True(t, bytes.HasSuffix(text, []byte("\n")))
	require.EqualValues(t, 1, calls.Load(), "recording must not serialize a payload twice")
	binaryPayload := []byte{0, 1, 255, 10}
	require.NoError(t, raw.WriteFrame(ctx, coderws.MessageBinary, binaryPayload))
	kind, binaryResponse, err := raw.ReadFrame(ctx)
	require.NoError(t, err)
	require.Equal(t, coderws.MessageBinary, kind)
	require.Equal(t, binaryPayload, binaryResponse)
	require.Equal(t, binaryPayload, <-received)
	require.NoError(t, conn.Close())
	events := readOpenAIRecordingEvents(t, svc, opts)
	var sends, reads [][]byte
	for _, event := range events {
		if event.Kind == "ws_send" {
			sends = append(sends, event.Data)
		}
		if event.Kind == "ws_receive" {
			reads = append(reads, event.Data)
		}
	}
	require.Equal(t, [][]byte{text, binaryPayload}, sends)
	require.Equal(t, sends, reads)
	data, _ := json.Marshal(events)
	require.NotContains(t, string(data), "never-save-this")
	require.Contains(t, string(data), "__oailb=route")
}
