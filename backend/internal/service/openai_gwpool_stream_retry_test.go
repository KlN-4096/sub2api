package service

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolBufferedRetryBeforeDelivery(t *testing.T) {
	for _, endpoint := range []string{"messages", "chat"} {
		for _, failure := range []string{"eof", "done", "read-error", "interval", "cancel", "oversized"} {
			t.Run(endpoint+"/"+failure, func(t *testing.T) {
				c, recorder := newCompactBridgeTestContext(t, false)
				ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
				defer cancel()
				c.Request = c.Request.WithContext(ctx)
				cfg := &config.Config{}
				body := io.NopCloser(strings.NewReader(""))
				switch failure {
				case "done":
					body = io.NopCloser(strings.NewReader("data: [DONE]\n\n"))
				case "read-error", "cancel":
					body = &openAICompatBufferedReadErrorCloser{err: io.ErrUnexpectedEOF}
					if failure == "cancel" {
						cancel()
					}
				case "oversized":
					body = &openAICompatBufferedReadErrorCloser{err: bufio.ErrTooLong}
				case "interval":
					reader, writer := io.Pipe()
					defer func() { _ = writer.Close() }()
					body = reader
					cfg.Gateway.StreamDataIntervalTimeout = 1
				}
				defer func() { _ = body.Close() }()
				svc := &OpenAIGatewayService{cfg: cfg}
				account := gwpoolTestAccount(1)
				resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}
				var err error
				if endpoint == "messages" {
					_, err = svc.handleAnthropicBufferedStreamingResponse(resp, c, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", time.Now())
				} else {
					_, err = svc.handleChatBufferedStreamingResponse(resp, c, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", time.Now())
				}
				var failover *UpstreamFailoverError
				if failure == "cancel" || failure == "oversized" {
					require.Error(t, err)
					require.NotErrorAs(t, err, &failover)
				} else {
					require.ErrorAs(t, err, &failover)
					require.Equal(t, http.StatusBadGateway, failover.StatusCode)
				}
				require.False(t, c.Writer.Written(), "do not commit an intermediate JSON error")
				require.Empty(t, recorder.Body.String())
			})
		}
	}
}

type gatewayPoolHeartbeatWriter struct {
	gin.ResponseWriter
	heartbeat chan struct{}
}

func (w *gatewayPoolHeartbeatWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if strings.Contains(string(data), "event: ping") || string(data) == ":\n\n" {
		select {
		case w.heartbeat <- struct{}{}:
		default:
		}
	}
	return n, err
}

func TestGatewayPoolStreamRetryPreservesPreambleAcrossNativeHeartbeat(t *testing.T) {
	for _, endpoint := range []string{"messages", "chat"} {
		for _, success := range []bool{false, true} {
			t.Run(endpoint+map[bool]string{false: "/failed", true: "/success"}[success], func(t *testing.T) {
				c, recorder := newCompactBridgeTestContext(t, false)
				heartbeat := make(chan struct{}, 1)
				c.Writer = &gatewayPoolHeartbeatWriter{ResponseWriter: c.Writer, heartbeat: heartbeat}
				ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
				defer cancel()
				c.Request = c.Request.WithContext(ctx)
				reader, writer := io.Pipe()
				defer func() { _ = reader.Close() }()
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					defer func() { _ = writer.Close() }()
					_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_staged\",\"model\":\"gpt-6-astra\"}}\n\n")
					select {
					case <-heartbeat:
					case <-ctx.Done():
						return
					}
					if !success {
						_, _ = io.WriteString(writer, "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"temporary\"}}}\n\n")
						return
					}
					_, _ = io.WriteString(writer,
						"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_ok\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n"+
							"data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n"+
							"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"answer\"}\n\n"+
							"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_staged\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
				}()
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{StreamKeepaliveInterval: 1}}}
				account := gwpoolTestAccount(1)
				resp := &http.Response{StatusCode: 200, Header: http.Header{}, Body: reader}
				var err error
				if endpoint == "messages" {
					_, err = svc.handleAnthropicStreamingResponse(resp, c, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", time.Now())
				} else {
					_, err = svc.handleChatStreamingResponse(resp, c, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", time.Now(), 0)
				}
				<-finished
				if success {
					require.NoError(t, err)
					require.Contains(t, recorder.Body.String(), "answer")
					if endpoint == "messages" {
						require.Contains(t, recorder.Body.String(), "event: message_start")
						require.Less(t, strings.Index(recorder.Body.String(), "event: message_start"),
							strings.Index(recorder.Body.String(), "answer"))
					}
				} else {
					var failure *UpstreamFailoverError
					require.ErrorAs(t, err, &failure)
					require.NotContains(t, recorder.Body.String(), "resp_staged")
					require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c))
					require.True(t, StopOpenAICompactSSEKeepaliveCommitted(c), "native heartbeat already committed SSE headers")
				}
			})
		}
	}
}

func TestGatewayPoolStreamRetryReadErrorAndIntervalBeforeOutput(t *testing.T) {
	for _, endpoint := range []string{"messages", "chat"} {
		for _, interval := range []bool{false, true} {
			t.Run(endpoint+map[bool]string{false: "/read-error", true: "/interval"}[interval], func(t *testing.T) {
				c, recorder := newCompactBridgeTestContext(t, false)
				reader, writer := io.Pipe()
				defer func() { _ = reader.Close() }()
				defer func() { _ = writer.Close() }()
				cfg := &config.Config{}
				if interval {
					cfg.Gateway.StreamDataIntervalTimeout = 1
				} else {
					require.NoError(t, writer.CloseWithError(io.ErrUnexpectedEOF))
				}
				svc := &OpenAIGatewayService{cfg: cfg}
				account := gwpoolTestAccount(1)
				resp := &http.Response{StatusCode: 200, Header: http.Header{}, Body: reader}
				var err error
				if endpoint == "messages" {
					_, err = svc.handleAnthropicStreamingResponse(resp, c, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", time.Now())
				} else {
					_, err = svc.handleChatStreamingResponse(resp, c, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", time.Now(), 0)
				}
				var failure *UpstreamFailoverError
				require.ErrorAs(t, err, &failure)
				require.Empty(t, recorder.Body.String())
			})
		}
	}
}
