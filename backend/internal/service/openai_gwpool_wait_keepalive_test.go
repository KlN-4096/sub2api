package service

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolWaitKeepalivePreservesNonsemanticBytesAcrossHandoff(t *testing.T) {
	c, recorder := newCompactBridgeTestContext(t, false)
	stop := startOpenAISSEKeepalive(c, time.Hour)
	defer stop()
	first, ok := c.MustGet(openAICompactSSEKeepaliveKey).(*openAICompactSSEKeepalive)
	require.True(t, ok)
	require.True(t, first.beat())
	require.True(t, first.beat())
	require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c))
	require.Equal(t, http.StatusOK, recorder.Code)
	// Receiving upstream headers stops the preparation heartbeat. The ordinary
	// stream may start its own keepalive before the first semantic output.
	c.Header("Content-Type", "text/event-stream")
	nextStop := startOpenAISSEKeepalive(c, time.Hour)
	defer nextStop()
	second, ok := c.MustGet(openAICompactSSEKeepaliveKey).(*openAICompactSSEKeepalive)
	require.True(t, ok)
	require.NotSame(t, first, second)
	require.True(t, second.beat())
	require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c))
	require.True(t, StopOpenAICompactSSEKeepaliveCommitted(c))
	_, err := c.Writer.WriteString("data: complete\n\n")
	require.NoError(t, err)
	require.Equal(t, len("data: complete\n\n"), OpenAICompactKeepaliveAdjustedWrittenSize(c))
	require.Equal(t, ": keepalive\n\n: keepalive\n\n: keepalive\n\ndata: complete\n\n", recorder.Body.String())
}

func TestGatewayPoolWaitKeepaliveBridgeErrorsStaySSE(t *testing.T) {
	for _, endpoint := range []string{"messages", "chat"} {
		t.Run(endpoint, func(t *testing.T) {
			c, recorder := newCompactBridgeTestContext(t, false)
			stop := startOpenAISSEKeepalive(c, time.Hour)
			defer stop()
			heartbeat, ok := c.MustGet(openAICompactSSEKeepaliveKey).(*openAICompactSSEKeepalive)
			require.True(t, ok)
			require.True(t, heartbeat.beat())
			if endpoint == "messages" {
				writeAnthropicError(c, 503, "api_error", "unavailable")
				require.Contains(t, recorder.Body.String(), "event: error\ndata:")
			} else {
				writeChatCompletionsError(c, 503, "api_error", "unavailable")
				require.Contains(t, recorder.Body.String(), "\ndata: {\"error\":")
			}
			require.Equal(t, 200, recorder.Code)
			require.NotContains(t, recorder.Body.String(), "\n\n{\"error\"")
			require.False(t, heartbeat.beat())
		})
	}
}

func TestGatewayPoolBridgePreambleFailureCanRecoverWithoutLeakingResponseID(t *testing.T) {
	for _, endpoint := range []string{"messages", "chat"} {
		t.Run(endpoint, func(t *testing.T) {
			c, recorder := newCompactBridgeTestContext(t, false)
			body := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"discarded-response\",\"model\":\"gpt-6-astra\"}}\n\n" +
				"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"temporary server error\"}}}\n\n"
			resp := &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
			svc := &OpenAIGatewayService{}
			account := gwpoolTestAccount(1)
			var err error
			if endpoint == "messages" {
				_, err = svc.handleAnthropicStreamingResponse(resp, c, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", time.Now())
			} else {
				_, err = svc.handleChatStreamingResponse(resp, c, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", time.Now(), 0)
			}
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Empty(t, recorder.Body.String())
			require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c))
		})
	}
}

func TestGatewayPoolWaitKeepaliveLeavesNonstreamUnchangedAndSharesActiveOwner(t *testing.T) {
	c, recorder := newCompactBridgeTestContext(t, false)
	stop := StartGatewayPoolWaitKeepalive(c, false)
	stop()
	require.Empty(t, recorder.Body.String())
	stop = startOpenAISSEKeepalive(c, time.Hour)
	defer stop()
	first := c.MustGet(openAICompactSSEKeepaliveKey)
	nextStop := StartGatewayPoolWaitKeepalive(c, true)
	defer nextStop()
	require.Same(t, first, c.MustGet(openAICompactSSEKeepaliveKey))
}
