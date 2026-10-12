package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolRetryWaitsForBusyWSTurnSlotAndCancels(t *testing.T) {
	h := &OpenAIGatewayHandler{gatewayService: &service.OpenAIGatewayService{}}
	account := &service.Account{
		ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true,
		Credentials: map[string]any{"chatgpt_account_id": "offline-account", "chatgpt_user_id": "offline-user"},
		Extra:       map[string]any{"openai_gwpool": true},
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil).WithContext(ctx)
	calls, released := 0, false
	release, acquired, err := h.acquireGatewayPoolWSTurnSlot(ctx, c, account, func() (func(), bool, error) {
		calls++
		if calls == 1 {
			return nil, false, nil
		}
		return func() { released = true }, true, nil
	})
	require.NoError(t, err)
	require.True(t, acquired)
	require.Equal(t, 2, calls)
	release()
	require.True(t, released)
	cancel()
	_, acquired, err = h.acquireGatewayPoolWSTurnSlot(ctx, c, account, func() (func(), bool, error) {
		t.Fatal("canceled turn must not acquire another slot")
		return nil, false, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, acquired)
}

func TestGatewayPoolRetryUsesDeadlineInsteadOfAttemptCount(t *testing.T) {
	failure := &service.UpstreamFailoverError{
		GatewayPoolRetry: true, RetryableOnSameAccount: true, SameAccountRetryOnly: true,
		NextAccountAction: service.NextAccountStop,
	}
	require.True(t, sameAccountRetryAllowed(failure, 1000, 0))
	count := 100
	failure.SafeToFailoverAfterWrite = true
	require.False(t, openAIFirstOutputFailoverExhausted(failure, &count))
	failure.SameAccountRetryDeadline = time.Now().Add(-time.Second)
	require.False(t, sameAccountRetryAllowed(failure, 0, 10))
}

// 同账号重试截止耗尽后冒出来的池错误可能不带说明（重试失败被改标成池原因）：Messages 入口
// 不能因此回 turn-state 暂停文案，要与 Responses 入口一样按上游状态码归一。
func TestGatewayPoolMessagelessFailureMatchesResponsesMapping(t *testing.T) {
	failure := func() *service.UpstreamFailoverError {
		return &service.UpstreamFailoverError{StatusCode: http.StatusBadGateway, Reason: service.OpenAIGatewayPoolReason}
	}
	h := &OpenAIGatewayHandler{}
	messages := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(messages)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	h.handleAnthropicFailoverExhausted(c, failure(), false)
	responses := httptest.NewRecorder()
	c, _ = gin.CreateTestContext(responses)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	h.handleFailoverExhausted(c, failure(), false)

	require.NotContains(t, messages.Body.String(), "turn-state")
	status, _, message := h.mapUpstreamError(http.StatusBadGateway)
	require.Equal(t, status, responses.Code)
	require.Equal(t, status, messages.Code)
	require.Contains(t, messages.Body.String(), message)
}

func TestGatewayPoolRetryMessagesFailureAfterHeartbeatIsTerminalSSE(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "failover", true: "fallback"}[fallback], func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			service.MarkOpenAICompactClientStream(c)
			stop := service.StartOpenAICompactSSEKeepalive(c, time.Millisecond)
			defer stop()
			require.Eventually(t, c.Writer.Written, time.Second, time.Millisecond)
			stop()
			h := &OpenAIGatewayHandler{}
			if fallback {
				require.True(t, h.ensureAnthropicErrorResponse(c, false))
			} else {
				h.handleAnthropicFailoverExhausted(c, &service.UpstreamFailoverError{
					StatusCode: 503, Reason: service.OpenAIGatewayPoolReason,
					ClientStatusCode: 503, ClientMessage: "no verified tickets",
				}, false)
				require.Contains(t, w.Body.String(), "no verified tickets")
			}
			require.Equal(t, 200, w.Code)
			require.Contains(t, w.Body.String(), "event: error\ndata:")
			require.NotContains(t, w.Body.String(), "\n\n{\"")
		})
	}
}
