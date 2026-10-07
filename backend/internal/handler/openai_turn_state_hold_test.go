//go:build unit

package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// TestOpenAITurnStateHoldExhaustionReturns503WithMessage 钉住降智暂停换号耗尽时给客户端的
// 形态：503 + 带模型名的说明，别被归一成「上游暂时不可用」的 502。
func TestOpenAITurnStateHoldExhaustionReturns503WithMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	message := "account has no healthy x-codex-turn-state for gpt-6-astra and is paused until the hunter finds one"

	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode:       http.StatusServiceUnavailable,
		Reason:           service.OpenAITurnStateHoldReason,
		ClientStatusCode: http.StatusServiceUnavailable,
		ClientMessage:    message,
	}, false)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Contains(t, recorder.Body.String(), message)

	// Claude 兼容桥（/v1/messages）是服务端注入的主要消费者，同样不能被归一成 502。
	bridge := httptest.NewRecorder()
	bc, _ := gin.CreateTestContext(bridge)
	(&OpenAIGatewayHandler{}).handleAnthropicFailoverExhausted(bc, &service.UpstreamFailoverError{
		StatusCode:       http.StatusServiceUnavailable,
		Reason:           service.OpenAITurnStateHoldReason,
		ClientStatusCode: http.StatusServiceUnavailable,
		ClientMessage:    message,
	}, false)
	require.Equal(t, http.StatusServiceUnavailable, bridge.Code)
	require.Contains(t, bridge.Body.String(), message)
}
