package service

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAnthropicStreamingPreludeLimitReturnsServerError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	payload, err := json.Marshal(map[string]any{
		"type": "response.created",
		"response": map[string]any{
			"id":    strings.Repeat("x", openAIFirstOutputStageMaxBytes),
			"model": "gpt-5", "status": "in_progress", "output": []any{},
		},
	})
	require.NoError(t, err)
	body := "data: " + string(payload) + "\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"done\",\"status\":\"completed\"}}\n\n"
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		MaxLineSize: 2 * openAIFirstOutputStageMaxBytes,
	}}}
	resp := &http.Response{
		StatusCode: http.StatusOK, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(body)),
	}
	result, err := svc.handleAnthropicStreamingResponse(
		resp, c, &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth},
		"gpt-5", "gpt-5", "gpt-5", time.Now(),
	)
	require.ErrorIs(t, err, errOpenAIFirstOutputStageLimit)
	require.NotNil(t, result)
	require.False(t, result.ClientDisconnect, "server staging errors are not client disconnects")
	require.False(t, c.Writer.Written())
	require.Empty(t, rec.Body.String())
}
