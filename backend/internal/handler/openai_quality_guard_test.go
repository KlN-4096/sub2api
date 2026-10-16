package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestQualityGuardFailoverErrorsReachEveryHTTPFormat(t *testing.T) {
	gin.SetMode(gin.TestMode)
	failure := &service.UpstreamFailoverError{
		StatusCode: 503, ClientStatusCode: 503, ClientMessage: "Quality guard discarded the response",
		Reason: service.OpenAIQualityGuardReason, NextAccountAction: service.NextAccountStop,
	}
	for name, write := range map[string]func(*gin.Context, bool){
		"responses": func(c *gin.Context, stream bool) {
			(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, failure, stream)
		},
		"messages": func(c *gin.Context, stream bool) {
			(&OpenAIGatewayHandler{}).handleAnthropicFailoverExhausted(c, failure, stream)
		},
		"compat-responses": func(c *gin.Context, stream bool) {
			(&GatewayHandler{}).handleResponsesFailoverExhausted(c, failure, stream)
		},
		"compat": func(c *gin.Context, stream bool) {
			(&GatewayHandler{}).handleFailoverExhausted(c, failure, service.PlatformOpenAI, stream)
		},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/json", true: "/sse"}[stream], func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				write(c, stream)
				require.Contains(t, rec.Body.String(), failure.ClientMessage)
				if !stream {
					require.Equal(t, 503, rec.Code)
				}
			})
		}
	}
}
