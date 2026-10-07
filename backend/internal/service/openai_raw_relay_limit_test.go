package service

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRawRelayUsesConfiguredBodyLimit(t *testing.T) {
	body := append([]byte(`{"input":"`), bytes.Repeat([]byte("x"), 64<<20)...)
	body = append(body, []byte(`"}`)...)
	cfg := &config.Config{}
	cfg.Gateway.MaxBodySize = 80 << 20
	svc := &OpenAIGatewayService{cfg: cfg}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/responses", bytes.NewReader(body))
	out, _, _, err := svc.openAIRawRelayOutboundBody(context.Background(), c, account, body)
	require.NoError(t, err, "a body within the gateway limit must not hit a hidden second 64MiB limit")
	require.Equal(t, len(body), len(out))
}

func TestOpenAIRawRelayBodyLimitIsLocal413NotAccountFault(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.MaxBodySize = 128
	svc := &OpenAIGatewayService{cfg: cfg}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	body := append([]byte(`{"input":"`), bytes.Repeat([]byte("x"), 256)...)
	body = append(body, []byte(`"}`)...)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/responses", bytes.NewReader(body))
	result, err := svc.forwardOpenAIRawRelay(context.Background(), c, account, body)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault)
	var tooLarge *http.MaxBytesError
	require.True(t, errors.As(err, &tooLarge))
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.Contains(t, rec.Body.String(), "request_too_large")
}
