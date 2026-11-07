package service

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
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

func TestOpenAIRawRelayReusesIngressNormalizedBody(t *testing.T) {
	body := []byte("{\"model\":\"gpt-6.1-sol\",\"input\":\"a\x00b\"}")
	ctx, release := httputil.WithRequestBodyMemoryBudget(context.Background(), httputil.NewBodyMemoryBudget(4096))
	defer release()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/responses", bytes.NewReader(body)).WithContext(ctx)
	normalized, err := httputil.ReadLenientJSONRequestBodyWithPrealloc(c.Request, 0)
	require.NoError(t, err)

	svc := &OpenAIGatewayService{}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	out, plain, _, err := svc.openAIRawRelayOutboundBody(ctx, c, account, normalized)
	require.NoError(t, err)
	require.Equal(t, body, out, "unmodified relay still sends original wire bytes")
	require.Equal(t, normalized, plain)
	require.Same(t, &normalized[0], &plain[0], "do not allocate the normalized body twice")
}

func TestOpenAIRawRelayChargesFallbackNormalization(t *testing.T) {
	body := []byte("{\"model\":\"gpt-6.1-sol\",\"input\":\"a\x00b\"}")
	ctx, release := httputil.WithRequestBodyMemoryBudget(context.Background(), httputil.NewBodyMemoryBudget(1))
	defer release()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/responses", bytes.NewReader(body)).WithContext(ctx)
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	_, _, _, err := svc.openAIRawRelayOutboundBody(ctx, c, account, body)
	require.ErrorIs(t, err, httputil.ErrRequestBodyMemoryBudget)
}
