package service

import (
	"context"
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

// RecordUsage 把上游响应头里的读数写进 UsageLog；删掉 openai_gateway_usage.go 里那两行赋值，本用例失败。
func TestOpenAIGatewayServiceRecordUsage_PersistsSafetyBufferingHeaders(t *testing.T) {
	record := func(t *testing.T, upstream http.Header) *UsageLog {
		t.Helper()
		usageRepo := &openAIRecordUsageLogRepoStub{inserted: true}
		billingRepo := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
		svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(usageRepo, billingRepo, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{}, nil)
		err := svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{
			Result: &OpenAIForwardResult{
				RequestID:       "resp_safety_buffering",
				Usage:           OpenAIUsage{},
				Model:           "gpt-6-astra",
				Duration:        time.Second,
				UpstreamHeaders: upstream,
			},
			APIKey:        &APIKey{ID: 1000, Quota: 100, Group: &Group{RateMultiplier: 1}},
			User:          &User{ID: 2000},
			Account:       &Account{ID: 3000, Type: AccountTypeAPIKey},
			APIKeyService: &openAIRecordUsageAPIKeyQuotaStub{},
		})
		require.NoError(t, err)
		require.NotNil(t, usageRepo.lastLog)
		return usageRepo.lastLog
	}

	upstream := http.Header{}
	upstream.Set("X-Codex-Safety-Buffering-Enabled", "true")
	upstream.Set("X-Codex-Safety-Buffering-Faster-Model", "gpt-5.6-luna")
	withHeaders := record(t, upstream)
	require.NotNil(t, withHeaders.SafetyBufferingEnabled)
	require.True(t, *withHeaders.SafetyBufferingEnabled)
	require.NotNil(t, withHeaders.SafetyBufferingFasterModel)
	require.Equal(t, "gpt-5.6-luna", *withHeaders.SafetyBufferingFasterModel)

	absent := record(t, http.Header{"Content-Type": []string{"application/json"}})
	require.Nil(t, absent.SafetyBufferingEnabled, "上游没带头时列保持 NULL")
	require.Nil(t, absent.SafetyBufferingFasterModel)
}

// 三条 HTTP 响应路径（SSE 流式、JSON 非流式、上游 SSE→JSON）都要把 safety-buffering 头交给
// 客户端：删掉任一处 stage/relay 调用，对应子用例失败。
func TestOpenAIResponseHandlers_RelaySafetyBufferingHeadersToClient(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{Gateway: config.GatewayConfig{OpenAIFirstOutputTimeoutSeconds: 1, MaxLineSize: defaultMaxLineSize}}
	account := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	upstreamHeaders := func(contentType string) http.Header {
		h := http.Header{"Content-Type": []string{contentType}}
		h.Set("X-Codex-Safety-Buffering-Enabled", "true")
		h.Set("X-Codex-Safety-Buffering-Faster-Model", "gpt-5.6-luna")
		return h
	}
	sseBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_sb"}}`, "",
		`data: {"type":"response.output_text.delta","delta":"hello"}`, "",
		`data: {"type":"response.completed","response":{"id":"resp_sb","usage":{"input_tokens":1,"output_tokens":1}}}`, "", "",
	}, "\n")
	const jsonBody = `{"id":"resp_sb","object":"response","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`

	newService := func() *OpenAIGatewayService {
		return &OpenAIGatewayService{cfg: cfg, responseHeaderFilter: compileResponseHeaderFilter(cfg)}
	}
	newContext := func() (*httptest.ResponseRecorder, *gin.Context) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-6-astra"}`))
		return rec, c
	}
	requireRelayed := func(t *testing.T, rec *httptest.ResponseRecorder) {
		t.Helper()
		require.Equal(t, "true", rec.Result().Header.Get("X-Codex-Safety-Buffering-Enabled"))
		require.Equal(t, "gpt-5.6-luna", rec.Result().Header.Get("X-Codex-Safety-Buffering-Faster-Model"))
	}

	t.Run("streaming_committed_with_first_output", func(t *testing.T) {
		rec, c := newContext()
		resp := &http.Response{StatusCode: http.StatusOK, Header: upstreamHeaders("text/event-stream"), Body: io.NopCloser(strings.NewReader(sseBody))}
		_, err := newService().handleStreamingResponse(c.Request.Context(), resp, c, account, time.Now(), "gpt-6-astra", "gpt-6-astra")
		require.NoError(t, err)
		requireRelayed(t, rec)
	})

	t.Run("non_streaming_json", func(t *testing.T) {
		rec, c := newContext()
		resp := &http.Response{StatusCode: http.StatusOK, Header: upstreamHeaders("application/json"), Body: io.NopCloser(strings.NewReader(jsonBody))}
		_, err := newService().handleNonStreamingResponse(c.Request.Context(), resp, c, account, "gpt-6-astra", "gpt-6-astra")
		require.NoError(t, err)
		requireRelayed(t, rec)
	})

	t.Run("non_streaming_upstream_sse_to_json", func(t *testing.T) {
		rec, c := newContext()
		resp := &http.Response{StatusCode: http.StatusOK, Header: upstreamHeaders("text/event-stream"), Body: io.NopCloser(strings.NewReader(sseBody))}
		_, err := newService().handleNonStreamingResponse(c.Request.Context(), resp, c, account, "gpt-6-astra", "gpt-6-astra")
		require.NoError(t, err)
		requireRelayed(t, rec)
	})
}
