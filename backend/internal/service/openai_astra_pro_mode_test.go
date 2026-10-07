//go:build unit

package service

// Integration-level tests for GPT-6 Astra reasoning.mode handling through
// OpenAIGatewayService.Forward: the request aligns with the real Codex client,
// which never sends reasoning.mode; the response side keeps it verbatim.
//
// These exercise the real forward pipeline through the httpUpstreamRecorder mock
// (no real network / credentials / config), covering the OpenAI OAuth account
// (native Codex transform when passthrough is disabled, and the passthrough
// branch when enabled). Client stream=true and stream=false both receive an
// upstream SSE fixture, because Codex upstreams stream: the stream=false client
// path exercises the real SSE->JSON aggregation instead of a synthetic JSON body.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const astraProCodexResponsesURL = "https://chatgpt.com/backend-api/codex/responses"

type astraForwardSetup struct {
	upstream *httpUpstreamRecorder
	svc      *OpenAIGatewayService
	c        *gin.Context
	rec      *httptest.ResponseRecorder
	account  *Account
}

// newAstraOAuthSetup builds an OpenAI OAuth account using the minimal
// svc+account harness pattern (mirrors openai_oauth_passthrough_test.go). When
// passthrough is true the account routes through forwardOpenAIPassthrough.
func newAstraOAuthSetup(t *testing.T, passthrough bool) *astraForwardSetup {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "codex_cli_rs/0.144.1")
	upstream := &httpUpstreamRecorder{}
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{Gateway: config.GatewayConfig{ForceCodexCLI: false}},
		httpUpstream: upstream,
	}
	account := &Account{
		ID:             888,
		Name:           "oauth-astra",
		Platform:       PlatformOpenAI,
		Type:           AccountTypeOAuth,
		Concurrency:    1,
		Credentials:    map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-acc"},
		Extra:          map[string]any{"openai_passthrough": passthrough},
		Status:         StatusActive,
		Schedulable:    true,
		RateMultiplier: f64p(1),
	}
	return &astraForwardSetup{upstream: upstream, svc: svc, c: c, rec: rec, account: account}
}

func codexCompletedSSE(inner string) string {
	return "data: {\"type\":\"response.completed\",\"response\":" + inner + "}\n\ndata: [DONE]\n\n"
}

func astraRequestBody(model string, stream bool, mode, effort string) []byte {
	var reasoningParts []string
	if mode != "" {
		reasoningParts = append(reasoningParts, `"mode":"`+mode+`"`)
	}
	if effort != "" {
		reasoningParts = append(reasoningParts, `"effort":"`+effort+`"`)
	}
	reasoning := ""
	if len(reasoningParts) > 0 {
		reasoning = `,"reasoning":{` + strings.Join(reasoningParts, ",") + `}`
	}
	streamJSON := "false"
	if stream {
		streamJSON = "true"
	}
	return []byte(`{"model":"` + model + `","stream":` + streamJSON + `,` +
		`"instructions":"test","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]` +
		reasoning + `}`)
}

// TestForward_AstraOAuth_NonAstraLegacyStillStrips asserts the historical strip
// behavior is preserved for a non-Astra model on an OAuth account.
func TestForward_AstraOAuth_NonAstraLegacyStillStrips(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := newAstraOAuthSetup(t, false)
	inner := `{"id":"resp_test","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`
	s.upstream.resp = &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(codexCompletedSSE(inner))),
	}
	body := astraRequestBody("gpt-5.6-sol", true, "pro", "")

	result, err := s.svc.Forward(context.Background(), s.c, s.account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, s.upstream.lastReq)
	require.Equal(t, astraProCodexResponsesURL, s.upstream.lastReq.URL.String())

	forwarded := s.upstream.lastBody
	require.False(t, gjson.GetBytes(forwarded, "reasoning.mode").Exists(),
		"non-Astra legacy must strip reasoning.mode")
	require.Equal(t, "max", gjson.GetBytes(forwarded, "reasoning.effort").String(),
		"non-Astra legacy mode=pro without effort injects max")
}

// TestForward_AstraOAuth_ModeMatrix_AlignedWithCodex runs the pro/standard/missing-mode
// x effort matrix across both forward branches and both client stream modes.
// Every attempt receives an upstream SSE fixture; assertions check the final
// upstream request drops mode (mode=pro without effort becomes effort=max), keeps
// effort, and the URL is the Codex responses endpoint.
func TestForward_AstraOAuth_ModeMatrix_AlignedWithCodex(t *testing.T) {
	cases := []struct {
		name    string
		mode    string
		effort  string
		wantEff string
	}{
		{name: "pro+max", mode: "pro", effort: "max", wantEff: "max"},
		{name: "standard+max", mode: "standard", effort: "max", wantEff: "max"},
		{name: "missing mode + max", mode: "", effort: "max", wantEff: "max"},
		{name: "pro+high", mode: "pro", effort: "high", wantEff: "high"},
		{name: "pro no effort", mode: "pro", effort: "", wantEff: "max"},
	}
	for _, passthrough := range []bool{false, true} {
		branch := "native-codex"
		if passthrough {
			branch = "passthrough"
		}
		for _, stream := range []bool{true, false} {
			for _, tt := range cases {
				t.Run(tt.name+"/"+branch+"/stream="+map[bool]string{true: "true", false: "false"}[stream], func(t *testing.T) {
					s := newAstraOAuthSetup(t, passthrough)
					inner := `{"id":"resp_test","model":"gpt-6-astra","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`
					s.upstream.resp = &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
						Body:       io.NopCloser(strings.NewReader(codexCompletedSSE(inner))),
					}
					body := astraRequestBody("gpt-6-astra", stream, tt.mode, tt.effort)

					result, err := s.svc.Forward(context.Background(), s.c, s.account, body)
					require.NoError(t, err)
					require.NotNil(t, result)
					require.NotNil(t, s.upstream.lastReq)
					require.Equal(t, astraProCodexResponsesURL, s.upstream.lastReq.URL.String(),
						"OAuth account must hit the Codex responses endpoint")

					forwarded := s.upstream.lastBody
					require.Equal(t, "gpt-6-astra", gjson.GetBytes(forwarded, "model").String())

					require.False(t, gjson.GetBytes(forwarded, "reasoning.mode").Exists(), "mode must be dropped for %q", tt.name)
					require.Equal(t, tt.wantEff, gjson.GetBytes(forwarded, "reasoning.effort").String(), "effort for %q", tt.name)
				})
			}
		}
	}
}

// TestForward_AstraOAuth_NonStreamAggregation_PreservesResponseReasoningMode
// drives client stream=false (which is forced to an SSE upstream) and asserts the
// aggregated JSON response object still carries response.reasoning.mode after
// SSE->JSON aggregation.
func TestForward_AstraOAuth_NonStreamAggregation_PreservesResponseReasoningMode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := newAstraOAuthSetup(t, false)
	// response.completed.response carries a top-level reasoning:{mode:pro,effort:max}.
	inner := `{"id":"resp_astra","object":"response","created_at":0,"status":"completed","model":"gpt-6-astra","reasoning":{"mode":"pro","effort":"max"},"output":[{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"think"}]},{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"hi"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`
	s.upstream.resp = &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(codexCompletedSSE(inner))),
	}
	body := astraRequestBody("gpt-6-astra", false, "pro", "max")

	result, err := s.svc.Forward(context.Background(), s.c, s.account, body)
	require.NoError(t, err)
	require.NotNil(t, result)

	downstream := s.rec.Body.String()
	// Downstream is JSON (SSE->JSON aggregation for the non-streaming client).
	require.True(t, gjson.Valid(downstream), "downstream must be aggregated JSON, got: %s", downstream)
	require.Equal(t, "pro", gjson.Get(downstream, "reasoning.mode").String(),
		"aggregated response object must keep reasoning.mode")
	require.Equal(t, "max", gjson.Get(downstream, "reasoning.effort").String())
}

// TestForward_AstraOAuth_Stream_PreservesResponseReasoningMode drives client
// stream=true and asserts the streamed response.completed event's response object
// still carries response.reasoning.mode.
func TestForward_AstraOAuth_Stream_PreservesResponseReasoningMode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := newAstraOAuthSetup(t, false)
	inner := `{"id":"resp_astra","object":"response","created_at":0,"status":"completed","model":"gpt-6-astra","reasoning":{"mode":"pro","effort":"max"},"output":[{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"hi"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`
	s.upstream.resp = &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(codexCompletedSSE(inner))),
	}
	body := astraRequestBody("gpt-6-astra", true, "pro", "max")

	result, err := s.svc.Forward(context.Background(), s.c, s.account, body)
	require.NoError(t, err)
	require.NotNil(t, result)

	downstream := s.rec.Body.String()
	require.Contains(t, downstream, "response.completed", "streaming downstream must include completed event")
	// Extract response.completed line and assert response.reasoning.mode inside it.
	var completedJSON string
	for _, line := range strings.Split(downstream, "\n") {
		if strings.HasPrefix(line, "data: ") && strings.Contains(line, `"type":"response.completed"`) {
			completedJSON = strings.TrimPrefix(line, "data: ")
			break
		}
	}
	require.NotEmpty(t, completedJSON, "no response.completed data line found in: %s", downstream)
	require.Equal(t, "pro", gjson.Get(completedJSON, "response.reasoning.mode").String(),
		"streamed completed response object must keep reasoning.mode")
	require.Equal(t, "max", gjson.Get(completedJSON, "response.reasoning.effort").String())
}
