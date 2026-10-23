//go:build unit

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestCPRDelegationIgnoresSub2RequestPolicies(t *testing.T) {
	seen := &rawRelaySeen{}
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		seen.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp_cpr","model":"gpt-5.5","service_tier":"default","usage":{"input_tokens":2,"output_tokens":1}}`)
	})
	body := []byte(`{ "model":"gpt-5.5","reasoning":{"effort":"xhigh"},"service_tier":"priority","input":"hi" }`)
	ctx := WithOpenAIForwardModel(context.Background(), "mapped-by-sub2", false)
	c, _, _ := rawRelayTestContext(t, ctx, "/v1/responses", body, "", &Group{
		MaxReasoningEffort: "low", MaxReasoningEffortOverLimit: "downgrade",
	})

	result, err := svc.Forward(ctx, c, account, body)

	require.NoError(t, err)
	require.Equal(t, body, seen.rawBody, "CPR owns model, effort and tier transformations")
	require.NotNil(t, result)
	require.Equal(t, "gpt-5.5", result.Model)
}

func TestCPRDelegationEndpointsKeepWireBodyAndQuery(t *testing.T) {
	for _, endpoint := range []string{"/v1/responses", "/v1/responses/compact", "/backend-api/codex/responses/input_tokens", "/v1/chat/completions", "/v1/messages", "/v1/messages/count_tokens", "/v1/images/generations", "/v1/alpha/search"} {
		t.Run(endpoint, func(t *testing.T) {
			seen := &rawRelaySeen{}
			seenQuery := ""
			svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
				seen.record(r)
				seenQuery = r.URL.RawQuery
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"resp_cpr","data":[{"b64_json":"abc"}],"usage":{"input_tokens":2,"output_tokens":1}}`)
			})
			plain := []byte(`{ "model":"gpt-image-1","prompt":"cat","input":"hi", "messages":[] }`)
			enc, err := zstd.NewWriter(nil)
			require.NoError(t, err)
			wire := enc.EncodeAll(plain, nil)
			enc.Close()
			c, rec, body := rawRelayTestContext(t, context.Background(), endpoint+"?a=1&a=2", wire, "zstd", &Group{MaxReasoningEffort: "low"})
			var result *OpenAIForwardResult
			switch endpoint {
			case "/v1/chat/completions":
				result, err = svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "mapped")
			case "/v1/messages":
				result, err = svc.ForwardAsAnthropic(context.Background(), c, account, body, "", "mapped")
			case "/v1/messages/count_tokens":
				err = svc.ForwardCountTokensAsAnthropic(context.Background(), c, account, body, "mapped")
			case "/backend-api/codex/responses/input_tokens":
				err = svc.ForwardResponsesInputTokens(context.Background(), c, account, body)
			case "/v1/images/generations":
				parsed, parseErr := svc.ParseOpenAIImagesRequest(c, body)
				require.NoError(t, parseErr)
				result, err = svc.ForwardImages(context.Background(), c, account, body, parsed, "mapped")
				require.NotNil(t, result)
				require.Equal(t, 1, result.ImageCount)
			case "/v1/alpha/search":
				result, err = svc.ForwardAlphaSearch(context.Background(), c, account, body)
			default:
				result, err = svc.Forward(context.Background(), c, account, body)
			}
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, wire, seen.rawBody)
			require.Equal(t, "zstd", seen.header.Get("Content-Encoding"))
			require.Equal(t, "a=1&a=2", seenQuery)
			want := endpoint
			if endpoint == "/backend-api/codex/responses/input_tokens" {
				want = "/v1/responses/input_tokens"
			}
			require.Equal(t, want, seen.path)
		})
	}
}

func TestCPRDelegationMultipartEditsStayMultipart(t *testing.T) {
	var wire bytes.Buffer
	writer := multipart.NewWriter(&wire)
	require.NoError(t, writer.WriteField("model", "gpt-image-1"))
	require.NoError(t, writer.WriteField("prompt", "draw"))
	part, err := writer.CreateFormFile("image", "a.png")
	require.NoError(t, err)
	_, err = part.Write([]byte("fake-image-bytes"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	seen := &rawRelaySeen{}
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		seen.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[{"b64_json":"abc"}]}`)
	})
	c, _, body := rawRelayTestContext(t, context.Background(), "/v1/images/edits", wire.Bytes(), "", nil)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	result, err := svc.ForwardImages(context.Background(), c, account, body, parsed, "mapped")
	require.NoError(t, err)
	require.Equal(t, wire.Bytes(), seen.rawBody)
	require.Equal(t, writer.FormDataContentType(), seen.header.Get("Content-Type"))
	require.Equal(t, "gpt-image-1", result.Model)
	require.Equal(t, 1, result.ImageCount)
}

func TestCPRDelegationConnectionRefusedDoesNotFailOver(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	account := newCPRTestAccount()
	account.Credentials["base_url"] = server.URL
	svc := &OpenAIGatewayService{cfg: cprTestConfig(), httpUpstream: rawRelayTransport{}}
	c, rec, body := rawRelayTestContext(t, context.Background(), "/v1/responses", []byte(`{"model":"gpt-5.5"}`), "", nil)
	_, err := svc.Forward(context.Background(), c, account, body)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault)
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover))
	require.Equal(t, http.StatusBadGateway, rec.Code)
}

func TestCPRDelegationTargetCannotReachAdmin(t *testing.T) {
	for _, path := range []string{"/api/admin/accounts", "/v1/admin/accounts", "/v1/responses/../admin/accounts"} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, path, nil)
		_, err := cprRelayTargetURL(cprTestGatewayBase, c)
		require.Error(t, err, path)
	}
}

func TestCPRDelegationModelManifestAndBillingUseUpstream(t *testing.T) {
	account := newCPRTestAccount()
	body := []byte(`{"models":[{"slug":"gpt-6-sol","use_responses_lite":true}]}`)
	got, err := adjustAPIKeyCodexModelsManifest(body, account)
	require.NoError(t, err)
	require.Equal(t, body, got)
	for _, tc := range []struct{ observed, bill string }{{"default", "default"}, {"priority", "priority"}, {"", ""}, {"unknown", ""}} {
		requested := "priority"
		result := &OpenAIForwardResult{ServiceTier: &requested, UpstreamResponseServiceTier: tc.observed}
		ApplyOpenAIServiceTierBillingResolution(account, result)
		require.Equal(t, tc.bill, *result.ServiceTier)
	}
}

func TestCPRDelegationDoesNotFailOverOnUpstreamErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			const body = `{"error":{"code":"no_available_provider","message":"CPR owns retries"}}`
			svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "7")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, body)
			})
			c, rec, request := rawRelayTestContext(t, context.Background(), "/v1/responses", []byte(`{"model":"gpt-5.5"}`), "", nil)

			_, err := svc.Forward(context.Background(), c, account, request)

			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Equal(t, status, rec.Code)
			require.Equal(t, body, rec.Body.String())
			require.Equal(t, "7", rec.Header().Get("Retry-After"))
		})
	}
}
