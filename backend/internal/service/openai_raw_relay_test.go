//go:build unit

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// rawRelayTransport 走真实 net/http 传输，fake CPR 是本机 httptest，不出网。
type rawRelayTransport struct{}

func (rawRelayTransport) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return http.DefaultTransport.RoundTrip(req)
}

func (u rawRelayTransport) DoWithTLS(req *http.Request, proxyURL string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, concurrency)
}

type rawRelaySeen struct {
	mu      sync.Mutex
	path    string
	header  http.Header
	rawBody []byte
}

func (s *rawRelaySeen) record(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.path, s.header, s.rawBody = r.URL.Path, r.Header.Clone(), body
}

func rawRelayTestSetup(t *testing.T, handler http.HandlerFunc) (*OpenAIGatewayService, *Account) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	account := newCPRTestAccount()
	account.Credentials["base_url"] = server.URL
	return &OpenAIGatewayService{cfg: cprTestConfig(), httpUpstream: rawRelayTransport{}}, account
}

// rawRelayTestContext 模拟 handler：入站体经 httputil 读取（留住线上原文）。
func rawRelayTestContext(t *testing.T, ctx context.Context, target string, wire []byte, encoding string, group *Group) (*gin.Context, *httptest.ResponseRecorder, []byte) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(wire)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer sk-sub2api-user-key")
	req.Header.Set("Cookie", "session=admin")
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "codex_cli_rs/0.160.0")
	req.Header.Set("x-codex-turn-state", "ts-inbound")
	req.Header.Set("session_id", "sess-1")
	req.Header.Set("x-openai-subagent", "review")
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	c.Request = req
	c.Set("api_key", &APIKey{ID: 77, Group: group})
	decoded, err := httputil.ReadRequestBodyWithPrealloc(c.Request)
	require.NoError(t, err)
	return c, rec, decoded
}

const rawRelaySSE = "event: response.created\n" +
	`data: {"type":"response.created","response":{"id":"resp_raw1","model":"gpt-5.5"}}` + "\n\n" +
	"event: response.output_text.delta\n" +
	`data: {"type":"response.output_text.delta","delta":"hi"}` + "\n\n" +
	"event: response.completed\n" +
	`data: {"type":"response.completed","response":{"id":"resp_raw1","model":"gpt-5.5","usage":{"input_tokens":11,"output_tokens":7,"input_tokens_details":{"cached_tokens":3}}}}` + "\n\n"

func TestOpenAIRawRelayForwardsWireBytesAndHeadersVerbatim(t *testing.T) {
	seen := &rawRelaySeen{}
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		seen.record(r)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("x-codex-turn-state", "ts-minted")
		w.Header().Set("x-request-id", "req-upstream")
		w.Header().Set("x-cpr-custom", "kept")
		_, _ = io.WriteString(w, rawRelaySSE)
	})
	plain := []byte(`{"model":"gpt-5.5","stream":true,"input":"hi","client_metadata":{"x":"y"}}`)
	enc, _ := zstd.NewWriter(nil)
	wire := enc.EncodeAll(plain, nil)
	_ = enc.Close()
	// 分组形如 31.108 的 group 8：没配推理强度策略，不得触发改写。
	group := &Group{MaxReasoningEffortOverLimit: "downgrade", ReasoningEffortMappings: []ReasoningEffortMapping{}}
	c, rec, decoded := rawRelayTestContext(t, context.Background(), "/v1/responses", wire, "zstd", group)

	// handler 的兼容改写不得出站：Forward 收到的是改过的体，上游必须拿到原文。
	rewritten := []byte(`{"model":"gpt-5.5","stream":true,"input":"rewritten"}`)
	require.Equal(t, plain, decoded)
	result, err := svc.Forward(context.Background(), c, account, rewritten)
	require.NoError(t, err)

	require.Equal(t, "/v1/responses", seen.path)
	require.Equal(t, wire, seen.rawBody, "未命中策略时必须发线上原文（含压缩）")
	require.Equal(t, "zstd", seen.header.Get("Content-Encoding"))
	require.Equal(t, "Bearer "+cprTestClientKey, seen.header.Get("Authorization"), "只换鉴权")
	require.Empty(t, seen.header.Get("Cookie"))
	require.Empty(t, seen.header.Get("X-Forwarded-For"))
	for _, key := range []string{"User-Agent", "x-codex-turn-state", "session_id", "x-openai-subagent", "Content-Type"} {
		require.Equal(t, c.Request.Header.Get(key), seen.header.Get(key), key)
	}

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, rawRelaySSE, rec.Body.String(), "SSE 逐字节原样")
	require.Equal(t, "kept", rec.Header().Get("x-cpr-custom"))
	require.Equal(t, "ts-minted", rec.Header().Get("x-codex-turn-state"))

	require.Equal(t, "resp_raw1", result.ResponseID)
	require.Equal(t, 11, result.Usage.InputTokens)
	require.Equal(t, 7, result.Usage.OutputTokens)
	require.NotNil(t, result.FirstTokenMs)
	require.True(t, result.Stream)
	require.Equal(t, "ts-inbound", OpenAITurnStateUsageSent(c), "用量表出站列 = 实际出站值")
	require.Equal(t, "ts-minted", *usageCodexTurnStatePtr(result.UpstreamHeaders))
}

func TestOpenAIRawRelayPatchesOnlyPolicyFields(t *testing.T) {
	seen := &rawRelaySeen{}
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		seen.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp_json","model":"gpt-5.5-mapped","usage":{"input_tokens":2,"output_tokens":1}}`)
	})
	plain := []byte(`{"model":"gpt-5.5","reasoning":{"effort":"xhigh","summary":"auto"},"input":"hi","z":[1,2]}`)
	enc, _ := zstd.NewWriter(nil)
	wire := enc.EncodeAll(plain, nil)
	_ = enc.Close()
	group := &Group{MaxReasoningEffort: "low", MaxReasoningEffortOverLimit: "downgrade"}
	ctx := WithOpenAIForwardModel(context.Background(), "gpt-5.5-mapped", false)
	c, rec, _ := rawRelayTestContext(t, ctx, "/v1/responses", wire, "zstd", group)

	result, err := svc.Forward(ctx, c, account, plain)
	require.NoError(t, err)

	require.Empty(t, seen.header.Get("Content-Encoding"), "改过的体按明文发")
	want := `{"model":"gpt-5.5-mapped","reasoning":{"effort":"low","summary":"auto"},"input":"hi","z":[1,2]}`
	require.JSONEq(t, want, string(seen.rawBody))
	require.Equal(t, `{"effort":"low","summary":"auto"}`, gjson.GetBytes(seen.rawBody, "reasoning").Raw, "其余字节不动")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "resp_json", result.ResponseID)
	require.Equal(t, 2, result.Usage.InputTokens)
}

func TestOpenAIRawRelayPassesOrdinaryErrorsVerbatim(t *testing.T) {
	const errBody = "{\n  \"error\": {\"message\": \"bad input\", \"type\": \"invalid_request_error\", \"code\": \"x\"}\n}"
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("x-gateway-request-id", "gw-1")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, errBody)
	})
	c, rec, body := rawRelayTestContext(t, context.Background(), "/v1/responses", []byte(`{"model":"gpt-5.5"}`), "", nil)

	_, err := svc.Forward(context.Background(), c, account, body)
	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr), "普通错误不换号")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, errBody, rec.Body.String())
	require.Equal(t, "gw-1", rec.Header().Get("x-gateway-request-id"))
}

func TestOpenAIRawRelayUnavailableFailsOverThenWritesVerbatim(t *testing.T) {
	const errBody = `{"error":{"code":"no_available_provider","message":"no provider"}}`
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, errBody)
	})
	c, rec, body := rawRelayTestContext(t, context.Background(), "/v1/responses", []byte(`{"model":"gpt-5.5"}`), "", nil)

	_, err := svc.Forward(context.Background(), c, account, body)
	var failoverErr *UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr))
	require.True(t, failoverErr.RawRelayResponse)
	require.False(t, c.Writer.Written(), "换号前不得写客户端")

	WriteOpenAIRawRelayUpstreamResponse(c, failoverErr.StatusCode, failoverErr.ResponseHeaders, failoverErr.ResponseBody)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, errBody, rec.Body.String())
	require.Equal(t, "7", rec.Header().Get("Retry-After"))
}

func TestOpenAIRawRelayConnectionRefusedFailsOver(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	base := server.URL
	server.Close()
	account := newCPRTestAccount()
	account.Credentials["base_url"] = base
	svc := &OpenAIGatewayService{cfg: cprTestConfig(), httpUpstream: rawRelayTransport{}}
	c, _, body := rawRelayTestContext(t, context.Background(), "/v1/responses", []byte(`{"model":"gpt-5.5"}`), "", nil)

	_, err := svc.Forward(context.Background(), c, account, body)
	var failoverErr *UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr))
	require.False(t, failoverErr.RawRelayResponse, "没有上游响应可原样返回")
	require.False(t, c.Writer.Written())
}

func TestOpenAIRawRelayClientDisconnectStopsUpstream(t *testing.T) {
	upstreamGone := make(chan struct{})
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.created\n"+`data: {"type":"response.created","response":{"id":"resp_cut"}}`+"\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(upstreamGone)
	})
	ctx, cancel := context.WithCancel(context.Background())
	c, _, body := rawRelayTestContext(t, ctx, "/v1/responses", []byte(`{"model":"gpt-5.5","stream":true}`), "", nil)

	done := make(chan struct{})
	var result *OpenAIForwardResult
	var err error
	go func() {
		result, err = svc.Forward(ctx, c, account, body)
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case <-upstreamGone:
	case <-time.After(3 * time.Second):
		t.Fatal("客户端断开后上游连接没有立即断开")
	}
	<-done
	require.Error(t, err)
	require.NotNil(t, result)
	require.True(t, result.ClientDisconnect)
	require.Equal(t, "resp_cut", result.ResponseID)
}

func TestOpenAIRawRelayKeepsOriginalPathWhenHandlerRewroteIt(t *testing.T) {
	seen := &rawRelaySeen{}
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		seen.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	c, _, body := rawRelayTestContext(t, context.Background(), "/v1/responses", []byte(`{"model":"gpt-5.5"}`), "", nil)
	c.Request.URL.Path += "/compact" // handler 对 body-signal compact 的改写
	_, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.Equal(t, "/v1/responses", seen.path)

	c, _, body = rawRelayTestContext(t, context.Background(), "/v1/responses/compact", []byte(`{"model":"gpt-5.5"}`), "", nil)
	_, err = svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.Equal(t, "/v1/responses/compact", seen.path)
}

func TestIsOpenAIRawRelayUnavailable(t *testing.T) {
	withGateway := http.Header{"X-Gateway-Request-Id": []string{"gw"}}
	cases := []struct {
		name   string
		status int
		header http.Header
		body   string
		want   bool
	}{
		{"cpr 自己拒 key", 401, nil, `{"error":{"code":"invalid_api_key"}}`, true},
		{"上游 401 经 CPR 转回", 401, withGateway, `{"error":{"code":"invalid_api_key"}}`, false},
		{"cpr 并发排队满", 429, nil, `{"error":{"code":"concurrency_queue_full"}}`, true},
		{"cpr 额度用完", 429, withGateway, `{"error":{"code":"usage_limit_reached"}}`, true},
		{"cpr 无可用账号", 503, nil, `{"error":{"code":"no_available_provider"}}`, true},
		{"上游 503 过载", 503, withGateway, `{"error":{"code":"server_is_overloaded"}}`, false},
		{"sub2api key 失效", 401, nil, `{"code":"INVALID_API_KEY","message":"Invalid API key"}`, true},
		{"sub2api 余额不足", 403, nil, `{"code":"INSUFFICIENT_BALANCE","message":"x"}`, true},
		{"sub2api 无可用账号", 503, nil, `{"error":{"type":"api_error","message":"Service temporarily unavailable"}}`, true},
		{"反代后端挂了", 502, nil, `<html>bad gateway</html>`, true},
		{"上游 502 JSON", 502, nil, `{"error":{"message":"upstream"}}`, false},
		{"请求错误", 400, nil, `{"error":{"type":"invalid_request_error"}}`, false},
		{"上游 500", 500, nil, `{"error":{"type":"server_error"}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isOpenAIRawRelayUnavailable(tc.status, tc.header, []byte(tc.body)))
		})
	}
}

func TestUsesOpenAIRawRelay(t *testing.T) {
	require.True(t, newCPRTestAccount().UsesOpenAIRawRelay(), "cpr 恒走原样中继")
	apikey := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{}}
	require.False(t, apikey.UsesOpenAIRawRelay())
	apikey.Extra[openAIRawRelayExtraKey] = true
	require.True(t, apikey.UsesOpenAIRawRelay())
	oauth := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{openAIRawRelayExtraKey: true}}
	require.False(t, oauth.UsesOpenAIRawRelay(), "开关只对 apikey 生效")
}

func TestWriteOpenAIRawRelayUpstreamResponseAfterCommit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	_, _ = c.Writer.Write([]byte(": ping\n\n"))

	WriteOpenAIRawRelayUpstreamResponse(c, http.StatusTooManyRequests, nil, []byte("{\n\"error\":{\"code\":\"usage_limit_reached\"}}"))
	require.Equal(t, http.StatusOK, rec.Code, "已提交的响应改不了状态码")
	require.True(t, strings.HasSuffix(rec.Body.String(), "event: error\ndata: {\"error\":{\"code\":\"usage_limit_reached\"}}\n\n"))
}

func TestOpenAIRawRelayRejectsDuplicateKeys(t *testing.T) {
	var hits int
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) { hits++ })
	body := []byte(`{"model":"gpt-5.5","reasoning":{"effort":"low","effort":"xhigh"},"input":"hi"}`)
	c, rec, decoded := rawRelayTestContext(t, context.Background(), "/v1/responses", body, "", &Group{MaxReasoningEffort: "low", MaxReasoningEffortOverLimit: "downgrade"})

	_, err := svc.Forward(context.Background(), c, account, decoded)
	require.ErrorIs(t, err, errOpenAIRawRelayDuplicateKeys)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault, "用户的坏请求不算账号失败")
	require.True(t, IsResponseCommitted(c), "已提交：handler 不再追加兜底响应")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid_request_error", gjson.Get(rec.Body.String(), "error.type").String())
	require.Zero(t, hits, "带重复键的请求不出站：上游按末个键生效，本站的策略只看得到首个")

	// compact 排队心跳已把 200 提交出去：拒绝改成一条 SSE 终态事件。
	c, rec, decoded = rawRelayTestContext(t, context.Background(), "/v1/responses/compact", body, "", nil)
	MarkOpenAICompactClientStream(c)
	stop := StartOpenAICompactSSEKeepalive(c, time.Millisecond)
	defer stop()
	require.Eventually(t, c.Writer.Written, time.Second, time.Millisecond)
	_, err = svc.Forward(context.Background(), c, account, decoded)
	require.ErrorIs(t, err, ErrOpenAIRawRelayNotAccountFault)
	require.True(t, IsResponseCommitted(c))
	require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.failed"`), rec.Body.String())
	require.Contains(t, rec.Body.String(), "invalid_request_error")
}

func TestJSONHasDuplicateObjectKeys(t *testing.T) {
	for _, tc := range []struct {
		in  string
		dup bool
	}{
		{`{"a":1,"b":{"a":2},"c":[{"a":3},{"a":4}]}`, false},
		{`{"a":1,"a":2}`, true},
		{`{"type":"session.update","type":"response.create"}`, true},
		{`{"type":"x","type":"response.create"}`, true},
		{`{"reasoning":{"effort":"low","effort":"xhigh"}}`, true},
		{`{"input":[{"content":[{"text":"a","text":"b"}]}]}`, true},
		{`{"huge":1e400,"model":"a","model":"b"}`, true},
		{`{"s":"{\"x\":1,\"x\":2}","t":"[}{]"}`, false},
		{`[{"k":1},{"k":1,"k":2}]`, true},
		{`{"a":`, false},
	} {
		require.Equal(t, tc.dup, jsonHasDuplicateObjectKeys([]byte(tc.in)), tc.in)
	}
}
