//go:build unit

package service

import (
	"bytes"
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
	"github.com/tidwall/gjson"
)

func TestOpenAIStreamDataStartsAnswer(t *testing.T) {
	tests := []struct {
		data, eventLine string
		want            bool
	}{
		{data: `{"type":"response.output_text.delta","delta":"hi"}`, want: true},
		{data: `{"type":"response.refusal.delta","delta":"no"}`, want: true},
		{data: `{"type":"response.function_call_arguments.delta","delta":"{"}`, want: true},
		{data: `{"type":"response.custom_tool_call_input.delta","delta":"x"}`, want: true},
		{data: `{"type":"response.mcp_call_arguments.delta","delta":"{"}`, want: true},
		{data: `{"delta":"hi"}`, eventLine: "response.output_text.delta", want: true},
		{data: `{"type":"response.output_text.delta","delta":""}`, want: false},
		{data: `{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`, want: false},
		{data: `{"type":"response.reasoning_text.delta","delta":"thinking"}`, want: false},
		{data: `{"type":"response.output_item.added","item":{"type":"message","content":[]}}`, want: false},
		{data: `{"type":"response.output_text.done","text":"hi"}`, want: false},
		{data: `{"type":"response.created","response":{"id":"resp_1"}}`, want: false},
	}
	for _, tt := range tests {
		eventType := effectiveOpenAISSEEventType([]byte(tt.data), tt.eventLine)
		require.Equal(t, tt.want, openAIStreamDataStartsAnswer(tt.data, eventType), tt.data)
	}
}

func TestOpenAIFirstAnswerObserve(t *testing.T) {
	start := time.Now().Add(-time.Second)
	feed := func(a *openAIFirstAnswer, data string) {
		a.observe(data, effectiveOpenAISSEEventType([]byte(data), ""), start)
	}

	var a openAIFirstAnswer
	feed(&a, `{"type":"response.created","response":{"id":"resp_1"},"safety_buffering":false}`)
	feed(&a, `{"type":"response.reasoning_summary_text.delta","delta":"x"}`)
	require.Nil(t, a.ms)
	feed(&a, `{"type":"response.output_text.delta","delta":"a"}`)
	require.NotNil(t, a.ms)
	first := *a.ms
	require.GreaterOrEqual(t, first, 1000)
	a.observe(`{"type":"response.output_text.delta","delta":"b"}`, "response.output_text.delta", start.Add(-time.Hour))
	require.Equal(t, first, *a.ms, "只记首个答案 delta")

	// 安全缓冲：标记在答案之前或之后出现都作废，之后不再记。
	for _, marker := range []string{
		`{"type":"response.output_text.delta","delta":"a","safety_buffering":{"use_cases":["cyber"],"reasons":["user_risk"]}}`,
		`{"type":"response.metadata","metadata":{"type":"safety_buffering","use_cases":["cyber"]}}`,
	} {
		var before, after openAIFirstAnswer
		feed(&before, marker)
		feed(&before, `{"type":"response.output_text.delta","delta":"b"}`)
		require.Nil(t, before.ms, marker)
		feed(&after, `{"type":"response.output_text.delta","delta":"a"}`)
		feed(&after, marker)
		feed(&after, `{"type":"response.output_text.delta","delta":"b"}`)
		require.Nil(t, after.ms, marker)
	}
	var other openAIFirstAnswer
	feed(&other, `{"type":"response.metadata","metadata":{"type":"turn_moderation"}}`)
	feed(&other, `{"type":"response.output_text.delta","delta":"a"}`)
	require.NotNil(t, other.ms)
}

// reasoning_tokens 上游报了数字才有值（含 0），缺失或 null 是 nil：前端据此决定能否扣掉推理部分。
func TestOpenAIUsageFromGJSONReasoningTokens(t *testing.T) {
	reasoning := func(raw string) *int {
		usage, ok := openAIUsageFromGJSON(gjson.Parse(raw))
		require.True(t, ok, raw)
		return usage.ReasoningTokens
	}
	got := reasoning(`{"input_tokens":5,"output_tokens":900,"output_tokens_details":{"reasoning_tokens":640}}`)
	require.NotNil(t, got)
	require.Equal(t, 640, *got)
	got = reasoning(`{"prompt_tokens":5,"completion_tokens":30,"completion_tokens_details":{"reasoning_tokens":12}}`)
	require.NotNil(t, got)
	require.Equal(t, 12, *got)
	got = reasoning(`{"input_tokens":5,"output_tokens":30,"output_tokens_details":{"reasoning_tokens":0}}`)
	require.NotNil(t, got)
	require.Equal(t, 0, *got)
	require.Nil(t, reasoning(`{"input_tokens":5,"output_tokens":30}`))
	require.Nil(t, reasoning(`{"input_tokens":5,"output_tokens":30,"output_tokens_details":{"reasoning_tokens":null}}`))

	dst := OpenAIUsage{OutputTokens: 30}
	mergeOpenAIUsageNonZero(&dst, OpenAIUsage{OutputTokens: 40, ReasoningTokens: got})
	require.Same(t, got, dst.ReasoningTokens)
	mergeOpenAIUsageNonZero(&dst, OpenAIUsage{OutputTokens: 50})
	require.Same(t, got, dst.ReasoningTokens, "后来的 usage 没报推理时保留已有读数")
}

const openAIAnswerTestCompleted = `{"type":"response.completed","response":{"id":"resp_answer","usage":{"input_tokens":3,"output_tokens":50,"output_tokens_details":{"reasoning_tokens":20}}}}`

// 推理事件先到，answerDelay 后到 answer；两条 HTTP 流式路径都要记下答案时刻和推理 token。
func runOpenAIAnswerTestStream(t *testing.T, passthrough bool, created string, answerDelay time.Duration, answer string) (firstTokenMs, firstAnswerMs *int, usage *OpenAIUsage) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
	reader, writer := io.Pipe()
	go func() {
		defer func() { _ = writer.Close() }()
		_, _ = io.WriteString(writer, "data: "+created+"\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"thinking\"}\n\n")
		time.Sleep(answerDelay)
		if answer != "" {
			_, _ = io.WriteString(writer, "data: "+answer+"\n\n")
		}
		_, _ = io.WriteString(writer, "data: "+openAIAnswerTestCompleted+"\n\n")
	}()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: reader}
	account := &Account{ID: 1, Name: "account_test", Platform: PlatformOpenAI}
	if passthrough {
		result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "test-model", "test-model")
		require.NoError(t, err)
		return result.firstTokenMs, result.firstAnswerMs, result.usage
	}
	result, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "test-model", "test-model")
	require.NoError(t, err)
	return result.firstTokenMs, result.firstAnswerMs, result.usage
}

func TestOpenAIStreamingRecordsFirstAnswerAfterReasoning(t *testing.T) {
	const created = `{"type":"response.created","response":{"id":"resp_answer"}}`
	const answer = `{"type":"response.output_text.delta","delta":"answer"}`
	for _, passthrough := range []bool{false, true} {
		firstToken, firstAnswer, usage := runOpenAIAnswerTestStream(t, passthrough, created, 150*time.Millisecond, answer)
		require.NotNil(t, firstToken, "passthrough=%v", passthrough)
		require.NotNil(t, firstAnswer, "passthrough=%v", passthrough)
		require.GreaterOrEqual(t, *firstAnswer, 150, "passthrough=%v", passthrough)
		require.Less(t, *firstToken, *firstAnswer, "首字落在推理事件，答案在其后")
		require.NotNil(t, usage.ReasoningTokens)
		require.Equal(t, 20, *usage.ReasoningTokens)
		require.Equal(t, 50, usage.OutputTokens)

		_, firstAnswer, _ = runOpenAIAnswerTestStream(t, passthrough, created, 0, "")
		require.Nil(t, firstAnswer, "没有答案 delta 不记，前端退回总耗时口径；passthrough=%v", passthrough)

		buffered := `{"type":"response.created","response":{"id":"resp_answer"},"safety_buffering":{"use_cases":["cyber"],"reasons":["user_risk"]}}`
		_, firstAnswer, _ = runOpenAIAnswerTestStream(t, passthrough, buffered, 0, answer)
		require.Nil(t, firstAnswer, "安全缓冲回合不记；passthrough=%v", passthrough)
	}
}

func TestOpenAIRawRelayRecordsFirstAnswer(t *testing.T) {
	svc := &OpenAIGatewayService{}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	var out openAIRawRelayOutcome
	observer, images, eventType := &upstreamResponseModelObserver{}, newOpenAIImageOutputCounter(), ""
	start := time.Now().Add(-time.Second)
	feed := func(event, data string) {
		for _, line := range []string{"event: " + event, "data: " + data, ""} {
			eventType = svc.observeOpenAIRawSSELine(c, &out, observer, images, []byte(line+"\n"), eventType, OpenAITTFTModeSemantic, start)
		}
	}
	feed("response.output_item.added", `{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`)
	feed("response.reasoning_summary_text.delta", `{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`)
	require.NotNil(t, out.firstTokenMs)
	require.Nil(t, out.firstAnswer.ms)
	feed("response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","delta":"{\"cmd\""}`)
	require.NotNil(t, out.firstAnswer.ms)
	require.GreaterOrEqual(t, *out.firstAnswer.ms, 1000)
	require.True(t, out.endedAt.IsZero())
	feed("response.completed", openAIAnswerTestCompleted)
	require.False(t, out.endedAt.IsZero())
	require.NotNil(t, out.usage.ReasoningTokens)
	require.Equal(t, 20, *out.usage.ReasoningTokens)
}

// CPR 发完终止事件后迟迟不关连接：用时截在终止事件，首答进结果。
func TestOpenAIRawRelayDurationEndsAtTerminalEvent(t *testing.T) {
	svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, rawRelaySSE)
		w.(http.Flusher).Flush()
		time.Sleep(400 * time.Millisecond)
	})
	c, _, body := rawRelayTestContext(t, context.Background(), "/v1/responses", []byte(`{"model":"gpt-5.5","stream":true,"input":"hi"}`), "", nil)

	begin := time.Now()
	result, err := svc.Forward(context.Background(), c, account, body)
	elapsed := time.Since(begin)
	require.NoError(t, err)
	require.NotNil(t, result.FirstAnswerMs)
	require.GreaterOrEqual(t, elapsed, 400*time.Millisecond, "Forward 要读到连接关闭")
	require.Less(t, result.Duration, elapsed-300*time.Millisecond, "关连接前的空等不算用时")
	require.GreaterOrEqual(t, result.Duration.Milliseconds(), int64(*result.FirstAnswerMs))
}

// RecordUsage 把首答时刻和推理 token 写进 UsageLog；删掉 openai_gateway_usage.go 里那两行赋值，本用例失败。
func TestOpenAIGatewayServiceRecordUsage_PersistsTPSInputs(t *testing.T) {
	record := func(t *testing.T, result *OpenAIForwardResult) *UsageLog {
		t.Helper()
		usageRepo := &openAIRecordUsageLogRepoStub{inserted: true}
		billingRepo := &openAIRecordUsageBillingRepoStub{result: &UsageBillingApplyResult{Applied: true}}
		svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(usageRepo, billingRepo, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{}, nil)
		require.NoError(t, svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{
			Result:        result,
			APIKey:        &APIKey{ID: 1000, Quota: 100, Group: &Group{RateMultiplier: 1}},
			User:          &User{ID: 2000},
			Account:       &Account{ID: 3000, Type: AccountTypeAPIKey},
			APIKeyService: &openAIRecordUsageAPIKeyQuotaStub{},
		}))
		require.NotNil(t, usageRepo.lastLog)
		return usageRepo.lastLog
	}

	firstAnswer, reasoning := 4200, 640
	log := record(t, &OpenAIForwardResult{
		RequestID: "resp_tps", Model: "gpt-6-astra", Duration: 9 * time.Second, Stream: true, FirstAnswerMs: &firstAnswer,
		Usage: OpenAIUsage{InputTokens: 10, OutputTokens: 900, ReasoningTokens: &reasoning},
	})
	require.Equal(t, &firstAnswer, log.FirstAnswerMs)
	require.Equal(t, &reasoning, log.ReasoningTokens)

	log = record(t, &OpenAIForwardResult{RequestID: "resp_tps_absent", Model: "gpt-6-astra", Duration: time.Second, Usage: OpenAIUsage{OutputTokens: 5}})
	require.Nil(t, log.FirstAnswerMs)
	require.Nil(t, log.ReasoningTokens)
}

// Forward 把流式结果的首答时刻交给 OpenAIForwardResult（原生与透传各一处接线）。
func TestOpenAIForwardCarriesFirstAnswerMs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstreamSSE := "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"h\"}\n\n" +
		"data: " + openAIAnswerTestCompleted + "\n\n"
	for _, passthrough := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		c.Request.Header.Set("User-Agent", "codex_cli_rs/0.1.0")
		upstream := &httpUpstreamRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(upstreamSSE)),
		}}
		svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
		account := &Account{
			ID: 123, Name: "acc", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1,
			Credentials: map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-acc"},
			Extra:       map[string]any{"openai_passthrough": passthrough},
			Status:      StatusActive, Schedulable: true, RateMultiplier: f64p(1),
		}
		result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-5.2","stream":true,"input":[{"type":"text","text":"hi"}]}`))
		require.NoError(t, err, "passthrough=%v", passthrough)
		require.NotNil(t, result.FirstAnswerMs, "passthrough=%v", passthrough)
		require.NotNil(t, result.Usage.ReasoningTokens, "passthrough=%v", passthrough)
		require.Equal(t, 20, *result.Usage.ReasoningTokens)
	}
}

// 客户端工具映射把被映射调用的参数 delta 扣到 .done 才放出，首答不可信：DeepSeek 原生与 OpenAI API Key 透传都不记。
func TestOpenAIForwardDropsFirstAnswerUnderClientToolMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sse := strings.Join([]string{
		`data: {"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"type":"function_call","id":"i1","call_id":"c1","name":"apply_patch","status":"in_progress"}}`,
		`data: {"type":"response.function_call_arguments.delta","sequence_number":1,"item_id":"i1","output_index":0,"delta":"{\"input\":\"*** Begin"}`,
		`data: {"type":"response.function_call_arguments.delta","sequence_number":2,"item_id":"i1","output_index":0,"delta":" Patch\"}"}`,
		`data: {"type":"response.function_call_arguments.done","sequence_number":3,"item_id":"i1","call_id":"c1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\"}"}`,
		`data: {"type":"response.output_item.done","sequence_number":4,"output_index":0,"item":{"type":"function_call","id":"i1","call_id":"c1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\"}","status":"completed"}}`,
		`data: {"type":"response.completed","sequence_number":5,"response":{"id":"resp_tools","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":9,"output_tokens_details":{"reasoning_tokens":0}}}}`,
	}, "\n\n") + "\n\n"
	accounts := map[string]*Account{
		"deepseek":           {ID: 5661, Platform: PlatformDeepseek, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key", "api_protocol": APIProtocolResponses, "base_url": "https://relay.example"}},
		"openai-passthrough": {ID: 5660, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key"}, Extra: map[string]any{"openai_passthrough": true}},
	}
	for name, account := range accounts {
		body := openAIClientToolsRequest(true)
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
		upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}}

		result, err := openAIClientToolsTestService(upstream).Forward(context.Background(), c, account, body)
		require.NoError(t, err, name)
		require.Contains(t, recorder.Body.String(), `"type":"response.custom_tool_call_input.delta"`, name)
		require.Nil(t, result.FirstAnswerMs, name)
		require.NotNil(t, result.Usage.ReasoningTokens, name)
	}
}
