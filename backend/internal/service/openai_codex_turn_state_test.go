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
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func newTurnStateTestContext(t *testing.T, apiKeyID int64, sessionID string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	if sessionID != "" {
		c.Request.Header.Set("session_id", sessionID)
	}
	if apiKeyID > 0 {
		c.Set("api_key", &APIKey{ID: apiKeyID})
	}
	return c, rec
}

func TestOpenAICodexTurnStateKey(t *testing.T) {
	// 溯源按 blob 值记录：同值同键、异值异键，键长固定（blob 不透明且可能很长）。
	require.Equal(t, openAICodexTurnStateKey("blob-A"), openAICodexTurnStateKey("blob-A"))
	require.NotEqual(t, openAICodexTurnStateKey("blob-A"), openAICodexTurnStateKey("blob-B"))
	require.Len(t, openAICodexTurnStateKey("blob-A"), 24)
}

func TestRelayOpenAICodexTurnState_SetsHeaderAndRecordsProvenance(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 42}
	c, _ := newTurnStateTestContext(t, 7, "sess-relay")

	upstream := http.Header{}
	upstream.Set("x-codex-turn-state", "blob-A")
	svc.relayOpenAICodexTurnState(c, account, upstream)

	require.Equal(t, "blob-A", c.Writer.Header().Get("X-Codex-Turn-State"))

	raw, ok := svc.openaiCodexTurnStateOrigins.Load(openAICodexTurnStateKey("blob-A"))
	require.True(t, ok)
	origin, ok := raw.(openAICodexTurnStateOrigin)
	require.True(t, ok)
	require.Equal(t, "id:42", origin.owner)
	require.True(t, origin.expiresAt.After(time.Now()))
}

func TestRelayOpenAICodexTurnState_ClearsStaleValueWhenUpstreamAbsent(t *testing.T) {
	svc := &OpenAIGatewayService{}
	c, _ := newTurnStateTestContext(t, 7, "sess-stale")
	// 模拟上一 failover attempt 残留的值
	c.Writer.Header().Set("X-Codex-Turn-State", "blob-old")

	svc.relayOpenAICodexTurnState(c, &Account{ID: 43}, http.Header{})

	require.Empty(t, c.Writer.Header().Get("X-Codex-Turn-State"))
	_, ok := svc.openaiCodexTurnStateOrigins.Load(openAICodexTurnStateKey("blob-old"))
	require.False(t, ok)
}

func TestStageOpenAICodexTurnState_StagedHeaders(t *testing.T) {
	svc := &OpenAIGatewayService{}

	// nil 集合 + 上游有值 → 创建集合并写入，但暂存本身不记录溯源
	var staged http.Header
	upstream := http.Header{}
	upstream.Set("x-codex-turn-state", "blob-B")
	stageOpenAICodexTurnState(&staged, upstream)
	require.NotNil(t, staged)
	require.Equal(t, "blob-B", staged.Get("X-Codex-Turn-State"))
	_, noted := svc.openaiCodexTurnStateOrigins.Load(openAICodexTurnStateKey("blob-B"))
	require.False(t, noted)

	// 真正提交时记录
	svc.noteStagedOpenAICodexTurnStateCommitted(nil, &Account{ID: 44}, staged)
	raw, ok := svc.openaiCodexTurnStateOrigins.Load(openAICodexTurnStateKey("blob-B"))
	require.True(t, ok)
	origin, ok := raw.(openAICodexTurnStateOrigin)
	require.True(t, ok)
	require.Equal(t, "id:44", origin.owner)

	// 上游无值 → 清除已暂存的值；nil 集合保持 nil
	stageOpenAICodexTurnState(&staged, http.Header{})
	require.Empty(t, staged.Get("X-Codex-Turn-State"))
	var nilStaged http.Header
	stageOpenAICodexTurnState(&nilStaged, http.Header{})
	require.Nil(t, nilStaged)
}

func TestNoteStagedOpenAICodexTurnStateCommitted_NoopWithoutState(t *testing.T) {
	svc := &OpenAIGatewayService{}

	svc.noteStagedOpenAICodexTurnStateCommitted(nil, &Account{ID: 60}, nil)
	svc.noteStagedOpenAICodexTurnStateCommitted(nil, &Account{ID: 60}, http.Header{"X-Request-Id": []string{"rid"}})

	empty := true
	svc.openaiCodexTurnStateOrigins.Range(func(any, any) bool { empty = false; return false })
	require.True(t, empty)
}

// 溯源按 blob 记而不是按会话记：客户端的 turn_state 是每轮新建、首写生效的 OnceLock
// （core/src/client.rs:292、:522-526；core/tests/suite/turn_state.rs:252-257），一轮内换过号
// 后它仍回带最早那个 blob。按"会话 → 最近一次铸造账号"记会同时误剥自己的值、放行别人的值。
func TestGuardOpenAICodexTurnState_TracksBlobNotSession(t *testing.T) {
	svc := &OpenAIGatewayService{}
	cA, _ := newTurnStateTestContext(t, 7, "sess-1")
	upstreamA := http.Header{}
	upstreamA.Set("x-codex-turn-state", "blob-A")
	svc.relayOpenAICodexTurnState(cA, &Account{ID: 42}, upstreamA)

	// 同一会话随后由账号 43 铸出新 blob（一轮内 failover）：客户端的 OnceLock 已锁定 blob-A，
	// 它回带的还是 blob-A。
	upstreamB := http.Header{}
	upstreamB.Set("x-codex-turn-state", "blob-B")
	svc.relayOpenAICodexTurnState(cA, &Account{ID: 43}, upstreamB)

	echoA := http.Header{}
	echoA.Set("x-codex-turn-state", "blob-A")
	svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 42}, echoA)
	require.Equal(t, "blob-A", echoA.Get("x-codex-turn-state"), "铸造它的账号自己的回带不得剥离")

	echoToB := http.Header{}
	echoToB.Set("x-codex-turn-state", "blob-A")
	svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 43}, echoToB)
	require.Empty(t, echoToB.Get("x-codex-turn-state"), "异账号回带必须剥离")

	// 换一个下游会话回带同一个 blob，判定不变（守卫与承载会话无关）。
	other := http.Header{}
	other.Set("x-codex-turn-state", "blob-A")
	svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 43}, other)
	require.Empty(t, other.Get("x-codex-turn-state"))
}

// WS 上客户端持有的 blob 只来自被转发的 response.metadata 事件，铸造账号必须在那里记录，
// 否则纯 WS 会话的守卫永远查不到、等于空转。
func TestNoteOpenAICodexTurnStateFromWSEvent(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 71}

	svc.noteOpenAICodexTurnStateFromWSEvent(nil, account, []byte(`{"type":"response.metadata","headers":{"X-Codex-Turn-State":"blob-ws"}}`))
	echo := http.Header{}
	echo.Set("x-codex-turn-state", "blob-ws")
	svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 72}, echo)
	require.Empty(t, echo.Get("x-codex-turn-state"), "异账号回带 WS 事件里的 blob 必须剥离")

	kept := http.Header{}
	kept.Set("x-codex-turn-state", "blob-ws")
	svc.guardOpenAICodexTurnStateEcho(nil, account, kept)
	require.Equal(t, "blob-ws", kept.Get("x-codex-turn-state"))

	// 其它事件类型不记录，即使它也带 headers（真客户端只认 response.metadata，sse/responses.rs:219-226）
	svc.noteOpenAICodexTurnStateFromWSEvent(nil, account, []byte(`{"type":"response.created","headers":{"x-codex-turn-state":"blob-fake"}}`))
	unknown := http.Header{}
	unknown.Set("x-codex-turn-state", "blob-fake")
	svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 72}, unknown)
	require.Equal(t, "blob-fake", unknown.Get("x-codex-turn-state"))
}

func TestGuardOpenAICodexTurnStateEcho(t *testing.T) {
	newOutbound := func(state string) http.Header {
		h := http.Header{}
		if state != "" {
			h.Set("x-codex-turn-state", state)
		}
		return h
	}

	t.Run("same_account_keeps_echo", func(t *testing.T) {
		svc := &OpenAIGatewayService{}
		svc.noteOpenAICodexTurnStateOrigin(nil, &Account{ID: 42}, "blob-A")

		h := newOutbound("blob-A")
		svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 42}, h)
		require.Equal(t, "blob-A", h.Get("x-codex-turn-state"))
	})

	t.Run("foreign_account_strips_echo", func(t *testing.T) {
		svc := &OpenAIGatewayService{}
		svc.noteOpenAICodexTurnStateOrigin(nil, &Account{ID: 42}, "blob-A")

		h := newOutbound("blob-A")
		svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 43}, h)
		require.Empty(t, h.Get("x-codex-turn-state"))
	})

	t.Run("no_provenance_passthrough", func(t *testing.T) {
		svc := &OpenAIGatewayService{}
		h := newOutbound("blob-unknown")
		svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 43}, h)
		require.Equal(t, "blob-unknown", h.Get("x-codex-turn-state"))
	})

	t.Run("expired_provenance_passthrough_and_pruned", func(t *testing.T) {
		svc := &OpenAIGatewayService{}
		key := openAICodexTurnStateKey("blob-A")
		svc.openaiCodexTurnStateOrigins.Store(key, openAICodexTurnStateOrigin{
			owner:     "id:42",
			expiresAt: time.Now().Add(-time.Minute),
		})
		h := newOutbound("blob-A")
		svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 43}, h)
		require.Equal(t, "blob-A", h.Get("x-codex-turn-state"))
		_, ok := svc.openaiCodexTurnStateOrigins.Load(key)
		require.False(t, ok)
	})

	t.Run("no_echo_noop", func(t *testing.T) {
		svc := &OpenAIGatewayService{}
		h := newOutbound("")
		svc.guardOpenAICodexTurnStateEcho(nil, &Account{ID: 43}, h)
		require.Empty(t, h.Get("x-codex-turn-state"))
	})

	t.Run("value_form_and_frame_form_share_the_rule", func(t *testing.T) {
		svc := &OpenAIGatewayService{}
		svc.noteOpenAICodexTurnStateOrigin(nil, &Account{ID: 42}, "blob-A")

		require.Empty(t, svc.guardOpenAICodexTurnStateValue(nil, &Account{ID: 43}, "blob-A"))
		require.Equal(t, "blob-A", svc.guardOpenAICodexTurnStateValue(nil, &Account{ID: 42}, "blob-A"))

		frame := []byte(`{"type":"response.create","client_metadata":{"session_id":"S","x-codex-turn-state":"blob-A"}}`)
		stripped := svc.guardOpenAICodexWSFrameTurnState(nil, &Account{ID: 43}, frame)
		require.False(t, gjson.GetBytes(stripped, "client_metadata.x-codex-turn-state").Exists())
		require.Equal(t, "S", gjson.GetBytes(stripped, "client_metadata.session_id").String(), "只剥 turn-state")
		require.Equal(t, string(frame), string(svc.guardOpenAICodexWSFrameTurnState(nil, &Account{ID: 42}, frame)))
	})
}

// 铸造者按凭证域身份计：同一 ChatGPT 账号的多个本地行（含 spark 影子行）共享同一出站身份
// （codexAccountIdentityNamespace），上游看到的是同一个客户端，它们之间回带 blob 不是矛盾信号；
// 只有真正的异凭证才剥。
func TestGuardOpenAICodexTurnState_OwnerIsCredentialIdentityNotLocalRow(t *testing.T) {
	newRow := func(id int64, chatgptAccountID string) *Account {
		return &Account{ID: id, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{
			"access_token": "tok", "chatgpt_account_id": chatgptAccountID, "chatgpt_user_id": "user-1",
		}}
	}
	rowA, rowB, rowC := newRow(9101, "acct-1"), newRow(9102, "acct-1"), newRow(9103, "acct-2")
	require.Equal(t, codexAccountIdentityNamespace(rowA), codexAccountIdentityNamespace(rowB), "前提：两行同一凭证域")
	require.NotEqual(t, codexAccountIdentityNamespace(rowA), codexAccountIdentityNamespace(rowC))

	svc := &OpenAIGatewayService{}
	svc.noteOpenAICodexTurnStateOrigin(nil, rowA, "blob-A")
	require.Equal(t, "blob-A", svc.guardOpenAICodexTurnStateValue(nil, rowB, "blob-A"), "同凭证域的另一本地行回带必须保留")
	require.Empty(t, svc.guardOpenAICodexTurnStateValue(nil, rowC, "blob-A"), "异凭证域仍剥离")
	frame := []byte(`{"type":"response.create","client_metadata":{"x-codex-turn-state":"blob-A"}}`)
	require.Equal(t, string(frame), string(svc.guardOpenAICodexWSFrameTurnState(nil, rowB, frame)))
	require.False(t, gjson.GetBytes(svc.guardOpenAICodexWSFrameTurnState(nil, rowC, frame), "client_metadata.x-codex-turn-state").Exists())

	// spark 影子行自身没有凭据；入口把解析到的母账号暂存在 gin 上下文（prepareCodexAccountIdentitySource），
	// 守卫与记录都按那份身份算。
	parentID := rowA.ID
	shadow := &Account{ID: 9104, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID,
		Credentials: map[string]any{"model_mapping": map[string]any{}}}
	c, _ := newTurnStateTestContext(t, 7, "sess-shadow")
	c.Set(codexAccountIdentitySourceContextKey, rowA)
	require.Equal(t, "blob-A", svc.guardOpenAICodexTurnStateValue(c, shadow, "blob-A"), "影子行按母账号身份判定")
	echo := http.Header{}
	echo.Set("x-codex-turn-state", "blob-A")
	svc.guardOpenAICodexTurnStateEcho(c, shadow, echo)
	require.Equal(t, "blob-A", echo.Get("x-codex-turn-state"))
	svc.noteOpenAICodexTurnStateOrigin(c, shadow, "blob-S")
	require.Equal(t, "blob-S", svc.guardOpenAICodexTurnStateValue(nil, rowA, "blob-S"), "影子行铸出的 blob 记在母账号名下")
	require.Empty(t, svc.guardOpenAICodexTurnStateValue(nil, rowC, "blob-S"))
}

// 线上 HTTP（非透传）路径唯一的记录点是首输出守卫的暂存提交：blob 随首个语义输出一起真正
// 交给客户端时才记；首输出超时被丢弃的 attempt 不留记录。
func TestOpenAICodexTurnStateProvenance_RecordedAtStagedCommit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{Gateway: config.GatewayConfig{OpenAIFirstOutputTimeoutSeconds: 1, MaxLineSize: defaultMaxLineSize}}
	minter := &Account{ID: 91, Platform: PlatformOpenAI}
	other := &Account{ID: 92, Platform: PlatformOpenAI}

	t.Run("committed_with_first_output", func(t *testing.T) {
		svc := &OpenAIGatewayService{cfg: cfg, responseHeaderFilter: compileResponseHeaderFilter(cfg)}
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		body := strings.Join([]string{
			`data: {"type":"response.created","response":{"id":"resp_ok"}}`, "",
			`data: {"type":"response.output_text.delta","delta":"hello"}`, "",
			`data: {"type":"response.completed","response":{"id":"resp_ok","usage":{"input_tokens":1,"output_tokens":1}}}`, "", "",
		}, "\n")
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Codex-Turn-State": []string{"blob-staged"}}, Body: io.NopCloser(strings.NewReader(body))}

		_, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, minter, time.Now(), "model", "model")
		require.NoError(t, err)
		require.Equal(t, "blob-staged", rec.Result().Header.Get("X-Codex-Turn-State"), "客户端确实收到了这个 blob")
		require.True(t, svc.openAICodexTurnStateMintedByOther(nil, other, "blob-staged"), "提交时必须记到铸造账号名下")
		require.False(t, svc.openAICodexTurnStateMintedByOther(nil, minter, "blob-staged"))
	})

	t.Run("timed_out_attempt_leaves_no_record", func(t *testing.T) {
		svc := &OpenAIGatewayService{cfg: cfg, responseHeaderFilter: compileResponseHeaderFilter(cfg)}
		pr, pw := io.Pipe()
		go func() {
			defer func() { _ = pw.Close() }()
			_, _ = pw.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_slow\"}}\n\n"))
			time.Sleep(200 * time.Millisecond)
		}()
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Codex-Turn-State": []string{"blob-dropped"}}, Body: pr}

		_, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, minter, time.Now().Add(-2*time.Second), "model", "model")
		require.Error(t, err)
		require.Empty(t, rec.Result().Header.Get("X-Codex-Turn-State"), "被丢弃的 attempt 没有把 blob 交给客户端")
		_, recorded := svc.openaiCodexTurnStateOrigins.Load(openAICodexTurnStateKey("blob-dropped"))
		require.False(t, recorded)
	})
}

// 透传 HTTP 路径的记录点在 forwardOpenAIPassthrough 拿到上游响应处：删掉它，客户端拿到的
// blob 就没有铸造者，守卫空转。
func TestOpenAICodexTurnStateProvenance_RecordedAtPassthrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"model":"gpt-5.4","stream":false,"input":"hi"}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Codex-Turn-State": []string{"blob-pt"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"resp_pt","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)),
	}}
	svc := openAIClientToolsTestService(upstream)
	minter := &Account{ID: 5659, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key"}}

	_, err := svc.forwardOpenAIPassthrough(context.Background(), c, minter, body, body, "gpt-5.4", false, nil, false, time.Now())
	require.NoError(t, err)
	require.Equal(t, "blob-pt", recorder.Header().Get("X-Codex-Turn-State"), "透传把 blob 回给了客户端")
	require.True(t, svc.openAICodexTurnStateMintedByOther(nil, &Account{ID: 5660, Platform: PlatformOpenAI}, "blob-pt"))
	require.False(t, svc.openAICodexTurnStateMintedByOther(nil, minter, "blob-pt"))
}

func TestSweepOpenAICodexTurnStateOrigins_PrunesExpiredEntries(t *testing.T) {
	svc := &OpenAIGatewayService{}
	svc.openaiCodexTurnStateOrigins.Store("expired", openAICodexTurnStateOrigin{
		owner:     "id:1",
		expiresAt: time.Now().Add(-time.Minute),
	})
	svc.openaiCodexTurnStateOrigins.Store("alive", openAICodexTurnStateOrigin{
		owner:     "id:2",
		expiresAt: time.Now().Add(time.Hour),
	})

	// 计数器推进到触发清扫的边界
	svc.openaiCodexTurnStateWrites.Store(255)
	svc.sweepOpenAICodexTurnStateOrigins()

	_, expiredOK := svc.openaiCodexTurnStateOrigins.Load("expired")
	require.False(t, expiredOK)
	_, aliveOK := svc.openaiCodexTurnStateOrigins.Load("alive")
	require.True(t, aliveOK)
}

func TestWriteOpenAIPassthroughResponseHeaders_RelaysAndClearsTurnState(t *testing.T) {
	// filter=nil 走 content-type 兜底分支；turn-state 强制放行不依赖 filter。
	dst := http.Header{}
	src := http.Header{}
	src.Set("X-Codex-Turn-State", "blob-P")
	writeOpenAIPassthroughResponseHeaders(dst, src, nil)
	require.Equal(t, "blob-P", dst.Get("X-Codex-Turn-State"))

	// 上游缺失时清除残留（failover 换号防串扰）
	writeOpenAIPassthroughResponseHeaders(dst, http.Header{"Content-Type": []string{"application/json"}}, nil)
	require.Empty(t, dst.Get("X-Codex-Turn-State"))
}

func TestWriteOpenAIPassthroughResponseHeaders_RelaysReasoningIncluded(t *testing.T) {
	dst := http.Header{}
	src := http.Header{}
	src.Set("X-Reasoning-Included", "1")

	writeOpenAIPassthroughResponseHeaders(
		dst,
		src,
		responseheaders.CompileHeaderFilter(config.ResponseHeaderConfig{}),
	)
	require.Equal(t, "1", dst.Get("X-Reasoning-Included"))
}

func TestEnsureOpenAIRemoteCompactionV2BetaFeature(t *testing.T) {
	t.Run("absent_sets_feature", func(t *testing.T) {
		h := http.Header{}
		ensureOpenAIRemoteCompactionV2BetaFeature(h)
		require.Equal(t, "remote_compaction_v2", h.Get("x-codex-beta-features"))
	})

	t.Run("present_unchanged", func(t *testing.T) {
		h := http.Header{}
		h.Set("x-codex-beta-features", "responses_websockets_v2, remote_compaction_v2")
		ensureOpenAIRemoteCompactionV2BetaFeature(h)
		require.Equal(t, "responses_websockets_v2, remote_compaction_v2", h.Get("x-codex-beta-features"))
	})

	t.Run("other_tokens_merged", func(t *testing.T) {
		h := http.Header{}
		h.Set("x-codex-beta-features", "responses_websockets_v2")
		ensureOpenAIRemoteCompactionV2BetaFeature(h)
		require.Equal(t, "responses_websockets_v2,remote_compaction_v2", h.Get("x-codex-beta-features"))
	})

	t.Run("multi_line_values_merged_single_line", func(t *testing.T) {
		h := http.Header{}
		h.Add("x-codex-beta-features", "feature_a")
		h.Add("x-codex-beta-features", "feature_b")
		ensureOpenAIRemoteCompactionV2BetaFeature(h)
		require.Equal(t, []string{"feature_a,feature_b,remote_compaction_v2"}, h.Values("x-codex-beta-features"))
	})
}

// 对齐真实 Codex：该头是会话级常量，挂在 OAuth 的每个请求上，而不是只在
// 压缩回合出现（codex-rs build_model_client_beta_features_header）。
func TestApplyOpenAICodexBetaFeatures(t *testing.T) {
	oauthAccount := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	apiKeyAccount := &Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	t.Run("oauth_plain_request_gets_default_codex_shape", func(t *testing.T) {
		c, _ := newTurnStateTestContext(t, 7, "sess-beta")
		h := http.Header{}
		applyOpenAICodexBetaFeatures(c, oauthAccount, h)
		require.Equal(t, "remote_compaction_v2", h.Get("x-codex-beta-features"),
			"OAuth 的普通请求也必须带会话级 beta 头")
	})

	t.Run("client_declared_header_preserved", func(t *testing.T) {
		c, _ := newTurnStateTestContext(t, 7, "sess-beta")
		h := http.Header{}
		h.Set("x-codex-beta-features", "some_other_feature")
		applyOpenAICodexBetaFeatures(c, oauthAccount, h)
		require.Equal(t, "some_other_feature", h.Get("x-codex-beta-features"),
			"客户端显式声明的能力集不得被网关改写（非空即视为用户已关闭 v2）")
	})

	t.Run("native_v2_forces_feature_even_when_client_trimmed_it", func(t *testing.T) {
		c, _ := newTurnStateTestContext(t, 7, "sess-beta")
		MarkOpenAINativeCompactionV2(c)
		h := http.Header{}
		h.Set("x-codex-beta-features", "some_other_feature")
		applyOpenAICodexBetaFeatures(c, oauthAccount, h)
		require.Contains(t, h.Get("x-codex-beta-features"), "remote_compaction_v2",
			"body 带 compaction_trigger 是实锤，必须确保 v2 在列")
		require.Contains(t, h.Get("x-codex-beta-features"), "some_other_feature")
	})

	t.Run("native_v2_applies_to_non_oauth_too", func(t *testing.T) {
		c, _ := newTurnStateTestContext(t, 7, "sess-beta")
		MarkOpenAINativeCompactionV2(c)
		h := http.Header{}
		applyOpenAICodexBetaFeatures(c, apiKeyAccount, h)
		require.Equal(t, "remote_compaction_v2", h.Get("x-codex-beta-features"))
	})

	t.Run("non_oauth_plain_request_untouched", func(t *testing.T) {
		c, _ := newTurnStateTestContext(t, 7, "sess-beta")
		h := http.Header{}
		applyOpenAICodexBetaFeatures(c, apiKeyAccount, h)
		require.Empty(t, h.Get("x-codex-beta-features"),
			"非 Codex 后端不做会话级注入")
	})

	t.Run("nil_account_plain_request_untouched", func(t *testing.T) {
		c, _ := newTurnStateTestContext(t, 7, "sess-beta")
		h := http.Header{}
		applyOpenAICodexBetaFeatures(c, nil, h)
		require.Empty(t, h.Get("x-codex-beta-features"))
	})
}

// WS 握手与 HTTP 出站必须给出同一份会话级 beta 头：真实 Codex 的
// build_websocket_headers 复用 build_responses_headers（client.rs），
// 两侧不一致还会让预热连接与实际请求落进不同的连接池兼容分桶。
func TestBuildOpenAIWSHeaders_CarriesSessionBetaFeatures(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{}
	decision := OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2}

	build := func(t *testing.T, account *Account, clientBeta string) http.Header {
		t.Helper()
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
		if clientBeta != "" {
			c.Request.Header.Set("x-codex-beta-features", clientBeta)
		}
		headers, _, err := svc.buildOpenAIWSHeaders(
			context.Background(), c, account, "test-token", decision,
			true, "", "", "", "gpt-5.6-codex", "",
		)
		require.NoError(t, err)
		return headers
	}

	oauthAccount := &Account{
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Credentials: map[string]any{"chatgpt_account_id": "test-account"},
	}

	headers := build(t, oauthAccount, "")
	require.Equal(t, "remote_compaction_v2", headers.Get("x-codex-beta-features"),
		"WS 握手也必须带会话级 beta 头")

	declared := build(t, oauthAccount, "some_other_feature")
	require.Equal(t, []string{"some_other_feature"}, declared.Values("x-codex-beta-features"),
		"客户端已声明时原样保留")

	apiKeyHeaders := build(t, &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}, "")
	require.Empty(t, apiKeyHeaders.Get("x-codex-beta-features"),
		"非 Codex 后端不注入")
}
