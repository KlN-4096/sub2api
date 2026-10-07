//go:build unit

package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const codexBridgeTestUserHash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

// codexBridgeTestBody 是一条带 Claude Code 风格 metadata.user_id 的 /v1/messages 请求：
// 桥的会话键来自其中的 session（promptCacheKeyFromAnthropicMetadataSession）。
func codexBridgeTestBody(session string) []byte {
	return []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}],` +
		`"metadata":{"user_id":"user_` + codexBridgeTestUserHash + `_account__session_` + session + `"},"stream":false}`)
}

func codexBridgeTestBodyWithoutSession() []byte {
	return []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
}

func codexBridgeUpstreamResponse(turnState string) *http.Response {
	body := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.3-codex","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	header := http.Header{"Content-Type": []string{"text/event-stream"}}
	if turnState != "" {
		header.Set(openAICodexTurnStateHeader, turnState)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

func codexBridgeUpstreamResponses(n int) []*http.Response {
	out := make([]*http.Response, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, codexBridgeUpstreamResponse(""))
	}
	return out
}

func codexBridgeTestService(up *httpUpstreamRecorder) *OpenAIGatewayService {
	return &OpenAIGatewayService{
		httpUpstream: up,
		cfg:          &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false}}},
	}
}

func runCodexBridgeTurnModel(t *testing.T, svc *OpenAIGatewayService, account *Account, body []byte, model string) *gin.Context {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	result, err := svc.ForwardAsAnthropic(context.Background(), c, account, body, "", model)
	require.NoError(t, err)
	require.NotNil(t, result)
	return c
}

func runCodexBridgeTurn(t *testing.T, svc *OpenAIGatewayService, account *Account, body []byte) *gin.Context {
	t.Helper()
	return runCodexBridgeTurnModel(t, svc, account, body, "gpt-5.3-codex")
}

// codexBridgeTurnMetadataKeyOrder 是真客户端 turn-metadata 的 serde 声明序（core/src/responses_metadata.rs:510-567）
// 里桥会带的那些键。
var codexBridgeTurnMetadataKeyOrder = []string{
	"installation_id", "session_id", "thread_id", "agent_name", "turn_id", "window_id",
	"window_number", "context_window_id", "request_kind", "turn_started_at_unix_ms",
}

func codexBridgeJSONKeys(value gjson.Result) []string {
	keys := make([]string, 0, 8)
	value.ForEach(func(key, _ gjson.Result) bool {
		keys = append(keys, key.String())
		return true
	})
	return keys
}

func requireCodexV7(t *testing.T, value, what string) uuid.UUID {
	t.Helper()
	parsed, err := uuid.Parse(value)
	require.NoError(t, err, what)
	require.Equal(t, uuid.Version(7), parsed.Version(), "%s 必须是 UUIDv7", what)
	require.Equal(t, value, parsed.String(), "%s 必须是规范小写形态", what)
	return parsed
}

// requireCodexBridgeIdentityCoherent 断言一轮双开桥出站的全部会话身份载体自洽——真客户端五个载体同源
// （core/src/client.rs:1306-1308 session-id/thread-id/x-client-request-id，responses_metadata.rs:307-315
// client_metadata，:346-357 x-codex-window-id / x-codex-turn-metadata，client.rs:504-516 默认 PCK = session_id）。
// 返回 session-id、turn_id、context_window_id。
func requireCodexBridgeIdentityCoherent(t *testing.T, header http.Header, body []byte) (sid, turnID, contextWindowID string) {
	t.Helper()
	sid = header.Get("session-id")
	requireCodexV7(t, sid, "session-id")
	require.Equal(t, sid, header.Get("thread-id"))
	require.Equal(t, sid, header.Get("x-client-request-id"))
	require.Equal(t, sid, gjson.GetBytes(body, "prompt_cache_key").String(), "体内 prompt_cache_key 与 session-id 同源同值")
	require.Empty(t, header.Get("session_id"))
	require.Empty(t, header.Get("conversation_id"))
	require.Empty(t, header.Get("x-codex-installation-id"), "双开的安装身份只在体内（同账号 /responses 亦然）")

	cm := gjson.GetBytes(body, "client_metadata")
	require.True(t, cm.IsObject())
	require.ElementsMatch(t, []string{"session_id", "thread_id", "turn_id", "x-codex-installation-id", "x-codex-turn-metadata", "x-codex-window-id"},
		codexBridgeJSONKeys(cm), "client_metadata 键集合 = 真客户端无子代理时的集合（responses_metadata.rs:307-340）")
	require.Equal(t, sid, cm.Get("session_id").String())
	require.Equal(t, sid, cm.Get("thread_id").String())
	windowID := sid + ":0"
	require.Equal(t, windowID, header.Get("x-codex-window-id"), "窗口 = <thread>:<序号>，序号从 0 起（session/mod.rs:4190）")
	require.Equal(t, windowID, cm.Get("x-codex-window-id").String())
	turnID = cm.Get("turn_id").String()
	requireCodexV7(t, turnID, "turn_id")
	installationID := cm.Get("x-codex-installation-id").String()
	require.NotEmpty(t, installationID)

	headerTM := header.Get(openAIWSTurnMetadataHeader)
	require.NotEmpty(t, headerTM)
	require.Equal(t, headerTM, cm.Get(openAIWSTurnMetadataHeader).String(), "桥没有 tool_namespaces_info，头与体内的 turn-metadata 逐字节相同")
	tm := gjson.Parse(headerTM)
	require.True(t, tm.IsObject())
	require.Equal(t, codexBridgeTurnMetadataKeyOrder, codexBridgeJSONKeys(tm), "turn-metadata 键序 = serde 声明序")
	require.Equal(t, installationID, tm.Get("installation_id").String())
	require.Equal(t, sid, tm.Get("session_id").String())
	require.Equal(t, sid, tm.Get("thread_id").String())
	require.Equal(t, "/root", tm.Get("agent_name").String())
	require.Equal(t, turnID, tm.Get("turn_id").String())
	require.Equal(t, windowID, tm.Get("window_id").String())
	require.Equal(t, int64(0), tm.Get("window_number").Int())
	contextWindowID = tm.Get("context_window_id").String()
	requireCodexV7(t, contextWindowID, "context_window_id")
	require.NotEqual(t, sid, contextWindowID)
	require.Equal(t, "turn", tm.Get("request_kind").String())
	require.WithinDuration(t, time.Now(), time.UnixMilli(tm.Get("turn_started_at_unix_ms").Int()), time.Minute)

	require.Equal(t, "auto", gjson.GetBytes(body, "tool_choice").String(), "真客户端恒发 tool_choice（codex-api/src/common.rs:289）")
	return sid, turnID, contextWindowID
}

// 双开账号经 /v1/messages 兼容桥出站的会话身份必须与真客户端同形：桥在入站侧合成客户端原始身份，
// 走与 /responses 相同的管线，五个载体同源、UUIDv7、同一会话跨轮不变、每轮 turn 不同、不跨轮回注
// 上一轮的 x-codex-turn-state（turn_state 每轮一个 OnceLock，core/src/client.rs:285-292、turn_state.rs:140/:152）。
// 非双开 OAuth 桥维持基线：下划线 session_id、体内无 prompt_cache_key / tool_choice、跨轮回注 turn-state。
func TestForwardAsAnthropic_DeviceWireProfileBridgeSessionIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const sessionA = "11111111-2222-4333-8444-555555555555"
	const sessionB = "99999999-2222-4333-8444-555555555555"

	t.Run("device/enabled", func(t *testing.T) {
		account := wireProfileTestAccount(true)
		up := &httpUpstreamRecorder{responses: []*http.Response{
			codexBridgeUpstreamResponse("blob-turn-1"),
			codexBridgeUpstreamResponse("blob-turn-2"),
			codexBridgeUpstreamResponse(""),
		}}
		svc := codexBridgeTestService(up)
		c1 := runCodexBridgeTurn(t, svc, account, codexBridgeTestBody(sessionA))
		runCodexBridgeTurn(t, svc, account, codexBridgeTestBody(sessionA))
		runCodexBridgeTurn(t, svc, account, codexBridgeTestBody(sessionB))
		require.Len(t, up.requests, 3)

		sid, turnID, contextWindowID := requireCodexBridgeIdentityCoherent(t, up.requests[0].Header, up.bodies[0])
		require.Empty(t, up.requests[0].Header.Get(openAICodexTurnStateHeader))
		// 同一会话第二轮：会话/窗口身份不变、turn 换新、不带上一轮上游发下来的 turn-state
		sid2, turnID2, contextWindowID2 := requireCodexBridgeIdentityCoherent(t, up.requests[1].Header, up.bodies[1])
		require.Equal(t, sid, sid2)
		require.Equal(t, contextWindowID, contextWindowID2)
		require.NotEqual(t, turnID, turnID2, "每轮一个新的 turn_id（new_submission_id 每次 now_v7）")
		require.Empty(t, up.requests[1].Header.Get(openAICodexTurnStateHeader), "双开不跨轮回注上一轮的 x-codex-turn-state")
		// 另一个会话：另一枚身份
		sid3, _, _ := requireCodexBridgeIdentityCoherent(t, up.requests[2].Header, up.bodies[2])
		require.NotEqual(t, sid, sid3)

		// 出站头集合（集合断言）。与同账号 /responses 双开转发相比，只有客户端自带的子代理头
		// （x-codex-parent-thread-id / x-openai-subagent）不会出现——桥不是子代理；体内仍无 tools（桥只转
		// 发客户端给的工具）。
		require.ElementsMatch(t, []string{
			"accept", "authorization", "chatgpt-account-id", "content-encoding", "content-type", "originator",
			"session-id", "thread-id", "user-agent", "version", "x-client-request-id",
			"x-codex-beta-features", "x-codex-routing-hint", "x-codex-turn-metadata", "x-codex-window-id",
		}, codexProbeHeaderNames(up.requests[0].Header))
		// 合成的入站头在桥返回后已还原，不会漏给 failover 到的下一账号。
		for _, name := range []string{"session-id", "thread-id", "x-codex-window-id", openAIWSTurnMetadataHeader} {
			require.Empty(t, c1.Request.Header.Get(name), "入站头 %s 必须还原", name)
		}
	})

	t.Run("device/disabled", func(t *testing.T) {
		account := wireProfileTestAccount(false)
		up := &httpUpstreamRecorder{responses: []*http.Response{
			codexBridgeUpstreamResponse("blob-turn-1"),
			codexBridgeUpstreamResponse(""),
		}}
		svc := codexBridgeTestService(up)
		body := codexBridgeTestBody(sessionA)
		runCodexBridgeTurn(t, svc, account, body)
		runCodexBridgeTurn(t, svc, account, body)
		require.Len(t, up.requests, 2)
		first, second := up.requests[0].Header, up.requests[1].Header
		require.NotEmpty(t, first.Get("session_id"), "非双开维持基线的下划线别名")
		for _, name := range []string{"session-id", "thread-id", "x-codex-window-id", openAIWSTurnMetadataHeader} {
			require.Empty(t, first.Get(name), "非双开桥不合成 %s", name)
		}
		require.False(t, gjson.GetBytes(up.bodies[0], "prompt_cache_key").Exists(), "非双开桥体不带 prompt_cache_key（基线）")
		require.False(t, gjson.GetBytes(up.bodies[0], "tool_choice").Exists(), "非双开桥体不补 tool_choice（基线）")
		require.Equal(t, []string{"x-codex-installation-id"}, codexBridgeJSONKeys(gjson.GetBytes(up.bodies[0], "client_metadata")))
		require.Equal(t, "blob-turn-1", second.Get(openAICodexTurnStateHeader), "非双开维持基线的跨轮回注")
	})

	t.Run("device/enabled/no-session-key", func(t *testing.T) {
		// 没有桥会话键（非 Codex 兼容模型 + 无 Claude Code metadata）时不合成任何身份，也不写空的 prompt_cache_key。
		account := wireProfileTestAccount(true)
		up := &httpUpstreamRecorder{responses: codexBridgeUpstreamResponses(1)}
		svc := codexBridgeTestService(up)
		runCodexBridgeTurnModel(t, svc, account, codexBridgeTestBodyWithoutSession(), "gpt-4o")
		require.Len(t, up.requests, 1)
		for _, name := range []string{"session-id", "thread-id", "session_id", "x-codex-window-id", openAIWSTurnMetadataHeader} {
			require.Empty(t, up.requests[0].Header.Get(name), "%s", name)
		}
		require.False(t, gjson.GetBytes(up.bodies[0], "prompt_cache_key").Exists())
		require.False(t, gjson.GetBytes(up.bodies[0], "client_metadata.session_id").Exists())
	})

	t.Run("device/enabled/ttl-refresh", func(t *testing.T) {
		// TTL 是空闲窗口：持续有流量的会话不会在首铸后 TTL 到点时换身份（上游不回 turn-state 也一样）。
		account := wireProfileTestAccount(true)
		up := &httpUpstreamRecorder{responses: codexBridgeUpstreamResponses(3)}
		svc := codexBridgeTestService(up)
		svc.cfg.Gateway.OpenAIWS.StickyResponseIDTTLSeconds = 1
		body := codexBridgeTestBody(sessionA)
		runCodexBridgeTurn(t, svc, account, body)
		time.Sleep(700 * time.Millisecond)
		runCodexBridgeTurn(t, svc, account, body)
		time.Sleep(700 * time.Millisecond)
		runCodexBridgeTurn(t, svc, account, body)
		require.Len(t, up.requests, 3)
		sid := up.requests[0].Header.Get("session-id")
		require.NotEmpty(t, sid)
		require.Equal(t, sid, up.requests[1].Header.Get("session-id"))
		require.Equal(t, sid, up.requests[2].Header.Get("session-id"), "命中即续期，1.4s 后仍是同一会话")
	})

	t.Run("device/enabled/expiry", func(t *testing.T) {
		// 空闲超过 TTL 后重新铸造：上游看到一个新会话（新 v7 + 新窗口），各载体仍自洽。
		account := wireProfileTestAccount(true)
		up := &httpUpstreamRecorder{responses: codexBridgeUpstreamResponses(2)}
		svc := codexBridgeTestService(up)
		body := codexBridgeTestBody(sessionA)
		runCodexBridgeTurn(t, svc, account, body)
		expired := 0
		svc.openaiCompatBridgeSessions.Range(func(key, value any) bool {
			session := value.(openAICompatBridgeSession)
			session.ExpiresAt = time.Now().Add(-time.Minute)
			svc.openaiCompatBridgeSessions.Store(key, session)
			expired++
			return true
		})
		require.Equal(t, 1, expired)
		runCodexBridgeTurn(t, svc, account, body)
		sid1, _, ctx1 := requireCodexBridgeIdentityCoherent(t, up.requests[0].Header, up.bodies[0])
		sid2, _, ctx2 := requireCodexBridgeIdentityCoherent(t, up.requests[1].Header, up.bodies[1])
		require.NotEqual(t, sid1, sid2, "过期后重新铸造")
		require.NotEqual(t, ctx1, ctx2)
	})

	t.Run("device/enabled/shared-credential-rows", func(t *testing.T) {
		// 会话键按凭证域命名空间：母账号行与 spark 影子行共用一份凭证，同一桥会话得到同一枚身份；
		// 另一份凭证的账号得到另一枚。
		parent, shadow, other := codexShadowTestRows(true)
		up := &httpUpstreamRecorder{responses: codexBridgeUpstreamResponses(3)}
		svc := codexBridgeTestService(up)
		svc.accountRepo = &stubQuotaAccountRepo{accounts: map[int64]*Account{parent.ID: parent}}
		body := codexBridgeTestBody(sessionA)
		runCodexBridgeTurn(t, svc, parent, body)
		runCodexBridgeTurn(t, svc, shadow, body)
		runCodexBridgeTurn(t, svc, other, body)
		require.Len(t, up.requests, 3)
		sidParent, _, _ := requireCodexBridgeIdentityCoherent(t, up.requests[0].Header, up.bodies[0])
		sidShadow, _, _ := requireCodexBridgeIdentityCoherent(t, up.requests[1].Header, up.bodies[1])
		sidOther, _, _ := requireCodexBridgeIdentityCoherent(t, up.requests[2].Header, up.bodies[2])
		require.Equal(t, sidParent, sidShadow, "同一凭证域的两行共享同一桥会话身份")
		require.NotEqual(t, sidParent, sidOther)
	})

	t.Run("device/enabled/concurrent-first-mint", func(t *testing.T) {
		// 并发首轮只会有一枚胜出（LoadOrStore + CompareAndSwap），不会各铸各的互相覆盖。
		account := wireProfileTestAccount(true)
		svc := codexBridgeTestService(&httpUpstreamRecorder{})
		const workers = 64
		for round := 0; round < 30; round++ {
			key := fmt.Sprintf("anthropic-metadata-%d", round)
			ids := make([]string, workers)
			start := make(chan struct{})
			var ready, done sync.WaitGroup
			for i := range ids {
				// 上下文在屏障前建好：goroutine 放行后只剩 Load/Store 那一小段，窗口才撞得上。
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				ready.Add(1)
				done.Add(1)
				go func(i int, c *gin.Context) {
					defer done.Done()
					ready.Done()
					<-start
					session, ok := svc.openAICompatBridgeSession(c, account, key)
					if ok {
						ids[i] = session.SessionID
					}
				}(i, c)
			}
			ready.Wait()
			close(start)
			done.Wait()
			require.NotEmpty(t, ids[0])
			for _, id := range ids[1:] {
				require.Equal(t, ids[0], id, "同一会话键并发首轮必须得到同一枚身份")
			}
		}
	})
}

// 直连 /v1/responses 的双开请求即使体里出现桥标记（<todo-guard> 字面量 / anthropic-* 缓存键），也是
// 客户端自己的会话身份原样派生——桥身份只在 ForwardAsAnthropic 入口合成，不按请求体嗅探。
// 双开直连 /v1/responses：请求体里出现兼容桥标记（<todo-guard> 字面量 / anthropic-* 缓存键）不得改变出站身份——
// 真客户端每条 /responses 都无条件带 originator / version（client.rs:1315 add_originator_header；provider http_headers
// 的 version），会话身份是客户端自己的。判定方式：与同一客户端不带标记的请求逐头相同、头集合相同。
func TestCodexDeviceWireProfileDirectResponsesWithBridgeMarkerKeepsClientIdentity(t *testing.T) {
	run := func(t *testing.T, mutate func([]byte) []byte) (http.Header, []byte) {
		t.Helper()
		body := mutate(wireProfileTestBody(t))
		c := newConvTestContext(t, body)
		account := wireProfileTestAccount(true)
		svc, up := wireProfileTestService()
		_, _ = svc.Forward(context.Background(), c, account, body)
		require.NotNil(t, up.lastReq)
		return up.lastReq.Header, up.lastBody
	}
	plain, _ := run(t, func(b []byte) []byte { return b })
	for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "originator", "version", "user-agent"} {
		require.NotEmpty(t, plain.Get(name), name)
	}
	cases := map[string]func([]byte) []byte{
		"todo-guard": func(b []byte) []byte {
			b, err := sjson.SetBytes(b, "input.0.content.0.text", "please "+openAICompatClaudeCodeTodoGuardMarker+" now")
			require.NoError(t, err)
			return b
		},
		"anthropic-prompt-cache-key": func(b []byte) []byte {
			b, err := sjson.SetBytes(b, "prompt_cache_key", "anthropic-metadata-deadbeef")
			require.NoError(t, err)
			return b
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			header, body := run(t, mutate)
			for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "originator", "version", "user-agent"} {
				require.Equal(t, plain.Get(name), header.Get(name), "%s 必须与不带标记的同一请求相同（客户端自己的身份）", name)
			}
			require.ElementsMatch(t, codexProbeHeaderNames(plain), codexProbeHeaderNames(header), "出站头集合不得随请求体内容变化")
			require.Empty(t, header.Get("session_id"))
			sid := header.Get("session-id")
			require.Equal(t, sid, gjson.GetBytes(body, "client_metadata.session_id").String(), "头与体内会话同源")
			require.Equal(t, header.Get("thread-id"), gjson.GetBytes(body, "client_metadata.thread_id").String())
			if name == "todo-guard" {
				require.Equal(t, sid, gjson.GetBytes(body, "prompt_cache_key").String())
			}
		})
	}
}
