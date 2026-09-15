package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 三条 WS 路径（passthrough / ctx_pool ingress / HTTP→WS v2）都直接驱动生产入口，
// 断言真正出站的握手头与帧字节，不在测试里复刻生产判断再调 helper。

func codexWSWireProfileConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	return cfg
}

// codexWSStagedDialer 每次拨号交出下一条预置连接，并记录每次握手头与握手响应头。
type codexWSStagedDialer struct {
	mu        sync.Mutex
	conns     []openAIWSClientConn
	handshake http.Header
	headers   []http.Header
}

func (d *codexWSStagedDialer) Dial(_ context.Context, _ string, headers http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.headers = append(d.headers, cloneHeader(headers))
	if len(d.conns) == 0 {
		return nil, 0, nil, errors.New("no staged upstream connection left")
	}
	conn := d.conns[0]
	d.conns = d.conns[1:]
	return conn, 0, cloneHeader(d.handshake), nil
}

func (d *codexWSStagedDialer) Headers() []http.Header {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]http.Header(nil), d.headers...)
}

func codexWSCompletedEvent(id string) []byte {
	return []byte(`{"type":"response.completed","response":{"id":"` + id + `","model":"gpt-5.5","usage":{"input_tokens":1,"output_tokens":1}}}`)
}

func codexWSWireProfileService(cfg *config.Config) *OpenAIGatewayService {
	return &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
	}
}

// runCodexWSIngress 起一个入站 WS 服务端，把连接交给 ProxyResponsesWebSocketFromClient；
// 客户端逐帧发送并等待每轮的终态事件。inbound 是网关看到的入站握手头。
func runCodexWSIngress(t *testing.T, svc *OpenAIGatewayService, account *Account, inbound http.Header, frames []string) {
	t.Helper()
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		req := r.Clone(r.Context())
		req.Header = req.Header.Clone()
		for name, values := range inbound {
			req.Header[name] = values
		}
		ginCtx.Request = req
		readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		msgType, first, readErr := conn.Read(readCtx)
		cancel()
		if readErr != nil {
			serverErrCh <- readErr
			return
		}
		if msgType != coderws.MessageText {
			serverErrCh <- errors.New("unexpected first message type")
			return
		}
		serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "offline-token", first, nil)
	}))
	defer server.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	client, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = client.CloseNow() }()
	for i, frame := range frames {
		writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
		err = client.Write(writeCtx, coderws.MessageText, []byte(frame))
		cancelWrite()
		require.NoError(t, err, "turn %d", i+1)
		// 终态之前可能还有 response.metadata 等事件，读到终态为止。
		var event []byte
		for read := 0; read < 8; read++ {
			readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
			_, next, readErr := client.Read(readCtx)
			cancelRead()
			require.NoError(t, readErr, "turn %d", i+1)
			event = next
			if gjson.GetBytes(event, "type").String() == "response.completed" {
				break
			}
		}
		require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String(), "turn %d: %s", i+1, event)
	}
	_ = client.Close(coderws.StatusNormalClosure, "done")
	select {
	case serverErr := <-serverErrCh:
		if serverErr != nil {
			require.Contains(t, serverErr.Error(), "StatusNormalClosure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("websocket ingress did not finish")
	}
}

func codexWSIngressInbound() http.Header {
	inbound := http.Header{}
	inbound.Set("User-Agent", "codex_cli_rs/0.98.0")
	inbound.Set("session-id", "S")
	inbound.Set(openAIWSTurnMetadataHeader, `{"session_id":"S","thread_id":"S"}`)
	return inbound
}

const codexWSTestFrame = `{"client_metadata":{"session_id":"S"},"type":"response.create","model":"gpt-5.5","stream":false,"input":[{"type":"message","role":"user","content":"hi"}]}`

// 第二帧：顶层乱序、自带 turn-state 与时间戳（真客户端后续轮次的形态）。
const codexWSSecondFrame = `{"input":[{"type":"message","role":"user","content":"hi 2"}],"client_metadata":{"session_id":"S","x-codex-turn-state":"own-2","x-codex-ws-stream-request-start-ms":"123456"},"stream":false,"model":"gpt-5.5","type":"response.create"}`

// requireCodexWSStreamRequestStart 断言帧带 x-codex-ws-stream-request-start-ms：want 为空时
// 要求是网关在发送前盖的 unix 毫秒（十进制字符串，落在测试时间窗内），否则必须原样等于 want。
func requireCodexWSStreamRequestStart(t *testing.T, frame []byte, want string) {
	t.Helper()
	got := gjson.GetBytes(frame, "client_metadata."+codexWSStreamRequestStartKey)
	require.Equal(t, gjson.String, got.Type, "时间戳必须是字符串（HashMap<String,String>）：%s", frame)
	if want != "" {
		require.Equal(t, want, got.Str, "自带的时间戳不得改写：%s", frame)
		return
	}
	ms, err := strconv.ParseInt(got.Str, 10, 64)
	require.NoError(t, err, "十进制毫秒：%s", got.Str)
	now := time.Now().UnixMilli()
	require.True(t, ms > now-60_000 && ms <= now+1_000, "时间戳不在当前时间窗内：%d vs %d", ms, now)
}

// 入站握手带 turn-state：双开握手不带、帧内承载（帧自带的不覆盖）；未开投影维持握手承载。
func TestCodexDeviceWireProfileWSIngressTurnState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool} {
		for _, enabled := range []bool{false, true} {
			for _, own := range []string{"", "client-own"} {
				t.Run(fmt.Sprintf("%s/enabled=%v/own=%q", mode, enabled, own), func(t *testing.T) {
					upstream := newCodexWSPacedConn(codexWSCompletedEvent("resp_1"), codexWSCompletedEvent("resp_2"))
					dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{upstream}}
					cfg := codexWSWireProfileConfig()
					svc := codexWSWireProfileService(cfg)
					if mode == OpenAIWSIngressModePassthrough {
						svc.openaiWSPassthroughDialer = dialer
					} else {
						pool := newOpenAIWSConnPool(cfg)
						pool.setClientDialerForTest(dialer)
						svc.openaiWSPool = pool
					}
					account := wireProfileTestAccount(enabled)
					account.Extra["openai_oauth_responses_websockets_v2_mode"] = mode
					inbound := codexWSIngressInbound()
					inbound.Set(openAICodexTurnStateHeader, "turn-state-1")
					frame := codexWSTestFrame
					if own != "" {
						var err error
						frame, err = sjson.Set(frame, "client_metadata."+openAICodexTurnStateHeader, own)
						require.NoError(t, err)
					}
					// 第二帧：自带 turn-state 与时间戳、顶层键乱序——后续帧的收口点（passthrough
					// 过滤回调 / ctx_pool sendAndRelay）必须与首帧同样处理：turn-state 保留、
					// 戳重盖、字段序重排。
					runCodexWSIngress(t, svc, account, inbound, []string{frame, codexWSSecondFrame})

					headers := dialer.Headers()
					require.Len(t, headers, 1)
					require.Equal(t, resolveCodexOutboundIdentity("").version, headers[0].Get("version"), "version 是 provider 头，握手必带")
					require.Len(t, upstream.rawWrites, 2)
					sent := upstream.rawWrites[0]
					second := upstream.rawWrites[1]
					got := gjson.GetBytes(sent, "client_metadata."+openAICodexTurnStateHeader).String()
					require.Equal(t, "own-2", gjson.GetBytes(second, "client_metadata."+openAICodexTurnStateHeader).String(), "第二帧自带的 turn-state 不被覆盖：%s", second)
					if enabled {
						require.Empty(t, headers[0].Get(openAICodexTurnStateHeader), "双开握手不带 turn-state（client.rs:1241）")
						want := "turn-state-1"
						if own != "" {
							want = own
						}
						require.Equal(t, want, got, "帧内承载：%s", sent)
						for i, raw := range [][]byte{sent, second} {
							keys := topLevelKeys(t, raw)
							require.Equal(t, "type", keys[0], "帧 %d: %v", i+1, keys)
							requireCodexFieldOrder(t, keys, codexWantWSCreateOrder)
						}
						requireCodexWSStreamRequestStart(t, sent, "")
						requireCodexWSStreamRequestStart(t, second, "")
						require.NotEqual(t, "123456", gjson.GetBytes(second, "client_metadata."+codexWSStreamRequestStartKey).String(),
							"发送边界无条件重盖（client.rs:2105-2111 insert）：%s", second)
					} else {
						require.Equal(t, "turn-state-1", headers[0].Get(openAICodexTurnStateHeader), "未开投影维持握手承载")
						require.Equal(t, own, got, "未开投影不往帧里补")
						require.Equal(t, "client_metadata", topLevelKeys(t, sent)[0], "未开投影不重排帧：%s", sent)
						require.Equal(t, "input", topLevelKeys(t, second)[0], "未开投影不重排第二帧：%s", second)
						require.False(t, gjson.GetBytes(sent, "client_metadata."+codexWSStreamRequestStartKey).Exists(), "未开投影不盖时间戳")
					}
				})
			}
		}
	}
}

// 上游握手铸造 turn-state 后：真客户端在 connect 之前构造帧（client.rs:1789-1796），
// 而且网关握手铸出的 turn-state 客户端根本收不到（core/src/client.rs:1174 connect 传 None、
// :1239-1242 握手不发），所以双开的帧里既不带它、重新拨号的握手也不把它带回去（写回门控）；
// 未开投影的账号维持既有的握手回填。
func TestCodexDeviceWireProfileWSIngressUpstreamMintedTurnState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			first := &openAIWSWriteFailAfterFirstTurnConn{events: [][]byte{codexWSCompletedEvent("resp_1")}}
			second := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_2")}}
			trace := &codexWSTrace{}
			firstCapture := &codexWSFrameRecorder{inner: first, name: "conn1", trace: trace}
			secondCapture := &codexWSFrameRecorder{inner: second, name: "conn2", trace: trace}
			dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{firstCapture, secondCapture}, handshake: http.Header{}}
			dialer.handshake.Set(openAICodexTurnStateHeader, "minted-1")
			cfg := codexWSWireProfileConfig()
			svc := codexWSWireProfileService(cfg)
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			svc.openaiWSPool = pool
			account := wireProfileTestAccount(enabled)
			account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
			frame2, err := sjson.Set(codexWSTestFrame, "input.0.content", "hi 2")
			require.NoError(t, err)
			runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{codexWSTestFrame, frame2})

			dump := func() string { return strings.Join(trace.lines, "\n") }
			headers := dialer.Headers()
			require.Len(t, headers, 2, "第二轮写失败后必须重新拨号\n%s", dump())
			require.Empty(t, headers[0].Get(openAICodexTurnStateHeader), "首次握手：客户端没带，网关也没有")
			// conn1 收到首轮帧与第二轮的首次写入（写失败后才换连重试）。
			require.Len(t, firstCapture.frames, 2, dump())
			require.False(t, gjson.GetBytes(firstCapture.frames[0], "client_metadata."+openAICodexTurnStateHeader).Exists(),
				"首帧在握手之前构造，不带本次握手铸出的状态")
			require.Len(t, secondCapture.frames, 1, dump())
			secondFrame := gjson.GetBytes(secondCapture.frames[0], "client_metadata."+openAICodexTurnStateHeader).String()
			if enabled {
				require.Empty(t, headers[1].Get(openAICodexTurnStateHeader), "双开重新拨号的握手不带（写回门控）")
				require.Empty(t, secondFrame, "网关铸出的 turn-state 客户端拿不到，不得进帧：%s", secondCapture.frames[0])
			} else {
				require.Equal(t, "minted-1", headers[1].Get(openAICodexTurnStateHeader), "未开投影维持握手回填")
				require.Empty(t, secondFrame)
			}
		})
	}
}

type codexWSTrace struct {
	mu    sync.Mutex
	lines []string
}

func (t *codexWSTrace) add(line string) {
	t.mu.Lock()
	t.lines = append(t.lines, line)
	t.mu.Unlock()
}

// codexWSFrameRecorder 记录写往上游的原始帧与读写结果，其余委托给内层连接。
type codexWSFrameRecorder struct {
	inner  openAIWSClientConn
	name   string
	trace  *codexWSTrace
	mu     sync.Mutex
	frames [][]byte
}

func (c *codexWSFrameRecorder) WriteJSON(ctx context.Context, value any) error {
	if raw, ok := value.(json.RawMessage); ok {
		c.mu.Lock()
		c.frames = append(c.frames, append([]byte(nil), raw...))
		c.mu.Unlock()
	}
	err := c.inner.WriteJSON(ctx, value)
	if c.trace != nil {
		c.trace.add(fmt.Sprintf("%s write err=%v payload=%s", c.name, err, value))
	}
	return err
}

// WriteFrame 记录原样写出的文本帧；内层不支持原始写时退回 WriteJSON(json.RawMessage)。
func (c *codexWSFrameRecorder) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	c.mu.Lock()
	c.frames = append(c.frames, append([]byte(nil), payload...))
	c.mu.Unlock()
	var err error
	if raw, ok := c.inner.(openAIWSRawTextWriter); ok {
		err = raw.WriteFrame(ctx, msgType, payload)
	} else {
		err = c.inner.WriteJSON(ctx, json.RawMessage(payload))
	}
	if c.trace != nil {
		c.trace.add(fmt.Sprintf("%s writeframe err=%v payload=%s", c.name, err, payload))
	}
	return err
}

func (c *codexWSFrameRecorder) ReadMessage(ctx context.Context) ([]byte, error) {
	msg, err := c.inner.ReadMessage(ctx)
	if c.trace != nil {
		c.trace.add(fmt.Sprintf("%s read err=%v msg=%s", c.name, err, msg))
	}
	return msg, err
}

func (c *codexWSFrameRecorder) Ping(ctx context.Context) error { return c.inner.Ping(ctx) }

func (c *codexWSFrameRecorder) Close() error { return c.inner.Close() }

// HTTP→WS v2：Forward 两次，中间淘汰连接迫使重连；首次握手铸造 turn-state 后，
// 双开的第二次握手不带、第二帧带；帧顶层字段序对齐真客户端。
func TestCodexDeviceWireProfileWSV2TurnState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			first := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_v2_1")}}
			second := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_v2_2")}}
			dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{first, second}, handshake: http.Header{}}
			dialer.handshake.Set(openAICodexTurnStateHeader, "minted-1")
			cfg := codexWSWireProfileConfig()
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 0
			svc := codexWSWireProfileService(cfg)
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			svc.openaiWSPool = pool
			account := wireProfileTestAccount(enabled)
			account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true

			body := wireProfileTestBody(t)
			newCtx := func() *gin.Context {
				c := newConvTestContext(t, body)
				c.Request.URL.Path = "/v1/responses"
				return c
			}
			c1 := newCtx()
			result1, err := svc.Forward(context.Background(), c1, account, body)
			require.NoError(t, err)
			require.NotNil(t, result1)
			store := svc.getOpenAIWSStateStore()
			connID, hasConn := store.GetResponseConn(result1.RequestID)
			require.True(t, hasConn)
			svc.getOpenAIWSConnPool().evictConn(account.ID, connID)

			c2 := newCtx()
			result2, err := svc.Forward(context.Background(), c2, account, body)
			require.NoError(t, err)
			require.NotNil(t, result2)

			headers := dialer.Headers()
			require.Len(t, headers, 2)
			require.Len(t, first.rawWrites, 1)
			require.Len(t, second.rawWrites, 1)
			for i, h := range headers {
				require.Equal(t, resolveCodexOutboundIdentity("").version, h.Get("version"), "握手 %d version", i+1)
			}
			require.Empty(t, headers[0].Get(openAICodexTurnStateHeader))
			require.False(t, gjson.GetBytes(first.rawWrites[0], "client_metadata."+openAICodexTurnStateHeader).Exists())
			secondFrame := gjson.GetBytes(second.rawWrites[0], "client_metadata."+openAICodexTurnStateHeader).String()
			if enabled {
				require.Empty(t, headers[1].Get(openAICodexTurnStateHeader), "双开重连握手不带 turn-state")
				require.Empty(t, secondFrame, "会话存储里的值不是客户端持有的值，不得进帧：%s", second.rawWrites[0])
				for _, raw := range [][]byte{first.rawWrites[0], second.rawWrites[0]} {
					keys := topLevelKeys(t, raw)
					require.Equal(t, "type", keys[0], "%v", keys)
					requireCodexFieldOrder(t, keys, codexWantWSCreateOrder)
					requireCodexWSStreamRequestStart(t, raw, "")
				}
			} else {
				require.Equal(t, "minted-1", headers[1].Get(openAICodexTurnStateHeader), "未开投影维持握手回填")
				require.Empty(t, secondFrame)
			}
		})
	}
}

// 握手构造器本身：双开不带 turn-state；version 头对所有账号都钉到规范身份。
func TestCodexDeviceWireProfileWSHandshakeBuilder(t *testing.T) {
	svc := &OpenAIGatewayService{}
	for _, enabled := range []bool{false, true} {
		c := newConvTestContext(t, nil)
		c.Request.Header.Set("version", "0.0.1")
		account := wireProfileTestAccount(enabled)
		headers, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, account, "tok",
			OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
			true, "turn-state-1", convTestTurnMetadata(), convTestSession, "", "")
		require.NoError(t, err)
		require.Equal(t, resolveCodexOutboundIdentity("").version, headers.Get("version"), "enabled=%v", enabled)
		require.Equal(t, !enabled, headers.Get(openAICodexTurnStateHeader) != "", "enabled=%v", enabled)
	}
}

// codexWSRecordTurnStateOrigin 模拟会话 S 先前经 HTTP 路径由 minter 铸造了 blob
// （relayOpenAICodexTurnState 的溯源记录）。API Key 与 WS 入站一致（未设置 → 0）。
func codexWSRecordTurnStateOrigin(t *testing.T, svc *OpenAIGatewayService, minter *Account, sessionID, blob string) {
	t.Helper()
	origin, _ := gin.CreateTestContext(httptest.NewRecorder())
	origin.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	origin.Request.Header.Set("session-id", sessionID)
	upstream := http.Header{}
	upstream.Set(openAICodexTurnStateHeader, blob)
	svc.relayOpenAICodexTurnState(origin, minter, upstream)
	require.Equal(t, blob, origin.Writer.Header().Get(openAICodexTurnStateHeader))
}

// 跨账号回声守卫（WS 入站，passthrough / ctx_pool）：客户端把 A 账号铸造的 turn-state
// 回带给 failover 后的 B 账号，握手头与帧内 client_metadata 都必须剥离；回带给 A 本人
// 则保持原样（双开：握手不带、帧内承载；未开投影：握手承载、帧原样）。
func TestCodexWSTurnStateEchoGuardIngress(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool} {
		for _, enabled := range []bool{false, true} {
			for _, sameAccount := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/enabled=%v/same=%v", mode, enabled, sameAccount), func(t *testing.T) {
					upstream := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_1")}}
					dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{upstream}}
					cfg := codexWSWireProfileConfig()
					svc := codexWSWireProfileService(cfg)
					if mode == OpenAIWSIngressModePassthrough {
						svc.openaiWSPassthroughDialer = dialer
					} else {
						pool := newOpenAIWSConnPool(cfg)
						pool.setClientDialerForTest(dialer)
						svc.openaiWSPool = pool
					}
					minter := wireProfileTestAccount(enabled)
					codexWSRecordTurnStateOrigin(t, svc, minter, "S", "blob-A")

					account := minter
					if !sameAccount {
						// 异账号 = 异凭证域（溯源按凭证域记，同一 ChatGPT 账号的另一本地行不算异账号）。
						account = wireProfileTestAccount(enabled)
						account.ID = minter.ID + 1
						account.Credentials = map[string]any{"access_token": "offline-token-b", "chatgpt_account_id": "other-account"}
					}
					account.Extra["openai_oauth_responses_websockets_v2_mode"] = mode
					inbound := codexWSIngressInbound()
					inbound.Set(openAICodexTurnStateHeader, "blob-A")
					frame, err := sjson.Set(codexWSTestFrame, "client_metadata."+openAICodexTurnStateHeader, "blob-A")
					require.NoError(t, err)
					runCodexWSIngress(t, svc, account, inbound, []string{frame})

					headers := dialer.Headers()
					require.Len(t, headers, 1)
					require.Len(t, upstream.rawWrites, 1)
					sent := upstream.rawWrites[0]
					frameState := gjson.GetBytes(sent, "client_metadata."+openAICodexTurnStateHeader)
					if sameAccount {
						require.Equal(t, !enabled, headers[0].Get(openAICodexTurnStateHeader) == "blob-A",
							"同账号：未开投影握手承载，双开握手不带")
						require.Equal(t, "blob-A", frameState.String(), "同账号回带原样：%s", sent)
						return
					}
					require.Empty(t, headers[0].Get(openAICodexTurnStateHeader), "异账号：握手剥离")
					require.False(t, frameState.Exists(), "异账号：帧内剥离 %s", sent)
					require.True(t, gjson.GetBytes(sent, "client_metadata.session_id").Exists(), "只剥 turn-state：%s", sent)
				})
			}
		}
	}
}

// HTTP→WS v2：上游握手铸造的 turn-state 会经响应头交给客户端，必须记录铸造账号；
// 客户端回带给 B 账号时握手/帧/会话存储回落三处都剥离，回带给 A 本人保持原样。
func TestCodexWSTurnStateEchoGuardV2(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, enabled := range []bool{false, true} {
		for _, sameAccount := range []bool{true, false} {
			t.Run(fmt.Sprintf("enabled=%v/same=%v", enabled, sameAccount), func(t *testing.T) {
				first := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_v2_1")}}
				second := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_v2_2")}}
				dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{first, second}, handshake: http.Header{}}
				dialer.handshake.Set(openAICodexTurnStateHeader, "minted-1")
				cfg := codexWSWireProfileConfig()
				cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 0
				svc := codexWSWireProfileService(cfg)
				pool := newOpenAIWSConnPool(cfg)
				pool.setClientDialerForTest(dialer)
				svc.openaiWSPool = pool
				minter := wireProfileTestAccount(enabled)
				minter.Extra["openai_oauth_responses_websockets_v2_enabled"] = true

				body := wireProfileTestBody(t)
				newCtx := func() *gin.Context {
					c := newConvTestContext(t, body)
					c.Request.URL.Path = "/v1/responses"
					return c
				}
				c1 := newCtx()
				result1, err := svc.Forward(context.Background(), c1, minter, body)
				require.NoError(t, err)
				require.NotNil(t, result1)
				require.Equal(t, "minted-1", c1.Writer.Header().Get(openAICodexTurnStateHeader), "铸出的 blob 交给了客户端")
				connID, hasConn := svc.getOpenAIWSStateStore().GetResponseConn(result1.RequestID)
				require.True(t, hasConn)
				svc.getOpenAIWSConnPool().evictConn(minter.ID, connID)

				account := minter
				if !sameAccount {
					account = wireProfileTestAccount(enabled)
					account.ID = minter.ID + 1
					account.Credentials = map[string]any{"access_token": "offline-token-b", "chatgpt_account_id": "other-account"}
					account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
				}
				c2 := newCtx()
				c2.Request.Header.Set(openAICodexTurnStateHeader, "minted-1")
				result2, err := svc.Forward(context.Background(), c2, account, body)
				require.NoError(t, err)
				require.NotNil(t, result2)

				headers := dialer.Headers()
				require.Len(t, headers, 2)
				require.Len(t, second.rawWrites, 1)
				frameState := gjson.GetBytes(second.rawWrites[0], "client_metadata."+openAICodexTurnStateHeader)
				if sameAccount {
					if enabled {
						require.Empty(t, headers[1].Get(openAICodexTurnStateHeader))
						require.Equal(t, "minted-1", frameState.String(), "同账号回带：双开帧内承载")
					} else {
						require.Equal(t, "minted-1", headers[1].Get(openAICodexTurnStateHeader), "同账号回带：握手承载")
						require.False(t, frameState.Exists())
					}
					return
				}
				require.Empty(t, headers[1].Get(openAICodexTurnStateHeader), "异账号：握手剥离")
				require.False(t, frameState.Exists(), "异账号：帧内不得由会话存储回落补回 A 的 blob：%s", second.rawWrites[0])
			})
		}
	}
}

// codexWSRealUpstream 是一个真正的 coder/websocket 上游：记录每条连接收到的原始文本帧字节，
// 每帧回一个 response.completed。只有在这里才能看到 wsjson/json.Encoder 之类编码层的效果。
type codexWSRealUpstream struct {
	mu     sync.Mutex
	frames [][]byte
	server *httptest.Server
}

func newCodexWSRealUpstream(t *testing.T) *codexWSRealUpstream {
	t.Helper()
	u := &codexWSRealUpstream{}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		for i := 1; ; i++ {
			msgType, payload, readErr := conn.Read(r.Context())
			if readErr != nil {
				return
			}
			if msgType != coderws.MessageText {
				return
			}
			u.mu.Lock()
			u.frames = append(u.frames, append([]byte(nil), payload...))
			u.mu.Unlock()
			if writeErr := conn.Write(r.Context(), coderws.MessageText, codexWSCompletedEvent(fmt.Sprintf("resp_%d", i))); writeErr != nil {
				return
			}
		}
	}))
	t.Cleanup(u.server.Close)
	return u
}

func (u *codexWSRealUpstream) Frames() [][]byte {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([][]byte(nil), u.frames...)
}

// Dial 用生产拨号器连到真上游（忽略网关算出的 URL），握手头照常记录在 headers 里。
type codexWSRealDialer struct {
	upstream *codexWSRealUpstream
	inner    coderOpenAIWSClientDialer
	mu       sync.Mutex
	headers  []http.Header
}

func (d *codexWSRealDialer) Dial(ctx context.Context, _ string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	d.mu.Lock()
	d.headers = append(d.headers, cloneHeader(headers))
	d.mu.Unlock()
	return d.inner.Dial(ctx, "ws"+strings.TrimPrefix(d.upstream.server.URL, "http"), headers, proxyURL)
}

func requireCodexWSWireBytes(t *testing.T, frame []byte, enabled bool, marker string) {
	t.Helper()
	require.True(t, gjson.ValidBytes(frame), "%s", frame)
	if enabled {
		require.Contains(t, string(frame), marker, "双开：<>& 原字节上线（serde_json 不转义）")
		require.NotContains(t, string(frame), `\u003c`)
		require.NotContains(t, string(frame), `\u0026`)
		require.False(t, strings.HasSuffix(string(frame), "\n"), "双开：帧尾不带 json.Encoder 的换行")
		keys := topLevelKeys(t, frame)
		require.Equal(t, "type", keys[0], "%v", keys)
		requireCodexFieldOrder(t, keys, codexWantWSCreateOrder)
		requireCodexWSStreamRequestStart(t, frame, "")
		return
	}
	// 非双开钉住既有形态：wsjson 的 json.Encoder 转义 + 尾部换行，字节不变。
	require.Contains(t, string(frame), `\u003c`, "未开投影维持既有编码：%s", frame)
	require.True(t, strings.HasSuffix(string(frame), "\n"), "未开投影维持既有换行：%q", frame)
	require.False(t, gjson.GetBytes(frame, "client_metadata."+codexWSStreamRequestStartKey).Exists())
}

// 真上游连接上的帧字节：ctx_pool ingress 与 HTTP→WS v2（含预热帧）都经 lease 写出，
// 双开必须绕开 wsjson 的 json.Encoder（HTML 转义 + 换行），与 passthrough 的原始写同形。
func TestCodexDeviceWireProfileWSFrameBytesOnTheWire(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const marker = "a <b> & c"
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("ingress/enabled=%v", enabled), func(t *testing.T) {
			upstream := newCodexWSRealUpstream(t)
			dialer := &codexWSRealDialer{upstream: upstream}
			cfg := codexWSWireProfileConfig()
			svc := codexWSWireProfileService(cfg)
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			svc.openaiWSPool = pool
			account := wireProfileTestAccount(enabled)
			account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
			frame, err := sjson.Set(codexWSTestFrame, "instructions", marker)
			require.NoError(t, err)
			runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{frame})
			frames := upstream.Frames()
			require.Len(t, frames, 1)
			requireCodexWSWireBytes(t, frames[0], enabled, marker)
		})
		t.Run(fmt.Sprintf("v2+prewarm/enabled=%v", enabled), func(t *testing.T) {
			upstream := newCodexWSRealUpstream(t)
			dialer := &codexWSRealDialer{upstream: upstream}
			cfg := codexWSWireProfileConfig()
			cfg.Gateway.OpenAIWS.PrewarmGenerateEnabled = true
			svc := codexWSWireProfileService(cfg)
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			svc.openaiWSPool = pool
			account := wireProfileTestAccount(enabled)
			account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
			body, err := sjson.SetBytes(wireProfileTestBody(t), "instructions", marker)
			require.NoError(t, err)
			c := newConvTestContext(t, body)
			c.Request.URL.Path = "/v1/responses"
			result, err := svc.Forward(context.Background(), c, account, body)
			require.NoError(t, err)
			require.NotNil(t, result)
			frames := upstream.Frames()
			require.Len(t, frames, 2, "预热帧 + 正式帧")
			require.Equal(t, "false", gjson.GetBytes(frames[0], "generate").Raw, "首帧是 generate=false 的预热帧：%s", frames[0])
			require.False(t, gjson.GetBytes(frames[1], "generate").Exists())
			for _, raw := range frames {
				requireCodexWSWireBytes(t, raw, enabled, marker)
			}
		})
	}
}

// 客户端在 WS 上持有的 turn-state 只来自被转发的 response.metadata 事件，铸造账号必须在
// 下行边界记录：不记，纯 WS 会话的回声守卫永远查不到、等于空转（codex-api/src/sse/
// responses.rs:219-226 → endpoint/responses_websocket.rs:764-767）。
func TestCodexWSTurnStateProvenanceFromRelayedMetadataEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, ingress := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		t.Run(ingress, func(t *testing.T) {
			blob := "blob-from-event-" + ingress
			metadata := []byte(`{"type":"response.metadata","headers":{"X-Codex-Turn-State":"` + blob + `"}}`)
			upstream := &openAIWSCaptureConn{events: [][]byte{metadata, codexWSCompletedEvent("resp_1")}}
			dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{upstream}, handshake: http.Header{}}
			cfg := codexWSWireProfileConfig()
			svc := codexWSWireProfileService(cfg)
			if ingress == OpenAIWSIngressModePassthrough {
				svc.openaiWSPassthroughDialer = dialer
			} else {
				pool := newOpenAIWSConnPool(cfg)
				pool.setClientDialerForTest(dialer)
				svc.openaiWSPool = pool
			}
			account := wireProfileTestAccount(true)
			account.Extra["openai_oauth_responses_websockets_v2_mode"] = ingress

			runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{codexWSTestFrame})

			// "异账号"= 异凭证域：溯源按凭证域记，同一 ChatGPT 账号的另一本地行不算异账号。
			other := wireProfileTestAccount(true)
			other.ID = account.ID + 1
			other.Credentials = map[string]any{"access_token": "offline-token-b", "chatgpt_account_id": "other-account"}
			require.Empty(t, svc.guardOpenAICodexTurnStateValue(nil, other, blob),
				"转发给客户端的事件里那个 blob 必须记在本账号名下，异账号回带时才剥得掉")
			require.Equal(t, blob, svc.guardOpenAICodexTurnStateValue(nil, account, blob))
		})
	}
}

// 会话存储是 turn-state 的第二条跨账号载体：BindSessionTurnState 只按 (groupID, sessionHash)
// 键控、不含 accountID，换号后同一下游会话取回的可能是别的账号铸的 blob。取值点必须过守卫。
func TestCodexWSTurnStateEchoGuardIngressStateStoreFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	connA := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_1")}}
	connB := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_2")}}
	dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{connA, connB}, handshake: http.Header{}}
	dialer.handshake.Set(openAICodexTurnStateHeader, "minted-A")
	cfg := codexWSWireProfileConfig()
	svc := codexWSWireProfileService(cfg)
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)
	svc.openaiWSPool = pool

	// 未开投影的账号：turn-state 走握手头，跨账号泄漏在握手上直接可见。
	accountA := wireProfileTestAccount(false)
	accountA.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
	runCodexWSIngress(t, svc, accountA, codexWSIngressInbound(), []string{codexWSTestFrame})
	require.Len(t, dialer.Headers(), 1)

	// 正控：A 这一轮确实把 blob 写进了会话存储，且溯源记到了 A 名下。
	hashCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	hashCtx.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	for name, values := range codexWSIngressInbound() {
		hashCtx.Request.Header[name] = values
	}
	sessionHash := svc.GenerateSessionHash(hashCtx, []byte(codexWSTestFrame))
	saved, ok := svc.getOpenAIWSStateStore().GetSessionTurnState(0, sessionHash)
	require.True(t, ok, "会话存储里应有 A 铸出的 turn-state（否则本用例没覆盖到回落路径）")
	require.Equal(t, "minted-A", saved)
	require.Equal(t, "minted-A", svc.guardOpenAICodexTurnStateValue(nil, accountA, "minted-A"))

	// 换号：同一下游会话（同 session-id、同帧 → 同 sessionHash），账号 B。
	dialer.handshake = http.Header{}
	accountB := wireProfileTestAccount(false)
	accountB.ID = accountA.ID + 1
	accountB.Credentials = map[string]any{"access_token": "offline-token-b", "chatgpt_account_id": "other-account"}
	accountB.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
	require.Empty(t, svc.guardOpenAICodexTurnStateValue(nil, accountB, "minted-A"), "A 铸的 blob 对 B 必须判为异账号")
	runCodexWSIngress(t, svc, accountB, codexWSIngressInbound(), []string{codexWSTestFrame})

	headers := dialer.Headers()
	require.Len(t, headers, 2, "换号必须重新拨号")
	require.Empty(t, headers[1].Get(openAICodexTurnStateHeader),
		"会话存储里 A 铸的 blob 不得随 B 的握手出站")
}

// 连接复用：第二个会话拿到池里已握手（铸出 minted-1）的连接。真客户端的 turn_state 是每轮
// 新建的 OnceLock（core/src/client.rs:522-526 + session/turn.rs:175），复用同一条物理连接开
// 新一轮时首帧不带该键——core/tests/suite/turn_state.rs:140 断言只握手一次、:152 断言四个帧的
// turn-state 依次是 [null, null, "ts-1", null]，最后那个 null 正是新一轮的首帧。
func TestCodexDeviceWireProfileWSIngressReusedConnTurnState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			upstream := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_1"), codexWSCompletedEvent("resp_2")}}
			dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{upstream}, handshake: http.Header{}}
			dialer.handshake.Set(openAICodexTurnStateHeader, "minted-1")
			cfg := codexWSWireProfileConfig()
			svc := codexWSWireProfileService(cfg)
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			svc.openaiWSPool = pool
			account := wireProfileTestAccount(enabled)
			account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
			runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{codexWSTestFrame})
			// 第二个会话换一个下游会话（不同 session-id → 不同 sessionHash）：会话存储里没有它的
			// turn-state，客户端也没带，所以帧内必须没有该键。
			second := codexWSIngressInbound()
			second.Set("session-id", "S2")
			second.Set(openAIWSTurnMetadataHeader, `{"session_id":"S2","thread_id":"S2"}`)
			secondFrame, err := sjson.Set(codexWSTestFrame, "client_metadata.session_id", "S2")
			require.NoError(t, err)
			runCodexWSIngress(t, svc, account, second, []string{secondFrame})

			require.Len(t, dialer.Headers(), 1, "第二个会话必须复用池里的连接，不再拨号")
			require.Len(t, upstream.rawWrites, 2)
			require.False(t, gjson.GetBytes(upstream.rawWrites[0], "client_metadata."+openAICodexTurnStateHeader).Exists(), "首个会话的首帧在握手之前构造")
			require.False(t, gjson.GetBytes(upstream.rawWrites[1], "client_metadata."+openAICodexTurnStateHeader).Exists(),
				"复用连接开新一轮：真客户端首帧不带 turn-state（turn_state.rs:152）：%s", upstream.rawWrites[1])
		})
	}
}

// 同一连接上的重试（上游 400 拒绝字段 → 网关剥掉字段后在同一个 lease 上重发）：重发帧同样
// 不带网关握手铸出的 turn-state（客户端拿不到那个值），时间戳则在发送边界重盖。
func TestCodexDeviceWireProfileWSIngressRejectedFieldRetryTurnState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rejected := []byte(`{"type":"error","status":400,"error":{"code":"unknown_parameter","message":"Unknown parameter: 'input[0].namespace'.","param":"input[0].namespace"}}`)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			upstream := &openAIWSCaptureConn{events: [][]byte{rejected, codexWSCompletedEvent("resp_1")}}
			dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{upstream}, handshake: http.Header{}}
			dialer.handshake.Set(openAICodexTurnStateHeader, "minted-1")
			cfg := codexWSWireProfileConfig()
			svc := codexWSWireProfileService(cfg)
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			svc.openaiWSPool = pool
			account := wireProfileTestAccount(enabled)
			account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
			// 只有工具调用项上的 namespace 才会被剥掉重试（removeOpenAIResponsesRejectedNamespaceAtIndex）。
			frame, err := sjson.Set(codexWSTestFrame, "input.0", map[string]any{
				"type": "function_call", "call_id": "call_1", "name": "shell", "arguments": "{}", "namespace": "x",
			})
			require.NoError(t, err)
			runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{frame})

			require.Len(t, dialer.Headers(), 1, "重试不重新拨号")
			require.Len(t, upstream.rawWrites, 2, "原帧 + 剥掉被拒字段后的重发帧")
			require.True(t, gjson.GetBytes(upstream.rawWrites[0], "input.0.namespace").Exists())
			require.False(t, gjson.GetBytes(upstream.rawWrites[1], "input.0.namespace").Exists(), "重发帧已剥掉被拒字段：%s", upstream.rawWrites[1])
			require.False(t, gjson.GetBytes(upstream.rawWrites[0], "client_metadata."+openAICodexTurnStateHeader).Exists())
			require.False(t, gjson.GetBytes(upstream.rawWrites[1], "client_metadata."+openAICodexTurnStateHeader).Exists(),
				"重发帧不带网关铸出的 turn-state：%s", upstream.rawWrites[1])
			if enabled {
				for i, raw := range upstream.rawWrites {
					requireCodexWSStreamRequestStart(t, raw, "")
					require.NotEmpty(t, gjson.GetBytes(raw, "client_metadata."+codexWSStreamRequestStartKey).Str, "帧 %d", i)
				}
			}
		})
	}
}

// codexWSPacedConn：每收到一帧才放出一个事件——真上游不会在下一帧到达之前先发下一轮终态，
// 而 openAIWSCaptureConn 会把预置事件一口气读完（passthrough 的上游读循环会因此提前收到
// 第二轮终态并在客户端第二帧到达前退出）。
type codexWSPacedConn struct {
	*openAIWSCaptureConn
	tokens    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func newCodexWSPacedConn(events ...[]byte) *codexWSPacedConn {
	return &codexWSPacedConn{
		openAIWSCaptureConn: &openAIWSCaptureConn{events: events},
		tokens:              make(chan struct{}, 64),
		done:                make(chan struct{}),
	}
}

func (c *codexWSPacedConn) WriteJSON(ctx context.Context, value any) error {
	err := c.openAIWSCaptureConn.WriteJSON(ctx, value)
	if err == nil {
		c.tokens <- struct{}{}
	}
	return err
}

func (c *codexWSPacedConn) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	err := c.openAIWSCaptureConn.WriteFrame(ctx, msgType, payload)
	if err == nil {
		c.tokens <- struct{}{}
	}
	return err
}

func (c *codexWSPacedConn) wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-c.tokens:
		return nil
	case <-c.done:
		return errOpenAIWSConnClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *codexWSPacedConn) ReadMessage(ctx context.Context) ([]byte, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return c.openAIWSCaptureConn.ReadMessage(ctx)
}

// ReadFrame：passthrough 走帧接口（内嵌类型的 ReadFrame 会绕过上面的节奏控制）。
func (c *codexWSPacedConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	if err := c.wait(ctx); err != nil {
		return coderws.MessageText, nil, err
	}
	return c.openAIWSCaptureConn.ReadFrame(ctx)
}

func (c *codexWSPacedConn) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return c.openAIWSCaptureConn.Close()
}

// ctx_pool 双开：同一下游会话再来一轮时，会话存储回落取回的 turn-state（上一轮握手铸出、客户端
// 从未收到）只用于守卫/存储，不得进帧——真客户端新一轮首帧不带 turn-state（turn_state.rs:152）。
func TestCodexDeviceWireProfileWSIngressStateStoreFallbackNotInFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_1"), codexWSCompletedEvent("resp_2")}}
	dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{upstream}, handshake: http.Header{}}
	dialer.handshake.Set(openAICodexTurnStateHeader, "minted-1")
	cfg := codexWSWireProfileConfig()
	svc := codexWSWireProfileService(cfg)
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)
	svc.openaiWSPool = pool
	account := wireProfileTestAccount(true)
	account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool

	runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{codexWSTestFrame})

	// 正控：会话存储里确有上一轮握手铸出的值，第二轮的回落路径真的会取到它。
	hashCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	hashCtx.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	for name, values := range codexWSIngressInbound() {
		hashCtx.Request.Header[name] = values
	}
	saved, ok := svc.getOpenAIWSStateStore().GetSessionTurnState(0, svc.GenerateSessionHash(hashCtx, []byte(codexWSTestFrame)))
	require.True(t, ok, "会话存储里应有握手铸出的 turn-state（否则本用例没覆盖到回落路径）")
	require.Equal(t, "minted-1", saved)

	runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{codexWSTestFrame})

	require.Len(t, dialer.Headers(), 1, "同账号同会话复用池里的连接")
	require.Len(t, upstream.rawWrites, 2)
	require.False(t, gjson.GetBytes(upstream.rawWrites[1], "client_metadata."+openAICodexTurnStateHeader).Exists(),
		"会话存储回落值不得进帧：%s", upstream.rawWrites[1])
}
