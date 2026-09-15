//go:build unit

package service

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
	"github.com/tidwall/sjson"
)

// spark 影子行没有自己的凭据，出站身份与 turn-state 铸造者都按入口暂存的母账号算
// （prepareCodexAccountIdentitySource）。下面的用例驱动真正的生产入口，钉住每个记录点与守卫点
// 传的都是入口上下文：任何一处退回 nil，owner 就按影子行自身算，母账号身份下的合法回带会被剥掉。
// convergence=false 的变体覆盖非双开影子行：turn-state 走握手头，会话存储回落值也要过守卫。

func codexShadowTestRows(convergence bool) (parent, shadow, other *Account) {
	parent = wireProfileTestAccount(convergence)
	parent.Credentials["chatgpt_user_id"] = "offline-user"
	parentID := parent.ID
	shadow = wireProfileTestAccount(convergence)
	shadow.ID = parentID + 3
	shadow.ParentAccountID = &parentID
	shadow.Credentials = map[string]any{"model_mapping": map[string]any{}}
	other = wireProfileTestAccount(convergence)
	other.ID = parentID + 9
	other.Credentials = map[string]any{"access_token": "offline-token-b", "chatgpt_account_id": "other-account"}
	return parent, shadow, other
}

func codexShadowUpstreamResponse(stream bool, blob string) *http.Response {
	header := http.Header{}
	if blob != "" {
		header.Set(openAICodexTurnStateHeader, blob)
	}
	if !stream {
		header.Set("Content-Type", "application/json")
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(
			`{"id":"offline","object":"response","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`))}
	}
	header.Set("Content-Type", "text/event-stream")
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_ok"}}`, "",
		`data: {"type":"response.output_text.delta","delta":"hello"}`, "",
		`data: {"type":"response.completed","response":{"id":"resp_ok","usage":{"input_tokens":1,"output_tokens":1}}}`, "", "",
	}, "\n")
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

// HTTP 三个记录点（非流式 relay、首输出暂存提交、透传响应头）与出站头守卫。
func TestCodexTurnStateShadowRowHTTPOwnerIsParentIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name        string
		passthrough bool
		stream      bool
	}{
		{"forward/json", false, false},
		{"forward/sse", false, true},
		// OAuth 透传体被 normalizeOpenAIPassthroughOAuthBody 强制 stream=true，只有流式形态。
		{"passthrough/sse", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, shadow, other := codexShadowTestRows(true)
			shadow.Extra["openai_passthrough"] = tc.passthrough
			blob := "blob-shadow-" + strings.ReplaceAll(tc.name, "/", "-")
			up := &httpUpstreamRecorder{responses: []*http.Response{
				codexShadowUpstreamResponse(tc.stream, blob), codexShadowUpstreamResponse(tc.stream, ""),
			}}
			cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
			if tc.stream {
				// 首输出守卫开着，流式的记录点才是暂存提交（response_handling.go）。
				cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 1
			}
			svc := &OpenAIGatewayService{
				cfg: cfg, httpUpstream: up, toolCorrector: NewCodexToolCorrector(),
				responseHeaderFilter: compileResponseHeaderFilter(cfg),
				accountRepo:          &stubQuotaAccountRepo{accounts: map[int64]*Account{parent.ID: parent}},
			}
			body := wireProfileTestBody(t)
			if tc.stream {
				var err error
				body, err = sjson.SetBytes(body, "stream", true)
				require.NoError(t, err)
			}

			c1 := newConvTestContext(t, body)
			_, err := svc.Forward(context.Background(), c1, shadow, body)
			require.NoError(t, err)
			require.Len(t, up.requests, 1)
			require.Equal(t, blob, c1.Writer.Header().Get(openAICodexTurnStateHeader), "影子行铸出的 blob 交给了客户端")
			require.Equal(t, blob, svc.guardOpenAICodexTurnStateValue(nil, parent, blob), "记在母账号凭证域名下")
			require.Empty(t, svc.guardOpenAICodexTurnStateValue(nil, other, blob), "异凭证域剥离")

			c2 := newConvTestContext(t, body)
			c2.Request.Header.Set(openAICodexTurnStateHeader, blob)
			_, err = svc.Forward(context.Background(), c2, shadow, body)
			require.NoError(t, err)
			require.Len(t, up.requests, 2)
			require.Equal(t, blob, up.requests[1].Header.Get(openAICodexTurnStateHeader),
				"影子行回带自己（母账号身份）铸出的 blob 不得被剥")
		})
	}
}

// WS 入站（ctx_pool / passthrough）：握手铸出的 blob、response.metadata 事件里的 blob 都记在母账号
// 名下；第二轮的入站头守卫、帧内守卫、会话存储回落守卫都按母账号判定。双开下入站头与帧内是两个
// 独立载体（帧缺键时才由 clientTurnState 补），所以分 header-only / frame-only 两种形态，任一守卫退回
// nil 都不能被另一个载体掩盖。
func TestCodexTurnStateShadowRowWSIngressOwnerIsParentIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name        string
		mode        string
		convergence bool
		header      bool // 第二轮入站握手头回带 eventBlob
		frame       bool // 第二轮帧内 client_metadata 回带 eventBlob
		frames      int  // 第二轮帧数（透传适配器首帧与后续帧是两个守卫点）
	}{
		{"ctx_pool/on/header-only", OpenAIWSIngressModeCtxPool, true, true, false, 1},
		{"ctx_pool/on/frame-only", OpenAIWSIngressModeCtxPool, true, false, true, 1},
		{"ctx_pool/off/fallback", OpenAIWSIngressModeCtxPool, false, false, false, 1},
		{"passthrough/on/frame-only", OpenAIWSIngressModePassthrough, true, false, true, 2},
		{"passthrough/off/header", OpenAIWSIngressModePassthrough, false, true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, shadow, other := codexShadowTestRows(tc.convergence)
			shadow.Extra["openai_oauth_responses_websockets_v2_mode"] = tc.mode
			minted, eventBlob := "", ""
			var firstEvents [][]byte
			if tc.header || tc.frame {
				eventBlob = "blob-event-" + strings.ReplaceAll(tc.name, "/", "-")
				firstEvents = append(firstEvents, []byte(`{"type":"response.metadata","headers":{"X-Codex-Turn-State":"`+eventBlob+`"}}`))
			}
			firstEvents = append(firstEvents, codexWSCompletedEvent("resp_1"))
			first := &openAIWSCaptureConn{events: firstEvents}
			var secondEvents [][]byte
			for i := 0; i < tc.frames; i++ {
				secondEvents = append(secondEvents, codexWSCompletedEvent("resp_2"))
			}
			secondCapture := &openAIWSCaptureConn{events: secondEvents}
			var second openAIWSClientConn = secondCapture
			if tc.frames > 1 {
				// 多帧要按帧放事件，否则透传的上游读循环会在第二帧到达前读完终态退出。
				second = &codexWSPacedConn{openAIWSCaptureConn: secondCapture, tokens: make(chan struct{}, 64), done: make(chan struct{})}
			}
			dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{first, second}, handshake: http.Header{}}
			if tc.mode == OpenAIWSIngressModeCtxPool {
				// 握手铸造的记录点只在 ctx_pool（ingress.go）；透传适配器只记 response.metadata。
				minted = "minted-" + strings.ReplaceAll(tc.name, "/", "-")
				dialer.handshake.Set(openAICodexTurnStateHeader, minted)
			}
			cfg := codexWSWireProfileConfig()
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 0
			svc := codexWSWireProfileService(cfg)
			svc.accountRepo = &stubQuotaAccountRepo{accounts: map[int64]*Account{parent.ID: parent}}
			if tc.mode == OpenAIWSIngressModePassthrough {
				svc.openaiWSPassthroughDialer = dialer
			} else {
				pool := newOpenAIWSConnPool(cfg)
				pool.setClientDialerForTest(dialer)
				svc.openaiWSPool = pool
			}

			runCodexWSIngress(t, svc, shadow, codexWSIngressInbound(), []string{codexWSTestFrame})
			for _, blob := range []string{minted, eventBlob} {
				if blob == "" {
					continue
				}
				require.Equal(t, blob, svc.guardOpenAICodexTurnStateValue(nil, parent, blob), "%s 记在母账号名下", blob)
				require.Empty(t, svc.guardOpenAICodexTurnStateValue(nil, other, blob), "%s 异凭证域剥离", blob)
			}
			if tc.mode == OpenAIWSIngressModeCtxPool {
				// MaxIdlePerAccount=0 只关空闲池，挡不住会话→连接亲和（GetSessionConn 优先复用第一轮的
				// 连接）；复用连接自带的握手头会绕过会话存储回落。把它逐出池，第二轮必须重新握手，回落
				// 守卫与握手头才可见——去掉这一步，ingress 会话存储回落守卫置 nil 的变异（nilctx_ingress_
				// saved_guard）在 off/fallback 上存活（review/ROUND8.md 有复现记录）。
				hashCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
				hashCtx.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
				for name, values := range codexWSIngressInbound() {
					hashCtx.Request.Header[name] = values
				}
				sessionHash := svc.GenerateSessionHash(hashCtx, []byte(codexWSTestFrame))
				if minted != "" {
					saved, ok := svc.getOpenAIWSStateStore().GetSessionTurnState(0, sessionHash)
					require.True(t, ok, "会话存储里应有握手铸出的 turn-state")
					require.Equal(t, minted, saved)
				}
				connID, ok := svc.getOpenAIWSStateStore().GetSessionConn(0, sessionHash)
				require.True(t, ok, "第一轮应把会话绑定到上游连接")
				svc.getOpenAIWSConnPool().evictConn(shadow.ID, connID)
			}

			inbound := codexWSIngressInbound()
			frame := codexWSTestFrame
			want := minted // 回落：会话存储里是握手铸出的值
			if tc.header {
				want = eventBlob
				inbound.Set(openAICodexTurnStateHeader, eventBlob)
			}
			if tc.frame {
				want = eventBlob
				var err error
				frame, err = sjson.Set(codexWSTestFrame, "client_metadata."+openAICodexTurnStateHeader, eventBlob)
				require.NoError(t, err)
			}
			frames := make([]string, tc.frames)
			for i := range frames {
				frames[i] = frame
			}
			runCodexWSIngress(t, svc, shadow, inbound, frames)
			headers := dialer.Headers()
			require.Len(t, headers, 2, "第二轮新拨")
			require.Len(t, secondCapture.rawWrites, tc.frames)
			if tc.convergence {
				require.Empty(t, headers[1].Get(openAICodexTurnStateHeader), "双开握手不带 turn-state")
				for i, sent := range secondCapture.rawWrites {
					require.Equal(t, want, gjson.GetBytes(sent, "client_metadata."+openAICodexTurnStateHeader).String(),
						"影子行回带母账号身份下的 blob 不得被剥（第 %d 帧）：%s", i+1, sent)
				}
				return
			}
			require.Equal(t, want, headers[1].Get(openAICodexTurnStateHeader),
				"非双开影子行：握手承载母账号身份下的 blob，不得按影子行自身身份剥掉")
		})
	}
}

// HTTP→WS v2：握手铸出的 blob 经响应头交给客户端并记在母账号名下；回带（双开帧内 / 非双开握手）
// 与会话存储回落（非双开握手）都按母账号判定。
func TestCodexTurnStateShadowRowWSV2OwnerIsParentIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name        string
		convergence bool
		echo        bool
	}{
		{"on/echo", true, true},
		{"off/echo", false, true},
		{"off/fallback", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, shadow, other := codexShadowTestRows(tc.convergence)
			shadow.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
			minted := "minted-v2-" + strings.ReplaceAll(tc.name, "/", "-")
			first := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_v2_1")}}
			second := &openAIWSCaptureConn{events: [][]byte{codexWSCompletedEvent("resp_v2_2")}}
			dialer := &codexWSStagedDialer{conns: []openAIWSClientConn{first, second}, handshake: http.Header{}}
			dialer.handshake.Set(openAICodexTurnStateHeader, minted)
			cfg := codexWSWireProfileConfig()
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 0
			svc := codexWSWireProfileService(cfg)
			svc.accountRepo = &stubQuotaAccountRepo{accounts: map[int64]*Account{parent.ID: parent}}
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			svc.openaiWSPool = pool

			body := wireProfileTestBody(t)
			newCtx := func() *gin.Context {
				c := newConvTestContext(t, body)
				c.Request.URL.Path = "/v1/responses"
				return c
			}
			c1 := newCtx()
			result1, err := svc.Forward(context.Background(), c1, shadow, body)
			require.NoError(t, err)
			require.NotNil(t, result1)
			require.Equal(t, minted, c1.Writer.Header().Get(openAICodexTurnStateHeader), "铸出的 blob 交给了客户端")
			require.Equal(t, minted, svc.guardOpenAICodexTurnStateValue(nil, parent, minted), "v2 握手铸出的 blob 记在母账号名下")
			require.Empty(t, svc.guardOpenAICodexTurnStateValue(nil, other, minted))
			connID, hasConn := svc.getOpenAIWSStateStore().GetResponseConn(result1.RequestID)
			require.True(t, hasConn)
			svc.getOpenAIWSConnPool().evictConn(shadow.ID, connID)

			c2 := newCtx()
			if tc.echo {
				c2.Request.Header.Set(openAICodexTurnStateHeader, minted)
			}
			_, err = svc.Forward(context.Background(), c2, shadow, body)
			require.NoError(t, err)
			headers := dialer.Headers()
			require.Len(t, headers, 2)
			require.Len(t, second.rawWrites, 1)
			frameState := gjson.GetBytes(second.rawWrites[0], "client_metadata."+openAICodexTurnStateHeader)
			if tc.convergence {
				require.Empty(t, headers[1].Get(openAICodexTurnStateHeader), "双开握手不带 turn-state")
				require.Equal(t, minted, frameState.String(), "影子行回带：双开帧内承载，不得按影子行自身身份剥掉：%s", second.rawWrites[0])
				return
			}
			require.Equal(t, minted, headers[1].Get(openAICodexTurnStateHeader), "非双开影子行：握手承载（回带或会话存储回落），不得按影子行自身身份剥掉")
			require.False(t, frameState.Exists())
		})
	}
}
