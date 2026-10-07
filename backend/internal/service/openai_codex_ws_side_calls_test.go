//go:build unit

package service

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Drive the production WS entrypoints with a real coder/websocket upstream.
// Only the extra HTTP request is intercepted; no request can leave the offline runner.
func TestCodexSideCallsWebSocketCadence(t *testing.T) {
	for _, mode := range []string{"v2", OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		for _, enabled := range []bool{false, true} {
			for _, firstOnly := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/enabled=%v/first_only=%v", mode, enabled, firstOnly), func(t *testing.T) {
					upstream := newCodexWSRealUpstream(t)
					dialer := &codexWSRealDialer{upstream: upstream}
					cfg := codexWSWireProfileConfig()
					svc := codexWSWireProfileService(cfg)
					_, side := codexSideCallTestService()
					svc.httpUpstream, svc.codexSideCalls = side, newCodexSideCallState()
					if mode == OpenAIWSIngressModePassthrough {
						svc.openaiWSPassthroughDialer = dialer
					} else {
						pool := newOpenAIWSConnPool(cfg)
						pool.setClientDialerForTest(dialer)
						svc.openaiWSPool = pool
					}
					account := wireProfileTestAccount(enabled)
					account.Extra[codexWireTimezoneExtraKey] = wireTimezoneTestExit
					expectedCalls, expectedFrames := 1, 2
					if firstOnly {
						expectedFrames = 1
					}
					if mode == "v2" {
						account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
						previousResponseID := ""
						for i := 0; i < expectedFrames; i++ {
							body := wireTimezoneTestBody(t)
							if previousResponseID != "" {
								var err error
								body, err = sjson.SetBytes(body, "previous_response_id", previousResponseID)
								require.NoError(t, err)
							}
							c := newConvTestContext(t, body)
							c.Request.URL.Path = "/v1/responses"
							result, err := svc.Forward(context.Background(), c, account, body)
							require.NoError(t, err)
							require.NotNil(t, result)
							require.NotEmpty(t, result.ResponseID)
							previousResponseID = result.ResponseID
							if i > 0 {
								require.True(t, c.GetBool(OpsOpenAIWSConnReusedKey))
							}
						}
					} else {
						account.Extra["openai_oauth_responses_websockets_v2_mode"] = mode
						frame, err := sjson.SetRaw(codexWSTestFrame, "input",
							gjson.GetBytes(wireTimezoneTestBody(t), "input").Raw)
						require.NoError(t, err)
						frame, err = sjson.Set(frame, "client_metadata.thread_id", "S")
						require.NoError(t, err)
						// 无 PCK 的后续帧更换 thread，不会重建握手头；必须读这次真正发送的帧。
						next, err := sjson.Set(frame, "client_metadata.thread_id", "T")
						require.NoError(t, err)
						if firstOnly {
							runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{frame})
						} else {
							runCodexWSIngress(t, svc, account, codexWSIngressInbound(), []string{frame, frame, next})
							expectedCalls, expectedFrames = 2, 3
						}
					}

					frames := upstream.Frames()
					require.Len(t, frames, expectedFrames)
					for _, frame := range frames {
						env := gjson.GetBytes(frame, "input.0.content.0.text").Str
						if enabled {
							require.Contains(t, env, "<timezone>"+wireTimezoneTestExit+"</timezone>")
							require.Contains(t, env, "<current_date>2026-02-28</current_date>")
						} else {
							require.Contains(t, env, "<timezone>Asia/Shanghai</timezone>")
						}
					}
					if !enabled {
						requireNoCodexSideCall(t, side)
						return
					}
					dialer.mu.Lock()
					handshake := cloneHeader(dialer.headers[0])
					connections := len(dialer.headers)
					dialer.mu.Unlock()
					require.Equal(t, 1, connections, "后续请求复用同一条真实 WS 连接")
					if mode != "v2" {
						if !firstOnly {
							require.NotEqual(t, gjson.GetBytes(frames[0], "client_metadata.thread_id").Str,
								gjson.GetBytes(frames[2], "client_metadata.thread_id").Str)
						}
					}
					for i := 0; i < expectedCalls; i++ {
						select {
						case req := <-side.ch:
							require.Equal(t, http.MethodGet, req.Method)
							require.Equal(t, chatGPTSettingsUserURL, req.URL.String())
							require.Nil(t, req.Body)
							for _, name := range []string{"authorization", "chatgpt-account-id", "user-agent"} {
								require.NotEmpty(t, handshake.Get(name), name)
								require.Equal(t, handshake.Get(name), req.Header.Get(name), name)
							}
							require.Empty(t, req.Header.Get("originator"))
							require.Empty(t, req.Header.Get("version"))
							require.Equal(t, codexWireTimezoneProxyURL(account), <-side.proxies)
						case <-time.After(3 * time.Second):
							t.Fatalf("WS 已发出 %d 帧，但只收到 %d/%d 条 settings/user", len(frames), i, expectedCalls)
						}
					}
					requireNoCodexSideCall(t, side)

					// 同一条最终线程接着走 HTTP 时仍命中同一去重表。
					lastThread := gjson.GetBytes(frames[len(frames)-1], "client_metadata.thread_id").Str
					require.NotEmpty(t, lastThread)
					c := newConvTestContext(t, wireProfileTestBody(t))
					svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest(lastThread))
					requireNoCodexSideCall(t, side)
				})
			}
		}
	}
}

func TestCodexWSSideCallsRequireARequestAndAStringThread(t *testing.T) {
	svc, side := codexSideCallTestService()
	account := wireProfileTestAccount(true)
	c := newConvTestContext(t, wireProfileTestBody(t))
	headers := codexSideCallTestRequest("handshake-thread").Header
	for _, frame := range []string{
		`{"type":"session.update","client_metadata":{"thread_id":"T"}}`,
		`{"type":"response.cancel","client_metadata":{"thread_id":"T"}}`,
		`{"client_metadata":{"thread_id":"T"}}`,
		`{"type":"response.create","client_metadata":{"thread_id":123}}`,
		`{"type":"response.create","client_metadata":{"thread_id":""}}`,
		`{"type":"response.create","client_metadata":{"thread_id":"  "}}`,
	} {
		svc.scheduleCodexWSSideCalls(c, account, headers, []byte(frame))
		require.Zero(t, svc.codexSideCalls.threadSeen.ItemCount(), frame)
	}
	requireNoCodexSideCall(t, side)

	svc.scheduleCodexWSSideCalls(c, account, headers, []byte(`{"type":"response.create"}`))
	require.NotNil(t, collectCodexSideCalls(t, side, 1)[chatGPTSettingsUserURL])
	// 缺少帧内线程才允许握手兜底；同线程 HTTP 接着来不会重复发。
	svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest("handshake-thread"))
	requireNoCodexSideCall(t, side)

	// 同线程换账号应独立触发，不能把另一台设备的首次查询吞掉。
	other := *account
	other.ID++
	svc.scheduleCodexWSSideCalls(c, &other, headers, []byte(`{"type":"response.create"}`))
	require.NotNil(t, collectCodexSideCalls(t, side, 1)[chatGPTSettingsUserURL])
}

func TestCodexWSSideCallsRefreshAgentIdentityAuthentication(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	account := wireProfileTestAccount(true)
	account.Credentials = map[string]any{
		"auth_mode": OpenAIAuthModeAgentIdentity, "agent_runtime_id": key.runtimeID,
		"agent_private_key": privateKey, "task_id": key.taskID, "chatgpt_account_id": "offline-account",
	}
	svc, side := codexSideCallTestService()
	svc.accountRepo = &agentIdentityForwardRepo{account: account}
	c := newConvTestContext(t, wireProfileTestBody(t))
	headers := codexSideCallTestRequest("agent-thread").Header
	// 与池化 WS 的构造器相同：它没有 assertion，只有真正拨号时才通过 factory 注入。
	headers.Del("authorization")
	svc.scheduleCodexWSSideCalls(c, account, headers, []byte(`{"type":"response.create"}`))

	req := collectCodexSideCalls(t, side, 1)[chatGPTSettingsUserURL]
	require.NotNil(t, req)
	require.Equal(t, key.taskID, decodeAgentAssertionTask(t, req.Header.Get("authorization")))
	require.NotContains(t, req.Header.Get("authorization"), privateKey)
	require.Equal(t, headers.Get("user-agent"), req.Header.Get("user-agent"))
	require.Empty(t, headers.Get("authorization"), "不得反写原握手头")
}

func TestCodexWSSideCallsRetryAfterAuthenticationFailure(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	account := wireProfileTestAccount(true)
	account.Credentials = map[string]any{
		"auth_mode": OpenAIAuthModeAgentIdentity, "agent_runtime_id": key.runtimeID,
		"agent_private_key": "invalid-offline-key", "task_id": key.taskID,
	}
	svc, side := codexSideCallTestService()
	svc.accountRepo = &agentIdentityForwardRepo{account: account}
	c := newConvTestContext(t, wireProfileTestBody(t))
	headers := codexSideCallTestRequest("retry-thread").Header
	headers.Del("authorization")
	svc.scheduleCodexWSSideCalls(c, account, headers, []byte(`{"type":"response.create"}`))
	require.Eventually(t, func() bool {
		_, exists := svc.codexSideCalls.threadSeen.Get(codexSideThreadKey(account.ID, "retry-thread"))
		return !exists
	}, time.Second, time.Millisecond, "发送前认证失败不得吞掉后续两小时的重试")
	requireNoCodexSideCall(t, side)

	account.Credentials["agent_private_key"] = privateKey
	// 下一轮即使改走 HTTP，也应能重试；成功后重新进入共享去重窗口。
	svc.scheduleCodexSideCalls(c, account, codexSideCallTestRequest("retry-thread"))
	req := collectCodexSideCalls(t, side, 1)[chatGPTSettingsUserURL]
	require.Equal(t, key.taskID, decodeAgentAssertionTask(t, req.Header.Get("authorization")))
	svc.scheduleCodexWSSideCalls(c, account, headers, []byte(`{"type":"response.create"}`))
	requireNoCodexSideCall(t, side)
}

func TestCodexWSSideCallsPrewarmSharesDedup(t *testing.T) {
	upstream := newCodexWSRealUpstream(t)
	dialer := &codexWSRealDialer{upstream: upstream}
	cfg := codexWSWireProfileConfig()
	cfg.Gateway.OpenAIWS.PrewarmGenerateEnabled = true
	svc := codexWSWireProfileService(cfg)
	_, side := codexSideCallTestService()
	svc.httpUpstream, svc.codexSideCalls = side, newCodexSideCallState()
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)
	svc.openaiWSPool = pool
	account := wireProfileTestAccount(true)
	account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
	body := wireTimezoneTestBody(t)
	c := newConvTestContext(t, body)
	c.Request.URL.Path = "/v1/responses"
	result, err := svc.Forward(context.Background(), c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	frames := upstream.Frames()
	require.Len(t, frames, 2, "预热帧和正式帧")
	require.Equal(t, "false", gjson.GetBytes(frames[0], "generate").Raw)
	require.NotNil(t, collectCodexSideCalls(t, side, 1)[chatGPTSettingsUserURL])
	requireNoCodexSideCall(t, side)
}
