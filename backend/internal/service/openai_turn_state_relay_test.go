//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// turnStateRelayUpstream 按调用顺序回放：minted[i] 是第 i 发响应头里的新票（"" = 不给），
// status[i] 缺省 200，bodies[i] 缺省正常收尾的 SSE。sent 记每一发实际带出去的票。
type turnStateRelayUpstream struct {
	HTTPUpstream
	minted []string
	status []int
	bodies []string
	sent   []string
}

func (u *turnStateRelayUpstream) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	i := len(u.sent)
	u.sent = append(u.sent, r.Header.Get(openAICodexTurnStateHeader))
	minted := ""
	if i < len(u.minted) {
		minted = u.minted[i]
	}
	body := qualityGuardCompleted
	if i < len(u.bodies) && u.bodies[i] != "" {
		body = u.bodies[i]
	}
	resp := qualityGuardResponse(minted, body)
	if i < len(u.status) && u.status[i] != 0 {
		resp.StatusCode = u.status[i]
	}
	return resp, nil
}

var turnStateRelayModelBody = []byte(`{"model":"gpt-5.5"}`)

func turnStateRelayAccount() *Account {
	account := convTestAccount(false)
	account.Credentials["access_token"] = "offline-token"
	account.Extra[openAITurnStateRelayEnabledKey] = true
	return account
}

func turnStateRelayStates(svc *OpenAIGatewayService) []string {
	var states []string
	svc.openaiTurnStateRelay.Range(func(_, value any) bool {
		states = append(states, value.(openAITurnStateRelayEntry).state)
		return true
	})
	return states
}

// turnStateRelaySend 走「构造器里的接力 → 传输边界收票」这一段，不经过响应处理。
func turnStateRelaySend(t *testing.T, svc *OpenAIGatewayService, c *gin.Context, account *Account, clientState string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(`{"model":"gpt-5.5"}`))
	require.NoError(t, err)
	if clientState != "" {
		req.Header.Set(openAICodexTurnStateHeader, clientState)
	}
	req = svc.applyOpenAITurnStateRelay(c, account, req, []byte(`{"model":"gpt-5.5"}`), convTestSession)
	resp, err := svc.doOpenAIUpstream(req, "", account)
	require.NoError(t, err)
	turnStateRelayDrain(resp)
	return req
}

// turnStateRelayRoundTrip 发一发到 doOpenAIUpstream，响应原样交回，读不读由调用方决定。
func turnStateRelayRoundTrip(t *testing.T, svc *OpenAIGatewayService, c *gin.Context, account *Account, model, clientState string) *http.Response {
	t.Helper()
	body := `{"model":"` + model + `"}`
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(body))
	require.NoError(t, err)
	if clientState != "" {
		req.Header.Set(openAICodexTurnStateHeader, clientState)
	}
	req = svc.applyOpenAITurnStateRelay(c, account, req, []byte(body), convTestSession)
	resp, err := svc.doOpenAIUpstream(req, "", account)
	require.NoError(t, err)
	return resp
}

func turnStateRelayDrain(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// turnStateRelayNext 返回同会话下一发会带出去的接力票（只走构造器里的接力，不发出去）。
func turnStateRelayNext(t *testing.T, svc *OpenAIGatewayService, c *gin.Context, account *Account, model string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	return svc.applyOpenAITurnStateRelay(c, account, req, []byte(`{"model":"`+model+`"}`), convTestSession).Header.Get(openAICodexTurnStateHeader)
}

// 票绑模型，跨模型回带是 400：同一会话换模型不带另一个模型的票。
func TestTurnStateRelayKeepsModelsApart(t *testing.T) {
	account := turnStateRelayAccount()
	c := newConvTestContext(t, nil)
	svc := &OpenAIGatewayService{httpUpstream: &turnStateRelayUpstream{minted: []string{"sol-A"}}}
	turnStateRelayDrain(turnStateRelayRoundTrip(t, svc, c, account, "gpt-6-sol", ""))
	require.Equal(t, "sol-A", turnStateRelayNext(t, svc, c, account, "gpt-6-sol"))
	require.Empty(t, turnStateRelayNext(t, svc, c, account, "gpt-5.5"), "换模型不带别的模型的票")
}

// 上游先回 200 带新票、随后流失败时，那张票之后从不被认（2026-10-20 pro1 实测）：新票等流以
// response.completed 收尾才接；所发的旧票被换掉了，立即丢。
func TestTurnStateRelayRecordsMintedStateOnlyAfterCompletion(t *testing.T) {
	const failed = "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n"
	readAll := func(r io.Reader) error { _, err := io.Copy(io.Discard, r); return err }
	oneByte := func(r io.Reader) error { _, err := io.Copy(io.Discard, iotest.OneByteReader(r)); return err }
	chunked := func(r io.Reader) error {
		buf := make([]byte, 97)
		for {
			if _, err := r.Read(buf); err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
		}
	}
	longDelta := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"" + strings.Repeat("x", 5000) + "\"}\n\n"
	for _, tc := range []struct {
		name string
		body string
		read func(io.Reader) error
		want string
	}{
		{"completed", qualityGuardCompleted, readAll, "B"},
		{"completed_split_reads", "event: response.completed\r\n" + qualityGuardCompleted, oneByte, "B"},
		{"completed_event_line", "event: response.completed\ndata: {}\n\n", readAll, "B"},
		{"completed_after_long_lines", longDelta + longDelta + qualityGuardCompleted, chunked, "B"},
		{"failed", failed, readAll, ""},
		{"closed_before_end", qualityGuardCompleted, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := turnStateRelayAccount()
			c := newConvTestContext(t, nil)
			up := &turnStateRelayUpstream{minted: []string{"A", "B"}, bodies: []string{"", tc.body}}
			svc := &OpenAIGatewayService{httpUpstream: up}
			turnStateRelayDrain(turnStateRelayRoundTrip(t, svc, c, account, "gpt-5.5", ""))
			resp := turnStateRelayRoundTrip(t, svc, c, account, "gpt-5.5", "")
			require.Equal(t, []string{"", "A"}, up.sent)
			require.Empty(t, turnStateRelayNext(t, svc, c, account, "gpt-5.5"), "A 已被换掉；B 还没收尾")
			if tc.read != nil {
				require.NoError(t, tc.read(resp.Body))
			}
			_ = resp.Body.Close()
			require.Equal(t, tc.want, turnStateRelayNext(t, svc, c, account, "gpt-5.5"))
		})
	}

	// 不看 Content-Type：OAuth 上游可能不带它却照发 SSE；见过 SSE 行就只认 response.completed。
	for _, tc := range []struct {
		name, contentType, body, want string
	}{
		{"json_read_to_end", "application/json", `{"status":"completed"}`, "B"},
		{"sse_without_content_type_failed", "", failed, ""},
		{"sse_without_content_type_completed", "", qualityGuardCompleted, "B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := turnStateRelayAccount()
			c := newConvTestContext(t, nil)
			svc := &OpenAIGatewayService{}
			req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
			require.NoError(t, err)
			req = svc.applyOpenAITurnStateRelay(c, account, req, turnStateRelayModelBody, convTestSession)
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(tc.body))}
			if tc.contentType != "" {
				resp.Header.Set("Content-Type", tc.contentType)
			}
			resp.Header.Set(openAICodexTurnStateHeader, "B")
			svc.settleOpenAITurnStateRelay(req, resp)
			require.Empty(t, turnStateRelayNext(t, svc, c, account, "gpt-5.5"))
			turnStateRelayDrain(resp)
			require.Equal(t, tc.want, turnStateRelayNext(t, svc, c, account, "gpt-5.5"))
		})
	}

	t.Run("echoed_same_state_is_not_new", func(t *testing.T) {
		account := turnStateRelayAccount()
		c := newConvTestContext(t, nil)
		svc := &OpenAIGatewayService{}
		svc.storeOpenAITurnStateRelay(openAITurnStateRelayKey(c, account, "gpt-5.5", convTestSession), "A")
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
		require.NoError(t, err)
		req = svc.applyOpenAITurnStateRelay(c, account, req, turnStateRelayModelBody, convTestSession)
		resp := qualityGuardResponse("A", failed)
		svc.settleOpenAITurnStateRelay(req, resp)
		turnStateRelayDrain(resp)
		require.Equal(t, "A", turnStateRelayNext(t, svc, c, account, "gpt-5.5"), "上游回显同一张不算换票")
	})
}

// 收尾晚到的旧票不能冲掉并发请求已接上的新票（客户端断开后网关仍读完上游流计费）。
func TestTurnStateRelayLateCompletionKeepsNewerState(t *testing.T) {
	account := turnStateRelayAccount()
	c := newConvTestContext(t, nil)
	svc := &OpenAIGatewayService{}
	older := turnStateFernetBlob(time.Now().Add(-10*time.Minute), 10)
	newer := turnStateFernetBlob(time.Now().Add(-time.Minute), 10)
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	req = svc.applyOpenAITurnStateRelay(c, account, req, turnStateRelayModelBody, convTestSession)
	resp := qualityGuardResponse(older, qualityGuardCompleted)
	svc.settleOpenAITurnStateRelay(req, resp)
	svc.storeOpenAITurnStateRelay(openAITurnStateRelayKey(c, account, "gpt-5.5", convTestSession), newer)
	turnStateRelayDrain(resp)
	require.Equal(t, newer, turnStateRelayNext(t, svc, c, account, "gpt-5.5"))
}

// 200 没换票：所发的票仍有效（客户端自带的也算），没有有效条目时接上；并发请求已换上更新的票时不倒退。
func TestTurnStateRelayAdoptsUnreplacedStateWithoutRollingBack(t *testing.T) {
	account := turnStateRelayAccount()
	c := newConvTestContext(t, nil)
	up := &turnStateRelayUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: up}
	turnStateRelayDrain(turnStateRelayRoundTrip(t, svc, c, account, "gpt-5.5", "client-own"))
	require.Equal(t, "client-own", turnStateRelayNext(t, svc, c, account, "gpt-5.5"), "上游认了客户端自带的票，接着带它")

	// R1 带着 client-own 发出；R2 在 R1 响应头回来之前换上了 B 并正常收尾。
	r1, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	r1 = svc.applyOpenAITurnStateRelay(c, account, r1, []byte(`{"model":"gpt-5.5"}`), convTestSession)
	require.Equal(t, "client-own", r1.Header.Get(openAICodexTurnStateHeader))
	up.minted = []string{"", "B"}
	turnStateRelayDrain(turnStateRelayRoundTrip(t, svc, c, account, "gpt-5.5", ""))
	svc.settleOpenAITurnStateRelay(r1, qualityGuardResponse("", qualityGuardCompleted))
	require.Equal(t, "B", turnStateRelayNext(t, svc, c, account, "gpt-5.5"), "迟到的 200 不能把 B 冲回 client-own")
}

// 上游约 60 分钟换票：留余量提前停带，免得组装时还有效、到上游已过期被换。
func TestTurnStateRelayStopsBeforeUpstreamExpiry(t *testing.T) {
	svc := &OpenAIGatewayService{}
	svc.storeOpenAITurnStateRelay("aging", turnStateFernetBlob(time.Now().Add(-57*time.Minute), 10))
	require.Empty(t, svc.loadOpenAITurnStateRelay("aging"))
	fresh := turnStateFernetBlob(time.Now().Add(-50*time.Minute), 10)
	svc.storeOpenAITurnStateRelay("fresh", fresh)
	require.Equal(t, fresh, svc.loadOpenAITurnStateRelay("fresh"))
}

// CPR 原样中继同样接力：CPR 回的票正常收尾后，下一发盖到 /v1/responses 上；其它端点与关着时不动。
func TestTurnStateRelayCoversCPRRawRelay(t *testing.T) {
	run := func(t *testing.T, enabled bool, endpoint string, rewrite ...func(*gin.Context)) []string {
		var mu sync.Mutex
		var sent []string
		svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			sent = append(sent, r.Header.Get(openAICodexTurnStateHeader))
			n := len(sent)
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set(openAICodexTurnStateHeader, fmt.Sprintf("cpr-%d", n))
			_, _ = io.WriteString(w, rawRelaySSE)
		})
		if enabled {
			account.Extra[openAITurnStateRelayEnabledKey] = true
		}
		body := []byte(`{"model":"gpt-5.5","stream":true,"input":"hi","prompt_cache_key":"pck-cpr"}`)
		for i := 0; i < 2; i++ {
			c, _, decoded := rawRelayTestContext(t, context.Background(), endpoint, body, "", nil)
			for _, apply := range rewrite {
				apply(c)
			}
			_, err := svc.Forward(context.Background(), c, account, decoded)
			require.NoError(t, err)
		}
		mu.Lock()
		defer mu.Unlock()
		return sent
	}
	require.Equal(t, []string{"ts-inbound", "cpr-1"}, run(t, true, "/v1/responses"), "客户端自带的也盖成 CPR 最近给的票")
	require.Equal(t, []string{"ts-inbound", "ts-inbound"}, run(t, false, "/v1/responses"), "关着时原样中继不动")
	require.Equal(t, []string{"ts-inbound", "ts-inbound"}, run(t, true, "/v1/responses/compact"), "只接力 Responses 推理")
	bodySignalCompact := func(c *gin.Context) { c.Request.URL.Path = "/v1/responses/compact" } // handler 按体信号改写，CPR 仍发原端点
	require.Equal(t, []string{"ts-inbound", "ts-inbound"}, run(t, true, "/v1/responses", bodySignalCompact), "体信号 compact 也不接力")

	// 混合分组里先试过 OAuth 号再换到 CPR：gin 上下文还暂存着 OAuth 的凭证身份，CPR 不能借它的票，
	// 也不能把 CPR 给的票记到 OAuth 名下。
	t.Run("after_oauth_failover", func(t *testing.T) {
		var sent string
		svc, account := rawRelayTestSetup(t, func(w http.ResponseWriter, r *http.Request) {
			sent = r.Header.Get(openAICodexTurnStateHeader)
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set(openAICodexTurnStateHeader, "cpr-minted")
			_, _ = io.WriteString(w, rawRelaySSE)
		})
		account.Extra[openAITurnStateRelayEnabledKey] = true
		oauth := turnStateRelayAccount()
		body := []byte(`{"model":"gpt-5.5","stream":true,"input":"hi","prompt_cache_key":"pck-cpr"}`)
		c, _, decoded := rawRelayTestContext(t, context.Background(), "/v1/responses", body, "", nil)
		_, err := svc.prepareCodexAccountIdentitySource(context.Background(), c, oauth)
		require.NoError(t, err)
		oauthKey := openAITurnStateRelayKey(c, oauth, "gpt-5.5", "pck-cpr")
		svc.storeOpenAITurnStateRelay(oauthKey, "oauth-state")

		_, err = svc.Forward(context.Background(), c, account, decoded)
		require.NoError(t, err)
		require.Equal(t, "ts-inbound", sent, "CPR 不带 OAuth 凭证域的票")
		require.Equal(t, "oauth-state", svc.loadOpenAITurnStateRelay(oauthKey), "CPR 的票不记到 OAuth 名下")
	})

	require.NoError(t, validateOpenAIQualityGuardExtra(newCPRTestAccount(), map[string]any{openAITurnStateRelayEnabledKey: true}))
	require.Error(t, validateOpenAIQualityGuardExtra(newCPRTestAccount(), map[string]any{openAIQualityGuardEnabledKey: true, openAIQualityGuardModeKey: qualityGuardModeBlock}),
		"降智防护仍只给本地 OAuth")
}

func TestTurnStateRelayCarriesLatestStateAcrossRequests(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "passthrough"}[passthrough], func(t *testing.T) {
			account := turnStateRelayAccount()
			account.Extra["openai_passthrough"] = passthrough
			body := wireProfileTestBody(t)
			svc, _ := wireProfileTestService()
			up := &turnStateRelayUpstream{minted: []string{"A", "B", "", "C"}}
			svc.httpUpstream = up
			var contexts []*gin.Context
			for i := 0; i < 4; i++ {
				c := newConvTestContext(t, body)
				if i == 3 {
					c.Request.Header.Set(openAICodexTurnStateHeader, "client-own")
				}
				contexts = append(contexts, c)
				// 只看出站与收票；回放的响应体不是完整的业务响应，响应处理的结果不在本测试范围。
				_, _ = svc.Forward(context.Background(), c, account, body)
			}
			require.Equal(t, []string{"", "A", "B", "B"}, up.sent, "发 A 回 B 带 B；上游没换票就继续带当前这张；客户端自带的也盖掉")
			require.Equal(t, "B", OpenAITurnStateUsageSent(contexts[3]), "用量表出站列记的是实际带出去的接力票")
			require.Equal(t, []string{"C"}, turnStateRelayStates(svc))
		})
	}
}

func TestTurnStateRelayDisabledLeavesOutboundUnchanged(t *testing.T) {
	account := turnStateRelayAccount()
	delete(account.Extra, openAITurnStateRelayEnabledKey)
	body := wireProfileTestBody(t)
	svc, _ := wireProfileTestService()
	up := &turnStateRelayUpstream{minted: []string{"A", "B"}}
	svc.httpUpstream = up
	for i := 0; i < 2; i++ {
		_, _ = svc.Forward(context.Background(), newConvTestContext(t, body), account, body)
	}
	require.Equal(t, []string{"", ""}, up.sent)
	require.Empty(t, turnStateRelayStates(svc), "关闭时不记票")
}

func TestTurnStateRelayIsolatesCredentialAPIKeyAndSession(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := turnStateRelayAccount()
	c := newConvTestContext(t, nil)
	svc.storeOpenAITurnStateRelay(openAITurnStateRelayKey(c, account, "gpt-5.5", convTestSession), "A")

	other := newConvTestContext(t, nil)
	other.Set("api_key", &APIKey{ID: 78})
	foreign := turnStateRelayAccount()
	foreign.ID, foreign.Credentials = 12, map[string]any{"chatgpt_account_id": "chatgpt-account-12", "chatgpt_user_id": "user-12"}
	for name, apply := range map[string]func(*http.Request) *http.Request{
		"api_key": func(r *http.Request) *http.Request {
			return svc.applyOpenAITurnStateRelay(other, account, r, turnStateRelayModelBody, convTestSession)
		},
		"credential": func(r *http.Request) *http.Request {
			return svc.applyOpenAITurnStateRelay(c, foreign, r, turnStateRelayModelBody, convTestSession)
		},
		"session": func(r *http.Request) *http.Request {
			return svc.applyOpenAITurnStateRelay(c, account, r, turnStateRelayModelBody, "other-session")
		},
		"no_session": func(r *http.Request) *http.Request {
			return svc.applyOpenAITurnStateRelay(c, account, r, turnStateRelayModelBody, "")
		},
		"no_model": func(r *http.Request) *http.Request {
			return svc.applyOpenAITurnStateRelay(c, account, r, []byte(`{"input":"hi"}`), convTestSession)
		},
	} {
		req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
		require.NoError(t, err)
		require.Empty(t, apply(req).Header.Get(openAICodexTurnStateHeader), name)
	}
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	require.Equal(t, "A", svc.applyOpenAITurnStateRelay(c, account, req, turnStateRelayModelBody, convTestSession).Header.Get(openAICodexTurnStateHeader))
}

func TestTurnStateRelaySkipsOutOfScopeRequests(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := turnStateRelayAccount()
	c := newConvTestContext(t, nil)
	svc.storeOpenAITurnStateRelay(openAITurnStateRelayKey(c, account, "gpt-5.5", convTestSession), "A")
	newReq := func(rawURL string) *http.Request {
		req, err := http.NewRequest(http.MethodPost, rawURL, nil)
		require.NoError(t, err)
		return req
	}
	apiKey := turnStateRelayAccount()
	apiKey.Type = AccountTypeAPIKey
	pool := turnStateRelayAccount()
	pool.Extra["openai_gwpool"] = true
	bridge := newConvTestContext(t, nil)
	bridge.Set("openai_ws_http_bridge", true)
	images := newReq(gwpoolTestURL)
	images = images.WithContext(withOpenAIImagesWireTarget(images.Context(), "https://chatgpt.com/backend-api/images"))
	for name, req := range map[string]*http.Request{
		"api_key_account": svc.applyOpenAITurnStateRelay(c, apiKey, newReq(gwpoolTestURL), turnStateRelayModelBody, convTestSession),
		"gateway_pool":    svc.applyOpenAITurnStateRelay(c, pool, newReq(gwpoolTestURL), turnStateRelayModelBody, convTestSession),
		"compact":         svc.applyOpenAITurnStateRelay(c, account, newReq(gwpoolTestURL+"/compact"), turnStateRelayModelBody, convTestSession),
		"ws_http_bridge":  svc.applyOpenAITurnStateRelay(bridge, account, newReq(gwpoolTestURL), turnStateRelayModelBody, convTestSession),
		"body_carrier":    svc.applyOpenAITurnStateRelay(c, account, newReq(gwpoolTestURL), []byte(`{"model":"gpt-5.5","client_metadata":{"x-codex-turn-state":"frame"}}`), convTestSession),
		"images":          svc.applyOpenAITurnStateRelay(c, account, images, turnStateRelayModelBody, convTestSession),
	} {
		require.Empty(t, req.Header.Get(openAICodexTurnStateHeader), name)
		require.Nil(t, req.Context().Value(openAITurnStateRelayRequestKey{}), name)
	}
}

func TestTurnStateRelayDropsRejectedStateOnly(t *testing.T) {
	account := turnStateRelayAccount()
	c := newConvTestContext(t, nil)
	key := openAITurnStateRelayKey(c, account, "gpt-5.5", convTestSession)

	up := &turnStateRelayUpstream{minted: []string{"", "", "fresh"}, status: []int{http.StatusTooManyRequests, http.StatusBadRequest}}
	svc := &OpenAIGatewayService{httpUpstream: up}
	svc.storeOpenAITurnStateRelay(key, "A")
	turnStateRelaySend(t, svc, c, account, "")
	require.Equal(t, "A", svc.loadOpenAITurnStateRelay(key), "429 不是票的问题")
	turnStateRelaySend(t, svc, c, account, "")
	require.Empty(t, svc.loadOpenAITurnStateRelay(key), "带着接力票被 400 拒绝就丢掉")
	turnStateRelaySend(t, svc, c, account, "")
	require.Equal(t, []string{"A", "A", ""}, up.sent, "丢掉后下一条不再盖票")
	require.Equal(t, "fresh", svc.loadOpenAITurnStateRelay(key), "上游重新给票后接着接力")

	// 400 时只丢这一发带出去的那张：并发请求已换上的新票保留。
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	req = svc.applyOpenAITurnStateRelay(c, account, req, turnStateRelayModelBody, convTestSession)
	svc.storeOpenAITurnStateRelay(key, "newer")
	svc.settleOpenAITurnStateRelay(req, &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{}})
	require.Equal(t, "newer", svc.loadOpenAITurnStateRelay(key))
}

func TestTurnStateRelayExpiresFromMintTime(t *testing.T) {
	svc := &OpenAIGatewayService{}
	svc.storeOpenAITurnStateRelay("stale", turnStateFernetBlob(time.Now().Add(-openAITurnStateRelayTTL-time.Minute), 10))
	require.Empty(t, svc.loadOpenAITurnStateRelay("stale"))
	_, ok := svc.openaiTurnStateRelay.Load("stale")
	require.False(t, ok, "过期条目读时删掉")

	live := turnStateFernetBlob(time.Now().Add(-time.Minute), 10)
	svc.storeOpenAITurnStateRelay("live", live)
	require.Equal(t, live, svc.loadOpenAITurnStateRelay("live"))

	// 解不出信封的票以第一次见到为准，重复记不续期。
	svc.openaiTurnStateRelay.Store("opaque", openAITurnStateRelayEntry{state: "opaque", expiresAt: time.Now().Add(-time.Second)})
	svc.storeOpenAITurnStateRelay("opaque", "opaque")
	require.Empty(t, svc.loadOpenAITurnStateRelay("opaque"))
}

func TestTurnStateRelaySweepPrunesExpiredEntries(t *testing.T) {
	svc := &OpenAIGatewayService{}
	svc.openaiTurnStateRelay.Store("expired", openAITurnStateRelayEntry{state: "x", expiresAt: time.Now().Add(-time.Second)})
	for i := 0; i < 256; i++ {
		svc.storeOpenAITurnStateRelay("live", strings.Repeat("s", i+1))
	}
	_, ok := svc.openaiTurnStateRelay.Load("expired")
	require.False(t, ok)
}

// 与降智防护同开：防护内部每一发（业务、确认、重发）都推进接力。
func TestTurnStateRelayAdvancesThroughQualityGuard(t *testing.T) {
	run := func(t *testing.T, mode string, up *turnStateRelayUpstream) (*OpenAIGatewayService, string, error) {
		svc, account, req := qualityGuardFixture(t, mode)
		account.Extra[openAITurnStateRelayEnabledKey] = true
		account.Extra[openAIQualityGuardIntervalKey] = 1
		c := newConvTestContext(t, nil)
		key := openAITurnStateRelayKey(c, account, "gpt-5.5", convTestSession)
		svc.httpUpstream = up
		svc.storeOpenAITurnStateRelay(key, "A")
		req = req.WithContext(withQualityGuardRequest(req.Context(), c, account, nil))
		req = svc.applyOpenAITurnStateRelay(c, account, req, turnStateRelayModelBody, convTestSession)
		resp, err := svc.doOpenAIUpstream(req, "", account)
		qualityGuardClose(resp)
		return svc, key, err
	}

	t.Run("block", func(t *testing.T) {
		up := &turnStateRelayUpstream{minted: []string{"B"}}
		svc, key, err := run(t, qualityGuardModeBlock, up)
		var failure *UpstreamFailoverError
		require.ErrorAs(t, err, &failure)
		require.Equal(t, []string{"A"}, up.sent)
		require.Empty(t, svc.loadOpenAITurnStateRelay(key), "被拦下的响应没收尾，B 不接；所发的 A 已被换掉，也不再带")
	})

	t.Run("wait", func(t *testing.T) {
		up := &turnStateRelayUpstream{minted: []string{"B", "C", "", ""}}
		svc, key, err := run(t, qualityGuardModeWait, up)
		require.NoError(t, err)
		require.Equal(t, []string{"A", "B", "C", "C"}, up.sent, "业务 A → 确认 B → 确认 C 稳定 → 带 C 重发")
		require.Equal(t, "C", svc.loadOpenAITurnStateRelay(key), "重发被上游认了才接上")
	})

	t.Run("wait_exhausted", func(t *testing.T) {
		up := &turnStateRelayUpstream{minted: []string{"B", "luna-C"}}
		svc, account, req := qualityGuardFixture(t, qualityGuardModeWait)
		account.Extra[openAITurnStateRelayEnabledKey] = true
		account.Extra[openAIQualityGuardAttemptsKey] = 1
		c := newConvTestContext(t, nil)
		key := openAITurnStateRelayKey(c, account, "gpt-6-astra", convTestSession)
		svc.httpUpstream = up
		svc.storeOpenAITurnStateRelay(key, "A")
		req = req.WithContext(withQualityGuardRequest(req.Context(), c, account, nil))
		req = svc.applyOpenAITurnStateRelay(c, account, req, []byte(`{"model":"gpt-6-astra"}`), convTestSession)
		resp, err := svc.doOpenAIUpstream(req, "", account)
		qualityGuardClose(resp)
		require.Error(t, err)
		require.Equal(t, []string{"A", "B"}, up.sent)
		require.Empty(t, svc.loadOpenAITurnStateRelay(key), "Luna 确认给的票不记到业务模型名下")
	})
}

func TestTurnStateRelayConfigValidation(t *testing.T) {
	account := convTestAccount(false)
	require.NoError(t, validateOpenAIQualityGuardExtra(account, map[string]any{openAITurnStateRelayEnabledKey: true}),
		"接力开关独立于降智防护，不要求选处理方式")
	require.NoError(t, validateOpenAIQualityGuardExtra(account, map[string]any{openAITurnStateRelayEnabledKey: false}))
	require.Error(t, validateOpenAIQualityGuardExtra(account, map[string]any{openAITurnStateRelayEnabledKey: "true"}))
	require.True(t, touchesOpenAIQualityGuardConfig(map[string]any{openAITurnStateRelayEnabledKey: true}))

	account.Type = AccountTypeAPIKey
	require.Error(t, validateOpenAIQualityGuardExtra(account, map[string]any{openAITurnStateRelayEnabledKey: true}))
	require.NoError(t, validateOpenAIQualityGuardExtra(account, map[string]any{openAITurnStateRelayEnabledKey: false}))
}
