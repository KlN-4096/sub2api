//go:build unit

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// 这一组测试从真入口打穿：gin 路由 → handler 归一化（compact 白名单、
// prompt_cache_key 种子暂存）→ 账号选择 → service 转发 → 假上游，断言真正出站的
// 头和体。service 包里的单元测试直接调 service 函数，看不到 handler 归一化那一段，
// 而 compact 的 prompt_cache_key 恰恰是在那里被丢掉的。

const (
	codexWireFPModeKey   = "codex_fingerprint_mode"
	codexWireFPSeedKey   = "codex_fingerprint_seed"
	codexWireConvergeKey = "codex_experimental_fingerprint_convergence"
	codexWireInboundInst = "7f582abd-05d2-4a59-b4e5-ec1b733b4edc"
	// 非 legacy 的 openai-beta：只有透传构造器原样转发，用作分支判别标记。
	codexWirePassthroughMarker = "responses_websockets=2026-02-06"
	codexWireInboundSess       = "01a07c73-e312-76e1-9054-e4665b8ee0a1"
	codexWireTurnMetadata      = `{"installation_id":"` + codexWireInboundInst + `","session_id":"` + codexWireInboundSess +
		`","thread_id":"` + codexWireInboundSess + `","turn_id":"01a07c73-e3a0-7ae1-baf0-ce1c532f019c",` +
		`"root_turn_id":"01a07c73-e3a0-7ae1-baf0-ce1c532f019c","window_id":"` + codexWireInboundSess + `:1",` +
		`"window_number":1,"tool_namespaces_info":["shell","apply_patch"]}`
)

type codexWireCapture struct {
	accountID int64
	method    string
	path      string
	header    http.Header
	body      []byte
}

type codexWireUpstream struct {
	service.HTTPUpstream
	mu       sync.Mutex
	captures []codexWireCapture
	// status 按账号强制返回的状态码，用于换号测试。
	status map[int64]int
}

func (u *codexWireUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		// 双开 /responses 的体是 zstd（真客户端默认 enable_request_compression）；断言看明文。
		if strings.EqualFold(req.Header.Get("Content-Encoding"), "zstd") {
			dec, err := zstd.NewReader(bytes.NewReader(body))
			if err == nil {
				if plain, err := io.ReadAll(dec); err == nil {
					body = plain
				}
				dec.Close()
			}
		}
	}
	u.mu.Lock()
	u.captures = append(u.captures, codexWireCapture{
		accountID: accountID,
		method:    req.Method,
		path:      req.URL.Path,
		header:    req.Header.Clone(),
		body:      body,
	})
	forced := u.status[accountID]
	u.mu.Unlock()

	if forced > 0 {
		return &http.Response{
			StatusCode: forced,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"upstream unavailable"}}`)),
		}, nil
	}
	if bytes.Contains(body, []byte(`"stream":true`)) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: io.NopCloser(strings.NewReader(
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"model\":\"gpt-5.4\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")),
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(
			`{"id":"resp_ok","object":"response","model":"gpt-5.4","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`)),
	}, nil
}

// taken 只返回推理请求（POST）。双开账号还会异步补一条只读 GET
// （settings/user，见 service/openai_codex_side_calls.go），
// 它不属于出站推理形态，由 sideCalls 单独断言。
func (u *codexWireUpstream) taken() []codexWireCapture {
	return u.capturedWithMethod(http.MethodPost)
}

// sideCalls 返回账号面的只读 GET。异步补发，调用方需自行等待。
func (u *codexWireUpstream) sideCalls() []codexWireCapture {
	return u.capturedWithMethod(http.MethodGet)
}

func (u *codexWireUpstream) capturedWithMethod(method string) []codexWireCapture {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]codexWireCapture, 0, len(u.captures))
	for _, capture := range u.captures {
		if capture.method == method {
			out = append(out, capture)
		}
	}
	return out
}

// codexWireAccount 造一个 OpenAI OAuth 账号；extra 决定指纹模式与收敛开关。
func codexWireAccount(id int64, name string, extra map[string]any) service.Account {
	base := map[string]any{}
	for k, v := range extra {
		base[k] = v
	}
	return service.Account{
		ID: id, Name: name, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, Concurrency: 1, Priority: int(id),
		Credentials: map[string]any{
			"access_token": "access-" + name,
			"expires_at":   time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339),
		},
		Extra: base,
	}
}

func newCodexWireEntry(t *testing.T, accounts []service.Account) (*codexWireUpstream, *gin.Engine, func()) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	groupID := int64(9001)
	repo := &grokCredentialHandlerRepo{accounts: accounts, missingOnGet: map[int64]bool{}}
	upstream := &codexWireUpstream{status: map[int64]int{}}

	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Gateway.MaxAccountSwitches = 3
	billingCache := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	// openAITokenProvider 传 nil：GetAccessToken 会降级直接读 credentials.access_token，
	// 省掉一整套 OAuth 刷新 mock。
	gateway := service.NewOpenAIGatewayService(
		repo, nil, nil, nil, nil, nil, nil, cfg, nil, nil,
		service.NewBillingService(cfg, nil), nil, billingCache, upstream,
		&service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil,
	)
	cache := &concurrencyCacheMock{
		acquireUserSlotFn:    func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(cache), billingCache,
		&service.APIKeyService{}, nil, nil, nil, nil, cfg)

	apiKey := &service.APIKey{
		ID: 9002, GroupID: &groupID,
		User:  &service.User{ID: 9003, Status: service.StatusActive},
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, AllowMessagesDispatch: true},
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.User.ID, Concurrency: 1})
		c.Next()
	})
	router.POST("/v1/responses", h.Responses)
	router.POST("/v1/messages", h.Messages)
	router.POST("/v1/responses/*subpath", h.Responses)
	return upstream, router, func() { billingCache.Stop() }
}

// codexWireSend 用真实 Codex 客户端形态的入站头发一次请求。
func codexWireSend(t *testing.T, router *gin.Engine, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	for k, v := range map[string]string{
		"content-type":              "application/json",
		"originator":                "codex-tui",
		"user-agent":                "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466 (codex-tui; 0.153.4)",
		"version":                   "0.153.4",
		"session-id":                codexWireInboundSess,
		"thread-id":                 codexWireInboundSess,
		"x-client-request-id":       codexWireInboundSess,
		"x-codex-installation-id":   codexWireInboundInst,
		"x-codex-window-id":         codexWireInboundSess + ":1",
		"x-codex-turn-metadata":     codexWireTurnMetadata,
		"openai-beta":               "responses=experimental",
		"chatgpt-account-id":        "inbound-account",
		"x-codex-terminal-metadata": `{"term":"xterm"}`,
	} {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func codexWireResponsesBody(stream bool) string {
	streamVal := "false"
	if stream {
		streamVal = "true"
	}
	return `{"model":"gpt-5.4","stream":` + streamVal + `,"prompt_cache_key":"` + codexWireInboundSess + `",` +
		`"client_metadata":{"session_id":"` + codexWireInboundSess + `","thread_id":"` + codexWireInboundSess +
		`","x-codex-installation-id":"` + codexWireInboundInst + `","x-codex-window-id":"` + codexWireInboundSess +
		`:1","x-codex-turn-metadata":` + codexWireJSONString(codexWireTurnMetadata) + `},` +
		`"input":[{"type":"message","role":"user","content":"hi"}]}`
}

func codexWireCompactBody() string {
	return `{"model":"gpt-5.4","stream":false,"store":false,"prompt_cache_key":"` + codexWireInboundSess + `",` +
		`"input":[{"type":"message","role":"user","content":"hi"}]}`
}

func codexWireJSONString(s string) string {
	encoded, _ := json.Marshal(s) // Encoding a string cannot fail.
	return string(encoded)
}

var (
	codexWireConverged = map[string]any{
		codexWireFPModeKey:   "device",
		codexWireFPSeedKey:   "951af12d-881d-4865-8f1b-3d952e328525",
		codexWireConvergeKey: true,
	}
	codexWireDeviceOnly = map[string]any{
		codexWireFPModeKey: "device",
		codexWireFPSeedKey: "951af12d-881d-4865-8f1b-3d952e328525",
	}
)

// TestCodexWireEntryCompact 覆盖 compact 请求从入口到出站的完整链路。
// compact 的 prompt_cache_key 在 handler 白名单里被放行、在 service 按账号收口，
// 两段都只有走真入口才能一起验到。
func TestCodexWireEntryCompact(t *testing.T) {
	cases := []struct {
		name  string
		extra map[string]any
		// 投影开启时保留并做账号隔离；未开启时删除，维持既有出站形态。
		wantCacheKey bool
		// 双开走真客户端的连字符会话头形态；未开启时仍是历史的下划线别名。
		wantHyphenSession bool
	}{
		{name: "双开", extra: codexWireConverged, wantCacheKey: true, wantHyphenSession: true},
		{name: "仅device", extra: codexWireDeviceOnly, wantCacheKey: false, wantHyphenSession: false},
		{name: "纯OAuth", extra: map[string]any{}, wantCacheKey: false, wantHyphenSession: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
				codexWireAccount(701, "target", tc.extra),
			})
			defer cleanup()

			rec := codexWireSend(t, router, "/v1/responses/compact", codexWireCompactBody())
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			taken := upstream.taken()
			require.Len(t, taken, 1)
			got := taken[0]
			require.True(t, strings.HasSuffix(got.path, "/responses/compact"), got.path)

			// handler 白名单：store 是 CompactionInput 里没有的字段，必须被剥掉。
			require.False(t, gjson.GetBytes(got.body, "store").Exists(), "compact 体不该带 store")
			require.False(t, gjson.GetBytes(got.body, "client_metadata").Exists(), "compact 体不该带 client_metadata")

			key := gjson.GetBytes(got.body, "prompt_cache_key")
			if tc.wantCacheKey {
				require.True(t, key.Exists(), "投影开启时 compact 必须保留 prompt_cache_key")
				require.NotEqual(t, codexWireInboundSess, key.String(), "必须按账号隔离，不能原样出站")
				require.Equal(t, got.header.Get("session-id"), key.String(), "会话默认键要与出站会话头同源")
			} else {
				require.False(t, key.Exists(), "投影未开时维持既有出站形态：删掉")
			}

			if tc.wantHyphenSession {
				require.NotEmpty(t, got.header.Get("session-id"))
				require.NotEmpty(t, got.header.Get("thread-id"))
			} else {
				require.Empty(t, got.header.Get("session-id"))
				require.NotEmpty(t, got.header.Get("session_id"), "未开投影仍走下划线旧别名")
			}
			// 真实 compact 的构造链没有 stream_request 那一步，任何形态都不该发该头。
			require.Empty(t, got.header.Get("x-client-request-id"))
			require.NotEmpty(t, got.header.Get("x-codex-installation-id"),
				"compact 是唯一发独立安装头的端点")
		})
	}
}

func TestCodexWireEntryCompactAccessPrograms(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, passthrough := range []bool{false, true} {
			for _, program := range []string{"", "standard", "daybreak_blue", "daybreak_red"} {
				name := "disabled/map/"
				if enabled {
					name = "enabled/map/"
				}
				if passthrough {
					name += "raw/"
				}
				t.Run(name+program, func(t *testing.T) {
					extra := map[string]any{
						codexWireFPModeKey:   "device",
						codexWireFPSeedKey:   codexWireConverged[codexWireFPSeedKey],
						codexWireConvergeKey: enabled,
						"openai_passthrough": passthrough,
					}
					upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
						codexWireAccount(707, "target", extra),
					})
					defer cleanup()
					body := codexWireCompactBody()
					if program != "" {
						body = strings.TrimSuffix(body, "}") + `,"access_programs":{"cyber":"` + program + `"}}`
					}
					rec := codexWireSend(t, router, "/v1/responses/compact", body)
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					taken := upstream.taken()
					require.Len(t, taken, 1)
					programs := gjson.GetBytes(taken[0].body, "access_programs")
					if enabled && program != "" {
						require.JSONEq(t, `{"cyber":"`+program+`"}`, programs.Raw)
					} else {
						require.False(t, programs.Exists(), "do not synthesize a program or change opt-out traffic")
					}
				})
			}
		}
	}
}

// 双开账号在 /responses 与 /compact 上必须声明同一个准入等级。/responses 那条路径没有
// 任何字段裁剪，access_programs 一路原样出站；compact 若单独丢弃，同一个账号就会按端点
// 报出两套准入等级——那是确凿的形态矛盾，比"透传客户端声明"更糟。
func TestCodexWireEntryAccessProgramsAgreeAcrossEndpoints(t *testing.T) {
	for _, program := range []string{"standard", "daybreak_blue", "daybreak_red"} {
		t.Run(program, func(t *testing.T) {
			extra := map[string]any{
				codexWireFPModeKey:   "device",
				codexWireFPSeedKey:   codexWireConverged[codexWireFPSeedKey],
				codexWireConvergeKey: true,
			}
			upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
				codexWireAccount(709, "target", extra),
			})
			defer cleanup()
			want := `{"cyber":"` + program + `"}`
			seen := make([]string, 0, 2)
			for _, path := range []string{"/v1/responses", "/v1/responses/compact"} {
				base := codexWireResponsesBody(false)
				if path == "/v1/responses/compact" {
					base = codexWireCompactBody()
				}
				rec := codexWireSend(t, router, path,
					strings.TrimSuffix(base, "}")+`,"access_programs":`+want+`}`)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				taken := upstream.taken()
				require.NotEmpty(t, taken)
				seen = append(seen, gjson.GetBytes(taken[len(taken)-1].body, "access_programs").Raw)
			}
			require.JSONEq(t, want, seen[0], "/v1/responses")
			require.JSONEq(t, seen[0], seen[1], "两个端点必须声明同一个准入等级")
		})
	}
}

// TestCodexWireEntryCompactDropsClientRequestID 把 compact 的 x-client-request-id
// 剥离钉死：同一个双开账号，非 compact 发该头、compact 不发。上一个用例里三种账号
// 都是空，单看无法区分「被投影删掉」和「这条路径本来就不发」。
func TestCodexWireEntryCompactDropsClientRequestID(t *testing.T) {
	upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
		codexWireAccount(703, "target", codexWireConverged),
	})
	defer cleanup()

	require.Equal(t, http.StatusOK,
		codexWireSend(t, router, "/v1/responses", codexWireResponsesBody(true)).Code)
	require.Equal(t, http.StatusOK,
		codexWireSend(t, router, "/v1/responses/compact", codexWireCompactBody()).Code)

	taken := upstream.taken()
	require.Len(t, taken, 2)
	require.NotEmpty(t, taken[0].header.Get("x-client-request-id"), "非 compact 照发")
	require.Empty(t, taken[1].header.Get("x-client-request-id"), "compact 必须收掉")
}

// TestCodexWireEntryCompactCacheKeyRules 把 compact 的 prompt_cache_key 规则在
// 两条转发路径（非透传 forward / 透传 passthrough）上逐格钉死。异常取值不能因为
// 走了哪条路径而有不同的出站形态。
func TestCodexWireEntryCompactCacheKeyRules(t *testing.T) {
	const composite = "codex:01a07c73-e312-76e1-9054-e4665b8ee0a9"
	rows := []struct {
		name string
		// raw 是 compact 请求体里 prompt_cache_key 的字面 JSON；空串表示不带该字段。
		raw string
		// 投影开启时期望的出站形态。
		wantExists bool
		// wantScoped 为 true 表示必须被改写成账号命名空间下的派生值。
		wantScoped bool
		// wantVerbatim 非空表示必须原样出站（异常取值不改写）。
		wantVerbatim string
	}{
		{name: "缺失", raw: "", wantExists: false},
		{name: "空串", raw: `""`, wantExists: true, wantVerbatim: ""},
		{name: "全空白", raw: `"   "`, wantExists: true, wantVerbatim: "   "},
		{name: "非字符串", raw: `123`, wantExists: true, wantVerbatim: "123"},
		{name: "自定义键", raw: `"my-custom-key"`, wantExists: true, wantScoped: true},
		{name: "复合键", raw: `"` + composite + `"`, wantExists: true, wantScoped: true},
		{name: "等于会话", raw: `"` + codexWireInboundSess + `"`, wantExists: true, wantScoped: true},
	}
	paths := []struct {
		name        string
		passthrough bool
	}{{name: "forward", passthrough: false}, {name: "passthrough", passthrough: true}}

	for _, p := range paths {
		for _, row := range rows {
			t.Run(p.name+"/"+row.name, func(t *testing.T) {
				extra := map[string]any{}
				for k, v := range codexWireConverged {
					extra[k] = v
				}
				if p.passthrough {
					extra["openai_passthrough"] = true
				}
				upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
					codexWireAccount(805, "target", extra),
				})
				defer cleanup()

				body := `{"model":"gpt-5.4","stream":false,` +
					`"input":[{"type":"message","role":"user","content":"hi"}]}`
				if row.raw != "" {
					body = `{"model":"gpt-5.4","stream":false,"prompt_cache_key":` + row.raw + `,` +
						`"input":[{"type":"message","role":"user","content":"hi"}]}`
				}
				rec := codexWireSend(t, router, "/v1/responses/compact", body)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				taken := upstream.taken()
				require.Len(t, taken, 1)
				key := gjson.GetBytes(taken[0].body, "prompt_cache_key")
				require.Equal(t, row.wantExists, key.Exists())
				if !row.wantExists {
					return
				}
				if row.wantScoped {
					require.NotEqual(t, gjson.Parse(row.raw).String(), key.String(),
						"必须落到账号命名空间，不能原样出站")
				} else {
					require.Equal(t, row.wantVerbatim, key.String(),
						"异常取值不改写，两条路径都保持原样")
				}
			})
		}
	}
}

// TestCodexWireEntryCompactCacheKeyPathsAgree 钉住两条转发路径对 compact 默认会话键
// 派生出同一个值。两边的旁证来源本来就不同——非透传读入站 session-id 头按 session 派生
// （applyCodexCompactPromptCacheKey），透传只看 client_metadata.session_id，而 compact
// 请求体没有该字段，于是按 prompt-cache 派生。之所以结果仍然一致，是因为收敛开启时
// codexConvergenceSeedKind 把 session 与 prompt-cache 都并入 thread。
//
// 也就是说这份一致性依赖那一处映射：改动 codexConvergenceSeedKind 会让同一入站请求
// 换条路径就换个上游缓存键，本用例会红。
func TestCodexWireEntryCompactCacheKeyPathsAgree(t *testing.T) {
	derive := func(passthrough bool) codexWireCapture {
		extra := map[string]any{}
		for k, v := range codexWireConverged {
			extra[k] = v
		}
		if passthrough {
			extra["openai_passthrough"] = true
		}
		upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
			codexWireAccount(806, "target", extra),
		})
		defer cleanup()
		// 非 legacy 的 openai-beta 只有透传构造器会原样转发，用它证明确实走了那条分支，
		// 而不是悄悄回落到非透传后两边"当然一致"。
		req := httptest.NewRequest(http.MethodPost, "/v1/responses/compact",
			strings.NewReader(codexWireCompactBody()))
		req.Header.Set("content-type", "application/json")
		req.Header.Set("originator", "codex-tui")
		req.Header.Set("session-id", codexWireInboundSess)
		req.Header.Set("thread-id", codexWireInboundSess)
		req.Header.Set("x-codex-turn-metadata", codexWireTurnMetadata)
		req.Header.Set("openai-beta", codexWirePassthroughMarker)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		taken := upstream.taken()
		require.Len(t, taken, 1)
		return taken[0]
	}

	forward, passthrough := derive(false), derive(true)
	require.Empty(t, forward.header.Get("openai-beta"), "非透传不转发该头")
	require.Equal(t, codexWirePassthroughMarker, passthrough.header.Get("openai-beta"),
		"标记头缺失说明没走透传分支，后面的一致性断言就没有意义了")

	forwardKey := gjson.GetBytes(forward.body, "prompt_cache_key").String()
	passthroughKey := gjson.GetBytes(passthrough.body, "prompt_cache_key").String()
	require.NotEmpty(t, forwardKey)
	require.Equal(t, forwardKey, passthroughKey, "两条路径必须派生出同一个上游缓存键")
	require.Equal(t, forward.header.Get("session-id"), forwardKey, "默认会话键与出站会话头同源")
	require.Equal(t, passthrough.header.Get("session-id"), passthroughKey)
	require.Equal(t, forward.header.Get("x-codex-installation-id"),
		passthrough.header.Get("x-codex-installation-id"), "设备身份也必须两路一致")
}

// TestCodexWireEntryFailoverDoesNotLeakPreviousAccountIdentity 钉住
// stageCodexFingerprintIDs 的无条件覆写：从收敛账号换到未开收敛的账号时，
// 上一账号的设备身份不得残留在第二次出站上。
func TestCodexWireEntryFailoverDoesNotLeakPreviousAccountIdentity(t *testing.T) {
	upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
		codexWireAccount(801, "converged", codexWireConverged),
		codexWireAccount(802, "plain", map[string]any{}),
	})
	defer cleanup()
	// 用 500 而不是 429：429 会先走同账号重试并 sleep 退避，拖长用例且换不了号。
	upstream.status[801] = http.StatusInternalServerError

	rec := codexWireSend(t, router, "/v1/responses", codexWireResponsesBody(true))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	taken := upstream.taken()
	require.GreaterOrEqual(t, len(taken), 2, "首号失败后必须换号重试")
	first, last := taken[0], taken[len(taken)-1]
	require.Equal(t, int64(801), first.accountID)
	require.Equal(t, int64(802), last.accountID)

	firstInstall := gjson.GetBytes(first.body, "client_metadata.x-codex-installation-id").String()
	lastInstall := gjson.GetBytes(last.body, "client_metadata.x-codex-installation-id").String()
	require.NotEmpty(t, firstInstall)
	require.NotEqual(t, firstInstall, lastInstall, "换号后不得沿用上一账号的收敛设备身份")
	require.Equal(t, codexWireInboundInst, lastInstall, "未开收敛的账号应保持客户端原值")
	require.Equal(t, codexWireInboundInst, last.header.Get("x-codex-installation-id"))

	// 会话头形态也必须跟着新账号走：收敛账号是连字符形态，未开的是下划线别名。
	require.NotEmpty(t, first.header.Get("session-id"))
	require.Empty(t, last.header.Get("session-id"))
	require.NotEmpty(t, last.header.Get("session_id"))
}

// TestCodexWireEntryNonOAuthAccountUntouched：收敛开关只对 OAuth 类账号生效
// （codexFingerprintConvergenceEnabled 先查 IsOpenAIOAuthLike）。API Key 账号即使
// 误配了同样的 extra，出站也必须保持既有形态。
func TestCodexWireEntryNonOAuthAccountUntouched(t *testing.T) {
	account := codexWireAccount(804, "apikey", codexWireConverged)
	account.Type = service.AccountTypeAPIKey
	account.Credentials = map[string]any{"api_key": "sk-third-party"}

	upstream, router, cleanup := newCodexWireEntry(t, []service.Account{account})
	defer cleanup()

	rec := codexWireSend(t, router, "/v1/responses", codexWireResponsesBody(true))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	taken := upstream.taken()
	require.Len(t, taken, 1)
	require.Empty(t, taken[0].header.Get("session-id"), "非 OAuth 账号不该被切到连字符会话头形态")
	require.Equal(t, codexWireInboundSess,
		gjson.GetBytes(taken[0].body, "prompt_cache_key").String(),
		"非 OAuth 账号的缓存键不做账号隔离改写")
}

// TestCodexWireEntryResponses 覆盖非 compact 的 /responses：设备身份留在体内，
// 双开时独立安装头必须被收掉。
func TestCodexWireEntryResponses(t *testing.T) {
	cases := []struct {
		name           string
		extra          map[string]any
		wantInstallHdr bool
	}{
		{name: "双开", extra: codexWireConverged, wantInstallHdr: false},
		{name: "仅device", extra: codexWireDeviceOnly, wantInstallHdr: true},
		{name: "纯OAuth", extra: map[string]any{}, wantInstallHdr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
				codexWireAccount(702, "target", tc.extra),
			})
			defer cleanup()

			rec := codexWireSend(t, router, "/v1/responses", codexWireResponsesBody(true))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			taken := upstream.taken()
			require.Len(t, taken, 1)
			got := taken[0]

			require.True(t, gjson.GetBytes(got.body, "client_metadata.x-codex-installation-id").Exists(),
				"非 compact 的设备身份留在体内")

			if tc.wantInstallHdr {
				require.NotEmpty(t, got.header.Get("x-codex-installation-id"))
			} else {
				require.Empty(t, got.header.Get("x-codex-installation-id"),
					"真实 /responses 不发独立安装头，设备身份只在体内")
			}

			// 兼容头剥工具清单，体内完整元数据保留。
			hdrMeta := got.header.Get("x-codex-turn-metadata")
			if !tc.wantInstallHdr {
				require.False(t, gjson.Get(hdrMeta, "tool_namespaces_info").Exists(),
					"双开时兼容头必须剥掉 tool_namespaces_info")
			}
			bodyMeta := gjson.GetBytes(got.body, "client_metadata.x-codex-turn-metadata").String()
			require.True(t, gjson.Get(bodyMeta, "tool_namespaces_info").Exists(),
				"体内 turn-metadata 的工具清单只该从头剥，不该从体里删")
		})
	}
}

// TestCodexWireEntryMessagesBridgeMatchesResponses 钉住 Messages 兼容桥与
// /responses 在双开账号上出站身份一致。此前三处不一致：
//   - ensureCodexIdentityHeaders 在投影之后把 OpenAI-Beta:responses=experimental 加回来；
//   - Messages 桥没做指纹收敛解析，设备身份是按账号命名空间哈希客户端原值得到的另一套；
//   - 桥自己 Set 的下划线别名 session_id 与连字符 session-id 取值不同，两套会话标识并存。
func TestCodexWireEntryMessagesBridgeMatchesResponses(t *testing.T) {
	upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
		codexWireAccount(809, "target", codexWireConverged),
	})
	defer cleanup()

	require.Equal(t, http.StatusOK,
		codexWireSend(t, router, "/v1/responses", codexWireResponsesBody(true)).Code)
	require.Equal(t, http.StatusOK,
		codexWireSend(t, router, "/v1/messages",
			`{"model":"gpt-5.4","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`).Code)

	taken := upstream.taken()
	require.Len(t, taken, 2)
	responses, messages := taken[0], taken[1]

	install := func(c codexWireCapture) string {
		return gjson.Get(c.header.Get("x-codex-turn-metadata"), "installation_id").String()
	}
	require.NotEmpty(t, install(responses))
	require.Equal(t, install(responses), install(messages), "两个端点必须是同一个设备身份")

	for _, c := range []codexWireCapture{responses, messages} {
		require.Empty(t, c.header.Get("openai-beta"), "真实客户端在 HTTP /responses 上不发该头")
		require.Empty(t, c.header.Get("x-codex-installation-id"), "设备身份只在体内")
		require.Empty(t, c.header.Get("session_id"), "下划线别名是网关历史形态")
		require.Empty(t, c.header.Get("conversation_id"))
		require.NotEmpty(t, c.header.Get("session-id"))
	}
}

// TestCodexWireEntrySideCalls 盯的是构造器真的把线程去重窗口装上了：codexSideCalls 为 nil
// 时整条侧信道静默停用，service 包里用裸结构体拼的用例发现不了（那里本来就期望它是 nil）。
func TestCodexWireEntrySideCalls(t *testing.T) {
	t.Run("双开按真客户端补发 settings/user", func(t *testing.T) {
		upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
			codexWireAccount(704, "target", codexWireConverged),
		})
		defer cleanup()

		require.Equal(t, http.StatusOK,
			codexWireSend(t, router, "/v1/responses", codexWireResponsesBody(true)).Code)

		require.Eventually(t, func() bool { return len(upstream.sideCalls()) == 1 },
			3*time.Second, 10*time.Millisecond, "线程首见补一条 settings/user")

		side := upstream.sideCalls()[0]
		require.Equal(t, "/backend-api/wham/settings/user", side.path,
			"config/bundle 只在 business/edu/enterprise plan 上由真客户端发起，一条都不该有")
		require.NotEmpty(t, side.header.Get("authorization"))
	})

	t.Run("非双开一条都不发", func(t *testing.T) {
		upstream, router, cleanup := newCodexWireEntry(t, []service.Account{
			codexWireAccount(705, "target", map[string]any{}),
		})
		defer cleanup()

		require.Equal(t, http.StatusOK,
			codexWireSend(t, router, "/v1/responses", codexWireResponsesBody(true)).Code)

		time.Sleep(300 * time.Millisecond) // 异步补发：给足触发窗口再判空
		require.Empty(t, upstream.sideCalls())
	})
}
