package service

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// codex rust-v0.156.1 对齐（相对 e763730 的差异审计，确定项）：
//   - client_metadata.guardian_credits_requested（core/src/client.rs:1122-1155 set_guardian_metadata）
//   - turn-metadata 的 model / reasoning_effort 与出站体同源（session/session.rs:667 ExecutionMetadata::apply_to）

func requireCodexGuardianCreditsRequested(t *testing.T, payload []byte, want bool) {
	t.Helper()
	got := gjson.GetBytes(payload, "client_metadata."+codexGuardianCreditsRequestedKey)
	if !want {
		require.False(t, got.Exists(), "不该带 guardian_credits_requested：%s", payload)
		return
	}
	require.Equal(t, gjson.String, got.Type, "值是字符串（HashMap<String,String>）：%s", payload)
	require.Equal(t, "true", got.Str)
}

func TestCodexGuardianCreditsRequestedHTTP(t *testing.T) {
	cases := []struct {
		name               string
		passthrough        bool
		subagentHeader     string
		subagentBody       string
		compact            bool
		noWireProfile      bool
		dropClientMetadata bool
		want               bool
	}{
		{name: "dual-open map", want: true},
		{name: "dual-open passthrough", passthrough: true, want: true},
		{name: "other subagent", subagentHeader: "explorer", subagentBody: "explorer", want: true},
		{name: "guardian reviewer", subagentHeader: "guardian", subagentBody: "guardian"},
		{name: "guardian reviewer passthrough", passthrough: true, subagentHeader: "guardian", subagentBody: "guardian"},
		{name: "guardian header only", subagentHeader: "guardian"},
		{name: "guardian body only", subagentBody: "guardian"},
		{name: "legacy compact", compact: true},
		{name: "wire profile off", noWireProfile: true},
		{name: "wire profile off passthrough", passthrough: true, noWireProfile: true},
		{name: "no client_metadata", dropClientMetadata: true, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := wireProfileTestBody(t)
			var err error
			body, err = sjson.DeleteBytes(body, "client_metadata.x-openai-subagent")
			require.NoError(t, err)
			if tc.subagentBody != "" {
				body, err = sjson.SetBytes(body, "client_metadata.x-openai-subagent", tc.subagentBody)
				require.NoError(t, err)
			}
			if tc.dropClientMetadata {
				body, err = sjson.DeleteBytes(body, "client_metadata")
				require.NoError(t, err)
			}
			c := newConvTestContext(t, body)
			c.Request.Header.Del(openAISubagentHeader)
			if tc.subagentHeader != "" {
				c.Request.Header.Set(openAISubagentHeader, tc.subagentHeader)
			}
			if tc.compact {
				c.Request.URL.Path = "/v1/responses/compact"
			}
			account := wireProfileTestAccount(!tc.noWireProfile)
			account.Extra["openai_passthrough"] = tc.passthrough
			svc, up := wireProfileTestService()
			_, _ = svc.Forward(context.Background(), c, account, body)
			require.NotNil(t, up.lastReq)
			requireCodexGuardianCreditsRequested(t, up.lastBody, tc.want)
		})
	}
}

// 只有 ChatGPT 登录态带（client.rs 只认 CodexAuth::Chatgpt / ChatgptAuthTokens）；凭证种类看凭证源，
// 不看被转发行（影子行共用母账号凭证）。
func TestCodexGuardianCreditsRequestedCredentialKinds(t *testing.T) {
	body := []byte(`{"client_metadata":{}}`)
	withMode := func(mode string) *Account {
		account := wireProfileTestAccount(true)
		if mode != "" {
			account.Credentials[openAIAuthModeCredentialKey] = mode
		}
		return account
	}
	for _, tc := range []struct {
		name string
		mode string
		want bool
	}{
		{name: "chatgpt", want: true},
		{name: "personal access token", mode: "personal_access_token"},
		{name: "agent identity", mode: OpenAIAuthModeAgentIdentity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newConvTestContext(t, body)
			c.Request.Header.Del(openAISubagentHeader)
			require.Equal(t, tc.want, codexGuardianCreditsRequested(c, withMode(tc.mode), body))
		})
	}
	t.Run("shadow row follows credential source", func(t *testing.T) {
		c := newConvTestContext(t, body)
		c.Request.Header.Del(openAISubagentHeader)
		c.Set(codexAccountIdentitySourceContextKey, withMode("personal_access_token"))
		require.False(t, codexGuardianCreditsRequested(c, withMode(""), body))

		c = newConvTestContext(t, body)
		c.Request.Header.Del(openAISubagentHeader)
		c.Set(codexAccountIdentitySourceContextKey, withMode(""))
		require.True(t, codexGuardianCreditsRequested(c, withMode("personal_access_token"), body))
	})
}

// client_metadata 不是对象、请求体不是 JSON 对象时原样不动（不把客户端的值整个换掉，不凭空造体）。
func TestApplyCodexGuardianCreditsRequestedLeavesNonObjectsAlone(t *testing.T) {
	c := newConvTestContext(t, nil)
	c.Request.Header.Del(openAISubagentHeader)
	account := wireProfileTestAccount(true)
	for _, body := range []string{``, `[1]`, `{"client_metadata":"scalar"}`, `{"client_metadata":null}`} {
		require.Equal(t, body, string(applyCodexGuardianCreditsRequested(c, account, chatgptCodexURL, []byte(body))))
	}
	requireCodexGuardianCreditsRequested(t, applyCodexGuardianCreditsRequested(c, account, chatgptCodexURL, []byte(`{"model":"m"}`)), true)
}

// WS 帧（含预热帧）同样经 set_guardian_metadata（client.rs:1976）；guardian 评审帧不带。
func TestCodexGuardianCreditsRequestedWSFrame(t *testing.T) {
	c := newConvTestContext(t, nil)
	c.Request.Header.Del(openAISubagentHeader)
	frame := []byte(codexWSTestFrame)
	requireCodexGuardianCreditsRequested(t, applyCodexWSFrameWireProfile(c, wireProfileTestAccount(true), frame, ""), true)
	requireCodexGuardianCreditsRequested(t, applyCodexWSFrameWireProfile(c, wireProfileTestAccount(false), frame, ""), false)

	guardianFrame, err := sjson.SetBytes(frame, "client_metadata.x-openai-subagent", "guardian")
	require.NoError(t, err)
	requireCodexGuardianCreditsRequested(t, applyCodexWSFrameWireProfile(c, wireProfileTestAccount(true), guardianFrame, ""), false)

	c.Request.Header.Set(openAISubagentHeader, "guardian")
	requireCodexGuardianCreditsRequested(t, applyCodexWSFrameWireProfile(c, wireProfileTestAccount(true), frame, ""), false)
}

// 0.156 起 turn-metadata 的 model 与请求体同源（core/tests/suite/step_settings.rs:1885-1908 逐请求断言相等）。
// 网关改了体里的 model（模型映射）时，头与体内两份 turn-metadata 跟着改；入站没有这个键时不补。
// reasoning_effort 不跟：真客户端那里写的是选中的档位（ultra 时体里是解析后的 xhigh / max），见
// codexTurnMetadataExecutionValues。
func TestCodexTurnMetadataExecutionFollowsOutboundBody(t *testing.T) {
	const clientMeta = `{"session_id":"` + convTestSession + `","thread_id":"` + convTestThread +
		`","turn_id":"` + convTestTurn + `","turn_started_at_unix_ms":1,"model":"client-model","reasoning_effort":"ultra"}`
	const clientMetaNoExecution = `{"session_id":"` + convTestSession + `","thread_id":"` + convTestThread +
		`","turn_id":"` + convTestTurn + `","turn_started_at_unix_ms":1}`
	for _, passthrough := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			for _, meta := range []string{clientMeta, clientMetaNoExecution} {
				hasExecution := meta == clientMeta
				name := map[bool]string{false: "map", true: "passthrough"}[passthrough] +
					map[bool]string{false: "/disabled", true: "/enabled"}[enabled] +
					map[bool]string{false: "/no-execution-keys", true: "/execution-keys"}[hasExecution]
				t.Run(name, func(t *testing.T) {
					body := wireProfileTestBody(t)
					var err error
					body, err = sjson.SetBytes(body, "model", "gpt-6-astra")
					require.NoError(t, err)
					body, err = sjson.SetBytes(body, "reasoning.effort", "minimal")
					require.NoError(t, err)
					body, err = sjson.SetBytes(body, "client_metadata.x-codex-turn-metadata", meta)
					require.NoError(t, err)
					c := newConvTestContext(t, body)
					c.Request.Header.Set(openAIWSTurnMetadataHeader, meta)
					account := wireProfileTestAccount(enabled)
					account.Extra["openai_passthrough"] = passthrough
					account.Credentials["model_mapping"] = map[string]any{"gpt-6-astra": "gpt-5.4"}
					svc, up := wireProfileTestService()
					_, _ = svc.Forward(context.Background(), c, account, body)
					require.NotNil(t, up.lastReq)

					outModel := gjson.GetBytes(up.lastBody, "model").String()
					outEffort := gjson.GetBytes(up.lastBody, "reasoning.effort").String()
					if !passthrough {
						require.Equal(t, "gpt-5.4", outModel, "前提：非透传路径做了模型映射")
						require.Equal(t, "none", outEffort, "前提：非透传路径把 minimal 归一成 none")
					}
					carriers := map[string]string{
						"header": up.lastReq.Header.Get(openAIWSTurnMetadataHeader),
						"body":   gjson.GetBytes(up.lastBody, "client_metadata."+openAIWSTurnMetadataHeader).String(),
					}
					for carrier, raw := range carriers {
						got := gjson.Parse(raw)
						require.True(t, got.IsObject(), "%s: %s", carrier, raw)
						switch {
						case !hasExecution:
							require.False(t, got.Get("model").Exists(), "%s 入站没有就不补：%s", carrier, raw)
							require.False(t, got.Get("reasoning_effort").Exists(), "%s 入站没有就不补：%s", carrier, raw)
						case enabled:
							require.Equal(t, outModel, got.Get("model").String(), "%s: %s", carrier, raw)
							require.Equal(t, "ultra", got.Get("reasoning_effort").String(), "%s 选中的档位不按体改写：%s", carrier, raw)
						default:
							require.Equal(t, "client-model", got.Get("model").String(), "%s 未开双开不动：%s", carrier, raw)
							require.Equal(t, "ultra", got.Get("reasoning_effort").String(), "%s 未开双开不动：%s", carrier, raw)
						}
					}
				})
			}
		}
	}
}

// WS 帧内嵌的 turn-metadata 同一条规则：model 跟随帧自己的 model，只改已有键，值已一致时字节不动；
// reasoning_effort 不跟（真 0.156.1 选 ultra 时帧里是解析后的 xhigh，metadata 仍是 "ultra"）。
func TestCodexTurnMetadataExecutionFollowsWSFrame(t *testing.T) {
	c := newConvTestContext(t, nil)
	c.Request.Header.Del(openAISubagentHeader)
	frame := []byte(`{"type":"response.create","model":"gpt-5.4","reasoning":{"effort":"xhigh"},"client_metadata":{"x-codex-turn-metadata":"{\"turn_id\":\"t\",\"model\":\"client-model\",\"reasoning_effort\":\"ultra\"}"}}`)
	out := applyCodexWSFrameWireProfile(c, wireProfileTestAccount(true), frame, "")
	meta := gjson.Parse(gjson.GetBytes(out, "client_metadata."+openAIWSTurnMetadataHeader).String())
	require.Equal(t, "gpt-5.4", meta.Get("model").String(), "%s", out)
	require.Equal(t, "ultra", meta.Get("reasoning_effort").String(), "%s", out)

	out = applyCodexWSFrameWireProfile(c, wireProfileTestAccount(false), frame, "")
	require.Equal(t, string(frame), string(out), "未开双开整帧不动")

	// 值已一致：内嵌字符串逐字节保留（含非 ASCII 的原样写法）。
	same := []byte(`{"type":"response.create","model":"gpt-5.4","reasoning":{"effort":"xhigh"},"client_metadata":{"x-codex-turn-metadata":"{\"cwd\":\"项目\",\"model\":\"gpt-5.4\",\"reasoning_effort\":\"ultra\"}"}}`)
	out = applyCodexWSFrameWireProfile(c, wireProfileTestAccount(true), same, "")
	require.Equal(t, gjson.GetBytes(same, "client_metadata."+openAIWSTurnMetadataHeader).Raw,
		gjson.GetBytes(out, "client_metadata."+openAIWSTurnMetadataHeader).Raw)
}

func TestAlignCodexTurnMetadataJSON(t *testing.T) {
	values := map[string]string{"model": "gpt-5.4", "reasoning_effort": ""}
	require.Equal(t, `{"model":"gpt-5.4","reasoning_effort":"low"}`,
		alignCodexTurnMetadataJSON(`{"model":"client-model","reasoning_effort":"low"}`, values),
		"出站值为空的键不动")
	require.Equal(t, `{"turn_id":"t"}`, alignCodexTurnMetadataJSON(`{"turn_id":"t"}`, values), "没有的键不补")
	require.Equal(t, `not json`, alignCodexTurnMetadataJSON(`not json`, values))
	raw := `{"cwd":"项目","model":"gpt-5.4"}`
	require.Equal(t, raw, alignCodexTurnMetadataJSON(raw, values), "值已一致时整段不改写（不顺带转义非 ASCII）")

	h := http.Header{}
	h.Set(openAIWSTurnMetadataHeader, `{"model":"client-model"}`)
	alignCodexTurnMetadataFields(h, values)
	require.Equal(t, `{"model":"gpt-5.4"}`, h.Get(openAIWSTurnMetadataHeader))
}
