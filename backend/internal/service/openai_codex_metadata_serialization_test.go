package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const metadataSerializationUnknown = `"z_extra" : {"z":9007199254740993, "a":1.2300e+04, "text":"\u4e2d\u6587\ud83d\ude00 <>&", "raw":"中文😀", "escape":"\\u003c"}`

func metadataSerializationFixture() string {
	return `{ ` + metadataSerializationUnknown + `, ` + convTestTurnMetadata()[1:]
}

func requireMetadataSerializationPreserved(t *testing.T, raw string) {
	t.Helper()
	require.True(t, strings.HasPrefix(raw, `{ `+metadataSerializationUnknown+`, `), raw)
	require.Less(t, strings.Index(raw, `"installation_id"`), strings.Index(raw, `"session_id"`), raw)
	require.Less(t, strings.Index(raw, `"window_id"`), strings.Index(raw, `"context_window_id"`), raw)
}

func TestCodexMetadataSerializationNamespace(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			account := convTestAccount(enabled)
			raw := `  { "session_id" : "old", ` + metadataSerializationUnknown + `, "session\u005fid":"last", "turn_id":"turn", "root_turn_id":"turn" }  `
			want := strings.ReplaceAll(raw, `"old"`, `"`+scopeCodexAccountIdentityValue(account, 77, "session", "last")+`"`)
			want = strings.ReplaceAll(want, `"last"`, `"`+scopeCodexAccountIdentityValue(account, 77, "session", "last")+`"`)
			want = strings.Replace(want, `"turn_id":"turn"`, `"turn_id":"`+scopeCodexAccountIdentityValue(account, 77, "turn", "turn")+`"`, 1)
			if enabled {
				want = strings.Replace(want, `"root_turn_id":"turn"`, `"root_turn_id":"`+scopeCodexAccountIdentityValue(account, 77, "turn", "turn")+`"`, 1)
			}
			h := make(http.Header)
			h.Set(openAIWSTurnMetadataHeader, raw)
			applyCodexAccountIdentityHeaders(h, account, 77)
			require.Equal(t, want, h.Get(openAIWSTurnMetadataHeader))
			cm := map[string]any{openAIWSTurnMetadataHeader: raw}
			require.True(t, applyCodexAccountIdentityEmbeddedMetadata(cm, account, 77))
			require.Equal(t, want, cm[openAIWSTurnMetadataHeader])
		})
	}
}

func TestCodexMetadataSerializationFingerprint(t *testing.T) {
	for _, mode := range []codexFingerprintMode{codexFingerprintDevice, codexFingerprintSession, codexFingerprintFull} {
		for _, root := range []string{"old-turn", "parent-turn"} {
			t.Run(string(mode)+"/"+root, func(t *testing.T) {
				ids := &codexFingerprintIDs{
					mode: mode, convergence: true, installationID: "new-install", sessionID: "new-session",
					threadID: "new-thread", turnID: "new-turn", windowID: "new-thread:2",
					windowNumber: 2, turnStartedAtUnixMs: 1234,
				}
				raw := `  { ` + metadataSerializationUnknown + `, "installation_id":"old-install", "session_id":"old-session", "thread_id":"old-thread", "turn_id":"old-turn", "root_turn_id":"` + root + `", "window_id":"old-window", "window_number":1, "turn_started_at_unix_ms":12 }  `
				want := strings.Replace(raw, `"old-install"`, `"new-install"`, 1)
				if mode != codexFingerprintDevice {
					want = strings.NewReplacer(`"old-session"`, `"new-session"`, `"old-thread"`, `"new-thread"`,
						`"old-turn"`, `"new-turn"`, `"old-window"`, `"new-thread:2"`,
						`"window_number":1`, `"window_number":2`, `"turn_started_at_unix_ms":12`, `"turn_started_at_unix_ms":1234`).Replace(want)
				}
				h := make(http.Header)
				h.Set(openAIWSTurnMetadataHeader, raw)
				applyCodexFingerprintHeaders(h, ids)
				require.Equal(t, want, h.Get(openAIWSTurnMetadataHeader))
				body := map[string]any{"client_metadata": map[string]any{openAIWSTurnMetadataHeader: raw}}
				applyCodexFingerprintClientMetadata(body, ids)
				clientMetadata, ok := body["client_metadata"].(map[string]any)
				require.True(t, ok)
				require.Equal(t, want, clientMetadata[openAIWSTurnMetadataHeader])
			})
		}
	}
}

func TestCodexMetadataSerializationBoundaries(t *testing.T) {
	account := convTestAccount(true)
	ids := &codexFingerprintIDs{mode: codexFingerprintDevice, installationID: "设备😀<>&"}
	for _, raw := range []string{"", " ", "null", "[]", `"scalar"`, "{broken", `{} {}`, `{"session_id":7,"thread_id":true}`} {
		t.Run(raw, func(t *testing.T) {
			h := make(http.Header)
			h.Set(openAIWSTurnMetadataHeader, raw)
			applyCodexAccountIdentityHeaders(h, account, 77)
			require.Equal(t, raw, h.Get(openAIWSTurnMetadataHeader), "namespace keeps invalid/non-string identities")
			applyCodexFingerprintHeaders(h, ids)
			if strings.TrimSpace(raw) == "" {
				require.Equal(t, raw, h.Get(openAIWSTurnMetadataHeader))
				return
			}
			require.True(t, json.Valid([]byte(h.Get(openAIWSTurnMetadataHeader))))
			require.Contains(t, h.Get(openAIWSTurnMetadataHeader), `"installation_id":"\u8bbe\u5907\ud83d\ude00<>&"`)
		})
	}
	for _, raw := range []string{
		`{ "installation_id":"old", "installation\u005fid":"设备😀<>&", "huge":1e400 }`,
		`{ "huge":1e400 }`,
	} {
		h := make(http.Header)
		h.Set(openAIWSTurnMetadataHeader, raw)
		applyCodexFingerprintHeaders(h, ids)
		next := h.Get(openAIWSTurnMetadataHeader)
		require.Contains(t, next, `"huge":1e400`)
		require.NotContains(t, next, `"old"`)
		gjson.Parse(next).ForEach(func(key, value gjson.Result) bool {
			if key.String() == "installation_id" {
				require.Equal(t, ids.installationID, value.String())
			}
			return true
		})
		applyCodexFingerprintHeaders(h, ids)
		require.Equal(t, next, h.Get(openAIWSTurnMetadataHeader), "reapplication must preserve bytes")
	}
	raw := `{ "installation_id":"already", "n":1.00 }`
	h := make(http.Header)
	h.Set(openAIWSTurnMetadataHeader, raw)
	applyCodexFingerprintHeaders(h, &codexFingerprintIDs{mode: codexFingerprintDevice, installationID: "already"})
	require.Equal(t, raw, h.Get(openAIWSTurnMetadataHeader))
}

func TestCodexMetadataSerializationHTTP(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, mode := range []string{"off", "device", "session", "full"} {
			for _, passthrough := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/enabled=%v/raw=%v", mode, enabled, passthrough), func(t *testing.T) {
					metadata := metadataSerializationFixture()
					body, err := sjson.SetBytes(wireProfileTestBody(t), "client_metadata."+openAIWSTurnMetadataHeader, metadata)
					require.NoError(t, err)
					c := newConvTestContext(t, body)
					c.Request.Header.Set(openAIWSTurnMetadataHeader, metadata)
					account := wireProfileTestAccount(enabled)
					account.Extra[codexFingerprintModeExtraKey] = mode
					account.Extra["openai_passthrough"] = passthrough
					svc, up := wireProfileTestService()
					_, _ = svc.Forward(context.Background(), c, account, body)
					require.NotNil(t, up.lastReq)
					requireMetadataSerializationPreserved(t, up.lastReq.Header.Get(openAIWSTurnMetadataHeader))
					requireMetadataSerializationPreserved(t, gjson.GetBytes(up.lastBody, "client_metadata."+openAIWSTurnMetadataHeader).String())
					// 整体编码器同样不转义 HTML：内嵌 metadata 的 <>& 必须以原字节出站
					// （json.Marshal 会写成 \u003c\u003e\u0026，gjson 反转义后看不出来）。
					require.Contains(t, string(up.lastBody), ` <>&`)
					require.NotContains(t, string(up.lastBody), `u003e`)
				})
			}
		}
	}
}
