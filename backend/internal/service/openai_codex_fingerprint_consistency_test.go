package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func convRunMap(t *testing.T, account *Account, raw []byte, stripInbound bool) (http.Header, []byte) {
	t.Helper()
	c := newConvTestContext(t, raw)
	if stripInbound {
		for _, name := range []string{"session-id", "thread-id", "x-client-request-id", "x-codex-parent-thread-id"} {
			c.Request.Header.Del(name)
		}
	}
	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body))
	originalKey, _ := body["prompt_cache_key"].(string)
	ids := resolveCodexFingerprintIDsWithBody(c, account, nil, body["client_metadata"])
	applyCodexAccountIdentityClientMetadataMap(body, account, 77)
	applyCodexFingerprintClientMetadata(body, ids)
	stageCodexFingerprintIDs(c, ids)
	stageCodexConvergenceBodyIdentityMap(c, account, body)
	finalBody, err := json.Marshal(body)
	require.NoError(t, err)
	svc := &OpenAIGatewayService{}
	req, err := svc.buildUpstreamRequest(context.Background(), c, account, finalBody, "dummy-offline-token", true, originalKey, true)
	require.NoError(t, err)
	return req.Header, finalBody
}

func TestCodexFingerprintConvergence_ConsistencyMatrix(t *testing.T) {
	for _, mode := range []string{"off", "device", "session", "full"} {
		for _, metadata := range []string{"full", "absent", "empty", "installation"} {
			for _, relay := range []bool{false, true} {
				for _, keyKind := range []string{"default", "override", "composite"} {
					for _, path := range []string{"map", "raw"} {
						t.Run(fmt.Sprintf("%s/%s/relay=%v/%s/%s", mode, metadata, relay, keyKind, path), func(t *testing.T) {
							account := convTestAccount(true)
							account.Extra[codexFingerprintModeExtraKey] = mode
							account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
							var body map[string]any
							require.NoError(t, json.Unmarshal(convTestBody(t), &body))
							switch metadata {
							case "absent":
								delete(body, "client_metadata")
							case "empty":
								body["client_metadata"] = map[string]any{}
							case "installation":
								body["client_metadata"] = map[string]any{"x-codex-installation-id": convTestInstallation}
							}
							key := convTestSession
							if keyKind == "override" {
								key = "explicit-cache-key"
							} else if keyKind == "composite" {
								key = "guardian:" + convTestParentThread
							}
							body["prompt_cache_key"] = key
							raw, err := json.Marshal(body)
							require.NoError(t, err)
							var headers http.Header
							var finalBody []byte
							if path == "map" {
								headers, finalBody = convRunMap(t, account, raw, relay)
							} else {
								headers, finalBody = convRunPassthrough(t, account, raw, relay)
							}
							cacheKey := gjson.GetBytes(finalBody, "prompt_cache_key").String()
							if keyKind == "default" {
								require.Equal(t, headers.Get("session-id"), cacheKey, "default cache key must follow the evidenced session")
							} else {
								require.Equal(t, scopeCodexAccountIdentityValue(account, 77, "prompt-cache", key), cacheKey, "explicit or composite cache key must survive fingerprint convergence")
							}
							turnMetadata := gjson.Parse(headers.Get(openAIWSTurnMetadataHeader))
							for _, field := range [][2]string{{"session-id", "session_id"}, {"thread-id", "thread_id"}} {
								if value := turnMetadata.Get(field[1]).String(); value != "" {
									require.Equal(t, value, headers.Get(field[0]), "header and turn metadata identity")
								}
								if value := gjson.GetBytes(finalBody, "client_metadata").Get(field[1]).String(); value != "" {
									require.Equal(t, value, headers.Get(field[0]), "header and body identity")
								}
							}
						})
					}
				}
			}
		}
	}
}

func TestCodexFingerprintConvergence_UnprovenCacheKeyIsNotSession(t *testing.T) {
	for _, mode := range []string{"off", "device", "session", "full"} {
		for _, path := range []string{"map", "raw"} {
			t.Run(mode+"/"+path, func(t *testing.T) {
				account := convTestAccount(true)
				account.Extra[codexFingerprintModeExtraKey] = mode
				account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
				raw := []byte(`{"model":"gpt-5.5","stream":true,"prompt_cache_key":"explicit-cache-key","input":[]}`)
				c := newConvTestContext(t, raw)
				c.Request.Header = make(http.Header) // No independent session carrier at all.
				ids := resolveCodexFingerprintIDsFromRequest(c, account, nil)
				var finalBody []byte
				if path == "map" {
					var body map[string]any
					require.NoError(t, json.Unmarshal(raw, &body))
					applyCodexAccountIdentityClientMetadataMap(body, account, 77)
					applyCodexFingerprintClientMetadata(body, ids)
					stageCodexConvergenceBodyIdentityMap(c, account, body)
					var err error
					finalBody, err = json.Marshal(body)
					require.NoError(t, err)
				} else {
					var err error
					finalBody, _, err = applyCodexAccountIdentityClientMetadataRaw(raw, account, 77)
					require.NoError(t, err)
					finalBody, _, err = applyCodexFingerprintClientMetadataRaw(finalBody, ids)
					require.NoError(t, err)
					stageCodexConvergenceBodyIdentityRaw(c, account, finalBody)
				}
				stageCodexFingerprintIDs(c, ids)
				want := scopeCodexAccountIdentityValue(account, 77, "prompt-cache", "explicit-cache-key")
				require.Equal(t, want, gjson.GetBytes(finalBody, "prompt_cache_key").String())
				if mode == "off" || mode == "device" {
					headers := make(http.Header)
					headers.Set("session_id", "existing-cache-affinity")
					applyCodexFingerprintConvergenceHeaders(c, account, headers)
					require.Empty(t, headers.Get("session-id"), "unknown cache key must not manufacture a client session")
					require.Equal(t, "existing-cache-affinity", headers.Get("session_id"))
				}
			})
		}
	}
}

func TestCodexFingerprintConvergence_OffDoesNotAddEmbeddedSessionEvidence(t *testing.T) {
	account := convTestAccount(false)
	account.Extra[codexFingerprintModeExtraKey] = string(codexFingerprintSession)
	account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
	for _, path := range []string{"map", "raw"} {
		t.Run(path, func(t *testing.T) {
			body := map[string]any{"prompt_cache_key": convTestSession, "client_metadata": map[string]any{openAIWSTurnMetadataHeader: convTestTurnMetadata()}}
			ids := resolveCodexFingerprintIDsFromRequest(nil, account, nil)
			if path == "map" {
				applyCodexFingerprintClientMetadata(body, ids)
				require.Equal(t, convTestSession, body["prompt_cache_key"])
			} else {
				raw, err := json.Marshal(body)
				require.NoError(t, err)
				next, _, err := applyCodexFingerprintClientMetadataRaw(raw, ids)
				require.NoError(t, err)
				require.Equal(t, convTestSession, gjson.GetBytes(next, "prompt_cache_key").String())
			}
		})
	}
}

func TestCodexFingerprintConvergence_StagingClearsMissingAndInvalidIdentity(t *testing.T) {
	account := convTestAccount(true)
	c := newConvTestContext(t, nil)
	stageCodexConvergenceBodyIdentityRaw(c, account, []byte(`{"client_metadata":{"session_id":"old-session"}}`))
	require.Equal(t, "old-session", stagedCodexConvergenceBodyIdentity(c, account)["session-id"])
	stageCodexConvergenceBodyIdentityRaw(c, account, []byte(`{}`))
	require.Empty(t, stagedCodexConvergenceBodyIdentity(c, account), "same-account follow-up must not reuse a previous body's identity")
	stageCodexConvergenceBodyIdentityRaw(c, account, []byte(`{"client_metadata":{"session_id":7,"thread_id":true}}`))
	require.Empty(t, stagedCodexConvergenceBodyIdentity(c, account), "non-string metadata is not session evidence")
	stageCodexConvergenceBodyIdentityMap(c, account, map[string]any{"client_metadata": map[string]any{"session_id": "another-session"}})
	stageCodexConvergenceBodyIdentityMap(c, account, nil)
	require.Empty(t, stagedCodexConvergenceBodyIdentity(c, account))
}

type convHandshakeDialer struct {
	conn    openAIWSClientConn
	headers chan http.Header
}

type convJSONFrameConn struct{ *stagedPassthroughConn }

func (c *convJSONFrameConn) WriteJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteFrame(ctx, coderws.MessageText, payload)
}

func (d *convHandshakeDialer) Dial(_ context.Context, _ string, headers http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	d.headers <- headers.Clone()
	return d.conn, http.StatusSwitchingProtocols, http.Header{}, nil
}

func TestCodexFingerprintConvergence_WSEntriesKeepIdentityAcrossTurns(t *testing.T) {
	for _, ingress := range []string{OpenAIWSIngressModePassthrough, OpenAIWSIngressModeCtxPool} {
		for _, enabled := range []bool{false, true} {
			for _, mode := range []string{"device", "session", "full"} {
				for _, bodyOnly := range []bool{false, true} {
					if bodyOnly && (!enabled || mode != "session") {
						continue
					}
					t.Run(fmt.Sprintf("%s/enabled=%v/%s/bodyOnly=%v", ingress, enabled, mode, bodyOnly), func(t *testing.T) {
						account := convTestAccount(enabled)
						account.Status, account.Schedulable, account.Concurrency = StatusActive, true, 1
						account.Extra[codexFingerprintModeExtraKey] = mode
						seed := "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
						account.Extra[codexFingerprintSeedExtraKey] = seed
						account.Extra["openai_oauth_responses_websockets_v2_mode"] = ingress
						cfg := passthroughLifecycleConfig()
						cfg.Gateway.OpenAIWS.OAuthEnabled = true
						upstream := newStagedPassthroughConn()
						dialer := &convHandshakeDialer{conn: &convJSONFrameConn{upstream}, headers: make(chan http.Header, 1)}
						svc := newPassthroughLifecycleService(cfg, upstream)
						svc.openaiWSPassthroughDialer = dialer
						if ingress == OpenAIWSIngressModeCtxPool {
							cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
							cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
							cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
							pool := newOpenAIWSConnPool(cfg)
							pool.setClientDialerForTest(dialer)
							svc.openaiWSPool = pool
							defer pool.Close()
						}
						controlCtx, cancel := context.WithCancelCause(context.Background())
						server, serverErr := startPassthroughLifecycleServerWithHooks(t, controlCtx, svc, account, func(c *gin.Context) *OpenAIWSIngressHooks {
							c.Set("api_key_id", int64(77))
							c.Set("api_key", &APIKey{ID: 77})
							return nil
						})
						defer server.Close()
						defer cancel(context.Canceled)
						raw := convTestBody(t)
						clientHeaders := newConvTestContext(t, raw).Request.Header.Clone()
						if bodyOnly {
							clientHeaders = http.Header{"User-Agent": []string{"codex_cli_rs/0.153.4"}}
						}
						ctx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
						client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &coderws.DialOptions{HTTPHeader: clientHeaders})
						cancelDial()
						require.NoError(t, err)
						defer func() { _ = client.CloseNow() }()
						var headers http.Header
						for turn := 1; turn <= 2; turn++ {
							var body map[string]any
							require.NoError(t, json.Unmarshal(raw, &body))
							body["type"] = "response.create"
							payload, err := json.Marshal(body)
							require.NoError(t, err)
							writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
							err = client.Write(writeCtx, coderws.MessageText, payload)
							cancelWrite()
							require.NoError(t, err)
							forwarded := requirePassthroughUpstreamWrite(t, upstream, 3*time.Second)
							if turn == 1 {
								select {
								case headers = <-dialer.headers:
								case <-time.After(3 * time.Second):
									t.Fatal("missing upstream handshake")
								}
							}
							wantInstallation := scopeCodexAccountIdentityValue(account, 77, "installation", convTestInstallation)
							if enabled {
								wantInstallation = resolveConvergedInstallationID(account, seed)
							}
							require.Equal(t, wantInstallation, headers.Get("x-codex-installation-id"), "handshake")
							if enabled && mode == "session" {
								require.Equal(t, resolveConvergedThreadID(seed, convTestSession), headers.Get("thread-id"), "body-only and direct clients must resolve the same per-session thread")
							}
							require.Equal(t, wantInstallation, gjson.GetBytes(forwarded, "client_metadata.x-codex-installation-id").String(), "turn %d installation", turn)
							for _, field := range [][2]string{{"session-id", "session_id"}, {"thread-id", "thread_id"}} {
								require.Equal(t, headers.Get(field[0]), gjson.GetBytes(forwarded, "client_metadata").Get(field[1]).String(), "turn %d %s", turn, field[0])
							}
							require.Equal(t, headers.Get("session-id"), gjson.GetBytes(forwarded, "prompt_cache_key").String(), "turn %d default cache", turn)
							upstream.Send(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_convergence_%d","model":"gpt-5.5","usage":{"input_tokens":1,"output_tokens":1}}}`, turn))
							_, err = readPassthroughLifecycleFrame(t, client, 3*time.Second)
							require.NoError(t, err)
						}
						cancel(context.Canceled)
						_ = client.CloseNow()
						select {
						case <-serverErr: // The test deliberately cancels the session after both frames.
						case <-time.After(3 * time.Second):
							t.Fatal("websocket handler did not stop")
						}
					})
				}
			}
		}
	}
}

func TestCodexFingerprintConvergence_RootTurnRelationshipsSurviveModes(t *testing.T) {
	for _, mode := range []string{"session", "full"} {
		for _, path := range []string{"map", "raw"} {
			t.Run(mode+"/"+path, func(t *testing.T) {
				account := convSessionModeAccount(t)
				account.Extra[codexFingerprintModeExtraKey] = mode
				var headers http.Header
				var body []byte
				if path == "map" {
					headers, body = convRunMap(t, account, convTestBody(t), false)
				} else {
					headers, body = convRunPassthrough(t, account, convTestBody(t), false)
				}
				for _, metadata := range []gjson.Result{
					gjson.GetBytes(body, "client_metadata"),
					gjson.Parse(gjson.GetBytes(body, "client_metadata").Get(openAIWSTurnMetadataHeader).String()),
					gjson.Parse(headers.Get(openAIWSTurnMetadataHeader)),
				} {
					require.NotEmpty(t, metadata.Get("turn_id").String())
					require.Equal(t, metadata.Get("turn_id").String(), metadata.Get("root_turn_id").String(), "a proven root turn must remain its own root")
				}
			})
		}
	}
}

func TestCodexFingerprintConvergence_OnlyRewritesProvenRootTurns(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, isRoot := range []bool{false, true} {
			t.Run(fmt.Sprintf("enabled=%v/root=%v", enabled, isRoot), func(t *testing.T) {
				account := convTestAccount(enabled)
				account.Extra[codexFingerprintModeExtraKey] = string(codexFingerprintFull)
				account.Extra[codexFingerprintSeedExtraKey] = "0d3c4d0e-5a7b-4d6e-8f90-1a2b3c4d5e6f"
				root := convTestParentThread
				if isRoot {
					root = convTestTurn
				}
				metadata := map[string]any{"turn_id": convTestTurn, "root_turn_id": root}
				body := map[string]any{"client_metadata": metadata}
				ids := resolveCodexFingerprintIDsFromRequest(nil, account, nil)
				applyCodexFingerprintClientMetadata(body, ids)
				want := root
				if enabled && isRoot {
					want = ids.turnID
				}
				require.Equal(t, want, metadata["root_turn_id"])
			})
		}
	}
}
