package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/chatgptcookies"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type qualityGuardUpstream struct {
	HTTPUpstream
	send func(*http.Request) (*http.Response, error)
}

func (u *qualityGuardUpstream) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return u.send(r)
}

func qualityGuardFixture(t *testing.T, mode string) (*OpenAIGatewayService, *Account, *http.Request) {
	t.Helper()
	account := codexCookieTestAccount(1, AccountTypeOAuth)
	account.Status, account.Schedulable = StatusActive, true
	account.Extra = map[string]any{openAIQualityGuardEnabledKey: true, openAIQualityGuardModeKey: mode}
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(`{"model":"gpt-6-astra","input":"private-business","prompt_cache_key":"final-pck","client_metadata":{"session_id":"final-session"}}`))
	require.NoError(t, err)
	request.Header.Set(openAICodexTurnStateHeader, "old-state")
	request.Header.Set("Authorization", "Bearer offline-only")
	return &OpenAIGatewayService{}, account, request
}

func qualityGuardResponse(state, body string) *http.Response {
	header := http.Header{"Content-Type": {"text/event-stream"}}
	if state != "" {
		header.Set(openAICodexTurnStateHeader, state)
	}
	return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

const qualityGuardCompleted = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"

func TestQualityGuardNormalAndDisabledAreUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name, sent, fresh string
		enabled           bool
		status            int
	}{
		{"same", "old-state", "old-state", true, 200},
		{"no-new", "old-state", "", true, 200},
		{"first-mint", "", "new", true, 200},
		{"disabled", "old-state", "new", false, 200},
		{"upstream429", "old-state", "new", true, 429},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, account, req := qualityGuardFixture(t, qualityGuardModeWait)
			account.Extra[openAIQualityGuardEnabledKey] = tc.enabled
			req.Header.Set(openAICodexTurnStateHeader, tc.sent)
			body := &gwpoolEchoBody{}
			calls := 0
			expected := qualityGuardResponse(tc.fresh, "")
			expected.StatusCode, expected.Body = tc.status, body
			svc.httpUpstream = &qualityGuardUpstream{send: func(*http.Request) (*http.Response, error) {
				calls++
				return expected, nil
			}}
			response, err := svc.doOpenAIUpstream(req, "", account)
			require.NoError(t, err)
			require.Same(t, expected, response)
			require.Equal(t, 1, calls)
			require.False(t, body.closed)
			require.Zero(t, body.reads)
			qualityGuardClose(response)
		})
	}
}

func TestQualityGuardBlockClosesWithoutReadingOrProbing(t *testing.T) {
	svc, account, req := qualityGuardFixture(t, qualityGuardModeBlock)
	body := &gwpoolEchoBody{}
	svc.httpUpstream = &qualityGuardUpstream{send: func(*http.Request) (*http.Response, error) {
		response := qualityGuardResponse("new", "")
		response.Body = body
		return response, nil
	}}
	response, err := svc.doOpenAIUpstream(req, "", account)
	require.Nil(t, response)
	var failure *UpstreamFailoverError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, OpenAIQualityGuardReason, failure.Reason)
	require.False(t, failure.ShouldRetryNextAccount())
	require.False(t, failure.ShouldReportAccountScheduleFailure())
	require.True(t, body.closed)
	require.Zero(t, body.reads)
}

func TestQualityGuardWaitReplaysOriginalWithConfirmedState(t *testing.T) {
	svc, account, req := qualityGuardFixture(t, qualityGuardModeWait)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req = req.WithContext(withQualityGuardRequest(req.Context(), c, account, nil))
	req.Header.Set("x-codex-inference-call-id", "original-attempt")
	discarded := &gwpoolEchoBody{}
	var bodies, states []string
	ids := map[string]bool{}
	svc.httpUpstream = &qualityGuardUpstream{send: func(r *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		bodies = append(bodies, string(raw))
		states = append(states, r.Header.Get(openAICodexTurnStateHeader))
		id := r.Header.Get("x-codex-inference-call-id")
		require.NotEmpty(t, id)
		require.False(t, ids[id], "every internal HTTP send has a distinct attempt ID")
		ids[id] = true
		require.Equal(t, "Bearer offline-only", r.Header.Get("Authorization"))
		switch len(bodies) {
		case 1:
			response := qualityGuardResponse("fresh-state", "")
			response.Body = discarded
			return response, nil
		case 2:
			require.True(t, discarded.closed, "discard before sending confirmation")
			require.Zero(t, discarded.reads)
			require.Equal(t, gatewayPoolProbeModelLuna, gjson.GetBytes(raw, "model").String())
			require.NotContains(t, string(raw), "private-business")
			require.Equal(t, "final-pck", gjson.GetBytes(raw, "prompt_cache_key").String())
			require.Equal(t, "final-session", gjson.GetBytes(raw, "client_metadata.session_id").String())
			return qualityGuardResponse("", qualityGuardCompleted), nil
		case 3:
			return qualityGuardResponse("", "recovered answer"), nil
		default:
			t.Fatal("unexpected extra request")
			return nil, nil
		}
	}}
	response, err := svc.doOpenAIUpstream(req, "", account)
	require.NoError(t, err)
	defer qualityGuardClose(response)
	require.Equal(t, []string{"old-state", "fresh-state", "fresh-state"}, states)
	require.Equal(t, bodies[0], bodies[2])
	require.Equal(t, "fresh-state", OpenAITurnStateUsageSent(c))
	require.Equal(t, "old-state", req.Header.Get(openAICodexTurnStateHeader))
}

func TestQualityGuardBudgetSurvivesBusinessReplay(t *testing.T) {
	svc, account, req := qualityGuardFixture(t, qualityGuardModeWait)
	account.Extra[openAIQualityGuardAttemptsKey] = 1
	calls := 0
	svc.httpUpstream = &qualityGuardUpstream{send: func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 2 {
			return qualityGuardResponse("", qualityGuardCompleted), nil
		}
		return qualityGuardResponse("state-"+string(rune('a'+calls)), ""), nil
	}}
	response, err := svc.doOpenAIUpstream(req, "", account)
	require.Nil(t, response)
	var failure *UpstreamFailoverError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, qualityGuardExhaustedMessage, failure.ClientMessage)
	require.Equal(t, 3, calls, "one business, one confirmation, one failed replay; no reset")
}

func TestQualityGuardCancellationStopsPendingRetry(t *testing.T) {
	svc, account, req := qualityGuardFixture(t, qualityGuardModeWait)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	calls := 0
	svc.httpUpstream = &qualityGuardUpstream{send: func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return qualityGuardResponse("new", "data: [DONE]\n\n"), nil
	}}
	response, err := svc.doOpenAIUpstream(req, "", account)
	require.Nil(t, response)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, calls)
	require.ErrorIs(t, qualityGuardWait(ctx, time.Now().Add(time.Hour)), context.Canceled)
	upstreamCtx, stop := gatewayPoolUpstreamContext(ctx, account)
	defer stop()
	require.ErrorIs(t, upstreamCtx.Err(), context.Canceled)
}

func TestQualityGuardProbeRequiresCompletedSuccess(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		success    bool
	}{
		{"completed", qualityGuardCompleted, true},
		{"done-only", "data: [DONE]\n\n", false},
		{"empty", "", false},
		{"failed", `data: {"type":"response.failed"}` + "\n\n", false},
		{"error-then-completed", `data: {"type":"error","error":{"message":"bad"}}` + "\n\n" + qualityGuardCompleted, false},
		{"bad-json-then-completed", "data: not-json\n\n" + qualityGuardCompleted, false},
		{"nested-error", `data: {"type":"response.completed","response":{"status":"completed","error":{"code":"limit"}}}` + "\n\n", false},
		{"incomplete", `data: {"type":"response.completed","response":{"status":"incomplete"}}` + "\n\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, _ := qualityGuardProbeCompleted([]byte(tc.body), true)
			require.Equal(t, tc.success, ok)
		})
	}
	ok, _ := qualityGuardProbeCompleted([]byte(`{"status":"completed","error":null}`), false)
	require.True(t, ok)
}

func TestQualityGuardProbeErrorsAndRouteChangesNeverPass(t *testing.T) {
	for _, kind := range []string{"429", "401", "500", "network", "read", "route", "refresh", "event-state"} {
		t.Run(kind, func(t *testing.T) {
			svc, account, req := qualityGuardFixture(t, qualityGuardModeWait)
			svc.httpUpstream = &qualityGuardUpstream{send: func(*http.Request) (*http.Response, error) {
				resp := qualityGuardResponse("", qualityGuardCompleted)
				switch kind {
				case "429":
					resp.StatusCode = 429
					resp.Header.Set("Retry-After", "12")
				case "401":
					resp.StatusCode = 401
				case "500":
					resp.StatusCode = 500
				case "network":
					return nil, errors.New("offline")
				case "read":
					resp.Body = io.NopCloser(qualityGuardReadError{})
				case "route":
					resp.Header.Add("Set-Cookie", "__oailb=new-route; Path=/; Secure")
				case "refresh":
					resp.Header.Set(openAICodexTurnStateHeader, "another-state")
				case "event-state":
					resp.Body = io.NopCloser(strings.NewReader("data: {\"type\":\"response.metadata\",\"headers\":{\"x-codex-turn-state\":\"another\"}}\n\n" + qualityGuardCompleted))
				}
				return resp, nil
			}}
			result := svc.qualityGuardProbe(req, account, "", []byte(`{"model":"gpt-6-luna"}`), "old-state", chatgptcookies.NewJar())
			require.False(t, result.accepted)
			require.Equal(t, kind == "401", result.stop)
			if kind == "429" {
				require.True(t, result.retryAt.After(time.Now().Add(10*time.Second)))
			}
		})
	}
}

type qualityGuardReadError struct{}

func (qualityGuardReadError) Read([]byte) (int, error) { return 0, errors.New("offline read failure") }

func TestQualityGuardConfigValidationAndBulkMerge(t *testing.T) {
	_, account, _ := qualityGuardFixture(t, qualityGuardModeWait)
	require.NoError(t, validateOpenAIQualityGuardExtra(account, account.Extra))
	for key, values := range map[string][]any{
		openAIQualityGuardModeKey:     {"", "automatic", true},
		openAIQualityGuardEnabledKey:  {"true", 1},
		openAIQualityGuardAttemptsKey: {0, -1, 101, 1.5, "20"},
		openAIQualityGuardIntervalKey: {0, 301, 1.5},
	} {
		for _, value := range values {
			require.Error(t, validateOpenAIQualityGuardExtra(account, mergeMap(account.Extra, map[string]any{key: value})))
		}
	}
	require.Error(t, validateOpenAIQualityGuardExtra(account, map[string]any{openAIQualityGuardEnabledKey: true}))
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{1: account}}
	admin := &adminServiceImpl{accountRepo: repo}
	_, err := admin.BulkUpdateAccounts(context.Background(), &BulkUpdateAccountsInput{
		AccountIDs: []int64{1}, Extra: map[string]any{openAIQualityGuardModeKey: qualityGuardModeBlock},
	})
	require.NoError(t, err)
	require.Len(t, repo.bulkUpdates, 1)
	err = admin.UpdateAccountExtra(context.Background(), 1, map[string]any{openAIQualityGuardModeKey: nil})
	require.Error(t, err, "partial clear cannot leave an enabled guard without a mode")
	account.Type = AccountTypeAPIKey
	_, err = admin.BulkUpdateAccounts(context.Background(), &BulkUpdateAccountsInput{
		AccountIDs: []int64{1}, Extra: map[string]any{openAIQualityGuardEnabledKey: true},
	})
	require.Error(t, err)
	require.Len(t, repo.bulkUpdates, 1)
}

func TestQualityGuardStopsAfterAccountConfigurationChanges(t *testing.T) {
	svc, account, req := qualityGuardFixture(t, qualityGuardModeWait)
	fresh := *account
	fresh.Credentials = map[string]any{"access_token": "replaced-offline"}
	svc.accountRepo = &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{1: &fresh}}
	calls := 0
	svc.httpUpstream = &qualityGuardUpstream{send: func(*http.Request) (*http.Response, error) {
		calls++
		return qualityGuardResponse("fresh", ""), nil
	}}
	_, err := svc.doOpenAIUpstream(req, "", account)
	var failure *UpstreamFailoverError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, qualityGuardChangedMessage, failure.ClientMessage)
	require.Equal(t, 1, calls)
}

func TestQualityGuardLatestStateAndPrivateCookies(t *testing.T) {
	svc, account, req := qualityGuardFixture(t, qualityGuardModeWait)
	account.Extra[openAIQualityGuardIntervalKey] = 1
	svc.codexCookies = openAICodexCookieStore{}
	svc.codexCookies.Store(account, req.URL.String(), http.Header{"Set-Cookie": {"__oailb=original-route; Path=/; Secure"}})
	var states []string
	var probesAt []time.Time
	svc.httpUpstream = &qualityGuardUpstream{send: func(r *http.Request) (*http.Response, error) {
		states = append(states, r.Header.Get(openAICodexTurnStateHeader))
		require.Contains(t, r.Header.Get("Cookie"), "__oailb=original-route")
		if len(states) == 1 {
			svc.codexCookies.Store(account, req.URL.String(), http.Header{"Set-Cookie": {"__oailb=concurrent-route; Path=/; Secure"}})
			return qualityGuardResponse("state-1", ""), nil
		}
		if len(states) <= 3 {
			probesAt = append(probesAt, time.Now())
		}
		if len(states) == 2 {
			return qualityGuardResponse("state-2", qualityGuardCompleted), nil
		}
		return qualityGuardResponse("", qualityGuardCompleted), nil
	}}
	response, err := svc.doOpenAIUpstream(req, "", account)
	require.NoError(t, err)
	qualityGuardClose(response)
	require.Equal(t, []string{"old-state", "state-1", "state-2", "state-2"}, states)
	require.GreaterOrEqual(t, probesAt[1].Sub(probesAt[0]), time.Second)
}

func TestQualityGuardCompressedReplayUpdatesBothCarriers(t *testing.T) {
	svc, account, _ := qualityGuardFixture(t, qualityGuardModeWait)
	plain := []byte(`{"model":"gpt-6-astra","input":"private-business","client_metadata":{"x-codex-turn-state":"old-state","session_id":"final"}}`)
	wire, err := encodeCodexZstdRequestBody(plain)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, bytes.NewReader(wire))
	require.NoError(t, err)
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set(openAICodexTurnStateHeader, "old-state")
	ctx := context.WithValue(req.Context(), gatewayPoolConfirmBodyKey{}, gatewayPoolConfirmBody(plain))
	req = req.WithContext(withQualityGuardRequest(ctx, nil, account, plain))
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	var decodedBodies [][]byte
	svc.httpUpstream = &qualityGuardUpstream{send: func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "zstd", r.Header.Get("Content-Encoding"))
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.EqualValues(t, len(raw), r.ContentLength)
		decoded, err := decoder.DecodeAll(raw, nil)
		require.NoError(t, err)
		decodedBodies = append(decodedBodies, decoded)
		if len(decodedBodies) == 1 {
			return qualityGuardResponse("confirmed", ""), nil
		}
		require.Equal(t, "confirmed", r.Header.Get(openAICodexTurnStateHeader))
		return qualityGuardResponse("", qualityGuardCompleted), nil
	}}
	response, err := svc.doOpenAIUpstream(req, "", account)
	require.NoError(t, err)
	qualityGuardClose(response)
	require.Len(t, decodedBodies, 3)
	require.Equal(t, gatewayPoolProbeModelLuna, gjson.GetBytes(decodedBodies[1], "model").String())
	require.False(t, gjson.GetBytes(decodedBodies[1], "client_metadata."+openAICodexTurnStateHeader).Exists())
	require.Equal(t, "confirmed", gjson.GetBytes(decodedBodies[2], "client_metadata."+openAICodexTurnStateHeader).String())
	require.Equal(t, "private-business", gjson.GetBytes(decodedBodies[2], "input").String())
	require.Equal(t, "gpt-6-astra", gjson.GetBytes(decodedBodies[2], "model").String())
}

func TestQualityGuardForwardDeadlineDoesNotBecomeAccountFailure(t *testing.T) {
	svc, account, _ := qualityGuardFixture(t, qualityGuardModeWait)
	account.Credentials = map[string]any{"access_token": "offline-token", "chatgpt_account_id": "offline-account"}
	svc.cfg = &config.Config{Gateway: config.GatewayConfig{OpenAIFirstOutputTimeoutSeconds: 1, MaxLineSize: defaultMaxLineSize}}
	body := []byte(`{"model":"gpt-6-astra","stream":true,"reasoning":{"effort":"low"},"input":"hello"}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	c.Request.Header.Set(openAICodexTurnStateHeader, "old-state")
	calls := 0
	svc.httpUpstream = &qualityGuardUpstream{send: func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			require.Equal(t, "old-state", r.Header.Get(openAICodexTurnStateHeader))
			return qualityGuardResponse("new-state", ""), nil
		}
		select {
		case <-r.Context().Done():
			return nil, r.Context().Err()
		case <-time.After(2 * time.Second):
			t.Fatal("confirmation was not stopped by the first-output deadline")
			return nil, nil
		}
	}}
	_, err := svc.Forward(context.Background(), c, account, body)
	var failure *UpstreamFailoverError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, OpenAIQualityGuardReason, failure.Reason)
	require.Equal(t, qualityGuardTimeoutMessage, failure.ClientMessage)
	require.False(t, failure.ShouldRetryNextAccount())
	require.False(t, failure.ShouldReportAccountScheduleFailure())
	require.Empty(t, rec.Body.String())
	require.Equal(t, 2, calls)
}

func TestQualityGuardImagesResponsesBlockAndCancel(t *testing.T) {
	for _, mode := range []string{qualityGuardModeBlock, qualityGuardModeWait} {
		t.Run(mode, func(t *testing.T) {
			svc, account, _ := qualityGuardFixture(t, mode)
			account.Credentials = map[string]any{"access_token": "offline-token", "chatgpt_account_id": "offline-account"}
			svc.cfg = &config.Config{}
			body := []byte(`{"model":"gpt-image-2","prompt":"draw a cat"}`)
			c, rec := newOpenAIImagesTestContext(t, body)
			c.Request.Header.Set(openAICodexTurnStateHeader, "old-state")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			svc.httpUpstream = &qualityGuardUpstream{send: func(r *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					require.Equal(t, openAIGatewayPoolInferencePath, r.URL.Path)
					return qualityGuardResponse("new-state", ""), nil
				}
				cancel()
				require.ErrorIs(t, r.Context().Err(), context.Canceled)
				return nil, r.Context().Err()
			}}
			parsed, err := svc.ParseOpenAIImagesRequest(c, body)
			require.NoError(t, err)
			_, err = svc.ForwardImages(withOpenAIImagesForceResponses(ctx), c, account, body, parsed, "")
			if mode == qualityGuardModeBlock {
				var failure *UpstreamFailoverError
				require.ErrorAs(t, err, &failure)
				require.Equal(t, OpenAIQualityGuardReason, failure.Reason)
				require.Equal(t, 1, calls)
			} else {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 2, calls)
			}
			require.Empty(t, rec.Body.String())
		})
	}
}

func TestQualityGuardForwardReplayNetworkErrorStopsWithoutAccountPenalty(t *testing.T) {
	svc, account, _ := qualityGuardFixture(t, qualityGuardModeWait)
	account.Credentials = map[string]any{"access_token": "offline-token", "chatgpt_account_id": "offline-account"}
	svc.cfg = &config.Config{}
	body := []byte(`{"model":"gpt-6-astra","stream":true,"input":"hello"}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	c.Request.Header.Set(openAICodexTurnStateHeader, "old-state")
	calls := 0
	svc.httpUpstream = &qualityGuardUpstream{send: func(*http.Request) (*http.Response, error) {
		calls++
		switch calls {
		case 1:
			return qualityGuardResponse("fresh", ""), nil
		case 2:
			return qualityGuardResponse("", qualityGuardCompleted), nil
		default:
			return nil, errors.New("proxyconnect tcp: offline proxy refused connection")
		}
	}}
	_, err := svc.Forward(context.Background(), c, account, body)
	var failure *UpstreamFailoverError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, OpenAIQualityGuardReason, failure.Reason)
	require.False(t, failure.ShouldRetryNextAccount())
	require.False(t, failure.ShouldReportAccountScheduleFailure())
	require.Equal(t, 3, calls)
	require.Empty(t, rec.Body.String())
	require.Nil(t, account.TempUnschedulableUntil)
}
