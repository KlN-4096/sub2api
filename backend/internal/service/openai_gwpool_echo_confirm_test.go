package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestStateEchoConfirmationFollowsNewestStateAndStops(t *testing.T) {
	for _, tc := range []struct {
		name             string
		age              time.Duration
		states           []string
		status           int
		err              error
		full, conclusive bool
		calls            int
	}{
		{"young-all-refreshed", time.Second, []string{"s2", "s3", "s4"}, 200, nil, false, true, 3},
		{"90s-total-two-including-business", 90 * time.Second, []string{"s2"}, 200, nil, false, true, 1},
		{"old-all-refreshed", 140 * time.Second, []string{"s2"}, 200, nil, false, true, 1},
		{"accepted-second", time.Second, []string{"s2", "s2"}, 200, nil, true, true, 2},
		{"accepted-first", time.Second, []string{"s1"}, 200, nil, true, true, 1},
		{"no-new-state", time.Second, []string{""}, 200, nil, true, true, 1},
		{"non200", time.Second, []string{"s2"}, 429, nil, false, false, 1},
		{"transport-error", time.Second, []string{""}, 0, errors.New("offline"), false, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sent []string
			full, conclusive, _ := gatewayPoolConfirmState(context.Background(), "cookie", "s1",
				gatewayPoolEchoStrikes(tc.age), func(_ context.Context, cookie, state string) (int, string, error) {
					require.Equal(t, "cookie", cookie)
					sent = append(sent, state)
					return tc.status, tc.states[len(sent)-1], tc.err
				})
			require.Equal(t, tc.full, full)
			require.Equal(t, tc.conclusive, conclusive)
			require.Len(t, sent, tc.calls)
			for i := 1; i < len(sent); i++ {
				require.Equal(t, tc.states[i-1], sent[i])
			}
		})
	}
}

func TestStateEchoConfirmationDoesNotAccumulateAcrossBusiness(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: 200, minted: "a1"}, {status: 200, minted: "a2"}, {status: 200, minted: "a2"},
		{status: 200, minted: "b1"}, {status: 200, minted: "b2"}, {status: 200, minted: "b3"}, {status: 200, minted: "b3"},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	for range 2 {
		_, response, err := gwpoolEchoRun(t, svc, fake.account(1), "old")
		require.NoError(t, err)
		_ = response.Body.Close()
	}
	require.Equal(t, []string{"old", "a1", "a2", "old", "b1", "b2", "b3"}, upstream.sentState)
}

func TestStateEchoConfirmationUnknownClosesBusinessWithoutDegrading(t *testing.T) {
	for _, failure := range []string{"non200", "transport", "cancel", "changed-pair", "expired-pair"} {
		t.Run(failure, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: 200, minted: "s1"}, {status: 200, minted: "s2"}}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			upstream.beforeReply = func(_ *http.Request, n int) error {
				if n == 1 {
					return nil
				}
				switch failure {
				case "non200":
					upstream.replies[1].status = 429
				case "transport":
					return errors.New("offline")
				case "cancel":
					cancel()
				case "changed-pair", "expired-pair":
					pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
					if failure == "changed-pair" {
						pair.version = "new-pair"
					} else {
						pair.routeExpiresAt = time.Now().Add(-time.Second)
					}
					svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
				}
				return nil
			}
			acct := fake.account(1)
			gwpoolEchoSeedVerified(t, svc, acct)
			ctx, sink := withOpenAIGatewayPoolSink(ctx, nil)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
			require.NoError(t, err)
			req.Header.Set(openAICodexTurnStateHeader, "old")
			resp, err := svc.doOpenAIUpstream(req, "", acct)
			require.Nil(t, resp)
			require.ErrorIs(t, err, errOpenAIGatewayPoolWarmUnverified)
			require.NotEqual(t, openAIGatewayVerdictDegraded, sink.snapshot().Verdict)
			require.Empty(t, sink.discarded, "unknown is not a degraded training/usage event")
			require.True(t, upstream.bodies[0].closed)
			require.Zero(t, upstream.bodies[0].reads)
			require.Len(t, upstream.sentState, 2, "unknown stops the round immediately")
		})
	}
}

func TestStateEchoConfirmationSharesDeadlineAndFreezesBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	calls := 0
	full, conclusive, err := gatewayPoolConfirmState(ctx, "cookie", "s0", 3,
		func(ctx context.Context, _, _ string) (int, string, error) {
			got, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, deadline, got)
			calls++
			if calls == 2 {
				cancel()
			}
			return 200, "s" + strings.Repeat("x", calls), nil
		})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, full || conclusive)
	require.Equal(t, 2, calls)
}

func TestStateEchoConfirmationPreservesWireIdentityWithoutUserContent(t *testing.T) {
	body := []byte(`{"model":"gpt-6-astra","prompt_cache_key":"final-scoped-session","client_metadata":{"session_id":"final-scoped-session","thread_id":"final-thread","turn_id":"final-turn","root_turn_id":"root","x-codex-installation-id":"install","x-codex-turn-metadata":"metadata","unrelated":"private-value"},"reasoning":{"effort":"high"},"service_tier":"priority","input":"private-input","instructions":"private-instructions","tools":[{"name":"private-tool"}],"previous_response_id":"private-response"}`)
	template := gatewayPoolConfirmBody(body)
	for _, field := range []string{"model", "prompt_cache_key", "client_metadata.session_id", "client_metadata.thread_id", "client_metadata.turn_id",
		"client_metadata.root_turn_id", "client_metadata.x-codex-installation-id", "client_metadata.x-codex-turn-metadata", "reasoning", "service_tier"} {
		require.JSONEq(t, gjson.GetBytes(body, field).Raw, gjson.GetBytes(template, field).Raw, field)
	}
	require.NotContains(t, string(template), "private-")
	original, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(string(body)))
	require.NoError(t, err)
	original.Header.Set("Authorization", "Bearer offline-test")
	original.Header.Set("Cookie", "__cflb=offline; __oailb=offline")
	original.Header.Set("session-id", "final-scoped-session")
	original.Header.Set(openAICodexTurnStateHeader, "original")
	for _, encoding := range []string{"", "zstd"} {
		original.Header.Set("Content-Encoding", encoding)
		req, err := gatewayPoolConfirmationRequest(context.Background(), original, template, "new-state")
		require.NoError(t, err)
		for _, key := range []string{"Authorization", "Cookie", "session-id", "Content-Encoding"} {
			require.Equal(t, original.Header.Get(key), req.Header.Get(key))
		}
		require.Equal(t, "original", original.Header.Get(openAICodexTurnStateHeader))
		require.Equal(t, "new-state", req.Header.Get(openAICodexTurnStateHeader))
		require.Equal(t, HTTPUpstreamProfileOpenAIConfirmation, HTTPUpstreamProfileFromContext(req.Context()))
		wire, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.EqualValues(t, len(wire), req.ContentLength)
		if encoding == "zstd" {
			dec, err := zstd.NewReader(nil)
			require.NoError(t, err)
			wire, err = dec.DecodeAll(wire, nil)
			dec.Close()
			require.NoError(t, err)
		}
		require.Equal(t, template, wire)
	}
	original.GetBody = nil
	_, err = gatewayPoolConfirmTemplate(original)
	require.ErrorIs(t, err, errOpenAIGatewayPoolWarmUnverified)
}

func TestStateEchoConfirmationRejectsOriginalResponseGatewayDrift(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc := &OpenAIGatewayService{}
	ctx, sink := withOpenAIGatewayPoolSink(context.Background(), nil)
	sink.mark(OpenAIGatewayPoolApplied{AccountID: 1, Cookie: "offline", Gateway: "unified-142", Version: "old"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	req.Header.Set(openAICodexTurnStateHeader, "s0")
	resp := &http.Response{StatusCode: 200, Header: http.Header{}}
	for _, fragment := range strings.Split(gwpoolTestPairCookie(t, "unified-143"), ";") {
		resp.Header.Add("Set-Cookie", strings.TrimSpace(fragment))
	}
	resp.Header.Set(openAICodexTurnStateHeader, "s1")
	degraded, err := svc.gatewayPoolRouteDegraded(req, resp, account, "", gwpoolTestIdentity, time.Now())
	require.False(t, degraded)
	require.ErrorIs(t, err, errOpenAIGatewayPoolWarmUnverified)
	require.Empty(t, sink.snapshot().Verdict)
}

func TestStateEchoConfirmationDiscardDoesNotPoisonNewVersion(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := gwpoolTestAccount(1)
	ctx, sink := withOpenAIGatewayPoolSink(context.Background(), nil)
	sink.mark(OpenAIGatewayPoolApplied{AccountID: 1, Cookie: "offline", Gateway: "unified-142", Version: "old"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	newPair := openAIGatewayPoolPair{cookie: "offline", gateway: "unified-142", version: "new", until: time.Now().Add(time.Minute)}
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, newPair)
	body := &gwpoolEchoBody{}
	require.False(t, svc.dropDegradedGatewayPoolRoute(req, &http.Response{StatusCode: 200, Body: body}, account))
	got, state := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, newPair, got)
	require.Equal(t, openAIGatewayPoolPairLive, state)
	require.Empty(t, sink.discarded)
	require.True(t, body.closed)
}

func TestStateEchoConfirmationRetainsOriginalBusinessResponse(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: 200, minted: "s1", requestID: "business-response"},
		{status: 200, minted: "s2", requestID: "confirmation-one"},
		{status: 200, minted: "s2", requestID: "confirmation-two"},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	_, response, err := gwpoolEchoRun(t, svc, fake.account(1), "old")
	require.NoError(t, err)
	require.Equal(t, "business-response", response.Header.Get("x-request-id"))
	require.Equal(t, "s1", response.Header.Get(openAICodexTurnStateHeader))
	require.Equal(t, []string{"old", "s1", "s2"}, upstream.sentState)
	require.Len(t, upstream.bodies, 3)
	require.Same(t, upstream.bodies[0], response.Body)
	require.False(t, upstream.bodies[0].closed)
	require.Zero(t, upstream.bodies[0].reads)
	require.True(t, upstream.bodies[1].closed)
	require.True(t, upstream.bodies[2].closed)
	require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[0])
	require.NotContains(t, upstream.sentBodies[1], `"input":"x"`)
	require.Equal(t, http.StatusOK, response.StatusCode)
	_ = response.Body.Close()
}
