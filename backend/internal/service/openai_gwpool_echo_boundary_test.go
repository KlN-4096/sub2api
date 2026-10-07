package service

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type gatewayEchoBoundaryRepo struct {
	*gatewayRuntimeRepo
	onContact func()
}

func (r *gatewayEchoBoundaryRepo) UpdateExtra(ctx context.Context, id int64, patch map[string]any) error {
	err := r.gatewayRuntimeRepo.UpdateExtra(ctx, id, patch)
	if patch[openAIGatewayPoolContactsExtraKey] != nil && r.onContact != nil {
		r.onContact()
	}
	return err
}

func TestStateEchoFinalDeliveryRechecksAfterContactPersistence(t *testing.T) {
	for _, atContact := range []int{2, 3} {
		t.Run(string(rune('0'+atContact)), func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			account := fake.account(1)
			repo := &gatewayEchoBoundaryRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}}
			upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: 200, minted: "s1"}, {status: 200, minted: "s1"}}}
			svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}
			calls := 0
			repo.onContact = func() {
				calls++
				if calls == atContact {
					pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
					pair.version = "replacement"
					svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
				}
			}
			_, resp, err := gwpoolEchoRun(t, svc, account, "old")
			require.ErrorIs(t, err, errOpenAIGatewayPoolWarmUnverified)
			require.Nil(t, resp)
			require.True(t, upstream.bodies[0].closed)
			current, _ := repo.GetByID(context.Background(), 1)
			raw, _ := json.Marshal(current.Extra[openAIGatewayPoolMetricsExtraKey])
			var metrics gatewayPoolProbeMetrics
			require.NoError(t, json.Unmarshal(raw, &metrics))
			require.EqualValues(t, 1, metrics.Confirmation.Requests)
			require.Zero(t, metrics.Confirmation.Full+metrics.Confirmation.Degraded)
		})
	}
}

func TestStateEchoReferenceExpiryDoesNotSuppressConfirmedDegradation(t *testing.T) {
	store := &openAICodexCookieStore{}
	store.poolPairs.Store("identity", openAIGatewayPoolPair{cookie: "offline", version: "v", gateway: "g", until: time.Now().Add(-time.Second)})
	_, marked := store.gatewayPoolMarkStaleMatched("identity", "v", "g", true)
	require.True(t, marked)
	_, marked = store.gatewayPoolMarkStaleMatched("identity", "v", "g", true)
	require.False(t, marked, "a confirmed rejection is recorded once")
}

func TestStateEchoBuiltCompressedRequestKeepsFinalIdentities(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	account.Extra[codexFingerprintModeExtraKey] = "device"
	account.Extra[codexFingerprintConvergenceExtraKey] = true
	body := wireProfileTestBody(t)
	c := newConvTestContext(t, body)
	svc, _ := wireProfileTestService()
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: 200, minted: "s1"}, {status: 200, minted: "s1"}}}
	svc.httpUpstream = upstream
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), c)
	request, err := svc.buildUpstreamRequest(ctx, c, account, body, "offline-token", true, convTestSession, true)
	require.NoError(t, err)
	require.Equal(t, "zstd", request.Header.Get("Content-Encoding"))
	request.Header.Set(openAICodexTurnStateHeader, "old")
	gwpoolEchoSeedVerified(t, svc, account)
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	_ = response.Body.Close()
	require.Len(t, upstream.sentBodies, 2)
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	original, err := decoder.DecodeAll([]byte(upstream.sentBodies[0]), nil)
	require.NoError(t, err)
	confirmation, err := decoder.DecodeAll([]byte(upstream.sentBodies[1]), nil)
	require.NoError(t, err)
	for _, field := range []string{"model", "prompt_cache_key", "client_metadata.session_id", "client_metadata.thread_id"} {
		if gjson.GetBytes(original, field).Exists() {
			require.JSONEq(t, gjson.GetBytes(original, field).Raw, gjson.GetBytes(confirmation, field).Raw, field)
		}
	}
	require.Equal(t, "hi", gjson.GetBytes(confirmation, "input.0.content.0.text").String())
}
