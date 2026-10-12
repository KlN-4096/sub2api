package service

import (
	"context"
	"net/http"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type gatewayEchoBoundaryRepo struct {
	*gatewayRuntimeRepo
	onContact func()
}

func (r *gatewayEchoBoundaryRepo) WriteGatewayPoolLedgers(ctx context.Context, id int64, ledgers, tags map[string]any) error {
	err := r.gatewayRuntimeRepo.WriteGatewayPoolLedgers(ctx, id, ledgers, tags)
	if ledgers[openAIGatewayPoolContactsExtraKey] != nil && r.onContact != nil {
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
		})
	}
}

func TestStateEchoReferenceExpiryDoesNotSuppressConfirmedDegradation(t *testing.T) {
	store := &openAICodexCookieStore{}
	store.poolPairs.Store("identity", openAIGatewayPoolPair{cookie: "offline", version: "v", gateway: "g"})
	_, _, marked := store.gatewayPoolMarkStaleMatched("identity", "v")
	require.True(t, marked)
	_, _, marked = store.gatewayPoolMarkStaleMatched("identity", "v")
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
	require.Equal(t, "gpt-5.5", gjson.GetBytes(original, "model").String(), "business model stays unchanged")
	require.Equal(t, gatewayPoolProbeModelLuna, gjson.GetBytes(confirmation, "model").String())
	for _, field := range []string{"prompt_cache_key", "client_metadata.session_id", "client_metadata.thread_id"} {
		if gjson.GetBytes(original, field).Exists() {
			require.JSONEq(t, gjson.GetBytes(original, field).Raw, gjson.GetBytes(confirmation, field).Raw, field)
		}
	}
	require.Equal(t, "hi", gjson.GetBytes(confirmation, "input.0.content.0.text").String())
}
