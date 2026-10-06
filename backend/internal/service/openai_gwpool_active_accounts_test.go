package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolActiveAccountsOrderedReplacementAndGroupIsolation(t *testing.T) {
	r := &gatewayPoolRounds{}
	present := map[int64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}
	require.Equal(t, map[string]int{"a": 1, "b": 2}, r.admit(1, []string{"a", "b", "c"}, present, true, 2))
	require.False(t, r.claim(1, "c", 2))
	require.True(t, r.claim(2, "c", 2), "groups have independent limits")
	r.rest(1, "a", time.Now().Add(time.Hour))
	require.Equal(t, map[string]int{"b": 1, "c": 2}, r.admit(1, []string{"a", "c", "b", "d"}, present, true, 2))
	r.recovered("a")
	require.Equal(t, map[string]int{"b": 1, "c": 2}, r.admit(1, []string{"a", "b", "c"}, present, true, 2),
		"a recovered primary does not evict the current full-strength windows")
	require.Equal(t, map[string]int{"b": 1}, r.admit(1, []string{"a", "b"}, present, true, 1))
	require.False(t, r.claim(1, "c", 1), "shrinking the setting must also constrain late selectors")
}

func TestGatewayPoolModelSubsetExhaustionPreservesHealthyActiveAccounts(t *testing.T) {
	group := int64(7)
	a, b, c := preferenceAccount(1, group, 20), preferenceAccount(2, group, 10), preferenceAccount(3, group, 30)
	a.Credentials["model_mapping"] = map[string]any{"gpt-6-astra": "gpt-6-astra"}
	b.Credentials["model_mapping"] = map[string]any{"gpt-6-astra": "gpt-6-astra"}
	c.Credentials["model_mapping"] = map[string]any{"only-other": "only-other"}
	svc := &OpenAIGatewayService{
		accountRepo:      gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b, *c}}},
		rateLimitService: gatewayPoolSchedulerTestSettings("legacy", 2),
	}
	aid, bid, cid := openAIGatewayPoolAccountKey(a), openAIGatewayPoolAccountKey(b), openAIGatewayPoolAccountKey(c)
	require.True(t, svc.codexCookies.poolRounds.claim(group, aid, 2))
	require.True(t, svc.codexCookies.poolRounds.claim(group, bid, 2))
	svc.codexCookies.poolRounds.rest(group, cid, time.Now().Add(time.Hour))
	ctx := svc.withGatewayPoolAccountPreferences(context.Background(), OpenAIAccountScheduleRequest{
		GroupID: &group, Platform: PlatformOpenAI, RequestedModel: "only-other",
		RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
	})
	require.Equal(t, uint64(0), svc.codexCookies.poolRounds.generation(group))
	require.Contains(t, gatewayPoolRoundExclusions(ctx, nil), c.ID)
	require.Equal(t, []string{gatewayPoolLedgerIdentity(aid), gatewayPoolLedgerIdentity(bid)},
		svc.codexCookies.poolRounds.groups[group].active, "a model subset cannot evict healthy group-wide primary accounts")
}

func TestGatewayPoolActiveAccountsUnknownSnapshotDoesNotOpenExtraSlots(t *testing.T) {
	r := &gatewayPoolRounds{}
	require.True(t, r.claim(1, "a", 1))
	require.Equal(t, map[string]int{"a": 1}, r.admit(1, []string{"b"}, map[int64]string{2: "b"}, false, 1))
	for _, raw := range []string{"", "0", "-1", "65", "1.5", "bad"} {
		require.Equal(t, 1, parseGatewayPoolActiveAccounts(raw))
	}
	require.Equal(t, 2, parseGatewayPoolActiveAccounts("2"))
}
