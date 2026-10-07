package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolRoundsAreIdempotentIsolatedAndIgnoreLateConfirmation(t *testing.T) {
	var rounds gatewayPoolRounds
	cohort := map[int64]string{1: "a", 2: "a", 3: "b"} // two rows, one credential vote
	var workers sync.WaitGroup
	for range 20 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			rounds.exhaust(7, "a", 0)
		}()
	}
	workers.Wait()
	excluded, restarted := rounds.snapshot(7, cohort, true)
	require.Equal(t, map[int64]struct{}{1: {}, 2: {}}, excluded)
	require.False(t, restarted)
	other, _ := rounds.snapshot(8, cohort, true)
	require.Empty(t, other, "groups must not share exhaustion")
	rounds.exhaust(7, "b", 0)
	excluded, restarted = rounds.snapshot(7, cohort, false)
	require.Len(t, excluded, 3, "incomplete repository evidence cannot reset the round")
	require.False(t, restarted)
	excluded, restarted = rounds.snapshot(7, cohort, true)
	require.Empty(t, excluded)
	require.True(t, restarted)
	rounds.exhaust(7, "a", 0)
	require.False(t, rounds.blocked(7, "a"), "late confirmation belongs to the old round")
	require.Equal(t, uint64(1), rounds.generation(7))
}

func TestGatewayPoolRoundsRestIncludesActualProbesAndKeepsRequestCloneExclusions(t *testing.T) {
	group := int64(7)
	a, b, clone := preferenceAccount(1, group, 20), preferenceAccount(2, group, 1), preferenceAccount(3, group, 20)
	clone.Credentials = a.Credentials
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b, *clone}}}
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}, rateLimitService: gatewayPoolSchedulerTestSettings("legacy")}
	aid, bid := openAIGatewayPoolAccountKey(a), openAIGatewayPoolAccountKey(b)
	now := time.Now()
	svc.codexCookies.poolRounds.touch(aid, now.Add(-time.Hour))
	// Even without a cached business pair, an actual background probe ends rest.
	svc.codexCookies.gatewayPoolMarkSent(aid, "background-candidate", now)
	svc.codexCookies.poolRounds.touch(bid, now.Add(-time.Minute))
	svc.codexCookies.poolRounds.exhaust(group, aid, 0)
	svc.codexCookies.poolRounds.exhaust(group, bid, 0)
	req := OpenAIAccountScheduleRequest{GroupID: &group, Platform: PlatformOpenAI,
		RequestedModel: "gpt-6-astra", RequiredTransport: OpenAIUpstreamTransportHTTPSSE}
	ctx := svc.withGatewayPoolAccountPreferences(gatewayPoolTestGroupContext(group, 2), req)
	prefs := gatewayPoolPreferences(ctx)
	require.True(t, gatewayPoolPreferenceBetter(prefs[b.ID], prefs[a.ID]), "rest beats cooled count after reset")
	require.Equal(t, prefs[a.ID].lastTouch, prefs[clone.ID].lastTouch)
	require.Empty(t, gatewayPoolRoundExclusions(ctx, nil))
	rotating := context.WithValue(gatewayPoolTestGroupContext(group, 2), gatewayPoolRotationKey{}, &gatewayPoolRotation{
		groupID: group, attempted: map[int64]struct{}{a.ID: {}}, domains: map[string]struct{}{gatewayPoolLedgerIdentity(aid): {}},
	})
	ctx = svc.withGatewayPoolAccountPreferences(rotating, req)
	require.Contains(t, gatewayPoolRoundExclusions(ctx, nil), clone.ID,
		"reset must not allow the same credential through another row in one request")
}

func TestGatewayPoolRoundsDoNotWaitForDisabledOrIncompatibleAccounts(t *testing.T) {
	for _, reason := range []string{"disabled", "model", "pool-off"} {
		t.Run(reason, func(t *testing.T) {
			group := int64(7)
			a, b := preferenceAccount(1, group, 1), preferenceAccount(2, group, 20)
			switch reason {
			case "disabled":
				b.Schedulable = false
			case "model":
				b.Credentials["model_mapping"] = map[string]any{"other": "other"}
			case "pool-off":
				b.Extra[openAIGatewayPoolExtraKey] = false
			}
			repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}}
			svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}}
			svc.codexCookies.poolRounds.exhaust(group, openAIGatewayPoolAccountKey(a), 0)
			ctx := svc.withGatewayPoolAccountPreferences(context.Background(), OpenAIAccountScheduleRequest{
				GroupID: &group, Platform: PlatformOpenAI, RequestedModel: "gpt-6-astra",
				RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
			})
			if reason == "model" {
				require.Equal(t, uint64(0), svc.codexCookies.poolRounds.generation(group))
				require.Contains(t, gatewayPoolRoundExclusions(ctx, nil), a.ID,
					"a healthy account for another model still belongs to the group-wide round")
			} else {
				require.Equal(t, uint64(1), svc.codexCookies.poolRounds.generation(group))
				require.NotContains(t, gatewayPoolRoundExclusions(ctx, nil), a.ID)
			}
		})
	}
}
