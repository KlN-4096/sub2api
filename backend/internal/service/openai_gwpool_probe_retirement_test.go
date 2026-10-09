package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolFailedProbeRetiresTicketAndContinues(t *testing.T) {
	svc, repo, fake, account, request, _ := ticketWaitFixture(t)
	repo.account = *account
	fake.forceCookie = gwpoolTestPairCookie(t, "unified-84")
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{
		{status: http.StatusOK, minted: "state"},
		{err: context.DeadlineExceeded},
	}}

	err := svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot)
	require.NoError(t, err, "an exhausted failed ticket must not end preparation while other candidates exist")
	require.Len(t, shooter.shots, 4)
	require.Equal(t, "unified-84", openAICodexRouteGateway(shooter.shots[2].cookie))
	_, cooling := svc.codexCookies.gatewayPoolUsedAt(gwpoolTestIdentity, "unified-142",
		account.gatewayPoolGatewayWindow())
	require.True(t, cooling)
	history, ok := readOpenAIGatewayHistory(&repo.account)
	require.True(t, ok)
	require.Contains(t, history.Seen, "unified-142")
	require.Empty(t, history.Seen["unified-142"].Verdict, "a timeout is not a measured quality failure")
	require.Equal(t, 3600, history.Seen["unified-142"].Cooldown.WindowSeconds)
	restarted := &openAICodexCookieStore{}
	restarted.gatewayPoolHydrateUsed(&repo.account, gwpoolTestIdentity)
	_, cooling = restarted.gatewayPoolUsedAt(gwpoolTestIdentity, "unified-142", account.gatewayPoolGatewayWindow())
	require.True(t, cooling, "failure cooldown survives restart")
	require.Zero(t, svc.GatewayPoolProgress([]int64{account.ID})[account.ID].Rejected)
}

func TestGatewayPoolFailedProbeHonorsRecoveryThenMovesOn(t *testing.T) {
	for _, retries := range []int{0, 1} {
		t.Run(string(rune('0'+retries)), func(t *testing.T) {
			svc, repo, fake, account, request, wait := ticketWaitFixture(t)
			account.Extra[openAIGatewayPoolRecoveryExtraKey] = retries
			repo.account = *account
			fake.cookieForHit = func(hit int64) string {
				if hit == 1 {
					return gwpoolTestPairCookie(t, "unified-142")
				}
				return gwpoolTestPairCookie(t, "unified-84")
			}
			sleeps := 0
			wait.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
			shooter := &gwpoolWarmShooter{}
			for i := 0; i <= retries; i++ {
				shooter.replies = append(shooter.replies, gwpoolWarmReply{status: http.StatusOK}) // no state
			}
			require.NoError(t, svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot))
			require.Equal(t, retries, sleeps)
			require.Len(t, shooter.shots, retries+3)
			require.Equal(t, "unified-84", openAICodexRouteGateway(shooter.shots[retries+1].cookie))
		})
	}
}

func TestGatewayPoolFailedProbeProtectsNewTicketAndClearGeneration(t *testing.T) {
	for _, mode := range []string{"replacement", "clear", "matching"} {
		t.Run(mode, func(t *testing.T) {
			store := &openAICodexCookieStore{}
			pair := openAIGatewayPoolPair{cookie: "offline", version: "new", gateway: "g"}
			store.poolPairs.Store(gwpoolTestIdentity, pair)
			applied := OpenAIGatewayPoolApplied{Version: "new", Gateway: "g"}
			if mode == "replacement" {
				applied.Version = "old"
			}
			if mode == "clear" {
				store.applyGatewayPoolCooldownClear(gwpoolTestIdentity, time.Now(), 3600)
			}
			retired := store.retireGatewayPoolFailedProbe(gwpoolTestIdentity, applied)
			next, _ := store.cachedPoolPair(gwpoolTestIdentity)
			require.Equal(t, mode == "matching", retired)
			require.Equal(t, mode == "matching", next.invalidated)
		})
	}
}

func TestGatewayPoolFailedProbeBeforeResponseStillCoolsLocally(t *testing.T) {
	svc, repo, fake, account, request, _ := ticketWaitFixture(t)
	fake.cookieForHit = func(hit int64) string {
		if hit == 1 {
			return gwpoolTestPairCookie(t, "unified-142")
		}
		return gwpoolTestPairCookie(t, "unified-84")
	}
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{{err: context.DeadlineExceeded}}}
	require.NoError(t, svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot))
	require.Len(t, shooter.shots, 3)
	require.Zero(t, fake.releaseHits.Load(), "unsent retirement is local only")
	_, cooling := svc.codexCookies.gatewayPoolUsedAt(gwpoolTestIdentity, "unified-142", account.gatewayPoolGatewayWindow())
	require.True(t, cooling)
	history, ok := readOpenAIGatewayHistory(&repo.account)
	require.True(t, ok)
	require.Empty(t, history.Seen["unified-142"].Verdict)
	require.Equal(t, 3600, history.Seen["unified-142"].Cooldown.WindowSeconds)
}

func TestGatewayPoolLateWarmVerdictCannotInvalidateReplacementOnSameGateway(t *testing.T) {
	store := &openAICodexCookieStore{}
	account := gwpoolTestAccount(1)
	old := openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "old"}
	store.poolPairs.Store(gwpoolTestIdentity, old)
	replacement := old
	replacement.version = "new"
	applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: "g", Version: "old"}
	full, conclusive, _, _, err := store.gatewayPoolWarmVerdict(context.Background(), account,
		gwpoolTestIdentity, applied, old.cookie, 1, func(_ context.Context, _, state string) (int, string, error) {
			if state == "" {
				return http.StatusOK, "s1", nil
			}
			store.poolPairs.Store(gwpoolTestIdentity, replacement)
			store.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "new", gatewayPoolProbeModelLuna)
			return http.StatusOK, "s2", nil
		})
	require.NoError(t, err)
	require.False(t, full)
	require.True(t, conclusive)
	got, live := store.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, replacement, got, "a late old-version result cannot retire a new ticket even on the same gateway")
	require.Equal(t, openAIGatewayPoolPairLive, live)
	require.True(t, store.gatewayPoolVerifiedFull(gwpoolTestIdentity))
}
