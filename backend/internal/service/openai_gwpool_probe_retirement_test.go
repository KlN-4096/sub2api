package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolFailedProbeRetiresTicketAndContinues(t *testing.T) {
	svc, repo, fake, account, request, wait := ticketWaitFixture(t)
	repo.account = *account
	fake.forceCookie = gwpoolTestPairCookie(t, "unified-84")
	sleeps := 0
	wait.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
	// 142 两轮都在 B 超时：等一次、重验同一张票，第二次还超时才退休换 84。
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{
		{status: http.StatusOK, minted: "state"}, {err: context.DeadlineExceeded},
		{status: http.StatusOK, minted: "state"}, {err: context.DeadlineExceeded},
	}}

	err := svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot)
	require.NoError(t, err, "an exhausted failed ticket must not end preparation while other candidates exist")
	require.Equal(t, 1, sleeps)
	require.Len(t, shooter.shots, 6)
	require.Equal(t, "unified-142", openAICodexRouteGateway(shooter.shots[2].cookie), "重验的是同一张票")
	require.Equal(t, "unified-84", openAICodexRouteGateway(shooter.shots[4].cookie))
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

// 准备阶段与业务阶段同一条规则（gatewayPoolTransientAttemptsPerTicket）：送达过的票临时
// 失败，等一次在同一张票上重验，第二次还失败才退休换票。
func TestGatewayPoolFailedProbeRetriesTheTicketOnceThenMovesOn(t *testing.T) {
	svc, _, fake, account, request, wait := ticketWaitFixture(t)
	fake.cookieForHit = func(hit int64) string {
		if hit == 1 {
			return gwpoolTestPairCookie(t, "unified-142")
		}
		return gwpoolTestPairCookie(t, "unified-84")
	}
	sleeps := 0
	wait.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{{status: http.StatusOK}, {status: http.StatusOK}}} // no state
	require.NoError(t, svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot))
	require.Equal(t, 1, sleeps)
	require.Len(t, shooter.shots, 4)
	require.Equal(t, "unified-142", openAICodexRouteGateway(shooter.shots[1].cookie), "重验的是同一张票")
	require.Equal(t, "unified-84", openAICodexRouteGateway(shooter.shots[2].cookie))
}

// 重验与退休同一组可恢复错误：送达后被取消不是临时故障，照旧直接结束准备。
func TestGatewayPoolCanceledProbeIsNotRetried(t *testing.T) {
	svc, _, _, account, request, wait := ticketWaitFixture(t)
	sleeps := 0
	wait.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{
		{status: http.StatusOK, minted: "state"}, {err: context.Canceled},
	}}
	err := svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot)
	require.ErrorIs(t, err, errOpenAIGatewayPoolWarmUnverified)
	require.Zero(t, sleeps)
	require.Len(t, shooter.shots, 2)
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
	svc, repo, fake, account, request, wait := ticketWaitFixture(t)
	sleeps := 0
	wait.sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
	fake.cookieForHit = func(hit int64) string {
		if hit == 1 {
			return gwpoolTestPairCookie(t, "unified-142")
		}
		return gwpoolTestPairCookie(t, "unified-84")
	}
	shooter := &gwpoolWarmShooter{replies: []gwpoolWarmReply{{err: context.DeadlineExceeded}}}
	require.NoError(t, svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel, shooter.shoot))
	require.Len(t, shooter.shots, 3)
	require.Zero(t, sleeps, "没送达的票已在本地清掉，不等也不重验")
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
