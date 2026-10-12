package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func flushGatewayPoolActiveUsage(svc *OpenAIGatewayService, ctx context.Context, account *Account, identity string) {
	svc.changeGatewayPoolUsage(ctx, account, identity, func(*gatewayPoolUsageLedger) bool { return false })
}

func TestGatewayPoolActiveUsageClosesWhenAttemptFinishes(t *testing.T) {
	svc, repo, _, account, request, _ := ticketWaitFixture(t)
	gwpoolEchoSeedVerified(t, svc, account)
	ctx, complete := svc.beginGatewayPoolUsageRequest(request.Context(), account)
	defer complete()
	ctx, finish := beginGatewayPoolUsageAttempt(ctx)
	pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: pair.gateway, Version: pair.version}
	require.True(t, svc.noteGatewayPoolFullUse(ctx, account, gwpoolTestIdentity, applied, time.Now().UTC()))
	finish()
	fresh, err := repo.stored(account.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	round := state.Rounds[0]
	require.Equal(t, 1, round.Attempted)
	require.Equal(t, 1, round.Full)
	require.NotNil(t, round.ActiveUsage)
	require.False(t, round.ActiveUsage.Open)
}

func TestGatewayPoolActiveUsageInvalidationSurvivesReplacementAndFailedWrite(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	store := &svc.codexCookies
	pair := openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "old"}
	store.poolPairs.Store(gwpoolTestIdentity, pair)
	start := time.Now().UTC().Add(-time.Minute)
	key := gatewayPoolUsageTicketKey(pair.gateway, pair.version)
	tracker := store.activeUsageTracker(gwpoolTestIdentity)
	// Two physical business requests overlap on the same ticket.
	for i := range 2 {
		attempt := &gatewayPoolActiveUseAttempt{}
		attempt.event = gatewayPoolActiveUseEvent{
			ticket: key, start: start.Add(time.Duration(i) * time.Second), sentAt: start,
			requestStarted: start, attempt: attempt,
		}
		tracker.attempts[attempt] = struct{}{}
	}
	store.gatewayPoolMarkStale(gwpoolTestIdentity, pair.version)
	invalidated, _ := store.cachedPoolPair(gwpoolTestIdentity)
	events := tracker.snapshot()
	require.Len(t, events, 2)
	for _, event := range events {
		require.Equal(t, invalidated.invalidatedAt, event.invalidatedAt)
	}
	repo.fail = true
	flushGatewayPoolActiveUsage(svc, context.Background(), account, gwpoolTestIdentity)
	// New pair must not erase the old exact-version cutoff.
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{cookie: "new", gateway: "new", version: "new"})
	repo.fail = false
	flushGatewayPoolActiveUsage(svc, context.Background(), account, gwpoolTestIdentity)
	fresh, err := repo.stored(account.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	view := state.Rounds[0].fullUsageView(store.gatewayPoolUsageSession(), time.Now().Add(time.Hour), tracker.snapshot()...)
	require.Equal(t, invalidated.invalidatedAt.Sub(start).Milliseconds(), view.FullDurationMS)
}
