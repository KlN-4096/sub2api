package service

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolFullUsageSumsTicketsNotRoundWallClock(t *testing.T) {
	state := gatewayPoolUsageLedger{Tag: "offline"}
	start := time.Now().UTC()
	at := func(seconds int) time.Time { return start.Add(time.Duration(seconds) * time.Second) }
	state.note("luna", "one", at(0), true) // verification is not business use
	require.Zero(t, state.Rounds[0].fullUseDuration(at(10)))
	require.True(t, state.startFullUse("one", at(10), at(1000), "process"))
	state.note("astra", "one", at(20), true) // same ticket, different model
	require.True(t, state.startFullUse("one", at(20), at(1000), "process"))
	require.EqualValues(t, 60000, state.Rounds[0].fullUseDuration(at(70)), "idle while holding the ticket counts")
	state.endFullUse("one", at(70))
	state.note("astra", "two", at(90), true)
	state.startFullUse("two", at(100), at(1000), "process")
	state.endFullUse("two", at(160))
	state.end(at(200)) // only later is zero supply observed
	require.Len(t, state.Rounds, 1)
	require.Equal(t, 2, state.Rounds[0].Attempted)
	require.Equal(t, 2, state.Rounds[0].Full)
	require.Equal(t, at(10), state.Rounds[0].FullStartedAt)
	require.EqualValues(t, 120000, state.Rounds[0].fullUseDuration(at(200)), "no verification, gap or final waiting")
}

func TestGatewayPoolFullUsageExpiryAndRestartDoNotInventTail(t *testing.T) {
	start := time.Now().UTC()
	state := gatewayPoolUsageLedger{Tag: "offline"}
	state.note("luna", "one", start, true)
	state.startFullUse("one", start, start.Add(time.Minute), "before")
	state.startFullUse("one", start.Add(20*time.Second), start.Add(time.Minute), "before")
	live := map[string]openAIGatewayPoolPair{"one": {cookie: "offline", version: "v"}}
	view := state.Rounds[0].fullUsageView(live, "before", start.Add(2*time.Minute))
	require.EqualValues(t, 60000, view.FullDurationMS)
	require.Empty(t, view.FullActiveUntil)
	require.False(t, view.DurationIncomplete)
	view = state.Rounds[0].fullUsageView(live, "after", start.Add(2*time.Minute))
	require.EqualValues(t, 20000, view.FullDurationMS)
	require.True(t, view.DurationIncomplete, "unknown process tail is a lower bound, not uptime")
	require.Zero(t, state.Rounds[0].Tickets["one"].UseEndedAt, "projection must not mutate durable state")
}

func TestGatewayPoolFullUsageLegacyWallClockRemainsSeparate(t *testing.T) {
	start := time.Now().UTC()
	state := gatewayPoolUsageLedger{Tag: "offline", Rounds: []GatewayPoolUsageRound{
		{ID: "old", Model: "luna", StartedAt: start.Add(-time.Hour), Attempted: 4, Full: 3},
	}}
	state.note("luna", "new", start, true)
	state.startFullUse("new", start, time.Time{}, "process")
	require.Len(t, state.Rounds, 2)
	require.Zero(t, state.Rounds[0].fullUsageView(nil, "process", start).FullDurationMS)
	require.EqualValues(t, 10000, state.Rounds[1].fullUsageView(
		map[string]openAIGatewayPoolPair{"new": {cookie: "offline"}}, "process", start.Add(10*time.Second)).FullDurationMS)
}

func TestGatewayPoolFullUsageKeepsEarliestOutOfOrderSend(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	start := time.Now().UTC().Add(-time.Minute)
	applied := OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v"})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "luna")
	svc.noteGatewayPoolFullUse(context.Background(), account, identity, applied, start.Add(2*time.Second))
	svc.noteGatewayPoolFullUse(context.Background(), account, identity, applied, start)
	fresh, err := repo.GetByID(context.Background(), 1)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Equal(t, start, state.Rounds[0].Tickets[gatewayPoolUsageTicketKey("g", "v")].UseStartedAt)
}

func TestGatewayPoolFullUsageFailedEndDoesNotAccumulateWaiting(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	start := time.Now().UTC().Add(-time.Minute)
	applied := OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}
	pair := openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v"}
	svc.codexCookies.poolPairs.Store(identity, pair)
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "luna")
	svc.noteGatewayPoolFullUse(context.Background(), account, identity, applied, start)
	pair.invalidated, pair.invalidatedAt = true, start.Add(30*time.Second)
	svc.codexCookies.poolPairs.Store(identity, pair)
	repo.fail = true
	svc.endGatewayPoolFullUse(context.Background(), account, identity, applied, pair.invalidatedAt)
	repo.fail = false
	fresh, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	for _, elapsed := range []time.Duration{time.Minute, time.Hour} {
		view := state.Rounds[0].fullUsageView(svc.codexCookies.gatewayPoolUsageLive(identity),
			svc.codexCookies.gatewayPoolUsageSession(), start.Add(elapsed))
		require.EqualValues(t, 30000, view.FullDurationMS)
	}
}

func TestGatewayPoolFullUsageSettlesBeforeExpiredTicketIsReplaced(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.poolUsageSettle = svc.settleGatewayPoolFullUsage
	identity := openAIGatewayPoolAccountKey(account)
	start := time.Now().UTC().Add(-10 * time.Minute)
	applied := OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "old", Version: "v"}
	pair := openAIGatewayPoolPair{cookie: "offline", gateway: "old", version: "v", routeExpiresAt: time.Now().Add(time.Hour)}
	svc.codexCookies.poolPairs.Store(identity, pair)
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "luna")
	svc.noteGatewayPoolFullUse(context.Background(), account, identity, applied, start)
	// Persist an exact deadline before replacement, without wall-clock sleeps.
	end := start.Add(9 * time.Minute)
	svc.changeGatewayPoolUsage(context.Background(), account, identity, func(state *gatewayPoolUsageLedger) bool {
		key := gatewayPoolUsageTicketKey("old", "v")
		ticket := state.Rounds[0].Tickets[key]
		ticket.UseExpiresAt = end
		state.Rounds[0].Tickets[key] = ticket
		return true
	})
	pair.routeExpiresAt = end
	svc.codexCookies.poolPairs.Store(identity, pair)
	require.NoError(t, attachRoute(context.Background(), &svc.codexCookies, account, gwpoolTestURL, http.Header{}))
	fresh, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.EqualValues(t, 540000, state.Rounds[0].fullUseDuration(time.Now()))
	require.False(t, state.Rounds[0].DurationIncomplete)
}

func TestGatewayPoolFullUsageStartsOnBusinessWriteBeforeResponse(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	svc, repo := gatewayRuntimeService(account)
	gwpoolEchoSeedVerified(t, svc, account)
	upstream := &gatewayPoolUsageHeldUpstream{written: make(chan struct{}), release: make(chan struct{})}
	svc.httpUpstream = upstream
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), nil)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = svc.doOpenAIUpstream(request, "", account)
	}()
	<-upstream.written
	fresh, readErr := repo.GetByID(context.Background(), 1)
	close(upstream.release)
	<-done
	require.NoError(t, readErr)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.False(t, state.Rounds[0].FullStartedAt.IsZero())
	require.Equal(t, 1, state.Rounds[0].Full)
	require.Equal(t, 1, state.Rounds[0].Attempted)
}
