package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFullWindowSamplesOnlyCountVerifiedPairs(t *testing.T) {
	store := &openAICodexCookieStore{}
	require.Zero(t, store.gatewayPoolNoteFullWindow("identity", "unverified"))
	store.poolVerified.Store("identity", gatewayPoolVerifiedMark{
		version: "verified", at: time.Now().Add(-3 * time.Minute),
	})
	require.Zero(t, store.gatewayPoolNoteFullWindow("identity", "different"))
	require.InDelta(t, 180, store.gatewayPoolNoteFullWindow("identity", "verified").Seconds(), 1)
	store.poolVerified.Store("identity", gatewayPoolVerifiedMark{
		version: "verified", at: time.Now().Add(time.Minute),
	})
	require.Zero(t, store.gatewayPoolNoteFullWindow("identity", "verified"))
}

func TestVerifiedMarkKeepsTheWindowStartForTheSameTicket(t *testing.T) {
	store := &openAICodexCookieStore{}
	before := gatewayPoolVerifiedMark{version: "first", at: time.Now().Add(-time.Minute)}
	store.poolVerified.Store(gwpoolTestIdentity, before)
	store.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "first")
	after, ok := store.gatewayPoolVerifiedMarkOf(gwpoolTestIdentity)
	require.True(t, ok)
	require.Equal(t, before, after)
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{cookie: "offline", version: "next"})
	store.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "next")
	after, _ = store.gatewayPoolVerifiedMarkOf(gwpoolTestIdentity)
	require.True(t, after.at.After(before.at))
}

// 2026-10-11 unified-132: the same ticket number came back in a new cycle after
// 14 idle minutes, and the card showed 1237s for a few seconds of business use.
func TestGatewayPoolFullUseCountsOnlyThisCycleOfThisTicket(t *testing.T) {
	store := &openAICodexCookieStore{}
	identity, tag := gwpoolTestIdentity, gatewayPoolLedgerTag(gwpoolTestIdentity)
	now := time.Now().UTC()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	store.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "unified-132", version: "v132",
		firstSent: ago(10 * time.Second)})
	store.poolVerified.Store(identity, gatewayPoolVerifiedMark{version: "v132", at: ago(20 * time.Minute)})
	store.poolUsageCache.Store(tag, &gatewayPoolUsageLedger{Tag: tag, Rounds: []GatewayPoolUsageRound{
		{ID: "earlier", Model: gatewayPoolUsageSharedModel, StartedAt: ago(30 * time.Minute), EndedAt: ago(14 * time.Minute),
			ActiveUsage: &gatewayPoolActiveUsage{Intervals: []gatewayPoolUsageInterval{{Start: ago(20 * time.Minute), End: ago(14 * time.Minute)}}}},
		{ID: "current", Model: gatewayPoolUsageSharedModel, StartedAt: ago(8 * time.Minute),
			ActiveUsage: &gatewayPoolActiveUsage{Intervals: []gatewayPoolUsageInterval{
				{Start: ago(6 * time.Minute), End: ago(5 * time.Minute)}, // the ticket before this one
				{Start: ago(5 * time.Second), End: ago(2 * time.Second)},
			}}},
	}})
	attempt := &gatewayPoolActiveUseAttempt{}
	attempt.event = gatewayPoolActiveUseEvent{roundID: "current", ticket: gatewayPoolUsageTicketKey("unified-132", "v132"),
		start: ago(time.Second), sentAt: ago(time.Second), requestStarted: ago(time.Second), attempt: attempt}
	store.activeUsageTracker(identity).attempts[attempt] = struct{}{}

	used := store.gatewayPoolMarkStale(identity, "v132")
	require.InDelta(t, 4, used.Seconds(), 0.5, "3s already persisted plus the 1s in flight; idle, the old cycle and the previous ticket do not count")
	require.InDelta(t, 1200, store.gatewayPoolNoteFullWindow(identity, "v132").Seconds(), 1,
		"the ticket window the pool and local ranking use is unchanged")
}

// When the last persisted cycle has ended, only requests started after it count,
// like the cycle tile: one still streaming from before the close stays with that cycle.
func TestGatewayPoolFullUseStartsAfterAnEndedCycle(t *testing.T) {
	store := &openAICodexCookieStore{}
	identity, tag := gwpoolTestIdentity, gatewayPoolLedgerTag(gwpoolTestIdentity)
	now := time.Now().UTC()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	store.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v", firstSent: ago(10 * time.Second)})
	store.poolUsageCache.Store(tag, &gatewayPoolUsageLedger{Tag: tag,
		ClosedBefore: map[string]time.Time{gatewayPoolUsageSharedModel: ago(3 * time.Second)},
		Rounds: []GatewayPoolUsageRound{{
			ID: "ended", Model: gatewayPoolUsageSharedModel, StartedAt: ago(time.Minute), EndedAt: ago(3 * time.Second),
			ActiveUsage: &gatewayPoolActiveUsage{Intervals: []gatewayPoolUsageInterval{{Start: ago(8 * time.Second), End: ago(3 * time.Second)}}},
		}}})
	for _, started := range []time.Duration{5 * time.Second, time.Second} {
		attempt := &gatewayPoolActiveUseAttempt{}
		attempt.event = gatewayPoolActiveUseEvent{ticket: gatewayPoolUsageTicketKey("g", "v"), start: ago(started),
			sentAt: ago(started), requestStarted: ago(started), attempt: attempt}
		store.activeUsageTracker(identity).attempts[attempt] = struct{}{}
	}
	require.InDelta(t, 1, store.gatewayPoolMarkStale(identity, "v").Seconds(), 0.3)
}

// With one ticket in the cycle, the card's reading is the cycle tile's full-use
// time: finished (persisted, acknowledged) and in-flight business, nothing else.
func TestGatewayPoolFullUseMatchesTheCycleTileForItsOnlyTicket(t *testing.T) {
	svc, repo, _, account, request, _ := ticketWaitFixture(t)
	gwpoolEchoSeedVerified(t, svc, account)
	store := &svc.codexCookies
	pair, _ := store.cachedPoolPair(gwpoolTestIdentity)
	store.gatewayPoolMarkSent(gwpoolTestIdentity, pair.version, time.Now().UTC().Add(-time.Minute))
	applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: pair.gateway, Version: pair.version}
	ctx, complete := svc.beginGatewayPoolUsageRequest(request.Context(), account)
	defer complete()
	first, finishFirst := beginGatewayPoolUsageAttempt(ctx)
	require.True(t, svc.noteGatewayPoolFullUse(first, account, gwpoolTestIdentity, applied, time.Now().UTC()))
	time.Sleep(30 * time.Millisecond)
	finishFirst()
	time.Sleep(30 * time.Millisecond) // idle between the two requests
	second, finishSecond := beginGatewayPoolUsageAttempt(ctx)
	defer finishSecond()
	require.True(t, svc.noteGatewayPoolFullUse(second, account, gwpoolTestIdentity, applied, time.Now().UTC()))
	time.Sleep(30 * time.Millisecond)

	used := store.gatewayPoolMarkStale(gwpoolTestIdentity, pair.version)
	flushGatewayPoolActiveUsage(svc, context.Background(), account, gwpoolTestIdentity)
	fresh, err := repo.stored(account.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	tile := state.Rounds[0].fullUsageView(time.Now(), store.activeUsageTracker(gwpoolTestIdentity).snapshot()...)
	// The tile never counts the idle gap, so equality is what proves it is excluded.
	require.InDelta(t, tile.FullDurationMS, used.Milliseconds(), 2)
	require.GreaterOrEqual(t, used, 55*time.Millisecond)
}

// A cycle closed concurrently after the verdict is still the verdict's cycle, and a
// cycle full of older intervals cannot crowd this ticket's latest use out of the cap.
func TestGatewayPoolFullUseKeepsTheVerdictsCycleAndItsLatestUse(t *testing.T) {
	store := &openAICodexCookieStore{}
	identity, tag := gwpoolTestIdentity, gatewayPoolLedgerTag(gwpoolTestIdentity)
	now := time.Now().UTC()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	store.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v", firstSent: ago(10 * time.Second)})
	older := make([]gatewayPoolUsageInterval, 0, gatewayPoolActiveUsageIntervalLimit)
	for i := range gatewayPoolActiveUsageIntervalLimit {
		start := ago(time.Hour).Add(time.Duration(2*i) * 100 * time.Millisecond)
		older = append(older, gatewayPoolUsageInterval{Start: start, End: start.Add(100 * time.Millisecond)})
	}
	later := now.Add(time.Minute) // closed after the verdict below
	store.poolUsageCache.Store(tag, &gatewayPoolUsageLedger{Tag: tag,
		ClosedBefore: map[string]time.Time{gatewayPoolUsageSharedModel: later},
		Rounds: []GatewayPoolUsageRound{{
			ID: "verdict-cycle", Model: gatewayPoolUsageSharedModel, StartedAt: ago(2 * time.Hour), EndedAt: later,
			ActiveUsage: &gatewayPoolActiveUsage{Intervals: append(older, gatewayPoolUsageInterval{Start: ago(5 * time.Second), End: ago(3 * time.Second)})},
		}}})
	attempt := &gatewayPoolActiveUseAttempt{}
	attempt.event = gatewayPoolActiveUseEvent{roundID: "verdict-cycle", ticket: gatewayPoolUsageTicketKey("g", "v"),
		start: ago(time.Second), sentAt: ago(time.Second), requestStarted: ago(time.Second), attempt: attempt}
	store.activeUsageTracker(identity).attempts[attempt] = struct{}{}
	require.InDelta(t, 3, store.gatewayPoolMarkStale(identity, "v").Seconds(), 0.3, "2s persisted plus 1s in flight")
}
