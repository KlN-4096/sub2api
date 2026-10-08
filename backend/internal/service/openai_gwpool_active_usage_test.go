package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type gatewayPoolActiveUsageReadRepo struct {
	*gatewayRuntimeRepo
	failRead bool
}

func (r *gatewayPoolActiveUsageReadRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	if r.failRead {
		return nil, errors.New("offline read failure")
	}
	return r.gatewayRuntimeRepo.GetByID(ctx, id)
}

func TestGatewayPoolActiveUsageReplaysFailedInitialReadOrWrite(t *testing.T) {
	for _, failure := range []string{"read", "write"} {
		t.Run(failure, func(t *testing.T) {
			account := gwpoolTestAccount(1)
			svc, base := gatewayRuntimeService(account)
			repo := &gatewayPoolActiveUsageReadRepo{gatewayRuntimeRepo: base}
			svc.accountRepo = repo
			identity := openAIGatewayPoolAccountKey(account)
			applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: "g", Version: "v"}
			svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{gateway: "g", version: "v", cookie: "offline"})
			svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "luna")
			ctx, complete := svc.beginGatewayPoolUsageRequest(context.Background(), account)
			defer complete()
			ctx, finish := beginGatewayPoolUsageAttempt(ctx)
			repo.failRead, base.fail = failure == "read", failure == "write"
			require.True(t, svc.noteGatewayPoolFullUse(ctx, account, identity, applied, time.Now().UTC()))
			finish()
			tracker := svc.codexCookies.activeUsageTracker(identity)
			require.Len(t, tracker.snapshot(), 1, "retain the original attested send until it can be persisted")
			repo.failRead, base.fail = false, false
			flushGatewayPoolActiveUsage(svc, ctx, account, identity)
			require.Empty(t, tracker.snapshot())
			fresh, err := repo.GetByID(ctx, account.ID)
			require.NoError(t, err)
			state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
			require.Len(t, state.Rounds, 1)
			require.Equal(t, 1, state.Rounds[0].Full)
			require.Equal(t, 1, state.Rounds[0].Attempted)
			require.NotNil(t, state.Rounds[0].ActiveUsage)
			require.False(t, state.Rounds[0].ActiveUsage.Open)
		})
	}
}

func TestGatewayPoolActiveUsageRejectsClosedLateSendWithoutLeaking(t *testing.T) {
	svc, _, _, account, request, _ := ticketWaitFixture(t)
	gwpoolEchoSeedVerified(t, svc, account)
	ctx, complete := svc.beginGatewayPoolUsageRequest(request.Context(), account)
	defer complete()
	ctx, finish := beginGatewayPoolUsageAttempt(ctx)
	svc.changeGatewayPoolUsage(ctx, account, gwpoolTestIdentity, func(state *gatewayPoolUsageLedger) bool {
		return state.end(time.Now().UTC(), "temporarily_unschedulable")
	})
	pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: pair.gateway, Version: pair.version}
	svc.noteGatewayPoolFullUse(ctx, account, gwpoolTestIdentity, applied, time.Now().UTC())
	finish()
	require.Empty(t, svc.codexCookies.activeUsageTracker(gwpoolTestIdentity).snapshot())
}

func TestGatewayPoolActiveUsageFailedNewTicketWriteKeepsExistingCycleCounts(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	ctx, complete := svc.beginGatewayPoolUsageRequest(context.Background(), account)
	defer complete()
	previous := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: "previous", Version: "a"}
	svc.noteGatewayPoolUsage(ctx, account, identity, "luna", previous, time.Now().UTC(), true)
	applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: "next", Version: "b"}
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{gateway: "next", version: "b", cookie: "offline"})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "b", "luna")
	ctx, finish := beginGatewayPoolUsageAttempt(ctx)
	repo.fail = true
	svc.noteGatewayPoolFullUse(ctx, account, identity, applied, time.Now().UTC())
	finish()
	repo.fail = false
	flushGatewayPoolActiveUsage(svc, ctx, account, identity)
	require.Empty(t, svc.codexCookies.activeUsageTracker(identity).snapshot())
	fresh, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Len(t, state.Rounds, 1)
	require.Len(t, state.Rounds[0].Tickets, 2)
	require.Equal(t, 2, state.Rounds[0].Attempted)
	require.Equal(t, 2, state.Rounds[0].Full)
	require.NotNil(t, state.Rounds[0].ActiveUsage)
	require.False(t, state.Rounds[0].Tickets[gatewayPoolUsageTicketKey("next", "b")].At.IsZero())
}

func TestGatewayPoolActiveUsageUnionsBusinessIntervalsAndHidesLegacy(t *testing.T) {
	start := time.Now().UTC().Add(-time.Hour)
	at := func(seconds int) time.Time { return start.Add(time.Duration(seconds) * time.Second) }
	round := GatewayPoolUsageRound{ID: "r", Model: gatewayPoolUsageSharedModel,
		Attempted: 20, Full: 20, StartedAt: start,
		Tickets: map[string]gatewayPoolUsageTicket{
			"one": {Full: true, UseStartedAt: start, UseEndedAt: at(3600)},
			"two": {Full: true},
		}}
	legacy := round.fullUsageView("process", at(3600))
	require.Zero(t, legacy.FullDurationMS)
	require.Empty(t, legacy.FullUsageMode)
	require.Equal(t, 20, legacy.Attempted)
	events := []gatewayPoolActiveUseEvent{
		{roundID: "r", ticket: "one", start: at(0), end: at(10)},
		{roundID: "r", ticket: "one", start: at(5), end: at(20)},
		{roundID: "r", ticket: "two", start: at(15), end: at(25)},
		{roundID: "r", ticket: "one", start: at(40), end: at(50)},
		{roundID: "other", ticket: "one", start: at(50), end: at(500)},
	}
	round.syncActiveUsage(events, "process", at(60))
	require.EqualValues(t, 35000, round.ActiveUsage.duration(at(60)),
		"overlapping requests and tickets count once; idle 25..40 is excluded")
	round.syncActiveUsage(events, "process", at(600))
	require.EqualValues(t, 35000, round.ActiveUsage.duration(at(600)), "repeated persistence is idempotent")
	require.Equal(t, at(3600), round.Tickets["one"].UseEndedAt, "retain the old raw record")
	view := round.fullUsageView("process", at(3600))
	require.Equal(t, gatewayPoolActiveUsageMode, view.FullUsageMode)
	require.EqualValues(t, 35000, view.FullDurationMS)
	require.Nil(t, view.ActiveUsage, "polling never exports interval detail")
}

func TestGatewayPoolActiveUsageRestartKeepsOnlyObservedTime(t *testing.T) {
	start := time.Now().UTC()
	round := GatewayPoolUsageRound{ID: "r", Model: gatewayPoolUsageSharedModel,
		Tickets: map[string]gatewayPoolUsageTicket{"one": {}}}
	event := gatewayPoolActiveUseEvent{roundID: "r", ticket: "one", start: start}
	round.syncActiveUsage([]gatewayPoolActiveUseEvent{event}, "before", start.Add(10*time.Second))
	require.True(t, round.ActiveUsage.Open)
	view := round.fullUsageView("after", start.Add(time.Hour))
	require.EqualValues(t, 10000, view.FullDurationMS)
	require.True(t, view.DurationIncomplete)
	require.True(t, round.ActiveUsage.Open, "read-only projection must not modify persisted state")
}

func TestGatewayPoolActiveUsageClipsInvalidationAndCycleBoundary(t *testing.T) {
	start := time.Now().UTC()
	round := GatewayPoolUsageRound{ID: "r", Model: gatewayPoolUsageSharedModel,
		EndedAt: start.Add(15 * time.Second),
		Tickets: map[string]gatewayPoolUsageTicket{"one": {}}}
	event := gatewayPoolActiveUseEvent{roundID: "r", ticket: "one", start: start,
		end: start.Add(time.Minute), expires: start.Add(20 * time.Second), invalidatedAt: start.Add(10 * time.Second)}
	round.syncActiveUsage([]gatewayPoolActiveUseEvent{event}, "p", start.Add(time.Hour))
	require.EqualValues(t, 10000, round.ActiveUsage.duration(time.Time{}))
}

func TestGatewayPoolActiveUsageFailedStopWriteCannotAccrueIdle(t *testing.T) {
	svc, repo, _, account, request, _ := ticketWaitFixture(t)
	gwpoolEchoSeedVerified(t, svc, account)
	svc.httpUpstream = &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: http.StatusOK}}}
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	repo.fail = true
	require.NoError(t, response.Body.Close())
	tracker := svc.codexCookies.activeUsageTracker(gwpoolTestIdentity)
	events := tracker.snapshot()
	require.Len(t, events, 1, "keep failed stop writes until a successful checkpoint")
	require.False(t, events[0].end.IsZero())
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
	now := time.Now().UTC()
	view := state.Rounds[0].fullUsageView(svc.codexCookies.gatewayPoolUsageSession(), now, events...)
	later := state.Rounds[0].fullUsageView(svc.codexCookies.gatewayPoolUsageSession(), now.Add(time.Hour), events...)
	require.Equal(t, view.FullDurationMS, later.FullDurationMS)
	require.Empty(t, later.FullActiveUntil)
	repo.fail = false
	flushGatewayPoolActiveUsage(svc, context.Background(), account, gwpoolTestIdentity)
	require.Empty(t, tracker.snapshot())
}

func TestGatewayPoolActiveUsageCancelStopsUnclosedBody(t *testing.T) {
	svc, _, _, account, request, _ := ticketWaitFixture(t)
	gwpoolEchoSeedVerified(t, svc, account)
	svc.httpUpstream = &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: http.StatusOK}}}
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	response, err := svc.doOpenAIUpstream(request.WithContext(ctx), "", account)
	require.NoError(t, err)
	tracker := svc.codexCookies.activeUsageTracker(gwpoolTestIdentity)
	require.Len(t, tracker.snapshot(), 1)
	cancel()
	require.Eventually(t, func() bool {
		for _, event := range tracker.snapshot() {
			if event.end.IsZero() {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond)
	require.NoError(t, response.Body.Close())
}

func TestGatewayPoolActiveUsageConfirmationPausesOnlyItsBusiness(t *testing.T) {
	svc, repo, _, account, request, _ := ticketWaitFixture(t)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	repo.account = *account
	gwpoolEchoSeedVerified(t, svc, account)
	paused := false
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "new"},
		{status: http.StatusOK, minted: "new"},
	}, beforeReply: func(_ *http.Request, call int) error {
		if call == 2 {
			events := svc.codexCookies.activeUsageTracker(gwpoolTestIdentity).snapshot()
			paused = len(events) == 1 && events[0].start.IsZero() && len(events[0].segments) == 1
		}
		return nil
	}}
	svc.httpUpstream = upstream
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	require.True(t, paused, "post-response probes are not business-active time")
	events := svc.codexCookies.activeUsageTracker(gwpoolTestIdentity).snapshot()
	require.Len(t, events, 1)
	require.False(t, events[0].start.IsZero(), "accepted response resumes business body timing")
	require.NoError(t, response.Body.Close())
}

func TestGatewayPoolActiveUsageIntervalLimitNeverBridgesIdle(t *testing.T) {
	start := time.Now().UTC()
	usage := gatewayPoolActiveUsage{}
	for i := 0; i < gatewayPoolActiveUsageIntervalLimit; i++ {
		at := start.Add(time.Duration(i) * 2 * time.Second)
		usage.Intervals = append(usage.Intervals, gatewayPoolUsageInterval{Start: at, End: at.Add(time.Second)})
	}
	at := start.Add(time.Duration(gatewayPoolActiveUsageIntervalLimit) * 2 * time.Second)
	usage.add(gatewayPoolUsageInterval{Start: at, End: at.Add(time.Second)})
	require.Len(t, usage.Intervals, gatewayPoolActiveUsageIntervalLimit)
	require.True(t, usage.Incomplete)
	require.EqualValues(t, gatewayPoolActiveUsageIntervalLimit*1000, usage.duration(time.Time{}))
}

func TestGatewayPoolActiveUsagePendingLimitReportsLowerBound(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	tracker := svc.codexCookies.activeUsageTracker(identity)
	for range gatewayPoolActiveUsagePendingLimit {
		tracker.attempts[&gatewayPoolActiveUseAttempt{}] = struct{}{}
	}
	start := time.Now().UTC()
	ctx := context.WithValue(context.Background(), gatewayPoolUsageActivityKey{}, &gatewayPoolUsageActivity{
		inventory: svc.codexCookies.gatewayPoolInventory(identity),
	})
	attempt, _ := svc.registerGatewayPoolActiveUse(ctx, account, identity, "ticket", start, time.Time{})
	require.Nil(t, attempt)
	require.Len(t, tracker.attempts, gatewayPoolActiveUsagePendingLimit)
	round := GatewayPoolUsageRound{ID: "r", Model: gatewayPoolUsageSharedModel, StartedAt: start}
	round.syncActiveUsage(tracker.snapshot(), "p", start.Add(time.Minute))
	require.NotNil(t, round.ActiveUsage)
	require.True(t, round.ActiveUsage.Incomplete)
	require.Zero(t, round.ActiveUsage.duration(time.Time{}))
}
