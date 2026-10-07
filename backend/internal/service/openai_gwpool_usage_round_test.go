package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolUsageCountsTicketOnceAcrossModels(t *testing.T) {
	state := gatewayPoolUsageLedger{Tag: "ledger"}
	at := time.Now().UTC()
	require.False(t, state.note("luna", "v", time.Time{}, false))
	require.True(t, state.note("luna", "v", at, false))
	require.False(t, state.note("luna", "v", at.Add(time.Second), false))
	require.True(t, state.note("luna", "v", at.Add(time.Second), true))
	require.False(t, state.note("luna", "v", at.Add(2*time.Second), true))
	require.False(t, state.note("astra", "v", at.Add(time.Second), false))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, 1, state.Rounds[0].Attempted)
	require.Equal(t, 1, state.Rounds[0].Full)
	require.Equal(t, at, state.Rounds[0].StartedAt)
	require.True(t, state.end(at.Add(time.Minute)))
	require.False(t, state.note("luna", "late-old", at.Add(time.Second), false))
	require.True(t, state.note("luna", "fresh", at.Add(2*time.Minute), false))
	require.Len(t, state.Rounds, 2)
	require.Equal(t, 1, state.Rounds[1].Attempted)
	require.True(t, state.Rounds[1].EndedAt.IsZero())
}

func TestGatewayPoolUsageClosedCycleFreezesAndSameTicketCanStartNewCycle(t *testing.T) {
	state := gatewayPoolUsageLedger{Tag: "ledger"}
	at := time.Now().UTC()
	state.note("luna", "v", at, true)
	state.startFullUse("v", at, time.Time{}, "session")
	state.end(at.Add(time.Minute))
	live := map[string]openAIGatewayPoolPair{"v": {version: "v"}}
	view := state.Rounds[0].fullUsageView(live, "session", at.Add(time.Hour))
	require.EqualValues(t, 60000, view.FullDurationMS)
	require.Empty(t, view.FullActiveUntil)
	require.True(t, state.note("luna", "v", at.Add(2*time.Minute), true))
	require.True(t, state.startFullUse("v", at.Add(2*time.Minute), time.Time{}, "session"))
	require.Len(t, state.Rounds, 2)
	require.EqualValues(t, 60000, state.Rounds[0].fullUseDuration(at.Add(time.Hour)))
	require.EqualValues(t, 1000, state.Rounds[1].fullUseDuration(at.Add(2*time.Minute+time.Second)))
}

func TestGatewayPoolUsageArchivesWithoutLosingTotals(t *testing.T) {
	state := gatewayPoolUsageLedger{Tag: "ledger"}
	at := time.Now().UTC()
	for i := 0; i < gatewayPoolUsageHistoryLimit+3; i++ {
		start := at.Add(time.Duration(i) * 2 * time.Minute)
		state.note("luna", fmt.Sprint(i), start, true)
		state.startFullUse(fmt.Sprint(i), start, time.Time{}, "offline")
		state.endFullUse(fmt.Sprint(i), start.Add(time.Minute))
		state.end(start.Add(time.Minute))
		state.prune()
	}
	require.Len(t, state.Rounds, gatewayPoolUsageHistoryLimit)
	require.EqualValues(t, 3, state.Archived[gatewayPoolUsageSharedModel].Rounds)
	require.EqualValues(t, 3, state.Archived[gatewayPoolUsageSharedModel].Attempted)
	require.EqualValues(t, 3, state.Archived[gatewayPoolUsageSharedModel].Full)
	require.EqualValues(t, 180000, state.Archived[gatewayPoolUsageSharedModel].DurationMS)
}

func TestGatewayPoolUsageConcurrentDedupPersistsAcrossRestart(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	applied := OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}
	identity := openAIGatewayPoolAccountKey(account)
	at := time.Now().UTC()
	var done sync.WaitGroup
	for i := 0; i < 24; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			svc.noteGatewayPoolUsage(context.Background(), account, identity, "gpt-6-luna", applied, at, true)
		}()
	}
	done.Wait()
	fresh, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, 1, state.Rounds[0].Attempted)
	require.Equal(t, 1, state.Rounds[0].Full)
	restarted := &OpenAIGatewayService{accountRepo: repo}
	restarted.noteGatewayPoolUsage(context.Background(), account, identity, "gpt-6-luna", applied, at.Add(time.Second), true)
	fresh, _ = repo.GetByID(context.Background(), 1)
	state = readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, 1, state.Rounds[0].Attempted)
}

func TestGatewayPoolUsageWriteFailureRemainsRetryable(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	applied := OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}
	repo.fail = true
	svc.noteGatewayPoolUsage(context.Background(), account, identity, "gpt-6-luna", applied, time.Now(), true)
	repo.fail = false
	svc.noteGatewayPoolUsage(context.Background(), account, identity, "gpt-6-luna", applied, time.Now(), true)
	fresh, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Len(t, state.Rounds, 1)
}

type gatewayPoolUsageHeldUpstream struct {
	gwpoolErrorUpstream
	written chan struct{}
	release chan struct{}
}

func (u *gatewayPoolUsageHeldUpstream) Do(request *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	httptrace.ContextClientTrace(request.Context()).WroteRequest(httptrace.WroteRequestInfo{})
	close(u.written)
	<-u.release
	return nil, &url.Error{Op: "Post", URL: "https://example.test", Err: errors.New("lost response")}
}

func TestGatewayPoolUsagePersistsWhileFirstResponseStillPending(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	upstream := &gatewayPoolUsageHeldUpstream{written: make(chan struct{}), release: make(chan struct{})}
	svc.httpUpstream = upstream
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	applied := OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}
	identity := openAIGatewayPoolAccountKey(account)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = svc.gatewayPoolObservedRoundTrip(request, "", account, true, func(at time.Time) {
			svc.noteGatewayPoolUsage(request.Context(), account, identity, "gpt-6-luna", applied, at, false)
		})
	}()
	<-upstream.written
	fresh, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	close(upstream.release)
	<-done
	require.Len(t, state.Rounds, 1, "attempt is durable before response completion")
	require.Equal(t, 1, state.Rounds[0].Attempted)
	require.Zero(t, state.Rounds[0].Full)
}

func (r *gatewayRuntimeRepo) GetByIDs(ctx context.Context, ids []int64) ([]*Account, error) {
	var rows []*Account
	for _, id := range ids {
		if id == r.account.ID {
			row, err := r.GetByID(ctx, id)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func TestGatewayPoolRuntimeViewOnlyShowsLiveExactModelProofWithoutPoolIO(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	account := fake.account(1)
	svc, _ := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	pair := openAIGatewayPoolPair{cookie: "secret-cookie", gateway: "g", version: "v", until: time.Now().Add(time.Minute)}
	svc.codexCookies.poolPairs.Store(identity, pair)
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "gpt-6-luna")
	svc.noteGatewayPoolUsage(context.Background(), account, identity, "gpt-6-luna",
		OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}, time.Now(), true)
	snapshot, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, "idle", snapshot[1].Phase)
	require.Equal(t, []string{"gpt-6-luna"}, snapshot[1].Runtime.Tickets[0].VerifiedModels)
	raw, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "secret-cookie")
	require.Nil(t, snapshot[1].Runtime.Rounds[0].Tickets)
	pair.routeExpiresAt = time.Now().Add(-time.Second)
	svc.codexCookies.poolPairs.Store(identity, pair)
	snapshot, err = svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Empty(t, snapshot[1].Runtime.Tickets)
	require.Zero(t, fake.hits.Load())
	require.Zero(t, fake.listHits.Load())
}

func TestGatewayPoolUsageEndsOnlyOnStableFreshZeroInventory(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	account := fake.account(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	svc.noteGatewayPoolUsage(context.Background(), account, identity, "gpt-6-luna",
		OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}, time.Now().Add(-time.Minute), false)
	ended := func() bool {
		fresh, _ := repo.GetByID(context.Background(), 1)
		state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
		return !state.Rounds[0].EndedAt.IsZero()
	}
	finish := svc.codexCookies.gatewayPoolInventoryOperation(identity)
	svc.finishGatewayPoolUsageIfExhausted(context.Background(), account)
	require.False(t, ended())
	require.Zero(t, fake.listHits.Load())
	finish()
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v", until: time.Now().Add(time.Minute)})
	svc.finishGatewayPoolUsageIfExhausted(context.Background(), account)
	require.False(t, ended())
	svc.codexCookies.poolPairs.Delete(identity)
	fake.listStatus = 503
	svc.finishGatewayPoolUsageIfExhausted(context.Background(), account)
	require.False(t, ended(), "failed listing is not zero supply")
	fake.listStatus = 0
	fake.onList = func() { done := svc.codexCookies.gatewayPoolInventoryOperation(identity); done() }
	svc.finishGatewayPoolUsageIfExhausted(context.Background(), account)
	require.False(t, ended(), "changed generation invalidates the zero")
	fake.onList = nil
	svc.finishGatewayPoolUsageIfExhausted(context.Background(), account)
	require.True(t, ended())
}

func TestGatewayPoolUsageUnreservedEarlyOpportunityDoesNotHoldCycleOpen(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-80", PairReady: true, UsedByYou: true}}
	account := fake.account(1)
	account.Status, account.Schedulable = StatusActive, true
	account.Extra[openAIGatewayPoolEarlyEnabledKey] = true
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	svc.codexCookies.gatewayPoolMarkUsed(identity, "unified-80")
	require.True(t, svc.codexCookies.gatewayPoolEarlyDue(context.Background(), account, identity))
	svc.noteGatewayPoolUsage(context.Background(), account, identity, "luna",
		OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "old", Version: "old"}, time.Now().Add(-time.Minute), false)
	svc.finishGatewayPoolUsageIfExhausted(context.Background(), account)
	fresh, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.False(t, state.Rounds[0].EndedAt.IsZero())
	require.True(t, readGatewayPoolEarlyState(fresh, identity).IsZero(), "settlement never spends an early probe budget")
	require.Zero(t, fake.hits.Load())
}

func TestGatewayPoolUsageLockWaitIsWithinContextBudget(t *testing.T) {
	var lock sync.Mutex
	lock.Lock()
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	require.False(t, gatewayPoolLockWithin(ctx, &lock))
	require.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestGatewayPoolUsageAbandonedFetchSettlesAfterStrictZero(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	fake.refuseStatus, fake.refuseCode = http.StatusConflict, "all_cooling"
	account := fake.account(1)
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.poolUsageFinished = svc.finishGatewayPoolUsageIfExhausted
	identity := openAIGatewayPoolAccountKey(account)
	svc.noteGatewayPoolUsage(context.Background(), account, identity, gatewayPoolProbeModelLuna,
		OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "old", Version: "old"}, time.Now().Add(-time.Minute), false)
	started, release := make(chan struct{}), make(chan struct{})
	var firstList, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	fake.onList = func() { firstList.Do(func() { close(started); <-release }) }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := svc.codexCookies.gatewayPoolPair(ctx, account, identity)
		done <- err
	}()
	<-started
	require.ErrorIs(t, <-done, context.DeadlineExceeded)
	_, pending := svc.codexCookies.gatewayPoolInventorySnapshot(identity, account)
	require.True(t, pending)
	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		fresh, _ := repo.GetByID(context.Background(), 1)
		state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
		return len(state.Rounds) == 1 && !state.Rounds[0].EndedAt.IsZero()
	}, time.Second, time.Millisecond, "the abandoned fetch's final zero must close the round without another business request")
}
