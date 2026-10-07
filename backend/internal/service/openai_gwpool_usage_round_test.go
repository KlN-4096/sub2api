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

func TestGatewayPoolUsageCountsModelTicketOnceAndStartsAtActualSend(t *testing.T) {
	state := gatewayPoolUsageLedger{Tag: "ledger"}
	at := time.Now().UTC()
	require.False(t, state.note("luna", "v", time.Time{}, false))
	require.True(t, state.note("luna", "v", at, false))
	require.False(t, state.note("luna", "v", at.Add(time.Second), false))
	require.True(t, state.note("luna", "v", at.Add(time.Second), true))
	require.False(t, state.note("luna", "v", at.Add(2*time.Second), true))
	require.True(t, state.note("astra", "v", at.Add(time.Second), false))
	require.Len(t, state.Rounds, 2)
	require.Equal(t, 1, state.Rounds[0].Attempted)
	require.Equal(t, 1, state.Rounds[0].Full)
	require.Equal(t, at, state.Rounds[0].StartedAt)
	require.Zero(t, state.Rounds[1].Full, "Luna is not Astra")
	require.True(t, state.end(at.Add(time.Minute)))
	require.False(t, state.note("luna", "late-old", at.Add(time.Second), false))
	require.True(t, state.note("luna", "fresh", at.Add(2*time.Minute), false))
	require.Len(t, state.Rounds, 3)
	require.Equal(t, 1, state.Rounds[2].Attempted)
	require.True(t, state.Rounds[2].EndedAt.IsZero())
}

func TestGatewayPoolUsageArchivesWithoutLosingTotals(t *testing.T) {
	state := gatewayPoolUsageLedger{Tag: "ledger"}
	at := time.Now().UTC()
	for i := 0; i < gatewayPoolUsageHistoryLimit+3; i++ {
		start := at.Add(time.Duration(i) * 2 * time.Minute)
		state.note("luna", fmt.Sprint(i), start, true)
		state.end(start.Add(time.Minute))
		state.prune()
	}
	require.Len(t, state.Rounds, gatewayPoolUsageHistoryLimit)
	require.EqualValues(t, 3, state.Archived["luna"].Rounds)
	require.EqualValues(t, 3, state.Archived["luna"].Attempted)
	require.EqualValues(t, 3, state.Archived["luna"].Full)
	require.EqualValues(t, 180000, state.Archived["luna"].DurationMS)
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
	pair.until = time.Now().Add(-time.Second)
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
