package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func gatewayPoolProgressContext(identity string, start time.Time) context.Context {
	ctx := context.WithValue(context.Background(), gatewayPoolUsageRequestKey{}, start)
	return context.WithValue(ctx, gatewayPoolUsageIdentityKey{}, gatewayPoolLedgerTag(identity))
}

func TestGatewayPoolProgressSequencePersistsAcrossConcurrentRunsAndRestart(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	const workers = 12
	runs := make(chan *gatewayPoolProgressRun, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			runs <- svc.startGatewayPoolProgress(context.Background(), account, identity)
		}()
	}
	group.Wait()
	close(runs)
	sequences := map[uint64]bool{}
	for run := range runs {
		require.False(t, sequences[run.progress.Sequence])
		require.NotZero(t, run.progress.Sequence)
		sequences[run.progress.Sequence] = true
		svc.codexCookies.poolProgress.update(run, "ready", 1, "", false, true)
		svc.codexCookies.poolProgress.resume(run)
		require.True(t, sequences[run.progress.Sequence], "re-entry keeps the same display number")
	}
	for sequence := uint64(1); sequence <= workers; sequence++ {
		require.True(t, sequences[sequence])
	}
	restarted := &OpenAIGatewayService{accountRepo: repo}
	next := restarted.startGatewayPoolProgress(context.Background(), account, identity)
	require.EqualValues(t, workers+1, next.progress.Sequence)
	end := time.Now().UTC()
	restarted.changeGatewayPoolUsage(context.Background(), account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.end(end, "observed_exhausted")
	})
	nextCycle := restarted.startGatewayPoolProgress(gatewayPoolProgressContext(identity, end.Add(time.Millisecond)), account, identity)
	require.EqualValues(t, 1, nextCycle.progress.Sequence)
	require.NotEqual(t, next.progress.RunID, nextCycle.progress.RunID)
}

func TestGatewayPoolProgressSequencePublishesOnlyAfterDurableWrite(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	repo.fail = true
	first := svc.startGatewayPoolProgress(context.Background(), account, identity)
	require.Zero(t, first.progress.Sequence, "failure must not publish a number that can be reused")
	repo.fail = false
	second := svc.startGatewayPoolProgress(context.Background(), account, identity)
	require.EqualValues(t, 1, second.progress.Sequence)
}

func TestGatewayPoolProgressFiltersOldRequestsBeforeChoosingOrPruning(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	now := time.Now().UTC()
	oldContext := gatewayPoolProgressContext(identity, now.Add(-2*time.Minute))
	first := svc.startGatewayPoolProgress(oldContext, account, identity)
	svc.changeGatewayPoolUsage(context.Background(), account, identity, func(state *gatewayPoolUsageLedger) bool {
		return state.end(now.Add(-time.Minute), "temporarily_unschedulable")
	})
	current := svc.startGatewayPoolProgress(gatewayPoolProgressContext(identity, now), account, identity)
	svc.codexCookies.poolProgress.update(current, "ready", 2, "", false, true)
	svc.codexCookies.poolProgress.update(first, "exhausted", 9, "", true, true)
	// The old request reaches its first warm run only after the new cycle ends
	// verification. Starting and completing it cannot erase the newer result.
	late := svc.startGatewayPoolProgress(oldContext, account, identity)
	require.Zero(t, late.progress.Sequence)
	svc.codexCookies.poolProgress.update(late, "unknown", 8, "", true, true)
	view, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, current.progress.RunID, view[1].RunID)
	require.EqualValues(t, 1, view[1].Sequence)
	require.Equal(t, 2, view[1].Attempt)
	require.Zero(t, view[1].Rejected)
}

func TestGatewayPoolProgressPureFetchIsVisibleAndIdleCycleIsDurable(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	run := svc.startGatewayPoolProgress(context.Background(), account, identity)
	view, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, "fetching", view[1].Phase)
	require.EqualValues(t, 1, view[1].Sequence)
	require.Empty(t, view[1].Runtime.Rounds, "a first fetch need not have a usage round yet")
	svc.codexCookies.poolProgress.update(run, "unknown", 0, "", false, true)
	fresh, err := repo.GetByID(context.Background(), 1)
	require.NoError(t, err)
	svc.maintainGatewayPoolUsage(context.Background(), fresh, time.Now().Add(time.Hour))
	fresh, err = repo.GetByID(context.Background(), 1)
	require.NoError(t, err)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Zero(t, state.VerificationSequence)
	require.False(t, state.ClosedBefore[gatewayPoolUsageSharedModel].IsZero())
	view, err = svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, "idle", view[1].Phase)
}

func TestGatewayPoolProgressOriginalIdentityCannotJoinNewCycle(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	ctx := gatewayPoolProgressContext("old-credential-domain", time.Now().UTC())
	run := svc.startGatewayPoolProgress(ctx, account, identity)
	require.Zero(t, run.progress.Sequence)
	view, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, "idle", view[1].Phase)
}
