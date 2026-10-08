package service

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolUsageIdleCutoffExcludesWaitingAndFreezesHistory(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour)
	state := gatewayPoolUsageLedger{Tag: "offline", LastRequestCompletedAt: at.Add(5 * time.Minute)}
	state.note("luna", "one", at, true)
	state.note("luna", "two", at.Add(3*time.Minute), true)
	state.Rounds[0].syncActiveUsage([]gatewayPoolActiveUseEvent{
		{roundID: state.Rounds[0].ID, ticket: "one", start: at, end: at.Add(time.Minute)},
		{roundID: state.Rounds[0].ID, ticket: "two", start: at.Add(4 * time.Minute), end: at.Add(5 * time.Minute)},
	}, "process", at.Add(5*time.Minute))
	require.True(t, state.idleCutoff(at.Add(35*time.Minute)).IsZero(), "exactly 30 minutes is not over 30")
	cutoff := state.idleCutoff(at.Add(36 * time.Minute))
	require.Equal(t, state.LastRequestCompletedAt, cutoff)
	state.end(cutoff, "idle_timeout")
	require.EqualValues(t, 120000, state.Rounds[0].ActiveUsage.duration(time.Now()), "one minute per ticket, no acquisition gap or 30min idle tail")
}

func TestGatewayPoolUsageBodyCompletesExactlyOnceAtEOFOrClose(t *testing.T) {
	var count atomic.Int32
	body := &gatewayPoolUsageBody{ReadCloser: io.NopCloser(strings.NewReader("stream")), finish: func() { count.Add(1) }}
	require.Zero(t, count.Load(), "headers alone are not completion")
	_, err := io.ReadAll(body)
	require.NoError(t, err)
	require.EqualValues(t, 1, count.Load())
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = body.Close() }()
	}
	wg.Wait()
	require.EqualValues(t, 1, count.Load())
}

func TestGatewayPoolUsageInFlightAndSharedWorkPreventIdleEnd(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	applied := OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v"})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "luna")
	ctx, finish := svc.beginGatewayPoolUsageRequest(context.Background(), account)
	svc.noteGatewayPoolFullUse(ctx, account, identity, applied, time.Now().UTC())
	fresh, _ := repo.GetByID(ctx, 1)
	svc.maintainGatewayPoolUsage(ctx, fresh, time.Now().Add(time.Hour))
	fresh, _ = repo.GetByID(ctx, 1)
	require.True(t, readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity)).Rounds[0].EndedAt.IsZero())
	finish()
	fresh, _ = repo.GetByID(ctx, 1)
	before := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.False(t, before.LastRequestCompletedAt.IsZero())
	finishShared := svc.codexCookies.gatewayPoolInventoryOperation(identity)
	svc.maintainGatewayPoolUsage(ctx, fresh, time.Now().Add(time.Hour))
	fresh, _ = repo.GetByID(ctx, 1)
	require.True(t, readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity)).Rounds[0].EndedAt.IsZero())
	finishShared()
	svc.maintainGatewayPoolUsage(ctx, fresh, time.Now().Add(time.Hour))
	fresh, _ = repo.GetByID(ctx, 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Equal(t, before.LastRequestCompletedAt, state.Rounds[0].EndedAt)
	require.Equal(t, "idle_timeout", state.Rounds[0].EndReason)
	// One old request cannot reopen a later cycle after an asynchronous result.
	svc.noteGatewayPoolFullUse(ctx, account, identity, applied, time.Now().Add(time.Minute))
	fresh, _ = repo.GetByID(ctx, 1)
	require.Len(t, readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity)).Rounds, 1)
	newCtx, newFinish := svc.beginGatewayPoolUsageRequest(context.Background(), account)
	nextAt := before.LastRequestCompletedAt.Add(time.Second)
	svc.noteGatewayPoolFullUse(newCtx, account, identity, applied, nextAt)
	newFinish()
	fresh, _ = repo.GetByID(ctx, 1)
	require.Len(t, readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity)).Rounds, 2)
}

func TestGatewayPoolUsageTemporaryBlockSurvivesClearAndEndsActiveCycle(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	applied := OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v"})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "luna")
	ctx, finish := svc.beginGatewayPoolUsageRequest(context.Background(), account)
	svc.noteGatewayPoolFullUse(ctx, account, identity, applied, time.Now().UTC())
	blocked := time.Now().UTC()
	require.NoError(t, repo.UpdateExtra(ctx, 1, map[string]any{GatewayPoolUsageBlockedAtKey: blocked.Format(time.RFC3339Nano)}))
	// The short temporary block has already been cleared; only the event remains.
	fresh, _ := repo.GetByID(ctx, 1)
	require.Nil(t, fresh.TempUnschedulableUntil)
	svc.maintainGatewayPoolUsage(ctx, fresh, time.Now())
	fresh, _ = repo.GetByID(ctx, 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Equal(t, blocked, state.Rounds[0].EndedAt)
	require.Equal(t, "temporarily_unschedulable", state.Rounds[0].EndReason)
	svc.noteGatewayPoolFullUse(ctx, account, identity, applied, blocked.Add(time.Second))
	finish()
	fresh, _ = repo.GetByID(ctx, 1)
	require.Len(t, readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity)).Rounds, 1)
}

func TestGatewayPoolUsageBlockAfterRestartDoesNotInventTail(t *testing.T) {
	account := gwpoolTestAccount(1)
	identity := openAIGatewayPoolAccountKey(account)
	start := time.Now().UTC().Add(-time.Hour)
	state := gatewayPoolUsageLedger{Tag: gatewayPoolLedgerTag(identity)}
	state.note("luna", "ticket", start, true)
	state.Rounds[0].syncActiveUsage([]gatewayPoolActiveUseEvent{{
		roundID: state.Rounds[0].ID, ticket: "ticket", start: start,
	}}, "old-process", start.Add(time.Minute))
	account.Extra[gatewayPoolUsageExtraKey] = state
	account.Extra[GatewayPoolUsageBlockedAtKey] = start.Add(20 * time.Minute).Format(time.RFC3339Nano)
	svc, repo := gatewayRuntimeService(account)
	svc.maintainGatewayPoolUsage(context.Background(), account, time.Now())
	fresh, _ := repo.GetByID(context.Background(), 1)
	state = readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.EqualValues(t, 60000, state.Rounds[0].ActiveUsage.duration(time.Now()))
	require.True(t, state.Rounds[0].ActiveUsage.Incomplete)
}

func TestGatewayPoolUsageBlockBeforeFirstAttemptStopsOldRequest(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	start := time.Now().UTC().Add(-time.Minute)
	ctx := context.WithValue(context.Background(), gatewayPoolUsageRequestKey{}, start)
	blocked := start.Add(time.Second)
	require.NoError(t, repo.UpdateExtra(ctx, 1, map[string]any{GatewayPoolUsageBlockedAtKey: blocked.Format(time.RFC3339Nano)}))
	svc.noteGatewayPoolUsage(ctx, account, identity, "luna",
		OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}, blocked.Add(time.Second), false)
	fresh, _ := repo.GetByID(ctx, 1)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity))
	require.Empty(t, state.Rounds)
	require.Equal(t, blocked, state.ClosedBefore[gatewayPoolUsageSharedModel])
}

type gatewayPoolUsageBlockOnReadRepo struct {
	*gatewayRuntimeRepo
	blocked time.Time
	once    sync.Once
}

func (r *gatewayPoolUsageBlockOnReadRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	r.once.Do(func() {
		_ = r.UpdateExtra(ctx, id, map[string]any{GatewayPoolUsageBlockedAtKey: r.blocked.Format(time.RFC3339Nano)})
	})
	return r.gatewayRuntimeRepo.GetByID(ctx, id)
}

func TestGatewayPoolUsageBlockDuringBeginDoesNotRewriteOldRequest(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	blocked := time.Now().UTC().Add(time.Second)
	svc.accountRepo = &gatewayPoolUsageBlockOnReadRepo{gatewayRuntimeRepo: repo, blocked: blocked}
	ctx, finish := svc.beginGatewayPoolUsageRequest(context.Background(), account)
	require.NotNil(t, finish)
	started, _ := ctx.Value(gatewayPoolUsageRequestKey{}).(time.Time)
	require.True(t, started.Before(blocked), "fresh block arrived after the request actually started")
	svc.noteGatewayPoolUsage(ctx, account, identity, "luna",
		OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "g", Version: "v"}, blocked.Add(time.Second), false)
	// Avoid any pool listing in this strictly local test.
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{cookie: "offline", gateway: "g", version: "v"})
	finish()
	fresh, _ := repo.GetByID(ctx, 1)
	require.Empty(t, readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(identity)).Rounds)
}
