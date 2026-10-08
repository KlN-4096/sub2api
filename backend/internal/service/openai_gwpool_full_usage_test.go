package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolFullUsageStopsWhenBusinessBodyFinishes(t *testing.T) {
	svc, _, _, account, request, _ := ticketWaitFixture(t)
	gwpoolEchoSeedVerified(t, svc, account)
	svc.httpUpstream = &gwpoolEchoUpstream{replies: []gwpoolEchoReply{{status: http.StatusOK}}}
	response, err := svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	_, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	view, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID})
	require.NoError(t, err)
	round := view[account.ID].Runtime.Rounds[0]
	require.Empty(t, round.FullActiveUntil, "EOF/Close must stop usage even while the verified ticket remains live")
}

func TestGatewayPoolLegacyHeldUsageIsReadOnlyAndNotDisplayed(t *testing.T) {
	start := time.Now().UTC().Add(-time.Hour)
	ticket := gatewayPoolUsageTicket{At: start, Full: true, UseStartedAt: start,
		UseObservedAt: start.Add(20 * time.Second), UseExpiresAt: start.Add(time.Minute), UseSession: "before"}
	round := GatewayPoolUsageRound{ID: "old", Model: gatewayPoolUsageSharedModel, StartedAt: start,
		FullStartedAt: start, Full: 1, Attempted: 1, Tickets: map[string]gatewayPoolUsageTicket{"one": ticket}}
	for _, session := range []string{"before", "after"} {
		view := round.fullUsageView(session, time.Now())
		require.Zero(t, view.FullDurationMS)
		require.Empty(t, view.FullActiveUntil)
		require.Empty(t, view.FullUsageMode)
		require.Equal(t, ticket, round.Tickets["one"], "read-only projection cannot settle raw legacy timing")
	}
	require.EqualValues(t, 20000, round.legacyFullUseDuration(), "archival keeps observed history, not an unobserved open tail")
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
	require.Equal(t, start, state.Rounds[0].Tickets[gatewayPoolUsageTicketKey("g", "v")].At)
}

func TestGatewayPoolFullUsageStartsOnBusinessWriteBeforeResponse(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	account := fake.account(1)
	account.Credentials["access_token"] = "offline-token"
	repo := &forkPerformanceRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.codexCookies.accountByID = repo.GetByID
	gwpoolEchoSeedVerified(t, svc, account)
	upstream := &gatewayPoolUsageHeldUpstream{written: make(chan struct{}), release: make(chan struct{})}
	svc.httpUpstream = upstream
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), nil)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	request.Header.Set(openAICodexTurnStateHeader, "business-state")
	done := make(chan error, 1)
	go func() {
		response, err := svc.doOpenAIUpstream(request, "", account)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	select {
	case <-upstream.written:
	case err := <-done:
		t.Fatalf("business returned before the expected write: %v", err)
	case <-time.After(time.Second):
		close(upstream.release)
		t.Fatal("business write did not start")
	}
	fresh, readErr := repo.GetByID(context.Background(), 1)
	writesBeforeResponse := repo.writes
	close(upstream.release)
	<-done
	require.NoError(t, readErr)
	state := readGatewayPoolUsage(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.NotNil(t, state.Rounds[0].ActiveUsage)
	require.True(t, state.Rounds[0].ActiveUsage.Open)
	require.Zero(t, state.Rounds[0].FullStartedAt)
	require.Equal(t, 1, state.Rounds[0].Full)
	require.Equal(t, 1, state.Rounds[0].Attempted)
	require.Equal(t, 2, writesBeforeResponse, "one request-begin write plus one combined business-send write")
}
