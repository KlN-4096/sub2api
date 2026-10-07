package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func contactEvent(at time.Time, version string) gatewayPoolContactEvent {
	return gatewayPoolContactEvent{
		Applied: OpenAIGatewayPoolApplied{AccountID: 1, Gateway: "unified-142", Version: version,
			RoundID: gatewayPoolContactRoundID(gwpoolTestIdentity, version, at)},
		Identity: gwpoolTestIdentity, Model: "gpt-6-astra", Source: "foreground", Outcome: "full",
		FirstSent: at, LastSent: at.Add(time.Second),
		Steps: []gatewayPoolProbeStep{{Shot: "a", Sent: true, Status: 200, GotState: true}, {Shot: "b", Sent: true, Status: 200, EchoAccepted: true}},
	}
}

func TestGatewayPoolContactsDeduplicatePersistAndMeasureActualGap(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	at := time.Now().UTC().Add(-2 * time.Hour)
	first := contactEvent(at, "first")
	svc.noteGatewayPoolContact(context.Background(), account, first)
	// Repeated business requests update actual contact time, not the initial outcome/denominator.
	later := first
	later.Source = "business"
	later.FirstSent = at.Add(time.Minute)
	later.LastSent = later.FirstSent
	svc.noteGatewayPoolContact(context.Background(), account, later)
	current, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, "tracked_first", state.Rounds[0].Report.First)
	require.Equal(t, "foreground", state.Rounds[0].Report.Source)
	require.True(t, state.Seen["unified-142"].FirstAt.Equal(at))
	require.True(t, state.Seen["unified-142"].LastAt.Equal(later.LastSent))
	second := contactEvent(at.Add(90*time.Minute), "second")
	restarted := &OpenAIGatewayService{accountRepo: repo}
	restarted.noteGatewayPoolContact(context.Background(), account, second)
	restarted.finishGatewayPoolContact(context.Background(), account, gwpoolTestIdentity, second.Applied, 80*time.Second)
	current, _ = repo.GetByID(context.Background(), 1)
	state = readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 2)
	require.Equal(t, "repeat", state.Rounds[1].Report.First)
	require.True(t, state.Rounds[1].Report.GapKnown)
	require.EqualValues(t, 89*60, state.Rounds[1].Report.ElapsedSeconds)
	require.EqualValues(t, 80000, state.Rounds[1].Report.FullWindowMS)
	box := readGatewayPoolOutbox(current, time.Now())
	require.Len(t, box.Pending, 2)
	require.True(t, box.Pending[1].Contact.WindowFinal)
	raw, _ := json.Marshal(state)
	require.NotContains(t, string(raw), "offline-cookie")
	require.NotContains(t, string(raw), gwpoolTestIdentity)
}

func TestGatewayPoolContactsUnsentLegacyAndWriteFailureStayUnknown(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	account.Extra[openAIGatewayHistoryExtraKey] = map[string]any{"seen": map[string]any{"unified-142": map[string]any{"at": time.Now().Add(-time.Hour)}}}
	svc, repo := gatewayRuntimeService(account)
	event := contactEvent(time.Now().UTC(), "first")
	unsent := event
	unsent.FirstSent = time.Time{}
	svc.noteGatewayPoolContact(context.Background(), account, unsent)
	current, _ := repo.GetByID(context.Background(), 1)
	require.Nil(t, current.Extra[openAIGatewayPoolContactsExtraKey])
	repo.fail = true
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ = repo.GetByID(context.Background(), 1)
	require.Nil(t, current.Extra[openAIGatewayPoolContactsExtraKey])
	require.Empty(t, readGatewayPoolOutbox(current, time.Now()).Pending)
	repo.fail = false
	event.Outcome = "unknown"
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ = repo.GetByID(context.Background(), 1)
	state := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Equal(t, "unknown", state.Rounds[0].Report.First)
	require.Equal(t, "unknown", state.Rounds[0].Report.Outcome)
	require.False(t, state.Rounds[0].Report.GapKnown)
	event.Outcome = "full"
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ = repo.GetByID(context.Background(), 1)
	state = readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, "unknown", state.Rounds[0].Report.Outcome, "later result must not create a second initial-contact sample")
}

func TestGatewayPoolContactOutboxKeepsFinalUpdateDuringInitialSend(t *testing.T) {
	started, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/contact/report", r.URL.Path)
		if calls.Add(1) == 1 {
			close(started)
			<-proceed
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	event := contactEvent(time.Now().UTC(), "first")
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ := repo.GetByID(context.Background(), 1)
	pending := readGatewayPoolOutbox(current, time.Now()).Pending[0]
	done := make(chan struct{})
	go func() { defer close(done); svc.sendGatewayPoolPending(context.Background(), 1, pending) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("report never sent")
	}
	svc.finishGatewayPoolContact(context.Background(), account, gwpoolTestIdentity, event.Applied, 80*time.Second)
	close(proceed)
	<-done
	current, _ = repo.GetByID(context.Background(), 1)
	box := readGatewayPoolOutbox(current, time.Now())
	require.Len(t, box.Pending, 1, "initial acknowledgement must not delete a newer window payload")
	require.True(t, box.Pending[0].Contact.WindowFinal)
	svc.flushGatewayPoolReports(context.Background())
	current, _ = repo.GetByID(context.Background(), 1)
	require.Empty(t, readGatewayPoolOutbox(current, time.Now()).Pending)
	require.EqualValues(t, 2, calls.Load())
}

func TestGatewayPoolContactsMergedProbeCanFinishThroughEitherRound(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	first := contactEvent(time.Now().UTC().Add(-time.Minute), "same-ticket")
	svc.noteGatewayPoolContact(context.Background(), account, first)
	concurrent := contactEvent(first.FirstSent.Add(time.Second), "same-ticket")
	svc.noteGatewayPoolContact(context.Background(), account, concurrent)
	svc.finishGatewayPoolContact(context.Background(), account, gwpoolTestIdentity, concurrent.Applied, 50*time.Second)
	current, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.True(t, state.Rounds[0].Report.WindowFinal, "a coalesced probe must retain its round association")
	require.EqualValues(t, 50000, state.Rounds[0].Report.FullWindowMS)
	require.Len(t, readGatewayPoolOutbox(current, time.Now()).Pending, 1)
}

func TestGatewayPoolContactsLateOlderRoundIsNotTrackedFirst(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	recent := contactEvent(time.Now().UTC().Add(-time.Minute), "recent")
	svc.noteGatewayPoolContact(context.Background(), account, recent)
	older := contactEvent(recent.FirstSent.Add(-time.Hour), "older")
	svc.noteGatewayPoolContact(context.Background(), account, older)
	current, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 2)
	require.Equal(t, "unknown", state.Rounds[0].Report.First)
	require.False(t, state.Rounds[0].Report.GapKnown)
	require.True(t, state.Seen["unified-142"].LastAt.Equal(recent.LastSent))
}

func TestGatewayPoolContactsLatestProbeContextIsSeparateFromInitialAndBusiness(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	first := contactEvent(time.Now().UTC().Add(-time.Hour), "ticket")
	svc.noteGatewayPoolContact(context.Background(), account, first)
	probe := first
	probe.Model, probe.Source, probe.Outcome = "luna", "background", "refreshed"
	probe.FirstSent, probe.LastSent = first.FirstSent.Add(time.Minute), first.LastSent.Add(time.Minute)
	svc.noteGatewayPoolContact(context.Background(), account, probe)
	business := first
	business.Source, business.Steps = "business", nil
	business.FirstSent, business.LastSent = first.FirstSent.Add(2*time.Minute), first.LastSent.Add(2*time.Minute)
	svc.noteGatewayPoolContact(context.Background(), account, business)
	svc.noteGatewayPoolContact(context.Background(), account, first) // out-of-order older completion
	current, _ := repo.GetByID(context.Background(), 1)
	state := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	round := state.Rounds[0]
	require.Equal(t, "gpt-6-astra", round.Report.Model)
	require.Equal(t, "foreground", round.Report.Source)
	require.Equal(t, "full", round.Report.Outcome)
	require.Equal(t, "luna", round.LastProbeModel)
	require.Equal(t, "background", round.LastProbeSource)
	require.Equal(t, "refreshed", round.LastProbeOutcome)
	require.True(t, round.LastProbeAt.Equal(probe.LastSent))
	require.True(t, round.LastAt.Equal(business.LastSent))
}
