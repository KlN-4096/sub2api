package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolPersistedDigestsKeepTheirBytes(t *testing.T) {
	// Durable ledger tags, ticket keys and round IDs; literals are sha256sum output.
	require.Equal(t, "997721ebc06a9278c1b1fa3c68fb1772d21e6a50674aa03eb805c644e6218a14", gatewayPoolLedgerTag("member"))
	require.Equal(t, "f9680054dcbb66bfa971d0aec13fbafea3d9cce979c361182896c0e7b3f3dd44", gatewayPoolUsageTicketKey("g", "v"))
	require.Equal(t, "9ec27bc78e2a82c135a9c88c9189e2099e40e323f5bb19a9e6e2e84d861d7ca1",
		gatewayPoolContactRoundID("member", "v", time.Unix(0, 0)))
}

func TestGatewayPoolBusinessContactSkipsOrdinaryResponseBody(t *testing.T) {
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	request := httptest.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	bodies := 0
	request.GetBody = func() (io.ReadCloser, error) {
		bodies++
		return io.NopCloser(strings.NewReader(`{"model":"gpt-5.4"}`)), nil
	}
	response := &http.Response{Header: http.Header{"Set-Cookie": {"__oailb=offline"}}}
	(&OpenAIGatewayService{}).noteGatewayPoolBusinessContact(request, account, "", OpenAIGatewayPoolApplied{}, time.Now(), response)
	require.Zero(t, bodies, "a response without this account's pool route has nothing to record")
}

type gatewayPoolContactPeersRepo struct {
	*gatewayRuntimeRepo
	rows []Account
}

func (r *gatewayPoolContactPeersRepo) FindByExtraField(ctx context.Context, key string, value any) ([]Account, error) {
	self, err := r.GetByID(ctx, r.account.ID)
	if err != nil {
		return nil, err
	}
	var out []Account
	for _, row := range append([]Account{*self}, r.rows...) {
		if row.Extra[key] == value {
			out = append(out, gatewayPoolWithoutLedgers(row))
		}
	}
	return out, nil
}

func (r *gatewayPoolContactPeersRepo) LoadGatewayPoolLedgers(_ context.Context, ids []int64, keys []string) (map[int64]map[string]json.RawMessage, error) {
	self, err := r.stored(r.account.ID)
	if err != nil {
		return nil, err
	}
	return fakeGatewayPoolLedgers(append([]Account{*self}, r.rows...), ids, keys)
}

func (r *gatewayPoolContactPeersRepo) LoadGatewayPoolLedgerWithout(_ context.Context, id int64, kind string, omit ...string) (json.RawMessage, error) {
	self, err := r.stored(r.account.ID)
	if err != nil {
		return nil, err
	}
	return fakeGatewayPoolLedgerWithout(append([]Account{*self}, r.rows...), id, kind, omit)
}

// Mirrors the repository's contacts projection: same ledger, minus exclude,
// contacts only.
type gatewayPoolContactNarrowRepo struct {
	*gatewayPoolContactPeersRepo
	calls []string
}

func (r *gatewayPoolContactNarrowRepo) FindGatewayPoolStatePeers(_ context.Context, tag, kind string, exclude int64) ([]Account, error) {
	r.calls = append(r.calls, kind+"/"+strconv.FormatInt(exclude, 10))
	self, err := r.stored(r.account.ID)
	if err != nil {
		return nil, err
	}
	var out []Account
	for _, row := range append([]Account{*self}, r.rows...) {
		if row.ID != exclude && row.Extra[openAIGatewayLedgerTagExtraKey] == tag {
			out = append(out, Account{ID: row.ID, Extra: map[string]any{
				openAIGatewayPoolContactsExtraKey: row.Extra[openAIGatewayPoolContactsExtraKey]}})
		}
	}
	return out, nil
}

func (r *gatewayPoolContactNarrowRepo) FindByExtraField(context.Context, string, any) ([]Account, error) {
	return nil, errors.New("contacts peers must use the narrow projection")
}

func TestGatewayPoolContactsWriteMergesSameLedgerPeers(t *testing.T) {
	account := gwpoolTestAccount(1)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	peer := func(id int64, ledger, gateway string) Account {
		return Account{ID: id, Extra: map[string]any{openAIGatewayLedgerTagExtraKey: ledger,
			openAIGatewayPoolContactsExtraKey: gatewayPoolContacts{LedgerTag: ledger,
				Seen: map[string]gatewayPoolContactSeen{gateway: {FirstAt: at, LastAt: at}}}}}
	}
	for _, narrow := range []bool{false, true} {
		peers := &gatewayPoolContactPeersRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account},
			rows: []Account{peer(2, tag, "unified-clone"), peer(3, "another-ledger", "unified-foreign")}}
		var repo AccountRepository = peers
		narrowRepo := &gatewayPoolContactNarrowRepo{gatewayPoolContactPeersRepo: peers}
		if narrow {
			repo = narrowRepo
		}
		svc := &OpenAIGatewayService{accountRepo: repo}
		svc.codexCookies.accountByID = svc.gatewayPoolLedgerRow
		for _, gateway := range []string{"unified-own", "unified-own-later"} {
			svc.noteGatewayPoolContact(context.Background(), account, gatewayPoolContactEvent{
				Applied:  OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: gateway, RoundID: gatewayPoolDigest(gateway), Version: "v"},
				Identity: gwpoolTestIdentity, Source: "business", FirstSent: time.Now().UTC(),
			})
		}
		fresh, err := peers.stored(account.ID)
		require.NoError(t, err)
		seen := readGatewayPoolContacts(fresh, tag).Seen
		require.Contains(t, seen, "unified-own")
		require.Contains(t, seen, "unified-own-later", "the second write keeps the row's own first contact")
		require.Contains(t, seen, "unified-clone", "a same-ledger clone's contacts are merged into the write")
		require.NotContains(t, seen, "unified-foreign")
		if narrow {
			require.Equal(t, []string{"contacts/1", "contacts/1"}, narrowRepo.calls, "only peers' contacts, never the writer's own row")
		}
	}
}

func TestGatewayPoolContactsRetainsNewest300RoundsAcrossReload(t *testing.T) {
	now := time.Now().UTC()
	state := gatewayPoolContacts{LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity)}
	for i := 0; i < 305; i++ {
		id := gatewayPoolDigest(strconv.Itoa(i))
		state.Rounds = append(state.Rounds, gatewayPoolContactRound{
			RoundID: id, Report: gwpool.ContactReport{ID: id, At: now.Add(time.Duration(i-305) * time.Minute)},
		})
	}
	pruneGatewayPoolContacts(&state, now)
	require.Len(t, state.Rounds, 300)
	require.Equal(t, gatewayPoolDigest("5"), state.Rounds[0].RoundID)
	require.Equal(t, gatewayPoolDigest("304"), state.Rounds[299].RoundID)
	account := gwpoolTestAccount(1)
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	var durable map[string]any
	require.NoError(t, json.Unmarshal(raw, &durable))
	account.Extra[openAIGatewayPoolContactsExtraKey] = durable
	require.Len(t, readGatewayPoolContacts(account, state.LedgerTag).Rounds, 300)
	pruneGatewayPoolContacts(&state, now.Add(8*24*time.Hour))
	require.Empty(t, state.Rounds, "the seven-day retention is unchanged")
}

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
	current, _ := repo.stored(1)
	state := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, "tracked_first", state.Rounds[0].Report.First)
	require.Equal(t, "foreground", state.Rounds[0].Report.Source)
	require.True(t, state.Seen["unified-142"].FirstAt.Equal(at))
	require.True(t, state.Seen["unified-142"].LastAt.Equal(later.LastSent))
	second := contactEvent(at.Add(90*time.Minute), "second")
	restarted := &OpenAIGatewayService{accountRepo: repo}
	restarted.noteGatewayPoolContact(context.Background(), account, second)
	restarted.finishGatewayPoolContact(context.Background(), account, gwpoolTestIdentity, second.Applied, 80*time.Second, 30*time.Second)
	current, _ = repo.stored(1)
	state = readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 2)
	require.Equal(t, "repeat", state.Rounds[1].Report.First)
	require.True(t, state.Rounds[1].Report.GapKnown)
	require.EqualValues(t, 89*60, state.Rounds[1].Report.ElapsedSeconds)
	require.EqualValues(t, 80000, state.Rounds[1].Report.FullWindowMS)
	require.EqualValues(t, 30000, state.Rounds[1].FullUseMs, "the card shows business use; the pool keeps the window")
	box := readGatewayPoolOutbox(current, time.Now())
	require.Len(t, box.Pending, 2)
	require.True(t, box.Pending[1].Contact.WindowFinal)
	sent, _ := json.Marshal(box)
	require.NotContains(t, string(sent), "full_use_ms", "the use reading never goes to the pool")
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
	current, _ := repo.stored(1)
	require.Nil(t, current.Extra[openAIGatewayPoolContactsExtraKey])
	repo.fail = true
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ = repo.stored(1)
	require.Nil(t, current.Extra[openAIGatewayPoolContactsExtraKey])
	require.Empty(t, readGatewayPoolOutbox(current, time.Now()).Pending)
	repo.fail = false
	event.Outcome = "unknown"
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ = repo.stored(1)
	state := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Equal(t, "unknown", state.Rounds[0].Report.First)
	require.Equal(t, "unknown", state.Rounds[0].Report.Outcome)
	require.False(t, state.Rounds[0].Report.GapKnown)
	event.Outcome = "full"
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ = repo.stored(1)
	state = readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, "unknown", state.Rounds[0].Report.Outcome, "later result must not create a second initial-contact sample")
}

func TestGatewayPoolContactOutboxKeepsFinalUpdateDuringInitialSend(t *testing.T) {
	started, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/contact/reports", r.URL.Path)
		if calls.Add(1) == 1 {
			close(started)
			<-proceed
		}
		ackGatewayPoolBatch[gwpool.ContactReport](t, w, r, nil)
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	event := contactEvent(time.Now().UTC(), "first")
	svc.noteGatewayPoolContact(context.Background(), account, event)
	done := make(chan struct{})
	go func() { defer close(done); svc.flushGatewayPoolReports(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("report never sent")
	}
	svc.finishGatewayPoolContact(context.Background(), account, gwpoolTestIdentity, event.Applied, 80*time.Second, 20*time.Second)
	close(proceed)
	<-done
	current, _ := repo.stored(1)
	box := readGatewayPoolOutbox(current, time.Now())
	require.Len(t, box.Pending, 1, "initial acknowledgement must not delete a newer window payload")
	require.True(t, box.Pending[0].Contact.WindowFinal)
	svc.flushGatewayPoolReports(context.Background())
	current, _ = repo.stored(1)
	require.Empty(t, readGatewayPoolOutbox(current, time.Now()).Pending)
	require.EqualValues(t, 2, calls.Load())
}

// A ticket verified more than 10h ago gives the pool no usable window, but the
// card still gets its use reading; clone copies carry that reading over.
func TestGatewayPoolContactUseReadingSurvivesAnUnusableWindowAndClones(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	event := contactEvent(time.Now().UTC(), "first")
	svc.noteGatewayPoolContact(context.Background(), account, event)
	svc.finishGatewayPoolContact(context.Background(), account, gwpoolTestIdentity, event.Applied, 11*time.Hour, 20*time.Second)
	current, _ := repo.stored(1)
	state := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.EqualValues(t, 20000, state.Rounds[0].FullUseMs)
	require.False(t, state.Rounds[0].Report.WindowFinal, "an over-long window is not a pool sample")
	box := readGatewayPoolOutbox(current, time.Now())
	require.Len(t, box.Pending, 1)
	require.False(t, box.Pending[0].Contact.WindowFinal, "only the initial report is queued")
	require.Zero(t, box.Pending[0].Contact.FullWindowMS)

	clone := gatewayPoolContacts{LedgerTag: state.LedgerTag, Seen: map[string]gatewayPoolContactSeen{},
		Rounds: []gatewayPoolContactRound{state.Rounds[0]}}
	clone.Rounds[0].FullUseMs = 0
	mergeGatewayPoolContacts(&clone, state)
	require.EqualValues(t, 20000, clone.Rounds[0].FullUseMs)
	clone.Rounds[0].FullUseMs = 0
	mergeGatewayPoolContacts(&state, clone)
	require.EqualValues(t, 20000, state.Rounds[0].FullUseMs, "a copy without the reading must not erase it")
}

func TestGatewayPoolContactsMergedProbeCanFinishThroughEitherRound(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	first := contactEvent(time.Now().UTC().Add(-time.Minute), "same-ticket")
	svc.noteGatewayPoolContact(context.Background(), account, first)
	concurrent := contactEvent(first.FirstSent.Add(time.Second), "same-ticket")
	svc.noteGatewayPoolContact(context.Background(), account, concurrent)
	svc.finishGatewayPoolContact(context.Background(), account, gwpoolTestIdentity, concurrent.Applied, 50*time.Second, 10*time.Second)
	current, _ := repo.stored(1)
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
	current, _ := repo.stored(1)
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
	current, _ := repo.stored(1)
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
