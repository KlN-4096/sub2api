package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

type gatewayPoolSnapshotCounter struct {
	ConcurrencyCache
	counts map[int64]int
	err    error
}

func (c *gatewayPoolSnapshotCounter) GetAccountConcurrencyBatch(context.Context, []int64) (map[int64]int, error) {
	return c.counts, c.err
}

func TestGatewayPoolRuntimeSnapshotIncludesFreshHistoryAndKnownCapacity(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	account := fake.account(1)
	account.Concurrency = 100
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Current: "new-gateway", UpdatedAt: time.Now(),
		Seen: map[string]openAIGatewaySeen{"new-gateway": {At: time.Now(), Region: "east-asia"}},
	}
	svc, _ := gatewayRuntimeService(account)
	counter := &gatewayPoolSnapshotCounter{counts: map[int64]int{1: 7}}
	svc.concurrencyService = NewConcurrencyService(counter)
	read := func() map[string]any {
		snapshot, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1}, true)
		require.NoError(t, err)
		raw, err := json.Marshal(snapshot[1].Runtime)
		require.NoError(t, err)
		var view map[string]any
		require.NoError(t, json.Unmarshal(raw, &view))
		return view
	}
	view := read()
	require.Equal(t, float64(7), view["current_concurrency"])
	require.Equal(t, float64(100), view["concurrency_limit"])
	history, ok := view["history"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "new-gateway", history["current"])
	counter.err = errors.New("counter unavailable")
	view = read()
	require.Contains(t, view, "current_concurrency")
	require.Nil(t, view["current_concurrency"], "read failure must remain unknown, not look like 0/100")
	counter.err = nil
	counter.counts[1] = 0
	require.Equal(t, float64(0), read()["current_concurrency"], "measured zero remains a real zero")
	require.Zero(t, fake.hits.Load())
	require.Zero(t, fake.listHits.Load(), "the 1s UI snapshot is local-only")
}

func TestGatewayPoolRuntimeSnapshotMergesCloneHistoryWithoutWriting(t *testing.T) {
	account := gwpoolTestAccount(1)
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	now := time.Now().UTC()
	newer := gatewayPoolCooldown{ResetAt: now, Until: now.Add(time.Hour), WindowSeconds: 3600}
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Current: "a", UpdatedAt: now.Add(-time.Minute),
		Seen: map[string]openAIGatewaySeen{"a": {At: now.Add(-time.Minute), Cooldown: &newer}},
	}
	clone := *gwpoolTestAccount(2)
	clone.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Current: "b", UpdatedAt: now,
		Seen: map[string]openAIGatewaySeen{"a": {At: now}, "b": {At: now}},
	}
	svc := &OpenAIGatewayService{}
	history, _ := svc.gatewayPoolDisplaySnapshot(account, gwpoolTestIdentity, []Account{clone}, nil)
	require.Equal(t, "b", history.Current)
	require.Len(t, history.Seen, 2)
	require.Equal(t, newer.ResetAt, history.Seen["a"].Cooldown.ResetAt,
		"a newer contact timestamp may not erase a newer cooldown generation")
	original, _ := readOpenAIGatewayHistory(account)
	require.Equal(t, "a", original.Current)
	require.Len(t, original.Seen, 1)
}

func TestGatewayPoolDisplayCachePreservesRowFreshnessAndIndependentResults(t *testing.T) {
	now := time.Now().UTC()
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	account := gwpoolTestAccount(1)
	older := *gwpoolTestAccount(1) // same ID, a different database observation
	clone := *gwpoolTestAccount(2)
	for _, row := range []*Account{account, &older, &clone} {
		row.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
			LedgerTag: tag, Current: "a", UpdatedAt: now,
			Seen: map[string]openAIGatewaySeen{"a": {At: now}},
		}
		row.Extra[openAIGatewayPoolContactsExtraKey] = gatewayPoolContacts{
			LedgerTag: tag, Seen: map[string]gatewayPoolContactSeen{"a": {LastAt: now}},
			Rounds: []gatewayPoolContactRound{{
				RoundID: gatewayPoolDigest("round"), Aliases: []string{gatewayPoolDigest(strconv.FormatInt(row.ID, 10))},
				Report: gwpool.ContactReport{ID: gatewayPoolDigest("report"), At: now},
			}},
		}
	}
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: tag, Current: "fresh", UpdatedAt: now.Add(time.Second),
		Seen: map[string]openAIGatewaySeen{"fresh": {At: now}},
	}
	peers := []Account{older, clone}
	before, err := json.Marshal(peers)
	require.NoError(t, err)
	svc := &OpenAIGatewayService{}
	cache := gatewayPoolDisplayCache{}
	for _, row := range []*Account{account, &peers[1], account} {
		wantHistory, wantContacts := svc.gatewayPoolDisplaySnapshot(row, gwpoolTestIdentity, peers, nil)
		gotHistory, gotContacts := svc.gatewayPoolDisplaySnapshot(row, gwpoolTestIdentity, peers, cache)
		require.Equal(t, wantHistory, gotHistory)
		require.Equal(t, wantContacts, gotContacts)
		delete(gotHistory.Seen, "a")
		delete(gotContacts.Seen, "a")
		gotContacts.Rounds[0].Aliases[0] = "modified-output-must-not-reach-cache"
	}
	require.Len(t, cache, 3, "each actual snapshot is parsed once, not once per clone output")
	after, err := json.Marshal(peers)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestGatewayPoolRuntimeSnapshotClearsReadyWhenVerifiedTicketDies(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	identity := openAIGatewayPoolAccountKey(account)
	now := time.Now()
	run := svc.codexCookies.poolProgress.start(account.ID, gatewayPoolProgressScope{
		tag: gatewayPoolLedgerTag(identity), identity: identity, requestStarted: now,
	})
	gatewayPoolTestTried(&svc.codexCookies.poolProgress, run, 1)
	svc.codexCookies.poolProgress.update(run, "ready", "offline", false, true)
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{
		gateway: "offline", cookie: "fixture", version: "v",
	})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "gpt-6-luna")
	before, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID}, true)
	require.NoError(t, err)
	require.Equal(t, "ready", before[account.ID].Phase)
	svc.codexCookies.gatewayPoolMarkStale(identity, "v")
	after, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{account.ID}, true)
	require.NoError(t, err)
	require.Equal(t, "pending", after[account.ID].Phase)
	require.Empty(t, after[account.ID].Runtime.Tickets)
	require.Equal(t, before[account.ID].Attempt, after[account.ID].Attempt)
}

// The fixture holds one merged history and contact ledger with every stored
// field set, as stored and as the progress API now sends it.
// AccountGatewayCell.runtimeDisplay.spec.ts renders both and must see no
// difference. After changing either record or its display, run:
//
//	UPDATE_GWPOOL_DISPLAY_FIXTURE=1 go test -tags unit ./internal/service/ -run TestGatewayPoolRuntimeDisplayFixtureInSync
func TestGatewayPoolRuntimeDisplayFixtureInSync(t *testing.T) {
	const path = "../../../frontend/src/components/account/__tests__/gatewayRuntimeDisplay.fixture.json"
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	tag := "fixture-ledger"
	history := openAIGatewayHistory{
		LedgerTag: tag, Current: "unified-999", CurrentRegion: "europe", UpdatedAt: ago(7 * time.Minute), PoolLive: 62, PoolFree: 5,
		CooldownReset: gatewayPoolCooldownResetState{IntervalHours: 6, StartedAt: ago(24 * time.Hour), LastAt: ago(6 * time.Hour), ClearedAt: ago(3 * time.Hour)},
		Seen: map[string]openAIGatewaySeen{
			"unified-101": {At: ago(10 * time.Minute), Region: "us-east", Verdict: "degraded", FullAt: ago(5 * time.Hour), FullHeldMs: 43000,
				Cooldown: &gatewayPoolCooldown{ResetAt: ago(24 * time.Hour), SourcesKnown: true, BaseSeconds: 3600, LocalFloorSeconds: 1800,
					CycleAt: ago(10 * time.Minute), ScheduleUpdatedAt: ago(10 * time.Minute), WindowSeconds: 7200, FixedSeconds: 5400,
					Until: now.Add(3 * time.Hour), UpdatedAt: ago(10 * time.Minute), AttemptAt: ago(10 * time.Minute), AttemptSeconds: 7200,
					ElapsedSeconds: 600, Outcome: "degraded", Successes: map[int]int{3600: 2}, LastSuccessSeconds: 3600,
					LastSuccessAt: ago(48 * time.Hour), LegacyRecommendedSeconds: 10800, LegacyRecommendationSource: "pool",
					LegacyRecommendationUntil: now.Add(time.Hour)}},
			"unified-102": {At: ago(30 * time.Minute), Region: "east-asia", Verdict: "full", FullAt: ago(2 * time.Hour), FullHeldMs: 120000,
				Cooldown: &gatewayPoolCooldown{WindowSeconds: 7200, LegacyRecommendedSeconds: 10800}},
			"unified-103": {At: ago(20 * time.Minute), Region: "europe", Cooldown: &gatewayPoolCooldown{Cleared: true, WindowSeconds: 3600}},
			"unified-104": {At: ago(3 * time.Hour)},
			"unified-105": {At: ago(15 * time.Minute), Region: "country-jp", Verdict: "degraded", FullAt: ago(4 * time.Hour), FullHeldMs: 60000},
		},
	}
	refreshed := func(id string, at time.Time) gatewayPoolContactRound {
		return gatewayPoolContactRound{VersionHash: "v-106", Report: gwpool.ContactReport{
			ID: id, Gateway: "unified-106", Model: "gpt-6-luna", Source: "background", At: at, Outcome: "refreshed"}}
	}
	contacts := gatewayPoolContacts{
		LedgerTag: tag, TrackingSince: ago(6 * 24 * time.Hour), HistoryTruncated: true, LastUSAt: ago(time.Hour),
		Seen: map[string]gatewayPoolContactSeen{"unified-104": {FirstAt: ago(6 * 24 * time.Hour), LastAt: ago(50 * time.Minute)}},
		Rounds: []gatewayPoolContactRound{{
			RoundID: "round-1", Aliases: []string{"alias-1"}, VersionHash: "v-104", Report: gwpool.ContactReport{
				ID: "r1", AccountTag: "account-tag", Gateway: "unified-104", Model: "gpt-6-luna", Criterion: gwpool.ContactCriterion,
				Source: "foreground", First: "repeat", At: ago(50 * time.Minute), GapKnown: true, ElapsedSeconds: 300,
				Outcome: "full", FullWindowMS: 150000, WindowFinal: true},
			LastAt: ago(50 * time.Minute), LastProbeOutcome: "full", LastProbeAt: ago(50 * time.Minute), LastProbeModel: "gpt-6-luna",
			LastProbeSource: "foreground", Steps: []gatewayPoolProbeStep{{Shot: "verify", Sent: true, Status: 200, GotState: true,
				EchoAccepted: true, ActualGateway: "unified-104", DurationMS: 812}},
		}, refreshed("r2", ago(40*time.Minute)), refreshed("r3", ago(41*time.Minute))},
	}
	// A field the cell may read must carry a value in the stored form.
	set := map[string]bool{}
	markGatewayPoolFixtureFields(reflect.ValueOf(history), "history", set)
	markGatewayPoolFixtureFields(reflect.ValueOf(contacts), "contacts", set)
	var unset []string
	for field, ok := range set {
		if !ok {
			unset = append(unset, field)
		}
	}
	sort.Strings(unset)
	require.Empty(t, unset, "give every stored field a value in this fixture")

	want, err := json.MarshalIndent(map[string]any{
		"now": now, "ledger_tag": tag, "gateway_window_seconds": 3600,
		"stored":    map[string]any{"history": history, "contacts": contacts},
		"displayed": map[string]any{"history": history.display(), "contacts": contacts.display()},
	}, "", "  ")
	require.NoError(t, err)
	want = append(want, '\n')
	if os.Getenv("UPDATE_GWPOOL_DISPLAY_FIXTURE") == "1" {
		require.NoError(t, os.WriteFile(path, want, 0o644))
		return
	}
	got, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("frontend sources not available")
	}
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(got), "the display fixture is stale; regenerate it as described above")
}

// markGatewayPoolFixtureFields records for each field path whether any value
// on it is set. An empty map or slice is unset.
func markGatewayPoolFixtureFields(v reflect.Value, path string, set map[string]bool) {
	value := !v.IsZero()
	if v.Kind() == reflect.Map || v.Kind() == reflect.Slice {
		value = v.Len() > 0
	}
	set[path] = set[path] || value
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			markGatewayPoolFixtureFields(v.Elem(), path, set)
		}
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			return
		}
		for i := range v.NumField() {
			markGatewayPoolFixtureFields(v.Field(i), path+"."+v.Type().Field(i).Name, set)
		}
	case reflect.Slice:
		for i := range v.Len() {
			markGatewayPoolFixtureFields(v.Index(i), path+"[]", set)
		}
	case reflect.Map:
		for entry := v.MapRange(); entry.Next(); {
			markGatewayPoolFixtureFields(entry.Value(), path+"{}", set)
		}
	}
}
