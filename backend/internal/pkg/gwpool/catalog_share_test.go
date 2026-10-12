package gwpool

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	shareTagA = strings.Repeat("a", 64)
	shareTagB = strings.Repeat("b", 64)
)

// The list tells which request produced it (priority.full = request number);
// each member's cooldown tells whose part it is. gate may hold a request or
// answer it itself (returning true).
func shareServer(t *testing.T, hits *atomic.Int32, gate func(n int32, w http.ResponseWriter, r *http.Request) bool) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if gate != nil && gate(n, w, r) {
			return
		}
		seconds := 3600
		if r.Header.Get(cooldownAccountHeader) == shareTagB {
			seconds = 7200
		}
		fmt.Fprintf(w, `{"account":%q,"gateways":[{"name":"g","pair_ready":true,"valid_for_s":3600,`+
			`"priority":{"model":"m","full":%d,"samples":10},"cooldown":{"seconds":%d,"samples":3,"source":"account"},`+
			`"contacts":[{"gateway":"g","model":"m","criterion":"c","full":1}]}]}`, r.URL.Query().Get("account"), n, seconds)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestCatalogMembersShareTheListButKeepTheirOwnPart(t *testing.T) {
	var hits atomic.Int32
	client := New(shareServer(t, &hits, nil).URL, "test-key", time.Second)
	start := time.Now()
	now := start
	client.catalog.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Catalog(ctx, "member-b", shareTagB, "m", 0); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 2 {
		t.Fatalf("a member without its own part must ask for itself: hits=%d", hits.Load())
	}
	now = now.Add(catalogFreshness + time.Second)
	refreshed, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0)
	if err != nil || hits.Load() != 3 || refreshed.Gateways[0].Cooldown.Seconds != 3600 {
		t.Fatalf("refresh: %+v %v hits=%d", refreshed, err, hits.Load())
	}
	shared, err := client.Catalog(ctx, "member-b", shareTagB, "m", 0)
	if err != nil || hits.Load() != 3 {
		t.Fatalf("member-b must reuse member-a's refresh: %v hits=%d", err, hits.Load())
	}
	gateway := shared.Gateways[0]
	if shared.Generation != refreshed.Generation || gateway.Priority.Full != 3 {
		t.Fatalf("member-b did not get the refreshed list: %+v", shared)
	}
	if gateway.Cooldown == nil || gateway.Cooldown.Seconds != 7200 || len(gateway.Contacts) != 1 || !gateway.MemberAt.Equal(start) {
		t.Fatalf("member-b lost its own part or took member-a's: %+v", gateway)
	}
	gateway.Cooldown.Seconds, gateway.Priority.Full = 1, 1
	again, _ := client.Catalog(ctx, "member-b", shareTagB, "m", 0)
	if again.Gateways[0].Cooldown.Seconds != 7200 || again.Gateways[0].Priority.Full != 3 {
		t.Fatal("a returned view shares mutable cache data")
	}
}

func TestCatalogMemberPartExpiresAfterTwoHours(t *testing.T) {
	var hits atomic.Int32
	client := New(shareServer(t, &hits, nil).URL, "test-key", time.Second)
	now := time.Now()
	client.catalog.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0); err != nil {
		t.Fatal(err)
	}
	now = now.Add(memberFreshness)
	if _, err := client.Catalog(ctx, "member-b", shareTagB, "m", 0); err != nil {
		t.Fatal(err)
	}
	if snapshot, ok := client.PeekCatalogSnapshot("member-a", shareTagA, "m"); !ok || !snapshot.Stale {
		t.Fatalf("a two-hour-old own part shows as settled: %+v %v", snapshot, ok)
	}
	now = now.Add(time.Second)
	own, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0)
	if err != nil || hits.Load() != 3 || !own.Gateways[0].MemberAt.Equal(now) {
		t.Fatalf("a two-hour-old own part must be refreshed under a fresh list: %+v %v hits=%d", own, err, hits.Load())
	}
}

func TestFreshCatalogChecksOfMembersShareOneRefresh(t *testing.T) {
	var hits atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	client := New(shareServer(t, &hits, func(n int32, _ http.ResponseWriter, _ *http.Request) bool {
		if n == 3 {
			close(entered)
			<-release
		}
		return false
	}).URL, "test-key", time.Second)
	ctx := context.Background()
	before, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Catalog(ctx, "member-b", shareTagB, "m", 0); err != nil {
		t.Fatal(err)
	}
	results := make(chan Catalog, 2)
	check := func(member, tag string) {
		catalog, err := client.FreshCatalog(ctx, member, tag, "m")
		if err != nil {
			t.Error(err)
		}
		results <- catalog
	}
	go check("member-a", shareTagA)
	<-entered
	go check("member-b", shareTagB)
	deadline := time.Now().Add(time.Second)
	for {
		client.catalog.mu.Lock()
		waiters := 0
		for _, flight := range client.catalog.lists["m"].flights {
			waiters += flight.waiters
		}
		client.catalog.mu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("second check never joined the refresh")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	first, second := <-results, <-results
	if hits.Load() != 3 || first.Generation != second.Generation || first.Generation <= before.Generation {
		t.Fatalf("concurrent checks: hits=%d generations %d %d", hits.Load(), first.Generation, second.Generation)
	}
}

// shareHold holds requests until opened; open it in a defer so a failing test
// never leaves the server waiting on a held request.
type shareHold struct {
	entered, release chan struct{}
	once             sync.Once
}

func newShareHold() *shareHold {
	return &shareHold{entered: make(chan struct{}, 8), release: make(chan struct{})}
}

func (h *shareHold) wait() { h.entered <- struct{}{}; <-h.release }
func (h *shareHold) open() { h.once.Do(func() { close(h.release) }) }

// shareClock drives the client's clock from several goroutines.
func shareClock(client *Client) (advance func(time.Duration) time.Time) {
	var at atomic.Int64
	at.Store(time.Now().UnixNano())
	client.catalog.now = func() time.Time { return time.Unix(0, at.Load()) }
	return func(d time.Duration) time.Time { return time.Unix(0, at.Add(int64(d))) }
}

func waitWaiters(t *testing.T, client *Client, model string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		client.catalog.mu.Lock()
		waiters := 0
		for _, flight := range client.catalog.lists[model].flights {
			waiters += flight.waiters
		}
		client.catalog.mu.Unlock()
		if waiters == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("refresh waiters=%d, want %d", waiters, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// catalogWithin fails instead of hanging when call waits on a held refresh.
func catalogWithin(t *testing.T, call func() (Catalog, error)) Catalog {
	t.Helper()
	done := make(chan Catalog, 1)
	go func() {
		catalog, err := call()
		if err != nil {
			t.Error(err)
		}
		done <- catalog
	}()
	select {
	case catalog := <-done:
		return catalog
	case <-time.After(2 * time.Second):
		t.Fatal("waited on a held refresh it must not join")
		return Catalog{}
	}
}

// A refresh overtaken by a later-started one keeps the newer list, and a fresh
// check that joined it asks again instead of taking the list it already saw.
func TestCatalogOvertakenRefreshKeepsNewerListAndFreshCheckAsksAgain(t *testing.T) {
	var hits atomic.Int32
	hold := newShareHold()
	defer hold.open()
	client := New(shareServer(t, &hits, func(n int32, _ http.ResponseWriter, _ *http.Request) bool {
		if n == 2 {
			hold.wait()
		}
		return false
	}).URL, "test-key", 5*time.Second)
	advance := shareClock(client)
	ctx := context.Background()
	if _, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0); err != nil {
		t.Fatal(err)
	}
	advance(catalogFreshness + time.Second)
	slow := make(chan Catalog, 1)
	go func() {
		catalog, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0)
		if err != nil {
			t.Error(err)
		}
		slow <- catalog
	}()
	<-hold.entered
	advance(time.Second)
	newer, err := client.Catalog(ctx, "member-b", shareTagB, "m", 0)
	if err != nil || newer.Gateways[0].Priority.Full != 3 {
		t.Fatalf("later refresh: %+v %v", newer, err)
	}
	fresh := make(chan Catalog, 1)
	go func() {
		catalog, err := client.FreshCatalog(ctx, "member-a", shareTagA, "m")
		if err != nil {
			t.Error(err)
		}
		fresh <- catalog
	}()
	waitWaiters(t, client, "m", 2)
	hold.open()
	if got := <-slow; got.Generation != newer.Generation || got.Gateways[0].Priority.Full != 3 {
		t.Fatalf("an overtaken refresh replaced the newer list: %+v", got)
	}
	if got := <-fresh; got.Generation <= newer.Generation || hits.Load() != 4 {
		t.Fatalf("fresh check took the list it had already seen: %+v hits=%d", got, hits.Load())
	}
}

// Only a member with a current own part joins another member's refresh, and
// only one that started after the current list.
func TestCatalogJoinsOnlyRefreshesThatCanRenewTheList(t *testing.T) {
	var hits atomic.Int32
	hold := newShareHold()
	defer hold.open()
	client := New(shareServer(t, &hits, func(n int32, _ http.ResponseWriter, _ *http.Request) bool {
		if n == 3 {
			hold.wait()
		}
		return false
	}).URL, "test-key", 5*time.Second)
	advance := shareClock(client)
	ctx := context.Background()
	for _, member := range [][2]string{{"member-a", shareTagA}, {"member-b", shareTagB}} {
		if _, err := client.Catalog(ctx, member[0], member[1], "m", 0); err != nil {
			t.Fatal(err)
		}
	}
	advance(catalogFreshness + time.Second)
	go func() { _, _ = client.Catalog(ctx, "member-a", shareTagA, "m", 0) }()
	<-hold.entered
	advance(time.Second)
	tagC := strings.Repeat("c", 64)
	own := catalogWithin(t, func() (Catalog, error) { return client.Catalog(ctx, "member-c", tagC, "m", 0) })
	if own.Gateways[0].Priority.Full != 4 || own.Gateways[0].MemberAt.IsZero() {
		t.Fatalf("a member without its own part must ask for itself: %+v", own)
	}
	advance(catalogFreshness + time.Second)
	renewed := catalogWithin(t, func() (Catalog, error) { return client.Catalog(ctx, "member-b", shareTagB, "m", 0) })
	if renewed.Gateways[0].Priority.Full != 5 || renewed.Generation <= own.Generation {
		t.Fatalf("a refresh older than the list cannot renew it: %+v", renewed)
	}
}

// A late refresh of an older own part, here from another model, never
// replaces the newer one.
func TestCatalogOlderOwnPartNeverReplacesANewerOne(t *testing.T) {
	var hits atomic.Int32
	hold := newShareHold()
	defer hold.open()
	client := New(shareServer(t, &hits, func(n int32, _ http.ResponseWriter, _ *http.Request) bool {
		if n == 1 {
			hold.wait()
		}
		return false
	}).URL, "test-key", 5*time.Second)
	advance := shareClock(client)
	ctx := context.Background()
	late := make(chan struct{})
	go func() {
		defer close(late)
		if _, err := client.Catalog(ctx, "member-a", shareTagA, "m1", 0); err != nil {
			t.Error(err)
		}
	}()
	<-hold.entered
	newer := advance(time.Second)
	if _, err := client.Catalog(ctx, "member-a", shareTagA, "m2", 0); err != nil {
		t.Fatal(err)
	}
	hold.open()
	<-late
	got, err := client.Catalog(ctx, "member-a", shareTagA, "m2", 0)
	if err != nil || !got.Gateways[0].MemberAt.Equal(newer) {
		t.Fatalf("an older own part replaced the newer one: %+v %v", got, err)
	}
}

// An own part taken while the pool listed nothing does not serve a later list;
// the member asks for itself instead of going without its part for hours.
func TestCatalogEmptyOwnPartDoesNotServeAListedPool(t *testing.T) {
	var hits atomic.Int32
	client := New(shareServer(t, &hits, func(n int32, w http.ResponseWriter, _ *http.Request) bool {
		if n == 1 {
			fmt.Fprint(w, `{"gateways":[]}`)
			return true
		}
		return false
	}).URL, "test-key", time.Second)
	shareClock(client)
	ctx := context.Background()
	for _, member := range [][2]string{{"member-a", shareTagA}, {"member-b", shareTagB}} {
		if _, err := client.Catalog(ctx, member[0], member[1], "m", 0); err != nil {
			t.Fatal(err)
		}
	}
	got, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0)
	if err != nil || hits.Load() != 3 || got.Gateways[0].Cooldown == nil || got.Gateways[0].MemberAt.IsZero() {
		t.Fatalf("member went without its own part: %+v %v hits=%d", got, err, hits.Load())
	}
}

// One member's refusal neither fails the members that joined its refresh nor
// voids the list they share.
func TestCatalogMemberRefusalStaysWithThatMember(t *testing.T) {
	var hits atomic.Int32
	hold := newShareHold()
	defer hold.open()
	client := New(shareServer(t, &hits, func(n int32, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Query().Get("account") != "member-b" {
			return false
		}
		if n == 3 {
			hold.wait()
		}
		w.WriteHeader(http.StatusBadRequest)
		return true
	}).URL, "test-key", 5*time.Second)
	advance := shareClock(client)
	ctx := context.Background()
	if _, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Catalog(ctx, "member-b", shareTagB, "m", 0); err == nil {
		t.Fatal("refusal lost")
	}
	if _, ok := client.CachedCatalog("member-a", shareTagA, "m"); !ok {
		t.Fatal("one member's refusal voided the shared list")
	}
	advance(catalogFreshness + time.Second)
	go func() { _, _ = client.Catalog(ctx, "member-b", shareTagB, "m", 0) }()
	<-hold.entered
	joined := make(chan error, 1)
	go func() {
		_, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0)
		joined <- err
	}()
	waitWaiters(t, client, "m", 2)
	hold.open()
	if err := <-joined; err != nil || hits.Load() != 4 {
		t.Fatalf("joined member inherited another member's refusal: %v hits=%d", err, hits.Load())
	}
}

// A newly listed gateway sends no member back to the pool: its row goes without
// the member's own part until the member next asks for itself.
func TestCatalogNewlyListedGatewayCostsNoRequest(t *testing.T) {
	var hits atomic.Int32
	client := New(shareServer(t, &hits, func(n int32, w http.ResponseWriter, _ *http.Request) bool {
		if n == 2 {
			fmt.Fprint(w, `{"gateways":[{"name":"g","pair_ready":true,"valid_for_s":3600},{"name":"g1","pair_ready":true,"valid_for_s":3600}]}`)
			return true
		}
		return false
	}).URL, "test-key", time.Second)
	advance := shareClock(client)
	ctx := context.Background()
	if _, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0); err != nil {
		t.Fatal(err)
	}
	advance(catalogFreshness + time.Second)
	if _, err := client.Catalog(ctx, "member-b", shareTagB, "m", 0); err != nil {
		t.Fatal(err)
	}
	got, err := client.Catalog(ctx, "member-a", shareTagA, "m", 0)
	if err != nil || hits.Load() != 2 || len(got.Gateways) != 2 {
		t.Fatalf("a newly listed gateway cost a request: %+v %v hits=%d", got, err, hits.Load())
	}
	if got.Gateways[0].Cooldown == nil || !got.Gateways[1].MemberAt.IsZero() {
		t.Fatalf("own part misplaced: %+v", got.Gateways)
	}
}
