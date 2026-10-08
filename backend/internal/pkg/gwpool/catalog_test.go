package gwpool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type catalogTestTransport func(*http.Request) (*http.Response, error)

func (f catalogTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCatalogNewWaiterDoesNotJoinCanceledFlight(t *testing.T) {
	client := New("http://offline.invalid", "offline", 0)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var hits atomic.Int32
	client.http.Transport = catalogTestTransport(func(r *http.Request) (*http.Response, error) {
		if hits.Add(1) == 1 {
			close(entered)
			<-r.Context().Done()
			close(canceled)
			<-release // old request has not yet returned to refreshCatalog
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"gateways":[]}`))}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.Catalog(ctx, "member", "", "", 0); done <- err }()
	<-entered
	client.catalog.mu.Lock()
	oldFlight := client.catalog.entries[catalogKey{"member", "", ""}].flight
	client.catalog.mu.Unlock()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-canceled
	nextCtx, stop := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stop()
	replacement, err := client.Catalog(nextCtx, "member", "", "", 0)
	if err != nil {
		t.Fatalf("healthy waiter inherited abandoned request: %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("replacement network requests=%d", hits.Load())
	}
	close(release)
	<-oldFlight.done
	after, err := client.Catalog(context.Background(), "member", "", "", 0)
	if err != nil || after.Generation != replacement.Generation || hits.Load() != 2 {
		t.Fatalf("late canceled flight overwrote replacement: %+v %v hits=%d", after, err, hits.Load())
	}
}

func TestCatalogRequiresExplicitArrayBeforeConfirmingEmpty(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"other":[]}`, `{"gateways":null}`, `{"gateways":{}}`,
		`{"gateways":[null]}`, `{"gateways":[{}]}`, `{"gateways":[{"name":" "}]}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			catalog, err := New(server.URL, "offline", 0).FreshCatalog(context.Background(), "member", "", "")
			if err == nil || catalog.Generation != 0 {
				t.Fatalf("malformed inventory became confirmed empty: %+v %v", catalog, err)
			}
		})
	}
}

func TestCatalogReusesMetadata(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, `{"gateways":[{"name":"g1","pair_ready":true,"valid_for_s":3600}]}`)
	}))
	defer server.Close()
	client := New(server.URL, "test-key", 0)
	for range 2 {
		gateways, err := client.GatewaysForModel(context.Background(), "member-a", "", "model")
		if err != nil || len(gateways) != 1 {
			t.Fatalf("catalog=%v err=%v", gateways, err)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("unchanged directory fetched %d times", hits.Load())
	}
}

func TestCatalogExpiryGenerationIsolationAndFailure(t *testing.T) {
	var hits atomic.Int64
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if fail.Load() {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, `{"gateways":[{"name":"g1","pair_ready":true,"valid_for_s":5,"priority":{"model":"model-a","full":5,"samples":5}}]}`)
	}))
	defer server.Close()
	client := New(server.URL, "test-key", 0)
	now := time.Now()
	client.catalog.now = func() time.Time { return now }
	ctx := context.Background()
	first, err := client.Catalog(ctx, "member-a", "", "model-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	first.Gateways[0].Name = "mutated"
	first.Gateways[0].Priority.Full = 0
	now = now.Add(4 * time.Second)
	again, err := client.Catalog(ctx, "member-a", "", "model-a", 0)
	if err != nil || hits.Load() != 1 || again.Gateways[0].Name != "g1" || again.Gateways[0].Priority.Full != 5 {
		t.Fatalf("cache shares mutable response or extended access: %+v err=%v hits=%d", again, err, hits.Load())
	}
	now = now.Add(time.Second)
	newer, err := client.Catalog(ctx, "member-a", "", "model-a", 0)
	if err != nil || newer.Generation <= first.Generation || hits.Load() != 2 {
		t.Fatalf("expiry must use the original lifetime: %+v %v hits=%d", newer, err, hits.Load())
	}
	// Readers of generation 1 reuse generation 2 instead of refetching it.
	_, err = client.Catalog(ctx, "member-a", "", "model-a", first.Generation)
	if err != nil || hits.Load() != 2 {
		t.Fatalf("duplicate generation refresh: %v %d", err, hits.Load())
	}
	for _, key := range []catalogKey{
		{"member-b", "", "model-a"}, {"member-a", "", "model-b"},
		{"member-a", fmt.Sprintf("%064x", 1), "model-a"},
	} {
		if _, err := client.Catalog(ctx, key.account, key.tag, key.model, 0); err != nil {
			t.Fatal(err)
		}
	}
	otherClient := New(server.URL, "different-key", 0)
	if _, err := otherClient.Catalog(ctx, "member-a", "", "model-a", 0); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 6 {
		t.Fatalf("identity/model/permission isolation failed: %d", hits.Load())
	}
	fail.Store(true)
	failed, err := client.Catalog(ctx, "member-a", "", "model-a", newer.Generation)
	if err == nil || failed.Generation != 0 || len(failed.Gateways) != 0 {
		t.Fatal("failure exposed stale inventory as fresh")
	}
	if _, err := client.Catalog(ctx, "member-a", "", "model-a", 0); err == nil {
		t.Fatal("failed refresh retained stale successful cache")
	}
}

func TestCatalogBoundsEntriesAndThirtySecondFreshness(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, `{"gateways":[{"name":"g","pair_ready":true,"valid_for_s":3600}]}`)
	}))
	defer server.Close()
	client := New(server.URL, "test-key", 0)
	now := time.Now()
	client.catalog.now = func() time.Time { return now }
	ctx := context.Background()
	for i := range maxCatalogEntries + 1 {
		if _, err := client.Catalog(ctx, fmt.Sprintf("member-%d", i), "", "", 0); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Millisecond)
	}
	if len(client.catalog.entries) != maxCatalogEntries {
		t.Fatal("cache grew beyond its bound")
	}
	before := hits.Load()
	now = now.Add(catalogFreshness)
	if _, err := client.Catalog(ctx, "member-256", "", "", 0); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != before+1 {
		t.Fatal("directory cache outlived 30 seconds")
	}
}

func TestCatalogSharesRefreshAndIndependentCancellation(t *testing.T) {
	var hits atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		close(entered)
		<-release
		fmt.Fprint(w, `{"gateways":[{"name":"g","pair_ready":true,"valid_for_s":3600}]}`)
	}))
	defer server.Close()
	client := New(server.URL, "test-key", 0)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := client.Catalog(firstCtx, "member", "", "", 0); firstDone <- err }()
	<-entered
	go func() { _, err := client.Catalog(context.Background(), "member", "", "", 0); secondDone <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		client.catalog.mu.Lock()
		waiters := client.catalog.entries[catalogKey{"member", "", ""}].flight.waiters
		client.catalog.mu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("second waiter never joined")
		}
		time.Sleep(time.Millisecond)
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first waiter: %v", err)
	}
	close(release)
	if err := <-secondDone; err != nil {
		t.Fatalf("first cancellation killed other waiter: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("duplicate refreshes: %d", hits.Load())
	}
}

func TestCatalogLastCancellationCancelsNetwork(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	client := New(server.URL, "test-key", 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := client.Catalog(ctx, "member", "", "", 0); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("orphaned directory request remained active")
	}
}
