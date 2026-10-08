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

func TestPeekCatalogIsIsolatedReadOnlyAndExpires(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, `{"gateways":[{"name":"g1","pair_ready":true,"valid_for_s":3600}]}`)
	}))
	defer server.Close()
	client := New(server.URL, "offline-key", time.Second)
	now := time.Now()
	client.catalog.now = func() time.Time { return now }
	tag := strings.Repeat("a", 64)
	if _, _, ok := client.PeekCatalog("member", tag, "model"); ok || len(client.catalog.entries) != 0 || hits.Load() != 0 {
		t.Fatal("unknown peek populated a cache or made a request")
	}
	loaded, err := client.Catalog(context.Background(), "member", tag, "model", 0)
	if err != nil {
		t.Fatal(err)
	}
	entry := client.catalog.entries[makeCatalogKey("member", tag, "model")]
	used, until := entry.used, entry.until
	now = now.Add(5 * time.Second)
	value, deadline, ok := client.PeekCatalog("member", tag, "model")
	if !ok || !deadline.Equal(until) || value.Generation != loaded.Generation {
		t.Fatalf("valid metadata was not returned: %+v %v %v", value, deadline, ok)
	}
	value.Gateways[0].Name = "mutated-output"
	again, _, _ := client.PeekCatalog("member", tag, "model")
	if again.Gateways[0].Name != "g1" || !entry.used.Equal(used) || !entry.until.Equal(until) || hits.Load() != 1 {
		t.Fatal("peek mutated shared data, LRU, TTL, or made an extra request")
	}
	for _, key := range []catalogKey{
		{"other-member", tag, "model"}, {"member", strings.Repeat("b", 64), "model"}, {"member", tag, "other-model"},
	} {
		if _, _, ok := client.PeekCatalog(key.account, key.tag, key.model); ok {
			t.Fatalf("scope leak: %+v", key)
		}
	}
	otherClient := New(server.URL, "other-key", time.Second)
	if _, _, ok := otherClient.PeekCatalog("member", tag, "model"); ok {
		t.Fatal("client credentials share a catalog")
	}
	entry.flight = &catalogFlight{}
	if _, _, ok := client.PeekCatalog("member", tag, "model"); ok {
		t.Fatal("in-flight refresh must not be presented as a settled snapshot")
	}
	entry.flight = nil
	now = until
	if _, _, ok := client.PeekCatalog("member", tag, "model"); ok || hits.Load() != 1 {
		t.Fatal("expiry boundary must remain unknown without a refresh")
	}
}

func TestPeekCatalogDistinguishesEmptyFromRefreshFailure(t *testing.T) {
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"gateways":[]}`)
	}))
	defer server.Close()
	client := New(server.URL, "offline-key", time.Second)
	if _, err := client.Catalog(context.Background(), "member", "", "", 0); err != nil {
		t.Fatal(err)
	}
	if value, _, ok := client.PeekCatalog("member", "", ""); !ok || len(value.Gateways) != 0 {
		t.Fatal("explicit successful empty inventory should remain known")
	}
	fail.Store(true)
	if _, err := client.FreshCatalog(context.Background(), "member", "", ""); err == nil {
		t.Fatal("expected refresh failure")
	}
	if _, _, ok := client.PeekCatalog("member", "", ""); ok {
		t.Fatal("failed refresh must not resurrect the previous empty catalog")
	}
}

func TestPeekCatalogCanceledRefreshStaysUnknownUntilSuccess(t *testing.T) {
	client := New("http://offline.invalid", "offline-key", time.Second)
	entered := make(chan struct{})
	var hits atomic.Int32
	client.http.Transport = catalogTestTransport(func(r *http.Request) (*http.Response, error) {
		if hits.Add(1) == 2 {
			close(entered)
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"gateways":[]}`)),
		}, nil
	})
	initial, err := client.Catalog(context.Background(), "member", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, refreshErr := client.FreshCatalog(ctx, "member", "", "")
		done <- refreshErr
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected canceled waiter, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh waiter did not cancel")
	}
	if _, _, ok := client.PeekCatalog("member", "", ""); ok {
		t.Fatal("canceled refresh resurrected a known-empty display snapshot")
	}
	// Display uncertainty must not change the business cache's reuse rules.
	cached, err := client.Catalog(context.Background(), "member", "", "", 0)
	if err != nil || cached.Generation != initial.Generation || hits.Load() != 2 {
		t.Fatalf("display guard changed business cache semantics: %+v %v hits=%d", cached, err, hits.Load())
	}
	if _, _, ok := client.PeekCatalog("member", "", ""); ok {
		t.Fatal("ordinary cache hit must not clear display uncertainty")
	}
	fresh, err := client.FreshCatalog(context.Background(), "member", "", "")
	if err != nil {
		t.Fatal(err)
	}
	value, _, ok := client.PeekCatalog("member", "", "")
	if !ok || len(value.Gateways) != 0 || value.Generation != fresh.Generation || fresh.Generation <= initial.Generation {
		t.Fatalf("successful refresh did not restore known empty inventory: %+v %v", value, ok)
	}
}
