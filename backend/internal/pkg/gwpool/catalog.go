package gwpool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	catalogFreshness  = 30 * time.Second
	maxCatalogEntries = 256
)

// Catalog contains metadata only. Generation allows all readers of a stale
// snapshot to share one refresh, without repeatedly invalidating a newer one.
type Catalog struct {
	Gateways   []Gateway
	Generation uint64
}

type catalogKey struct{ account, tag, model string }

func makeCatalogKey(account, tag, model string) catalogKey {
	key := catalogKey{strings.TrimSpace(account), tag, sanitizeOpaque(model, 128)}
	if !validCooldownTag(key.tag) {
		key.tag = ""
	}
	return key
}

type catalogEntry struct {
	value  Catalog
	until  time.Time
	used   time.Time
	flight *catalogFlight
	// A canceled refresh leaves business reuse unchanged, but its old snapshot
	// cannot claim a settled display until a new refresh succeeds.
	displayUnknown bool
}
type catalogFlight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	value   Catalog
	err     error
}
type catalogCache struct {
	mu         sync.Mutex
	entries    map[catalogKey]*catalogEntry
	generation uint64
	now        func() time.Time
}

func (c *catalogCache) timeNow() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// Catalog returns a fresh cached snapshot, or shares an on-demand refresh.
// after=0 permits a cache hit; otherwise the result must be newer than after.
// The Client already isolates base URL, consumer key and request timeout.
func (c *Client) Catalog(ctx context.Context, account, tag, model string, after uint64) (Catalog, error) {
	if c == nil {
		return Catalog{}, fmt.Errorf("%w: client is nil", ErrPool)
	}
	if err := ctx.Err(); err != nil {
		return Catalog{}, err
	}
	key := makeCatalogKey(account, tag, model)
	cache := &c.catalog
	cache.mu.Lock()
	now := cache.timeNow()
	if cache.entries == nil {
		cache.entries = make(map[catalogKey]*catalogEntry)
	}
	entry := cache.entries[key]
	if entry == nil {
		if len(cache.entries) >= maxCatalogEntries {
			var oldest catalogKey
			var oldestAt time.Time
			for k, candidate := range cache.entries {
				if candidate.flight == nil && (oldestAt.IsZero() || candidate.used.Before(oldestAt)) {
					oldest, oldestAt = k, candidate.used
				}
			}
			if oldestAt.IsZero() {
				cache.mu.Unlock()
				return Catalog{}, fmt.Errorf("%w: directory refresh capacity reached", ErrPool)
			}
			delete(cache.entries, oldest)
		}
		entry = &catalogEntry{}
		cache.entries[key] = entry
	}
	entry.used = now
	if now.Before(entry.until) && (after == 0 || entry.value.Generation > after) {
		value := cloneCatalog(entry.value)
		cache.mu.Unlock()
		return value, nil
	}
	if entry.flight == nil {
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.http.Timeout)
		entry.flight = &catalogFlight{done: make(chan struct{}), cancel: cancel}
		go c.refreshCatalog(workCtx, key, entry, entry.flight, now)
	}
	flight := entry.flight
	flight.waiters++
	cache.mu.Unlock()
	select {
	case <-ctx.Done():
		cache.mu.Lock()
		flight.waiters--
		if flight.waiters == 0 {
			flight.cancel()
			if entry.flight == flight {
				entry.flight = nil
				entry.displayUnknown = true
			}
		}
		cache.mu.Unlock()
		return Catalog{}, ctx.Err()
	case <-flight.done:
		if err := ctx.Err(); err != nil {
			return Catalog{}, err
		}
		return cloneCatalog(flight.value), flight.err
	}
}

// FreshCatalog is for conclusive exhaustion checks. Concurrent checks share the
// same in-flight refresh; sequential checks cannot reuse an older empty result.
func (c *Client) FreshCatalog(ctx context.Context, account, tag, model string) (Catalog, error) {
	if c == nil {
		return Catalog{}, fmt.Errorf("%w: client is nil", ErrPool)
	}
	key := makeCatalogKey(account, tag, model)
	c.catalog.mu.Lock()
	var after uint64
	if entry := c.catalog.entries[key]; entry != nil {
		after = entry.value.Generation
	}
	c.catalog.mu.Unlock()
	return c.Catalog(ctx, account, tag, model, after)
}

// PeekCatalog is display-only: no network, refresh, TTL extension, LRU touch,
// or new cache entry. In-flight, missing and expired snapshots remain unknown.
func (c *Client) PeekCatalog(account, tag, model string) (Catalog, time.Time, bool) {
	if c == nil {
		return Catalog{}, time.Time{}, false
	}
	c.catalog.mu.Lock()
	defer c.catalog.mu.Unlock()
	entry := c.catalog.entries[makeCatalogKey(account, tag, model)]
	if entry == nil || entry.flight != nil || entry.displayUnknown || entry.value.Generation == 0 || !c.catalog.timeNow().Before(entry.until) {
		return Catalog{}, time.Time{}, false
	}
	return cloneCatalog(entry.value), entry.until, true
}

func (c *Client) refreshCatalog(ctx context.Context, key catalogKey, entry *catalogEntry, flight *catalogFlight, started time.Time) {
	defer flight.cancel()
	gateways, err := c.fetchGateways(ctx, key.account, key.tag, key.model)
	c.catalog.mu.Lock()
	defer c.catalog.mu.Unlock()
	// A canceled flight can finish after a replacement has already succeeded.
	// It must not publish old data or erase the replacement's in-flight state.
	current := entry.flight == flight
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && current {
		c.catalog.generation++
		flight.value = Catalog{Gateways: gateways, Generation: c.catalog.generation}
		entry.value = flight.value
		entry.displayUnknown = false
		// Age starts before the network call, never on cache access. Missing or
		// expired lifetimes make a response non-cacheable, not immortal.
		entry.until = started.Add(catalogFreshness)
		for _, gateway := range gateways {
			if gateway.PairReady {
				entry.until = minTime(entry.until, gateway.AvailableUntil)
			}
		}
	} else if current {
		entry.until = time.Time{}
	}
	flight.err = err
	if current {
		entry.flight = nil
	}
	close(flight.done)
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func cloneCatalog(value Catalog) Catalog {
	copyValue := value
	copyValue.Gateways = append([]Gateway(nil), value.Gateways...)
	for i := range copyValue.Gateways {
		gateway := &copyValue.Gateways[i]
		gateway.Contacts = append([]ContactStats(nil), gateway.Contacts...)
		if gateway.Priority != nil {
			value := *gateway.Priority
			gateway.Priority = &value
		}
		if gateway.Cooldown != nil {
			value := *gateway.Cooldown
			gateway.Cooldown = &value
		}
	}
	return copyValue
}
