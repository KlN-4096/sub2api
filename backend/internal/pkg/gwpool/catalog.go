package gwpool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	catalogFreshness = 30 * time.Second
	// memberFreshness is how long one member's own part of /gateways (cooldown
	// and contacts) serves under fresher shared lists. Both change slowly, and
	// report acknowledgements carry newer recommendations sooner.
	memberFreshness   = 2 * time.Hour
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

type catalogMemberKey struct{ account, tag string }

func (k catalogKey) member() catalogMemberKey { return catalogMemberKey{k.account, k.tag} }

// catalogList is the member-independent part of /gateways for one model. The
// pool guarantees it does not depend on the asking member, and the Client
// already isolates base URL, consumer key and timeout, so all members share it.
type catalogList struct {
	gateways   []Gateway // without Cooldown, Contacts and MemberAt
	generation uint64
	until      time.Time
	used       time.Time
	flights    map[catalogMemberKey]*catalogFlight
	// A canceled refresh leaves business reuse unchanged, but its old snapshot
	// cannot claim a settled display until a new refresh succeeds.
	displayUnknown bool
	snapshotAt     time.Time
	snapshotUntil  time.Time
}

// catalogMember is one member's own part of /gateways. The cooldown map has a
// key for every gateway that member's last own catalog listed.
type catalogMember struct {
	cooldown map[string]*CooldownRecommendation
	contacts map[string][]ContactStats
	at       time.Time
	used     time.Time
}

// splitCatalog separates the asking member's own part from the shared list.
func splitCatalog(gateways []Gateway, at time.Time) ([]Gateway, *catalogMember) {
	member := &catalogMember{cooldown: make(map[string]*CooldownRecommendation, len(gateways)),
		contacts: make(map[string][]ContactStats), at: at}
	shared := make([]Gateway, len(gateways))
	for i, gateway := range gateways {
		member.cooldown[gateway.Name] = gateway.Cooldown
		if len(gateway.Contacts) > 0 {
			member.contacts[gateway.Name] = gateway.Contacts
		}
		gateway.Cooldown, gateway.Contacts = nil, nil
		shared[i] = gateway
	}
	return shared, member
}

// serves reports whether this part may stand in for the member under list: it
// is younger than memberFreshness and, unless the list is empty, not empty.
// Gateways listed after the part was taken go without it until the member next
// asks for itself; asking again for every newly listed gateway would cost a
// request per member per listing, and the pool lists only live pairs.
func (m *catalogMember) serves(list *catalogList, now time.Time) bool {
	return m != nil && now.Before(m.at.Add(memberFreshness)) && (len(m.cooldown) > 0 || len(list.gateways) == 0)
}

// view returns a private copy of the shared list carrying this member's own
// part. Gateways the member's last own catalog did not list carry none.
func (m *catalogMember) view(gateways []Gateway, generation uint64) Catalog {
	out := Catalog{Gateways: make([]Gateway, len(gateways)), Generation: generation}
	for i, gateway := range gateways {
		if gateway.Priority != nil {
			priority := *gateway.Priority
			gateway.Priority = &priority
		}
		if m != nil {
			if cooldown, listed := m.cooldown[gateway.Name]; listed {
				gateway.MemberAt = m.at
				if cooldown != nil {
					value := *cooldown
					gateway.Cooldown = &value
				}
				gateway.Contacts = append([]ContactStats(nil), m.contacts[gateway.Name]...)
			}
		}
		out.Gateways[i] = gateway
	}
	return out
}

// CatalogSnapshot may be stale and is exclusively for presentation. Dispatch
// still uses Catalog/FreshCatalog and their unchanged freshness checks.
type CatalogSnapshot struct {
	Catalog
	ObservedAt time.Time
	ValidUntil time.Time
	Stale      bool
}

// PeekCatalogSnapshot shows the shared list of the model with this member's own
// part; it stays unknown until the list and the member each have a success.
func (c *Client) PeekCatalogSnapshot(account, tag, model string) (CatalogSnapshot, bool) {
	if c == nil {
		return CatalogSnapshot{}, false
	}
	c.catalog.mu.Lock()
	defer c.catalog.mu.Unlock()
	key := makeCatalogKey(account, tag, model)
	list, member := c.catalog.lists[key.model], c.catalog.members[key.member()]
	if list == nil || list.generation == 0 || member == nil {
		return CatalogSnapshot{}, false
	}
	return CatalogSnapshot{
		Catalog: member.view(list.gateways, list.generation), ObservedAt: list.snapshotAt, ValidUntil: list.snapshotUntil,
		Stale: len(list.flights) > 0 || list.displayUnknown || !c.catalog.timeNow().Before(list.until) ||
			!member.serves(list, c.catalog.timeNow()),
	}, true
}

type catalogFlight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	started time.Time
	waiters int
	value   Catalog
	err     error
}
type catalogCache struct {
	mu         sync.Mutex
	lists      map[string]*catalogList
	members    map[catalogMemberKey]*catalogMember
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
// A member whose own part serves the list reuses or joins any member's refresh
// of the shared list; otherwise it asks for itself.
func (c *Client) Catalog(ctx context.Context, account, tag, model string, after uint64) (Catalog, error) {
	if c == nil {
		return Catalog{}, fmt.Errorf("%w: client is nil", ErrPool)
	}
	key := makeCatalogKey(account, tag, model)
	member := key.member()
	cache := &c.catalog
	alone := false // set once another member's refresh was refused for that member
	for {
		if err := ctx.Err(); err != nil {
			return Catalog{}, err
		}
		cache.mu.Lock()
		now := cache.timeNow()
		list := cache.lists[key.model]
		if list == nil {
			if len(cache.lists) >= maxCatalogEntries && !cache.evictList() {
				cache.mu.Unlock()
				return Catalog{}, fmt.Errorf("%w: directory refresh capacity reached", ErrPool)
			}
			if cache.lists == nil {
				cache.lists = make(map[string]*catalogList)
			}
			list = &catalogList{}
			cache.lists[key.model] = list
		}
		list.used = now
		own := cache.members[member]
		current := own.serves(list, now)
		if current {
			own.used = now
			if now.Before(list.until) && list.generation > after {
				value := own.view(list.gateways, list.generation)
				cache.mu.Unlock()
				return value, nil
			}
		}
		flight := list.flights[member]
		if flight == nil && current && !alone {
			for _, other := range list.flights {
				// Only a refresh that started after the current list can renew it.
				if !other.started.Before(list.snapshotAt) {
					flight = other
					break
				}
			}
		}
		if flight == nil {
			workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.http.Timeout)
			flight = &catalogFlight{done: make(chan struct{}), cancel: cancel, started: now}
			if list.flights == nil {
				list.flights = make(map[catalogMemberKey]*catalogFlight)
			}
			list.flights[member] = flight
			go c.refreshCatalog(workCtx, key, list, flight)
		}
		joined := list.flights[member] != flight
		flight.waiters++
		cache.mu.Unlock()
		select {
		case <-ctx.Done():
			cache.mu.Lock()
			flight.waiters--
			if flight.waiters == 0 {
				flight.cancel()
				for owner, candidate := range list.flights {
					if candidate == flight {
						delete(list.flights, owner)
						list.displayUnknown = true
					}
				}
			}
			cache.mu.Unlock()
			return Catalog{}, ctx.Err()
		case <-flight.done:
		}
		if err := ctx.Err(); err != nil {
			return Catalog{}, err
		}
		if joined && isMemberRefusal(flight.err) {
			alone = true
			continue // another member's refusal is not this member's answer
		}
		if flight.err != nil {
			return Catalog{}, flight.err
		}
		// A refresh overtaken by a later-started one hands out that newer list,
		// which can still be no newer than after.
		if flight.value.Generation <= after {
			continue
		}
		cache.mu.Lock()
		value := cache.members[member].view(flight.value.Gateways, flight.value.Generation)
		cache.mu.Unlock()
		return value, nil
	}
}

// CachedCatalog returns the model's shared list while it is fresh, with this
// member's own part if any, without a request, LRU touch or TTL extension.
func (c *Client) CachedCatalog(account, tag, model string) (Catalog, bool) {
	if c == nil {
		return Catalog{}, false
	}
	c.catalog.mu.Lock()
	defer c.catalog.mu.Unlock()
	key := makeCatalogKey(account, tag, model)
	list := c.catalog.lists[key.model]
	if list == nil || list.generation == 0 || !c.catalog.timeNow().Before(list.until) {
		return Catalog{}, false
	}
	return c.catalog.members[key.member()].view(list.gateways, list.generation), true
}

// FreshCatalog is for conclusive exhaustion checks. Concurrent checks of members
// whose own part serves the list share one in-flight refresh; sequential checks
// cannot reuse an older empty result.
func (c *Client) FreshCatalog(ctx context.Context, account, tag, model string) (Catalog, error) {
	if c == nil {
		return Catalog{}, fmt.Errorf("%w: client is nil", ErrPool)
	}
	key := makeCatalogKey(account, tag, model)
	c.catalog.mu.Lock()
	var after uint64
	if list := c.catalog.lists[key.model]; list != nil {
		after = list.generation
	}
	c.catalog.mu.Unlock()
	return c.Catalog(ctx, account, tag, model, after)
}

func (c *Client) refreshCatalog(ctx context.Context, key catalogKey, list *catalogList, flight *catalogFlight) {
	defer flight.cancel()
	gateways, err := c.fetchGateways(ctx, key.account, key.tag, key.model)
	cache := &c.catalog
	cache.mu.Lock()
	defer cache.mu.Unlock()
	// A canceled flight can finish after a replacement has already succeeded.
	// It must not publish old data or erase the replacement's in-flight state.
	member := key.member()
	current := list.flights[member] == flight
	if err == nil {
		err = ctx.Err()
	}
	// A refresh overtaken by a later-started one keeps the newer list.
	newest := !flight.started.Before(list.snapshotAt)
	if current {
		delete(list.flights, member)
		if err == nil {
			shared, own := splitCatalog(gateways, flight.started)
			if old := cache.members[member]; old == nil || !own.at.Before(old.at) {
				own.used = cache.timeNow()
				cache.storeMember(member, own)
			}
			if newest {
				cache.generation++
				list.gateways, list.generation, list.displayUnknown = shared, cache.generation, false
				// Age starts before the network call, never on cache access. Missing or
				// expired lifetimes make a response non-cacheable, not immortal.
				list.until = flight.started.Add(catalogFreshness)
				for _, gateway := range shared {
					if gateway.PairReady {
						list.until = minTime(list.until, gateway.AvailableUntil)
					}
				}
				list.snapshotAt, list.snapshotUntil = flight.started, list.until
			}
			flight.value = Catalog{Gateways: list.gateways, Generation: list.generation}
		} else if newest && !isMemberRefusal(err) {
			list.until = time.Time{} // one member's refusal says nothing about the list
		}
	}
	flight.err = err
	close(flight.done)
}

func (c *catalogCache) storeMember(key catalogMemberKey, member *catalogMember) {
	if c.members == nil {
		c.members = make(map[catalogMemberKey]*catalogMember)
	}
	if _, exists := c.members[key]; !exists && len(c.members) >= maxCatalogEntries {
		var oldest catalogMemberKey
		var oldestAt time.Time
		for candidateKey, candidate := range c.members {
			if oldestAt.IsZero() || candidate.used.Before(oldestAt) {
				oldest, oldestAt = candidateKey, candidate.used
			}
		}
		delete(c.members, oldest)
	}
	c.members[key] = member
}

// evictList drops the least recently used list without a refresh in flight.
func (c *catalogCache) evictList() bool {
	var oldest string
	var oldestAt time.Time
	found := false
	for model, list := range c.lists {
		if len(list.flights) == 0 && (!found || list.used.Before(oldestAt)) {
			oldest, oldestAt, found = model, list.used, true
		}
	}
	if found {
		delete(c.lists, oldest)
	}
	return found
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
