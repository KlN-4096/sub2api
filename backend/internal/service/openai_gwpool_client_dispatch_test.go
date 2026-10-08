package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolClientDispatchNeverFallsBackToBare(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.listStatus = http.StatusServiceUnavailable
	store := &openAICodexCookieStore{}
	err := attachRoute(context.Background(), store, fake.account(1), gwpoolTestURL, http.Header{})
	require.Error(t, err)
	require.Zero(t, fake.hits.Load(), "unreadable directory must not delegate selection to the pool")
}

func TestGatewayPoolClientDispatchDirectoryTimeoutIsUnknownAndRetryable(t *testing.T) {
	var lists atomic.Int32
	cookie := gwpoolTestPairCookie(t, "g")
	account := dispatchTestAccount(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gateways" {
			if lists.Add(1) == 1 {
				<-r.Context().Done()
				return
			}
			writeDispatchCatalog(w, "g")
			return
		}
		writeDispatchPair(w, "g", cookie, "v")
	})
	account.Extra[openAIGatewayPoolListTimeoutExtraKey] = 1
	svc, _ := gatewayRuntimeService(account)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = svc.gatewayPoolWaitContext(ctx, account)
	sleeps := 0
	gatewayPoolWaitFrom(ctx).sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
	_, err := svc.attachGatewayPoolRouteWithWait(ctx, account, gwpoolTestURL, http.Header{})
	require.NoError(t, err, "directory timeout must not terminate a still-live request")
	require.Equal(t, 1, sleeps)
	require.EqualValues(t, 2, lists.Load())
}

func TestGatewayPoolClientDispatchMalformedCatalogIsNotExhaustion(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"gateways":null}`, `{"gateways":[null]}`, `{"gateways":[{}]}`} {
		t.Run(body, func(t *testing.T) {
			account := dispatchTestAccount(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })
			svc, _ := gatewayRuntimeService(account)
			require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account))
		})
	}
}

func dispatchTestAccount(t *testing.T, handler http.HandlerFunc) *Account {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "offline"
	return account
}

func writeDispatchCatalog(w http.ResponseWriter, names ...string) {
	rows := make([]map[string]any, 0, len(names))
	for _, name := range names {
		rows = append(rows, map[string]any{"name": name, "pair_ready": true, "valid_for_s": 3600})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"gateways": rows})
}

func writeDispatchPair(w http.ResponseWriter, gateway, cookie, version string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"gateway": gateway, "cookie": cookie, "cookie_version": version, "valid_for_s": 3600,
	})
}

func TestGatewayPoolClientDispatchRefreshesEmptyAndMissing(t *testing.T) {
	for _, first := range []string{"empty", "missing"} {
		t.Run(first, func(t *testing.T) {
			var lists, tickets atomic.Int64
			cookie := gwpoolTestPairCookie(t, "new")
			account := dispatchTestAccount(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/gateways":
					if lists.Add(1) == 1 {
						if first == "empty" {
							writeDispatchCatalog(w)
						} else {
							writeDispatchCatalog(w, "gone")
						}
					} else {
						writeDispatchCatalog(w, "new")
					}
				case "/cookie":
					tickets.Add(1)
					require.NotEmpty(t, r.URL.Query().Get("gateway"))
					require.False(t, r.URL.Query().Has("force"))
					require.False(t, r.URL.Query().Has("wait"))
					if r.URL.Query().Get("gateway") == "gone" {
						w.WriteHeader(503)
						fmt.Fprint(w, `{"error":{"code":"no_live_pair"}}`)
					} else {
						writeDispatchPair(w, "new", cookie, "new-version")
					}
				}
			})
			store := &openAICodexCookieStore{}
			pair, fresh, err := store.gatewayPoolPair(context.Background(), account, gwpoolTestIdentity)
			require.NoError(t, err)
			require.True(t, fresh)
			require.Equal(t, "new", pair.gateway)
			require.EqualValues(t, 2, lists.Load())
			wantTickets := int64(1)
			if first == "missing" {
				wantTickets = 2
			}
			require.Equal(t, wantTickets, tickets.Load())
		})
	}
}

func TestGatewayPoolClientDispatchRepeatedMissingIsBoundedNotExhausted(t *testing.T) {
	var lists, tickets atomic.Int64
	account := dispatchTestAccount(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gateways" {
			lists.Add(1)
			writeDispatchCatalog(w, "g")
			return
		}
		tickets.Add(1)
		require.Equal(t, "g", r.URL.Query().Get("gateway"))
		w.WriteHeader(503)
		fmt.Fprint(w, `{"error":{"code":"no_live_pair"}}`)
	})
	store := &openAICodexCookieStore{}
	_, _, err := store.gatewayPoolPair(context.Background(), account, gwpoolTestIdentity)
	var refused *gwpool.PoolError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, gwpool.CodeNoLivePair, refused.Code, "missing stock must not invent local exhaustion")
	require.EqualValues(t, 2, lists.Load())
	require.EqualValues(t, 2, tickets.Load())
}

func TestGatewayPoolClientDispatchLocallyExcludesStaleRoute(t *testing.T) {
	cookie := gwpoolTestPairCookie(t, "new")
	for _, version := range []string{"", "old-version"} {
		t.Run(version, func(t *testing.T) {
			account := dispatchTestAccount(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/gateways" {
					writeDispatchCatalog(w, "old", "new")
					return
				}
				require.Equal(t, "new", r.URL.Query().Get("gateway"))
				require.False(t, r.URL.Query().Has("force"))
				require.Equal(t, version, r.URL.Query().Get("exclude_versions"))
				writeDispatchPair(w, "new", cookie, "new-version")
			})
			store := &openAICodexCookieStore{}
			store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
				cookie: "__cflb=old; __oailb=old", gateway: "old", version: version, invalidated: true,
			})
			for i := range gwpool.MaxExcludeItems + 5 {
				store.gatewayPoolMarkUsed(gwpoolTestIdentity, fmt.Sprintf("cooling-%d", i))
			}
			pair, _, err := store.gatewayPoolPair(context.Background(), account, gwpoolTestIdentity)
			require.NoError(t, err)
			require.Equal(t, "new", pair.gateway, "old gateway exclusion cannot depend on version presence or the 64-item wire limit")
		})
	}
}

func TestGatewayPoolClientDispatchRejectsMismatchedRouteAndVersion(t *testing.T) {
	cookie := gwpoolTestPairCookie(t, "new")
	for _, wrong := range []string{"gateway", "version"} {
		t.Run(wrong, func(t *testing.T) {
			account := dispatchTestAccount(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/gateways" {
					writeDispatchCatalog(w, "new")
					return
				}
				gateway, version := "new", "new-version"
				if wrong == "gateway" {
					gateway = "unexpected"
				} else {
					version = "old-version"
				}
				writeDispatchPair(w, gateway, cookie, version)
			})
			store := &openAICodexCookieStore{}
			store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
				cookie: "__cflb=old; __oailb=old", gateway: "old", version: "old-version", invalidated: true,
			})
			_, _, err := store.gatewayPoolPair(context.Background(), account, gwpoolTestIdentity)
			require.ErrorIs(t, err, gwpool.ErrPool)
			current, state := store.cachedPoolPair(gwpoolTestIdentity)
			require.Equal(t, openAIGatewayPoolPairStale, state)
			require.Equal(t, "old-version", current.version)
		})
	}
}

func TestGatewayPoolClientDispatchConsumerLimitSharesBackoff(t *testing.T) {
	var tickets atomic.Int64
	account := dispatchTestAccount(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gateways" {
			writeDispatchCatalog(w, "g")
			return
		}
		tickets.Add(1)
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(429)
		fmt.Fprint(w, `{"error":{"code":"consumer_rate_limited","retry_after_seconds":7}}`)
	})
	store := &openAICodexCookieStore{}
	for range 2 {
		_, _, err := store.gatewayPoolPair(context.Background(), account, gwpoolTestIdentity)
		var refused *gwpool.PoolError
		require.ErrorAs(t, err, &refused)
		require.Equal(t, gwpool.CodeConsumerRateLimited, refused.Code)
		require.Greater(t, refused.RetryAfter, 6*time.Second)
		require.Contains(t, gatewayPoolClientMessage(err), "频率")
		require.False(t, gatewayPoolRotationFailure(err))
	}
	require.EqualValues(t, 1, tickets.Load(), "shared member backoff must stop subsequent pool traffic")
	store.applyGatewayPoolCooldownClear(gwpoolTestIdentity, time.Now(), 3600)
	left, code := store.gatewayPoolBackoffFor(gwpoolTestIdentity)
	require.Positive(t, left)
	require.Equal(t, gwpool.CodeConsumerRateLimited, code, "clearing local CD cannot clear a server consumer limit")
}

func TestGatewayPoolClientDispatchWaitHonorsConsumerRetryAfter(t *testing.T) {
	var limited atomic.Bool
	limited.Store(true)
	var tickets, lists atomic.Int64
	cookie := gwpoolTestPairCookie(t, "g")
	account := dispatchTestAccount(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gateways" {
			lists.Add(1)
			writeDispatchCatalog(w, "g")
			return
		}
		tickets.Add(1)
		if limited.Load() {
			w.WriteHeader(429)
			fmt.Fprint(w, `{"error":{"code":"consumer_rate_limited","retry_after_seconds":7}}`)
			return
		}
		writeDispatchPair(w, "g", cookie, "version")
	})
	svc, _ := gatewayRuntimeService(account)
	ctx := svc.gatewayPoolWaitContext(context.Background(), account)
	state := gatewayPoolWaitFrom(ctx)
	require.NotNil(t, state)
	sleeps := 0
	state.sleep = func(ctx context.Context, gap time.Duration) error {
		sleeps++
		require.GreaterOrEqual(t, gap, 6*time.Second)
		require.LessOrEqual(t, gap, 7*time.Second)
		// Simulate expiry without spending seven wall-clock seconds.
		svc.codexCookies.poolBackoff.Delete(gwpoolTestIdentity)
		limited.Store(false)
		return nil
	}
	_, err := svc.attachGatewayPoolRouteWithWait(ctx, account, gwpoolTestURL, http.Header{})
	require.NoError(t, err)
	require.Equal(t, 1, sleeps)
	require.EqualValues(t, 2, tickets.Load())
	require.EqualValues(t, 1, lists.Load(), "a consumer limit does not prove directory exhaustion")
}

func TestGatewayPoolClientDispatchExhaustionRefreshesAndFailureIsUnknown(t *testing.T) {
	var mode atomic.Int64
	var lists atomic.Int64
	account := dispatchTestAccount(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/gateways", r.URL.Path)
		lists.Add(1)
		switch mode.Load() {
		case 0:
			writeDispatchCatalog(w)
		case 1:
			writeDispatchCatalog(w, "new")
		default:
			w.WriteHeader(503)
		}
	})
	svc, _ := gatewayRuntimeService(account)
	pool, err := svc.codexCookies.poolClient(account)
	require.NoError(t, err)
	_, err = pool.Catalog(context.Background(), "acc-a/user-a", gatewayPoolAccountTag(account, gwpoolTestIdentity), "", 0)
	require.NoError(t, err)
	mode.Store(1)
	require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account), "cached empty cannot prove exhaustion")
	mode.Store(2)
	require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account), "refresh failure is unknown, never empty")
	require.EqualValues(t, 3, lists.Load())
}

func TestGatewayPoolClientDispatchPoolTouchHistoryCannotRank(t *testing.T) {
	store := &openAICodexCookieStore{}
	candidates := []gwpool.Gateway{
		{Name: "first", PairReady: true},
		{Name: "second", PairReady: true},
	}
	got := store.gatewayPoolPick(context.Background(), gwpoolTestAccount(1), gwpoolTestIdentity, candidates, nil)
	require.Equal(t, "first", got)
	require.False(t, strings.Contains(got, "\x00"))
}
