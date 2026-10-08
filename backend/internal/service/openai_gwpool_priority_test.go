package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolFreshSelectionRespectsLatestCatalogAndLocalRules(t *testing.T) {
	for _, mode := range []string{"priority", "US", "list-failure", "cooling"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if mode == "list-failure" {
					w.WriteHeader(503)
					return
				}
				_, _ = w.Write([]byte(`{"gateways":[
					{"name":"low","pair_ready":true,"datacenter_country":"US","priority":{"model":"luna","full":1,"samples":10}},
					{"name":"new","pair_ready":true,"datacenter_country":"JP","priority":{"model":"luna","full":9,"samples":10}},
					{"name":"not-ready","pair_ready":false,"priority":{"model":"luna","full":10,"samples":10}}]}`))
			}))
			defer server.Close()
			account := gwpoolTestAccount(1)
			account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
			account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "offline"
			svc, repo := gatewayRuntimeService(account)
			if mode == "US" {
				require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{
					openAIGatewayPoolContactsExtraKey: gatewayPoolContacts{
						LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity), LastUSAt: time.Now(),
					},
				}))
			}
			if mode == "cooling" {
				svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "new")
			}
			ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "luna")
			pool, err := svc.codexCookies.poolClient(account)
			require.NoError(t, err)
			picked := svc.codexCookies.gatewayPoolPick(ctx, pool, account, gwpoolTestIdentity)
			switch mode {
			case "list-failure":
				require.Empty(t, picked, "listing failure permits bare acquisition, not invented exhaustion")
			case "cooling":
				require.Equal(t, "low", picked, "pool priority cannot bypass local cooldown")
			default:
				require.Equal(t, "new", picked)
			}
			require.False(t, svc.codexCookies.gatewayPoolUsedRecently(gwpoolTestIdentity, "low", time.Hour), "listing alone is not a touch")
		})
	}
}

func TestGatewayPoolGlobalPriorityAndUSSoftBackoff(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "luna")
	candidates := []gwpool.Gateway{
		{Name: "low", DatacenterCountry: "JP", Priority: &gwpool.GatewayPriority{Model: "luna", Full: 1, Samples: 10}},
		{Name: "us-high", DatacenterCountry: "US", Priority: &gwpool.GatewayPriority{Model: "luna", Full: 9, Samples: 10}},
		{Name: "unknown"},
	}
	require.Equal(t, "us-high", svc.codexCookies.gatewayPoolRankContacts(ctx, account, gwpoolTestIdentity, candidates)[0].Name)
	now := time.Now().UTC()
	require.NoError(t, repo.UpdateExtra(ctx, 1, map[string]any{openAIGatewayPoolContactsExtraKey: gatewayPoolContacts{
		LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity), LastUSAt: now,
	}}))
	for range 6 {
		ranked := svc.codexCookies.gatewayPoolRankContacts(ctx, account, gwpoolTestIdentity, candidates)
		require.Equal(t, "us-high", ranked[2].Name, "US remains fallback even on exploration picks")
	}
	onlyUS := candidates[1:2]
	require.Equal(t, "us-high", svc.codexCookies.gatewayPoolRankContacts(ctx, account, gwpoolTestIdentity, onlyUS)[0].Name)
	require.Equal(t, "low", candidates[0].Name, "input listing must remain immutable")
	require.False(t, gatewayPoolUSBackoffActive(now.Add(-4*time.Hour), now))
	require.True(t, gatewayPoolUSBackoffActive(now.Add(-4*time.Hour+time.Nanosecond), now))
	require.False(t, gatewayPoolUSBackoffActive(now.Add(time.Hour), now))
}

func TestGatewayPoolUSBackoffOnlyFromSentContactsAndSurvivesReload(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	event := contactEvent(time.Now().UTC().Add(-time.Second), "us")
	event.Applied.DatacenterCountry = "US"
	event.FirstSent = time.Time{}
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ := repo.GetByID(context.Background(), 1)
	require.Zero(t, readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity)).LastUSAt)
	event.FirstSent = event.LastSent.Add(-time.Second)
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ = repo.GetByID(context.Background(), 1)
	reloaded := readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Equal(t, event.LastSent, reloaded.LastUSAt)
	other := readGatewayPoolContacts(current, "different-identity")
	require.Zero(t, other.LastUSAt)
	merged := gatewayPoolContacts{LedgerTag: reloaded.LedgerTag, Seen: map[string]gatewayPoolContactSeen{}}
	mergeGatewayPoolContacts(&merged, reloaded)
	require.Equal(t, reloaded.LastUSAt, merged.LastUSAt)
}

func TestGatewayPoolUSBackoffDoesNotGuessDriftCountry(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	event := contactEvent(time.Now().UTC().Add(-time.Second), "drift")
	event.Applied.DatacenterCountry = "US"
	event.Steps = []gatewayPoolProbeStep{{Sent: true, Status: 200, ActualGateway: "unknown-location"}}
	svc.noteGatewayPoolContact(context.Background(), account, event)
	current, _ := repo.GetByID(context.Background(), 1)
	require.Zero(t, readGatewayPoolContacts(current, gatewayPoolLedgerTag(gwpoolTestIdentity)).LastUSAt)
}
