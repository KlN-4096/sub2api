package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The runtime view feeds the estimate its display deadlines and rest snapshot.
func gatewayPoolTestCooldownEstimate(store *openAICodexCookieStore, identity string, account *Account,
	history openAIGatewayHistory, now time.Time, peers ...Account,
) GatewayPoolCooldownEstimate {
	return gatewayPoolCooldownEstimate(account, store.gatewayPoolDisplayCooldownDeadlines(identity, account, history, now),
		store.gatewayPoolRestSnapshot(account, identity, peers), now)
}

func TestGatewayPoolCooldownEstimateIsUnclampedAndReadOnly(t *testing.T) {
	now := time.Now().UTC()
	account := gwpoolTestAccount(1)
	store := &openAICodexCookieStore{}
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 2
	history := openAIGatewayHistory{Seen: map[string]openAIGatewaySeen{
		"done": {At: now.Add(-2 * time.Hour)},
		"later": {At: now, Cooldown: &gatewayPoolCooldown{
			WindowSeconds: 7200, UpdatedAt: now, Until: now.Add(2 * time.Hour),
		}},
	}}
	estimate := gatewayPoolTestCooldownEstimate(store, gwpoolTestIdentity, account, history, now)
	require.Equal(t, 2, estimate.ResumeGateways)
	require.Equal(t, now.Add(2*time.Hour), estimate.EligibleAt, "must not clamp to the next 10-minute recheck")
	require.Equal(t, now, history.Seen["later"].Cooldown.UpdatedAt)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 3
	require.Equal(t, now.Add(2*time.Hour), gatewayPoolTestCooldownEstimate(store, gwpoolTestIdentity, account, history, now).EligibleAt)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 2
	var cleared gatewayPoolCooldown
	cleared.clearCooldown(now, 3600)
	history.Seen["later"] = openAIGatewaySeen{At: now, Cooldown: &cleared}
	require.Equal(t, now, gatewayPoolTestCooldownEstimate(store, gwpoolTestIdentity, account, history, now).EligibleAt)
}

func TestGatewayPoolCooldownEstimateRefreshesPrivateCopyOnly(t *testing.T) {
	now := time.Now().UTC()
	for _, mode := range []string{"recommendation-off", "recommendation-expired", "base-changed", "reset-generation"} {
		t.Run(mode, func(t *testing.T) {
			account := gwpoolTestAccount(1)
			account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
			account.Extra["openai_gwpool_use_recommended_cooldown"] = mode != "recommendation-off"
			at := now.Add(-10 * time.Minute)
			c := gatewayPoolCooldown{SourcesKnown: true, BaseSeconds: 3600, CycleAt: at,
				WindowSeconds: 14400, LegacyRecommendedSeconds: 14400, LegacyRecommendationSource: "pool",
				LegacyRecommendationUntil: now.Add(time.Hour), UpdatedAt: at, Until: at.Add(4 * time.Hour)}
			want := at.Add(time.Hour)
			if mode == "recommendation-expired" {
				c.LegacyRecommendationUntil = now.Add(-time.Second)
			}
			if mode == "base-changed" {
				account.Extra[openAIGatewayPoolGatewayWindowExtraKey] = 7200
				account.Extra["openai_gwpool_use_recommended_cooldown"] = false
				want = at.Add(2 * time.Hour)
			}
			history := openAIGatewayHistory{Seen: map[string]openAIGatewaySeen{"g": {At: at, Cooldown: &c}}}
			if mode == "reset-generation" {
				history.CooldownReset.LastAt = now
			}
			require.True(t, validGatewayPoolCooldown(&c, now, 3600))
			before := c.clone()
			store := &openAICodexCookieStore{poolCooldown: map[string]gatewayPoolCooldown{gatewayPoolLedgerKey(gwpoolTestIdentity, "g"): c}}
			estimate := gatewayPoolTestCooldownEstimate(store, gwpoolTestIdentity, account, history, now)
			require.Equal(t, want, estimate.EligibleAt)
			require.Equal(t, before, c, "persisted display snapshot remains unchanged")
			current, _ := store.cooldownEntry(gwpoolTestIdentity, "g")
			require.Equal(t, before, current, "no shared cooldown state is published by display")
			require.True(t, store.gatewayPoolCooldownResetAt(gwpoolTestIdentity).IsZero())
		})
	}
}

func TestGatewayPoolRuntimeProgressEstimatesCooldownOnlyOnRequest(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	touched := time.Now().UTC().Add(-10 * time.Minute)
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity), Seen: map[string]openAIGatewaySeen{"g": {At: touched}},
	}
	svc, _ := gatewayRuntimeService(account)
	admin := &adminServiceImpl{runtimeBlocker: svc} // the handler reaches the gateway through the admin service
	read := func(estimate bool) (*GatewayPoolCooldownEstimate, map[string]any) {
		progress, err := admin.GatewayPoolRuntimeProgress(context.Background(), []int64{1}, estimate)
		require.NoError(t, err)
		require.NotNil(t, progress[1].Runtime)
		raw, err := json.Marshal(progress[1].Runtime)
		require.NoError(t, err)
		var view map[string]any
		require.NoError(t, json.Unmarshal(raw, &view))
		delete(view, "observed_at")
		return progress[1].Runtime.CooldownEstimate, view
	}
	omitted, lean := read(false)
	require.Nil(t, omitted)
	require.NotContains(t, lean, "cooldown_estimate", "the one-second poll does not compute the estimate")
	estimate, full := read(true)
	require.NotNil(t, estimate)
	require.Equal(t, 1, estimate.ResumeGateways)
	base := time.Duration(gatewayPoolCooldownBase(account.gatewayPoolGatewayWindow())) * time.Second
	require.True(t, estimate.EligibleAt.Equal(touched.Add(base)), "got %v", estimate.EligibleAt)
	delete(full, "cooldown_estimate")
	require.Equal(t, lean, full, "the flag changes nothing but the estimate")
}
