package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	estimate := store.gatewayPoolCooldownEstimate(gwpoolTestIdentity, account, history, now)
	require.Equal(t, 2, estimate.ResumeGateways)
	require.Equal(t, now.Add(2*time.Hour), estimate.EligibleAt, "must not clamp to the next 10-minute recheck")
	require.Equal(t, now, history.Seen["later"].Cooldown.UpdatedAt)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 3
	require.Equal(t, now.Add(2*time.Hour), store.gatewayPoolCooldownEstimate(gwpoolTestIdentity, account, history, now).EligibleAt)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 2
	var cleared gatewayPoolCooldown
	cleared.clearCooldown(now, 3600)
	history.Seen["later"] = openAIGatewaySeen{At: now, Cooldown: &cleared}
	require.Equal(t, now, store.gatewayPoolCooldownEstimate(gwpoolTestIdentity, account, history, now).EligibleAt)
}

func TestGatewayPoolCooldownEstimateRefreshesPrivateCopyOnly(t *testing.T) {
	now := time.Now().UTC()
	for _, mode := range []string{"recommendation-off", "recommendation-expired", "base-changed", "reset-generation"} {
		t.Run(mode, func(t *testing.T) {
			account := gwpoolTestAccount(1)
			account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
			account.Extra[openAIGatewayPoolUseRecommendationKey] = mode != "recommendation-off"
			at := now.Add(-10 * time.Minute)
			c := gatewayPoolCooldown{SourcesKnown: true, BaseSeconds: 3600, CycleAt: at,
				WindowSeconds: 14400, RecommendedSeconds: 14400, RecommendationSource: "pool",
				RecommendationUntil: now.Add(time.Hour), UpdatedAt: at, Until: at.Add(4 * time.Hour)}
			want := at.Add(time.Hour)
			if mode == "recommendation-expired" {
				c.RecommendationUntil = now.Add(-time.Second)
			}
			if mode == "base-changed" {
				account.Extra[openAIGatewayPoolGatewayWindowExtraKey] = 7200
				account.Extra[openAIGatewayPoolUseRecommendationKey] = false
				want = at.Add(2 * time.Hour)
			}
			history := openAIGatewayHistory{Seen: map[string]openAIGatewaySeen{"g": {At: at, Cooldown: &c}}}
			if mode == "reset-generation" {
				history.CooldownReset.LastAt = now
			}
			require.True(t, validGatewayPoolCooldown(&c, now, 3600))
			before := c.clone()
			store := &openAICodexCookieStore{poolCooldown: map[string]gatewayPoolCooldown{gatewayPoolLedgerKey(gwpoolTestIdentity, "g"): c}}
			estimate := store.gatewayPoolCooldownEstimate(gwpoolTestIdentity, account, history, now)
			require.Equal(t, want, estimate.EligibleAt)
			require.Equal(t, before, c, "persisted display snapshot remains unchanged")
			current, _ := store.cooldownEntry(gwpoolTestIdentity, "g")
			require.Equal(t, before, current, "no shared cooldown state is published by display")
			require.True(t, store.gatewayPoolCooldownResetAt(gwpoolTestIdentity).IsZero())
		})
	}
}
