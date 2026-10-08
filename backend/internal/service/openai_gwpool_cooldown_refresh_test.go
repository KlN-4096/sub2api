package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolLocalCooldownIgnoresLegacyOverlayAndPreservesLocalFloor(t *testing.T) {
	now := time.Now().UTC()
	for _, known := range []bool{false, true} {
		c := gatewayPoolCooldown{
			SourcesKnown: known, BaseSeconds: 3600, CycleAt: now, WindowSeconds: 14400,
			LegacyRecommendedSeconds: 14400, LegacyRecommendationSource: "pool",
			UpdatedAt: now, Until: now.Add(4 * time.Hour),
		}
		want := 3600
		if known {
			c.LocalFloorSeconds, want = 7200, 7200
		}
		store := &openAICodexCookieStore{poolCooldown: map[string]gatewayPoolCooldown{gatewayPoolLedgerKey("id", "g"): c}}
		store.gatewayPoolUsedAt("id", "g", time.Hour)
		current, _ := store.cooldownEntry("id", "g")
		require.Equal(t, want, current.WindowSeconds)
		require.Equal(t, now.Add(time.Duration(want)*time.Second), current.Until)
		require.Equal(t, c.UpdatedAt, current.UpdatedAt, "removing a pool overlay is not a local observation")
		require.False(t, current.hasLegacyRecommendation())
	}
}

func TestGatewayPoolCooldownRefreshIsNotNewContactOrSample(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, repo := gatewayRuntimeService(account)
	store := &svc.codexCookies
	require.True(t, store.beginGatewayPoolAttempt(gwpoolTestIdentity, "g", time.Hour))
	before, _ := store.cooldownEntry(gwpoolTestIdentity, "g")
	before.AttemptSeconds, before.ElapsedSeconds = 3600, 3605
	before.LocalFloorSeconds, before.WindowSeconds = 7200, 14400
	before.LegacyRecommendedSeconds, before.LegacyRecommendationSource = 14400, "pool"
	before.Until = before.CycleAt.Add(4 * time.Hour)
	store.poolCooldown[gatewayPoolLedgerKey(gwpoolTestIdentity, "g")] = before
	history := openAIGatewayHistory{LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity),
		Seen: map[string]openAIGatewaySeen{"g": {At: before.CycleAt, FullAt: before.CycleAt, Verdict: "full", Cooldown: &before}}}
	require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{openAIGatewayHistoryExtraKey: history}))
	store.gatewayPoolUsedAt(gwpoolTestIdentity, "g", time.Hour)
	after, _ := store.cooldownEntry(gwpoolTestIdentity, "g")
	require.Equal(t, before.CycleAt.Add(2*time.Hour), after.Until)
	require.Equal(t, before.UpdatedAt, after.UpdatedAt)
	require.Equal(t, before.AttemptSeconds, after.AttemptSeconds)
	require.Equal(t, before.ElapsedSeconds, after.ElapsedSeconds)
	require.True(t, validGatewayPoolCooldown(&after, time.Now(), 3600))
	svc.persistGatewayPoolCooldownRefresh(context.Background(), account, gwpoolTestIdentity)
	fresh, err := repo.GetByID(context.Background(), 1)
	require.NoError(t, err)
	saved, ok := readOpenAIGatewayHistory(fresh)
	require.True(t, ok)
	require.Equal(t, history.Seen["g"].At, saved.Seen["g"].At)
	require.Equal(t, history.Seen["g"].FullAt, saved.Seen["g"].FullAt)
	require.Equal(t, "full", saved.Seen["g"].Verdict)
	require.Equal(t, after.Until, saved.Seen["g"].Cooldown.Until)
	store.gatewayPoolUsedAt(gwpoolTestIdentity, "g", time.Hour)
	again, _ := store.cooldownEntry(gwpoolTestIdentity, "g")
	require.Equal(t, after.Until, again.Until, "polling must not restart cooldown")
}

func TestGatewayPoolLateHistoryCannotRestoreRemovedPoolOverlay(t *testing.T) {
	store := &openAICodexCookieStore{}
	at := time.Now().UTC().Add(-20 * time.Minute)
	old := gatewayPoolCooldown{
		SourcesKnown: true, BaseSeconds: 3600, CycleAt: at, WindowSeconds: 14400,
		LegacyRecommendedSeconds: 14400, LegacyRecommendationSource: "pool",
		UpdatedAt: at, Until: at.Add(4 * time.Hour),
	}
	store.hydrateCooldown("id", "g", &old, 3600, at)
	after, _ := store.cooldownEntry("id", "g")
	require.Equal(t, at.Add(time.Hour), after.Until)
	require.False(t, after.hasLegacyRecommendation())
	require.True(t, after.changedAt().After(old.changedAt()))
	store.hydrateCooldown("id", "g", &old, 3600, at)
	again, _ := store.cooldownEntry("id", "g")
	require.Equal(t, after, again, "late history cannot regain precedence by being migrated again")
}
