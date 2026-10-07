package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolRecommendationDefaultsOffAndPreservesLocalFloor(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		store := &openAICodexCookieStore{}
		store.noteGatewayPoolRecommendation("id", "g", &gwpool.CooldownRecommendation{Seconds: 14400, Samples: 6, Source: "pool"})
		require.True(t, store.beginGatewayPoolAttempt("id", "g", time.Hour, enabled))
		c, _ := store.cooldownEntry("id", "g")
		want := 3600
		if enabled {
			want = 14400
		}
		require.Equal(t, want, c.WindowSeconds)
		c.LocalFloorSeconds = 7200
		store.poolCooldown[gatewayPoolLedgerKey("id", "g")] = c
		store.noteGatewayPoolRecommendation("id", "g", nil)
		store.gatewayPoolUsedAt("id", "g", time.Hour, enabled)
		c, _ = store.cooldownEntry("id", "g")
		require.Equal(t, 7200, c.WindowSeconds, "withdrawal cannot erase a real local lower bound")
	}
}

func TestGatewayPoolRecommendationRefreshIsNotNewContactOrSample(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolUseRecommendationKey] = true
	svc, repo := gatewayRuntimeService(account)
	store := &svc.codexCookies
	rec := &gwpool.CooldownRecommendation{Seconds: 7200, Samples: 6, Source: "pool"}
	store.noteGatewayPoolRecommendation(gwpoolTestIdentity, "g", rec)
	require.True(t, store.beginGatewayPoolAttempt(gwpoolTestIdentity, "g", time.Hour, true))
	before, _ := store.cooldownEntry(gwpoolTestIdentity, "g")
	before.AttemptSeconds, before.ElapsedSeconds = 3600, 3605
	store.poolCooldown[gatewayPoolLedgerKey(gwpoolTestIdentity, "g")] = before
	history := openAIGatewayHistory{LedgerTag: gatewayPoolLedgerTag(gwpoolTestIdentity),
		Seen: map[string]openAIGatewaySeen{"g": {At: before.CycleAt, FullAt: before.CycleAt, Verdict: "full", Cooldown: &before}}}
	require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{openAIGatewayHistoryExtraKey: history}))
	store.noteGatewayPoolRecommendation(gwpoolTestIdentity, "g", nil)
	store.gatewayPoolUsedAt(gwpoolTestIdentity, "g", time.Hour, true)
	after, _ := store.cooldownEntry(gwpoolTestIdentity, "g")
	require.Equal(t, before.CycleAt.Add(time.Hour), after.Until)
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
	store.gatewayPoolUsedAt(gwpoolTestIdentity, "g", time.Hour, true)
	again, _ := store.cooldownEntry(gwpoolTestIdentity, "g")
	require.Equal(t, after.Until, again.Until, "polling must not restart cooldown")
}

func TestGatewayPoolLegacyRecommendationPeriodIsNotGuessed(t *testing.T) {
	now := time.Now().UTC()
	old := gatewayPoolCooldown{WindowSeconds: 14400, RecommendedSeconds: 14400,
		RecommendationSource: "pool", UpdatedAt: now, Until: now.Add(4 * time.Hour)}
	store := &openAICodexCookieStore{poolCooldown: map[string]gatewayPoolCooldown{gatewayPoolLedgerKey("id", "g"): old}}
	store.noteGatewayPoolRecommendation("id", "g", nil)
	store.gatewayPoolUsedAt("id", "g", time.Hour, false)
	current, _ := store.cooldownEntry("id", "g")
	require.Equal(t, old, current, "legacy mixed data has no safe recommendation-only subtraction")
}

func TestGatewayPoolRecommendationLateAckCannotUndoWithdrawal(t *testing.T) {
	store := &openAICodexCookieStore{}
	at := time.Now().UTC()
	rec := &gwpool.CooldownRecommendation{Seconds: 14400, Samples: 6, Source: "pool"}
	store.noteGatewayPoolRecommendationAt("id", "g", nil, at.Add(time.Second))
	store.noteGatewayPoolRecommendationAt("id", "g", rec, at)
	base, got := store.gatewayPoolInitialCooldown("id", "g", time.Hour, true)
	require.Equal(t, 3600, base)
	require.Nil(t, got)
}

func TestGatewayPoolSameRecommendationPersistsTheLatestTTL(t *testing.T) {
	store := &openAICodexCookieStore{}
	now := time.Now().UTC()
	rec := &gwpool.CooldownRecommendation{Seconds: 14400, Samples: 6, Source: "pool"}
	store.noteGatewayPoolRecommendationAt("id", "g", rec, now.Add(-20*time.Minute))
	require.True(t, store.beginGatewayPoolAttempt("id", "g", time.Hour, true))
	before, _ := store.cooldownEntry("id", "g")
	store.noteGatewayPoolRecommendationAt("id", "g", rec, now)
	store.gatewayPoolUsedAt("id", "g", time.Hour, true)
	after, _ := store.cooldownEntry("id", "g")
	require.Equal(t, now.Add(gatewayPoolRecommendationTTL), after.RecommendationUntil)
	require.Equal(t, before.Until, after.Until, "TTL refresh is not a new contact")
	require.True(t, after.changedAt().After(before.changedAt()))
}
