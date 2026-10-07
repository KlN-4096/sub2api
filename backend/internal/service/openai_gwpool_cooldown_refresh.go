package service

import (
	"context"
	"log/slog"
	"time"
)

func (c gatewayPoolCooldown) changedAt() time.Time {
	if c.ScheduleUpdatedAt.After(c.UpdatedAt) {
		return c.ScheduleUpdatedAt
	}
	return c.UpdatedAt
}

func (c *gatewayPoolCooldown) initSources(previous gatewayPoolCooldown, base int, now time.Time) {
	c.Cleared = false
	c.SourcesKnown, c.BaseSeconds, c.CycleAt = true, base, now
	if !previous.SourcesKnown && previous.WindowSeconds > base && previous.Outcome != openAIGatewayVerdictFull {
		// The old combined record cannot tell failure backoff from a recommendation.
		// Preserve that unknown lower bound until a real success resets it.
		c.LocalFloorSeconds = previous.WindowSeconds
	}
	if previous.Outcome == openAIGatewayVerdictFull {
		c.LocalFloorSeconds = 0
	}
}

// Called under poolCooldownMu. Recompute from the original cycle/touch, never
// now+duration; refreshing a recommendation must not extend the resting period.
func (s *openAICodexCookieStore) refreshGatewayPoolCooldown(c *gatewayPoolCooldown,
	identity, gateway string, window time.Duration, touched, now time.Time, enabled ...bool,
) {
	c.clearCooldown(s.gatewayPoolCooldownClearAt(identity), gatewayPoolCooldownBase(window))
	c.resetBackoff(s.gatewayPoolCooldownResetAt(identity), touched, gatewayPoolCooldownBase(window))
	if c.Cleared || !c.SourcesKnown {
		return // conservative migration of old, irreversibly mixed records
	}
	beforeWindow, beforeUntil, beforeBase := c.WindowSeconds, c.Until, c.BaseSeconds
	beforeRec, beforeExpiry, beforeSource := c.RecommendedSeconds, c.RecommendationUntil, c.RecommendationSource
	previousChange := c.changedAt()
	c.BaseSeconds = gatewayPoolCooldownBase(window)
	use := len(enabled) != 0 && enabled[0]
	raw, loaded := s.poolRecommendations.Load(gatewayPoolLedgerKey(identity, gateway))
	if !use {
		c.RecommendedSeconds, c.RecommendationSource, c.RecommendationUntil = 0, "", time.Time{}
	} else if rec, ok := raw.(gatewayPoolRecommendation); loaded && ok {
		if !rec.Valid() || now.Sub(rec.at) > gatewayPoolRecommendationTTL || !rec.at.After(c.ResetAt) {
			c.RecommendedSeconds, c.RecommendationSource, c.RecommendationUntil = 0, "", time.Time{}
		} else {
			c.RecommendedSeconds, c.RecommendationSource = rec.Seconds, rec.Source
			c.RecommendationUntil = rec.at.Add(gatewayPoolRecommendationTTL)
		}
	} else if !now.Before(c.RecommendationUntil) {
		c.RecommendedSeconds, c.RecommendationSource, c.RecommendationUntil = 0, "", time.Time{}
	}
	c.WindowSeconds = max(c.BaseSeconds, max(c.LocalFloorSeconds, c.RecommendedSeconds))
	if c.FixedSeconds > 0 {
		c.WindowSeconds = c.FixedSeconds
	}
	if touched.After(c.CycleAt) {
		c.CycleAt = touched
	}
	c.Until = c.CycleAt.Add(time.Duration(c.WindowSeconds) * time.Second)
	if c.WindowSeconds != beforeWindow || !c.Until.Equal(beforeUntil) || c.BaseSeconds != beforeBase ||
		c.RecommendedSeconds != beforeRec || c.RecommendationSource != beforeSource || !c.RecommendationUntil.Equal(beforeExpiry) {
		c.ScheduleUpdatedAt = now
		if !now.After(previousChange) {
			c.ScheduleUpdatedAt = previousChange.Add(time.Nanosecond)
		}
	}
}

// Persist only cooldown metadata; preserving Seen.At/FullAt/verdict is crucial
// because rewriting a recommendation is not a new contact or quality sample.
func (s *OpenAIGatewayService) persistGatewayPoolCooldownRefresh(ctx context.Context, account *Account, identity string) {
	if s.accountRepo == nil || account == nil {
		return
	}
	unlock := s.codexCookies.gatewayPoolHistoryLock(account.ID)
	defer unlock()
	fresh, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || fresh == nil {
		return
	}
	current, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if err != nil || current != identity {
		return
	}
	rec, ok := readOpenAIGatewayHistory(fresh)
	if !ok || rec.LedgerTag != gatewayPoolLedgerTag(identity) {
		return
	}
	changed := s.codexCookies.syncGatewayPoolCooldownResetHistory(&rec, identity,
		gatewayPoolCooldownBase(account.gatewayPoolGatewayWindow()))
	for gateway, seen := range rec.Seen {
		cooldown, exists := s.codexCookies.cooldownEntry(identity, gateway)
		if exists && cooldown.SourcesKnown && newerGatewayPoolCooldown(&cooldown, seen.Cooldown) {
			copy := cooldown.clone()
			seen.Cooldown = &copy
			rec.Seen[gateway], changed = seen, true
		}
	}
	if changed {
		if err := s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{openAIGatewayHistoryExtraKey: rec}); err != nil {
			slog.Warn("gwpool_cooldown_refresh_persist_failed", "account_id", account.ID)
		}
	}
}
