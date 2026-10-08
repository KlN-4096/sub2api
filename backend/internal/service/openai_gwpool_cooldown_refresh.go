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
	if !previous.SourcesKnown && !previous.hasLegacyRecommendation() &&
		previous.WindowSeconds > base && previous.Outcome != openAIGatewayVerdictFull {
		c.LocalFloorSeconds = previous.WindowSeconds
	}
	if previous.Outcome == openAIGatewayVerdictFull {
		c.LocalFloorSeconds = 0
	}
}

func (c gatewayPoolCooldown) hasLegacyRecommendation() bool {
	return c.LegacyRecommendedSeconds != 0 || c.LegacyRecommendationSource != "" || !c.LegacyRecommendationUntil.IsZero()
}

// Called under poolCooldownMu. Only local base, learned backoff and fixed tiers
// control scheduling. Recompute from the original cycle/touch, never now+window.
func (s *openAICodexCookieStore) refreshGatewayPoolCooldown(c *gatewayPoolCooldown,
	identity string, window time.Duration, touched, now time.Time,
) {
	c.clearCooldown(s.gatewayPoolCooldownClearAt(identity), gatewayPoolCooldownBase(window))
	c.resetBackoff(s.gatewayPoolCooldownResetAt(identity), touched, gatewayPoolCooldownBase(window))
	if c.Cleared || (c.WindowSeconds == 0 && c.UpdatedAt.IsZero()) {
		return
	}
	beforeWindow, beforeUntil, beforeBase := c.WindowSeconds, c.Until, c.BaseSeconds
	legacy := c.hasLegacyRecommendation()
	previousChange := c.changedAt()
	c.BaseSeconds = gatewayPoolCooldownBase(window)
	if !c.SourcesKnown {
		c.SourcesKnown = true
		c.CycleAt = c.UpdatedAt
		if !legacy && c.Outcome != openAIGatewayVerdictFull {
			c.LocalFloorSeconds = max(c.LocalFloorSeconds, c.WindowSeconds)
		} else if legacy {
			// A combined legacy overlay cannot prove a local failure floor.
			// Keep the old record as history, not as authority over local policy.
			c.LocalFloorSeconds = 0
		}
	}
	c.LegacyRecommendedSeconds, c.LegacyRecommendationSource, c.LegacyRecommendationUntil = 0, "", time.Time{}
	c.WindowSeconds = max(c.BaseSeconds, c.LocalFloorSeconds)
	if c.FixedSeconds > 0 {
		c.WindowSeconds = c.FixedSeconds
	}
	if touched.After(c.CycleAt) {
		c.CycleAt = touched
	}
	c.Until = c.CycleAt.Add(time.Duration(c.WindowSeconds) * time.Second)
	if c.WindowSeconds != beforeWindow || !c.Until.Equal(beforeUntil) || c.BaseSeconds != beforeBase || legacy {
		c.ScheduleUpdatedAt = now
		if !now.After(previousChange) {
			c.ScheduleUpdatedAt = previousChange.Add(time.Nanosecond)
		}
	}
}

// Persist only cooldown metadata; preserving Seen.At/FullAt/verdict is crucial
// because changing the local schedule is not a new contact or quality sample.
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
