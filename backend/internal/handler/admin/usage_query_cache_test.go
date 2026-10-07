package admin

import (
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/stretchr/testify/require"
)

func TestUsageStatsCacheKey_StableAndDistinct(t *testing.T) {
	start := time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC)
	base := usagestats.UsageLogFilters{StartTime: &start, EndTime: &end, Model: "claude-3"}

	k1 := usageStatsCacheKey(base)
	k2 := usageStatsCacheKey(base)
	require.NotEmpty(t, k1)
	require.Equal(t, k1, k2, "same filters must produce same key")

	other := base
	other.Model = "gpt-4o"
	require.NotEqual(t, k1, usageStatsCacheKey(other), "different model must change key")

	withUser := base
	withUser.UserID = 7
	require.NotEqual(t, k1, usageStatsCacheKey(withUser), "different user must change key")

	// 漏掉任何一个进 WHERE 的筛选维度，30s 内切换该筛选就会拿到上一次的统计。
	withTurnState := base
	withTurnState.TurnState = usagestats.TurnStateFilterHealthy
	require.NotEqual(t, k1, usageStatsCacheKey(withTurnState), "different turn_state must change key")

	otherTurnState := base
	otherTurnState.TurnState = usagestats.TurnStateFilterSuspect
	require.NotEqual(t, usageStatsCacheKey(withTurnState), usageStatsCacheKey(otherTurnState),
		"两个不同的 turn-state 取值不得共用缓存")
}
