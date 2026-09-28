package repository

import (
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/stretchr/testify/require"
)

// TestAppendTurnStateWhereCondition 钉住用量明细的 Turn-State 筛选。
//
// 前三项看的是上游新铸的 turn_state，后几项看的是本次出站带了什么——两列语义不同
// （带了 turn-state 的请求只有 8% 会拿到新铸值），筛错列会得到完全相反的结论。
func TestAppendTurnStateWhereCondition(t *testing.T) {
	cases := []struct {
		filter   string
		wantCond string
		wantArgs []any
	}{
		{"", "", nil},
		{"   ", "", nil},
		{"bogus", "", nil},
		{usagestats.TurnStateFilterMinted, "turn_state IS NOT NULL", nil},
		{usagestats.TurnStateFilterHealthy, "char_length(turn_state) = 292", nil},
		{usagestats.TurnStateFilterSuspect, "turn_state IS NOT NULL AND char_length(turn_state) <> 292", nil},
		{usagestats.TurnStateFilterSent, "turn_state_sent IS NOT NULL", nil},
		{usagestats.TurnStateFilterInjected, "turn_state_overridden IS TRUE", nil},
		{usagestats.TurnStateFilterAuto, "turn_state_source = $1", []any{"auto"}},
		{usagestats.TurnStateFilterAutoStale, "turn_state_source = $1", []any{"auto_stale"}},
		{usagestats.TurnStateFilterManual, "turn_state_source = $1", []any{"manual"}},
	}
	for _, tc := range cases {
		t.Run(strings.TrimSpace(tc.filter), func(t *testing.T) {
			conds, args := appendTurnStateWhereCondition(nil, nil, tc.filter)
			if tc.wantCond == "" {
				require.Empty(t, conds, "未识别的筛选值不得凭空加条件")
				require.Empty(t, args)
				return
			}
			require.Equal(t, []string{tc.wantCond}, conds)
			require.Equal(t, tc.wantArgs, args)
		})
	}
}

// TestAppendTurnStateWhereCondition_PlaceholderFollowsExistingArgs 钉住占位符编号
// 接在已有参数之后：ListWithFilters 是按 len(args)+1 编号的，写死 $1 会串参数。
func TestAppendTurnStateWhereCondition_PlaceholderFollowsExistingArgs(t *testing.T) {
	conds, args := appendTurnStateWhereCondition(
		[]string{"user_id = $1", "account_id = $2"},
		[]any{int64(7), int64(9)},
		usagestats.TurnStateFilterAuto,
	)
	require.Equal(t, "turn_state_source = $3", conds[len(conds)-1])
	require.Equal(t, []any{int64(7), int64(9), "auto"}, args)
}
