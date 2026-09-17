//go:build unit

package repository

import (
	"context"
	"database/sql"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

var (
	usageLogStaticInsertShapeRe = regexp.MustCompile(`(?s)INSERT INTO usage_logs \((.*?)\) VALUES \((.*?)\)`)
	usageLogPlaceholderRe       = regexp.MustCompile(`\$(\d+)`)
)

// newSQLCapturingMock 返回把实际下发 SQL 记录到 captured 的 sqlmock；语句一律视为匹配，
// 参数仍由 WithArgs 校验。
func newSQLCapturingMock(t *testing.T, captured *[]string) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	matcher := sqlmock.QueryMatcherFunc(func(_, actualSQL string) error {
		*captured = append(*captured, actualSQL)
		return nil
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

// requireStaticInsertMatchesArgTypes 断言手写的 INSERT：列清单长度与 VALUES 占位符数量
// 都等于 usageLogInsertArgTypes，且占位符恰为 $1..$N 各出现一次。
func requireStaticInsertMatchesArgTypes(t *testing.T, query string) {
	t.Helper()
	m := usageLogStaticInsertShapeRe.FindStringSubmatch(query)
	require.Len(t, m, 3, "unrecognised INSERT shape:\n%s", query)

	want := len(usageLogInsertArgTypes)
	columns := 0
	for _, col := range strings.Split(m[1], ",") {
		if strings.TrimSpace(col) != "" {
			columns++
		}
	}
	require.Equal(t, want, columns, "INSERT column list must match usageLogInsertArgTypes")

	seen := make(map[int]struct{}, want)
	for _, ph := range usageLogPlaceholderRe.FindAllStringSubmatch(m[2], -1) {
		n, err := strconv.Atoi(ph[1])
		require.NoError(t, err)
		_, dup := seen[n]
		require.False(t, dup, "duplicate placeholder $%d", n)
		seen[n] = struct{}{}
	}
	require.Len(t, seen, want, "VALUES placeholder count must match usageLogInsertArgTypes")
	for i := 1; i <= want; i++ {
		_, ok := seen[i]
		require.True(t, ok, "missing placeholder $%d", i)
	}
}

// TestUsageLogStaticInsertShape_PlaceholdersMatchArgTypes 覆盖两条不经占位符生成器、
// 直接手写 $1..$N 的 INSERT 路径，防止加列后漏补占位符只在集成测试才暴露。
func TestUsageLogStaticInsertShape_PlaceholdersMatchArgTypes(t *testing.T) {
	upstreamRequestID := "20260902080329-oneapi"
	log := &service.UsageLog{
		UserID:            1,
		APIKeyID:          2,
		AccountID:         3,
		RequestID:         "client:insert-shape",
		UpstreamRequestID: &upstreamRequestID,
		Model:             "claude-3",
		InputTokens:       10,
		CreatedAt:         time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
	}
	prepared := prepareUsageLogInsert(log)
	args := anySliceToDriverValues(prepared.args)

	t.Run("createSingle", func(t *testing.T) {
		var captured []string
		db, mock := newSQLCapturingMock(t, &captured)
		repo := &usageLogRepository{sql: db}

		mock.ExpectQuery("INSERT INTO usage_logs").
			WithArgs(args...).
			WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(int64(1), log.CreatedAt))

		inserted, err := repo.Create(context.Background(), log)
		require.NoError(t, err)
		require.True(t, inserted)
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, captured, 1)
		requireStaticInsertMatchesArgTypes(t, captured[0])
	})

	t.Run("execUsageLogInsertNoResult", func(t *testing.T) {
		var captured []string
		db, mock := newSQLCapturingMock(t, &captured)

		mock.ExpectExec("INSERT INTO usage_logs").
			WithArgs(args...).
			WillReturnResult(sqlmock.NewResult(0, 1))

		require.NoError(t, execUsageLogInsertNoResult(context.Background(), db, prepared))
		require.NoError(t, mock.ExpectationsWereMet())
		require.Len(t, captured, 1)
		requireStaticInsertMatchesArgTypes(t, captured[0])
	})
}

// TestPrepareUsageLogInsert_UpstreamRequestIDArgWiring 把 upstream_request_id 钉在
// session_id 之前，与参数类型表保持同位；缺失时落 NULL 而不是空串。
func TestPrepareUsageLogInsert_UpstreamRequestIDArgWiring(t *testing.T) {
	upstreamRequestID := "req_upstream_123"
	prepared := prepareUsageLogInsert(&service.UsageLog{
		UserID:            1,
		APIKeyID:          2,
		RequestID:         "client:wiring",
		Model:             "gpt-5",
		UpstreamRequestID: &upstreamRequestID,
		CreatedAt:         time.Now().UTC(),
	})
	require.Len(t, prepared.args, len(usageLogInsertArgTypes))

	// 尾部顺序：upstream_request_id, session_id, native_compaction_v2,
	// turn_state, turn_state_overridden, turn_state_source, created_at
	idx := len(prepared.args) - 7
	arg, ok := prepared.args[idx].(sql.NullString)
	require.True(t, ok, "upstream_request_id arg should be sql.NullString, got %T", prepared.args[idx])
	require.True(t, arg.Valid)
	require.Equal(t, upstreamRequestID, arg.String)
	require.Equal(t, "text", usageLogInsertArgTypes[idx])

	absent := prepareUsageLogInsert(&service.UsageLog{UserID: 1, APIKeyID: 2, RequestID: "client:absent", Model: "gpt-5", CreatedAt: time.Now().UTC()})
	nullArg, ok := absent.args[idx].(sql.NullString)
	require.True(t, ok)
	require.False(t, nullArg.Valid, "absent upstream request id must be NULL")

	require.Contains(t, usageLogSelectColumns, "upstream_request_id")
}

// TestPrepareUsageLogInsert_TurnStateArgWiring 把插入参数尾部五位全部钉死。
//
// 本文件顶部的契约要求新增 usage_logs 列时同步更新 4 处清单；turn_state /
// turn_state_overridden / turn_state_source 加进来后，既有断言只钉到
// native_compaction_v2，这三个新列与 created_at 之间互换位置所有单测都照过
// （生产会在 lib/pq 那里炸——响亮失败，但正是这条契约该拦住的一类）。
func TestPrepareUsageLogInsert_TurnStateArgWiring(t *testing.T) {
	turnState := "gAAAAAB-turn-state-blob"
	overridden := true
	source := "auto_stale"
	prepared := prepareUsageLogInsert(&service.UsageLog{
		UserID:              1,
		APIKeyID:            2,
		RequestID:           "client:turn-state-wiring",
		Model:               "gpt-5",
		TurnState:           &turnState,
		TurnStateOverridden: &overridden,
		TurnStateSource:     &source,
		CreatedAt:           time.Now().UTC(),
	})
	require.Len(t, prepared.args, len(usageLogInsertArgTypes))

	n := len(prepared.args)
	// 尾部顺序：... native_compaction_v2, turn_state, turn_state_overridden,
	// turn_state_source, created_at
	require.Equal(t, "boolean", usageLogInsertArgTypes[n-5], "native_compaction_v2 必须仍在倒数第 5")

	tsArg, ok := prepared.args[n-4].(sql.NullString)
	require.True(t, ok, "turn_state 应是 sql.NullString，实际 %T", prepared.args[n-4])
	require.True(t, tsArg.Valid)
	require.Equal(t, turnState, tsArg.String)
	require.Equal(t, "text", usageLogInsertArgTypes[n-4])

	ovArg, ok := prepared.args[n-3].(sql.NullBool)
	require.True(t, ok, "turn_state_overridden 应是 sql.NullBool，实际 %T", prepared.args[n-3])
	require.True(t, ovArg.Valid)
	require.True(t, ovArg.Bool)
	require.Equal(t, "boolean", usageLogInsertArgTypes[n-3])

	srcArg, ok := prepared.args[n-2].(sql.NullString)
	require.True(t, ok, "turn_state_source 应是 sql.NullString，实际 %T", prepared.args[n-2])
	require.True(t, srcArg.Valid)
	require.Equal(t, source, srcArg.String)
	require.Equal(t, "text", usageLogInsertArgTypes[n-2])

	_, ok = prepared.args[n-1].(time.Time)
	require.True(t, ok, "created_at 必须仍在末位，实际 %T", prepared.args[n-1])
	require.Equal(t, "timestamptz", usageLogInsertArgTypes[n-1])

	// 三列都未提供时必须是 NULL，而不是空串 / false
	absent := prepareUsageLogInsert(&service.UsageLog{
		UserID: 1, APIKeyID: 2, RequestID: "client:turn-state-absent", Model: "gpt-5",
		CreatedAt: time.Now().UTC(),
	})
	nullTS, ok := absent.args[n-4].(sql.NullString)
	require.True(t, ok)
	require.False(t, nullTS.Valid, "没有 turn_state 时必须写 NULL")
	nullOV, ok := absent.args[n-3].(sql.NullBool)
	require.True(t, ok)
	require.False(t, nullOV.Valid, "不适用的账号类型必须写 NULL，而不是 false")
	nullSrc, ok := absent.args[n-2].(sql.NullString)
	require.True(t, ok)
	require.False(t, nullSrc.Valid, "没注入覆写时来源必须写 NULL")

	require.Contains(t, usageLogSelectColumns, "turn_state")
	require.Contains(t, usageLogSelectColumns, "turn_state_overridden")
	require.Contains(t, usageLogSelectColumns, "turn_state_source")
}
