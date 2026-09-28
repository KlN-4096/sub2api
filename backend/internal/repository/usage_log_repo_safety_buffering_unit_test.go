//go:build unit

package repository

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// TestPrepareUsageLogInsert_SafetyBufferingArgWiring 把 safety_buffering_enabled /
// safety_buffering_faster_model 钉在 created_at 之前的倒数第 3、第 2 位，与本文件顶部的
// 四处清单契约同步（usage_log_repo_insert_shape_unit_test.go）。
func TestPrepareUsageLogInsert_SafetyBufferingArgWiring(t *testing.T) {
	enabled := true
	faster := "gpt-5.6-luna"
	prepared := prepareUsageLogInsert(&service.UsageLog{
		UserID:                     1,
		APIKeyID:                   2,
		RequestID:                  "client:safety-buffering-wiring",
		Model:                      "gpt-6-astra",
		SafetyBufferingEnabled:     &enabled,
		SafetyBufferingFasterModel: &faster,
		CreatedAt:                  time.Now().UTC(),
	})
	require.Len(t, prepared.args, len(usageLogInsertArgTypes))

	n := len(prepared.args)
	enabledArg, ok := prepared.args[n-3].(sql.NullBool)
	require.True(t, ok, "safety_buffering_enabled 应是 sql.NullBool，实际 %T", prepared.args[n-3])
	require.True(t, enabledArg.Valid)
	require.True(t, enabledArg.Bool)
	require.Equal(t, "boolean", usageLogInsertArgTypes[n-3])

	fasterArg, ok := prepared.args[n-2].(sql.NullString)
	require.True(t, ok, "safety_buffering_faster_model 应是 sql.NullString，实际 %T", prepared.args[n-2])
	require.True(t, fasterArg.Valid)
	require.Equal(t, faster, fasterArg.String)
	require.Equal(t, "text", usageLogInsertArgTypes[n-2])

	_, ok = prepared.args[n-1].(time.Time)
	require.True(t, ok, "created_at 必须仍在末位，实际 %T", prepared.args[n-1])

	absent := prepareUsageLogInsert(&service.UsageLog{
		UserID: 1, APIKeyID: 2, RequestID: "client:safety-buffering-absent", Model: "gpt-6-astra",
		CreatedAt: time.Now().UTC(),
	})
	nullEnabled, ok := absent.args[n-3].(sql.NullBool)
	require.True(t, ok)
	require.False(t, nullEnabled.Valid, "上游没带头时必须写 NULL，而不是 false")
	nullFaster, ok := absent.args[n-2].(sql.NullString)
	require.True(t, ok)
	require.False(t, nullFaster.Valid, "上游没带头时必须写 NULL，而不是空串")

	require.Contains(t, usageLogSelectColumns, "safety_buffering_enabled")
	require.Contains(t, usageLogSelectColumns, "safety_buffering_faster_model")
}

// usageLogTailScannerStub 只写 scan 参数的尾部三列（enabled、faster_model、created_at），其余列零值：
// 钉住 scanUsageLog 的尾部顺序与类型——两列写反或漏 scan，这里的类型断言/取值就失败。
type usageLogTailScannerStub struct {
	t       *testing.T
	enabled sql.NullBool
	faster  sql.NullString
	created time.Time
}

func (s usageLogTailScannerStub) Scan(dest ...any) error {
	s.t.Helper()
	n := len(dest)
	require.GreaterOrEqual(s.t, n, 3)
	enabled, ok := dest[n-3].(*sql.NullBool)
	require.True(s.t, ok, "倒数第 3 个 scan 目标应是 *sql.NullBool，实际 %T", dest[n-3])
	*enabled = s.enabled
	faster, ok := dest[n-2].(*sql.NullString)
	require.True(s.t, ok, "倒数第 2 个 scan 目标应是 *sql.NullString，实际 %T", dest[n-2])
	*faster = s.faster
	created, ok := dest[n-1].(*time.Time)
	require.True(s.t, ok, "created_at 必须仍在末位，实际 %T", dest[n-1])
	*created = s.created
	return nil
}

func TestScanUsageLog_SafetyBufferingColumns(t *testing.T) {
	now := time.Now().UTC()
	log, err := scanUsageLog(usageLogTailScannerStub{
		t:       t,
		enabled: sql.NullBool{Valid: true, Bool: true},
		faster:  sql.NullString{Valid: true, String: "gpt-5.6-luna"},
		created: now,
	})
	require.NoError(t, err)
	require.NotNil(t, log.SafetyBufferingEnabled)
	require.True(t, *log.SafetyBufferingEnabled)
	require.NotNil(t, log.SafetyBufferingFasterModel)
	require.Equal(t, "gpt-5.6-luna", *log.SafetyBufferingFasterModel)
	require.Equal(t, now, log.CreatedAt)

	nulls, err := scanUsageLog(usageLogTailScannerStub{t: t, created: now})
	require.NoError(t, err)
	require.Nil(t, nulls.SafetyBufferingEnabled)
	require.Nil(t, nulls.SafetyBufferingFasterModel)
}
