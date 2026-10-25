//go:build unit

package repository

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// TestPrepareUsageLogInsert_TPSInputsArgWiring 把 first_answer_ms / reasoning_tokens 钉在
// created_at 之前的倒数第 3、第 2 位，未提供时写 NULL（不是 0：0 是「上游报了没推理」）。
func TestPrepareUsageLogInsert_TPSInputsArgWiring(t *testing.T) {
	firstAnswer, reasoning := 9350, 412
	prepared := prepareUsageLogInsert(&service.UsageLog{
		UserID: 1, APIKeyID: 2, RequestID: "client:tps-inputs", Model: "gpt-6.1-sol",
		FirstAnswerMs: &firstAnswer, ReasoningTokens: &reasoning, CreatedAt: time.Now().UTC(),
	})
	require.Len(t, prepared.args, len(usageLogInsertArgTypes))
	n := len(prepared.args)
	require.Equal(t, sql.NullInt64{Int64: 9350, Valid: true}, prepared.args[n-3])
	require.Equal(t, "integer", usageLogInsertArgTypes[n-3])
	require.Equal(t, sql.NullInt64{Int64: 412, Valid: true}, prepared.args[n-2])
	require.Equal(t, "integer", usageLogInsertArgTypes[n-2])

	absent := prepareUsageLogInsert(&service.UsageLog{UserID: 1, APIKeyID: 2, RequestID: "client:tps-absent", Model: "gpt-5", CreatedAt: time.Now().UTC()})
	require.Equal(t, sql.NullInt64{}, absent.args[n-3])
	require.Equal(t, sql.NullInt64{}, absent.args[n-2])
	require.Contains(t, usageLogSelectColumns, "first_answer_ms, reasoning_tokens, created_at")
}

type usageLogTPSScannerStub struct {
	firstAnswer, reasoning sql.NullInt64
}

func (s usageLogTPSScannerStub) Scan(dest ...any) error {
	n := len(dest)
	*dest[n-3].(*sql.NullInt64) = s.firstAnswer
	*dest[n-2].(*sql.NullInt64) = s.reasoning
	*dest[n-1].(*time.Time) = time.Now().UTC()
	return nil
}

func TestScanUsageLog_TPSInputs(t *testing.T) {
	log, err := scanUsageLog(usageLogTPSScannerStub{
		firstAnswer: sql.NullInt64{Int64: 9350, Valid: true},
		reasoning:   sql.NullInt64{Int64: 0, Valid: true},
	})
	require.NoError(t, err)
	require.NotNil(t, log.FirstAnswerMs)
	require.Equal(t, 9350, *log.FirstAnswerMs)
	require.NotNil(t, log.ReasoningTokens, "上游报了 0 个推理 token 也要保留，与 NULL 区分")
	require.Equal(t, 0, *log.ReasoningTokens)

	log, err = scanUsageLog(usageLogTPSScannerStub{})
	require.NoError(t, err)
	require.Nil(t, log.FirstAnswerMs)
	require.Nil(t, log.ReasoningTokens)
}

// 四条 INSERT 的每个列清单里两列都紧跟 route_pair_pool_version、在 created_at 之前，与参数顺序一致：
// 两列同为 integer，列名对调库不报错，值会落错列。
func TestUsageLogInsertStatements_TPSColumnOrder(t *testing.T) {
	log := &service.UsageLog{UserID: 1, APIKeyID: 2, RequestID: "client:tps-order", Model: "gpt-5", CreatedAt: time.Now().UTC()}
	prepared := prepareUsageLogInsert(log)
	batchQuery, _ := buildUsageLogBatchInsertQuery([]string{"k"}, map[string]usageLogInsertPrepared{"k": prepared})
	bestEffortQuery, _ := buildUsageLogBestEffortInsertQuery([]usageLogInsertPrepared{prepared})

	var captured []string
	db, mock := newSQLCapturingMock(t, &captured)
	mock.ExpectQuery("INSERT INTO usage_logs").WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(int64(1), log.CreatedAt))
	mock.ExpectExec("INSERT INTO usage_logs").WillReturnResult(sqlmock.NewResult(0, 1))
	_, err := (&usageLogRepository{sql: db}).Create(context.Background(), log)
	require.NoError(t, err)
	require.NoError(t, execUsageLogInsertNoResult(context.Background(), db, prepared))
	require.Len(t, captured, 2)

	order := regexp.MustCompile(`route_pair_pool_version\s*,\s*first_answer_ms\s*,\s*reasoning_tokens\s*,\s*created_at\b`)
	for _, query := range append(captured, batchQuery, bestEffortQuery) {
		lists := strings.Count(query, "route_pair_pool_version")
		require.Positive(t, lists)
		require.Len(t, order.FindAllString(query, -1), lists, query)
	}
}
