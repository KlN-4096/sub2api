package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestSchedulerMetadataAccountKeepsGatewayPoolSwitch(t *testing.T) {
	metadata := buildSchedulerMetadataAccount(service.Account{
		ID: 26, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Extra: map[string]any{service.OpenAIGatewayPoolExtraKey: true, "openai_gwpool_contacts": map[string]any{"ledger_tag": "tag"}},
	})
	require.True(t, metadata.UsesGatewayPool(), "selection fresh-reads only the rows this projection marks as pool rows")
	require.NotContains(t, metadata.Extra, "openai_gwpool_contacts", "runtime ledgers stay out of the projection")
}

func TestGatewayPoolStatePeersNarrowSingleQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := &accountRepository{sql: db}
	for _, tc := range []struct {
		kind, tagKey, projected string
		exclude                 int64
	}{
		{"usage", "openai_gwpool_usage_tag", "openai_gwpool_usage_rounds", 0},
		{"rest", "openai_gwpool_rest_tag", "openai_gwpool_rest_state", 0},
		{"contacts", "openai_gwpool_ledger_tag", "openai_gwpool_contacts", 1},
	} {
		// Rows are found by their extra tag; usage and contacts come from the ledger table.
		query := `SELECT id, jsonb_build_object\(\s*'` + tc.projected + `'[\s\S]*FROM accounts WHERE deleted_at IS NULL AND extra->>'` +
			tc.tagKey + `' = \$1 AND id <> \$2 ORDER BY id$`
		if tc.kind != "rest" {
			blockedAt := `\)\s*` // usage also projects the blocked-at mark, which stays in extra
			if tc.kind == "usage" {
				blockedAt = `,\s*'openai_gwpool_usage_blocked_at', a\.extra->'openai_gwpool_usage_blocked_at'\)\s*`
			}
			query = `SELECT a\.id, jsonb_build_object\(\s*'` + tc.projected + `', l\.value` + blockedAt + `FROM accounts a LEFT JOIN gateway_pool_ledgers l ` +
				`ON l\.account_id = a\.id AND l\.kind = '` + tc.projected + `'\s*WHERE a\.deleted_at IS NULL AND a\.extra->>'` +
				tc.tagKey + `' = \$1 AND a\.id <> \$2 ORDER BY a\.id$`
		}
		mock.ExpectQuery(query).
			WithArgs("offline-tag", tc.exclude).
			WillReturnRows(sqlmock.NewRows([]string{"id", "extra"}).AddRow(2, `{"openai_gwpool_usage_blocked_at":"2026-10-06T10:00:00Z"}`))
		rows, err := repo.FindGatewayPoolStatePeers(context.Background(), "offline-tag", tc.kind, tc.exclude)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 2, rows[0].ID)
		require.Nil(t, rows[0].Credentials)
		require.Equal(t, "2026-10-06T10:00:00Z", rows[0].GetExtraString("openai_gwpool_usage_blocked_at"))
	}
	_, err = repo.FindGatewayPoolStatePeers(context.Background(), "offline-tag", "other", 0)
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGatewayPoolStatePeersPropagatesErrorsAndUsesTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	mock.ExpectBegin()
	tx, err := client.Tx(context.Background())
	require.NoError(t, err)
	// A nil outer executor makes accidentally ignoring the transaction fail.
	repo := &accountRepository{}
	ctx := dbent.NewTxContext(context.Background(), tx)
	mock.ExpectQuery(`SELECT a.id, jsonb_build_object`).WithArgs("tag", int64(0)).WillReturnError(errors.New("offline query"))
	_, err = repo.FindGatewayPoolStatePeers(ctx, "tag", "usage", 0)
	require.ErrorContains(t, err, "offline query")
	mock.ExpectQuery(`SELECT id, jsonb_build_object`).WithArgs("tag", int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "extra"}).AddRow(2, `{bad json`))
	_, err = repo.FindGatewayPoolStatePeers(ctx, "tag", "rest", 0)
	require.Error(t, err)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGatewayPoolLedgersWriteInOneStatementAndLoadByRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	write := `^WITH tags AS \(\s*UPDATE accounts SET extra = COALESCE\(extra, '\{\}'::jsonb\) \|\| \$3::jsonb\s*` +
		`WHERE id = \$1 AND deleted_at IS NULL AND NOT COALESCE\(extra, '\{\}'::jsonb\) @> \$3::jsonb\s*\)\s*` +
		`INSERT INTO gateway_pool_ledgers \(account_id, kind, value, updated_at\)\s*SELECT a\.id, l\.key, l\.value, NOW\(\)\s*` +
		`FROM accounts a CROSS JOIN jsonb_each\(\$2::jsonb\) AS l\(key, value\)\s*WHERE a\.id = \$1 AND a\.deleted_at IS NULL\s*` +
		`ON CONFLICT \(account_id, kind\) DO UPDATE SET value = EXCLUDED\.value, updated_at = EXCLUDED\.updated_at$`
	ledgers := map[string]any{"openai_gwpool_usage_rounds": map[string]any{"tag": "t"}}
	tags := map[string]any{"openai_gwpool_usage_tag": "t"}

	// One statement: an outbox insert or a second write after it would be unexpected and fail.
	mock.ExpectExec(write).WithArgs(int64(7), `{"openai_gwpool_usage_rounds":{"tag":"t"}}`, `{"openai_gwpool_usage_tag":"t"}`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, (&accountRepository{client: client}).WriteGatewayPoolLedgers(context.Background(), 7, ledgers, tags))

	// Inside a caller's transaction the outer executors are never used.
	mock.ExpectBegin()
	tx, err := client.Tx(context.Background())
	require.NoError(t, err)
	ctx := dbent.NewTxContext(context.Background(), tx)
	mock.ExpectExec(write).WithArgs(int64(8), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 0))
	require.ErrorIs(t, (&accountRepository{}).WriteGatewayPoolLedgers(ctx, 8, ledgers, tags), service.ErrAccountNotFound,
		"a missing or deleted row writes no ledger")
	keys := []string{"openai_gwpool_usage_rounds"}
	mock.ExpectQuery(`^SELECT account_id, kind, value FROM gateway_pool_ledgers WHERE account_id = ANY\(\$1\) AND kind = ANY\(\$2\)$`).
		WithArgs(pq.Array([]int64{7, 8}), pq.Array(keys)).
		WillReturnRows(sqlmock.NewRows([]string{"account_id", "kind", "value"}).AddRow(7, keys[0], `{"tag":"t"}`))
	loaded, err := (&accountRepository{}).LoadGatewayPoolLedgers(ctx, []int64{7, 8}, keys)
	require.NoError(t, err)
	require.Equal(t, map[int64]map[string]json.RawMessage{7: {keys[0]: json.RawMessage(`{"tag":"t"}`)}}, loaded)
	without := `^SELECT value - \$3::text\[\] FROM gateway_pool_ledgers WHERE account_id = \$1 AND kind = \$2$`
	mock.ExpectQuery(without).WithArgs(int64(7), keys[0], pq.Array([]string{"rounds"})).
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(`{"tag":"t"}`))
	summary, err := (&accountRepository{}).LoadGatewayPoolLedgerWithout(ctx, 7, keys[0], "rounds")
	require.NoError(t, err)
	require.JSONEq(t, `{"tag":"t"}`, string(summary))
	mock.ExpectQuery(without).WithArgs(int64(8), keys[0], pq.Array([]string{"rounds"})).WillReturnRows(sqlmock.NewRows([]string{"value"}))
	summary, err = (&accountRepository{}).LoadGatewayPoolLedgerWithout(ctx, 8, keys[0], "rounds")
	require.NoError(t, err)
	require.Nil(t, summary, "no ledger, no value")
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())

	loaded, err = (&accountRepository{}).LoadGatewayPoolLedgers(context.Background(), nil, keys)
	require.NoError(t, err)
	require.Empty(t, loaded, "no rows asked, no query")
	require.NoError(t, mock.ExpectationsWereMet())
}
