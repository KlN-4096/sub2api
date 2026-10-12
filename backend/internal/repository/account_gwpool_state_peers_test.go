package repository

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
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
		mock.ExpectQuery(`SELECT id, jsonb_build_object\(\s*'`+tc.projected+`'[\s\S]*FROM accounts WHERE deleted_at IS NULL AND extra->>'`+
			tc.tagKey+`' = \$1 AND id <> \$2 ORDER BY id$`).
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
	mock.ExpectQuery(`SELECT id, jsonb_build_object`).WithArgs("tag", int64(0)).WillReturnError(errors.New("offline query"))
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

func TestGatewayPoolRuntimeExtraMergesOnlyTheRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	update := "^" + regexp.QuoteMeta("UPDATE accounts SET extra = COALESCE(extra, '{}'::jsonb) || $1::jsonb, "+
		"updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL") + "$"
	patch := map[string]any{"openai_gwpool_usage_tag": "tag"}

	// One statement: an outbox insert after it would be unexpected and fail.
	mock.ExpectExec(update).WithArgs(`{"openai_gwpool_usage_tag":"tag"}`, int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, (&accountRepository{client: client}).UpdateGatewayPoolRuntimeExtra(context.Background(), 7, patch))

	// Inside a caller's transaction the outer client is never used.
	mock.ExpectBegin()
	tx, err := client.Tx(context.Background())
	require.NoError(t, err)
	mock.ExpectExec(update).WithArgs(`{"openai_gwpool_usage_tag":"tag"}`, int64(8)).WillReturnResult(sqlmock.NewResult(0, 0))
	err = (&accountRepository{}).UpdateGatewayPoolRuntimeExtra(dbent.NewTxContext(context.Background(), tx), 8, patch)
	require.ErrorIs(t, err, service.ErrAccountNotFound)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}
