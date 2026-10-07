package repository

import (
	"context"
	"errors"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolStatePeersNarrowSingleQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := &accountRepository{sql: db}
	for _, kind := range []string{"usage", "rest"} {
		mock.ExpectQuery(`SELECT id, jsonb_build_object\([\s\S]*FROM accounts WHERE deleted_at IS NULL AND\s*\(extra->>'openai_gwpool_` + kind + `_tag' = \$1 OR extra->>'openai_gwpool_` + kind + `_previous_tag' = \$1\)\s*ORDER BY CASE WHEN extra->>'openai_gwpool_` + kind + `_tag' = \$1 THEN 0 ELSE 1 END`).
			WithArgs("offline-tag").
			WillReturnRows(sqlmock.NewRows([]string{"id", "extra"}).AddRow(2, `{"openai_gwpool_usage_blocked_at":"2026-10-06T10:00:00Z"}`))
		rows, err := repo.FindGatewayPoolStatePeers(context.Background(), "offline-tag", kind)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 2, rows[0].ID)
		require.Nil(t, rows[0].Credentials)
		require.Equal(t, "2026-10-06T10:00:00Z", rows[0].GetExtraString("openai_gwpool_usage_blocked_at"))
	}
	_, err = repo.FindGatewayPoolStatePeers(context.Background(), "offline-tag", "other")
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
	mock.ExpectQuery(`SELECT id, jsonb_build_object`).WithArgs("tag").WillReturnError(errors.New("offline query"))
	_, err = repo.FindGatewayPoolStatePeers(ctx, "tag", "usage")
	require.ErrorContains(t, err, "offline query")
	mock.ExpectQuery(`SELECT id, jsonb_build_object`).WithArgs("tag").
		WillReturnRows(sqlmock.NewRows([]string{"id", "extra"}).AddRow(2, `{bad json`))
	_, err = repo.FindGatewayPoolStatePeers(ctx, "tag", "rest")
	require.Error(t, err)
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}
