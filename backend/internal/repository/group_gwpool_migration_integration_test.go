//go:build integration

package repository

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolGroupMigrationBackfillsAndPreservesExplicitValues(t *testing.T) {
	ctx := context.Background()
	tx := testTx(t)
	// A transaction-local table shadows the real groups relation. The exact
	// shipped migration runs against existing synthetic rows, never live data.
	_, err := tx.ExecContext(ctx, `CREATE TEMP TABLE groups (id bigint PRIMARY KEY);
		INSERT INTO groups (id) VALUES (1), (2)`)
	require.NoError(t, err)
	migration, err := migrations.FS.ReadFile("250_add_group_gwpool_active_accounts.sql")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, string(migration))
	require.NoError(t, err)
	var defaults int
	require.NoError(t, tx.QueryRowContext(ctx,
		`SELECT count(*) FROM groups WHERE openai_gwpool_active_accounts = 1`).Scan(&defaults))
	require.Equal(t, 2, defaults, "existing groups start at 1")
	_, err = tx.ExecContext(ctx, `INSERT INTO groups (id) VALUES (3);
		UPDATE groups SET openai_gwpool_active_accounts = 3 WHERE id = 1`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, string(migration))
	require.NoError(t, err)
	var explicit, newDefault int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT openai_gwpool_active_accounts FROM groups WHERE id = 1`).Scan(&explicit))
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT openai_gwpool_active_accounts FROM groups WHERE id = 3`).Scan(&newDefault))
	require.Equal(t, 3, explicit, "idempotent migration must not overwrite an explicit value")
	require.Equal(t, 1, newDefault, "new groups default to 1")
	for _, invalid := range []any{0, 65, nil} {
		_, err = tx.ExecContext(ctx, `SAVEPOINT invalid_limit`)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `UPDATE groups SET openai_gwpool_active_accounts = $1 WHERE id = 2`, invalid)
		require.Error(t, err)
		_, err = tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT invalid_limit`)
		require.NoError(t, err)
	}
	_, err = tx.ExecContext(ctx, `UPDATE groups SET openai_gwpool_active_accounts = 64 WHERE id = 2`)
	require.NoError(t, err)
}
