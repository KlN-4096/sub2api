//go:build integration

package repository

import (
	"context"
	"testing"

	dbmigrations "github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func TestMigration251DropsGatewayPoolPreviousLedgers(t *testing.T) {
	tx := testTx(t)
	ctx := context.Background()
	_, err := tx.ExecContext(ctx, `CREATE TEMP TABLE accounts (id bigint PRIMARY KEY, extra jsonb NOT NULL)`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO accounts (id, extra) VALUES
		(1, '{"openai_gwpool_ledger_tag":"cur","openai_gwpool_previous_ledger_tag":"old",
			"openai_gwpool_usage_tag":"cur","openai_gwpool_usage_previous_tag":"old",
			"openai_gwpool_rest_tag":"cur","openai_gwpool_rest_previous_tag":"old",
			"openai_gwpool_usage_rounds":{"tag":"cur","rounds":[1],"previous":{"tag":"old"}},
			"openai_gwpool_contacts":{"ledger_tag":"cur","previous":{"ledger_tag":"old"}},
			"openai_gwpool_gateways":{"ledger_tag":"cur","current":"g1","previous":{"ledger_tag":"old"}},
			"openai_gwpool_rest_state":{"tag":"cur","active":true,"previous":{"tag":"old"}},
			"unrelated":{"previous":"keep"}}'),
		(2, '{"openai_gwpool_usage_rounds":{"tag":"cur","previous":{"tag":"old"}}}'),
		(3, '{"openai_gwpool_previous_ledger_tag":"old","openai_gwpool_usage_rounds":null}'),
		(4, '{"unrelated":{"previous":"keep"}}'),
		(5, '{"openai_gwpool_usage_previous_tag":"old","openai_gwpool_contacts":["previous"]}')`)
	require.NoError(t, err)

	migrationSQL, err := dbmigrations.FS.ReadFile("251_drop_gwpool_previous_ledgers.sql")
	require.NoError(t, err)
	for range 2 { // replay is a no-op
		_, err = tx.ExecContext(ctx, string(migrationSQL))
		require.NoError(t, err)
	}
	want := map[int64]string{
		1: `{"openai_gwpool_ledger_tag":"cur","openai_gwpool_usage_tag":"cur","openai_gwpool_rest_tag":"cur",
			"openai_gwpool_usage_rounds":{"tag":"cur","rounds":[1]},"openai_gwpool_contacts":{"ledger_tag":"cur"},
			"openai_gwpool_gateways":{"ledger_tag":"cur","current":"g1"},
			"openai_gwpool_rest_state":{"tag":"cur","active":true},"unrelated":{"previous":"keep"}}`,
		2: `{"openai_gwpool_usage_rounds":{"tag":"cur"}}`,
		3: `{"openai_gwpool_usage_rounds":null}`,
		4: `{"unrelated":{"previous":"keep"}}`,
		5: `{"openai_gwpool_contacts":["previous"]}`,
	}
	for id, expected := range want {
		var got string
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT extra::text FROM accounts WHERE id = $1`, id).Scan(&got))
		require.JSONEq(t, expected, got, "account %d", id)
	}
}
