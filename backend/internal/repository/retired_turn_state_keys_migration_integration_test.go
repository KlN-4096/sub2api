//go:build integration

package repository

import (
	"context"
	"testing"

	dbmigrations "github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func TestMigration252DropsRetiredTurnStateAndGatewayPoolKeys(t *testing.T) {
	tx := testTx(t)
	ctx := context.Background()
	_, err := tx.ExecContext(ctx, `CREATE TEMP TABLE accounts (id bigint PRIMARY KEY, extra jsonb NOT NULL)`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO accounts (id, extra) VALUES
		(1, '{"openai_turn_state_hunter":{"enabled":true},"openai_turn_state_hunt":{"hour_count":3},
			"openai_turn_state_auto":true,"openai_turn_state_pool":[{"blob":"b"}],
			"openai_turn_state_pool_size":3,"openai_turn_state_fail_threshold":1,
			"openai_turn_state_recovery":{"enabled":true},"openai_turn_state_recovery_state":{"fail_streak":1},
			"openai_gwpool_metrics":{"probes":1},"openai_gwpool_prepare_retries":2,
			"openai_turn_state_override":{"gpt-5.6-luna":"blob"},"openai_turn_state_stale_after_minutes":30,
			"openai_turn_state_observed":{"chars":292},
			"openai_gwpool":true}'),
		(2, '{"model_rate_limits":{
			"gpt-6-astra":{"rate_limit_reset_at":"2099-01-01T00:00:00Z","reason":"turn_state_hold"},
			"gpt-5.5":{"rate_limit_reset_at":"2099-01-01T00:00:00Z","reason":"upstream_429"},
			"odd":"scalar"}}'),
		(3, '{"model_rate_limits":{"gpt-6-astra":{"reason":"turn_state_hold"}}}'),
		(4, '{"model_rate_limits":["turn_state_hold"],"unrelated":1}'),
		(5, '{"model_rate_limits":{"gpt-5.5":{"reason":"upstream_429"}}}')`)
	require.NoError(t, err)

	migrationSQL, err := dbmigrations.FS.ReadFile("252_drop_retired_turn_state_and_gwpool_keys.sql")
	require.NoError(t, err)
	for range 2 { // replay is a no-op
		_, err = tx.ExecContext(ctx, string(migrationSQL))
		require.NoError(t, err)
	}
	want := map[int64]string{
		1: `{"openai_turn_state_observed":{"chars":292},"openai_gwpool":true}`,
		2: `{"model_rate_limits":{"gpt-5.5":{"rate_limit_reset_at":"2099-01-01T00:00:00Z","reason":"upstream_429"},"odd":"scalar"}}`,
		3: `{"model_rate_limits":{}}`,
		4: `{"model_rate_limits":["turn_state_hold"],"unrelated":1}`,
		5: `{"model_rate_limits":{"gpt-5.5":{"reason":"upstream_429"}}}`,
	}
	for id, expected := range want {
		var got string
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT extra::text FROM accounts WHERE id = $1`, id).Scan(&got))
		require.JSONEq(t, expected, got, "account %d", id)
	}
}
