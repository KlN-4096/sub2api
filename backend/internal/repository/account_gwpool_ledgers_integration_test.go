//go:build integration

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	dbmigrations "github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

// The real gateway_pool_ledgers references the real accounts. These shadow
// both for the session, so fixed IDs need no account rows.
const gatewayPoolLedgerTempTables = `
CREATE TEMP TABLE accounts (id bigint PRIMARY KEY, extra jsonb NOT NULL);
CREATE TEMP TABLE gateway_pool_ledgers (account_id bigint NOT NULL, kind text NOT NULL, value jsonb NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT NOW(), PRIMARY KEY (account_id, kind))`

func requireGatewayPoolExtra(t *testing.T, tx *sql.Tx, want map[int64]string) {
	t.Helper()
	for id, expected := range want {
		var got string
		require.NoError(t, tx.QueryRowContext(context.Background(), `SELECT extra::text FROM accounts WHERE id = $1`, id).Scan(&got))
		require.JSONEq(t, expected, got, "account %d", id)
	}
}

func TestMigration253MovesGatewayPoolLedgersToTable(t *testing.T) {
	tx := testTx(t)
	ctx := context.Background()
	_, err := tx.ExecContext(ctx, gatewayPoolLedgerTempTables)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO accounts (id, extra) VALUES
		(1, '{"openai_gwpool":true,"openai_gwpool_ledger_tag":"t","openai_gwpool_usage_tag":"t","openai_gwpool_rest_tag":"t",
			"openai_gwpool_gateways":{"ledger_tag":"t","current":"g1"},"openai_gwpool_contacts":{"ledger_tag":"t"},
			"openai_gwpool_usage_rounds":{"tag":"t"},"openai_gwpool_feedback_outbox":{"pending":[]},
			"openai_gwpool_rest_state":{"tag":"t"},"openai_gwpool_usage_blocked_at":"2026-10-06T12:00:00Z"}'),
		(2, '{"openai_gwpool_contacts":null,"openai_gwpool_usage_rounds":["bad"],"unrelated":1}'),
		(3, '{"unrelated":1}')`)
	require.NoError(t, err)
	// A copy left by an earlier run yields to the row's current extra.
	_, err = tx.ExecContext(ctx, `INSERT INTO gateway_pool_ledgers (account_id, kind, value)
		VALUES (1, 'openai_gwpool_gateways', '{"stale":true}')`)
	require.NoError(t, err)

	migrationSQL, err := dbmigrations.FS.ReadFile("253_move_gwpool_ledgers_to_table.sql")
	require.NoError(t, err)
	for range 2 { // replay is a no-op
		_, err = tx.ExecContext(ctx, string(migrationSQL))
		require.NoError(t, err)
	}
	requireGatewayPoolExtra(t, tx, map[int64]string{
		1: `{"openai_gwpool":true,"openai_gwpool_ledger_tag":"t","openai_gwpool_usage_tag":"t","openai_gwpool_rest_tag":"t",
			"openai_gwpool_rest_state":{"tag":"t"},"openai_gwpool_usage_blocked_at":"2026-10-06T12:00:00Z"}`,
		2: `{"unrelated":1}`, // non-object values were never readable ledgers
		3: `{"unrelated":1}`,
	})
	rows, err := tx.QueryContext(ctx, `SELECT account_id, kind, value::text FROM gateway_pool_ledgers`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	got := map[string]string{}
	for rows.Next() {
		var id int64
		var kind, value string
		require.NoError(t, rows.Scan(&id, &kind, &value))
		require.EqualValues(t, 1, id)
		got[kind] = value
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 4)
	require.JSONEq(t, `{"ledger_tag":"t","current":"g1"}`, got["openai_gwpool_gateways"])
	require.JSONEq(t, `{"ledger_tag":"t"}`, got["openai_gwpool_contacts"])
	require.JSONEq(t, `{"tag":"t"}`, got["openai_gwpool_usage_rounds"])
	require.JSONEq(t, `{"pending":[]}`, got["openai_gwpool_feedback_outbox"])
}

func TestGatewayPoolLedgerRollbackScriptRestoresExtra(t *testing.T) {
	tx := testTx(t)
	ctx := context.Background()
	_, err := tx.ExecContext(ctx, gatewayPoolLedgerTempTables+`;
		CREATE TEMP TABLE schema_migrations (filename text PRIMARY KEY, checksum text NOT NULL)`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO accounts (id, extra) VALUES
		(1, '{"openai_gwpool_ledger_tag":"t","openai_gwpool_gateways":{"written":"by the old release"}}'),
		(2, '{"unrelated":1}');
		INSERT INTO gateway_pool_ledgers (account_id, kind, value) VALUES
		(1, 'openai_gwpool_gateways', '{"ledger_tag":"t"}'), (1, 'openai_gwpool_contacts', '{"ledger_tag":"t"}');
		INSERT INTO schema_migrations VALUES ('252_drop_retired_turn_state_and_gwpool_keys.sql', 'a'),
		('253_move_gwpool_ledgers_to_table.sql', 'b')`)
	require.NoError(t, err)

	script, err := os.ReadFile("../../scripts/gwpool-ledger-rollback.sql")
	require.NoError(t, err)
	// The script commits its own transaction; here it runs inside the test's.
	body := strings.NewReplacer("BEGIN;", "", "COMMIT;", "").Replace(string(script))
	_, err = tx.ExecContext(ctx, body)
	require.NoError(t, err)
	requireGatewayPoolExtra(t, tx, map[int64]string{
		1: `{"openai_gwpool_ledger_tag":"t","openai_gwpool_gateways":{"ledger_tag":"t"},"openai_gwpool_contacts":{"ledger_tag":"t"}}`,
		2: `{"unrelated":1}`,
	})
	var temp, public sql.NullString
	require.NoError(t, tx.QueryRowContext(ctx,
		`SELECT to_regclass('pg_temp.gateway_pool_ledgers')::text, to_regclass('public.gateway_pool_ledgers')::text`).Scan(&temp, &public))
	require.False(t, temp.Valid, "the table is dropped")
	require.True(t, public.Valid, "this test only touched its own session's tables")
	var remaining []string
	rows, err := tx.QueryContext(ctx, `SELECT filename FROM schema_migrations ORDER BY filename`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		remaining = append(remaining, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"252_drop_retired_turn_state_and_gwpool_keys.sql"}, remaining)
}

func TestGatewayPoolLedgersActualPostgresWriteLoadAndDelete(t *testing.T) {
	tx := testEntTx(t)
	ctx := dbent.NewTxContext(context.Background(), tx)
	client := tx.Client()
	tag := "ledger-it-" + t.Name()
	account := mustCreateAccount(t, client, &service.Account{Name: "gwpool-ledgers", Platform: service.PlatformOpenAI,
		Extra: map[string]any{"openai_gwpool_usage_tag": "old", "unrelated": 1}})
	queryString := func(query string) string {
		rows, err := client.QueryContext(ctx, query, account.ID)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		var value string
		if rows.Next() {
			require.NoError(t, rows.Scan(&value))
		}
		require.NoError(t, rows.Err())
		return value
	}
	usage := func(sequence int) map[string]any {
		return map[string]any{"openai_gwpool_usage_rounds": map[string]any{"tag": tag, "verification_sequence": sequence}}
	}
	tags := map[string]any{"openai_gwpool_usage_tag": tag}
	repo := &accountRepository{} // no outer executors: only the transaction can serve these calls

	require.NoError(t, repo.WriteGatewayPoolLedgers(ctx, account.ID, usage(1), tags))
	// Only these keys: insert triggers add their own (migration 175's billing flag).
	require.Equal(t, tag+" 1 false", queryString(`SELECT concat_ws(' ', extra->>'openai_gwpool_usage_tag', extra->>'unrelated',
		(extra ? 'openai_gwpool_usage_rounds')::text) FROM accounts WHERE id = $1`), "the tag merges into extra, the ledger does not")
	version := queryString(`SELECT ctid::text FROM accounts WHERE id = $1`)
	require.NoError(t, repo.WriteGatewayPoolLedgers(ctx, account.ID, usage(2), tags))
	require.Equal(t, version, queryString(`SELECT ctid::text FROM accounts WHERE id = $1`), "an unchanged tag leaves the accounts row alone")

	loaded, err := repo.LoadGatewayPoolLedgers(ctx, []int64{account.ID, account.ID + 1_000_000},
		[]string{"openai_gwpool_usage_rounds", "openai_gwpool_contacts"})
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	require.Len(t, loaded[account.ID], 1)
	require.JSONEq(t, `{"tag":"`+tag+`","verification_sequence":2}`, string(loaded[account.ID]["openai_gwpool_usage_rounds"]))
	summary, err := repo.LoadGatewayPoolLedgerWithout(ctx, account.ID, "openai_gwpool_usage_rounds", "verification_sequence", "absent")
	require.NoError(t, err)
	require.JSONEq(t, `{"tag":"`+tag+`"}`, string(summary), "the database drops the omitted fields")

	peers, err := repo.FindGatewayPoolStatePeers(ctx, tag, "usage", 0)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	raw, err := json.Marshal(peers[0].Extra["openai_gwpool_usage_rounds"])
	require.NoError(t, err)
	require.JSONEq(t, `{"tag":"`+tag+`","verification_sequence":2}`, string(raw), "peers read the ledger table")

	_, err = client.ExecContext(ctx, `UPDATE accounts SET deleted_at = NOW() WHERE id = $1`, account.ID)
	require.NoError(t, err)
	require.ErrorIs(t, repo.WriteGatewayPoolLedgers(ctx, account.ID, usage(3), tags), service.ErrAccountNotFound)
	_, err = client.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, account.ID)
	require.NoError(t, err)
	require.Equal(t, "0", queryString(`SELECT count(*)::text FROM gateway_pool_ledgers WHERE account_id = $1`), "ledgers go with their row")
}
