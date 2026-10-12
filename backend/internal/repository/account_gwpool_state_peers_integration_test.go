//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"testing"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolStatePeersActualPostgresProjectionAndOrder(t *testing.T) {
	tx := testEntTx(t)
	ctx := dbent.NewTxContext(context.Background(), tx)
	_, err := tx.Client().ExecContext(ctx,
		`CREATE TEMP TABLE accounts (id bigint PRIMARY KEY, extra jsonb NOT NULL, deleted_at timestamptz);
		CREATE TEMP TABLE gateway_pool_ledgers (account_id bigint NOT NULL, kind text NOT NULL, value jsonb NOT NULL,
			PRIMARY KEY (account_id, kind))`)
	require.NoError(t, err)
	tag := "offline' OR 1=1 --"
	for id := 1; id <= 4; id++ {
		extra := map[string]any{"unrelated": "must-not-be-projected"}
		value := tag
		if id == 4 {
			value = "different-domain"
		}
		for key, legacy := range map[string]string{"openai_gwpool_usage_tag": "openai_gwpool_usage_previous_tag",
			"openai_gwpool_rest_tag": "openai_gwpool_rest_previous_tag", "openai_gwpool_ledger_tag": "openai_gwpool_previous_ledger_tag"} {
			if id == 1 {
				key = legacy // legacy previous-ledger keys from before 251 must not match
			}
			extra[key] = value
		}
		extra["openai_gwpool_rest_state"] = map[string]any{"tag": tag, "active": id == 1}
		extra["openai_gwpool_usage_blocked_at"] = "2026-10-06T12:00:00Z"
		raw, marshalErr := json.Marshal(extra)
		require.NoError(t, marshalErr)
		_, err = tx.Client().ExecContext(ctx, `INSERT INTO accounts (id, extra, deleted_at)
			VALUES ($1, $2::jsonb, CASE WHEN $1::bigint = 3 THEN now() ELSE NULL END)`, id, string(raw))
		require.NoError(t, err)
		_, err = tx.Client().ExecContext(ctx, `INSERT INTO gateway_pool_ledgers (account_id, kind, value) VALUES
			($1, 'openai_gwpool_usage_rounds', jsonb_build_object('tag', $2::text, 'verification_sequence', $1::bigint)),
			($1, 'openai_gwpool_contacts', jsonb_build_object('ledger_tag', $2::text, 'row', $1::bigint))`, id, tag)
		require.NoError(t, err)
	}
	// No fallback SQL executor: this also proves the Ent transaction is used.
	repo := &accountRepository{}
	for _, kind := range []string{"usage", "rest", "contacts"} {
		rows, readErr := repo.FindGatewayPoolStatePeers(ctx, tag, kind, 0)
		require.NoError(t, readErr)
		require.Len(t, rows, 1, "exclude legacy previous tags, soft-deleted and unrelated domains")
		require.EqualValues(t, 2, rows[0].ID)
		for _, row := range rows {
			require.Nil(t, row.Credentials)
			require.NotContains(t, row.Extra, "unrelated")
			if kind == "usage" {
				require.Len(t, row.Extra, 2)
				require.Equal(t, "2026-10-06T12:00:00Z", row.GetExtraString("openai_gwpool_usage_blocked_at"))
				usage, _ := row.Extra["openai_gwpool_usage_rounds"].(map[string]any)
				require.EqualValues(t, 2, usage["verification_sequence"], "the usage ledger comes from gateway_pool_ledgers")
			} else if kind == "contacts" {
				require.Len(t, row.Extra, 1)
				contacts, _ := row.Extra["openai_gwpool_contacts"].(map[string]any)
				require.EqualValues(t, 2, contacts["row"], "the contacts ledger comes from gateway_pool_ledgers")
			} else {
				require.Len(t, row.Extra, 1)
				require.Contains(t, row.Extra, "openai_gwpool_rest_state")
			}
		}
	}
}
