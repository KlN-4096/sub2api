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
		`CREATE TEMP TABLE accounts (id bigint PRIMARY KEY, extra jsonb NOT NULL, deleted_at timestamptz)`)
	require.NoError(t, err)
	tag := "offline' OR 1=1 --"
	for id := 1; id <= 4; id++ {
		extra := map[string]any{"unrelated": "must-not-be-projected"}
		for _, kind := range []string{"usage", "rest"} {
			key := "openai_gwpool_" + kind + "_tag"
			if id == 1 {
				key = "openai_gwpool_" + kind + "_previous_tag"
			}
			value := tag
			if id == 4 {
				value = "different-domain"
			}
			extra[key] = value
		}
		extra["openai_gwpool_usage_rounds"] = map[string]any{"tag": tag, "verification_sequence": id}
		extra["openai_gwpool_rest_state"] = map[string]any{"tag": tag, "active": id == 1}
		extra["openai_gwpool_usage_blocked_at"] = "2026-10-06T12:00:00Z"
		raw, marshalErr := json.Marshal(extra)
		require.NoError(t, marshalErr)
		_, err = tx.Client().ExecContext(ctx, `INSERT INTO accounts (id, extra, deleted_at)
			VALUES ($1, $2::jsonb, CASE WHEN $1::bigint = 3 THEN now() ELSE NULL END)`, id, string(raw))
		require.NoError(t, err)
	}
	// No fallback SQL executor: this also proves the Ent transaction is used.
	repo := &accountRepository{}
	for _, kind := range []string{"usage", "rest"} {
		rows, readErr := repo.FindGatewayPoolStatePeers(ctx, tag, kind)
		require.NoError(t, readErr)
		require.Len(t, rows, 2, "exclude soft-deleted and unrelated domains")
		require.EqualValues(t, 2, rows[0].ID, "current precedes previous despite insertion order")
		require.EqualValues(t, 1, rows[1].ID)
		for _, row := range rows {
			require.Nil(t, row.Credentials)
			require.NotContains(t, row.Extra, "unrelated")
			if kind == "usage" {
				require.Len(t, row.Extra, 2)
				require.Equal(t, "2026-10-06T12:00:00Z", row.GetExtraString("openai_gwpool_usage_blocked_at"))
			} else {
				require.Len(t, row.Extra, 1)
				require.Contains(t, row.Extra, "openai_gwpool_rest_state")
			}
		}
	}
}
