package repository

import (
	"context"
	"encoding/json"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

// gateway_pool_ledgers (migration 253) keeps the gateway-pool runtime ledgers
// one row per (account, original extra key). The discovery tags they are
// grouped by stay in accounts.extra.

// Without one of these the gateway pool silently takes its fallback path.
var (
	_ service.GatewayPoolLedgerRepository    = (*accountRepository)(nil)
	_ service.GatewayPoolStatePeerRepository = (*accountRepository)(nil)
	_ service.GatewayPoolRestRepository      = (*accountRepository)(nil)
	_ service.GatewayPoolClearRestRepository = (*accountRepository)(nil)
)

// LoadGatewayPoolLedgers reads the named ledgers of the given rows.
func (r *accountRepository) LoadGatewayPoolLedgers(ctx context.Context, ids []int64, keys []string) (map[int64]map[string]json.RawMessage, error) {
	ledgers := map[int64]map[string]json.RawMessage{}
	if len(ids) == 0 || len(keys) == 0 {
		return ledgers, nil
	}
	exec := r.sql
	if tx := dbent.TxFromContext(ctx); tx != nil {
		exec = tx.Client()
	}
	rows, err := exec.QueryContext(ctx,
		`SELECT account_id, kind, value FROM gateway_pool_ledgers WHERE account_id = ANY($1) AND kind = ANY($2)`,
		pq.Array(ids), pq.Array(keys))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int64
		var kind string
		var value []byte
		if err := rows.Scan(&id, &kind, &value); err != nil {
			return nil, err
		}
		if ledgers[id] == nil {
			ledgers[id] = map[string]json.RawMessage{}
		}
		ledgers[id][kind] = value
	}
	return ledgers, rows.Err()
}

// LoadGatewayPoolLedgerWithout reads one ledger of a row less the named
// top-level fields, for readers that need little of a large ledger: the
// database drops them, so they are neither sent nor parsed.
func (r *accountRepository) LoadGatewayPoolLedgerWithout(ctx context.Context, id int64, kind string, omit ...string) (json.RawMessage, error) {
	exec := r.sql
	if tx := dbent.TxFromContext(ctx); tx != nil {
		exec = tx.Client()
	}
	rows, err := exec.QueryContext(ctx,
		`SELECT value - $3::text[] FROM gateway_pool_ledgers WHERE account_id = $1 AND kind = $2`, id, kind, pq.Array(omit))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var value []byte
	if rows.Next() {
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
	}
	return value, rows.Err()
}

// WriteGatewayPoolLedgers upserts ledgers and, in the same statement, merges
// tags into extra only when they differ, so steady-state ledger writes never
// rewrite the accounts row. A missing or soft-deleted row writes nothing.
func (r *accountRepository) WriteGatewayPoolLedgers(ctx context.Context, id int64, ledgers, tags map[string]any) error {
	ledgerJSON, err := json.Marshal(ledgers)
	if err != nil {
		return err
	}
	tagJSON, err := json.Marshal(tags)
	if err != nil {
		return err
	}
	result, err := clientFromContext(ctx, r.client).ExecContext(ctx, `WITH tags AS (
	UPDATE accounts SET extra = COALESCE(extra, '{}'::jsonb) || $3::jsonb
	WHERE id = $1 AND deleted_at IS NULL AND NOT COALESCE(extra, '{}'::jsonb) @> $3::jsonb
)
INSERT INTO gateway_pool_ledgers (account_id, kind, value, updated_at)
SELECT a.id, l.key, l.value, NOW()
FROM accounts a CROSS JOIN jsonb_each($2::jsonb) AS l(key, value)
WHERE a.id = $1 AND a.deleted_at IS NULL
ON CONFLICT (account_id, kind) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at`,
		id, string(ledgerJSON), string(tagJSON))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return service.ErrAccountNotFound
	}
	return nil
}
