package repository

import (
	"context"
	"encoding/json"
	"fmt"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// FindGatewayPoolStatePeers reads only the state merged by usage/rest/contacts
// ledgers, skipping row exclude (0 keeps every row). No credentials, unrelated
// extra, proxies or group edges are materialized.
func (r *accountRepository) FindGatewayPoolStatePeers(ctx context.Context, tag, kind string, exclude int64) ([]service.Account, error) {
	var query string
	switch kind {
	case "usage":
		query = `SELECT id, jsonb_build_object(
			'openai_gwpool_usage_rounds', extra->'openai_gwpool_usage_rounds',
			'openai_gwpool_usage_blocked_at', extra->'openai_gwpool_usage_blocked_at')
			FROM accounts WHERE deleted_at IS NULL AND extra->>'openai_gwpool_usage_tag' = $1 AND id <> $2 ORDER BY id`
	case "rest":
		query = `SELECT id, jsonb_build_object('openai_gwpool_rest_state', extra->'openai_gwpool_rest_state')
			FROM accounts WHERE deleted_at IS NULL AND extra->>'openai_gwpool_rest_tag' = $1 AND id <> $2 ORDER BY id`
	case "contacts":
		query = `SELECT id, jsonb_build_object('openai_gwpool_contacts', extra->'openai_gwpool_contacts')
			FROM accounts WHERE deleted_at IS NULL AND extra->>'openai_gwpool_ledger_tag' = $1 AND id <> $2 ORDER BY id`
	default:
		return nil, fmt.Errorf("unsupported gateway-pool state kind %q", kind)
	}
	exec := r.sql
	if tx := dbent.TxFromContext(ctx); tx != nil {
		exec = tx.Client()
	}
	rows, err := exec.QueryContext(ctx, query, tag, exclude)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var peers []service.Account
	for rows.Next() {
		var account service.Account
		var extra []byte
		if err := rows.Scan(&account.ID, &extra); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(extra, &account.Extra); err != nil {
			return nil, err
		}
		peers = append(peers, account)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return peers, nil
}

// UpdateGatewayPoolRuntimeExtra is UpdateExtra's scheduler-neutral merge
// without the per-write snapshot sync. Only gateway-pool runtime ledgers come
// here; their readers load the row from the database, never the snapshot.
func (r *accountRepository) UpdateGatewayPoolRuntimeExtra(ctx context.Context, id int64, updates map[string]any) error {
	payload, err := json.Marshal(updates)
	if err != nil {
		return err
	}
	result, err := clientFromContext(ctx, r.client).ExecContext(ctx,
		"UPDATE accounts SET extra = COALESCE(extra, '{}'::jsonb) || $1::jsonb, updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL",
		string(payload), id)
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
