package repository

import (
	"context"
	"encoding/json"
	"fmt"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// FindGatewayPoolStatePeers reads only the state merged by usage/rest ledgers.
// No credentials, unrelated extra, proxies or group edges are materialized.
func (r *accountRepository) FindGatewayPoolStatePeers(ctx context.Context, tag, kind string) ([]service.Account, error) {
	var query string
	switch kind {
	case "usage":
		query = `SELECT id, jsonb_build_object(
			'openai_gwpool_usage_rounds', extra->'openai_gwpool_usage_rounds',
			'openai_gwpool_usage_blocked_at', extra->'openai_gwpool_usage_blocked_at')
			FROM accounts WHERE deleted_at IS NULL AND
			(extra->>'openai_gwpool_usage_tag' = $1 OR extra->>'openai_gwpool_usage_previous_tag' = $1)
			ORDER BY CASE WHEN extra->>'openai_gwpool_usage_tag' = $1 THEN 0 ELSE 1 END`
	case "rest":
		query = `SELECT id, jsonb_build_object('openai_gwpool_rest_state', extra->'openai_gwpool_rest_state')
			FROM accounts WHERE deleted_at IS NULL AND
			(extra->>'openai_gwpool_rest_tag' = $1 OR extra->>'openai_gwpool_rest_previous_tag' = $1)
			ORDER BY CASE WHEN extra->>'openai_gwpool_rest_tag' = $1 THEN 0 ELSE 1 END`
	default:
		return nil, fmt.Errorf("unsupported gateway-pool state kind %q", kind)
	}
	exec := r.sql
	if tx := dbent.TxFromContext(ctx); tx != nil {
		exec = tx.Client()
	}
	rows, err := exec.QueryContext(ctx, query, tag)
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
