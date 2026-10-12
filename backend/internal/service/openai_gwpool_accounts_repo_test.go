package service

import (
	"context"
	"encoding/json"
	"maps"
)

// Independent row snapshots model the shared credential ledger without sharing
// mutable Account maps between concurrently running requests.
type gatewayPoolAccountsRepo struct {
	AccountRepository
	rows map[int64]*gatewayRuntimeRepo
}

func (r *gatewayPoolAccountsRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	return r.rows[id].GetByID(ctx, id)
}

func (r *gatewayPoolAccountsRepo) UpdateExtra(ctx context.Context, id int64, patch map[string]any) error {
	return r.rows[id].UpdateExtra(ctx, id, patch)
}

func (r *gatewayPoolAccountsRepo) FindByExtraField(ctx context.Context, key string, value any) ([]Account, error) {
	var accounts []Account
	for id := range r.rows {
		account, err := r.GetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if account.Extra[key] == value {
			accounts = append(accounts, *account)
		}
	}
	return accounts, nil
}

func (r *gatewayPoolAccountsRepo) stored(id int64) (*Account, error) {
	return r.rows[id].stored(id)
}

func (r *gatewayPoolAccountsRepo) LoadGatewayPoolLedgers(ctx context.Context, ids []int64, keys []string) (map[int64]map[string]json.RawMessage, error) {
	ledgers := map[int64]map[string]json.RawMessage{}
	for _, id := range ids {
		if row := r.rows[id]; row != nil {
			loaded, err := row.LoadGatewayPoolLedgers(ctx, []int64{id}, keys)
			if err != nil {
				return nil, err
			}
			maps.Copy(ledgers, loaded)
		}
	}
	return ledgers, nil
}

func (r *gatewayPoolAccountsRepo) LoadGatewayPoolLedgerWithout(ctx context.Context, id int64, kind string, omit ...string) (json.RawMessage, error) {
	if row := r.rows[id]; row != nil {
		return row.LoadGatewayPoolLedgerWithout(ctx, id, kind, omit...)
	}
	return nil, nil
}

func (r *gatewayPoolAccountsRepo) WriteGatewayPoolLedgers(ctx context.Context, id int64, ledgers, tags map[string]any) error {
	return r.rows[id].WriteGatewayPoolLedgers(ctx, id, ledgers, tags)
}
