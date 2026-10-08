package service

import "context"

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
