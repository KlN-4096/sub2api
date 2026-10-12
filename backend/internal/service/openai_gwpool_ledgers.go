package service

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
)

// gatewayPoolLedgerKeys are the runtime ledgers kept in gateway_pool_ledgers
// (migration 253) instead of accounts.extra. Readers still find them in Extra
// under these keys, but only on rows hydrated with hydrateGatewayPoolLedgers.
var gatewayPoolLedgerKeys = []string{
	openAIGatewayHistoryExtraKey, openAIGatewayPoolContactsExtraKey, openAIGatewayPoolOutboxExtraKey, gatewayPoolUsageExtraKey,
}

// Optional like gatewayPoolStatePeerRepository: repositories without it keep
// the ledgers in extra.
type gatewayPoolLedgerRepository interface {
	LoadGatewayPoolLedgers(ctx context.Context, ids []int64, keys []string) (map[int64]map[string]json.RawMessage, error)
	LoadGatewayPoolLedgerWithout(ctx context.Context, id int64, kind string, omit ...string) (json.RawMessage, error)
	WriteGatewayPoolLedgers(ctx context.Context, id int64, ledgers, tags map[string]any) error
}

// The gateway pool finds its optional repository interfaces by type assertion
// and quietly takes the fallback without one. These names let the repository
// assert at compile time that it still implements every one of them.
type (
	GatewayPoolLedgerRepository    = gatewayPoolLedgerRepository
	GatewayPoolStatePeerRepository = gatewayPoolStatePeerRepository
	GatewayPoolRestRepository      = gatewayPoolRestRepository
	GatewayPoolClearRestRepository = gatewayPoolClearRestRepository
)

// hydrateGatewayPoolLedgers replaces the named ledgers in each row's Extra with
// the stored ones; a row without one gets none. Each row gets its own Extra map.
func (s *OpenAIGatewayService) hydrateGatewayPoolLedgers(ctx context.Context, accounts []*Account, keys ...string) error {
	repo, ok := s.accountRepo.(gatewayPoolLedgerRepository)
	if !ok || len(accounts) == 0 || len(keys) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(accounts))
	for _, account := range accounts {
		ids = append(ids, account.ID)
	}
	loaded, err := repo.LoadGatewayPoolLedgers(ctx, ids, keys)
	if err != nil {
		return err
	}
	for _, account := range accounts {
		extra := maps.Clone(account.Extra)
		if extra == nil {
			extra = map[string]any{}
		}
		for _, key := range keys {
			delete(extra, key)
			if raw, found := loaded[account.ID][key]; found {
				extra[key] = raw
			}
		}
		account.Extra = extra
	}
	return nil
}

func gatewayPoolRowPointers(rows []Account) []*Account {
	pointers := make([]*Account, len(rows))
	for i := range rows {
		pointers[i] = &rows[i]
	}
	return pointers
}

// gatewayPoolLedgerRow is GetByID plus the named ledgers.
func (s *OpenAIGatewayService) gatewayPoolLedgerRow(ctx context.Context, id int64, keys ...string) (*Account, error) {
	account, err := s.accountRepo.GetByID(ctx, id)
	if err != nil || account == nil {
		return account, err
	}
	return account, s.hydrateGatewayPoolLedgers(ctx, []*Account{account}, keys...)
}

// updateGatewayPoolRuntimeExtra stores a gateway-pool runtime ledger write:
// the ledgers go to gateway_pool_ledgers and the rest of the patch to extra, in
// one statement. Callers pass at least one ledger and nothing but its discovery
// tag besides: the extra merge skips UpdateExtra's scheduler sync.
func (s *OpenAIGatewayService) updateGatewayPoolRuntimeExtra(ctx context.Context, id int64, patch map[string]any) error {
	repo, ok := s.accountRepo.(gatewayPoolLedgerRepository)
	if !ok {
		return s.accountRepo.UpdateExtra(ctx, id, patch)
	}
	ledgers, tags := map[string]any{}, map[string]any{}
	for key, value := range patch {
		if slices.Contains(gatewayPoolLedgerKeys, key) {
			ledgers[key] = value
		} else {
			tags[key] = value
		}
	}
	return repo.WriteGatewayPoolLedgers(ctx, id, ledgers, tags)
}
