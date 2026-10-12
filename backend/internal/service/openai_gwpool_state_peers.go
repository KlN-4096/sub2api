package service

import (
	"context"
	"slices"
)

// Optional narrow projection. Keep this out of AccountRepository: lightweight
// repositories that embed that interface must not accidentally claim support.
type gatewayPoolStatePeerRepository interface {
	FindGatewayPoolStatePeers(context.Context, string, string, int64) ([]Account, error)
}

// gatewayPoolStatePeers returns the rows of one usage/rest/contacts ledger.
// The narrow projection skips row exclude (0 keeps every row); the fallback
// scan does not, so callers that pass exclude still skip their own row.
func (s *OpenAIGatewayService) gatewayPoolStatePeers(ctx context.Context, tag, kind string, exclude int64) ([]Account, error) {
	if repo, ok := s.accountRepo.(gatewayPoolStatePeerRepository); ok {
		return repo.FindGatewayPoolStatePeers(ctx, tag, kind, exclude)
	}
	key := gatewayPoolUsageTagKey
	switch kind {
	case "rest":
		key = gatewayPoolRestTagKey
	case "contacts":
		key = openAIGatewayLedgerTagExtraKey
	}
	return s.accountRepo.FindByExtraField(ctx, key, tag)
}

// Optional like gatewayPoolRestRepository: merges extra without syncing the
// scheduler snapshot on every write.
type gatewayPoolRuntimeExtraRepository interface {
	UpdateGatewayPoolRuntimeExtra(context.Context, int64, map[string]any) error
}

// updateGatewayPoolRuntimeExtra stores a gateway-pool runtime ledger write.
// Every reader of these keys loads the row from the database, so the snapshot
// copy may lag until the next other write or full rebuild. A patch holding
// any other key keeps UpdateExtra and its snapshot sync.
func (s *OpenAIGatewayService) updateGatewayPoolRuntimeExtra(ctx context.Context, id int64, patch map[string]any) error {
	repo, ok := s.accountRepo.(gatewayPoolRuntimeExtraRepository)
	for key := range patch {
		ok = ok && slices.Contains(gatewayPoolRuntimeExtraKeys, key)
	}
	if ok {
		return repo.UpdateGatewayPoolRuntimeExtra(ctx, id, patch)
	}
	return s.accountRepo.UpdateExtra(ctx, id, patch)
}
