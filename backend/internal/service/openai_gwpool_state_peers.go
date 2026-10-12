package service

import "context"

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
	key, ledger := gatewayPoolUsageTagKey, gatewayPoolUsageExtraKey
	switch kind {
	case "rest":
		key, ledger = gatewayPoolRestTagKey, ""
	case "contacts":
		key, ledger = openAIGatewayLedgerTagExtraKey, openAIGatewayPoolContactsExtraKey
	}
	rows, err := s.accountRepo.FindByExtraField(ctx, key, tag)
	if err != nil || ledger == "" {
		return rows, err
	}
	return rows, s.hydrateGatewayPoolLedgers(ctx, gatewayPoolRowPointers(rows), ledger)
}
