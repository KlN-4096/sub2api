package service

import "context"

// gatewayPoolHistoryPeers returns the rows sharing a ledger tag with the named
// ledgers; callers that only need the rows name none.
func (s *OpenAIGatewayService) gatewayPoolHistoryPeers(ctx context.Context, tag string, ledgers ...string) ([]Account, error) {
	peers, err := s.accountRepo.FindByExtraField(ctx, openAIGatewayLedgerTagExtraKey, tag)
	if err != nil {
		return nil, err
	}
	if err := s.hydrateGatewayPoolLedgers(ctx, gatewayPoolRowPointers(peers), ledgers...); err != nil {
		return nil, err
	}
	return peers, nil
}
