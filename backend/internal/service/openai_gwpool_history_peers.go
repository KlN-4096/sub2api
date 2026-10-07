package service

import "context"

func (s *OpenAIGatewayService) gatewayPoolHistoryPeers(ctx context.Context, tag string) ([]Account, error) {
	accounts, err := s.accountRepo.FindByExtraField(ctx, openAIGatewayLedgerTagExtraKey, tag)
	if err != nil {
		return nil, err
	}
	previous, err := s.accountRepo.FindByExtraField(ctx, openAIGatewayPreviousLedgerTagExtraKey, tag)
	if err != nil {
		return nil, err
	}
	seen := make(map[int64]bool, len(accounts))
	for _, account := range accounts {
		seen[account.ID] = true
	}
	for _, account := range previous {
		if !seen[account.ID] {
			accounts = append(accounts, account)
			seen[account.ID] = true
		}
	}
	return accounts, nil
}
