package service

import "context"

func (s *OpenAIGatewayService) gatewayPoolHistoryPeers(ctx context.Context, tag string) ([]Account, error) {
	return s.accountRepo.FindByExtraField(ctx, openAIGatewayLedgerTagExtraKey, tag)
}
