package service

import "context"

// Optional narrow projection. Keep this out of AccountRepository: lightweight
// repositories that embed that interface must not accidentally claim support.
type gatewayPoolStatePeerRepository interface {
	FindGatewayPoolStatePeers(context.Context, string, string) ([]Account, error)
}

func (s *OpenAIGatewayService) gatewayPoolStatePeers(ctx context.Context, tag, kind string) ([]Account, error) {
	if repo, ok := s.accountRepo.(gatewayPoolStatePeerRepository); ok {
		return repo.FindGatewayPoolStatePeers(ctx, tag, kind)
	}
	keys := []string{gatewayPoolUsageTagKey, gatewayPoolUsagePreviousTagKey}
	if kind == "rest" {
		keys = []string{gatewayPoolRestTagKey, gatewayPoolRestPreviousTagKey}
	}
	var peers []Account
	for _, key := range keys {
		rows, err := s.accountRepo.FindByExtraField(ctx, key, tag)
		if err != nil {
			return nil, err
		}
		peers = append(peers, rows...)
	}
	return peers, nil
}
