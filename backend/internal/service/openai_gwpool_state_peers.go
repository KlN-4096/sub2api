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
	key := gatewayPoolUsageTagKey
	if kind == "rest" {
		key = gatewayPoolRestTagKey
	}
	return s.accountRepo.FindByExtraField(ctx, key, tag)
}
