package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"
)

// Only an absent binding means direct access, including an explicitly persisted
// direct fallback. A failed lookup must not erase an existing proxy binding.
func resolveConfiguredProxyURL(ctx context.Context, repo ProxyRepository, proxyID *int64, loaded *Proxy) (string, error) {
	if proxyID == nil {
		return "", nil
	}
	proxy := loaded
	if proxy == nil {
		if repo == nil {
			return "", fmt.Errorf("configured proxy %d lookup is unavailable", *proxyID)
		}
		var err error
		proxy, err = repo.GetByID(ctx, *proxyID)
		if err != nil {
			return "", fmt.Errorf("configured proxy %d lookup failed: %w", *proxyID, err)
		}
	}
	if proxy == nil {
		return "", fmt.Errorf("configured proxy %d was not found", *proxyID)
	}
	if proxy.ID != 0 && proxy.ID != *proxyID {
		return "", fmt.Errorf("configured proxy %d does not match the loaded binding", *proxyID)
	}
	raw := proxy.URL()
	resolved, _, err := proxyurl.Parse(raw)
	if err != nil || resolved == "" {
		// Do not include parser errors or URLs: they may contain proxy credentials.
		return "", fmt.Errorf("configured proxy %d has an invalid URL", *proxyID)
	}
	// Validation must not change the URL presented to plugins/callers; transport
	// construction keeps owning normalization such as socks5 -> socks5h.
	return raw, nil
}

// Last-mile guard for OpenAI transports whose caller already resolved the URL.
func requireOpenAIProxyBinding(account *Account, proxyURL string) error {
	if account != nil && account.Platform == PlatformOpenAI && account.ProxyID != nil && strings.TrimSpace(proxyURL) == "" {
		return fmt.Errorf("configured proxy %d is unavailable", *account.ProxyID)
	}
	return nil
}
