package service

import "net/http"

func (s *OpenAIGatewayService) SetPluginManager(manager *PluginManager) {
	s.pluginManager = manager
}

// doOpenAIUpstream 只在 OpenAI OAuth 能力绑定已启用时把真实请求交给插件。
// 插件返回标准 http.Response，响应解析、错误映射、SSE 和计费仍由现有核心链处理。
func (s *OpenAIGatewayService) doOpenAIUpstream(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	if err := requireOpenAIProxyBinding(account, proxyURL); err != nil {
		return nil, err
	}
	// ChatGPT cookie 回放（openai_codex_cookies.go）：出站前带上该账号罐里的 cookie，拿到响应
	// 后收 Set-Cookie。插件路径与直连路径都经过这里，两条路一致。
	rawURL := ""
	if request != nil && request.URL != nil {
		rawURL = request.URL.String()
	}
	s.codexCookies.Attach(account, rawURL, request.Header)
	resp, err := s.doOpenAIUpstreamRoundTrip(request, proxyURL, account)
	if err == nil && resp != nil {
		s.codexCookies.Store(account, rawURL, resp.Header)
	}
	return resp, err
}

func (s *OpenAIGatewayService) doOpenAIUpstreamRoundTrip(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	if s.pluginManager != nil {
		response, handled, err := s.pluginManager.RoundTripOpenAIOAuth(request.Context(), request, proxyURL, account)
		if handled {
			return response, err
		}
	}
	return s.httpUpstream.Do(request, proxyURL, account.ID, account.Concurrency)
}

// doOpenAIAccountTestUpstream 让 OpenAI OAuth 账号测试与真实转发使用同一插件路径。
// API Key 和未命中插件的账号保持各自原有的 HTTPUpstream 行为。
func (s *AccountTestService) doOpenAIAccountTestUpstream(
	request *http.Request,
	proxyURL string,
	account *Account,
	useTLSFallback bool,
) (*http.Response, error) {
	if err := requireOpenAIProxyBinding(account, proxyURL); err != nil {
		return nil, err
	}
	if s.pluginManager != nil {
		response, handled, err := s.pluginManager.RoundTripOpenAIOAuth(request.Context(), request, proxyURL, account)
		if handled {
			return response, err
		}
	}
	if useTLSFallback {
		return s.httpUpstream.DoWithTLS(
			request,
			proxyURL,
			account.ID,
			account.Concurrency,
			s.tlsFPProfileService.ResolveTLSProfile(account),
		)
	}
	return s.httpUpstream.Do(request, proxyURL, account.ID, account.Concurrency)
}
