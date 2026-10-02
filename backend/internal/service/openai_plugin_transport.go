package service

import (
	"context"
	"errors"
	"net/http"
	"net/url"
)

func (s *OpenAIGatewayService) SetPluginManager(manager *PluginManager) {
	s.pluginManager = manager
}

// doOpenAIUpstream 只在 OpenAI OAuth 能力绑定已启用时把真实请求交给插件。
// 插件返回标准 http.Response，响应解析、错误映射、SSE 和计费仍由现有核心链处理。
//
// 这里还夹着 state-echo 降智判据（openai_gwpool_state_echo.go）。判定点落在这一层是因为它同时
// 满足两个条件：响应头已经到手，而调用方还一个字节都没往下游写（调用方要等这个函数返回才开始
// 解析响应）。所以「截断」在这里是干净的，不会留一个半截的 SSE 流。
func (s *OpenAIGatewayService) doOpenAIUpstream(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	// 判据有假阳性（见 openai_gwpool_state_echo.go 的纪律 2）⇒ 换票重发的放大系数硬封顶在 1：
	// 重试那一发若又判降智，直接走错误路径，绝不再试。
	for remaining := account.gatewayPoolDegradedRetries(); ; remaining-- {
		resp, degraded, err := s.doOpenAIUpstreamOnce(request, proxyURL, account)
		if !degraded {
			return resp, err
		}
		// 换票重发要把同一个请求体再发一遍；重放不了（GetBody 为 nil）就退回「只截断」，
		// 绝不发一个半截的请求体。
		var replay *http.Request
		if remaining > 0 {
			replay = gatewayPoolReplayRequest(request)
		}
		s.dropDegradedGatewayPoolRoute(request, resp, account, replay != nil)
		if replay == nil {
			return nil, errOpenAIGatewayPoolRouteDegraded
		}
		// 当前 pair 已标 Stale ⇒ 下一圈的 AttachRoute 自然带 force=1 取一张别的网关的票。
		request = replay
	}
}

// doOpenAIUpstreamOnce 发一发上游，并把 state-echo 判据跑在 Store / 续期**之前**。
//
// 顺序是承重的：判到降智这张票就不要了，再去 Store 它的 Set-Cookie、拿它去池子续寿命都是在给
// 一条已经废掉的路由延命，而续期是异步的（gatewayPoolRenew 起 goroutine），会和随后的标 Stale
// 抢同一个缓存槽。
func (s *OpenAIGatewayService) doOpenAIUpstreamOnce(
	request *http.Request,
	proxyURL string,
	account *Account,
) (*http.Response, bool, error) {
	if err := requireOpenAIProxyBinding(account, proxyURL); err != nil {
		return nil, false, err
	}
	// ChatGPT cookie 回放（openai_codex_cookies.go）：出站前带上该账号罐里的 cookie，拿到响应
	// 后收 Set-Cookie。插件路径与直连路径都经过这里，两条路一致。
	// 网关池接管时这里换成池子下发的 pair（openai_gwpool.go）；池子没有满血槽位就报错，
	// 由调用方走既有失败路径，不回落罐回放。
	rawURL := ""
	if request != nil && request.URL != nil {
		rawURL = request.URL.String()
	}
	release, err := s.codexCookies.AttachRoute(request.Context(), account, rawURL, request.Header)
	if err != nil {
		return nil, false, err
	}
	// 取到票之后、真正发出去之前，这一发还可能死在三处：客户端已经走了（ctx 取消）、
	// 上游主机校验否掉、取 HTTP 客户端/并发槽失败。它们都在 httpUpstream.Do 进 http.Client
	// **之前**，一个字节都没出去 ⇒ 槽位还给池子（见 gatewayPoolReleasesUnsent）。
	if ctxErr := request.Context().Err(); ctxErr != nil {
		gatewayPoolReleaseUnsent(release)
		return nil, false, ctxErr
	}
	resp, err := s.doOpenAIUpstreamRoundTrip(request, proxyURL, account)
	if err == nil && resp != nil {
		if s.gatewayPoolRouteDegraded(request, resp, account) {
			return resp, true, nil
		}
		s.codexCookies.Store(account, rawURL, resp.Header)
		// 取票后的第一发（且票临期）会摘掉 __oailb 出站，换回来的那张新 __cflb 要回传池子续寿命。
		// 异步 + 自带 ctx，绝不拖业务响应；没抢到名额 / 上游没下发新两件时它什么都不做。
		s.codexCookies.gatewayPoolRenew(request.Context(), account, resp.Header)
		return resp, false, nil
	}
	if gatewayPoolReleasesUnsent(resp, err) {
		gatewayPoolReleaseUnsent(release)
	}
	return resp, false, err
}

// gatewayPoolReleasesUnsent 判「这一发一个字节都没发出去」，决定取到的池子票要不要还。
//
// 两条出口都要判：doOpenAIUpstreamRoundTrip 先走插件（RoundTripOpenAIOAuth），没命中才走
// httpUpstream.Do。错还的代价不是自损——池子会把一个其实烧过的槽位当新鲜的再发给别人。
//
// 判据只认**确证**，不猜：
//   - 拿到响应 ⇒ 确证发过了（而且满血窗口已经烧掉）。
//   - 插件自己用 *PluginTransportError.RequestSent 把这件事显式说了（plugin_runtime.go），
//     照 handleOpenAIUpstreamTransportError 的同一条判据读。它**没有 Unwrap**，所以下面那条
//     *url.Error 的规则看不见它 —— 这正是「返回裸 error 但其实已经发出去了」的第三类。
//   - ctx 已死时 normalizePluginRPCError 会提前返回裸 ctx.Err()，同样不是 *url.Error ⇒ 显式
//     判成「可能发过」。非插件路径的 ctx 取消本来就被 net/http 包成 *url.Error，加这条不改它。
//   - 没拿到响应，且错误不是上面那几类、也不是 *url.Error ⇒ 还没进 http.Client：httpUpstream.Do
//     里取客户端之前那几道（主机校验 validateRequestHost、取客户端/并发槽 acquireClientWithProfile）
//     返回的都是裸 error，而 client.Do 的失败一律被 net/http 包成 *url.Error ⇒ 确证没发。
//   - 没拿到响应但是 *url.Error ⇒ 传输跑过了，「连上没连上」分不出来。满血窗口是**首次接触**就
//     烧掉的（docs/conventions/codex-full-strength-tickets.md），所以这里**刻意不还**。
func gatewayPoolReleasesUnsent(resp *http.Response, err error) bool {
	if resp != nil || err == nil {
		return false
	}
	var pluginErr *PluginTransportError
	if errors.As(err, &pluginErr) {
		return !pluginErr.RequestSent
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var transport *url.Error
	return !errors.As(err, &transport)
}

// gatewayPoolReleaseUnsent 调一次还票闭包（nil = 这一发没自己取票，没什么可还）。
// 同步调：它自带超时与独立 ctx，而且不还就白丢一个槽位——不值得为它开一条 goroutine。
func gatewayPoolReleaseUnsent(release func()) {
	if release != nil {
		release()
	}
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
