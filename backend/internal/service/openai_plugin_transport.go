package service

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/chatgptcookies"
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
	// 业务请求只落在**验过满血**的槽上。判据跑在这一发之前、用便宜的垫话，所以用户的请求不会是
	// 那个去试网关的人（openai_gwpool_warm.go）。验不出来就把错误往上抛，**绝不降级放行**。
	// 显式关闭防护时跳过质量验证；取票、冷却和限流仍在后续路径生效。
	if err := s.gatewayPoolWarmUp(request, proxyURL, account); err != nil {
		return nil, err
	}
	resp, degraded, err := s.doOpenAIUpstreamOnce(request, proxyURL, account)
	if !degraded {
		return resp, err
	}
	// 判到降智就**只截断**，不在这里换票重发：重发走 AttachRoute 换一张没验过的票就把用户的
	// prompt 打出去，而判据对首轮请求结构性失效（没送 turn-state ⇒ 没有回声），所以「客户端
	// 无感」实际是「降智静默交付」。
	// 当前 pair 已标 Stale ⇒ 下一发客户端请求的 AttachRoute 自然带 force=1 换网关。
	s.dropDegradedGatewayPoolRoute(request, resp, account)
	return nil, errOpenAIGatewayPoolRouteDegraded
}

// doOpenAIUpstreamOnce 发一发上游，并把 state-echo 判据跑在 Store **之前**。
//
// 顺序是承重的：判到降智这张票就不要了，再去 Store 它的 Set-Cookie 等于给一条已经废掉的
// 路由延命。
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
	applied := openAIGatewayPoolSinkFrom(request.Context()).snapshot()
	poolRequest := s.codexCookies.gatewayPoolTakeover(account) && request.URL != nil &&
		chatgptcookies.IsChatGPTURL(request.URL) && request.URL.Path == openAIGatewayPoolInferencePath
	var identity string
	var identityErr error
	if poolRequest && applied.AccountID == account.ID && applied.Version != "" {
		identity, identityErr = s.codexCookies.gatewayPoolIdentity(request.Context(), account)
	}
	if poolRequest && account.gatewayPoolGuardEnabled() {
		// WarmUp 与实际发送之间可能过期/换票。只检查本次真正附带的那一张，
		// 不允许“旧票验证成功，新票直接发业务”的竞态穿过严格模式。
		mark, verified := s.codexCookies.gatewayPoolVerifiedMarkOf(identity)
		if identityErr != nil || applied.AccountID != account.ID || applied.Version == "" ||
			!verified || mark.version != applied.Version || !s.codexCookies.gatewayPoolVerifiedFull(identity) {
			gatewayPoolReleaseUnsent(release)
			return nil, false, errOpenAIGatewayPoolWarmUnverified
		}
	}
	if identityErr != nil {
		slog.Warn("gwpool_first_send_identity_unavailable", "account_id", account.ID)
	}
	resp, sentAt, err := s.gatewayPoolObservedRoundTrip(request, proxyURL, account, poolRequest)
	if !sentAt.IsZero() {
		s.codexCookies.gatewayPoolMarkSent(identity, applied.Version, sentAt)
	}
	if err == nil && resp != nil {
		degraded := s.gatewayPoolRouteDegraded(request, resp, account)
		s.noteGatewayPoolBusinessContact(request, account, identity, applied, sentAt)
		if degraded {
			return resp, true, nil
		}
		s.codexCookies.Store(account, rawURL, resp.Header)
		return resp, false, nil
	}
	if gatewayPoolReleasesUnsent(resp, err) {
		gatewayPoolReleaseUnsent(release)
	} else if !sentAt.IsZero() {
		s.noteGatewayPoolBusinessContact(request, account, identity, applied, sentAt)
	}
	return resp, false, err
}

// Not being safe to return a ticket is not evidence of a send. Native HTTP
// records a completed request write; plugins may explicitly attest RequestSent.
// A response itself is sufficient evidence when the transport has no trace.
func (s *OpenAIGatewayService) gatewayPoolObservedRoundTrip(request *http.Request, proxyURL string, account *Account, observe bool) (*http.Response, time.Time, error) {
	if !observe {
		resp, err := s.doOpenAIUpstreamRoundTrip(request, proxyURL, account)
		return resp, time.Time{}, err
	}
	started := time.Now()
	var wroteAt atomic.Int64
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			wroteAt.CompareAndSwap(0, time.Now().UnixNano())
		}
	}}
	traced := request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	resp, err := s.doOpenAIUpstreamRoundTrip(traced, proxyURL, account)
	if at := wroteAt.Load(); at != 0 {
		return resp, time.Unix(0, at), err
	}
	var pluginErr *PluginTransportError
	if resp != nil || (errors.As(err, &pluginErr) && pluginErr.RequestSent) {
		return resp, started, err
	}
	return resp, time.Time{}, err
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
