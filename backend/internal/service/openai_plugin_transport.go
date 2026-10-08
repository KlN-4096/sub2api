package service

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync"
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
func (s *OpenAIGatewayService) doOpenAIUpstreamAttempt(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	if request != nil {
		if failure := GatewayPoolRetryFailure(request.Context()); failure != nil {
			return nil, failure
		}
	}
	if request != nil && request.URL != nil && chatgptcookies.IsChatGPTURL(request.URL) && request.URL.Path == openAIGatewayPoolInferencePath {
		if allowed, err := s.gatewayPoolResumeAllowed(request.Context(), account, true); err != nil || !allowed {
			return nil, gatewayPoolRestError()
		}
	}
	if request != nil {
		ctx := s.gatewayPoolWaitContext(request.Context(), account)
		request = request.WithContext(ctx)
	}
	// 业务请求只落在**验过满血**的槽上。判据跑在这一发之前、用便宜的垫话，所以用户的请求不会是
	// 那个去试网关的人（openai_gwpool_warm.go）。验不出来就把错误往上抛，**绝不降级放行**。
	var resp *http.Response
	var degraded bool
	var err error
	for {
		if err = s.gatewayPoolWarmUp(request, proxyURL, account); err != nil {
			return nil, err
		}
		if err = s.refreshGatewayPoolWaitingAuth(request, account); err != nil {
			return nil, err
		}
		resp, degraded, err = s.doOpenAIUpstreamOnce(request, proxyURL, account)
		if !errors.Is(err, errGatewayPoolPreflightChanged) || gatewayPoolWaitFrom(request.Context()) == nil {
			break
		}
		retry, waitErr := s.waitGatewayPoolRetry(request.Context(), account, gatewayPoolVerificationRetryGap)
		if waitErr != nil {
			return nil, waitErr
		}
		if !retry {
			break
		}
	}
	if !degraded {
		return resp, err
	}
	// End this attempt only. The request supervisor may replay the original
	// business on a newly verified ticket before exposing any response bytes.
	marked := s.dropDegradedGatewayPoolRoute(request, resp, account)
	return nil, gatewayPoolDegradedAttemptError(request.Context(), marked)
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
	release, err := s.attachGatewayPoolRouteWithWait(request.Context(), account, rawURL, request.Header)
	if err != nil {
		return nil, false, err
	}
	// 取到票之后、真正发出去之前，这一发还可能死在三处：客户端已经走了（ctx 取消）、
	// 上游主机校验否掉、取 HTTP 客户端/并发槽失败。它们都在 httpUpstream.Do 进 http.Client
	// **之前**，确证未发可本地丢弃独占新票（见 gatewayPoolDefinitelyUnsent）。
	if ctxErr := request.Context().Err(); ctxErr != nil {
		gatewayPoolCleanupUnsent(release)
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
	if poolRequest {
		// WarmUp 与实际发送之间可能过期/换票。只检查本次真正附带的那一张，
		// 不允许“旧票验证成功，新票直接发业务”的竞态穿过严格模式。
		mark, verified := s.codexCookies.gatewayPoolVerifiedMarkOf(identity)
		if identityErr != nil || applied.AccountID != account.ID || applied.Version == "" ||
			!verified || mark.version != applied.Version || !s.codexCookies.gatewayPoolVerifiedFull(identity) {
			gatewayPoolCleanupUnsent(release)
			return nil, false, errGatewayPoolPreflightChanged
		}
	}
	if identityErr != nil {
		slog.Warn("gwpool_first_send_identity_unavailable", "account_id", account.ID)
	}
	if identity != "" {
		finish := s.codexCookies.gatewayPoolInventoryOperation(identity)
		defer finish() // includes response classification and durable accounting
	}
	if failure := GatewayPoolRetryFailure(request.Context()); failure != nil {
		gatewayPoolCleanupUnsent(release)
		return nil, false, failure
	}
	if poolRequest {
		gatewayPoolUsageMarkSending(request.Context(), identity)
	}
	resp, sentAt, err := s.gatewayPoolObservedRoundTrip(request, proxyURL, account, poolRequest, func(at time.Time) {
		if !s.noteGatewayPoolFullUse(request.Context(), account, identity, applied, at) {
			s.noteGatewayPoolUsage(request.Context(), account, identity, gatewayPoolWarmModel(request), applied, at, false)
		}
	})
	if !sentAt.IsZero() {
		s.codexCookies.gatewayPoolMarkSent(identity, applied.Version, sentAt)
	}
	if err == nil && resp != nil {
		var degraded bool
		var verifyErr error
		if poolRequest {
			degraded, verifyErr = s.gatewayPoolRouteDegraded(request, resp, account, proxyURL, identity, sentAt)
		}
		s.noteGatewayPoolBusinessContact(request, account, identity, applied, sentAt, resp)
		if verifyErr != nil {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			return nil, false, verifyErr
		}
		if degraded {
			return resp, true, nil
		}
		if poolRequest {
			current, state := s.codexCookies.cachedPoolPair(identity)
			if state == openAIGatewayPoolPairNone || current.invalidated || current.version != applied.Version {
				openAIGatewayPoolSinkFrom(request.Context()).noteVerdict(applied.Gateway, "")
				if resp.Body != nil {
					_ = resp.Body.Close()
				}
				return nil, false, retryGatewayPoolAttempt(errOpenAIGatewayPoolWarmUnverified)
			}
		}
		if err := request.Context().Err(); err != nil {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			return nil, false, err
		}
		s.codexCookies.Store(account, rawURL, resp.Header)
		return resp, false, nil
	}
	if gatewayPoolDefinitelyUnsent(resp, err) {
		gatewayPoolCleanupUnsent(release)
	} else if !sentAt.IsZero() {
		s.noteGatewayPoolBusinessContact(request, account, identity, applied, sentAt)
	}
	return resp, false, err
}

// Unknown sending is not evidence of a send. Native HTTP
// records a completed request write; plugins may explicitly attest RequestSent.
// A response itself is sufficient evidence when the transport has no trace.
func (s *OpenAIGatewayService) gatewayPoolObservedRoundTrip(request *http.Request, proxyURL string, account *Account, observe bool, onSent ...func(time.Time)) (*http.Response, time.Time, error) {
	if !observe {
		resp, err := s.doOpenAIUpstreamRoundTrip(request, proxyURL, account)
		return resp, time.Time{}, err
	}
	if allowed, err := s.gatewayPoolResumeAllowed(request.Context(), account, false); err != nil || !allowed {
		return nil, time.Time{}, gatewayPoolRestError()
	}
	// Final business dispatch admission after fresh rest checks. A probe trace
	// denotes quality verification, including confirmation of an already-sent
	// successful response; the retry deadline must not truncate that response.
	if request.Context().Value(gatewayPoolProbeTraceKey{}) == nil {
		if failure := GatewayPoolRetryFailure(request.Context()); failure != nil {
			return nil, time.Time{}, failure
		}
	}
	started := time.Now()
	var wroteAt atomic.Int64
	var noted sync.Once
	note := func(at time.Time) {
		noted.Do(func() {
			for _, callback := range onSent {
				callback(at)
			}
		})
	}
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			at := time.Now()
			if wroteAt.CompareAndSwap(0, at.UnixNano()) {
				note(at)
			}
		}
	}}
	traced := request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	resp, err := s.doOpenAIUpstreamRoundTrip(traced, proxyURL, account)
	if at := wroteAt.Load(); at != 0 {
		note(time.Unix(0, at))
		return resp, time.Unix(0, at), err
	}
	var pluginErr *PluginTransportError
	if resp != nil || (errors.As(err, &pluginErr) && pluginErr.RequestSent) {
		note(started)
		return resp, started, err
	}
	return resp, time.Time{}, err
}

// gatewayPoolDefinitelyUnsent reports positive evidence that no bytes were sent.
//
// 两条出口都要判：doOpenAIUpstreamRoundTrip 先走插件（RoundTripOpenAIOAuth），没命中才走
// httpUpstream.Do。未知发送不能当作未发送，不能据此丢弃共享中的票。
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
//     烧掉的（docs/conventions/codex-full-strength-tickets.md），所以不能确认为未发送。
func gatewayPoolDefinitelyUnsent(resp *http.Response, err error) bool {
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

// gatewayPoolCleanupUnsent runs local CAS cleanup; nil means shared/cache reuse.
func gatewayPoolCleanupUnsent(cleanup func()) {
	if cleanup != nil {
		cleanup()
	}
}

func (s *OpenAIGatewayService) doOpenAIUpstreamRoundTrip(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	return s.recordOpenAIHTTP(account, request, "inference", func(request *http.Request) (*http.Response, error) {
		return s.doOpenAIUpstreamRoundTripUnrecorded(request, proxyURL, account)
	})
}

func (s *OpenAIGatewayService) doOpenAIUpstreamRoundTripUnrecorded(request *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	if request.Context().Value(gatewayPoolProbeTraceKey{}) == nil {
		// Fresh rest/deadline checks and the optional recording-policy read are
		// preparation, not upstream first-output time. A/B probes never arm it.
		startGatewayPoolFirstOutputGuard(request.Context())
	}
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
	return s.recordOpenAIHTTP(account, request, func(request *http.Request) (*http.Response, error) {
		return s.doOpenAIAccountTestUpstreamUnrecorded(request, proxyURL, account, useTLSFallback)
	})
}

func (s *AccountTestService) doOpenAIAccountTestUpstreamUnrecorded(
	request *http.Request, proxyURL string, account *Account, useTLSFallback bool,
) (*http.Response, error) {
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
