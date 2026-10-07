package service

import (
	"context"
	"errors"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// openAITransportErrorTempUnschedDuration is how long an account is temporarily
// unscheduled after a durable transport failure (matches tokenRefreshTempUnschedDuration).
const openAITransportErrorTempUnschedDuration = 10 * time.Minute

// openAITransportFailoverBody is the OpenAI-format error body attached to the
// failover error for a transport-level failure. Kept identical to the legacy
// inline 502 body so the client-visible payload is unchanged if failover is
// ultimately exhausted.
var openAITransportFailoverBody = []byte(`{"error":{"type":"upstream_error","message":"Upstream request failed"}}`)

// upstreamTransportErrorClass describes how to react to a transport-level upstream
// failure — i.e. the HTTP round-trip never completed (proxy / DNS / TCP / TLS
// error, no HTTP status code received).
type upstreamTransportErrorClass struct {
	// Persistent marks failures where retrying the same proxy/account is
	// pointless: expired or rejected proxy credentials, a dead proxy endpoint,
	// or DNS/routing failure. Such accounts should be temporarily unscheduled
	// (and alerted on) instead of being repeatedly scheduled into hard failures.
	Persistent bool
}

// persistentUpstreamTransportErrorMarkers are substrings (matched case-insensitively
// against the raw transport error) that indicate a durable proxy/network fault.
// Matched signals are intentionally specific failure *reasons*, not the operation
// (e.g. we match "connection refused", not "proxyconnect") so that a transient
// failure of the same operation (a proxy timeout) is NOT misclassified as durable.
var persistentUpstreamTransportErrorMarkers = []string{
	"authentication failed",         // SOCKS5 RFC1929 / proxy credentials rejected (expired account)
	"proxy authentication required", // HTTP proxy 407
	"connection refused",            // proxy/upstream endpoint down
	"no route to host",
	"network is unreachable",
	"no such host", // DNS resolution failure (bad/expired proxy hostname)
}

// classifyUpstreamTransportError decides whether a transport-level upstream error
// is durable (Persistent — evict the account + alert) or a transient blip
// (fail over to a healthy account but keep this one schedulable).
//
// Motivating incident: a SOCKS5 proxy whose subscription lapsed returned
// `username/password authentication failed`; the account was nonetheless
// rescheduled on every request, hard-failing users with 502s.
//
// Classification strategy (mirrors sanitizeStreamError in gateway_service.go):
//  1. Typed-error checks first (syscall constants, *net.DNSError) — portable and
//     unambiguous.
//  2. String-marker fallback for errors that have no typed form (e.g. the plain
//     string returned by golang.org/x/net/proxy for SOCKS5 credential rejection).
//     The network-layer string markers ("connection refused", "no route to host",
//     "network is unreachable", "no such host") are kept as a cross-platform safety
//     net even though the typed checks should cover them on modern Go+Linux.
func classifyUpstreamTransportError(err error) upstreamTransportErrorClass {
	if err == nil {
		return upstreamTransportErrorClass{}
	}

	// 网关池这一侧的失败不是这个账号的代理/网络故障：重启池子、改端口、容器没起都是日常操作，
	// 而它们的报错字面（connection refused / no such host）和真实代理死掉一模一样，照字符串判就会
	// 把一批真账号按「代理持久故障」停调度 10 分钟并发告警。失败仍然 failover、仍然记 Ops 错误，
	// 只是不摘账号（openai_gwpool.go / pkg/gwpool）。
	if errors.Is(err, gwpool.ErrPool) || errors.Is(err, ErrGatewayPoolWSIncompatible) {
		return upstreamTransportErrorClass{}
	}

	// — Typed checks (preferred) ——————————————————————————————————————————————
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) {
		return upstreamTransportErrorClass{Persistent: true}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return upstreamTransportErrorClass{Persistent: true}
	}

	// — String-marker fallback ————————————————————————————————————————————————
	msg := strings.ToLower(err.Error())
	for _, marker := range persistentUpstreamTransportErrorMarkers {
		if strings.Contains(msg, marker) {
			return upstreamTransportErrorClass{Persistent: true}
		}
	}
	return upstreamTransportErrorClass{}
}

// isClientCanceledTransportError reports whether a transport-level failure was
// caused by the client disconnecting: the request context itself is canceled
// and the round-trip aborted with context.Canceled. Such a failure says nothing
// about the upstream, so it is not recorded as an Ops upstream error event.
func isClientCanceledTransportError(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) && ctx != nil && errors.Is(ctx.Err(), context.Canceled)
}

// handleOpenAIUpstreamTransportError handles a transport-level upstream failure
// (Do/DoWithTLS returned a non-HTTP error: proxy/DNS/TCP/TLS). It:
//  1. records the failure in Ops error logs (status 0, kind=request_error),
//     except when the client disconnected (see isClientCanceledTransportError);
//  2. for durable faults (expired/rejected proxy creds, dead proxy, DNS/routing)
//     temporarily unschedules the account (DB + in-memory) and logs a stable
//     warn event that alert rules can key on;
//  3. returns an error that is *UpstreamFailoverError (so the handler fails over
//     to a healthy account) for all non-canceled errors, or a plain error for
//     context.Canceled (client gone — no failover, no eviction).
//
// It deliberately does NOT write to the response: the handler owns the response
// (failover, or a protocol-correct error once failover is exhausted).
//
// passthrough tags the Ops error event for the OpenAI passthrough forward path.
func (s *OpenAIGatewayService) handleOpenAIUpstreamTransportError(ctx context.Context, c *gin.Context, account *Account, err error, passthrough bool) error {
	if isClientCanceledTransportError(ctx, err) {
		return err
	}
	var rejectedRetry *UpstreamFailoverError
	if errors.As(err, &rejectedRetry) {
		return rejectedRetry // pre-dispatch retry expiry is not a transport/account fault
	}
	safeErr := sanitizeUpstreamErrorMessage(err.Error())
	setOpsUpstreamError(c, 0, safeErr, "")
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		ProxyID:            opsUpstreamProxyID(account),
		ProxyName:          opsUpstreamProxyName(account),
		Platform:           account.Platform,
		AccountID:          account.ID,
		AccountName:        account.Name,
		UpstreamStatusCode: 0,
		Passthrough:        passthrough,
		Kind:               "request_error",
		Message:            safeErr,
	})

	// Client disconnected: do NOT fail over to another account and do NOT evict
	// this one — the upstream never had a chance to exhibit a fault.
	if errors.Is(err, context.Canceled) || (errors.Is(err, context.DeadlineExceeded) && errors.Is(ctx.Err(), context.DeadlineExceeded)) {
		return err
	}

	// Transport attempt reached the network path; count as Ollama Cloud / OpenCode Go activity.
	if s != nil {
		scheduleOllamaCloudUsageActivity(s.deferredService, account)
		scheduleOpenCodeGoUsageActivity(s.deferredService, account)
	}

	// 插件已把请求交给上游时，自动切换账号可能造成重复扣费或重复执行。
	var pluginErr *PluginTransportError
	if errors.As(err, &pluginErr) && pluginErr.RequestSent {
		return err
	}

	if classifyUpstreamTransportError(err).Persistent {
		s.tempUnscheduleOpenAITransportError(ctx, account, safeErr)
	}

	out := &UpstreamFailoverError{
		StatusCode:   http.StatusBadGateway,
		ResponseBody: openAITransportFailoverBody,
	}
	// 判降智**不换账号**：这是**路由**问题不是账号问题，换个账号换不出满血路由。
	//
	// 不拦的话放大系数在 handler 层：换账号上限 maxAccountSwitches 默认 10，而内层
	// degraded_retries 封顶 1 ⇒ 一次客户端请求最坏 2×(1+10) = 22 发真实上游，各烧一张
	// pair 和一个 (上游账号 × 网关) 单位——而 pair 按文档是个位数张/小时。
	// degraded_retries 的常量注释写着「放大系数必须封顶」，但真正的乘数在这儿，
	// 不设 Stop 的话那句话只封住了内层。
	// 同型先例：gatewayPoolRetriesBare 对 no_exit 也是「不值得换网关」。
	//
	// queue 档那两条同理，而且更凶：预热每换一个账号要重来一遍「最多 N 张票 × 2 发垫话」（N 默认 5、可配），
	// 不设 Stop 的话一次客户端请求最坏 11 个账号 × 5 张票 = 55 张票 —— 而再生预算约 25 张/小时。
	// 「读不出模型」换账号理论上有用（换到一个没开 device 收敛的号就读得出来了），但那个代价
	// 不值得：文案已经直接告诉运营方该换档还是升客户端。
	if errors.Is(err, errOpenAIGatewayPoolRouteDegraded) ||
		errors.Is(err, errOpenAIGatewayPoolWarmExhausted) ||
		errors.Is(err, errOpenAIGatewayPoolWarmNoModel) {
		out.NextAccountAction = NextAccountStop
	}
	// 把池子那三条双语说明交到客户端手里。不填的话 handler 的分支全不命中，最后落到
	// mapUpstreamError(502) 的通用文案「Upstream request failed」——而「池子没票，等会儿
	// 再试」和「池子地址配错了，去改配置」对使用者要做的事完全相反。
	// Reason 同时让 ShouldReportAccountScheduleFailure 放过这个账号：池子挂了不是它的错，
	// 与 classifyUpstreamTransportError 对 gwpool.ErrPool 的豁免同一个道理。
	if msg := gatewayPoolClientMessage(err); msg != "" {
		out.Reason = OpenAIGatewayPoolReason
		out.GatewayPoolRotation = gatewayPoolRotationFailure(err)
		// Pool-specific rotation is opt-in for both ends. Only the handler,
		// after checking replay safety and fresh source settings, can reopen it.
		out.NextAccountAction = NextAccountStop
		out.ClientMessage = msg
		out.ClientStatusCode = http.StatusServiceUnavailable
		// 带上 Retry-After：handler 的 copyFailoverRetryAfter 从 ResponseHeaders 里取它。
		// 池子这边的失败都是秒级返回的 503，而 Codex CLI 对 503 立刻重发 ⇒ 不报这个数就是
		// 一个纯热循环，每一轮还可能再烧几张票（见 gatewayPoolRetryAfter 的现场数据）。
		if retry := gatewayPoolRetryAfter(err); retry > 0 {
			out.ResponseHeaders = http.Header{
				"Retry-After": []string{strconv.Itoa(int(math.Ceil(retry.Seconds())))},
			}
		}
	}
	return out
}

// tempUnscheduleOpenAITransportError marks an account temporarily unschedulable
// after a durable transport failure, both persistently (DB, survives restart)
// and in-memory (immediate scheduler effect before the DB/account cache propagates).
//
// Log semantics:
//   - "openai.account_temp_unscheduled_transport" — emitted ONLY after a
//     successful DB write (both in-memory + persisted).
//   - "openai.account_temp_unscheduled_transport_memory_only" — emitted when
//     accountRepo is nil (in-memory only; no persistence).
//   - "openai.account_temp_unscheduled_transport_failed" — DB write attempted
//     but returned an error.
func (s *OpenAIGatewayService) tempUnscheduleOpenAITransportError(ctx context.Context, account *Account, safeErr string) {
	if s == nil || account == nil {
		return
	}
	until := time.Now().Add(openAITransportErrorTempUnschedDuration)
	reason := "upstream transport error (proxy/network): " + safeErr

	// Immediate in-memory block so this process skips the account until the
	// persisted cooldown is visible on the scheduling Account. Selection is
	// fail-open: empty snapshot/DB cooldown fields drop a stale local block.
	s.BlockAccountScheduling(account, until, "transport_error")

	if s.accountRepo == nil {
		// No DB configured — block is in-memory only; emit a distinct event so
		// operators are not misled into thinking the block survived a restart.
		logger.L().With(zap.String("component", "service.openai_gateway")).Warn(
			"openai.account_temp_unscheduled_transport_memory_only",
			zap.Int64("account_id", account.ID),
			zap.String("account_name", account.Name),
			zap.String("platform", account.Platform),
			zap.Time("until", until),
			zap.String("reason", reason),
		)
		return
	}

	bgCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openAIAccountStateUpdateTimeout)
	defer cancel()
	if err := s.accountRepo.SetTempUnschedulable(bgCtx, account.ID, until, reason); err != nil {
		logger.L().With(zap.String("component", "service.openai_gateway")).Warn(
			"openai.account_temp_unscheduled_transport_failed",
			zap.Int64("account_id", account.ID),
			zap.Error(err),
		)
		return
	}

	// DB write succeeded: both in-memory and persisted.
	logger.L().With(zap.String("component", "service.openai_gateway")).Warn(
		"openai.account_temp_unscheduled_transport",
		zap.Int64("account_id", account.ID),
		zap.String("account_name", account.Name),
		zap.String("platform", account.Platform),
		zap.Time("until", until),
		zap.String("reason", reason),
	)
}
