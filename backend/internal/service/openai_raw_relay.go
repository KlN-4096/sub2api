package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 原样中继：sub2api 只管用户体系（鉴权、分组、额度、统计），请求按原字节交给上游，
// 响应的状态码/头/正文按原样回给客户端。cpr 账号恒走这里；OpenAI apikey 账号由
// extra.openai_raw_relay 打开（下一跳是另一个 sub2api 时用）。
//
// 与透传路径的区别：不做头白名单、兼容改写、错误脱敏、重试、熔断和账号状态变更。
// 只有上游「整体不可用」才换号（isOpenAIRawRelayUnavailable），换不了由 handler
// 原样写回最后一次的上游响应（UpstreamFailoverError.RawRelayResponse）。

const openAIRawRelayExtraKey = "openai_raw_relay"

// UsesOpenAIRawRelay 报告账号是否走原样中继。
func (a *Account) UsesOpenAIRawRelay() bool {
	return a.IsCPR() || (a.IsOpenAIApiKey() && a.getExtraBool(openAIRawRelayExtraKey))
}

// openAIRawRelayDroppedRequestHeaders 不随原样中继出站的入站头：逐跳头、本级重算的
// 长度/主机、本站的鉴权凭据，以及描述「客户端→本站」这一跳的代理转发头。
// accept-encoding 也不带：交给 Transport 自己协商并解压，SSE 才能边转发边记账。
var openAIRawRelayDroppedRequestHeaders = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-connection": true, "proxy-authorization": true,
	"te": true, "trailer": true, "transfer-encoding": true, "upgrade": true, "expect": true,
	"host": true, "content-length": true, "content-encoding": true, "accept-encoding": true,
	"authorization": true, "x-api-key": true, "x-goog-api-key": true, "cookie": true,
	"forwarded": true, "x-forwarded-for": true, "x-forwarded-host": true, "x-forwarded-proto": true,
	"x-forwarded-port": true, "x-real-ip": true, "cf-connecting-ip": true, "true-client-ip": true, "via": true,
}

var openAIRawRelayHopByHopResponseHeaders = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-connection": true, "proxy-authenticate": true,
	"te": true, "trailer": true, "transfer-encoding": true, "upgrade": true,
}

func (s *OpenAIGatewayService) forwardOpenAIRawRelay(ctx context.Context, c *gin.Context, account *Account, body []byte) (*OpenAIForwardResult, error) {
	startTime := time.Now()
	// 本站自己的拒绝都不算账号失败，免得一个用户的坏请求抬高共享账号的错误率。
	if restriction := s.detectCodexClientRestriction(c, account, body); restriction.Enabled && !restriction.Matched {
		writeOpenAIRawRelayLocalRejection(c, http.StatusForbidden, "forbidden_error", CodexClientRestrictionMessage(restriction))
		return nil, openAIRawRelayNotAccountFault(errors.New("codex_cli_only restriction: only codex official clients are allowed"))
	}

	outBody, plainBody, encoding, err := s.openAIRawRelayOutboundBody(ctx, c, account, body)
	if err != nil {
		var blocked *OpenAIFastBlockedError
		if errors.As(err, &blocked) {
			writeOpenAIFastPolicyBlockedResponse(c, blocked)
			MarkResponseCommitted(c)
			return nil, openAIRawRelayNotAccountFault(err)
		}
		if errors.Is(err, errOpenAIRawRelayDuplicateKeys) {
			writeOpenAIRawRelayLocalRejection(c, http.StatusBadRequest, "invalid_request_error", err.Error())
			return nil, openAIRawRelayNotAccountFault(err)
		}
		return nil, err
	}
	reqModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	sentModel := strings.TrimSpace(gjson.GetBytes(plainBody, "model").String())
	SetOpsUpstreamModel(c, sentModel)

	req, err := s.buildOpenAIRawRelayRequest(ctx, c, account, outBody, encoding)
	if err != nil {
		return nil, err
	}
	markOpenAITurnStateSent(c, account, req.Header.Get(openAICodexTurnStateHeader))

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	upstreamStart := time.Now()
	// 不 detach：客户端一断，请求上下文取消，上游连接随之断开，CPR 那边立即停。
	resp, err := s.doOpenAIUpstream(req, proxyURL, account)
	SetOpsLatencyMs(c, OpsUpstreamLatencyMsKey, time.Since(upstreamStart).Milliseconds())
	if err != nil {
		return nil, s.openAIRawRelayTransportError(ctx, c, account, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, s.handleOpenAIRawRelayErrorResponse(c, account, resp)
	}

	if !c.Writer.Written() {
		copyOpenAIRawRelayResponseHeaders(c.Writer.Header(), resp.Header)
	}
	// 记铸造者与形态观测；上游没带时顺手清掉上一次 failover 尝试残留的头。
	s.relayOpenAICodexTurnState(c, account, resp.Header)

	var relay openAIRawRelayOutcome
	if isEventStreamResponse(resp.Header) {
		relay, err = s.relayOpenAIRawStream(ctx, c, resp, startTime)
	} else {
		relay, err = s.relayOpenAIRawBody(c, resp)
	}
	result := s.openAIRawRelayResult(c, resp, relay, reqModel, sentModel, plainBody, startTime)
	if err != nil {
		result.ClientDisconnect = relay.clientGone
		return result, err
	}

	s.bindHTTPResponseAccount(ctx, c, account, relay.responseID)
	if snapshot := ParseCodexRateLimitHeaders(resp.Header); snapshot != nil {
		s.updateCodexUsageSnapshot(ctx, account.ID, snapshot)
	}
	return result, nil
}

// openAIRawRelayOutboundBody 决定出站请求体：分组策略没改动任何字段就发线上原文（含压缩），
// 改了才发改过的明文。策略只动它们管的字段：推理强度（分组）、model（渠道映射）、
// service_tier（Fast 策略，按账号作用域）。plain 始终是明文，供记账解析用。
func (s *OpenAIGatewayService) openAIRawRelayOutboundBody(ctx context.Context, c *gin.Context, account *Account, forwardBody []byte) (out, plain []byte, encoding string, err error) {
	preread, _ := c.Request.Body.(*httputil.PrereadBody)
	decoded := preread.Bytes()
	if decoded == nil {
		decoded = forwardBody
	}
	base, err := httputil.NormalizeLenientJSONRequestBody(decoded, 0)
	if err != nil {
		return nil, nil, "", err
	}
	if jsonHasDuplicateObjectKeys(base) {
		return nil, nil, "", errOpenAIRawRelayDuplicateKeys
	}
	patched := base
	if group := apiKeyGroup(getAPIKeyFromContext(c)); group != nil {
		if patched, _, err = ApplyOpenAIReasoningEffortPolicy(patched, group.MaxReasoningEffort, group.ReasoningEffortMappings, group.MaxReasoningEffortOverLimit); err != nil {
			return nil, nil, "", err
		}
	}
	if forward, ok := openAIForwardModelFromContext(ctx); ok && forward.model != "" && gjson.GetBytes(patched, "model").String() != forward.model {
		if patched, err = sjson.SetBytes(patched, "model", forward.model); err != nil {
			return nil, nil, "", fmt.Errorf("set raw relay model: %w", err)
		}
	}
	if patched, err = s.applyOpenAIFastPolicyToBody(ctx, account, strings.TrimSpace(gjson.GetBytes(patched, "model").String()), patched); err != nil {
		return nil, nil, "", err
	}
	if bytes.Equal(patched, base) {
		if wire, wireEncoding, ok := preread.Wire(); ok {
			return wire, patched, wireEncoding, nil
		}
	}
	// ponytail: 改过的明文不再按入站编码重新压缩；客户端压缩请求体又恰好命中策略时才会用到。
	return patched, patched, "", nil
}

func (s *OpenAIGatewayService) buildOpenAIRawRelayRequest(ctx context.Context, c *gin.Context, account *Account, body []byte, encoding string) (*http.Request, error) {
	targetURL, err := s.openAIRawRelayTargetURL(c, account)
	if err != nil {
		return nil, err
	}
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}
	authHeaders, err := s.buildOpenAIAuthenticationHeaders(ctx, account, token)
	if err != nil {
		return nil, fmt.Errorf("build openai authentication headers: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAI))
	copyOpenAIRawRelayRequestHeaders(req.Header, c.Request.Header)
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	for key, values := range authHeaders {
		req.Header.Del(key)
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	return req, nil
}

func (s *OpenAIGatewayService) openAIRawRelayTargetURL(c *gin.Context, account *Account) (string, error) {
	baseURL := account.GetOpenAIBaseURL()
	if account.IsCPR() {
		// 与透传路径同理：cpr 缺 base_url 不能回落到 api.openai.com，否则 client key 发给官方。
		if baseURL = account.GetCPRGatewayBaseURL(); baseURL == "" {
			return "", errors.New("cpr account requires credentials.base_url")
		}
	}
	targetURL := openaiPlatformAPIURL
	if baseURL != "" {
		validatedURL, err := s.validateUpstreamBaseURL(baseURL)
		if err != nil {
			return "", err
		}
		targetURL = buildOpenAIResponsesURL(validatedURL)
	}
	return appendOpenAIResponsesRequestPathSuffix(targetURL, openAIRawRelayRequestPathSuffix(c)), nil
}

// openAIRawRelayRequestPathSuffix 取入站原始路径的 /responses 子路径。handler 会把
// body-signal compact 的 URL 改写成 /compact，原样中继不跟随这类协议转换。
func openAIRawRelayRequestPathSuffix(c *gin.Context) string {
	path := c.Request.URL.Path
	if c.Request.RequestURI != "" {
		if original, err := url.ParseRequestURI(c.Request.RequestURI); err == nil {
			path = original.Path
		}
	}
	suffix, ok := sanitizedUpstreamPathSuffix(openAIResponsesPathSuffix(path))
	if !ok {
		return ""
	}
	return suffix
}

// errOpenAIRawRelayDuplicateKeys：本站用 gjson 按首个键读字段做准入与策略，CPR（serde_json）按末个键
// 生效；带重复键的请求两边看到的不是同一个，一律拒绝。真 Codex 客户端从结构体序列化，不会出现。
var errOpenAIRawRelayDuplicateKeys = errors.New("duplicate JSON object keys are not supported")

// jsonHasDuplicateObjectKeys 报告任一层对象里有没有重复键（按转义还原后的键比较）。线性扫描，
// 不受嵌套深度影响；不是合法 JSON 时返回 false，交给上游按非法请求拒绝。
func jsonHasDuplicateObjectKeys(data []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // 超出 float64 的数字不能让扫描提前退出
	type object struct {
		keys      map[string]struct{}
		expectKey bool
	}
	var stack []*object // 数组层记 nil
	for {
		tok, err := dec.Token()
		if err != nil {
			return false
		}
		switch t := tok.(type) {
		case json.Delim:
			switch t {
			case '{':
				stack = append(stack, &object{keys: map[string]struct{}{}, expectKey: true})
				continue
			case '[':
				stack = append(stack, nil)
				continue
			}
			stack = stack[:len(stack)-1]
		case string:
			if n := len(stack); n > 0 && stack[n-1] != nil && stack[n-1].expectKey {
				if _, dup := stack[n-1].keys[t]; dup {
					return true
				}
				stack[n-1].keys[t] = struct{}{}
				stack[n-1].expectKey = false
				continue
			}
		}
		// 一个值结束：外层若是对象，下一个 token 是键。
		if n := len(stack); n > 0 && stack[n-1] != nil {
			stack[n-1].expectKey = true
		}
	}
}

func copyOpenAIRawRelayRequestHeaders(dst, src http.Header) {
	listed := connectionListedHeaders(src)
	for key, values := range src {
		lower := strings.ToLower(key)
		// cf-* / cdn-loop 描述的是客户端→本站这一跳的 CDN，下一跳也在 Cloudflare 后面时会被当成环路。
		if openAIRawRelayDroppedRequestHeaders[lower] || listed[lower] || strings.HasPrefix(lower, "cf-") || lower == "cdn-loop" {
			continue
		}
		dst[key] = append([]string(nil), values...)
	}
}

func copyOpenAIRawRelayResponseHeaders(dst, src http.Header) {
	listed := connectionListedHeaders(src)
	for key, values := range src {
		lower := strings.ToLower(key)
		if openAIRawRelayHopByHopResponseHeaders[lower] || listed[lower] {
			continue
		}
		dst[textproto.CanonicalMIMEHeaderKey(key)] = append([]string(nil), values...)
	}
}

// connectionListedHeaders 返回 Connection 头里点名的逐跳头（RFC 9110 §7.6.1）。
func connectionListedHeaders(h http.Header) map[string]bool {
	listed := map[string]bool{}
	for _, value := range h.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if token = strings.ToLower(strings.TrimSpace(token)); token != "" {
				listed[token] = true
			}
		}
	}
	return listed
}

// isOpenAIRawRelayUnavailable 只认「这一路整体用不了」：CPR 自身的鉴权/额度/容量拒绝，
// 以及下一跳是 sub2api 时它自己的鉴权、额度、无可用账号，外加反代在后端挂掉时回的
// 非 JSON 502/504。上游 OpenAI 经 CPR 转回来的错误不在其列，原样透传。
func isOpenAIRawRelayUnavailable(status int, header http.Header, body []byte) bool {
	switch gjson.GetBytes(body, "code").String() {
	case "INVALID_API_KEY", "API_KEY_REQUIRED", "API_KEY_DISABLED", "API_KEY_EXPIRED", "USER_INACTIVE",
		"INSUFFICIENT_BALANCE", "API_KEY_QUOTA_EXHAUSTED", "SUBSCRIPTION_NOT_FOUND":
		return true
	}
	code := gjson.GetBytes(body, "error.code").String()
	switch status {
	case http.StatusUnauthorized:
		// CPR 自己拒 key 不带 x-gateway-request-id；带了说明是上游 401 经 CPR 转回。
		return code == "invalid_api_key" && header.Get("x-gateway-request-id") == ""
	case http.StatusTooManyRequests:
		// CPR 的 429（额度、key 预算、限速、并发排队）与 sub2api 的限流/额度，全是这一路暂时用不了。
		return true
	case http.StatusServiceUnavailable:
		switch code {
		case "no_available_provider", "account_capacity_unavailable", "provider_infrastructure_unavailable",
			"runtime_configuration_unavailable", "key_budget_unavailable":
			return true
		}
		// sub2api 无可用账号（no_account_error.go 的兜底文案）。
		return gjson.GetBytes(body, "error.type").String() == "api_error" &&
			gjson.GetBytes(body, "error.message").String() == "Service temporarily unavailable"
	case http.StatusBadGateway, http.StatusGatewayTimeout:
		return !gjson.ValidBytes(body)
	}
	return false
}

func (s *OpenAIGatewayService) handleOpenAIRawRelayErrorResponse(c *gin.Context, account *Account, resp *http.Response) error {
	body := s.readUpstreamErrorBody(resp)
	if failoverErr := s.recordOpenAIRawRelayUpstreamError(c, account, resp.StatusCode, resp.Header, body); failoverErr != nil {
		return failoverErr
	}
	WriteOpenAIRawRelayUpstreamResponse(c, resp.StatusCode, resp.Header, body)
	return fmt.Errorf("openai raw relay upstream status %d", resp.StatusCode)
}

// recordOpenAIRawRelayUpstreamError 记上游错误到运维；整体不可用时返回可换号错误，否则返回 nil。
func (s *OpenAIGatewayService) recordOpenAIRawRelayUpstreamError(c *gin.Context, account *Account, status int, header http.Header, body []byte) *UpstreamFailoverError {
	message := sanitizeUpstreamErrorMessage(strings.TrimSpace(extractUpstreamErrorMessage(body)))
	detail := ""
	if s.cfg != nil && s.cfg.Gateway.LogUpstreamErrorBody {
		maxBytes := s.cfg.Gateway.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		detail = truncateString(string(body), maxBytes)
	}
	setOpsUpstreamError(c, status, message, detail)
	if hit, code, msg := detectOpenAICyberPolicy(body); hit {
		MarkOpsCyberPolicy(c, CyberPolicyMark{Code: code, Message: msg, Body: truncateString(string(body), 4096), UpstreamStatus: status})
	}
	unavailable := isOpenAIRawRelayUnavailable(status, header, body)
	kind := "http_error"
	if unavailable {
		kind = "failover"
	}
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		ProxyID:              opsUpstreamProxyID(account),
		ProxyName:            opsUpstreamProxyName(account),
		Platform:             account.Platform,
		AccountID:            account.ID,
		AccountName:          account.Name,
		UpstreamStatusCode:   status,
		UpstreamRequestID:    header.Get("x-request-id"),
		Passthrough:          true,
		Kind:                 kind,
		Message:              message,
		Detail:               detail,
		UpstreamResponseBody: detail,
	})
	if unavailable {
		return &UpstreamFailoverError{
			StatusCode:       status,
			ResponseBody:     body,
			ResponseHeaders:  header.Clone(),
			Scope:            GatewayFailureScopeProvider,
			RawRelayResponse: true,
		}
	}
	return nil
}

// openAIRawRelayTransportError 连不上上游（拒连/DNS/TLS/超时）算整体不可用：换号，但不改账号状态。
func (s *OpenAIGatewayService) openAIRawRelayTransportError(ctx context.Context, c *gin.Context, account *Account, err error) error {
	safeErr := sanitizeUpstreamErrorMessage(err.Error())
	setOpsUpstreamError(c, 0, safeErr, "")
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
		ProxyID:     opsUpstreamProxyID(account),
		ProxyName:   opsUpstreamProxyName(account),
		Platform:    account.Platform,
		AccountID:   account.ID,
		AccountName: account.Name,
		Passthrough: true,
		Kind:        "request_error",
		Message:     safeErr,
	})
	if ctx.Err() != nil {
		return err
	}
	return &UpstreamFailoverError{
		StatusCode:   http.StatusBadGateway,
		ResponseBody: openAITransportFailoverBody,
		Scope:        GatewayFailureScopeProvider,
	}
}

// WriteOpenAIRawRelayUpstreamResponse 把上游响应原样写回。本站已先提交过响应（排队心跳）
// 时状态码和头都改不了，退化成一条 SSE error 事件，正文照旧。
func WriteOpenAIRawRelayUpstreamResponse(c *gin.Context, status int, header http.Header, body []byte) {
	MarkResponseCommitted(c)
	if c.Writer.Written() {
		_, _ = c.Writer.Write(openAIRawRelaySSEErrorFrame(body))
		c.Writer.Flush()
		return
	}
	copyOpenAIRawRelayResponseHeaders(c.Writer.Header(), header)
	// 正文可能被读取上限截断，长度交给 net/http 重算。
	c.Writer.Header().Del("Content-Length")
	c.Writer.Header().Del("Content-Encoding")
	c.Writer.WriteHeader(status)
	_, _ = c.Writer.Write(body)
}

// writeOpenAIRawRelayLocalRejection 写本站自己的拒绝；compact 排队心跳已把 200 提交出去时改发 SSE 失败事件。
// 写完标记已提交，否则 handler 见到 Forward 返回的错误还会再补一条兜底 response.failed。
func writeOpenAIRawRelayLocalRejection(c *gin.Context, status int, errType, message string) {
	MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
	defer MarkResponseCommitted(c)
	if StopOpenAICompactSSEKeepaliveCommitted(c) {
		writeOpenAICompactSSEFailureMessage(c, status, errType, message)
		return
	}
	c.JSON(status, gin.H{"error": gin.H{"type": errType, "message": message}})
}

func openAIRawRelaySSEErrorFrame(body []byte) []byte {
	var payload bytes.Buffer
	if json.Compact(&payload, body) != nil {
		payload.Reset()
		wrapped, _ := json.Marshal(map[string]any{"type": "error", "error": map[string]string{"message": string(body)}})
		_, _ = payload.Write(wrapped)
	}
	return []byte("event: error\ndata: " + payload.String() + "\n\n")
}

type openAIRawRelayOutcome struct {
	usage        OpenAIUsage
	responseID   string
	firstTokenMs *int
	imageCount   int
	imageSizes   []string
	clientGone   bool
}

// relayOpenAIRawStream 逐行原样转发 SSE，旁路解析记账字段。转发字节不经过解析结果。
func (s *OpenAIGatewayService) relayOpenAIRawStream(ctx context.Context, c *gin.Context, resp *http.Response, startTime time.Time) (out openAIRawRelayOutcome, err error) {
	observer := upstreamResponseModelObserverFromContext(c)
	images := newOpenAIImageOutputCounter()
	ttftMode := s.openAITTFTMode(ctx)
	eventType := ""
	defer func() {
		out.imageCount, out.imageSizes = images.Count(), images.Sizes()
	}()

	if !c.Writer.Written() {
		c.Writer.WriteHeader(resp.StatusCode)
	}
	c.Writer.Flush()
	reader := bufio.NewReaderSize(resp.Body, 32<<10)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if _, err = c.Writer.Write(line); err != nil {
				out.clientGone = true
				return out, err
			}
			if reader.Buffered() == 0 {
				c.Writer.Flush()
			}
			eventType = s.observeOpenAIRawSSELine(c, &out, observer, images, line, eventType, ttftMode, startTime)
		}
		if readErr == nil {
			continue
		}
		c.Writer.Flush()
		if readErr == io.EOF {
			return out, nil
		}
		if ctx.Err() != nil {
			out.clientGone = true
		}
		return out, fmt.Errorf("openai raw relay stream: %w", readErr)
	}
}

func (s *OpenAIGatewayService) observeOpenAIRawSSELine(
	c *gin.Context,
	out *openAIRawRelayOutcome,
	observer *upstreamResponseModelObserver,
	images *openAIImageOutputCounter,
	line []byte,
	eventType string,
	ttftMode string,
	startTime time.Time,
) string {
	text := strings.TrimRight(string(line), "\r\n")
	if text == "" {
		return ""
	}
	if event, ok := extractOpenAISSEEventLine(text); ok {
		return event
	}
	data, ok := extractOpenAISSEDataLine(text)
	if !ok {
		return eventType
	}
	payload := []byte(data)
	effective := effectiveOpenAISSEEventType(payload, eventType)
	observer.ObserveOpenAI(payload, effective)
	if out.responseID == "" {
		out.responseID = extractOpenAIResponseIDFromJSONBytes(payload)
	}
	images.AddSSEData(payload)
	if out.firstTokenMs == nil && openAIStreamDataStartsTTFT(strings.TrimSpace(data), effective, false, ttftMode) {
		ms := int(time.Since(startTime).Milliseconds())
		out.firstTokenMs = &ms
	}
	s.parseSSEUsageBytesWithType(payload, effective, &out.usage)
	if effective == "response.failed" || effective == "error" {
		if hit, code, msg := detectOpenAICyberPolicy(payload); hit {
			MarkOpsCyberPolicy(c, CyberPolicyMark{
				Code: code, Message: msg, Body: truncateString(data, 4096), UpstreamStatus: http.StatusOK,
				UpstreamInTok: out.usage.InputTokens, UpstreamOutTok: out.usage.OutputTokens,
			})
		}
	}
	return eventType
}

// relayOpenAIRawBody 非流式：整体读完再原样写回（与透传路径一样按上限读取）。
func (s *OpenAIGatewayService) relayOpenAIRawBody(c *gin.Context, resp *http.Response) (openAIRawRelayOutcome, error) {
	var out openAIRawRelayOutcome
	body, err := ReadUpstreamResponseBody(resp.Body, s.cfg, c, openAITooLargeError)
	if err != nil {
		out.clientGone = c.Request.Context().Err() != nil
		return out, err
	}
	if observer := upstreamResponseModelObserverFromContext(c); observer != nil {
		observer.ObserveOpenAI(body, strings.TrimSpace(gjson.GetBytes(body, "type").String()))
	}
	if usage, ok := extractOpenAIUsageFromJSONBytes(body); ok {
		out.usage = usage
	}
	out.responseID = extractOpenAIResponseIDFromJSONBytes(body)
	out.imageCount = countOpenAIResponseImageOutputsFromJSONBytes(body)
	out.imageSizes = collectOpenAIResponseImageOutputSizesFromJSONBytes(body)
	if !c.Writer.Written() {
		c.Writer.WriteHeader(resp.StatusCode)
	}
	if _, err := c.Writer.Write(body); err != nil {
		out.clientGone = true
		return out, err
	}
	return out, nil
}

func (s *OpenAIGatewayService) openAIRawRelayResult(c *gin.Context, resp *http.Response, relay openAIRawRelayOutcome, reqModel, sentModel string, plainBody []byte, startTime time.Time) *OpenAIForwardResult {
	upstreamModel := ""
	if sentModel != reqModel {
		upstreamModel = sentModel
	}
	result := &OpenAIForwardResult{
		RequestID:                     resp.Header.Get("x-request-id"),
		UpstreamHeaders:               resp.Header,
		ResponseID:                    relay.responseID,
		Usage:                         relay.usage,
		Model:                         reqModel,
		UpstreamModel:                 upstreamModel,
		UpstreamResponseModel:         observedUpstreamResponseModel(c),
		UpstreamResponseModelConflict: observedUpstreamResponseModelConflict(c),
		UpstreamResponseServiceTier:   observedUpstreamResponseServiceTier(c),
		ServiceTier:                   resolvedOpenAIUpstreamServiceTier(c, extractOpenAIServiceTierFromBody(plainBody)),
		ReasoningEffort:               extractOpenAIReasoningEffortFromBody(plainBody, sentModel),
		Stream:                        gjson.GetBytes(plainBody, "stream").Bool(),
		Duration:                      time.Since(startTime),
		FirstTokenMs:                  relay.firstTokenMs,
	}
	if relay.imageCount > 0 {
		result.ImageCount = relay.imageCount
		result.ImageOutputSizes = relay.imageSizes
		if cfg, err := resolveOpenAIResponsesImageBillingConfigDetailedFromBody(plainBody, reqModel); err == nil {
			result.ImageSize = cfg.SizeTier
			result.ImageInputSize = cfg.InputSize
			result.BillingModel = cfg.Model
		}
	}
	return result
}
