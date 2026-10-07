package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// WS 原样中继：握手带上客户端的全部请求头、只换鉴权；帧双向原样转发，每个文本帧只按分组
// 策略改对应字段；一侧关闭就用同一个关闭码关另一侧，客户端一走上游立刻关。
// 换号只发生在握手阶段（连不上、被拒且属于整体不可用），连上之后的错误都原样给客户端。
//
// 轮次按 CPR 的语义划分（v3.13.1 gateway-api/src/openai/responses/websocket/mod.rs、forward.rs）：
// 它逐帧串行处理，每个文本帧都按 response.create 解码（不是就回一条 error），每帧恰好以一个
// 结束事件收尾——终态（completed/incomplete/failed）或 error，二者不会先后都来。所以这里每个
// 客户端数据帧都算一轮（准入、并发槽、计费），按到达顺序结算。

// openAIRawRelayWSPingInterval 是给客户端发 ping 的间隔。本站不按空闲断连，
// 靠它防前面的反代因空闲断线（nginx 默认 60 秒）。
const openAIRawRelayWSPingInterval = 25 * time.Second

// ErrOpenAIRawRelayNotAccountFault 标记原样中继上不算账号失败的结束：用户侧策略拒绝、上游握手
// 拒绝原样透传。handler 据此不上报账号调度失败。
var ErrOpenAIRawRelayNotAccountFault = errors.New("openai raw relay: not an account fault")

func openAIRawRelayNotAccountFault(err error) error {
	return fmt.Errorf("%w: %w", ErrOpenAIRawRelayNotAccountFault, err)
}

// errOpenAIRawRelayWSDone：relay 已结束，客户端读协程里迟到的帧不再放行。
var errOpenAIRawRelayWSDone = errors.New("openai raw relay: relay already finished")

// openAIRawRelayWSSide 是中继的一侧连接。Close 照抄另一侧收到的关闭帧；另一侧没收到
// 关闭帧（断线或本站结束）就直接断开，和直连时对端看到的一样。
type openAIRawRelayWSSide struct {
	conn     *coderws.Conn
	read     func(ctx context.Context) (coderws.MessageType, []byte, error)
	filter   func(payload []byte) ([]byte, error)
	received atomic.Pointer[coderws.CloseError]
	peer     *openAIRawRelayWSSide
}

func (s *openAIRawRelayWSSide) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	msgType, payload, err := s.read(ctx)
	if err != nil {
		var closeErr coderws.CloseError
		if errors.As(err, &closeErr) {
			s.received.Store(&closeErr)
		}
		return msgType, payload, err
	}
	// 二进制帧同样过准入：CPR 收到会按策略关连接，但 apikey 原样中继的下一跳是 sub2api，它会执行。
	if s.filter != nil {
		payload, err = s.filter(payload)
	}
	return msgType, payload, err
}

func (s *openAIRawRelayWSSide) WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error {
	return s.conn.Write(ctx, msgType, payload)
}

func (s *openAIRawRelayWSSide) Close() error {
	if peerClose := s.peer.received.Load(); peerClose != nil {
		_ = s.conn.Close(peerClose.Code, peerClose.Reason)
	}
	return s.conn.CloseNow()
}

func (s *OpenAIGatewayService) proxyResponsesWebSocketRawRelay(
	ctx context.Context,
	c *gin.Context,
	clientConn *coderws.Conn,
	account *Account,
	token string,
	firstClientMessage []byte,
	hooks *OpenAIWSIngressHooks,
) error {
	clearOpenAITurnStateInjected(c)
	if hooks == nil {
		hooks = &OpenAIWSIngressHooks{}
	}
	meta := newOpenAIWSPassthroughUsageMeta(hooks.InitialRequestModel, firstClientMessage)
	if err := rejectOpenAIRawRelayWSDuplicateKeys(firstClientMessage); err != nil {
		return openAIRawRelayNotAccountFault(err)
	}
	first, err := s.patchOpenAIRawRelayWSFrame(ctx, c, clientConn, account, hooks, meta, 1, firstClientMessage)
	if err != nil {
		return openAIRawRelayNotAccountFault(err)
	}

	targetURL, err := s.openAIRawRelayTargetURL(c, account)
	if err != nil {
		return err
	}
	wsURL, err := openAIWSURLFromHTTP(targetURL)
	if err != nil {
		return err
	}
	headers := http.Header{}
	copyOpenAIRawRelayRequestHeaders(headers, c.Request.Header)
	for key := range headers {
		// Sec-WebSocket-* 描述的是客户端→本站这次握手，出站握手由拨号库自己生成。
		if strings.HasPrefix(strings.ToLower(key), "sec-websocket-") {
			delete(headers, key)
		}
	}
	authHeaders, err := s.buildOpenAIAuthenticationHeaders(ctx, account, token)
	if err != nil {
		return err
	}
	for key, values := range authHeaders {
		headers[key] = append([]string(nil), values...)
	}
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, s.openAIWSDialTimeout())
	upstream, status, handshakeHeaders, err := s.getOpenAIWSPassthroughDialer().Dial(dialCtx, wsURL, headers, proxyURL)
	cancelDial()
	if err != nil {
		return s.openAIRawRelayWSDialError(ctx, c, clientConn, account, status, handshakeHeaders, err)
	}
	upstreamConn, ok := upstream.(*coderOpenAIWSClientConn)
	if !ok {
		_ = upstream.Close()
		return errors.New("openai raw relay: upstream websocket does not expose its connection")
	}

	// 已发的一轮（含首帧）与已结算的一轮；有保护保证同一时刻只有一轮在途。
	var accepted, settled atomic.Int32
	accepted.Store(1)
	var turnStartedAt atomic.Int64
	firstTurnStartedAt := hooks.InitialTurnStartedAt
	if firstTurnStartedAt.IsZero() {
		firstTurnStartedAt = time.Now()
	}
	turnStartedAt.Store(firstTurnStartedAt.UnixNano())
	var turnPayload atomic.Pointer[[]byte]
	turnPayload.Store(&first)
	turns := newOpenAIWSPassthroughTurnLifecycle(true)

	client := &openAIRawRelayWSSide{
		conn: clientConn,
		read: func(context.Context) (coderws.MessageType, []byte, error) {
			return ReadOpenAIWSClientMessage(ctx, clientConn, 0, 0, "")
		},
	}
	upstreamSide := &openAIRawRelayWSSide{
		conn: upstreamConn.conn,
		// 读不挂 relay 的 ctx：ctx 一取消 coder/websocket 会直接掐断连接，关闭帧就发不出去了。
		// relay 退出时总会先调 Close，读随之结束。
		read: func(context.Context) (coderws.MessageType, []byte, error) {
			return upstreamConn.conn.Read(context.Background())
		},
		peer: client,
	}
	client.peer = upstreamSide
	// filterMu 让客户端读协程里的准入与 relay 收尾互斥：收尾先等在跑的准入做完再兜底，之后迟到的
	// 帧一律不放行，免得在 handler 释放并发槽之后又去抢槽。
	var filterMu sync.Mutex
	relayDone := false
	client.filter = func(payload []byte) ([]byte, error) {
		filterMu.Lock()
		defer filterMu.Unlock()
		if relayDone {
			return nil, errOpenAIRawRelayWSDone
		}
		if !turns.beginResponseCreate(nil) {
			err := errors.New("overlapping response.create is not supported")
			return nil, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, err.Error(), err)
		}
		turn := accepted.Load() + 1
		startedAt := time.Now()
		out, err := s.acceptOpenAIRawRelayWSTurn(ctx, c, clientConn, account, hooks, meta, int(turn), payload)
		if err != nil {
			turns.cancelResponseCreate()
			return nil, err
		}
		turnStartedAt.Store(startedAt.UnixNano())
		turnPayload.Store(&out)
		accepted.Store(turn)
		return out, nil
	}

	writeCtx, cancelWrite := context.WithTimeout(ctx, s.openAIWSWriteTimeout())
	err = upstreamSide.WriteFrame(writeCtx, coderws.MessageText, first)
	cancelWrite()
	if err != nil {
		_ = upstreamConn.conn.CloseNow()
		return wrapOpenAIWSIngressTurnError("write_upstream", err, false)
	}

	var lastClientWrite atomic.Int64
	lastClientWrite.Store(time.Now().UnixNano())
	pingDone := make(chan struct{})
	// 不挂请求 ctx：它一取消若正撞上 ping 在写，库会直接断连接，租约丢失时该发的 1013 就发不出去。
	pingCtx, stopPing := context.WithCancel(context.Background())
	go func() {
		defer close(pingDone)
		pingOpenAIRawRelayWSClient(pingCtx, clientConn, &lastClientWrite)
	}()
	defer func() {
		stopPing()
		<-pingDone
	}()

	// 以下只在上游读协程里用。
	var turnHeaders http.Header
	images := newOpenAIImageOutputCounter()
	turnResult := func(requestModel string) *OpenAIForwardResult {
		requestModel, upstreamModel := meta.turnModels(requestModel)
		return &OpenAIForwardResult{
			Model:                    requestModel,
			UpstreamModel:            openAIWSDifferentModel(requestModel, upstreamModel),
			ServiceTier:              meta.serviceTier.Load(),
			ReasoningEffort:          meta.reasoningEffort.Load(),
			RequestedReasoningEffort: meta.requestedReasoningEffort.Load(),
			Stream:                   true,
			OpenAIWSMode:             true,
			ResponseHeaders:          cloneHeader(handshakeHeaders),
		}
	}

	_, relayExit := openaiwsv2.RunEntry(openaiwsv2.EntryInput{
		Ctx:                ctx,
		ClientConn:         client,
		UpstreamConn:       upstreamSide,
		FirstClientMessage: first,
		Options: openaiwsv2.RelayOptions{
			WriteTimeout: s.openAIWSWriteTimeout(),
			// 客户端一走就关上游，不为等这一轮的用量多挂着（用户定的取舍）。
			UpstreamDrainTimeout: time.Millisecond,
			FirstTurnStartedAt:   firstTurnStartedAt,
			FirstMessageType:     coderws.MessageText,
			FirstMessageSent:     true,
			BareErrorEndsTurn:    true,
			OnTurnComplete: func(turn openaiwsv2.RelayTurnResult) {
				turnNo := accepted.Load()
				if settled.Load() >= turnNo {
					return // 没有在途的一轮（多出来的 error 事件）
				}
				settled.Store(turnNo)
				result := turnResult(turn.RequestModel)
				result.RequestID = turn.RequestID
				result.Usage = OpenAIUsage{
					InputTokens:              turn.Usage.InputTokens,
					OutputTokens:             turn.Usage.OutputTokens,
					CacheCreationInputTokens: turn.Usage.CacheCreationInputTokens,
					CacheReadInputTokens:     turn.Usage.CacheReadInputTokens,
					ImageOutputTokens:        turn.Usage.ImageOutputTokens,
				}
				result.UpstreamResponseModel = turn.ResponseModel
				result.UpstreamResponseModelConflict = turn.ResponseModelConflict
				result.UpstreamResponseServiceTier = normalizeObservedOpenAIServiceTier(turn.ResponseServiceTier)
				result.UpstreamTerminalEvent = normalizeOpenAIWSTerminalEvent(turn.TerminalEventType)
				result.UpstreamHeaders = turnHeaders
				result.Duration = turn.Duration
				result.FirstTokenMs = turn.FirstTokenMs
				startedAt := turn.StartedAt
				if startedAt.IsZero() {
					// 裸 error 结束的一轮没有 response id，引擎不计时；用放行这一轮时记下的时刻。
					startedAt = time.Unix(0, turnStartedAt.Load())
					result.Duration = time.Since(startedAt)
				}
				if count := images.Count(); count > 0 {
					result.ImageCount = count
					result.ImageOutputSizes = images.Sizes()
					if payload := turnPayload.Load(); payload != nil {
						if cfg, err := resolveOpenAIResponsesImageBillingConfigDetailedFromBody(*payload, result.Model); err == nil {
							result.ImageSize = cfg.SizeTier
							result.ImageInputSize = cfg.InputSize
							result.BillingModel = cfg.Model
						}
					}
				}
				turnHeaders = nil
				images = newOpenAIImageOutputCounter()
				s.bindHTTPResponseAccount(ctx, c, account, turn.RequestID)
				if result.RequestID == "" {
					result.RequestID = openAIRawRelayWSTurnRequestID()
				}
				if hooks.TurnStarted != nil {
					hooks.TurnStarted(int(turnNo), startedAt)
				}
				if hooks.AfterTurn != nil {
					hooks.AfterTurn(int(turnNo), result, nil)
				}
			},
			BeforeWriteClient: func(msgType coderws.MessageType, payload []byte, _ bool) error {
				if msgType != coderws.MessageText {
					return nil
				}
				if headers := openAIWSResponseMetadataHeaders(payload); headers != nil {
					turnHeaders = headers
					if state := extractOpenAICodexTurnState(headers); state != "" {
						s.noteOpenAICodexTurnStateOrigin(c, account, state)
						s.observeOpenAITurnStateMint(c, account, state)
					}
					if snapshot := ParseCodexRateLimitHeaders(headers); snapshot != nil {
						s.updateCodexUsageSnapshot(ctx, account.ID, snapshot)
					}
					return nil
				}
				images.AddSSEData(payload)
				if eventType := gjson.GetBytes(payload, "type").String(); eventType == "error" || eventType == "response.failed" {
					markOpenAIWSV2PassthroughCyberPolicy(c, payload)
				}
				return nil
			},
			BeforeClientWrite: func(msgType coderws.MessageType, payload []byte) {
				if msgType == coderws.MessageText && openAIRawRelayWSTurnEnded(payload) {
					turns.beginTerminalWrite()
				}
			},
			AfterClientWrite: func(msgType coderws.MessageType, payload []byte, writeErr error) {
				if writeErr == nil {
					lastClientWrite.Store(time.Now().UnixNano())
				}
				if msgType != coderws.MessageText {
					return
				}
				if writeErr == nil {
					markOpenAIWSClientVisibleFailure(c, gjson.GetBytes(payload, "type").String(), payload)
				}
				if openAIRawRelayWSTurnEnded(payload) {
					turns.finishTerminalWrite(writeErr == nil, nil)
				}
			},
		},
	})

	var closeErr *OpenAIWSClientCloseError
	policyClose := relayExit != nil && errors.As(relayExit.Err, &closeErr)
	if policyClose {
		// 本站自己的决定（策略拒绝、并发满、租约丢失等），按它的关闭码关客户端。
		_ = clientConn.Close(closeErr.StatusCode(), truncateString(closeErr.Reason(), 120))
		_ = clientConn.CloseNow()
	} else {
		_ = client.Close()
	}
	filterMu.Lock()
	relayDone = true
	filterMu.Unlock()
	if turnNo := accepted.Load(); settled.Load() < turnNo {
		// 在途的一轮没等到终态（客户端走了或上游断了）：照样记一条 0 token 的使用记录。
		startedAt := time.Unix(0, turnStartedAt.Load())
		result := turnResult("")
		result.RequestID = openAIRawRelayWSTurnRequestID()
		result.Duration = time.Since(startedAt)
		if hooks.TurnStarted != nil {
			hooks.TurnStarted(int(turnNo), startedAt)
		}
		if hooks.AfterTurn != nil {
			hooks.AfterTurn(int(turnNo), result, nil)
		}
	}
	if relayExit != nil {
		logOpenAIRawRelayWS("relay_exit account_id=%d stage=%s err=%s", account.ID, relayExit.Stage, relayErrorText(relayExit.Err))
	}
	if policyClose {
		return openAIRawRelayNotAccountFault(relayExit.Err)
	}
	return nil
}

// openAIRawRelayWSTurnRequestID 给没有 response id 的一轮（上游一上来就报错、或中途断开）一个独立的
// 记账键；否则 WS 模式回落到连接级请求 ID，同一连接里这类行会撞唯一索引被丢掉。
func openAIRawRelayWSTurnRequestID() string {
	return "generated:" + generateRequestID()
}

// rejectOpenAIRawRelayWSDuplicateKeys 见 errOpenAIRawRelayDuplicateKeys。
func rejectOpenAIRawRelayWSDuplicateKeys(frame []byte) error {
	if !jsonHasDuplicateObjectKeys(frame) {
		return nil
	}
	return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, errOpenAIRawRelayDuplicateKeys.Error(), errOpenAIRawRelayDuplicateKeys)
}

// patchOpenAIRawRelayWSFrame 只按分组策略改 response.create 的对应字段：推理强度、渠道模型
// 映射、Fast（按账号作用域），其余字节不动。同时记下这一轮的计费元数据。
func (s *OpenAIGatewayService) patchOpenAIRawRelayWSFrame(
	ctx context.Context,
	c *gin.Context,
	clientConn *coderws.Conn,
	account *Account,
	hooks *OpenAIWSIngressHooks,
	meta *openAIWSPassthroughUsageMeta,
	turn int,
	frame []byte,
) ([]byte, error) {
	requestModel := meta.requestModelForFrame(frame)
	patched, err := applyOpenAIWSReasoningEffortPolicy(frame, hooks)
	if err != nil {
		return nil, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, err.Error(), err)
	}
	meta.captureRequestedReasoningEffort(frame, requestModel)
	if hooks.MapRequestModel != nil {
		mapped, err := hooks.MapRequestModel(turn, requestModel)
		if err != nil {
			return nil, err
		}
		if mapped = strings.TrimSpace(mapped); mapped != "" && mapped != requestModel {
			patched = ReplaceModelInBody(patched, mapped)
		}
	}
	model := strings.TrimSpace(gjson.GetBytes(patched, "model").String())
	if model == "" {
		model = requestModel
	}
	patched, blocked, err := s.applyOpenAIFastPolicyToWSResponseCreate(ctx, account, model, patched)
	if err != nil {
		return nil, err
	}
	if blocked != nil {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
		writeCtx, cancel := context.WithTimeout(ctx, s.openAIWSWriteTimeout())
		_ = clientConn.Write(writeCtx, coderws.MessageText, buildOpenAIFastPolicyBlockedWSEvent(blocked))
		cancel()
		return nil, NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, blocked.Message, blocked)
	}
	meta.updateFromResponseCreate(patched, model, requestModel)
	SetOpsUpstreamModel(c, model)
	sent := gjson.GetBytes(patched, "client_metadata."+openAICodexTurnStateHeader).String()
	if sent == "" && turn == 1 {
		sent = c.GetHeader(openAICodexTurnStateHeader)
	}
	markOpenAITurnStateSent(c, account, sent)
	return patched, nil
}

// acceptOpenAIRawRelayWSTurn 放行后续一轮：先挡重复键，再过 handler 的逐轮准入（白名单、审计、
// 并发槽），最后按策略改字段。
func (s *OpenAIGatewayService) acceptOpenAIRawRelayWSTurn(
	ctx context.Context,
	c *gin.Context,
	clientConn *coderws.Conn,
	account *Account,
	hooks *OpenAIWSIngressHooks,
	meta *openAIWSPassthroughUsageMeta,
	turn int,
	frame []byte,
) ([]byte, error) {
	if err := rejectOpenAIRawRelayWSDuplicateKeys(frame); err != nil {
		return nil, err
	}
	if hooks.BeforeRequest != nil {
		if err := hooks.BeforeRequest(turn, frame, meta.requestModelForFrame(frame)); err != nil {
			return nil, err
		}
	}
	if hooks.BeforeTurn != nil {
		if err := hooks.BeforeTurn(turn); err != nil {
			return nil, err
		}
	}
	return s.patchOpenAIRawRelayWSFrame(ctx, c, clientConn, account, hooks, meta, turn, frame)
}

// openAIRawRelayWSDialError 处理上游握手失败。连不上、或被拒且属于整体不可用 → 换号；
// 其余拒绝包成 Codex 认的错误事件发给客户端后关闭，与直连时握手被拒看到的内容一致。
func (s *OpenAIGatewayService) openAIRawRelayWSDialError(
	ctx context.Context,
	c *gin.Context,
	clientConn *coderws.Conn,
	account *Account,
	status int,
	header http.Header,
	err error,
) error {
	logOpenAIRawRelayWS("dial_failed account_id=%d status=%d err=%s", account.ID, status, err.Error())
	if status == 0 {
		return s.openAIRawRelayTransportError(ctx, c, account, err)
	}
	var handshakeErr *openAIWSHandshakeError
	var body []byte
	if errors.As(err, &handshakeErr) {
		body = handshakeErr.Body
	}
	event := openAIRawRelayWSErrorEvent(status, header, body)
	if failoverErr := s.recordOpenAIRawRelayUpstreamError(c, account, status, header, body); failoverErr != nil {
		// WS 换号耗尽时 handler 把 ResponseBody 当一帧原样发给客户端。
		failoverErr.ResponseBody = event
		return failoverErr
	}
	WriteOpenAIRawRelayWSError(c, clientConn, event)
	closeStatus, reason := coderws.StatusPolicyViolation, "upstream websocket handshake rejected"
	if status >= http.StatusInternalServerError {
		closeStatus, reason = coderws.StatusInternalError, "upstream websocket handshake failed"
	}
	_ = clientConn.Close(closeStatus, reason)
	_ = clientConn.CloseNow()
	return openAIRawRelayNotAccountFault(NewOpenAIWSClientCloseError(closeStatus, reason, err))
}

// openAIRawRelayWSErrorEvent 把握手被拒的 HTTP 响应包成 Codex 认的错误事件
// {"type":"error","status":N,"error":{...},"headers":{...}}，Codex 按 HTTP 错误处理它。
func openAIRawRelayWSErrorEvent(status int, header http.Header, body []byte) []byte {
	event := map[string]any{"type": "error", "status": status}
	parsed := gjson.ParseBytes(body)
	switch {
	case parsed.Get("error").IsObject():
		event["error"] = json.RawMessage(parsed.Get("error").Raw)
	case parsed.IsObject():
		event["error"] = json.RawMessage(parsed.Raw)
	default:
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = http.StatusText(status)
		}
		event["error"] = map[string]string{"message": message}
	}
	if len(header) > 0 {
		values := make(map[string]string, len(header))
		for key := range header {
			if !openAIRawRelayHopByHopResponseHeaders[strings.ToLower(key)] {
				values[strings.ToLower(key)] = header.Get(key)
			}
		}
		event["headers"] = values
	}
	out, err := json.Marshal(event)
	if err != nil {
		return []byte(`{"type":"error","status":502,"error":{"message":"upstream websocket handshake failed"}}`)
	}
	return out
}

// WriteOpenAIRawRelayWSError 把原样中继的错误帧发给客户端并记运维失败；关连接由调用方负责。
func WriteOpenAIRawRelayWSError(c *gin.Context, conn *coderws.Conn, frame []byte) {
	if conn == nil || len(frame) == 0 {
		return
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if conn.Write(writeCtx, coderws.MessageText, frame) == nil {
		markOpenAIWSClientVisibleFailure(c, gjson.GetBytes(frame, "type").String(), frame)
	}
}

// openAIWSResponseMetadataHeaders 取 response.metadata 事件里的上游响应头（CPR 每轮一条，
// 带 turn-state、实际模型与额度头）；不是这类事件返回 nil。
func openAIWSResponseMetadataHeaders(frame []byte) http.Header {
	// 下行帧绝大多数是 delta，先做字节扫描再解析。
	if !bytes.Contains(frame, []byte("response.metadata")) || gjson.GetBytes(frame, "type").String() != "response.metadata" {
		return nil
	}
	headers := http.Header{}
	gjson.GetBytes(frame, "headers").ForEach(func(key, value gjson.Result) bool {
		if value.IsArray() {
			for _, item := range value.Array() {
				headers.Add(key.String(), item.String())
			}
		} else {
			headers.Add(key.String(), value.String())
		}
		return true
	})
	return headers
}

// openAIRawRelayWSTurnEnded 判断下行帧是否结束一轮。裸 error 也算：CPR 每轮失败只发这一条。
func openAIRawRelayWSTurnEnded(payload []byte) bool {
	return openAIWSPassthroughIsTerminalOutput(payload) || gjson.GetBytes(payload, "type").String() == "error"
}

func pingOpenAIRawRelayWSClient(ctx context.Context, conn *coderws.Conn, lastWrite *atomic.Int64) {
	ticker := time.NewTicker(openAIRawRelayWSPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 最近一个间隔里写过下行帧就不 ping，数据帧本身就防空闲断线。库给控制帧的写固定 5 秒
			// 截止、超时直接断连接，只在真空闲时发，才不会在发送缓冲堵住时误断正常的一轮。
			if time.Since(time.Unix(0, lastWrite.Load())) < openAIRawRelayWSPingInterval {
				continue
			}
			// 只为保活，不据 pong 判死活：等 pong 超时库不会断连接。
			pingCtx, cancel := context.WithTimeout(ctx, openAIRawRelayWSPingInterval)
			_ = conn.Ping(pingCtx)
			cancel()
		}
	}
}

func logOpenAIRawRelayWS(format string, args ...any) {
	logger.LegacyPrintf("service.openai_ws_raw_relay", "[OpenAI WS raw relay] "+format, args...)
}
