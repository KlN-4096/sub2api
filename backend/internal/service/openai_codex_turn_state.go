package service

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// openAICodexTurnStateHeader 是 Codex 的回合状态头。上游铸造该不透明 blob，客户端在
// 同一回合的后续请求中原样回带。客户端的三个捕获点：/responses SSE 的 HTTP 响应头
// （codex-api/src/sse/responses.rs:64-70）、/responses/compact 响应头（endpoint/compact.rs:58-64）、
// WS 的 response.metadata 事件 headers（sse/responses.rs:219-226 → endpoint/
// responses_websocket.rs:764-767）。WS 握手响应头不是捕获点：core 建连时传
// turn_state=None（core/src/client.rs:1174、:1239-1242），握手上的值客户端拿不到。
const openAICodexTurnStateHeader = "x-codex-turn-state"

// turn-state blob 是上游在"出站身份"（含 #5553 指纹收敛改写后的 installation/session/
// thread 标识）下铸造的，同身份回放自洽；跨身份回放（failover 换号后客户端仍回带旧账号
// 的 blob）是代理链独有、真实 Codex 永远不会产生的矛盾信号。
//
// 溯源表按 blob 值记录铸造者，不按会话：客户端侧的 turn_state 是每轮新建的 OnceLock
// （core/src/client.rs:292、:522-526，四处 set 全是 `let _ =` 首写生效，
// core/tests/suite/turn_state.rs:252-257 钉住"第二个值被忽略"），一轮内换过号后它仍回带
// 最早那个 blob。按"会话 → 最近一次铸造账号"记录会同时犯两个错：把该账号自己的合法回带
// 剥掉，又把别的账号的 blob 放行。按值记录则与承载通道、时序都无关。
//
// 铸造者按"凭证域身份"计，不按本地账号行：同一 ChatGPT 账号的多个本地行（含 spark 影子行）
// 刻意共享同一出站身份（codexAccountIdentityNamespace），上游看到的是同一个客户端，它们
// 之间回带 blob 不是矛盾信号，剥了反而制造矛盾。
type openAICodexTurnStateOrigin struct {
	owner     string
	expiresAt time.Time
}

// openAICodexTurnStateKey 用 blob 的哈希做键：blob 不透明且可能很长，哈希把键长钉死。
func openAICodexTurnStateKey(state string) string {
	sum := sha256.Sum256([]byte(state))
	return hex.EncodeToString(sum[:12])
}

// openAICodexTurnStateOwner 是溯源表里"铸造者"的键：出站身份所属的凭证域。影子行自身没有
// 凭据，各入口已把解析到的母账号暂存在 gin 上下文（prepareCodexAccountIdentitySource），
// 这里读同一份，保证"谁的身份出站、blob 就记在谁名下"。没有凭证域（API-key 等）退回本地行 ID。
// 故意不再按下游 API Key 分域：blob 只活一个 turn（每轮新建的 OnceLock），同一客户端不会在一个
// turn 内换 Key，而不同 Key 的客户端拿不到彼此的 blob。
func openAICodexTurnStateOwner(c *gin.Context, account *Account) string {
	source := codexAccountIdentitySource(c, account)
	if source == nil {
		return ""
	}
	if namespace := codexAccountIdentityNamespace(source); namespace != "" {
		return namespace
	}
	if source.ID <= 0 {
		return ""
	}
	return "id:" + strconv.FormatInt(source.ID, 10)
}

// relayOpenAICodexTurnState 将上游响应中的 turn-state 显式写入下游响应头，并记录铸造
// 者。上游无该头时主动清除 writer 上可能残留的上一 failover attempt 的值——否则换号
// 后旧账号的 blob 会粘到新账号的响应上，这正是本文件要防止的跨账号矛盾。
func (s *OpenAIGatewayService) relayOpenAICodexTurnState(c *gin.Context, account *Account, upstream http.Header) {
	if c == nil || c.Writer == nil {
		return
	}
	canonical := http.CanonicalHeaderKey(openAICodexTurnStateHeader)
	state := extractOpenAICodexTurnState(upstream)
	if state == "" {
		c.Writer.Header().Del(canonical)
		return
	}
	c.Writer.Header().Set(canonical, state)
	s.noteOpenAICodexTurnStateOrigin(c, account, state)
}

// stageOpenAICodexTurnState 将上游 turn-state 暂存到延迟提交的响应头集合（首输出守卫
// 路径先缓存头、见到首个输出事件才提交）。
func stageOpenAICodexTurnState(dst *http.Header, upstream http.Header) {
	if dst == nil {
		return
	}
	canonical := http.CanonicalHeaderKey(openAICodexTurnStateHeader)
	state := extractOpenAICodexTurnState(upstream)
	if state == "" {
		if *dst != nil {
			dst.Del(canonical)
		}
		return
	}
	if *dst == nil {
		*dst = http.Header{}
	}
	dst.Set(canonical, state)
}

// noteStagedOpenAICodexTurnStateCommitted 在暂存响应头真正写入下游时记录铸造者。
// 按值记录之后，"记早了"不再有害（客户端不会回带它从未收到的 blob，那条记录只会随
// TTL 过期），但记录点仍放在提交处：这里才拿得到最终交给客户端的那个值。
func (s *OpenAIGatewayService) noteStagedOpenAICodexTurnStateCommitted(c *gin.Context, account *Account, staged http.Header) {
	if staged == nil {
		return
	}
	s.noteOpenAICodexTurnStateOrigin(c, account, staged.Get(openAICodexTurnStateHeader))
}

func extractOpenAICodexTurnState(upstream http.Header) string {
	if upstream == nil {
		return ""
	}
	return strings.TrimSpace(upstream.Get(openAICodexTurnStateHeader))
}

// noteOpenAICodexTurnStateOrigin 记录（blob → 铸造者）。
func (s *OpenAIGatewayService) noteOpenAICodexTurnStateOrigin(c *gin.Context, account *Account, state string) {
	if s == nil || account == nil {
		return
	}
	state = strings.TrimSpace(state)
	if state == "" {
		return
	}
	owner := openAICodexTurnStateOwner(c, account)
	if owner == "" {
		return
	}
	s.openaiCodexTurnStateOrigins.Store(openAICodexTurnStateKey(state), openAICodexTurnStateOrigin{
		owner:     owner,
		expiresAt: time.Now().Add(s.openAIWSSessionStickyTTL()),
	})
	s.sweepOpenAICodexTurnStateOrigins()
}

// noteOpenAICodexTurnStateFromWSEvent 记录上游 WS 事件里铸出的 turn-state。客户端在 WS 上
// 持有的 blob 只来自被原样转发的 response.metadata 事件（codex-api/src/sse/responses.rs:219-226
// → endpoint/responses_websocket.rs:764-767，头名大小写不敏感匹配见 sse/responses.rs:292）。
// 不在这里记，纯 WS 会话就永远查不到铸造者，回声守卫等于空转。
func (s *OpenAIGatewayService) noteOpenAICodexTurnStateFromWSEvent(c *gin.Context, account *Account, frame []byte) {
	if s == nil || account == nil || len(frame) == 0 {
		return
	}
	// 下行帧绝大多数是 delta，先做一次字节扫描再解析。
	if !containsASCIIFold(frame, []byte(openAICodexTurnStateHeader)) {
		return
	}
	if gjson.GetBytes(frame, "type").String() != "response.metadata" {
		return
	}
	headers := gjson.GetBytes(frame, "headers")
	if !headers.IsObject() {
		return
	}
	headers.ForEach(func(key, value gjson.Result) bool {
		if !strings.EqualFold(key.String(), openAICodexTurnStateHeader) {
			return true
		}
		s.noteOpenAICodexTurnStateOrigin(c, account, value.String())
		return false
	})
}

// openAICodexTurnStateMintedByOther 只在"查得到且不是本凭证域铸的"时为真。查不到就放行：
// 可能是别的实例铸的、也可能已过期，不猜。
func (s *OpenAIGatewayService) openAICodexTurnStateMintedByOther(c *gin.Context, account *Account, state string) bool {
	if s == nil || account == nil {
		return false
	}
	state = strings.TrimSpace(state)
	if state == "" {
		return false
	}
	key := openAICodexTurnStateKey(state)
	raw, ok := s.openaiCodexTurnStateOrigins.Load(key)
	if !ok {
		return false
	}
	origin, ok := raw.(openAICodexTurnStateOrigin)
	if !ok {
		s.openaiCodexTurnStateOrigins.Delete(key)
		return false
	}
	if !origin.expiresAt.IsZero() && time.Now().After(origin.expiresAt) {
		s.openaiCodexTurnStateOrigins.Delete(key)
		return false
	}
	owner := openAICodexTurnStateOwner(c, account)
	return owner == "" || origin.owner != owner
}

// guardOpenAICodexTurnStateEcho 出站守卫：客户端回带的 turn-state 若已知由其他凭证域铸造
// 则剥离，同身份或无溯源记录时保持原样。只剥离、不注入——真实 Codex 客户端会按自身回合
// 语义自行回带；服务端注入是 Claude 兼容桥（无法回带的客户端）的专属行为。
func (s *OpenAIGatewayService) guardOpenAICodexTurnStateEcho(c *gin.Context, account *Account, h http.Header) {
	if s == nil || h == nil {
		return
	}
	if s.openAICodexTurnStateMintedByOther(c, account, h.Get(openAICodexTurnStateHeader)) {
		h.Del(openAICodexTurnStateHeader)
	}
}

// guardOpenAICodexTurnStateValue 是 guardOpenAICodexTurnStateEcho 的值形态：WS 三条路径的
// turn-state 不落在出站请求头集合上（握手前先读出；双开时握手不带，帧内只承载客户端自己的
// 值），在每个取值点套同一条守卫。已知由其他凭证域铸造则返回空串。
func (s *OpenAIGatewayService) guardOpenAICodexTurnStateValue(c *gin.Context, account *Account, state string) string {
	state = strings.TrimSpace(state)
	if state == "" || s.openAICodexTurnStateMintedByOther(c, account, state) {
		return ""
	}
	return state
}

// guardOpenAICodexWSFrameTurnState 剥离 WS 帧 client_metadata 内已知异凭证域铸造的 turn-state。
// 真客户端把该 blob 放在帧内（core/src/client.rs:1792-1793），failover 换号后照样回带旧账号
// 的值——与 HTTP 头守卫同一条规则：只剥离、不注入。
func (s *OpenAIGatewayService) guardOpenAICodexWSFrameTurnState(c *gin.Context, account *Account, payload []byte) []byte {
	path := "client_metadata." + openAICodexTurnStateHeader
	state := gjson.GetBytes(payload, path).String()
	if state == "" || !s.openAICodexTurnStateMintedByOther(c, account, state) {
		return payload
	}
	if next, err := sjson.DeleteBytes(payload, path); err == nil {
		return next
	}
	return payload
}

// sweepOpenAICodexTurnStateOrigins 机会式清扫过期溯源记录：每 256 次写入全量遍历一轮，
// 防止仅靠读侧惰性删除导致的慢泄漏（blob 键无上界）。
func (s *OpenAIGatewayService) sweepOpenAICodexTurnStateOrigins() {
	if s.openaiCodexTurnStateWrites.Add(1)%256 != 0 {
		return
	}
	now := time.Now()
	s.openaiCodexTurnStateOrigins.Range(func(key, value any) bool {
		origin, ok := value.(openAICodexTurnStateOrigin)
		if !ok || (!origin.expiresAt.IsZero() && now.After(origin.expiresAt)) {
			s.openaiCodexTurnStateOrigins.Delete(key)
		}
		return true
	})
}
