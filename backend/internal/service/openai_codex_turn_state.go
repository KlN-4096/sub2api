package service

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	s.observeOpenAITurnStateMint(c, account, state)
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
	s.observeOpenAITurnStateMint(c, account, staged.Get(openAICodexTurnStateHeader))
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

// 已废弃（2026-09-23）：手填覆写同样依赖「注入 292 能换回正常服务」，已失效，后续版本移除。
//
// openAITurnStateOverrideExtraKey 是账号级 turn-state 覆写开关。空值=功能不存在，
// 出站行为与改动前逐字节一致。
//
// 用途是排查「回合状态影响上游算力档位」这类假设：turn-state 由上游每轮新铸、
// 绑死在铸它的那个 session 上，真实 Codex 客户端自己管理它（每轮新建的 OnceLock），
// 外部注入不进去，只能在网关这一层强制改写。
const openAITurnStateOverrideExtraKey = "openai_turn_state_override"

// maxOpenAITurnStateOverrideLen 是覆写值长度上限。实测 blob 为 292/312 字符，
// 留足余量的同时挡住把整个请求体误粘进来这类事故。
const maxOpenAITurnStateOverrideLen = 4096

// maxUsageCodexTurnStateLen 是写进 usage_logs 的上游观测值上限。与写入校验上限
// 分开：一个约束管理员能配什么，一个约束我们记什么，语义不同不该共用常量
// （对照 maxUsageUpstreamRequestIDLen）。实测 blob 为 292/312 字符。
const maxUsageCodexTurnStateLen = 4096

// readOpenAITurnStateOverrides 读手填覆写表。解析失败按未配置处理——宁可不注入，
// 也不要拿半个损坏的结构去改写出站头。
//
// 走 marshal/unmarshal 而不是裸类型断言，与 readOpenAITurnStatePool 同一套做法：
// extra 是 JSONB，同一个键在「刚从请求体解出来」和「从 DB 读回来」两条路径上的
// 具体 Go 类型不保证相同，裸断言失败是静默的——覆写永不生效且没有任何线索。
func readOpenAITurnStateOverrides(a *Account) map[string]string {
	if a == nil {
		return nil
	}
	raw, ok := a.Extra[openAITurnStateOverrideExtraKey]
	if !ok || raw == nil {
		return nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var table map[string]string
	if err := json.Unmarshal(encoded, &table); err != nil {
		// 旧的单字符串形态会落到这里：它没有可用的模型归属，按未配置处理。
		return nil
	}
	out := make(map[string]string, len(table))
	for model, blob := range table {
		if model = strings.TrimSpace(model); model != "" && strings.TrimSpace(blob) != "" {
			out[model] = strings.TrimSpace(blob)
		}
	}
	return out
}

// OpenAICodexTurnStateOverride 返回本次模型对应的手填覆写值；未配置、模型对不上、
// 过期或账号类型不适用时返回空串。只对最终落到 ChatGPT Codex 后端的账号生效
// （oauth / setup-token / cpr）——其余上游根本不认这个头，写进去是纯污染。
//
// 为什么按模型存：turn-state 绑死在铸它的那个模型上，换个模型那张票就不认了。blob
// 本身是密文（信封里只有铸造时间戳），系统无从得知它来自哪个模型，只能由管理员在
// 填的时候指定。取不到本次模型时不注入——这里没有「不限模型」这种配置了。
//
// 手填值与候选池同一条有效期：turn-state 自铸造起 1 小时可用，过期就不再注入。
// 这道闸必须放在这个共享入口上——HTTP 与 WS 两条路径都从这里取值，放到调用点就会
// 漏掉其中一条。过期后按「未配置」处理：不注入、不报错，等管理员换一条新的。
//
// 为什么非加不可：手填只在自动接管关闭时生效，而失效归因（noteOpenAITurnStateRejected）
// 要求自动接管开着。所以一条过期的手填票会对每个请求注入、每次换回一个 400
// invalid_encrypted_content，而且永远不会被任何机制发现。
func (a *Account) OpenAICodexTurnStateOverride(model string) string {
	if a == nil || !a.TargetsChatGPTCodexUpstream() {
		return ""
	}
	if model = strings.TrimSpace(model); model == "" {
		return ""
	}
	value := ""
	for configured, blob := range readOpenAITurnStateOverrides(a) {
		// 大小写不敏感，与候选池的 alive() 保持同一套模型判据。
		if strings.EqualFold(configured, model) {
			value = blob
			break
		}
	}
	if value == "" || !openAITurnStateBlobExpired(value, a.openAITurnStateStaleAfter(), time.Now()) {
		return value
	}
	logOpenAITurnStateAuto("account=%d model=%s manual turn-state override expired, not injecting", a.ID, model)
	return ""
}

// openAITurnStateBlobExpired 判一条 blob 是否已过铸造后 ttl。
// 信封解不出来时按不过期处理，与候选池的 expired() 同一套取舍：宁可注进去撞一次
// 400，也不要因为解码失败把整个功能静默关掉。
func openAITurnStateBlobExpired(blob string, ttl time.Duration, now time.Time) bool {
	env, ok := parseOpenAITurnStateEnvelope(blob)
	if !ok || env.MintedAt.IsZero() {
		return false
	}
	return !now.Before(env.MintedAt.Add(ttl))
}

// applyOpenAICodexTurnStateOverrideWSManualOnly 是 WS 路径的值形态入口：只应用手填覆写。
//
// 必须排在 guardOpenAICodexTurnStateEcho / guardOpenAICodexTurnStateValue 之后：
// 守卫只剥不注，覆写是管理员的显式动作，要能盖过剥离结果——否则「配了但不生效」
// 是最难排查的那种失败。
//
// 自动接管刻意不覆盖 WS。它的降智判定和失效判定都挂在「一次请求」的 gin 上下文上，
// 而下游 WS 直通是一条长连接跑多个回合（openai_ws_v2_passthrough_adapter.go 的帧
// 循环全程共用同一个 c）：注入标记会在整条连接上粘住，把后续每一轮的铸造结果都算到
// 第一次注入头上，失效判定直接失真。首版按 HTTP-only 落地，WS 保持既有手填行为。
func (s *OpenAIGatewayService) applyOpenAICodexTurnStateOverrideWSManualOnly(c *gin.Context, account *Account, current string) string {
	// 整个 WS 上下文都不参与自动接管——包括 WS ingress 的 HTTP 桥，它会拿同一个 c
	// 去走 passthrough 的出站构建（openai_ws_http_bridge.go），不挡住就漏进去了。
	markOpenAITurnStateAutoSkipped(c)
	// 与 HTTP 侧同理：每次重新判定前先清注入标记。attempt 1 走 HTTP 注入过、
	// attempt 2 failover 到另一个账号且走 WS 时，不清就会把上一个账号的注入值
	// 记到这一轮的使用记录上（overridden=true / source=manual，而本轮根本没注入）。
	clearOpenAITurnStateInjected(c)
	if account == nil || account.IsOpenAITurnStateAutoEnabled() {
		return current
	}
	if manual := account.OpenAICodexTurnStateOverride(openAITurnStateRequestModel(c)); manual != "" {
		// 记进上下文：帧填充据此判断「本次是不是覆写」，使用记录据此记 overridden/来源。
		markOpenAITurnStateInjected(c, manual, turnStateSourceManual)
		markOpenAITurnStateSent(c, account, manual)
		return manual
	}
	markOpenAITurnStateSent(c, account, current)
	return current
}

// applyOpenAICodexTurnStateOverrideHeader 是请求头形态。未配置时一个字节都不碰
// （不做多余的 Set，避免改变原有的头顺序/大小写）。
func (s *OpenAIGatewayService) applyOpenAICodexTurnStateOverrideHeader(c *gin.Context, account *Account, h http.Header) {
	if h == nil {
		return
	}
	// 真实流量水位（猎手的空闲门槛）记在出站这一刻：按响应头记的话，上游不铸 turn-state 的
	// 成功请求就不算流量，缺票的模型会被空闲门槛挡住。探测自己不算。
	if account != nil && account.TargetsChatGPTCodexUpstream() && !openAITurnStateProbeContext(c) {
		s.noteOpenAITurnStateTraffic(account.ID, openAITurnStateRequestModel(c), time.Now())
	}
	// 每个 failover attempt 都重新判定：c 在整个重试循环里是同一个。
	clearOpenAITurnStateInjected(c)
	if override, _ := s.resolveOpenAITurnStateOverride(c, account); override != "" {
		h.Set(openAICodexTurnStateHeader, override)
	}
	// 记下本次真正出站的值（可能来自客户端回带，也可能是刚注入的）。
	markOpenAITurnStateSent(c, account, h.Get(openAICodexTurnStateHeader))
}

// ValidateOpenAITurnStateAutoExtra 校验自动接管的配置键。
//
// 开关只能是 bool：写成字符串 "true" 时 getExtraBool 返回 false，开关静默失效——
// UI 之外用 API 配置时最容易踩这个。候选池等运行态键由网关维护，不在这里校验。
func ValidateOpenAITurnStateAutoExtra(extra map[string]any) error {
	if extra == nil {
		return nil
	}
	if raw, ok := extra[openAITurnStateAutoExtraKey]; ok && raw != nil {
		if _, ok := raw.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", openAITurnStateAutoExtraKey)
		}
	}
	return nil
}

// maxOpenAITurnStateOverrideModels 是手填覆写表的条目上限。Codex 侧同时在用的模型
// 就那么几个，16 条足够，同时挡住把整张表当垃圾桶用。
const maxOpenAITurnStateOverrideModels = 16

// ValidateOpenAITurnStateOverrideExtra 校验并规范化 extra 里的 turn-state 覆写表。
//
// 形态是 {模型: blob}。空模型名、空 blob 一律剔除；整表空了就把键删掉。每条 blob
// 必须是 Fernet 信封形状——urlsafe base64、解出至少 57 字节（1 版本 + 8 时间戳 +
// 16 IV + 32 HMAC）、首字节 0x80。这道校验挡的是手滑粘错内容后把垃圾原样发给上游。
func ValidateOpenAITurnStateOverrideExtra(extra map[string]any) error {
	if extra == nil {
		return nil
	}
	raw, ok := extra[openAITurnStateOverrideExtraKey]
	if !ok || raw == nil {
		// 显式传 JSON null 与未配置等价，别在 extra 里留个 null。
		delete(extra, openAITurnStateOverrideExtraKey)
		return nil
	}
	table, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be an object keyed by model", openAITurnStateOverrideExtraKey)
	}
	if len(table) > maxOpenAITurnStateOverrideModels {
		return fmt.Errorf("%s exceeds %d models", openAITurnStateOverrideExtraKey, maxOpenAITurnStateOverrideModels)
	}
	cleaned := make(map[string]any, len(table))
	// 取值按大小写不敏感匹配（OpenAICodexTurnStateOverride 用 EqualFold），而 Go map
	// 的遍历顺序是随机的：留着 {"GPT-5": A, "gpt-5": B} 这种表，注出去的是哪条每次
	// 调用都可能不同。与其让它随机，不如当场拒掉。
	seen := make(map[string]string, len(table))
	for model, v := range table {
		model = strings.TrimSpace(model)
		if model == "" {
			// 空模型名是畸形输入，不是「清空这条票」——后者由空 blob 表达。
			// 静默丢掉会让管理员看到「保存成功但未配置」。
			return fmt.Errorf("%s has an empty model name", openAITurnStateOverrideExtraKey)
		}
		if prev, dup := seen[strings.ToLower(model)]; dup {
			return fmt.Errorf("%s has case-conflicting models %q and %q",
				openAITurnStateOverrideExtraKey, prev, model)
		}
		seen[strings.ToLower(model)] = model
		blob, ok := v.(string)
		if !ok {
			return fmt.Errorf("%s[%s] must be a string", openAITurnStateOverrideExtraKey, model)
		}
		// 空 blob = 删掉这个模型的票，是 UI 上「清空输入框」的正常语义，不报错。
		if blob = strings.TrimSpace(blob); blob == "" {
			continue
		}
		if err := validateOpenAITurnStateBlob(fmt.Sprintf("%s[%s]", openAITurnStateOverrideExtraKey, model), blob); err != nil {
			return err
		}
		cleaned[model] = blob
	}
	if len(cleaned) == 0 {
		delete(extra, openAITurnStateOverrideExtraKey)
		return nil
	}
	extra[openAITurnStateOverrideExtraKey] = cleaned
	return nil
}

// validateOpenAITurnStateBlob 校验单条 turn-state blob 的信封形状。
func validateOpenAITurnStateBlob(label, value string) error {
	if len(value) > maxOpenAITurnStateOverrideLen {
		return fmt.Errorf("%s exceeds %d characters", label, maxOpenAITurnStateOverrideLen)
	}
	decoded, err := base64.URLEncoding.WithPadding(base64.StdPadding).DecodeString(value)
	if err != nil {
		decoded, err = base64.RawURLEncoding.DecodeString(value)
	}
	if err != nil {
		return fmt.Errorf("%s must be urlsafe base64", label)
	}
	if len(decoded) < 57 || decoded[0] != 0x80 {
		return fmt.Errorf("%s does not look like a Codex turn-state blob", label)
	}
	return nil
}

// usageCodexTurnStatePtr 从上游响应头取本次新铸的 turn-state，写进使用记录。
// 与 usageUpstreamRequestIDPtr 同型：取不到返回 nil（列保持 NULL）。
// 保留：用量表「Turn-State」入站列的数据源，292 功能（手填覆写等）移除时不删。
func usageCodexTurnStatePtr(h http.Header) *string {
	return truncateUsageTurnState(extractOpenAICodexTurnState(h))
}

// truncateUsageTurnState 收边到列上限；空值返回 nil（列保持 NULL）。
func truncateUsageTurnState(state string) *string {
	if state == "" {
		return nil
	}
	if len(state) > maxUsageCodexTurnStateLen {
		state = state[:maxUsageCodexTurnStateLen]
		// 与 usageUpstreamRequestIDPtr 同型：切在多字节字符中间会让 Postgres
		// 拒掉整行用量。blob 是 ASCII base64，实际到不了这里，但对齐是免费的。
		for len(state) > 0 && !utf8.ValidString(state) {
			state = state[:len(state)-1]
		}
		if state == "" {
			return nil
		}
	}
	return &state
}

// usageCodexTurnStateOverriddenPtr 记录本次请求是否真的注入了覆写值。
//
// source 由 handler 从请求上下文取出后放进 OpenAIRecordUsageInput（RecordUsage 是
// 异步的，拿不到 gin.Context）。取「实际注入」而非「账号配了开关」：自动接管下
// 只有被判定降智的 session 才注入，两者并不等价。
// 账号类型不适用时返回 nil（列保持 NULL），与「没注入」区分开。
func usageCodexTurnStateOverriddenPtr(account *Account, source string) *bool {
	if account == nil || !account.TargetsChatGPTCodexUpstream() {
		return nil
	}
	overridden := strings.TrimSpace(source) != ""
	return &overridden
}

// usageCodexTurnStateSentPtr 记录本次出站实际带的 turn-state。
// 与 usageCodexTurnStatePtr（上游**新铸**的）分开：带了 turn-state 的请求只有 8%
// 会拿到新铸值，只记新铸的话，注入场景九成以上都是空的。
func usageCodexTurnStateSentPtr(account *Account, sent string) *string {
	if account == nil || !account.TargetsChatGPTCodexUpstream() {
		return nil
	}
	sent = strings.TrimSpace(sent)
	if sent == "" {
		return nil
	}
	return truncateUsageTurnState(sent)
}

// usageCodexTurnStateSourcePtr 记录覆写来源：manual / auto / auto_stale，没注入为 nil。
func usageCodexTurnStateSourcePtr(account *Account, source string) *string {
	if account == nil || !account.TargetsChatGPTCodexUpstream() {
		return nil
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return nil
	}
	return &source
}
