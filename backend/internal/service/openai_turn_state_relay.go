package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// turn-state 接力：账号开启后，按「凭证域 + 下游 API Key + 出站模型 + 会话」记住上游最近一次给出的
// 票，同会话同模型后续每条请求都带最新那张——发 A 回 B 就带 B，发 B 回 C 就带 C；上游没换票就继续带
// 当前这张。票绑模型（跨模型回带是 400），会话中途换模型或 compact 兜底换模型都不带另一个模型的票。
//
// 这刻意偏离真客户端：codex 的 turn_state 是每轮一个 OnceLock（core/src/client.rs:311、:1727），
// 一轮内只带第一张、新一轮从空开始。是否接力由管理员按账号决定，与降智防护同一配置区；两者同开时
// 出站几乎总带票，降智防护的「票被换掉」判据因此覆盖到每一轮。
//
// 范围：本地 OpenAI OAuth / setup-token 账号（非网关池，同降智防护）发往 ChatGPT 推理端点的 HTTP
// 请求，以及 CPR 原样中继的 /v1/responses（只改这一个头，请求体照旧原文）。不参与：WS 上游与 WS
// 入站（含 WS 转 HTTP 桥，票走帧内 client_metadata）、请求体自带票的请求（两个载体不能不一致）、
// 图片与 compact（含 handler 按体信号改判的 compact）、没有模型或会话标识（prompt_cache_key）的请求。
//
// 收票落在单次 HTTP 往返上（doOpenAIUpstreamRoundTrip），降智防护的业务重发也一样推进接力；防护的
// Luna 确认不记票（模型不同，openai_quality_guard_probe.go）：
//   - 200 没给新票（或回显所发那张）：上游认了所发的票，没有有效条目时记下它（客户端自带的也算）；
//     已有条目不动，免得一张更早发出的票冲掉并发请求刚接上的新票。
//   - 200 给了新票：所发的票已被换掉，立即丢；新票等这条流以 response.completed 收尾（没有任何 SSE
//     行的正文读到 EOF）才接，且不顶掉铸得更晚的有效票。上游先回 200 发票、随后 response.failed
//     的那张，之后从不被认（2026-10-20 pro1 实测）；被降智防护拦下关掉的响应同理不接。
//   - 400：这一发带的正是接力票时丢掉（票解不开时上游回 400 invalid_encrypted_content），下一条
//     不再盖票，等上游重新给。其它状态码不动：429 / 5xx 带回的票是限流、故障的副产物。
//
// 票只在进程内存里，重启即清空；自铸造（信封时间戳）起 openAITurnStateRelayTTL 后停带。

// openAITurnStateRelayTTL：上游约 60 分钟换票（信封里没有过期时间），留 5 分钟余量——组装时还在
// 期限内、到上游已过期的票会被换掉，降智防护会把它当成被覆盖（实测 <55 分钟 0/8943 被换）。
const openAITurnStateRelayTTL = 55 * time.Minute

type openAITurnStateRelayEntry struct {
	state     string
	expiresAt time.Time
}

// openAITurnStateRelayRequestKey 把会话键挂在出站请求的 ctx 上，传输边界据此收票。
// 降智防护的确认与重发都从原请求 Clone，键随之带过去。
type openAITurnStateRelayRequestKey struct{}

// openAITurnStateRelayKey 按凭证域分：同一 ChatGPT 账号的多个本地行共享出站身份，票通用；
// 再按下游 API Key 分，别的用户拿同一个 prompt_cache_key 进不了这条链；再按出站模型分。
func openAITurnStateRelayKey(c *gin.Context, account *Account, model, sessionID string) string {
	model, sessionID = strings.TrimSpace(model), strings.TrimSpace(sessionID)
	owner := openAICodexTurnStateOwner(c, account)
	if model == "" || sessionID == "" || owner == "" {
		return ""
	}
	return owner + "\x00" + strconv.FormatInt(getAPIKeyIDFromContext(c), 10) + "\x00" + model + "\x00" + sessionID
}

// openAITurnStateRelayURL：CPR 原样中继打 CPR 自己的 /v1/responses（cprRelayTargetURL），其余只认
// ChatGPT 推理端点。
func openAITurnStateRelayURL(account *Account, u *url.URL) bool {
	if account.IsCPR() {
		return strings.HasSuffix(u.Path, "/v1/responses")
	}
	return gatewayPoolInferenceURL(u)
}

// withoutOpenAITurnStateRelay 让这条出站不收票：从业务请求 Clone 出来、换了模型的请求（降智防护的 Luna 确认）用。
func withoutOpenAITurnStateRelay(ctx context.Context) context.Context {
	return context.WithValue(ctx, openAITurnStateRelayRequestKey{}, "")
}

// applyOpenAITurnStateRelay 在出站守卫之后、记录出站值之前调用：有接力票就盖到请求头上（客户端
// 自带的也盖），并把会话键挂到请求 ctx 上供传输边界收票。不参与接力时原样返回。
func (s *OpenAIGatewayService) applyOpenAITurnStateRelay(c *gin.Context, account *Account, req *http.Request, body []byte, sessionID string) *http.Request {
	if s == nil || req == nil || account == nil || !account.turnStateRelayEnabled() ||
		!openAITurnStateRelayURL(account, req.URL) || isOpenAIResponsesCompactPath(c) || openAIImagesWireTarget(req.Context()) != "" ||
		(c != nil && c.GetBool("openai_ws_http_bridge")) ||
		gjson.GetBytes(body, "client_metadata."+openAICodexTurnStateHeader).Exists() {
		return req
	}
	key := openAITurnStateRelayKey(c, account, gjson.GetBytes(body, "model").String(), sessionID)
	if key == "" {
		return req
	}
	if state := s.loadOpenAITurnStateRelay(key); state != "" {
		req.Header.Set(openAICodexTurnStateHeader, state)
	}
	return req.WithContext(context.WithValue(req.Context(), openAITurnStateRelayRequestKey{}, key))
}

// settleOpenAITurnStateRelay 在单次 HTTP 往返后收票，规则见文件头。
func (s *OpenAIGatewayService) settleOpenAITurnStateRelay(request *http.Request, resp *http.Response) {
	if s == nil || request == nil || resp == nil {
		return
	}
	key, _ := request.Context().Value(openAITurnStateRelayRequestKey{}).(string)
	if key == "" {
		return
	}
	sent := extractOpenAICodexTurnState(request.Header)
	switch resp.StatusCode {
	case http.StatusOK:
		fresh := extractOpenAICodexTurnState(resp.Header)
		if fresh == "" || fresh == sent {
			s.putOpenAITurnStateRelay(key, sent, func(_, _ openAITurnStateRelayEntry) bool { return true })
			return
		}
		s.dropOpenAITurnStateRelay(key, sent)
		resp.Body = &openAITurnStateRelayBody{ReadCloser: resp.Body, commit: func() { s.storeOpenAITurnStateRelay(key, fresh) }}
	case http.StatusBadRequest:
		s.dropOpenAITurnStateRelay(key, sent)
	}
}

// dropOpenAITurnStateRelay 只丢这一发带出去的那张：并发请求可能已经换上了更新的票。
func (s *OpenAIGatewayService) dropOpenAITurnStateRelay(key, sent string) {
	if raw, ok := s.openaiTurnStateRelay.Load(key); ok && sent != "" {
		if entry, _ := raw.(openAITurnStateRelayEntry); entry.state == sent {
			s.openaiTurnStateRelay.CompareAndDelete(key, raw)
		}
	}
}

func (s *OpenAIGatewayService) loadOpenAITurnStateRelay(key string) string {
	raw, ok := s.openaiTurnStateRelay.Load(key)
	if !ok {
		return ""
	}
	entry, _ := raw.(openAITurnStateRelayEntry)
	if !time.Now().Before(entry.expiresAt) {
		s.openaiTurnStateRelay.CompareAndDelete(key, raw)
		return ""
	}
	return entry.state
}

// storeOpenAITurnStateRelay 记下这条会话的最新票，不顶掉铸得更晚的有效票（迟到收尾的旧票）。
func (s *OpenAIGatewayService) storeOpenAITurnStateRelay(key, state string) {
	s.putOpenAITurnStateRelay(key, state, func(current, next openAITurnStateRelayEntry) bool {
		return current.expiresAt.After(next.expiresAt)
	})
}

// putOpenAITurnStateRelay 写入 state；现有有效条目在 keep 为 true 时保留。同一张票重复记不续期：
// 过期按铸造时刻算，解不出信封时以第一次见到它为准。
func (s *OpenAIGatewayService) putOpenAITurnStateRelay(key, state string, keep func(current, next openAITurnStateRelayEntry) bool) {
	if state == "" {
		return
	}
	now := time.Now()
	next := newOpenAITurnStateRelayEntry(state, now)
	for {
		raw, loaded := s.openaiTurnStateRelay.LoadOrStore(key, next)
		if !loaded {
			break
		}
		current, _ := raw.(openAITurnStateRelayEntry)
		if current.state == state || now.Before(current.expiresAt) && keep(current, next) {
			return
		}
		if s.openaiTurnStateRelay.CompareAndSwap(key, raw, next) {
			break
		}
	}
	s.sweepOpenAITurnStateRelay(now)
}

func newOpenAITurnStateRelayEntry(state string, now time.Time) openAITurnStateRelayEntry {
	minted := now
	if env, ok := parseOpenAITurnStateEnvelope(state); ok {
		minted = env.MintedAt
	}
	return openAITurnStateRelayEntry{state: state, expiresAt: minted.Add(openAITurnStateRelayTTL)}
}

// sweepOpenAITurnStateRelay 机会式清扫过期条目：每 256 次写入全量遍历一轮，会话键无上界，
// 不能只靠读侧惰性删除。
func (s *OpenAIGatewayService) sweepOpenAITurnStateRelay(now time.Time) {
	if s.openaiTurnStateRelayWrites.Add(1)%256 != 0 {
		return
	}
	s.openaiTurnStateRelay.Range(func(key, value any) bool {
		if entry, _ := value.(openAITurnStateRelayEntry); !now.Before(entry.expiresAt) {
			s.openaiTurnStateRelay.CompareAndDelete(key, value)
		}
		return true
	})
}

// openAITurnStateRelayBody 等响应真正成功收尾才接上游给的新票：读到 response.completed（事件行或
// data 的 type）；一行 SSE 都没有的正文（JSON）读到 EOF。失败、截断、提前关闭都不接。不看
// Content-Type（OAuth 上游可能不带它却照发 SSE）；只看每行开头，不缓存正文。
type openAITurnStateRelayBody struct {
	io.ReadCloser
	commit func()
	line   []byte
	sse    bool
}

// openAITurnStateRelayLineHead 是认事件类型要看的行首长度：`data: {"type":"response.completed"` 只有 34 字节。
const openAITurnStateRelayLineHead = 128

func (b *openAITurnStateRelayBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.commit != nil {
		b.scan(p[:n])
	}
	if err == io.EOF && b.commit != nil {
		b.check() // 最后一行可能没有换行
		if b.commit != nil && !b.sse {
			b.done()
		}
	}
	return n, err
}

func (b *openAITurnStateRelayBody) scan(chunk []byte) {
	for len(chunk) > 0 && b.commit != nil {
		end := bytes.IndexByte(chunk, '\n')
		part := chunk
		if end >= 0 {
			part = chunk[:end]
		}
		if room := openAITurnStateRelayLineHead - len(b.line); room > 0 {
			b.line = append(b.line, part[:min(room, len(part))]...)
		}
		if end < 0 {
			return
		}
		chunk = chunk[end+1:]
		b.check()
	}
}

// check 看完一行就清掉行首缓冲。
func (b *openAITurnStateRelayBody) check() {
	line := strings.TrimSpace(string(b.line))
	b.line = b.line[:0]
	if event, ok := extractOpenAISSEEventLine(line); ok {
		b.sse = true
		if event == "response.completed" {
			b.done()
		}
	} else if data, ok := extractOpenAISSEDataLine(line); ok {
		b.sse = true
		if gjson.Get(data, "type").String() == "response.completed" {
			b.done()
		}
	}
}

func (b *openAITurnStateRelayBody) done() {
	commit := b.commit
	b.commit = nil
	commit()
}
