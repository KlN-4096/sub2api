package openai_ws_v2

import (
	"strings"
	"time"
)

// IsResponseFrameEvent 判断上游消息算不算响应帧。sub2api 的首字（first_token_ms）统一记第一个响应帧，
// 无类型帧与 keepalive、error、response.failed 不算（docs/conventions/usage-logs.md「首字」）。
func IsResponseFrameEvent(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "", "keepalive", "error", "response.failed":
		return false
	}
	return true
}

// notePendingTurnFrame 记下客户端发出 response.create 后、本轮第一个带 response id 的事件之前到达的
// 第一个响应帧（codex.rate_limits、codex.response.metadata 等）。轮次是串行的，这段时间的帧属于待开始的这一轮。
func (s *relayState) notePendingTurnFrame(eventType, responseID string, now time.Time) {
	if responseID != "" || s.activeTurn != nil || !s.pendingTurnFrameAt.IsZero() ||
		s.pendingTurnStart.Load() == nil || !IsResponseFrameEvent(eventType) {
		return
	}
	s.pendingTurnFrameAt = now
}

// takePendingTurnFirstToken 在本轮开始计时时取出并清掉 notePendingTurnFrame 记下的首帧。
// 客户端在等待中又发了请求、起点被换掉时，旧帧早于新起点，不算。
func (s *relayState) takePendingTurnFirstToken(startAt time.Time) *int {
	at := s.pendingTurnFrameAt
	s.pendingTurnFrameAt = time.Time{}
	if at.IsZero() || at.Before(startAt) {
		return nil
	}
	ms := int(at.Sub(startAt).Milliseconds())
	return &ms
}

// dropPendingTurnStart 用于不经计时就收掉待开始轮次的地方（无 id 的终止事件、二进制终止帧），
// 连同已记的首帧一起清掉，不带进下一轮。这类轮次没有用时，也不报首字。
func (s *relayState) dropPendingTurnStart() {
	s.consumePendingTurnStartedAt()
	s.pendingTurnFrameAt = time.Time{}
}
