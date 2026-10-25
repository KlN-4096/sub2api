package openai_ws_v2

import (
	"context"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestIsResponseFrameEvent(t *testing.T) {
	t.Parallel()

	for _, eventType := range []string{
		"codex.response.metadata", "codex.rate_limits", "response.created", "response.in_progress",
		"response.output_item.added", "response.output_text.delta", "response.completed",
	} {
		require.True(t, IsResponseFrameEvent(eventType), eventType)
	}
	for _, eventType := range []string{"", " ", "keepalive", "error", "response.failed"} {
		require.False(t, IsResponseFrameEvent(eventType), eventType)
	}
}

func TestObserveUpstreamMessageTurnFirstTokenCountsFramesBeforeResponseID(t *testing.T) {
	t.Parallel()

	relayStart := time.Unix(1000, 0)
	now := relayStart
	nowFn := func() time.Time { return now }
	state := &relayState{}
	observe := func(at time.Duration, message string) observedUpstreamEvent {
		now = relayStart.Add(at)
		return observeUpstreamMessage(state, []byte(message), relayStart, nowFn, nil)
	}
	turn := func(start time.Duration, frames ...string) *int {
		state.setPendingTurnStartedAt(relayStart.Add(start))
		var last observedUpstreamEvent
		for i, frame := range frames {
			last = observe(start+time.Duration(i+1)*100*time.Millisecond, frame)
		}
		require.True(t, last.terminal)
		return last.firstToken
	}
	const metadata = `{"type":"codex.response.metadata"}`
	created := func(id string) string { return `{"type":"response.created","response":{"id":"` + id + `"}}` }
	completed := func(id string) string { return `{"type":"response.completed","response":{"id":"` + id + `"}}` }

	first := turn(0, `{"type":"keepalive"}`, metadata, created("resp_1"), completed("resp_1"))
	require.NotNil(t, first)
	require.Equal(t, 200, *first, "keepalive 不算，codex.response.metadata 是本轮第一个响应帧")

	// 上一轮结束后、下一轮请求发出前到的帧不属于任何一轮，也不能挡住下一轮自己的无 id 帧。
	observe(1500*time.Millisecond, `{"type":"codex.rate_limits"}`)
	second := turn(2000*time.Millisecond, metadata, created("resp_2"), completed("resp_2"))
	require.NotNil(t, second)
	require.Equal(t, 100, *second)

	// 客户端在等待中又发了请求，起点被换掉：旧帧早于新起点，退回到首个带 id 的帧，不能出负数。
	state.setPendingTurnStartedAt(relayStart.Add(3000 * time.Millisecond))
	observe(3100*time.Millisecond, metadata)
	third := turn(3500*time.Millisecond, created("resp_3"), completed("resp_3"))
	require.NotNil(t, third)
	require.Equal(t, 100, *third)

	// 只有无 id 帧、以裸 error 收尾的一轮没有用时，不报首字，也不把帧留给下一轮。
	state.bareErrorEndsTurn = true
	bare := turn(5000*time.Millisecond, `{"type":"codex.rate_limits"}`, `{"type":"error","error":{"message":"boom"}}`)
	require.Nil(t, bare)
	require.True(t, state.pendingTurnFrameAt.IsZero())
}

func TestRelay_FirstTokenMsSkipsKeepaliveAndErrorFrames(t *testing.T) {
	t.Parallel()

	clientConn := newPassthroughTestFrameConn(nil, false)
	upstreamConn := newPassthroughTestFrameConn([]passthroughTestFrame{
		{msgType: coderws.MessageText, payload: []byte(`{"type":"keepalive"}`)},
		{msgType: coderws.MessageText, payload: []byte(`{"type":"error","error":{"type":"server_error","message":"boom"}}`)},
	}, true)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, _ := Relay(ctx, clientConn, upstreamConn, []byte(`{"type":"response.create","model":"gpt-5.3-codex","input":[]}`), RelayOptions{})
	require.Equal(t, int64(2), result.UpstreamToClientFrames)
	require.Nil(t, result.FirstTokenMs, "keepalive 与 error 不算响应帧")
}
