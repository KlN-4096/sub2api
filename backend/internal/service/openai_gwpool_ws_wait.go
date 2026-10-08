package service

import (
	"context"
	"time"

	coderws "github.com/coder/websocket"
)

// A WS request context is not cancelled merely because a hijacked connection
// closes. Keep one reader alive during ticket preparation to observe disconnects
// and answer control pings. A single queued next turn is bounded backpressure.
type GatewayPoolWSWaitClient struct {
	ctx      context.Context
	cancel   context.CancelFunc
	conn     *coderws.Conn
	frames   chan openAIWSClientReadResult
	readDone chan struct{}
	pingDone chan struct{}
}

func StartGatewayPoolWSWaitClient(ctx context.Context, conn *coderws.Conn) *GatewayPoolWSWaitClient {
	ctx, cancel := context.WithCancel(ctx)
	client := &GatewayPoolWSWaitClient{ctx: ctx, cancel: cancel, conn: conn,
		frames: make(chan openAIWSClientReadResult, 1), readDone: make(chan struct{}), pingDone: make(chan struct{})}
	go client.read()
	go client.ping()
	return client
}

func (client *GatewayPoolWSWaitClient) Context() context.Context { return client.ctx }

func (client *GatewayPoolWSWaitClient) read() {
	defer close(client.readDone)
	defer client.cancel()
	for {
		kind, payload, err := client.conn.Read(client.ctx)
		if err != nil {
			return
		}
		select {
		case client.frames <- openAIWSClientReadResult{messageType: kind, payload: payload}:
		default:
			// Do not let queued application frames hide a disconnect behind an
			// unbounded buffer while an earlier turn waits for a ticket.
			_ = client.conn.CloseNow()
			return
		}
	}
}

func (client *GatewayPoolWSWaitClient) ping() {
	defer close(client.pingDone)
	ticker := time.NewTicker(gatewayPoolWaitKeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-client.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(client.ctx, gatewayPoolWaitConfigPoll)
			err := client.conn.Ping(ctx)
			cancel()
			if err != nil {
				client.cancel()
				return
			}
		}
	}
}

func (client *GatewayPoolWSWaitClient) Read(ctx context.Context, timeout time.Duration) (coderws.MessageType, []byte, error) {
	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timeoutCh = timer.C
		defer timer.Stop()
	}
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-client.ctx.Done():
		return 0, nil, client.ctx.Err()
	case <-timeoutCh:
		return 0, nil, NewOpenAIWSClientCloseError(coderws.StatusNormalClosure, "websocket idle timeout", context.DeadlineExceeded)
	case frame := <-client.frames:
		return frame.messageType, frame.payload, frame.err
	}
}

func (client *GatewayPoolWSWaitClient) Close() {
	client.cancel()
	_ = client.conn.CloseNow()
	<-client.readDone
	<-client.pingDone
}
