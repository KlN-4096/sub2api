package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolWSWaitDetectsDisconnectBeforeUpstreamResponse(t *testing.T) {
	waiting := make(chan *GatewayPoolWSWaitClient, 1)
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		client := StartGatewayPoolWSWaitClient(r.Context(), conn)
		waiting <- client
		<-client.Context().Done()
		client.Close()
		close(finished)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer func() { _ = conn.CloseNow() }()
	client := <-waiting
	require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte("next-turn")))
	kind, payload, err := client.Read(ctx, time.Second)
	require.NoError(t, err)
	require.Equal(t, coderws.MessageText, kind)
	require.Equal(t, "next-turn", string(payload))
	// The dedicated read pump also handles client control pings while no
	// inference response and no next application frame are available.
	readDone := make(chan struct{})
	go func() { _, _, _ = conn.Read(ctx); close(readDone) }()
	require.NoError(t, conn.Ping(ctx))
	require.NoError(t, conn.CloseNow())
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("disconnect left ticket preparation alive")
	}
	<-readDone
}
