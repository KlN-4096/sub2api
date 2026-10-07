package repository

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestHTTPUpstreamConfirmationDoesNotWaitForOpenBusinessBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		require.NoError(t, http.NewResponseController(w).Flush())
		if r.URL.Path == "/business" {
			<-r.Context().Done()
		}
	}))
	defer server.Close()
	upstream := NewHTTPUpstream(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(service.WithHTTPUpstreamProfile(ctx, service.HTTPUpstreamProfileOpenAI),
		http.MethodGet, server.URL+"/business", nil)
	require.NoError(t, err)
	business, err := upstream.Do(request, "", 1, 1)
	require.NoError(t, err)
	defer func() { _ = business.Body.Close() }()
	confirmation, err := http.NewRequestWithContext(service.WithHTTPUpstreamProfile(ctx, service.HTTPUpstreamProfileOpenAIConfirmation),
		http.MethodGet, server.URL+"/confirmation", nil)
	require.NoError(t, err)
	response, err := upstream.Do(confirmation, "", 1, 1)
	require.NoError(t, err, "a confirmation must not wait for its own original streaming response to close")
	_ = response.Body.Close()
}
