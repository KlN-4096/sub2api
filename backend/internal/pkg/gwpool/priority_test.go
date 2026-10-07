package gwpool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewaysForModelCarriesPriorityAndValidatesMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "luna", r.URL.Query().Get("model"))
		_, _ = w.Write([]byte(`{"gateways":[{"name":"known","pair_ready":true,"datacenter_country":"us","priority":{"model":"luna","full":4,"samples":5}},{"name":"unknown","datacenter_country":"USA","priority":{"model":"astra","full":5,"samples":5}}]}`))
	}))
	defer server.Close()
	client := New(server.URL, "offline-key", time.Second)
	got, err := client.GatewaysForModel(context.Background(), "", "", "luna")
	require.NoError(t, err)
	require.Equal(t, "US", got[0].DatacenterCountry)
	require.Equal(t, 4, got[0].Priority.Full)
	require.Empty(t, got[1].DatacenterCountry)
	require.Nil(t, got[1].Priority)
}
