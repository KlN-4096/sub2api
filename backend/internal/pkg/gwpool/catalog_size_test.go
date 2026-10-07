package gwpool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientGatewaysLargeCatalog(t *testing.T) {
	rows := make([]map[string]any, 106)
	for i := range rows {
		rows[i] = map[string]any{
			"name": fmt.Sprintf("unified-%d", i), "pair_ready": true,
			"metadata": strings.Repeat("x", 1100),
		}
	}
	body, err := json.Marshal(map[string]any{"account": "offline-account", "gateways": rows})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) <= 64<<10 {
		t.Fatal("fixture must exceed the former catalog limit")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	got, err := New(srv.URL, "offline-key", 0).Gateways(context.Background(), "offline-account")
	if err != nil || len(got) != len(rows) {
		t.Fatalf("large valid catalog: count=%d err=%v", len(got), err)
	}
}

func TestClientGatewaysRejectsOversizedAndTrailingCatalog(t *testing.T) {
	for name, body := range map[string]string{
		"oversized": `{"gateways":[],"padding":"` + strings.Repeat("x", 2<<20) + `"}`,
		"trailing":  `{"gateways":[]} {"gateways":[]}`,
		"truncated": `{"gateways":[`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, body)
			}))
			defer srv.Close()
			got, err := New(srv.URL, "offline-key", 0).Gateways(context.Background(), "")
			if !errors.Is(err, ErrPool) || got != nil {
				t.Fatalf("invalid catalog must remain unknown: count=%d err=%v", len(got), err)
			}
		})
	}
}
