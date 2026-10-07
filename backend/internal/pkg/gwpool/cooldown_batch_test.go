package gwpool

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCooldownBatchLegacyFallbackKeepsIDsAndTimestampHeader(t *testing.T) {
	at := time.Now().UTC()
	var singles []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cooldown/reports" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if _, exists := body["observed_at"]; exists || r.Header.Get(cooldownObservedHeader) != at.Format(time.RFC3339Nano) {
			t.Error("legacy JSON shape or stable event header changed")
		}
		id, ok := body["id"].(string)
		if !ok {
			t.Error("missing report ID")
			return
		}
		singles = append(singles, id)
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()
	reports := []CooldownReport{
		{ID: strings.Repeat("a", 64), AccountTag: strings.Repeat("c", 64), Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full", ObservedAt: at},
		{ID: strings.Repeat("b", 64), AccountTag: strings.Repeat("c", 64), Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full", ObservedAt: at},
	}
	results, err := New(server.URL, "test-key", time.Second).ReportCooldownBatch(context.Background(), reports)
	if err != nil || len(results) != 2 || len(singles) != 2 || results[0].Status != 200 || singles[1] != reports[1].ID {
		t.Fatalf("fallback lost acknowledgement or stable ID: %+v %v %v", results, singles, err)
	}
}

func TestCooldownBatchRejectsAmbiguousAcknowledgements(t *testing.T) {
	id := strings.Repeat("a", 64)
	for _, result := range []string{
		`{"ok":true,"results":[]}`,
		`{"ok":true,"results":[{"id":"wrong","status":200}]}`,
		fmt.Sprintf(`{"ok":true,"results":[{"id":%q,"status":0}]}`, id),
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, result) }))
		report := CooldownReport{ID: id, AccountTag: strings.Repeat("b", 64), Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full"}
		_, err := New(server.URL, "test-key", time.Second).ReportCooldownBatch(context.Background(), []CooldownReport{report})
		server.Close()
		if err == nil {
			t.Fatalf("malformed acknowledgement accepted: %s", result)
		}
	}
}
