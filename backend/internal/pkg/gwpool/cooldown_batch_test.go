package gwpool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCooldownBatchDoesNotFallbackToLegacyEndpoint(t *testing.T) {
	at := time.Now().UTC()
	var singles []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cooldown/reports" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		singles = append(singles, r.URL.Path)
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()
	reports := []CooldownReport{
		{ID: strings.Repeat("a", 64), AccountTag: strings.Repeat("c", 64), Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full", ObservedAt: at},
		{ID: strings.Repeat("b", 64), AccountTag: strings.Repeat("c", 64), Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full", ObservedAt: at},
	}
	results, err := New(server.URL, "test-key", time.Second).ReportCooldownBatch(context.Background(), reports)
	var refused *PoolError
	if !errors.As(err, &refused) || refused.Status != http.StatusNotFound || len(results) != 0 || len(singles) != 0 {
		t.Fatalf("unsupported batch endpoint must fail without single-report fallback: %+v %v %v", results, singles, err)
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

// A full batch acknowledgement with recommendations is far larger than a single
// ticket response; truncating it would fail a batch the pool already stored.
func TestCooldownBatchAcceptsFullAcknowledgementWithRecommendations(t *testing.T) {
	at := time.Now().UTC()
	reports := make([]CooldownReport, CooldownBatchLimit)
	results := make([]string, CooldownBatchLimit)
	for i := range reports {
		id := fmt.Sprintf("%064x", i+1)
		reports[i] = CooldownReport{ID: id, AccountTag: strings.Repeat("c", 64), Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full", ObservedAt: at}
		results[i] = fmt.Sprintf(`{"id":%q,"status":200,"added":true,"recommendation":{"seconds":3600,"samples":6,"source":"account",`+
			`"report_interval_seconds":21600,"report_policy_expires_at":%q}}`, id, at.Add(time.Hour).Format(time.RFC3339Nano))
	}
	body := `{"ok":true,"results":[` + strings.Join(results, ",") + `]}`
	if len(body) <= maxResponseBytes {
		t.Fatalf("fixture must exceed the single-response read limit: %d bytes", len(body))
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, body) }))
	defer server.Close()
	got, err := New(server.URL, "test-key", time.Second).ReportCooldownBatch(context.Background(), reports)
	if err != nil || len(got) != CooldownBatchLimit || got[CooldownBatchLimit-1].Recommendation == nil {
		t.Fatalf("full acknowledgement rejected: %v (%d results)", err, len(got))
	}
}

// Contact reports of several members share one request, and each keeps its
// own acknowledgement.
func TestContactBatchCarriesMembersAndPerReportStatus(t *testing.T) {
	reports := []ContactReport{
		{ID: strings.Repeat("a", 64), AccountTag: strings.Repeat("c", 64), Gateway: "g", Model: "m", Criterion: ContactCriterion},
		{ID: strings.Repeat("b", 64), AccountTag: strings.Repeat("d", 64), Gateway: "g", Model: "m", Criterion: ContactCriterion},
	}
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		var in struct {
			Reports []ContactReport `json:"reports"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || r.URL.Path != "/contact/reports" || len(in.Reports) != 2 ||
			in.Reports[1].AccountTag != reports[1].AccountTag {
			t.Errorf("contact batch wire changed: %s %+v %v", r.URL.Path, in, err)
		}
		_, _ = fmt.Fprintf(w, `{"ok":true,"results":[{"id":%q,"status":200},{"id":%q,"status":409}]}`, reports[1].ID, reports[0].ID)
	}))
	defer server.Close()
	client := New(server.URL, "test-key", time.Second)
	results, err := client.ReportContactBatch(context.Background(), reports)
	if err != nil || len(results) != 2 || results[0] != (ContactReportResult{ID: reports[1].ID, Status: 200}) ||
		results[1] != (ContactReportResult{ID: reports[0].ID, Status: 409}) {
		t.Fatalf("per-report acknowledgements lost: %+v %v", results, err)
	}
	for _, bad := range [][]ContactReport{
		{reports[0], reports[0]},
		{{ID: "short", AccountTag: reports[0].AccountTag, Gateway: "g", Model: "m", Criterion: ContactCriterion}},
		make([]ContactReport, ContactBatchLimit+1),
	} {
		if _, err := client.ReportContactBatch(context.Background(), bad); err == nil {
			t.Fatalf("invalid batch sent: %d reports", len(bad))
		}
	}
	if hits != 1 {
		t.Fatalf("invalid batches reached the pool: hits=%d", hits)
	}
}
