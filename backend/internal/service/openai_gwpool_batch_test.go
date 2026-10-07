package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func stableFeedbackPolicy(binding string, now time.Time) gatewayPoolFeedbackPolicy {
	return gatewayPoolFeedbackPolicy{Binding: binding, UpdatedAt: now,
		Recommendation: gwpool.CooldownRecommendation{Seconds: 3600, Samples: 6, Source: "account",
			ReportIntervalSeconds: 21600, ReportPolicyExpiresAt: now.Add(24 * time.Hour)}}
}

func TestGatewayPoolBatchHoldDeadlineRetryAndExpiryAreIndependent(t *testing.T) {
	now := time.Now().UTC()
	policy := stableFeedbackPolicy("binding", now)
	box := gatewayPoolOutbox{Policies: map[string]gatewayPoolFeedbackPolicy{"g": policy}}
	first := gatewayPoolPendingReport{Binding: "binding", Report: gwpool.CooldownReport{
		Gateway: "g", Result: "full", WindowSeconds: 3600, ElapsedSeconds: 3600}}
	prepareGatewayPoolReportHold(&box, &first, now)
	require.Equal(t, now.Add(6*time.Hour), first.HoldUntil)
	box.Pending = append(box.Pending, first)
	second := first
	second.HoldUntil = time.Time{}
	prepareGatewayPoolReportHold(&box, &second, now.Add(time.Hour))
	require.Equal(t, first.HoldUntil, second.HoldUntil, "arrivals cannot move the six-hour deadline")
	require.False(t, gatewayPoolReportReady(box, first, "binding", now.Add(time.Hour)))
	require.True(t, gatewayPoolReportReady(box, first, "binding", now.Add(6*time.Hour)))
	first.NextAt = now.Add(7 * time.Hour)
	box.Pending = make([]gatewayPoolPendingReport, gwpool.CooldownBatchLimit)
	require.False(t, gatewayPoolReportReady(box, first, "binding", now.Add(6*time.Hour)), "pressure cannot clear retry backoff")
	first.NextAt = time.Time{}
	first.Permanent = true
	require.False(t, gatewayPoolReportReady(box, first, "binding", now.Add(6*time.Hour)))
	first.Permanent = false
	box.Pending = nil
	policy.Recommendation.ReportPolicyExpiresAt = now.Add(time.Minute)
	box.Policies["g"] = policy
	require.True(t, gatewayPoolReportReady(box, first, "binding", now.Add(time.Hour)), "policy expiry resumes reporting")
}

func TestGatewayPoolBatchHoldsStableSuccessButFlushesNewFailureWithoutDroppingEvents(t *testing.T) {
	calls := 0
	var received []gwpool.CooldownReport
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		rec := map[string]any{"seconds": 3600, "samples": 6, "source": "account",
			"report_interval_seconds": 21600, "report_policy_expires_at": time.Now().Add(24 * time.Hour)}
		if r.URL.Path == "/cooldown/reports" {
			var in struct {
				Reports []gwpool.CooldownReport `json:"reports"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
			received = append(received, in.Reports...)
			var results []map[string]any
			for _, report := range in.Reports {
				results = append(results, map[string]any{"id": report.ID, "status": 200})
			}
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"ok": true, "results": results}))
			return
		}
		var report gwpool.CooldownReport
		require.NoError(t, json.NewDecoder(r.Body).Decode(&report))
		received = append(received, report)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"ok": true, "recommendation": rec}))
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	report := func(full bool, at time.Time) {
		svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
			Gateway: "g", AttemptAt: at, WindowSeconds: 3600, ElapsedSeconds: 3600, Full: full,
		})
	}
	now := time.Now().UTC()
	report(true, now.Add(-time.Second))
	svc.flushGatewayPoolReports(context.Background())
	require.Equal(t, 1, calls)
	report(true, now)
	svc.flushGatewayPoolReports(context.Background())
	require.Equal(t, 1, calls, "stable success must be held rather than sent immediately")
	report(false, now.Add(time.Millisecond))
	svc.flushGatewayPoolReports(context.Background())
	require.Equal(t, 2, calls, "failure must flush itself and held raw events in one request")
	require.Len(t, received, 3)
	require.True(t, received[1].ObservedAt.Equal(now), "batching preserves the observation clock")
	current, _ := repo.GetByID(context.Background(), account.ID)
	require.Empty(t, readGatewayPoolOutbox(current, time.Now()).Pending)
}

func TestGatewayPoolBatchAccountPressurePartialAcksAndConcurrentAppend(t *testing.T) {
	var svc *OpenAIGatewayService
	var account *Account
	var received []gwpool.CooldownReport
	calls := 0
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "/cooldown/reports", r.URL.Path)
		var in struct {
			Reports []gwpool.CooldownReport `json:"reports"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		received = in.Reports
		svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
			Gateway: "concurrent", AttemptAt: now, WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
		})
		var results []map[string]any
		for i, report := range in.Reports {
			status := 200
			if i == 1 {
				status = 409
			}
			if i == 2 {
				status = 503
			}
			results = append(results, map[string]any{"id": report.ID, "status": status})
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"ok": true, "results": results}))
	}))
	defer server.Close()
	account = gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	var repo *gatewayRuntimeRepo
	svc, repo = gatewayRuntimeService(account)
	binding := gatewayPoolReportBinding(account, gatewayPoolAccountTag(account, gwpoolTestIdentity))
	require.NoError(t, svc.changeGatewayPoolOutbox(context.Background(), 1, func(_ *Account, box *gatewayPoolOutbox) {
		box.Policies = map[string]gatewayPoolFeedbackPolicy{}
		for i := range gwpool.CooldownBatchLimit {
			box.Policies[fmt.Sprintf("g-%d", i)] = stableFeedbackPolicy(binding, now)
		}
	}))
	for i := range gwpool.CooldownBatchLimit {
		svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
			Gateway: fmt.Sprintf("g-%d", i), AttemptAt: now, WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
		})
	}
	restarted := &OpenAIGatewayService{accountRepo: repo}
	restarted.flushGatewayPoolReports(context.Background())
	require.Equal(t, 1, calls, "32 across all combinations trigger one early batch, including after restart")
	require.Len(t, received, gwpool.CooldownBatchLimit)
	current, _ := repo.GetByID(context.Background(), 1)
	box := readGatewayPoolOutbox(current, time.Now())
	require.EqualValues(t, 30, box.Sent)
	require.Len(t, box.Pending, 3)
	require.True(t, box.Pending[0].Permanent)
	require.False(t, box.Pending[1].Permanent)
	require.Equal(t, 1, box.Pending[1].Attempts)
	require.Equal(t, "concurrent", box.Pending[2].Report.Gateway, "partial ACK must merge rather than replace fresh queue")
}

func TestGatewayPoolBatchCatalogueChangeReleasesHoldWithoutClearingNetworkBackoff(t *testing.T) {
	now := time.Now().UTC()
	account := gwpoolTestAccount(1)
	binding := gatewayPoolReportBinding(account, gatewayPoolAccountTag(account, gwpoolTestIdentity))
	policy := stableFeedbackPolicy(binding, now.Add(-time.Minute))
	pending := gatewayPoolPendingReport{Binding: binding, HoldUntil: now.Add(6 * time.Hour), NextAt: now.Add(time.Minute),
		Report: gwpool.CooldownReport{Gateway: "g"}}
	box := gatewayPoolOutbox{Policies: map[string]gatewayPoolFeedbackPolicy{"g": policy}, Pending: []gatewayPoolPendingReport{pending}}
	var store openAICodexCookieStore
	changed := policy.Recommendation
	changed.Seconds = 7200
	store.noteGatewayPoolFeedbackPolicy(account, gwpoolTestIdentity, "g", &changed)
	require.True(t, store.syncGatewayPoolFeedbackPolicies(&box, now))
	require.False(t, store.syncGatewayPoolFeedbackPolicies(&box, now), "an applied revocation is not another database write")
	require.True(t, box.Pending[0].HoldUntil.IsZero())
	require.True(t, box.Policies["g"].Immediate)
	require.False(t, gatewayPoolReportReady(box, box.Pending[0], binding, now))
	require.True(t, gatewayPoolReportReady(box, box.Pending[0], binding, now.Add(time.Minute)))
}

func TestGatewayPoolBatchLateAckCannotReviveRevokedPolicy(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(fmt.Sprint(revoke), func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			now := time.Now().UTC()
			rec := stableFeedbackPolicy("", now).Recommendation
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				<-release
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "recommendation": rec})
			}))
			defer server.Close()
			account := gwpoolTestAccount(1)
			account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
			account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
			svc, repo := gatewayRuntimeService(account)
			svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
				Gateway: "g", AttemptAt: now, WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
			})
			done := make(chan struct{})
			go func() { defer close(done); svc.flushGatewayPoolReports(context.Background()) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("report request did not start")
			}
			newRec := rec
			newRec.Seconds = 7200
			update := &newRec
			if revoke {
				update = nil
			}
			svc.codexCookies.noteGatewayPoolFeedbackPolicy(account, gwpoolTestIdentity, "g", update)
			close(release)
			<-done
			current, _ := repo.GetByID(context.Background(), 1)
			box := readGatewayPoolOutbox(current, time.Now())
			svc.codexCookies.syncGatewayPoolFeedbackPolicies(&box, time.Now())
			binding := gatewayPoolReportBinding(account, gatewayPoolAccountTag(account, gwpoolTestIdentity))
			updateEvidence, _ := svc.codexCookies.poolFeedbackPolicies.Load(binding + "\x00g")
			require.False(t, box.Policies["g"].stable(binding, time.Now()),
				"late ACK must not overwrite a newer catalogue change/revocation: policy=%+v update=%+v", box.Policies["g"], updateEvidence)
		})
	}
}

func TestGatewayPoolBatchPolicyCacheExpiresOldBindings(t *testing.T) {
	var store openAICodexCookieStore
	store.poolFeedbackPolicies.Store("old-binding\x00g", gatewayPoolRecommendation{at: time.Now().Add(-25 * time.Hour)})
	store.noteGatewayPoolFeedbackPolicy(gwpoolTestAccount(1), gwpoolTestIdentity, "g", nil)
	_, exists := store.poolFeedbackPolicies.Load("old-binding\x00g")
	require.False(t, exists)
}
