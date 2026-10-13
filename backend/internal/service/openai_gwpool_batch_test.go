package service

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

// ackGatewayPoolBatch reads a /cooldown/reports or /contact/reports body and
// acknowledges every report, each result carrying extra.
func ackGatewayPoolBatch[T any](t *testing.T, w http.ResponseWriter, r *http.Request, extra map[string]any) []T {
	var in struct {
		Reports []json.RawMessage `json:"reports"`
	}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
	reports, results := make([]T, len(in.Reports)), make([]map[string]any, len(in.Reports))
	for i, raw := range in.Reports {
		var id struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal(raw, &reports[i]))
		require.NoError(t, json.Unmarshal(raw, &id))
		results[i] = map[string]any{"id": id.ID, "status": http.StatusOK}
		maps.Copy(results[i], extra)
	}
	require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"ok": true, "results": results}))
	return reports
}

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
		require.Equal(t, "/cooldown/reports", r.URL.Path)
		rec := map[string]any{"seconds": 3600, "samples": 6, "source": "account",
			"report_interval_seconds": 21600, "report_policy_expires_at": time.Now().Add(24 * time.Hour)}
		received = append(received, ackGatewayPoolBatch[gwpool.CooldownReport](t, w, r, map[string]any{"recommendation": rec})...)
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
	current, _ := repo.stored(account.ID)
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
	current, _ := repo.stored(1)
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
	store.noteGatewayPoolFeedbackPolicy(account, gwpoolTestIdentity, "g", &changed, time.Now())
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
				ackGatewayPoolBatch[gwpool.CooldownReport](t, w, r, map[string]any{"recommendation": rec})
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
			svc.codexCookies.noteGatewayPoolFeedbackPolicy(account, gwpoolTestIdentity, "g", update, time.Now())
			close(release)
			<-done
			current, _ := repo.stored(1)
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
	store.poolFeedbackPolicies.Store("old-binding\x00g", gatewayPoolFeedbackPolicyUpdate{at: time.Now().Add(-25 * time.Hour)})
	store.noteGatewayPoolFeedbackPolicy(gwpoolTestAccount(1), gwpoolTestIdentity, "g", nil, time.Now())
	_, exists := store.poolFeedbackPolicies.Load("old-binding\x00g")
	require.False(t, exists)
}

// Rows on one pool key share report requests; a clone row's copy of a report
// ID goes in the next request, never twice in one.
func TestGatewayPoolBatchSharesRequestsAcrossMembers(t *testing.T) {
	var cooldownBatches [][]string
	contactCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cooldown/reports":
			var gateways []string
			for _, report := range ackGatewayPoolBatch[gwpool.CooldownReport](t, w, r, nil) {
				gateways = append(gateways, report.Gateway)
			}
			cooldownBatches = append(cooldownBatches, gateways)
		case "/contact/reports":
			contactCalls++
			require.Len(t, ackGatewayPoolBatch[gwpool.ContactReport](t, w, r, nil), 2)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	svc, repo, accounts := gatewayPoolBatchMembers(server.URL)
	now := time.Now().UTC()
	for id, account := range accounts {
		for _, gateway := range []string{fmt.Sprint("own-", id), "shared"} {
			svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
				Gateway: gateway, AttemptAt: now, WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
			})
		}
		event := contactEvent(now, fmt.Sprint("v", id))
		event.Applied.AccountID = id
		svc.noteGatewayPoolContact(context.Background(), account, event)
	}
	svc.flushGatewayPoolReports(context.Background())
	require.Equal(t, 1, contactCalls, "both rows' contacts share one request")
	require.Len(t, cooldownBatches, 2, "the clone's copy of the shared report waits for a second request")
	require.ElementsMatch(t, []string{"own-1", "own-2", "shared"}, cooldownBatches[0])
	require.Equal(t, []string{"shared"}, cooldownBatches[1])
	for _, id := range []int64{1, 2} {
		current, _ := repo.stored(id)
		require.Empty(t, readGatewayPoolOutbox(current, time.Now()).Pending)
	}
}

// gatewayPoolBatchMembers are two rows of one credential on one pool key.
func gatewayPoolBatchMembers(url string) (*OpenAIGatewayService, *gatewayPoolAccountsRepo, map[int64]*Account) {
	repo := &gatewayPoolAccountsRepo{rows: map[int64]*gatewayRuntimeRepo{}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.codexCookies.accountByID = svc.gatewayPoolLedgerRow
	accounts := map[int64]*Account{}
	for _, id := range []int64{1, 2} {
		account := gwpoolTestAccount(id)
		account.Extra[openAIGatewayPoolBaseURLExtraKey] = url
		account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
		repo.rows[id] = &gatewayRuntimeRepo{account: *account}
		accounts[id] = account
	}
	return svc, repo, accounts
}

// A report the pool cannot accept fails alone; the rest of the pass is sent.
func TestGatewayPoolBatchInvalidReportFailsAlone(t *testing.T) {
	var received []gwpool.CooldownReport
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = append(received, ackGatewayPoolBatch[gwpool.CooldownReport](t, w, r, nil)...)
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	for _, gateway := range []string{"good", "bad"} {
		svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
			Gateway: gateway, AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
		})
	}
	require.NoError(t, svc.changeGatewayPoolOutbox(context.Background(), 1, func(_ *Account, box *gatewayPoolOutbox) {
		for i := range box.Pending {
			if box.Pending[i].Report.Gateway == "bad" {
				box.Pending[i].Report.Result = "network_error"
			}
		}
	}))
	svc.flushGatewayPoolReports(context.Background())
	require.Len(t, received, 1)
	require.Equal(t, "good", received[0].Gateway)
	current, _ := repo.stored(1)
	box := readGatewayPoolOutbox(current, time.Now())
	require.Len(t, box.Pending, 1)
	require.Equal(t, "bad", box.Pending[0].Report.Gateway)
	require.True(t, box.Pending[0].Permanent)
}

// A batch-level answer applies to every member's report in that batch.
func TestGatewayPoolBatchRefusalReachesEveryMember(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(status)
			}))
			defer server.Close()
			svc, repo, accounts := gatewayPoolBatchMembers(server.URL)
			for id, account := range accounts {
				svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
					Gateway: fmt.Sprint("g-", id), AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
				})
			}
			svc.flushGatewayPoolReports(context.Background())
			require.Equal(t, 1, calls)
			for id := range accounts {
				current, _ := repo.stored(id)
				box := readGatewayPoolOutbox(current, time.Now())
				require.Len(t, box.Pending, 1)
				require.Equal(t, 1, box.Pending[0].Attempts)
				require.Equal(t, status == http.StatusConflict, box.Pending[0].Permanent)
			}
		})
	}
}

// Stopping the reporter mid-send leaves every report queued with its attempts
// unspent, and nothing is sent after the stop.
func TestGatewayPoolBatchStopLeavesReportsUnspent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls++
		cancel()
	}))
	defer server.Close()
	svc, repo, accounts := gatewayPoolBatchMembers(server.URL)
	now := time.Now().UTC()
	for id, account := range accounts {
		svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
			Gateway: fmt.Sprint("g-", id), AttemptAt: now, WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
		})
		event := contactEvent(now, fmt.Sprint("v", id))
		event.Applied.AccountID = id
		svc.noteGatewayPoolContact(context.Background(), account, event)
	}
	svc.flushGatewayPoolReports(ctx)
	require.Equal(t, 1, calls)
	for id := range accounts {
		current, _ := repo.stored(id)
		box := readGatewayPoolOutbox(current, time.Now())
		require.Len(t, box.Pending, 2)
		for _, pending := range box.Pending {
			require.Zero(t, pending.Attempts)
			require.True(t, pending.NextAt.IsZero())
		}
	}
}
