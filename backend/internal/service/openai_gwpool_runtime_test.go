package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

type gatewayRuntimeRepo struct {
	AccountRepository
	mu      sync.Mutex
	account Account
	fail    bool
	scans   chan struct{}
}

// GetByID returns the row the way the database does since migration 253: the
// runtime ledgers live apart and come only through LoadGatewayPoolLedgers, so a
// read that forgets to hydrate them sees none, as it would in production.
func (r *gatewayRuntimeRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	account, err := r.stored(id)
	if err == nil {
		for _, key := range gatewayPoolLedgerKeys {
			delete(account.Extra, key)
		}
	}
	return account, err
}

// stored is an independent copy of the row with its ledgers, for assertions.
func (r *gatewayRuntimeRepo) stored(_ int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, err := json.Marshal(r.account)
	if err != nil {
		return nil, err
	}
	var account Account
	err = json.Unmarshal(raw, &account)
	return &account, err
}

func (r *gatewayRuntimeRepo) LoadGatewayPoolLedgers(_ context.Context, ids []int64, keys []string) (map[int64]map[string]json.RawMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fakeGatewayPoolLedgers([]Account{r.account}, ids, keys)
}

// gatewayPoolWithoutLedgers is row as a database read returns it since
// migration 253, on its own Extra map.
func gatewayPoolWithoutLedgers(row Account) Account {
	row.Extra = maps.Clone(row.Extra)
	for _, key := range gatewayPoolLedgerKeys {
		delete(row.Extra, key)
	}
	return row
}

// fakeGatewayPoolLedgers serves LoadGatewayPoolLedgers from rows that keep
// their ledgers in Extra, as every row did before migration 253.
func fakeGatewayPoolLedgers(rows []Account, ids []int64, keys []string) (map[int64]map[string]json.RawMessage, error) {
	ledgers := map[int64]map[string]json.RawMessage{}
	for _, row := range rows {
		if !slices.Contains(ids, row.ID) {
			continue
		}
		for _, key := range keys {
			if value, ok := row.Extra[key]; ok {
				raw, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				if ledgers[row.ID] == nil {
					ledgers[row.ID] = map[string]json.RawMessage{}
				}
				ledgers[row.ID][key] = raw
			}
		}
	}
	return ledgers, nil
}

// fakeGatewayPoolLedgerWithout serves LoadGatewayPoolLedgerWithout the same way.
func fakeGatewayPoolLedgerWithout(rows []Account, id int64, kind string, omit []string) (json.RawMessage, error) {
	loaded, err := fakeGatewayPoolLedgers(rows, []int64{id}, []string{kind})
	if err != nil || loaded[id] == nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(loaded[id][kind], &fields); err != nil {
		return nil, err
	}
	for _, field := range omit {
		delete(fields, field)
	}
	return json.Marshal(fields)
}

func (r *gatewayRuntimeRepo) LoadGatewayPoolLedgerWithout(_ context.Context, id int64, kind string, omit ...string) (json.RawMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fakeGatewayPoolLedgerWithout([]Account{r.account}, id, kind, omit)
}

func (r *gatewayRuntimeRepo) WriteGatewayPoolLedgers(ctx context.Context, id int64, ledgers, tags map[string]any) error {
	patch := maps.Clone(ledgers)
	maps.Copy(patch, tags)
	return r.UpdateExtra(ctx, id, patch)
}

func (r *gatewayRuntimeRepo) UpdateExtra(_ context.Context, _ int64, patch map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("offline write failure")
	}
	if r.account.Extra == nil {
		r.account.Extra = map[string]any{}
	}
	raw, _ := json.Marshal(patch)
	var generic map[string]any
	_ = json.Unmarshal(raw, &generic)
	for key, value := range generic {
		r.account.Extra[key] = value
	}
	return nil
}

func (r *gatewayRuntimeRepo) FindByExtraField(ctx context.Context, _ string, _ any) ([]Account, error) {
	if r.scans != nil {
		select {
		case r.scans <- struct{}{}:
		default:
		}
	}
	account, err := r.GetByID(ctx, r.account.ID)
	return []Account{*account}, err
}

func gatewayRuntimeService(account *Account) (*OpenAIGatewayService, *gatewayRuntimeRepo) {
	repo := &gatewayRuntimeRepo{account: *account}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.codexCookies.accountByID = svc.gatewayPoolLedgerRow
	return svc, repo
}

func TestGatewayPoolFirstSendDoesNotCountShelfWaitOrReset(t *testing.T) {
	store := &openAICodexCookieStore{}
	pair := openAIGatewayPoolPair{version: "a", gateway: "g", cookie: "offline-cookie"}
	store.poolPairs.Store("id", pair)
	first := time.Now()
	store.gatewayPoolMarkSent("id", "a", first)
	store.gatewayPoolMarkSent("id", "a", first.Add(time.Minute))
	store.gatewayPoolMarkSent("id", "old", first.Add(-time.Hour))
	got, _ := store.cachedPoolPair("id")
	require.Equal(t, first, got.firstSent)
	age := time.Since(got.firstSent)
	require.Less(t, age, time.Second, "备用架待了3分钟，首次接触仍然是年轻窗口")
}

func TestGatewayPoolOutboxPersistsBeforeSendAndResumesAfterRestart(t *testing.T) {
	var reports []string
	var repo *gatewayRuntimeRepo
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		account, _ := repo.stored(1)
		require.Len(t, readGatewayPoolOutbox(account, time.Now()).Pending, 1, "先持久再发")
		raw, _ := io.ReadAll(r.Body)
		reports = append(reports, string(raw))
		if len(reports) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		ackGatewayPoolBatch[gwpool.CooldownReport](t, w, r, nil)
	}))
	defer server.Close()
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
		Gateway: "g", AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
	})
	require.Empty(t, reports, "入队本身不先发一次临时请求")
	svc.flushGatewayPoolReports(context.Background())
	current, _ := repo.stored(1)
	box := readGatewayPoolOutbox(current, time.Now())
	require.Len(t, box.Pending, 1)
	require.Equal(t, 1, box.Pending[0].Attempts)
	require.True(t, box.Pending[0].NextAt.After(time.Now()))
	require.NoError(t, svc.changeGatewayPoolOutbox(context.Background(), 1, func(_ *Account, b *gatewayPoolOutbox) {
		b.Pending[0].NextAt = time.Time{}
	}))
	restarted := &OpenAIGatewayService{accountRepo: repo}
	restarted.flushGatewayPoolReports(context.Background())
	current, _ = repo.stored(1)
	box = readGatewayPoolOutbox(current, time.Now())
	require.Empty(t, box.Pending)
	require.EqualValues(t, 1, box.Sent)
	require.Len(t, reports, 2)
	require.Equal(t, reports[0], reports[1], "重启重发保留同ID和载荷")
}

func TestGatewayPoolOutboxWriteFailureAndChangedBindingNeverSend(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:1"
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	sample := &gatewayPoolCooldownSample{Gateway: "g", AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true}
	repo.fail = true
	svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, sample)
	current, _ := repo.stored(1)
	require.Empty(t, readGatewayPoolOutbox(current, time.Now()).Pending)
	repo.fail = false
	svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, sample)
	require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{OpenAIGatewayPoolConsumerKeyExtraKey: "different-key"}))
	svc.flushGatewayPoolReports(context.Background())
	current, _ = repo.stored(1)
	box := readGatewayPoolOutbox(current, time.Now())
	require.Empty(t, box.Pending)
	require.EqualValues(t, 1, box.Discarded, "密钥/身份变更丢弃旧归属的报告，不能发到新配置")
}

func TestGatewayPoolOutboxBoundsAndPersistsExpiryWithoutSending(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:1"
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	for i := 0; i < gatewayPoolOutboxLimit+3; i++ {
		svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
			Gateway: "g", AttemptAt: time.Now().Add(time.Duration(i)), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
		})
	}
	current, _ := repo.stored(1)
	box := readGatewayPoolOutbox(current, time.Now())
	require.Len(t, box.Pending, gatewayPoolOutboxLimit)
	require.EqualValues(t, 3, box.Discarded)
	require.NoError(t, svc.changeGatewayPoolOutbox(context.Background(), 1, func(_ *Account, b *gatewayPoolOutbox) {
		for i := range b.Pending {
			b.Pending[i].CreatedAt = time.Now().Add(-gatewayPoolOutboxRetention - time.Hour)
			b.Pending[i].Permanent = true
		}
	}))
	svc.flushGatewayPoolReports(context.Background())
	current, _ = repo.stored(1)
	raw, _ := json.Marshal(current.Extra[openAIGatewayPoolOutboxExtraKey])
	require.NoError(t, json.Unmarshal(raw, &box))
	require.Empty(t, box.Pending, "数据库中的过期项也必须清理，不能只在读取时隐藏")
	require.EqualValues(t, gatewayPoolOutboxLimit+3, box.Discarded)
}

func TestGatewayPoolOutboxCompletionPreservesConcurrentAppend(t *testing.T) {
	var svc *OpenAIGatewayService
	var account *Account
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
			Gateway: "other", AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
		})
		ackGatewayPoolBatch[gwpool.CooldownReport](t, w, r, nil)
	}))
	defer server.Close()
	account = gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = server.URL
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	var repo *gatewayRuntimeRepo
	svc, repo = gatewayRuntimeService(account)
	svc.reportGatewayPoolCooldown(account, gwpoolTestIdentity, &gatewayPoolCooldownSample{
		Gateway: "first", AttemptAt: time.Now(), WindowSeconds: 3600, ElapsedSeconds: 3600, Full: true,
	})
	svc.flushGatewayPoolReports(context.Background())
	current, _ := repo.stored(1)
	box := readGatewayPoolOutbox(current, time.Now())
	require.EqualValues(t, 1, box.Sent)
	require.Len(t, box.Pending, 1)
	require.Equal(t, "other", box.Pending[0].Report.Gateway)
}

func TestGatewayPoolOutboxWorkerStartsOnceAndStops(t *testing.T) {
	svc, repo := gatewayRuntimeService(gwpoolTestAccount(1))
	repo.scans = make(chan struct{}, 1)
	svc.StartGatewayPoolReporter()
	worker := svc.gatewayReporter
	svc.StartGatewayPoolReporter()
	require.Same(t, worker, svc.gatewayReporter)
	select {
	case <-repo.scans:
	case <-time.After(time.Second):
		t.Fatal("worker did not resume its persisted queue at startup")
	}
	svc.StopGatewayPoolReporter()
	svc.StopGatewayPoolReporter()
	select {
	case <-worker.done:
	default:
		t.Fatal("Stop returned before worker exit")
	}
}

func TestGatewayPoolProbePreflightFailureDoesNotCountAsSent(t *testing.T) {
	for _, failure := range []error{gwpoolBareError(), &PluginTransportError{RequestSent: false, Code: "PLUGIN_X"}} {
		account := gwpoolTestAccount(1)
		account.Credentials["access_token"] = "fake-token"
		svc, _ := gatewayRuntimeService(account)
		svc.httpUpstream = &gwpoolErrorUpstream{err: failure}
		svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
			cookie: "offline-cookie", gateway: "g", version: "v",
		})
		_, conclusive, sent, first, err := svc.codexCookies.gatewayPoolWarmVerdict(
			context.Background(), account, gwpoolTestIdentity, OpenAIGatewayPoolApplied{Gateway: "g", Version: "v"},
			"offline-cookie", 1, func(ctx context.Context, cookie, state string) (int, string, error) {
				return svc.gatewayPoolWarmShot(ctx, account, "", cookie, gwpoolWarmModel, state)
			})
		require.Error(t, err)
		require.False(t, conclusive)
		require.False(t, sent)
		require.True(t, first.IsZero(), "明确未发出的错误不许启动票龄")
		pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
		require.True(t, pair.firstSent.IsZero())
	}
}

func TestGatewayPoolProbeNoSendEvidenceDoesNotCreateContactOrReturnUncertainTicket(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Credentials["access_token"] = "fake-token"
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.poolProbeObserved = svc.noteGatewayPoolProbeAndContact
	svc.httpUpstream = &gwpoolErrorUpstream{err: &url.Error{Op: "Post", URL: "https://example.test", Err: errors.New("dial failed")}}
	event := contactEvent(time.Now(), "ticket")
	_, conclusive, mayHaveSent, first, err := svc.codexCookies.gatewayPoolWarmVerdict(
		context.Background(), account, gwpoolTestIdentity, event.Applied, "offline-cookie", 1,
		func(ctx context.Context, cookie, state string) (int, string, error) {
			return svc.gatewayPoolWarmShot(ctx, account, "", cookie, gwpoolWarmModel, state)
		})
	require.Error(t, err)
	require.False(t, conclusive)
	require.True(t, mayHaveSent, "an uncertain transport error is still insufficient proof to return the ticket")
	require.True(t, first.IsZero(), "no response or write evidence must not invent an actual send")
	fresh, _ := repo.stored(1)
	require.Nil(t, fresh.Extra[openAIGatewayPoolContactsExtraKey])
}

type gatewayPoolWrittenErrorUpstream struct{ gwpoolErrorUpstream }

func (u *gatewayPoolWrittenErrorUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	trace := httptrace.ContextClientTrace(req.Context())
	if trace != nil && trace.WroteRequest != nil {
		trace.WroteRequest(httptrace.WroteRequestInfo{})
	}
	return nil, &url.Error{Op: "Post", URL: "https://example.test", Err: errors.New("response lost")}
}

func TestGatewayPoolProbeWriteEvidenceSurvivesLostResponse(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Credentials["access_token"] = "fake-token"
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc, repo := gatewayRuntimeService(account)
	svc.httpUpstream = &gatewayPoolWrittenErrorUpstream{}
	svc.codexCookies.poolProbeObserved = svc.noteGatewayPoolProbeAndContact
	event := contactEvent(time.Now(), "ticket")
	_, conclusive, sent, first, err := svc.codexCookies.gatewayPoolWarmVerdict(
		context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, "astra"), account, gwpoolTestIdentity,
		event.Applied, "offline-cookie", 1, func(ctx context.Context, cookie, state string) (int, string, error) {
			return svc.gatewayPoolWarmShot(ctx, account, "", cookie, "astra", state)
		})
	require.Error(t, err)
	require.False(t, conclusive)
	require.True(t, sent)
	require.False(t, first.IsZero())
	fresh, _ := repo.stored(1)
	state := readGatewayPoolContacts(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, "unknown", state.Rounds[0].Report.Outcome)
}

func TestGatewayPoolBusinessPreflightFailureDoesNotStartReusedPairAge(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = false
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:1"
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc := &OpenAIGatewayService{httpUpstream: &gwpoolErrorUpstream{err: &PluginTransportError{RequestSent: false}}}
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: "offline-cookie", gateway: "g", version: "v",
	})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "v", gatewayPoolProbeModelLuna)
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(request.Context(), ginCtx)
	_, _, err = svc.doOpenAIUpstreamOnce(request.WithContext(ctx), "", account)
	require.Error(t, err)
	pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.True(t, pair.firstSent.IsZero(), "复用票未出站，也不能提前启动计时")
}

func TestGatewayPoolProbeDispatchedErrorRetainsContact(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Credentials["access_token"] = "fake-token"
	svc, _ := gatewayRuntimeService(account)
	svc.httpUpstream = &gwpoolErrorUpstream{err: &PluginTransportError{RequestSent: true, Code: "PLUGIN_X"}}
	_, conclusive, sent, first, err := svc.codexCookies.gatewayPoolWarmVerdict(
		context.Background(), account, gwpoolTestIdentity, OpenAIGatewayPoolApplied{Gateway: "g", Version: "v"},
		"offline-cookie", 1, func(ctx context.Context, cookie, state string) (int, string, error) {
			return svc.gatewayPoolWarmShot(ctx, account, "", cookie, gwpoolWarmModel, state)
		})
	require.Error(t, err)
	require.False(t, conclusive)
	require.True(t, sent, "明确已发出但没响应的票不可当成未接触归还")
	require.False(t, first.IsZero())
}

func TestGatewayPoolStrictGuardReusesResolvedShadowIdentityForSentMark(t *testing.T) {
	account := gwpoolTestAccount(77)
	parentID := int64(1)
	account.ParentAccountID = &parentID
	account.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:1"
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
	svc := &OpenAIGatewayService{httpUpstream: &cookieRecordingUpstream{}}
	resolutions := 0
	svc.codexCookies.identity = func(_ context.Context, row *Account) (string, error) {
		require.True(t, row.IsShadow())
		resolutions++
		if resolutions > 2 {
			return "", errors.New("redundant parent lookup failed")
		}
		return gwpoolTestIdentity, nil
	}
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: "offline-cookie", gateway: "g", version: "v",
	})
	svc.codexCookies.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "v", "gpt-6-luna")
	request, err := http.NewRequest(http.MethodPost, gwpoolTestURL, nil)
	require.NoError(t, err)
	request.Header.Set(openAICodexTurnStateHeader, "business-state")
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, _ := withOpenAIGatewayPoolSink(request.Context(), ginCtx)
	response, _, err := svc.doOpenAIUpstreamOnce(request.WithContext(ctx), "", account)
	require.NoError(t, err)
	_ = response.Body.Close()
	require.Equal(t, 2, resolutions, "AttachRoute 一次、末端验票一次；发送后必须复用")
	pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.False(t, pair.firstSent.IsZero())
}

func TestGatewayPoolSideRequestsDoNotStartPairAgeFromSharedSink(t *testing.T) {
	for _, rawURL := range []string{
		"https://chatgpt.com/backend-api/wham/settings/user",
		"https://example.test/backend-api/codex/responses",
	} {
		account := gwpoolTestAccount(1)
		account.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:1"
		account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "test-key"
		svc := &OpenAIGatewayService{httpUpstream: &cookieRecordingUpstream{}}
		svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
			cookie: "offline-cookie", gateway: "g", version: "v",
		})
		ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx, _ := withOpenAIGatewayPoolSink(context.Background(), ginCtx)
		_, err := svc.codexCookies.AttachRoute(ctx, account, gwpoolTestURL, http.Header{})
		require.NoError(t, err, "模拟主请求已取票，但尚未实际出站")
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		require.NoError(t, err)
		response, _, err := svc.doOpenAIUpstreamOnce(request, "", account)
		require.NoError(t, err, "非接管请求不应套用主请求的严格票据闸")
		_ = response.Body.Close()
		pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
		require.True(t, pair.firstSent.IsZero(), "共享sink的侧请求没有使用池子票，不能给它计龄")
	}
}

// Every requested ID is answered: a deleted or no-longer-pool row is explicitly
// inactive, so the admin page's complete-batch freeze cannot wait forever.
func TestGatewayPoolRuntimeProgressAnswersEveryRequestedID(t *testing.T) {
	account := gwpoolTestAccount(1)
	svc, _ := gatewayRuntimeService(account)
	view, err := svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1, 999}, true)
	require.NoError(t, err)
	require.NotNil(t, view[1].Runtime)
	require.False(t, view[1].Inactive)
	require.True(t, view[999].Inactive, "deleted row")
	require.Nil(t, view[999].Runtime)

	off := gwpoolTestAccount(1)
	off.Extra[openAIGatewayPoolExtraKey] = false
	svc, _ = gatewayRuntimeService(off)
	view, err = svc.GatewayPoolRuntimeProgress(context.Background(), []int64{1}, true)
	require.NoError(t, err)
	require.True(t, view[1].Inactive, "pool switched off")
}
