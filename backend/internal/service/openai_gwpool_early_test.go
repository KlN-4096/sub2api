package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestGatewayPoolEarlyBudgetPersistsAndFailsClosed(t *testing.T) {
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolEarlyEnabledKey] = true
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.poolEarlyClaim = svc.claimGatewayPoolEarly
	now := time.Now().UTC()
	require.True(t, svc.codexCookies.gatewayPoolEarlyDue(context.Background(), account, gwpoolTestIdentity))
	require.NoError(t, svc.claimGatewayPoolEarly(context.Background(), account, gwpoolTestIdentity, now))
	require.False(t, svc.codexCookies.gatewayPoolEarlyDue(context.Background(), account, gwpoolTestIdentity))
	clone := *account
	clone.ID = 42
	clone.Extra = map[string]any{openAIGatewayPoolExtraKey: true, openAIGatewayPoolEarlyEnabledKey: true}
	restarted := &openAICodexCookieStore{accountByID: func(ctx context.Context, id int64) (*Account, error) {
		if id == 42 {
			return &clone, nil
		}
		return repo.GetByID(ctx, id)
	}, historyByTag: func(ctx context.Context, _ string) ([]Account, error) {
		a, err := repo.GetByID(ctx, 1)
		return []Account{*a}, err
	}}
	require.False(t, restarted.gatewayPoolEarlyDue(context.Background(), &clone, gwpoolTestIdentity))
	restarted.historyByTag = nil
	require.True(t, restarted.gatewayPoolEarlyDue(context.Background(), &clone, gwpoolTestIdentity),
		"the clone has no own state; only shared durable history blocked it")
	delete(repo.account.Extra, openAIGatewayPoolEarlyStateKey)
	svc.codexCookies.poolEarlyAt.Delete(gatewayPoolLedgerIdentity(gwpoolTestIdentity))
	repo.fail = true
	require.Error(t, svc.claimGatewayPoolEarly(context.Background(), account, gwpoolTestIdentity, now))
	_, reserved := svc.codexCookies.poolEarlyAt.Load(gatewayPoolLedgerIdentity(gwpoolTestIdentity))
	require.False(t, reserved)
}

type gatewayEarlyAccountsRepo struct {
	AccountRepository
	rows map[int64]*gatewayRuntimeRepo
}

func (r *gatewayEarlyAccountsRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	return r.rows[id].GetByID(ctx, id)
}
func (r *gatewayEarlyAccountsRepo) UpdateExtra(ctx context.Context, id int64, patch map[string]any) error {
	return r.rows[id].UpdateExtra(ctx, id, patch)
}
func (r *gatewayEarlyAccountsRepo) FindByExtraField(ctx context.Context, key string, value any) ([]Account, error) {
	var accounts []Account
	for id := range r.rows {
		account, err := r.GetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if account.Extra[key] == value {
			accounts = append(accounts, *account)
		}
	}
	return accounts, nil
}

func TestGatewayPoolEarlyDifferentUsersShareOneAtomicLedgerBudget(t *testing.T) {
	repo := &gatewayEarlyAccountsRepo{rows: map[int64]*gatewayRuntimeRepo{}}
	for _, id := range []int64{1, 42} {
		account := rotationAccount(id, 7)
		account.Extra[openAIGatewayPoolEarlyEnabledKey] = true
		if id == 42 {
			account.Credentials["chatgpt_user_id"] = "user-b"
		}
		raw, err := json.Marshal(account)
		require.NoError(t, err)
		var copy Account
		require.NoError(t, json.Unmarshal(raw, &copy))
		repo.rows[id] = &gatewayRuntimeRepo{account: copy}
	}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.codexCookies.accountByID = repo.GetByID
	svc.codexCookies.historyByTag = func(ctx context.Context, tag string) ([]Account, error) {
		return repo.FindByExtraField(ctx, openAIGatewayLedgerTagExtraKey, tag)
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, id := range []int64{1, 42} {
		account, err := repo.GetByID(context.Background(), id)
		require.NoError(t, err)
		identity := openAIGatewayPoolAccountKey(account)
		require.True(t, svc.codexCookies.gatewayPoolEarlyDue(context.Background(), account, identity))
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if svc.claimGatewayPoolEarly(context.Background(), account, identity, time.Now().UTC()) == nil {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	require.EqualValues(t, 1, successes.Load())
	// Restarted service restores the budget for BOTH distinct pair-cache identities.
	restarted := &openAICodexCookieStore{accountByID: repo.GetByID, historyByTag: svc.codexCookies.historyByTag}
	for _, id := range []int64{1, 42} {
		account, _ := repo.GetByID(context.Background(), id)
		require.False(t, restarted.gatewayPoolEarlyDue(context.Background(), account, openAIGatewayPoolAccountKey(account)))
	}
}

func TestGatewayPoolEarlyProductionPathUsesBusinessModelAndSamePair(t *testing.T) {
	cookie := gwpoolTestPairCookie(t, "unified-142")
	fake := newGwpoolFakePool(t, cookie, 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolEarlyEnabledKey] = true
	account.Credentials["access_token"] = "offline-test-token"
	fake.configure(account)
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.poolEarlyClaim = svc.claimGatewayPoolEarly
	svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-142")
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "probe-state"}, {status: http.StatusOK}, {status: http.StatusOK},
	}}
	svc.httpUpstream = upstream
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(gwpoolEchoBody1))
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)
	resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", account)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Len(t, upstream.sentBodies, 3, "exactly A/B then waiting business, no extra confirmation")
	for _, body := range upstream.sentBodies {
		require.Equal(t, "gpt-6-astra", gjson.Get(body, "model").String(), "default Luna must not substitute for early Astra")
	}
	require.Equal(t, gwpoolEchoBody1, upstream.sentBodies[2])
	require.Equal(t, []string{"", "probe-state", ""}, upstream.sentState)
	require.Equal(t, []string{cookie, cookie, cookie}, upstream.sentCookies)
	pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Equal(t, cookie, pair.cookie)
	require.NotNil(t, pair.early)
	require.False(t, pair.firstSent.IsZero())
	require.EqualValues(t, 1, fake.hits.Load())
	fresh, _ := repo.GetByID(ctx, account.ID)
	require.False(t, readGatewayPoolEarlyState(fresh, gwpoolTestIdentity).IsZero())
}

func TestGatewayPoolEarlyStaleTicketDoesNotBlockNormalDifferentModel(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-143"), 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-143", PairReady: true}}
	account := fake.account(1) // early switch now off
	account.Credentials["access_token"] = "offline-test-token"
	upstream := &gwpoolEchoUpstream{replies: []gwpoolEchoReply{
		{status: http.StatusOK, minted: "probe-state"}, {status: http.StatusOK}, {status: http.StatusOK},
	}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: "old-cookie", gateway: "unified-142", version: "old-v", routeExpiresAt: time.Now().Add(-time.Second),
		early: newGatewayPoolEarlyAttempt("unified-142", "gpt-6-astra"),
	})
	body := `{"model":"gpt-6-sol","input":"x"}`
	req, err := http.NewRequest(http.MethodPost, gwpoolTestURL, strings.NewReader(body))
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(req.Context(), nil)
	resp, err := svc.doOpenAIUpstream(req.WithContext(ctx), "", account)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Len(t, upstream.sentBodies, 3)
	require.Equal(t, "gpt-6-luna", gjson.Get(upstream.sentBodies[0], "model").String())
	require.Equal(t, body, upstream.sentBodies[2])
	pair, _ := svc.codexCookies.cachedPoolPair(gwpoolTestIdentity)
	require.Nil(t, pair.early)
}

func TestGatewayPoolEarlyConcurrentVerdictAndCancelledFollower(t *testing.T) {
	store := &openAICodexCookieStore{}
	early := newGatewayPoolEarlyAttempt("g", "gpt-6-astra")
	applied := OpenAIGatewayPoolApplied{Gateway: "g", Version: "v", early: early}
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, early.model)
	started, resume := make(chan struct{}), make(chan struct{})
	var shots atomic.Int32
	shoot := func(_ context.Context, _, state string) (int, string, error) {
		if shots.Add(1) == 1 {
			close(started)
			<-resume
		}
		if state == "" {
			return http.StatusOK, "state", nil
		}
		return http.StatusOK, "", nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, _, _, _ = store.gatewayPoolWarmVerdict(ctx, gwpoolTestAccount(1), "id", applied, "offline", 1, shoot)
	}()
	<-started
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, known, _, _, err := store.gatewayPoolWarmVerdict(cancelled, gwpoolTestAccount(42), "id", applied, "offline", 1, shoot)
	require.Error(t, err)
	require.False(t, known)
	close(resume)
	wg.Wait()
	// Even after the pair has been evicted, its captured result must not launch a second A/B.
	full, known, _, _, err := store.gatewayPoolWarmVerdict(ctx, gwpoolTestAccount(42), "id", applied, "offline", 1, shoot)
	require.NoError(t, err)
	require.True(t, full && known)
	require.EqualValues(t, 2, shots.Load())
}

func TestGatewayPoolEarlyCooldownDoesNotLearnUnwaitedFailure(t *testing.T) {
	now := time.Now().UTC()
	c := gatewayPoolCooldown{WindowSeconds: 8 * 3600, UpdatedAt: now.Add(-time.Hour), Until: now.Add(7 * time.Hour)}
	c.beginEarly(now, 3600)
	sample, changed := c.observe(now.Add(time.Second), openAIGatewayVerdictDegraded, 3600)
	require.True(t, changed)
	require.Nil(t, sample)
	require.Equal(t, 8*3600, c.WindowSeconds)
	require.True(t, validGatewayPoolCooldown(&c, now.Add(time.Second), 3600))
}

func TestGatewayPoolEarlyOnlyLocalCoolingAndSingleTicket(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		normal, used, ready, enabled bool
		wantEarly                    bool
	}{
		{"local cooling", false, false, true, true, true},
		{"normal supply remains", true, false, true, true, false},
		{"pool already used", false, true, true, true, false},
		{"nonready", false, false, false, true, false},
		{"disabled", false, false, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: tc.ready, UsedByYou: tc.used}}
			if tc.normal {
				fake.listGateways = append(fake.listGateways, gwpoolFakeGateway{Name: "unified-143", PairReady: true})
			}
			account := rotationAccount(1, 7)
			account.Extra[openAIGatewayPoolEarlyEnabledKey] = tc.enabled
			fake.configure(account)
			svc, _ := gatewayRuntimeService(account)
			svc.codexCookies.poolEarlyClaim = svc.claimGatewayPoolEarly
			svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-142")
			ctx := context.WithValue(context.Background(), gatewayPoolEarlyIntentKey{}, &gatewayPoolEarlyIntent{ctx: context.Background(), model: "gpt-6-astra"})
			pool, err := svc.codexCookies.poolClient(account)
			require.NoError(t, err)
			batch, err := svc.codexCookies.gatewayPoolTakeBatch(ctx, pool, account, gwpoolTestIdentity, false, nil, 3, time.Minute)
			require.NoError(t, err)
			require.Equal(t, tc.wantEarly, batch.pairs[0].early != nil)
			query, err := url.ParseQuery(<-fake.queries)
			require.NoError(t, err)
			if tc.wantEarly {
				require.Equal(t, "unified-142", query.Get("gateway"))
				require.Empty(t, query.Get("exclude"))
				require.NotEqual(t, "3", query.Get("count"))
				pair, ok := batch.next(ctx)
				require.True(t, ok)
				require.Equal(t, "gpt-6-astra", pair.early.model)
			}
		})
	}
}

func TestGatewayPoolEarlySelectionDoesNotBypassAccountBlocks(t *testing.T) {
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}
	account := rotationAccount(1, 7)
	account.Extra[openAIGatewayPoolEarlyEnabledKey] = true
	fake.configure(account)
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-142")
	require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account))
	for _, block := range []string{"disabled", "auth", "rate"} {
		copy := *account
		future := time.Now().Add(time.Hour)
		switch block {
		case "disabled":
			copy.Schedulable = false
		case "auth":
			copy.TempUnschedulableUntil = &future
		case "rate":
			copy.RateLimitResetAt = &future
		}
		repo.account = copy
		require.False(t, svc.codexCookies.gatewayPoolEarlyDue(context.Background(), account, gwpoolTestIdentity), block)
	}
	require.Zero(t, fake.hits.Load())
}

func TestGatewayPoolEarlyVerdictOnlyOneABAndModelBound(t *testing.T) {
	store := &openAICodexCookieStore{}
	early := newGatewayPoolEarlyAttempt("unified-142", "gpt-6-astra")
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{
		cookie: "offline", gateway: early.gateway, version: "v", early: early, until: time.Now().Add(time.Minute),
	})
	ctx := context.WithValue(context.Background(), gatewayPoolProbeModelKey{}, early.model)
	shots := 0
	shoot := func(_ context.Context, _, state string) (int, string, error) {
		shots++
		if state == "" {
			return http.StatusOK, "state", nil
		}
		return http.StatusOK, "", nil
	}
	for i := 0; i < 2; i++ {
		full, known, _, _, err := store.gatewayPoolWarmVerdict(ctx, gwpoolTestAccount(1), gwpoolTestIdentity,
			OpenAIGatewayPoolApplied{Gateway: early.gateway, Version: "v", early: early}, "offline", 1, shoot)
		require.NoError(t, err)
		require.True(t, full && known)
	}
	require.Equal(t, 2, shots)
	require.True(t, store.gatewayPoolEarlyModelMatches(gwpoolTestIdentity, "gpt-5.6-luna"), "verified early tickets share eligibility")
	require.True(t, store.gatewayPoolEarlyModelMatches(gwpoolTestIdentity, "gpt-6-astra"))
	store.gatewayPoolMarkStale(gwpoolTestIdentity, "v", early.gateway)
	require.True(t, store.gatewayPoolEarlyModelMatches(gwpoolTestIdentity, "gpt-6-sol"),
		"a stale early ticket must not block normal acquisition for another model")
}
