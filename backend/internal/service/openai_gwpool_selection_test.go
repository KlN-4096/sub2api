package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type gatewaySelectionReadFails struct{ AccountRepository }

func (gatewaySelectionReadFails) GetByID(context.Context, int64) (*Account, error) {
	return nil, errors.New("unexpected extra read")
}

func TestGatewayPoolSelectionLeavesOrdinaryOAuthOutsideFreshGate(t *testing.T) {
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	svc := &OpenAIGatewayService{accountRepo: gatewaySelectionReadFails{}}
	group := int64(7)
	require.True(t, svc.gatewayPoolRoundSelectionAllowed(context.Background(), &group, &AccountSelectionResult{Account: account}))
}

type gatewaySelectionReadCounter struct {
	gatewayRotationRepo
	reads map[int64]int
}

func (r gatewaySelectionReadCounter) GetByID(ctx context.Context, id int64) (*Account, error) {
	r.reads[id]++
	return r.gatewayRotationRepo.GetByID(ctx, id)
}

func TestGatewayPoolSelectionReadsOnlyPoolRowsFresh(t *testing.T) {
	group := int64(7)
	pool := preferenceAccount(1, group, 1)
	ordinary := Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		Schedulable: true, Concurrency: 1, GroupIDs: []int64{group}, Extra: map[string]any{}}
	repo := gatewaySelectionReadCounter{gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*pool, ordinary}}}, map[int64]int{}}
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}}
	svc.codexCookies.poolRotationAccounts.Store(ordinary.ID, true)
	ctx := svc.withGatewayPoolAccountPreferences(context.Background(), OpenAIAccountScheduleRequest{
		GroupID: &group, Platform: PlatformOpenAI, RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
	})
	require.Contains(t, gatewayPoolPreferences(ctx), pool.ID)
	require.Positive(t, repo.reads[pool.ID])
	require.Zero(t, repo.reads[ordinary.ID], "the projected switch already says this row is ordinary")
	known, _ := svc.codexCookies.poolRotationAccounts.Load(ordinary.ID)
	require.Equal(t, false, known, "an ordinary row must not keep a stale opt-in for the round gate")
}

type gatewaySelectionMissingRow struct {
	gatewayRotationRepo
	missing int64
}

func (r gatewaySelectionMissingRow) GetByID(ctx context.Context, id int64) (*Account, error) {
	if id == r.missing {
		return nil, ErrAccountNotFound
	}
	return r.gatewayRotationRepo.GetByID(ctx, id)
}

func TestGatewayPoolSelectionUnreadPoolRowLeavesRoundIncomplete(t *testing.T) {
	group := int64(7)
	a, b := preferenceAccount(1, group, 1), preferenceAccount(2, group, 2)
	for _, missing := range []int64{0, b.ID} {
		repo := gatewaySelectionMissingRow{gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}}, missing}
		svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}}
		require.True(t, svc.codexCookies.poolRounds.claim(group, "departed-member", 1))
		svc.withGatewayPoolAccountPreferences(context.Background(), OpenAIAccountScheduleRequest{
			GroupID: &group, Platform: PlatformOpenAI, RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
		})
		active := svc.codexCookies.poolRounds.groups[group].active
		require.Equal(t, missing != 0, slices.Contains(active, "departed-member"),
			"an unread pool row is unknown, so admission must not drop members it cannot see (missing=%d)", missing)
	}
}

func TestGatewayPoolSelectionTouchesNewestLedgerContact(t *testing.T) {
	group := int64(7)
	now := time.Now().UTC().Truncate(time.Second)
	account := preferenceAccount(1, group, 1)
	identity := openAIGatewayPoolAccountKey(account)
	newest, lastUsed := now.Add(-time.Minute), now.Add(-time.Hour)
	account.LastUsedAt = &lastUsed
	seen := map[string]gatewayPoolContactSeen{"unified-newest": {FirstAt: now.Add(-48 * time.Hour), LastAt: newest}}
	for i := range gatewayPoolContactGatewayLimit + 3 { // more than prune keeps
		seen[fmt.Sprintf("unified-%d", i)] = gatewayPoolContactSeen{FirstAt: now.Add(-48 * time.Hour), LastAt: now.Add(-time.Duration(i+2) * time.Hour)}
	}
	for _, tag := range []string{gatewayPoolLedgerTag(identity), "another-ledger"} {
		account.Extra[openAIGatewayPoolContactsExtraKey] = gatewayPoolContacts{LedgerTag: tag, Seen: seen,
			Rounds: []gatewayPoolContactRound{{RoundID: "not-read-by-selection", LastAt: now}}}
		repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}
		svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}}
		svc.withGatewayPoolAccountPreferences(context.Background(), OpenAIAccountScheduleRequest{
			GroupID: &group, Platform: PlatformOpenAI, RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
		})
		want := lastUsed
		if tag == gatewayPoolLedgerTag(identity) {
			want = newest
		}
		require.True(t, want.Equal(svc.codexCookies.poolRounds.lastTouch(identity)), "tag %s", tag)
	}
}

func TestGatewayPoolSelectionCanReachHealthyFourthAccount(t *testing.T) {
	group := int64(7)
	var accounts []Account
	for id := int64(1); id <= 4; id++ {
		account := preferenceAccount(id, group, int(10-id))
		fake := newGwpoolFakePool(t, "offline", 150)
		fake.listGateways = []gwpoolFakeGateway{{Name: "unified-300", PairReady: id == 4, UsedByYou: id != 4}}
		fake.configure(account)
		accounts = append(accounts, *account)
	}
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: accounts}}
	svc := &OpenAIGatewayService{accountRepo: repo, cache: &schedulerTestGatewayCache{}, cfg: &config.Config{},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	selected, _, err := svc.SelectAccountWithSchedulerForCapability(context.Background(), &group, "", "", "gpt-6-astra", nil,
		OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
	require.NoError(t, err)
	require.EqualValues(t, 4, selected.Account.ID)
	if selected.ReleaseFunc != nil {
		selected.ReleaseFunc()
	}
}

func preferenceAccount(id, group int64, cooled int) *Account {
	account := rotationAccount(id, group)
	account.Credentials = map[string]any{"chatgpt_account_id": fmt.Sprintf("preference-%d", id), "chatgpt_user_id": "user"}
	identity := openAIGatewayPoolAccountKey(account)
	seen := map[string]openAIGatewaySeen{}
	for i := range cooled {
		seen[fmt.Sprintf("unified-%d", i+1)] = openAIGatewaySeen{At: time.Now().Add(-12 * time.Hour)}
	}
	seen["unified-200"] = openAIGatewaySeen{At: time.Now()}
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{LedgerTag: gatewayPoolLedgerTag(identity), Seen: seen}
	return account
}

func TestGatewayPoolSelectionPrioritizesFreshCooledCountsAcrossSchedulers(t *testing.T) {
	for _, mode := range []string{"legacy", "load-batch", "advanced"} {
		t.Run(mode, func(t *testing.T) {
			group := int64(7)
			a, b, c := preferenceAccount(1, group, 1), preferenceAccount(2, group, 6), preferenceAccount(3, group, 2)
			a.Priority, b.Priority, c.Priority = 0, 9, 1
			repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b, *c}}}
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.LBTopK = 1
			cfg.Gateway.Scheduling.LoadBatchEnabled = mode == "load-batch"
			svc := &OpenAIGatewayService{accountRepo: repo, cache: &schedulerTestGatewayCache{}, cfg: cfg,
				concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
				rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService(map[bool]string{true: "true", false: "false"}[mode == "advanced"])}
			selectAccount := func(ctx context.Context, excluded map[int64]struct{}) int64 {
				t.Helper()
				selection, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &group, "", "", "gpt-6-astra", excluded,
					OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
				require.NoError(t, err)
				if selection.ReleaseFunc != nil {
					defer selection.ReleaseFunc()
				}
				return selection.Account.ID
			}
			require.Equal(t, b.ID, selectAccount(context.Background(), nil), "higher known cooled count wins")
			identity := openAIGatewayPoolAccountKey(a)
			svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{
				cookie: "offline", version: "full", gateway: "unified-200",
			})
			svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "full")
			require.Equal(t, b.ID, selectAccount(context.Background(), nil), "another live ticket must not evict the active primary")
			svc.codexCookies.poolRounds.rest(group, openAIGatewayPoolAccountKey(a), time.Now().Add(time.Minute))
			svc.codexCookies.poolRounds.rest(group, openAIGatewayPoolAccountKey(b), time.Now().Add(time.Minute))
			ctx := context.WithValue(context.Background(), gatewayPoolRotationKey{}, &gatewayPoolRotation{
				groupID: group, attempted: map[int64]struct{}{a.ID: {}, b.ID: {}},
			})
			require.Equal(t, c.ID, selectAccount(ctx, nil), "never revisit A/B during rotation")
		})
	}
}

func TestGatewayPoolSelectionReordersOnlyKnownOptedInSlots(t *testing.T) {
	ctx := context.WithValue(context.Background(), gatewayPoolPreferenceKey{}, gatewayPoolAccountPreferences{
		1: {cooled: 1}, 2: {cooled: 4}, 3: {cooled: 4},
	})
	rows := []*Account{{ID: 1}, {ID: 99}, {ID: 3}, {ID: 2}, {ID: 100}}
	gatewayPoolOrder(ctx, rows, func(a *Account) *Account { return a })
	var ids []int64
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	require.Equal(t, []int64{3, 99, 2, 1, 100}, ids, "ordinary positions and equal-score baseline order must survive")
}

func TestGatewayPoolSelectionKeepsSessionUntilFreshExclusion(t *testing.T) {
	prefs := gatewayPoolAccountPreferences{
		1:  {verified: true, cooled: 1},
		40: {verified: true, cooled: 20},
	}
	ctx := context.WithValue(context.Background(), gatewayPoolPreferenceKey{}, prefs)
	require.False(t, gatewayPoolPreferAlternative(ctx, 1),
		"a better future inventory must not evict this session's current verified window")
	prefs[1] = gatewayPoolAccountPreference{cooled: 1}
	require.False(t, gatewayPoolPreferAlternative(ctx, 1), "finding a new ticket on the old row does not by itself move its session")
	ctx = context.WithValue(ctx, gatewayPoolRoundExclusionsKey{}, map[int64]struct{}{1: {}})
	require.True(t, gatewayPoolPreferAlternative(ctx, 1), "fresh rest/exhaustion can release a soft binding")
}

func TestGatewayPoolSelectionWeightedSessionKeepsVerifiedWindow(t *testing.T) {
	group := int64(7)
	a, b := preferenceAccount(1, group, 1), preferenceAccount(2, group, 20)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}}
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.LBTopK = 1
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: cfg,
		cache:              &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:verified-session": a.ID}},
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true", "true"),
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	for _, account := range []*Account{a, b} {
		identity := openAIGatewayPoolAccountKey(account)
		svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{
			cookie: "offline", version: "full", gateway: "unified-200",
		})
		svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "full")
	}
	selection, _, err := svc.SelectAccountWithSchedulerForCapability(context.Background(), &group, "", "verified-session", "gpt-6-astra", nil,
		OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
	require.NoError(t, err)
	if selection.ReleaseFunc != nil {
		defer selection.ReleaseFunc()
	}
	require.Equal(t, a.ID, selection.Account.ID)
}

type gatewayRoundSelectionRace struct {
	OpenAIAccountScheduler
	selectFn func(context.Context, OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error)
}

func (s gatewayRoundSelectionRace) Select(ctx context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
	return s.selectFn(ctx, req)
}

func TestGatewayPoolSelectionRechecksConcurrentExhaustionBySelectingRemainingAccount(t *testing.T) {
	group := int64(7)
	a, b := preferenceAccount(1, group, 20), preferenceAccount(2, group, 1)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}}
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{},
		rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true")}
	calls, released := 0, 0
	svc.openaiScheduler = gatewayRoundSelectionRace{selectFn: func(_ context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
		calls++
		selected := b
		if calls == 1 {
			selected = a
			svc.codexCookies.poolRounds.exhaust(group, openAIGatewayPoolAccountKey(a), 0)
		} else {
			require.Contains(t, req.ExcludedIDs, a.ID)
		}
		return &AccountSelectionResult{Account: selected, ReleaseFunc: func() { released++ }}, OpenAIAccountScheduleDecision{}, nil
	}}
	selection, _, err := svc.SelectAccountWithSchedulerForCapability(context.Background(), &group, "", "", "gpt-6-astra", nil,
		OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
	require.NoError(t, err)
	require.Equal(t, b.ID, selection.Account.ID)
	require.Equal(t, 2, calls)
	require.Equal(t, 1, released, "release the rejected slot before retrying selection")
	selection.ReleaseFunc()
}

func TestGatewayPoolSelectionRoundPersistsAcrossRequestsAndRestartsAfterLastAccount(t *testing.T) {
	group := int64(7)
	a, b := preferenceAccount(1, group, 20), preferenceAccount(2, group, 1)
	a.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	b.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 1
	fake := newGwpoolFakePool(t, "offline-cookie", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-201", PairReady: true}}
	fake.configure(a, b)
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}}
	svc := &OpenAIGatewayService{accountRepo: repo, cache: &schedulerTestGatewayCache{}, cfg: &config.Config{},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
	selectID := func(ctx context.Context) (int64, error) {
		selection, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &group, "", "", "gpt-6-astra", nil,
			OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
		if err != nil {
			return 0, err
		}
		if selection.ReleaseFunc != nil {
			selection.ReleaseFunc()
		}
		return selection.Account.ID, nil
	}
	id, err := selectID(context.Background())
	require.NoError(t, err)
	require.Equal(t, a.ID, id, "the initial round keeps cooled-count priority")
	exhaust := func(ctx context.Context, account *Account) context.Context {
		identity := openAIGatewayPoolAccountKey(account)
		history, _ := readOpenAIGatewayHistory(account)
		for gateway := range history.Seen {
			svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(identity, gateway), time.Now())
		}
		fake.listGateways = []gwpoolFakeGateway{{Name: "unified-200", PairReady: true, UsedByYou: true}}
		failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
		next := svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, failure, nil)
		require.True(t, failure.ShouldRetryNextAccount())
		fake.listGateways = []gwpoolFakeGateway{{Name: "unified-201", PairReady: true}}
		return next
	}
	ctx := exhaust(context.Background(), a)
	id, err = selectID(context.Background())
	require.NoError(t, err)
	require.Equal(t, b.ID, id, "a new request must not revisit this round's exhausted account")
	ctx = exhaust(ctx, b)
	_, err = selectID(ctx)
	require.ErrorIs(t, err, ErrNoAvailableAccounts, "round reset cannot erase this request's attempts")
	_, err = selectID(context.Background())
	require.ErrorIs(t, err, ErrNoAvailableAccounts, "all resting accounts must not restart immediately")
	for i := range repo.accounts {
		past := time.Now().Add(-time.Second)
		repo.accounts[i].TempUnschedulableUntil = &past
		identity := openAIGatewayPoolAccountKey(&repo.accounts[i])
		gatewayPoolRestDue(t, svc, repo, repo.accounts[i].ID, identity)
		history, _ := readOpenAIGatewayHistory(&repo.accounts[i])
		for gateway := range history.Seen {
			svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(identity, gateway), time.Now().Add(-2*time.Hour))
		}
		svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(identity, "unified-200"), time.Now().Add(-2*time.Hour))
	}
	svc.codexCookies.poolRounds.mu.Lock()
	for domain := range svc.codexCookies.poolRounds.groups[group].resting {
		svc.codexCookies.poolRounds.groups[group].resting[domain] = time.Now().Add(-time.Second)
	}
	svc.codexCookies.poolRounds.mu.Unlock()
	id, err = selectID(context.Background())
	require.NoError(t, err)
	require.NotZero(t, id, "expired rests permit a fresh evaluation")
}

func TestGatewayPoolSelectionFreshHistoryIgnoresPoolFreeAndWrongIdentity(t *testing.T) {
	group := int64(7)
	a, b := preferenceAccount(1, group, 1), preferenceAccount(2, group, 6)
	a.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: "another-credential", PoolFree: 999,
		Seen: map[string]openAIGatewaySeen{"unified-1": {At: time.Now().Add(-12 * time.Hour)}},
	}
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}}
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}, rateLimitService: gatewayPoolSchedulerTestSettings("legacy")}
	ctx := svc.withGatewayPoolAccountPreferences(gatewayPoolTestGroupContext(group, 2), OpenAIAccountScheduleRequest{
		GroupID: &group, Platform: PlatformOpenAI, RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
	})
	prefs := gatewayPoolPreferences(ctx)
	require.Equal(t, gatewayPoolCooledUnknown, prefs[a.ID].cooled, "wrong-identity inventory is unknown, not zero")
	require.False(t, prefs[a.ID].loadKnown, "missing load is not converted into an idle row")
}

func TestGatewayPoolSelectionIneligibleAlternativeCannotBreakSticky(t *testing.T) {
	for _, reason := range []string{"attempted", "model", "disabled", "other-group", "pool-off"} {
		t.Run(reason, func(t *testing.T) {
			group := int64(7)
			a, b := preferenceAccount(1, group, 1), preferenceAccount(2, group, 20)
			req := OpenAIAccountScheduleRequest{GroupID: &group, Platform: PlatformOpenAI,
				RequestedModel: "gpt-6-astra", RequiredTransport: OpenAIUpstreamTransportHTTPSSE}
			switch reason {
			case "attempted":
				req.ExcludedIDs = map[int64]struct{}{b.ID: {}}
			case "model":
				b.Credentials["model_mapping"] = map[string]any{"only-another-model": "only-another-model"}
			case "disabled":
				b.Schedulable = false
			case "other-group":
				b.GroupIDs = []int64{8}
			case "pool-off":
				b.Extra[openAIGatewayPoolExtraKey] = false
			}
			repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}}
			svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}}
			ctx := svc.withGatewayPoolAccountPreferences(context.Background(), req)
			require.False(t, gatewayPoolPreferAlternative(ctx, a.ID))
		})
	}
}

func TestGatewayPoolSelectionSharesCloneCooldownWithoutSummingCapacity(t *testing.T) {
	group := int64(7)
	a, clone := preferenceAccount(1, group, 2), preferenceAccount(2, group, 10)
	clone.Credentials = a.Credentials
	identity := openAIGatewayPoolAccountKey(a)
	clone.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: gatewayPoolLedgerTag(identity),
		Seen:      map[string]openAIGatewaySeen{"unified-1": {At: time.Now()}},
	}
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *clone}}}
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}}
	svc.codexCookies.historyByTag = func(context.Context, string) ([]Account, error) {
		return repo.accounts, nil
	}
	ctx := svc.withGatewayPoolAccountPreferences(context.Background(), OpenAIAccountScheduleRequest{
		GroupID: &group, Platform: PlatformOpenAI, RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
	})
	prefs := gatewayPoolPreferences(ctx)
	require.Equal(t, 1, prefs[a.ID].cooled)
	require.Equal(t, prefs[a.ID], prefs[clone.ID])
}

// Selection processes pool rows one by one; the first row's locked rest
// admission (its rest-peer query, lock waits up to 3s in production) precedes
// the second row's use. A write to the second row in that gap must be seen, so
// each row is read right before its own turn, not in one batch up front. The
// repository returns private copies, as a database does.
type selectionGapRepo struct {
	gatewayRotationRepo
	fired *atomic.Bool
	write func()
}

func (r selectionGapRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	row, err := r.gatewayRotationRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	copied := *row
	return &copied, nil
}

// A batch read must hand out private copies too, or a reintroduced batch read
// would see the write through the shared slice and pass.
func (r selectionGapRepo) GetByIDs(ctx context.Context, ids []int64) ([]*Account, error) {
	var found []*Account
	for _, id := range ids {
		if row, err := r.GetByID(ctx, id); err == nil {
			found = append(found, row)
		}
	}
	return found, nil
}

func (r selectionGapRepo) FindByExtraField(ctx context.Context, key string, value any) ([]Account, error) {
	if key == gatewayPoolRestTagKey && r.fired.CompareAndSwap(false, true) {
		r.write() // lands while the first row's rest admission holds the domain lock
	}
	return r.gatewayRotationRepo.FindByExtraField(ctx, key, value)
}

func TestGatewayPoolSelectionSeesWriteMadeWhileEarlierRowIsAdmitted(t *testing.T) {
	group := int64(7)
	a, b := preferenceAccount(1, group, 1), preferenceAccount(2, group, 2)
	accounts := []Account{*a, *b}
	repo := selectionGapRepo{gatewayRotationRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: accounts}},
		fired: &atomic.Bool{}}
	repo.write = func() { accounts[1].Schedulable = false } // e.g. an admin disables B
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}}
	ctx := svc.withGatewayPoolAccountPreferences(context.Background(), OpenAIAccountScheduleRequest{
		GroupID: &group, Platform: PlatformOpenAI, RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
	})
	require.True(t, repo.fired.Load(), "the first row's rest admission ran")
	prefs := gatewayPoolPreferences(ctx)
	require.NotContains(t, prefs, b.ID, "B was disabled before selection used its row")
	require.Contains(t, prefs, a.ID, "the healthy row keeps the round slot")
}
