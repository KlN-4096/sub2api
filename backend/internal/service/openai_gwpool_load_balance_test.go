package service

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func gatewayPoolSchedulerTestSettings(mode string) *RateLimitService {
	return newOpenAIAdvancedSchedulerRateLimitService(map[bool]string{true: "true", false: "false"}[mode == "advanced"])
}

func gatewayPoolTestGroupContext(group int64, limit int) context.Context {
	return context.WithValue(context.Background(), ctxkey.Group, &Group{
		ID: group, Platform: PlatformOpenAI, Status: StatusActive, Hydrated: true,
		OpenAIGatewayPoolActiveAccounts: limit,
	})
}

func TestGatewayPoolNewSessionsFillPrimaryBeforeStandby(t *testing.T) {
	for _, mode := range []string{"legacy", "load-batch", "advanced"} {
		t.Run(mode, func(t *testing.T) {
			group := int64(7)
			a, b := preferenceAccount(1, group, 20), preferenceAccount(2, group, 1)
			a.Concurrency, b.Concurrency = 100, 100
			softLoadFactor := 10
			a.LoadFactor = &softLoadFactor
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.listGateways = []gwpoolFakeGateway{{Name: "unified-300", PairReady: true}}
			fake.configure(a, b)
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.LBTopK = 1
			cfg.Gateway.Scheduling.LoadBatchEnabled = mode == "load-batch"
			cache := schedulerTestConcurrencyCache{loadMap: map[int64]*AccountLoadInfo{
				a.ID: {AccountID: a.ID, CurrentConcurrency: 10, LoadRate: 100},
				b.ID: {AccountID: b.ID, CurrentConcurrency: 0, LoadRate: 0},
			}}
			svc := &OpenAIGatewayService{
				accountRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}},
				cache:       &schedulerTestGatewayCache{}, cfg: cfg,
				concurrencyService: NewConcurrencyService(cache),
				rateLimitService:   gatewayPoolSchedulerTestSettings(mode),
			}
			identity := openAIGatewayPoolAccountKey(a)
			svc.codexCookies.poolRounds.claim(group, identity)
			svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{
				cookie: "offline", gateway: "unified-200", version: "full", until: time.Now().Add(time.Minute),
			})
			svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "full")
			selection, _, err := svc.SelectAccountWithSchedulerForCapability(gatewayPoolTestGroupContext(group, 2), &group, "", "", "gpt-6-astra", nil,
				OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
			require.NoError(t, err)
			if selection.ReleaseFunc != nil {
				defer selection.ReleaseFunc()
			}
			require.Equal(t, a.ID, selection.Account.ID, "a primary with free capacity must be filled before opening another full-strength window")
		})
	}
}

type gatewayPoolCountingSlots struct {
	schedulerTestConcurrencyCache
	mu     sync.Mutex
	counts map[int64]int
}

func (c *gatewayPoolCountingSlots) AcquireAccountSlot(_ context.Context, id int64, limit int, _ string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts[id] >= limit {
		return false, nil
	}
	c.counts[id]++
	return true, nil
}

func (c *gatewayPoolCountingSlots) ReleaseAccountSlot(_ context.Context, id int64, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[id]--
	return nil
}

func (c *gatewayPoolCountingSlots) GetAccountsLoadBatch(_ context.Context, accounts []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := map[int64]*AccountLoadInfo{}
	for _, account := range accounts {
		result[account.ID] = &AccountLoadInfo{
			AccountID: account.ID, CurrentConcurrency: c.counts[account.ID],
			LoadRate: c.counts[account.ID] * 100 / account.MaxConcurrency,
		}
	}
	return result, nil
}

func TestGatewayPoolFiftyRequestsStayInsideTwoActiveAccounts(t *testing.T) {
	for _, mode := range []string{"legacy", "load-batch", "advanced"} {
		t.Run(mode, func(t *testing.T) {
			group := int64(7)
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.listGateways = []gwpoolFakeGateway{{Name: "unified-300", PairReady: true}}
			var accounts []Account
			for id := int64(1); id <= 10; id++ {
				account := preferenceAccount(id, group, 1)
				account.Concurrency = 10
				fake.configure(account)
				accounts = append(accounts, *account)
			}
			clone := accounts[0]
			clone.ID = 11
			accounts = append(accounts, clone)
			slots := &gatewayPoolCountingSlots{counts: map[int64]int{}}
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.LBTopK = 1
			cfg.Gateway.Scheduling.LoadBatchEnabled = mode == "load-batch"
			svc := &OpenAIGatewayService{
				accountRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: accounts}},
				cache:       &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:old-clone": clone.ID}},
				cfg:         cfg, concurrencyService: NewConcurrencyService(slots),
				rateLimitService: gatewayPoolSchedulerTestSettings(mode),
			}
			var releases []func()
			defer func() {
				for _, release := range releases {
					release()
				}
			}()
			for n := range 50 {
				selection, _, err := svc.SelectAccountWithSchedulerForCapability(gatewayPoolTestGroupContext(group, 2), &group, "", "", "gpt-6-astra", nil,
					OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
				require.NoError(t, err)
				require.Contains(t, []int64{1, 2}, selection.Account.ID, "full A/B must not leak a new request to C")
				if n < 20 {
					require.NotNil(t, selection.ReleaseFunc)
					require.EqualValues(t, n/10+1, selection.Account.ID, "fill A before B, rather than average distribution")
					releases = append(releases, selection.ReleaseFunc)
				} else {
					require.Nil(t, selection.ReleaseFunc)
					require.NotNil(t, selection.WaitPlan, "excess traffic gets a bounded wait plan")
				}
			}
			for id := int64(1); id <= 10; id++ {
				want := 0
				if id <= 2 {
					want = 10
				}
				require.Equal(t, want, slots.counts[id])
			}
			require.Zero(t, slots.counts[clone.ID], "a cloned credential must not gain an extra new-session vote")
			sticky, _, err := svc.SelectAccountWithSchedulerForCapability(gatewayPoolTestGroupContext(group, 2), &group, "", "old-clone", "gpt-6-astra", nil,
				OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
			require.NoError(t, err)
			require.Equal(t, clone.ID, sticky.Account.ID, "deduplication must not move an old clone-bound session")
			require.NotNil(t, sticky.ReleaseFunc)
			releases = append(releases, sticky.ReleaseFunc)
			svc.codexCookies.poolRounds.rest(group, openAIGatewayPoolAccountKey(&accounts[0]), time.Now().Add(time.Minute))
			releases[10]()
			releases[10] = func() {}
			selection, _, err := svc.SelectAccountWithSchedulerForCapability(gatewayPoolTestGroupContext(group, 2), &group, "", "", "gpt-6-astra", nil,
				OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
			require.NoError(t, err)
			require.EqualValues(t, 2, selection.Account.ID, "B keeps priority after A rests; C joins the tail")
			require.NotNil(t, selection.ReleaseFunc)
			releases = append(releases, selection.ReleaseFunc)
			next, _, err := svc.SelectAccountWithSchedulerForCapability(gatewayPoolTestGroupContext(group, 2), &group, "", "", "gpt-6-astra", nil,
				OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
			require.NoError(t, err)
			require.EqualValues(t, 3, next.Account.ID)
			require.NotNil(t, next.ReleaseFunc)
			releases = append(releases, next.ReleaseFunc)
		})
	}
}

func TestGatewayPool429OnlyDeprioritizesNewSessions(t *testing.T) {
	group := int64(7)
	a, b := preferenceAccount(1, group, 20), preferenceAccount(2, group, 1)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-300", PairReady: true}}
	fake.configure(a, b)
	a.Concurrency, b.Concurrency = 100, 100
	svc := &OpenAIGatewayService{
		accountRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}},
		cfg:         &config.Config{}, cache: &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:old": a.ID}},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{loadMap: map[int64]*AccountLoadInfo{
			a.ID: {AccountID: a.ID}, b.ID: {AccountID: b.ID, LoadRate: 20, CurrentConcurrency: 20},
		}}),
		rateLimitService: gatewayPoolSchedulerTestSettings("legacy"),
	}
	start := time.Now()
	svc.openaiOAuth429RetryStartedAt.Store(a.ID, start)
	for session, want := range map[string]int64{"": b.ID, "old": a.ID} {
		selected, _, err := svc.SelectAccountWithSchedulerForCapability(gatewayPoolTestGroupContext(group, 2), &group, "", session, "gpt-6-astra", nil,
			OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
		require.NoError(t, err)
		require.Equal(t, want, selected.Account.ID)
		if selected.ReleaseFunc != nil {
			selected.ReleaseFunc()
		}
	}
	original, _ := svc.openaiOAuth429RetryStartedAt.Load(a.ID)
	require.Equal(t, start, original, "read-only scoring must not extend the existing retry window")
	_, exists := svc.openaiOAuth429RetryStartedAt.Load(b.ID)
	require.False(t, exists, "scoring cannot create a 429 window on a healthy account")
}

func TestGatewayPoolOldSessionOutsideActiveLimitKeepsAccount(t *testing.T) {
	for _, mode := range []string{"legacy", "load-batch", "advanced"} {
		for _, images := range []bool{false, true} {
			for _, restarted := range []bool{false, true} {
				name := mode + "/images=" + strconv.FormatBool(images) + "/restarted=" + strconv.FormatBool(restarted)
				t.Run(name, func(t *testing.T) {
					group := int64(7)
					a, b := preferenceAccount(1, group, 20), preferenceAccount(2, group, 1)
					a.Concurrency, b.Concurrency = 10, 10
					fake := newGwpoolFakePool(t, "offline", 150)
					fake.listGateways = []gwpoolFakeGateway{{Name: "unified-300", PairReady: true}}
					fake.configure(a, b)
					cfg := &config.Config{}
					cfg.Gateway.Scheduling.LoadBatchEnabled = mode == "load-batch"
					slots := &gatewayPoolCountingSlots{counts: map[int64]int{}}
					svc := &OpenAIGatewayService{
						accountRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}},
						cache:       &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:old-b": b.ID}},
						cfg:         cfg, concurrencyService: NewConcurrencyService(slots),
						rateLimitService: gatewayPoolSchedulerTestSettings(mode),
					}
					if !restarted {
						require.True(t, svc.codexCookies.poolRounds.claim(group, openAIGatewayPoolAccountKey(a), 2))
						require.True(t, svc.codexCookies.poolRounds.claim(group, openAIGatewayPoolAccountKey(b), 2))
					}
					selectSession := func(session string) *AccountSelectionResult {
						var result *AccountSelectionResult
						var err error
						if images {
							result, _, err = svc.SelectAccountWithSchedulerForImages(gatewayPoolTestGroupContext(group, 1), &group, session, "gpt-image-1", nil, OpenAIImagesCapabilityBasic)
						} else {
							result, _, err = svc.SelectAccountWithSchedulerForCapability(gatewayPoolTestGroupContext(group, 1), &group, "", session, "gpt-6-astra", nil,
								OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
						}
						require.NoError(t, err)
						require.NotNil(t, result)
						if result.ReleaseFunc != nil {
							t.Cleanup(result.ReleaseFunc)
						}
						return result
					}
					require.Equal(t, a.ID, selectSession("").Account.ID, "new traffic admits A after restart or shrink")
					require.Equal(t, b.ID, selectSession("old-b").Account.ID, "old B binding survives even outside N")
					for range 9 {
						require.Equal(t, a.ID, selectSession("").Account.ID)
					}
					waiting := selectSession("")
					require.Equal(t, a.ID, waiting.Account.ID, "the old B exception cannot admit new sessions to B")
					require.NotNil(t, waiting.WaitPlan)
					require.Equal(t, 1, slots.counts[b.ID])
				})
			}
		}
	}
}

func TestGatewayPoolDifferentGroupsUseDifferentActiveLimits(t *testing.T) {
	for _, mode := range []string{"legacy", "load-batch", "advanced"} {
		t.Run(mode, func(t *testing.T) {
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.listGateways = []gwpoolFakeGateway{{Name: "unified-300", PairReady: true}}
			var accounts []Account
			for _, group := range []int64{7, 8} {
				for offset := int64(1); offset <= 3; offset++ {
					account := preferenceAccount(group*10+offset, group, int(10-offset))
					account.Concurrency = 1
					fake.configure(account)
					accounts = append(accounts, *account)
				}
			}
			cfg := &config.Config{}
			cfg.Gateway.Scheduling.LoadBatchEnabled = mode == "load-batch"
			cfg.Gateway.OpenAIWS.LBTopK = 1
			slots := &gatewayPoolCountingSlots{counts: map[int64]int{}}
			svc := &OpenAIGatewayService{
				accountRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: accounts}},
				cfg:         cfg, cache: &schedulerTestGatewayCache{},
				concurrencyService: NewConcurrencyService(slots),
				rateLimitService:   gatewayPoolSchedulerTestSettings(mode),
			}
			settings, ok := svc.rateLimitService.settingService.settingRepo.(*openAIAdvancedSchedulerSettingRepoStub)
			require.True(t, ok)
			settings.values[SettingKeyOpenAIGatewayPoolActiveAccounts] = "64"
			for _, group := range []int64{7, 8} {
				limit := int(group - 6)
				for request := 0; request <= limit; request++ {
					selected, _, err := svc.SelectAccountWithSchedulerForCapability(gatewayPoolTestGroupContext(group, limit),
						&group, "", "", "gpt-6-astra", nil, OpenAIUpstreamTransportHTTPSSE,
						OpenAIEndpointCapabilityChatCompletions, false, false, true)
					require.NoError(t, err)
					require.LessOrEqual(t, selected.Account.ID, group*10+int64(limit),
						"the retired global 64 must not override this group's limit")
					if request < limit {
						require.Equal(t, group*10+int64(request+1), selected.Account.ID)
						require.NotNil(t, selected.ReleaseFunc)
						t.Cleanup(selected.ReleaseFunc)
					} else {
						require.Nil(t, selected.ReleaseFunc)
						require.NotNil(t, selected.WaitPlan)
					}
				}
			}
		})
	}
}
