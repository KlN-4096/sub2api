package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

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
				cookie: "offline", version: "full", gateway: "unified-200", until: time.Now().Add(time.Minute),
			})
			svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "full")
			require.Equal(t, a.ID, selectAccount(context.Background(), nil), "reuse an actual verified live window")
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

func TestGatewayPoolSelectionFreshHistoryIgnoresPoolFreeAndWrongIdentity(t *testing.T) {
	group := int64(7)
	a, b := preferenceAccount(1, group, 1), preferenceAccount(2, group, 6)
	a.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{
		LedgerTag: "another-credential", PoolFree: 999,
		Seen: map[string]openAIGatewaySeen{"unified-1": {At: time.Now().Add(-12 * time.Hour)}},
	}
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*a, *b}}}
	svc := &OpenAIGatewayService{accountRepo: repo, cfg: &config.Config{}}
	ctx := svc.withGatewayPoolAccountPreferences(context.Background(), OpenAIAccountScheduleRequest{
		GroupID: &group, Platform: PlatformOpenAI, RequiredTransport: OpenAIUpstreamTransportHTTPSSE,
	})
	require.Empty(t, gatewayPoolPreferences(ctx), "one unknown account cannot be assumed to have zero capacity")
}

func TestGatewayPoolSelectionIneligibleAlternativeCannotBreakSticky(t *testing.T) {
	for _, reason := range []string{"attempted", "model", "disabled", "other-group", "rotation-off"} {
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
			case "rotation-off":
				b.Extra[openAIGatewayPoolRotationExtraKey] = false
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
