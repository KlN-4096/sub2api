package service

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolQualityRestUsesEarlierOfFrozenQualityAndCount(t *testing.T) {
	for _, tc := range []struct {
		name      string
		quality   []string
		ready     []string
		threshold int
		allowed   bool
	}{
		{"quality first", []string{"good"}, []string{"good"}, 3, true},
		{"configured count first", []string{"good"}, []string{"ordinary-a", "ordinary-b"}, 2, true},
		{"neither ready", []string{"good"}, []string{"ordinary-a"}, 3, false},
		{"empty quality is not automatically ready", nil, []string{"ordinary-a"}, 3, false},
		{"unknown quality cooldown is not proof", []string{"missing"}, []string{"ordinary-a"}, 3, false},
		{"fewer than configured waits for all known", nil, []string{"good", "ordinary-a", "ordinary-b"}, 50, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := rotationAccount(1, 7)
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.configure(account)
			now := time.Now().UTC()
			account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = tc.threshold
			account.Extra[gatewayPoolRestStateKey] = map[string]any{
				"tag": gatewayPoolLedgerTag(gwpoolTestIdentity), "active": true,
				"changed_at": now.Add(-time.Minute), "started_at": now.Add(-time.Minute),
				"resume_at": now.Add(time.Hour), "quality_gateways": tc.quality,
			}
			svc := rotationService(account)
			for _, name := range []string{"good", "ordinary-a", "ordinary-b"} {
				at := now
				for _, ready := range tc.ready {
					if name == ready {
						at = now.Add(-2 * time.Hour)
					}
				}
				svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, name), at)
			}
			allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
			require.NoError(t, err)
			require.Equal(t, tc.allowed, allowed)
			require.Zero(t, fake.listHits.Load(), "local recovery never refreshes the catalog")
			require.Zero(t, fake.hits.Load(), "local recovery never mints or probes")
		})
	}
}

func TestGatewayPoolQualityRestBothEntrypointsFreezeSameFreshLunaCohort(t *testing.T) {
	for _, entry := range []string{"rotation", "selection"} {
		t.Run(entry, func(t *testing.T) {
			ctx, now, group := context.Background(), time.Now().UTC(), int64(7)
			account := rotationAccount(1, group)
			account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 3
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.configure(account)
			fake.listGateways = []gwpoolFakeGateway{
				{Name: "good", PairReady: true, Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 100, Samples: 100}},
				{Name: "neutral", PairReady: true, Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 50, Samples: 100}},
				// Keep the cohort prior neutral as well; a 50% raw rate alone
				// need not produce exactly 0.5 under the shared scorer.
				{Name: "bad", PairReady: true, Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 0, Samples: 100}},
				{Name: "unknown", PairReady: true},
			}
			svc := rotationService(account)
			for _, row := range fake.listGateways {
				at := now
				if row.Name == "good" {
					at = now.Add(-50 * time.Minute)
				}
				svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, row.Name), at)
			}
			counter := &atomic.Uint64{}
			counter.Store(4)
			svc.codexCookies.poolContactPicks.Store(gatewayPoolLedgerTag(gwpoolTestIdentity), counter)
			if entry == "rotation" {
				failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
				svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, failure)
				require.True(t, failure.ShouldRetryNextAccount())
			} else {
				require.False(t, svc.gatewayPoolRoundSelectionAllowed(ctx, &group, &AccountSelectionResult{Account: account}))
			}
			fresh, err := svc.accountRepo.GetByID(ctx, account.ID)
			require.NoError(t, err)
			state := readGatewayPoolRest(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
			require.True(t, state.Active)
			require.Equal(t, []string{"good"}, state.QualityGateways)
			require.WithinDuration(t, now.Add(10*time.Minute), state.ResumeAt, time.Second)
			require.Equal(t, uint64(4), counter.Load(), "policy capture must not consume the fifth exploration pick")
			require.Equal(t, int64(1), fake.listHits.Load(), "use the exhaustion catalog, not a second request")
			query, err := url.ParseQuery(<-fake.listQueries)
			require.NoError(t, err)
			require.Equal(t, gatewayPoolProbeModelLuna, query.Get("model"))
			require.Zero(t, fake.hits.Load())
		})
	}
}

func TestGatewayPoolQualityRestFreezeSurvivesRestartAndLaterEntry(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	account := rotationAccount(1, 7)
	fake := newGwpoolFakePool(t, "offline", 150)
	fake.configure(account)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 3
	svc := rotationService(account)
	for _, name := range []string{"first-quality", "new-quality", "ordinary"} {
		svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, name), now)
	}
	require.NoError(t, svc.enterGatewayPoolRest(ctx, account, gwpoolTestIdentity, now, now.Add(time.Hour), gatewayPoolRestPlan{qualityGateways: []string{"first-quality"}}))
	restarted := &OpenAIGatewayService{accountRepo: svc.accountRepo}
	for _, name := range []string{"first-quality", "new-quality", "ordinary"} {
		at := now
		if name == "first-quality" {
			at = now.Add(-2 * time.Hour)
		}
		restarted.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, name), at)
	}
	require.NoError(t, restarted.enterGatewayPoolRest(ctx, account, gwpoolTestIdentity, now.Add(time.Second), now.Add(2*time.Hour), gatewayPoolRestPlan{qualityGateways: []string{"new-quality"}}))
	fresh, err := svc.accountRepo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, []string{"first-quality"}, readGatewayPoolRest(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity)).QualityGateways)
	allowed, err := restarted.gatewayPoolResumeAllowed(ctx, account, true)
	require.NoError(t, err)
	require.True(t, allowed)
	fresh, err = svc.accountRepo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	state := readGatewayPoolRest(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity))
	require.False(t, state.Active)
	require.Empty(t, state.QualityGateways, "inactive tombstones must discard the old cohort")
	require.Zero(t, fake.listHits.Load())
}

type gatewayQualityRestRepo struct {
	gatewayRotationRepo
	onPeers func()
	onGet   func()
	fail    bool
}

func (r *gatewayQualityRestRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	if r.onGet != nil {
		r.onGet()
	}
	return r.gatewayRotationRepo.GetByID(ctx, id)
}

func (r *gatewayQualityRestRepo) FindByExtraField(ctx context.Context, key string, value any) ([]Account, error) {
	if r.onPeers != nil {
		r.onPeers()
	}
	if r.fail {
		return nil, errors.New("offline peer read failed")
	}
	return r.gatewayRotationRepo.FindByExtraField(ctx, key, value)
}

func TestGatewayPoolQualityRestUsesFreshPeerEvidenceAndRejectsUnknownOrChangedInventory(t *testing.T) {
	for _, mode := range []string{"peer-local-wins", "peer-failure", "inventory-changed", "catalog-failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, now := context.Background(), time.Now().UTC()
			account, peer := rotationAccount(1, 7), rotationAccount(2, 7)
			fake := newGwpoolFakePool(t, "offline", 150)
			fake.configure(account, peer)
			fake.listGateways = []gwpoolFakeGateway{
				{Name: "good", PairReady: true, Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 0, Samples: 100}},
				{Name: "bad", PairReady: true, Priority: &gwpool.GatewayPriority{Model: gatewayPoolProbeModelLuna, Full: 100, Samples: 100}},
			}
			peer.Extra[openAIGatewayLedgerTagExtraKey] = gatewayPoolLedgerTag(gwpoolTestIdentity)
			peer.Extra[openAIGatewayPoolContactsExtraKey] = localRankHistory(now, "good", "bad", 5)
			repo := &gatewayQualityRestRepo{gatewayRotationRepo: gatewayRotationRepo{
				schedulerTestOpenAIAccountRepo{accounts: []Account{*account, *peer}},
			}}
			svc := &OpenAIGatewayService{accountRepo: repo}
			for _, name := range []string{"good", "bad"} {
				svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, name), now)
			}
			switch mode {
			case "peer-failure":
				repo.fail = true
			case "inventory-changed":
				repo.onPeers = func() { done := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity); done() }
			case "catalog-failure":
				fake.listStatus = 503
			}
			plan := svc.gatewayPoolExhaustedRestQuality(ctx, account)
			require.Equal(t, mode == "peer-local-wins", plan != nil)
			if plan != nil {
				require.Equal(t, []string{"good"}, plan.qualityGateways)
			}
			require.Zero(t, fake.hits.Load())
		})
	}
}

func TestGatewayPoolQualityRestEstimateUsesNewestPeerCohortWithoutWriting(t *testing.T) {
	now := time.Now().UTC()
	account, peer := gwpoolTestAccount(1), gwpoolTestAccount(2)
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 3
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	older := gatewayPoolRestState{Tag: tag, Active: true, ChangedAt: now.Add(-time.Hour), QualityGateways: []string{"late"}}
	newer := gatewayPoolRestState{Tag: tag, Active: true, ChangedAt: now, QualityGateways: []string{"early"}}
	account.Extra[gatewayPoolRestStateKey], peer.Extra[gatewayPoolRestStateKey] = older, newer
	history := openAIGatewayHistory{Seen: map[string]openAIGatewaySeen{
		"early": {At: now.Add(-50 * time.Minute)},
		"late":  {At: now},
	}}
	svc := &OpenAIGatewayService{}
	estimate := svc.codexCookies.gatewayPoolCooldownEstimate(gwpoolTestIdentity, account, history, now, *peer)
	require.Equal(t, now.Add(10*time.Minute), estimate.EligibleAt)
	require.Contains(t, svc.gatewayPoolRestDisplay(account, gwpoolTestIdentity, []Account{*peer}).Reason, "or 3 local cooldowns")
	require.Equal(t, older, readGatewayPoolRest(account, tag))
	_, cached := svc.codexCookies.poolRestState.Load(tag)
	require.False(t, cached, "display cannot publish a runtime policy")
	newer.Active, newer.QualityGateways = false, nil
	peer.Extra[gatewayPoolRestStateKey] = newer
	estimate = svc.codexCookies.gatewayPoolCooldownEstimate(gwpoolTestIdentity, account, history, now, *peer)
	require.Equal(t, now.Add(time.Hour), estimate.EligibleAt, "a newer tombstone must not reuse an old quality cohort")
}

func TestGatewayPoolQualityRestChangedInventoryCannotPublishAtEitherEntrypoint(t *testing.T) {
	for _, entry := range []string{"rotation", "selection"} {
		for _, keepActive := range []bool{false, true} {
			t.Run(entry+"/active="+strconv.FormatBool(keepActive), func(t *testing.T) {
				ctx, now, group := context.Background(), time.Now().UTC(), int64(7)
				account := rotationAccount(1, group)
				fake := newGwpoolFakePool(t, "offline", 150)
				fake.configure(account)
				fake.listGateways = []gwpoolFakeGateway{{Name: "good", PairReady: true}}
				repo := &gatewayQualityRestRepo{gatewayRotationRepo: gatewayRotationRepo{
					schedulerTestOpenAIAccountRepo{accounts: []Account{*account}},
				}}
				svc := &OpenAIGatewayService{accountRepo: repo}
				svc.codexCookies.poolUsed.Store(gatewayPoolLedgerKey(gwpoolTestIdentity, "good"), now)
				reads, changed := 0, false
				repo.onGet = func() {
					reads++
					// Admission and quality capture each read once; entry loads
					// once, then the writer re-reads before publication.
					if reads == 4 {
						changed = true
						done := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
						if keepActive {
							t.Cleanup(done)
						} else {
							done()
						}
					}
				}
				if entry == "rotation" {
					failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
					result := svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, failure)
					require.False(t, failure.ShouldRetryNextAccount(), "stale exhaustion cannot authorize rotation")
					require.Nil(t, gatewayPoolRotationFrom(result))
				} else {
					require.False(t, svc.gatewayPoolRoundSelectionAllowed(ctx, &group, &AccountSelectionResult{Account: account}))
				}
				require.True(t, changed, "exercise the final persistence boundary")
				fresh, err := repo.GetByID(ctx, account.ID)
				require.NoError(t, err)
				require.False(t, readGatewayPoolRest(fresh, gatewayPoolLedgerTag(gwpoolTestIdentity)).Active)
				require.Nil(t, fresh.TempUnschedulableUntil)
				_, cached := svc.codexCookies.poolRestState.Load(gatewayPoolLedgerTag(gwpoolTestIdentity))
				require.False(t, cached, "do not publish an invalid local rest before the guarded write")
				require.False(t, svc.codexCookies.poolRounds.blocked(group, gwpoolTestIdentity))
				require.Zero(t, fake.hits.Load())
			})
		}
	}
}

func TestGatewayPoolQualityRestPartialReadCannotReplaceFrozenPolicyOrTombstone(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run("active="+strconv.FormatBool(active), func(t *testing.T) {
			ctx, now := context.Background(), time.Now().UTC()
			account := rotationAccount(1, 7)
			tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
			state := gatewayPoolRestState{Tag: tag, Active: active, ChangedAt: now.Add(-time.Minute)}
			if active {
				state.QualityGateways = []string{"original"}
				state.StartedAt, state.ResumeAt = state.ChangedAt, now.Add(time.Hour)
			}
			account.Extra[gatewayPoolRestStateKey] = state
			repo := &gatewayQualityRestRepo{gatewayRotationRepo: gatewayRotationRepo{
				schedulerTestOpenAIAccountRepo{accounts: []Account{*account}},
			}, fail: true}
			svc := &OpenAIGatewayService{accountRepo: repo} // restart: cache is empty
			err := svc.enterGatewayPoolRest(ctx, account, gwpoolTestIdentity, now, now.Add(2*time.Hour), gatewayPoolRestPlan{qualityGateways: []string{"replacement"}})
			require.Error(t, err)
			require.Equal(t, state, readGatewayPoolRest(account, tag))
			if cached, ok := svc.codexCookies.poolRestState.Load(tag); ok {
				require.Equal(t, state, cached, "a partial read must not invent a newer policy")
			}
		})
	}
}
