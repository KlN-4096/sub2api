package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

type gatewayRotationRepo struct{ schedulerTestOpenAIAccountRepo }

func (r gatewayRotationRepo) ListByPlatform(context.Context, string) ([]Account, error) {
	return r.accounts, nil
}

// Row reads leave the runtime ledgers out, as the database does since
// migration 253; they come only through LoadGatewayPoolLedgers.
func (r gatewayRotationRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	account, err := r.stored(id)
	if err != nil {
		return nil, err
	}
	row := gatewayPoolWithoutLedgers(*account)
	return &row, nil
}

// stored is the row with its ledgers, for assertions.
func (r gatewayRotationRepo) stored(id int64) (*Account, error) {
	return r.schedulerTestOpenAIAccountRepo.GetByID(context.Background(), id)
}

func (r gatewayRotationRepo) FindByExtraField(_ context.Context, key string, value any) ([]Account, error) {
	var found []Account
	for _, account := range r.accounts {
		if account.Extra[key] == value {
			found = append(found, gatewayPoolWithoutLedgers(account))
		}
	}
	return found, nil
}

func (r gatewayRotationRepo) LoadGatewayPoolLedgers(_ context.Context, ids []int64, keys []string) (map[int64]map[string]json.RawMessage, error) {
	return fakeGatewayPoolLedgers(r.accounts, ids, keys)
}

func (r gatewayRotationRepo) LoadGatewayPoolLedgerWithout(_ context.Context, id int64, kind string, omit ...string) (json.RawMessage, error) {
	return fakeGatewayPoolLedgerWithout(r.accounts, id, kind, omit)
}

func (r gatewayRotationRepo) WriteGatewayPoolLedgers(ctx context.Context, id int64, ledgers, tags map[string]any) error {
	patch := maps.Clone(ledgers)
	maps.Copy(patch, tags)
	return r.UpdateExtra(ctx, id, patch)
}

func (r gatewayRotationRepo) UpdateExtra(_ context.Context, id int64, patch map[string]any) error {
	for i := range r.accounts {
		if r.accounts[i].ID == id {
			for key, value := range patch {
				r.accounts[i].Extra[key] = value
			}
		}
	}
	return nil
}

func (r gatewayRotationRepo) SetTempUnschedulable(_ context.Context, id int64, until time.Time, reason string) error {
	for i := range r.accounts {
		if r.accounts[i].ID == id && (r.accounts[i].TempUnschedulableUntil == nil || r.accounts[i].TempUnschedulableUntil.Before(until)) {
			r.accounts[i].TempUnschedulableUntil = &until
			r.accounts[i].TempUnschedulableReason = reason
		}
	}
	return nil
}

func (r gatewayRotationRepo) ClearTempUnschedulable(_ context.Context, id int64) error {
	for i := range r.accounts {
		if r.accounts[i].ID == id {
			r.accounts[i].TempUnschedulableUntil = nil
			r.accounts[i].TempUnschedulableReason = ""
		}
	}
	return nil
}
func rotationAccount(id, group int64) *Account {
	account := gwpoolTestAccount(id)
	account.GroupIDs = []int64{group}
	account.Status, account.Schedulable, account.Concurrency = StatusActive, true, 1
	account.Extra[openAIGatewayPoolRotationExtraKey] = true
	return account
}

// withGatewayPoolRowRead wires the fresh row read (accountByID) as
// NewOpenAIGatewayService does; without it the gateway reads fall back to the
// row they are handed, which since migration 253 carries no ledgers. Peer
// history (historyByTag) stays unwired unless a test sets it.
func withGatewayPoolRowRead(svc *OpenAIGatewayService) *OpenAIGatewayService {
	svc.codexCookies.accountByID = svc.gatewayPoolLedgerRow
	return svc
}

func rotationService(account *Account) *OpenAIGatewayService {
	return withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account}}}})
}

func TestGatewayPoolRotationOnlyAfterFreshCompleteExhaustion(t *testing.T) {
	for _, tc := range []struct {
		name         string
		list         []gwpoolFakeGateway
		status       int
		rotate       bool
		localCooling bool
	}{
		{"historical pool use does not block candidates", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true, UsedByYou: true}}, 0, false, false},
		{"candidate still available", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true, UsedByYou: true}, {Name: "unified-143", PairReady: true}}, 0, false, false},
		{"successful empty supply is zero", []gwpoolFakeGateway{}, 0, true, false},
		{"list failure is not exhaustion", nil, http.StatusBadGateway, false, false},
		{"local cooling independent of pool", []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}}, 0, true, true},
		{"nonready listing has zero candidates", []gwpoolFakeGateway{{Name: "unified-142", PairReady: false, UsedByYou: true}}, 0, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newGwpoolFakePool(t, "offline-cookie", 150)
			fake.listGateways, fake.listStatus = tc.list, tc.status
			account := rotationAccount(1, 7)
			fake.configure(account)
			svc := rotationService(account)
			if tc.localCooling {
				svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, "unified-142")
			}
			failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
			group := int64(7)
			ctx := svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure, nil)
			require.Equal(t, tc.rotate, failure.ShouldRetryNextAccount())
			require.Equal(t, tc.rotate, gatewayPoolRotationFrom(ctx) != nil)
			if tc.rotate {
				remaining, _ := svc.codexCookies.gatewayPoolBackoffFor(gwpoolTestIdentity)
				require.Positive(t, remaining)
				require.LessOrEqual(t, remaining, openAIGatewayPoolDefaultBackoff)
			}
			require.True(t, account.Schedulable, "rest must not disable the account")
			require.Zero(t, fake.hits.Load(), "exhaustion confirmation must not mint or probe")
		})
	}
}

func TestGatewayPoolRotationFreshlyEnabledSourceStopsUnrelatedFailure(t *testing.T) {
	group := int64(7)
	fresh := rotationAccount(1, group)
	stale := *fresh
	stale.Extra = map[string]any{} // scheduler snapshot predates both switches
	svc := rotationService(fresh)
	failure := &UpstreamFailoverError{StatusCode: http.StatusBadGateway}
	svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, &stale, failure, nil)
	require.False(t, failure.ShouldRetryNextAccount(), "current opt-in must apply even when the old snapshot lacked both keys")
}

func TestGatewayPoolRotationNeverSwitchesOnAnyOtherError(t *testing.T) {
	for _, err := range []error{
		errOpenAIGatewayPoolRouteDegraded, errOpenAIGatewayPoolWarmExhausted,
		errOpenAIGatewayPoolWarmUnverified,
		context.DeadlineExceeded, errors.New("authentication failed"), gwpool.ErrNoSlot,
		&gwpool.PoolError{Code: gwpool.CodeNoLivePair}, &gwpool.PoolError{Code: gwpool.CodeNoGateway},
		&gwpool.PoolError{Code: gwpool.CodeRateLimited}, &gwpool.PoolError{Code: gwpool.CodeConsumerRejected},
	} {
		require.False(t, gatewayPoolRotationFailure(err), err.Error())
	}
	require.True(t, gatewayPoolRotationFailure(errGatewayPoolWarmAttemptsFinished))
	require.True(t, gatewayPoolRotationFailure(&gwpool.PoolError{Code: gwpool.CodeAllCooling}))
	account := rotationAccount(1, 7)
	svc := rotationService(account)
	group := int64(7)
	for _, status := range []int{400, 401, 403, 500, 502, 503, 504} { // 429: see the 429 switch tests
		for _, alreadyRotating := range []bool{false, true} {
			ctx := context.Background()
			if alreadyRotating {
				ctx = context.WithValue(ctx, gatewayPoolRotationKey{}, &gatewayPoolRotation{groupID: group})
			}
			failure := &UpstreamFailoverError{StatusCode: status, RetryableOnSameAccount: true}
			svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, failure, nil)
			require.False(t, failure.ShouldRetryNextAccount(), "status %d", status)
			require.Equal(t, gatewayPoolBusinessRetryStatus(status), failure.RetryableOnSameAccount)
		}
	}
}

// A pool member gets one same-account retry per request for a transient 429;
// the next 429 hands the request only to another pool member of its group.
func TestGatewayPoolRotationTransient429RetriesOnceThenSwitches(t *testing.T) {
	group := int64(7)
	account, other := rotationAccount(1, group), rotationAccount(2, group)
	ordinary, elsewhere := rotationAccount(3, group), rotationAccount(4, group+1)
	ordinary.Extra[openAIGatewayPoolExtraKey] = false
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*account, *other, *ordinary, *elsewhere}}}
	svc := withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: repo})
	body := []byte(`{"detail":"Rate limit exceeded"}`)
	failure := svc.newOpenAIAccountFailoverError(account, http.StatusTooManyRequests, nil, body, "Rate limit exceeded", false, false)
	require.True(t, failure.RetryableOnSameAccount)
	ctx := svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure, nil)
	require.True(t, failure.SameAccountRetryOnly, "the first transient 429 retries its credential")
	require.False(t, failure.ShouldRetryNextAccount())
	omit, allowed, err := gatewayPoolRotationExclusions(ctx, repo, &group, nil)
	require.NoError(t, err)
	require.Contains(t, omit, other.ID)
	require.Equal(t, map[int64]struct{}{account.ID: {}}, allowed)
	require.Nil(t, gatewayPoolRotationFrom(ctx), "waiting must not start a shortage rotation")

	next := svc.newOpenAIAccountFailoverError(account, http.StatusTooManyRequests, nil, body, "Rate limit exceeded", false, false)
	ctx = svc.PrepareGatewayPoolAccountRotation(ctx, &group, account, next, nil)
	require.False(t, next.RetryableOnSameAccount)
	require.False(t, next.SameAccountRetryOnly)
	require.True(t, next.ShouldRetryNextAccount(), "a second 429 moves on")
	omit, allowed, err = gatewayPoolRotationExclusions(ctx, repo, &group, nil)
	require.NoError(t, err)
	require.Equal(t, map[int64]struct{}{other.ID: {}}, allowed, "only another pool member of the group")
	for _, id := range []int64{account.ID, ordinary.ID, elsewhere.ID} {
		require.Contains(t, omit, id)
	}
	identity := gwpoolRotationTestIdentity(t, svc, account)
	require.True(t, svc.codexCookies.poolRounds.blocked(group, identity), "the member yields its round")
	remaining, _ := svc.codexCookies.gatewayPoolBackoffFor(identity)
	require.Zero(t, remaining, "a 429 is not a pool shortage: ticket acquisition is not backed off")
}

// Quota, spent-window and unreserved 429s move a pool request on at once,
// still only to another pool member.
func TestGatewayPoolRotationQuotaOrSpent429Switches(t *testing.T) {
	group := int64(7)
	account := rotationAccount(1, group)
	svc := rotationService(account)
	for _, failure := range []*UpstreamFailoverError{
		svc.newOpenAIAccountFailoverError(account, http.StatusTooManyRequests, nil,
			[]byte(`{"error":{"type":"usage_limit_reached","resets_in_seconds":3600}}`), "limit", false, false),
		{StatusCode: http.StatusTooManyRequests, RetryableOnSameAccount: true},
		{StatusCode: http.StatusTooManyRequests, RetryableOnSameAccount: true, SameAccountRetryDeadline: time.Now().Add(-time.Second)},
		{StatusCode: http.StatusTooManyRequests, RetryableOnSameAccount: true, SameAccountRetryDeadline: time.Now().Add(time.Minute),
			NextAccountAction: NextAccountStop},
	} {
		ctx := svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure, nil)
		require.False(t, failure.RetryableOnSameAccount)
		require.False(t, failure.SameAccountRetryOnly)
		require.True(t, failure.ShouldRetryNextAccount())
		require.NotNil(t, gatewayPoolRotationFrom(ctx), "the switch keeps to pool members")
	}
}

func gwpoolRotationTestIdentity(t *testing.T, svc *OpenAIGatewayService, account *Account) string {
	t.Helper()
	identity, err := svc.codexCookies.gatewayPoolIdentity(context.Background(), account)
	require.NoError(t, err)
	return identity
}

func TestGatewayPoolRetryDeadlinePreventsLateBusinessDispatch(t *testing.T) {
	for _, expiresDuringFetch := range []bool{false, true} {
		t.Run(fmt.Sprintf("during_fetch=%t", expiresDuringFetch), func(t *testing.T) {
			account := gwpoolTestAccount(1)
			account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = false
			fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
			fake.configure(account)
			upstream := &gwpoolEchoUpstream{}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			deadline := time.Now().Add(-time.Second)
			if expiresDuringFetch {
				deadline = time.Now().Add(30 * time.Millisecond)
				fake.onCookie = func() {
					delay := time.Until(deadline)
					if delay > 0 {
						time.Sleep(delay + time.Millisecond)
					}
				}
			}
			ctx := context.WithValue(context.Background(), gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{
				accountID: account.ID, deadline: deadline,
				failure: &UpstreamFailoverError{StatusCode: 429, ResponseBody: []byte(`{"detail":"Rate limit exceeded"}`)},
			})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, nil)
			require.NoError(t, err)
			response, err := svc.doOpenAIUpstream(req, "", account)
			require.Nil(t, response)
			var failure *UpstreamFailoverError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, 429, failure.StatusCode)
			require.False(t, failure.RetryableOnSameAccount)
			require.Empty(t, upstream.sentBodies)
			require.NoError(t, ctx.Err(), "dispatch admission must not impose a response-stream deadline")
		})
	}
}

func TestGatewayPoolRotationFreshOptInsAndGroupIsolation(t *testing.T) {
	group := int64(7)
	source := rotationAccount(1, group)
	eligible := rotationAccount(2, group)
	off := rotationAccount(3, group)
	off.Extra[openAIGatewayPoolRotationExtraKey] = false
	noPool := rotationAccount(4, group)
	noPool.Extra[openAIGatewayPoolExtraKey] = false
	other := rotationAccount(5, group+1)
	disabled := rotationAccount(6, group)
	disabled.Schedulable = false
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*source, *eligible, *off, *noPool, *other, *disabled}}}
	ctx := context.WithValue(context.Background(), gatewayPoolRotationKey{}, &gatewayPoolRotation{
		groupID: group, attempted: map[int64]struct{}{1: {}},
	})
	omit, allowed, err := gatewayPoolRotationExclusions(ctx, repo, &group, nil)
	require.NoError(t, err)
	require.Equal(t, map[int64]struct{}{2: {}, 3: {}}, allowed, "rotation is mandatory even with legacy false")
	for _, id := range []int64{1, 4, 5, 6} {
		require.Contains(t, omit, id)
	}
	for _, bad := range []*int64{nil, &other.GroupIDs[0]} {
		_, _, err = gatewayPoolRotationExclusions(ctx, repo, bad, nil)
		require.Error(t, err)
	}
	// Simulate changing an opt-in after the listing but before final selection.
	repo.accounts[1].Extra[openAIGatewayPoolExtraKey] = false
	require.False(t, gatewayPoolRotationRecheck(ctx, repo, allowed, &AccountSelectionResult{Account: eligible}))
	// A stale enabled source cannot initiate rotation after its DB setting was disabled.
	failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
	stale := *source
	stale.Extra = map[string]any{openAIGatewayPoolExtraKey: true, openAIGatewayPoolRotationExtraKey: true}
	repo.accounts[0].Extra[openAIGatewayPoolExtraKey] = false
	svc := withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: repo})
	svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, &stale, failure, nil)
	require.False(t, failure.ShouldRetryNextAccount())
}

func TestGatewayPoolRotationExclusionsReturnExactSets(t *testing.T) {
	group := int64(7)
	paused := rotationAccount(4, group)
	paused.Schedulable = false
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{
		*rotationAccount(1, group), *rotationAccount(2, group), *rotationAccount(3, group+1), *paused,
	}}}
	set := func(ids ...int64) map[int64]struct{} {
		out := map[int64]struct{}{}
		for _, id := range ids {
			out[id] = struct{}{}
		}
		return out
	}
	retry := func(id int64) context.Context {
		return context.WithValue(context.Background(), gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{
			accountID: id, groupID: group, deadline: time.Now().Add(time.Minute),
		})
	}
	excluded := set(9)
	omit, allowed, err := gatewayPoolRotationExclusions(retry(2), repo, &group, excluded)
	require.NoError(t, err)
	require.Equal(t, set(1, 3, 4, 9), omit)
	require.Equal(t, set(2), allowed)
	require.Equal(t, set(9), excluded, "the caller's set is cloned, not mutated")
	for _, tc := range []struct {
		id       int64
		excluded map[int64]struct{}
	}{{4, nil}, {3, nil}, {8, nil}, {2, set(2)}} {
		_, _, err = gatewayPoolRotationExclusions(retry(tc.id), repo, &group, tc.excluded)
		require.ErrorIs(t, err, ErrNoAvailableAccounts, "retry-only never widens to another credential")
	}
	rotating := context.WithValue(context.Background(), gatewayPoolRotationKey{}, &gatewayPoolRotation{
		groupID: group, attempted: set(1, 8),
	})
	omit, allowed, err = gatewayPoolRotationExclusions(rotating, repo, &group, excluded)
	require.NoError(t, err)
	require.Equal(t, set(1, 3, 4, 8, 9), omit, "an attempted ID stays omitted even when no longer listed")
	require.Equal(t, set(2), allowed)
	_, _, err = gatewayPoolRotationExclusions(rotating, repo, &group, set(2))
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	omit, allowed, err = gatewayPoolRotationExclusions(context.Background(), repo, &group, excluded)
	require.NoError(t, err)
	require.Equal(t, excluded, omit)
	require.Nil(t, allowed)
}

func TestGatewayPoolRotationUsesExistingSchedulerAndDoesNotRevisitAccounts(t *testing.T) {
	group := int64(7)
	source := rotationAccount(1, group)
	target := rotationAccount(2, group)
	target.Credentials = map[string]any{"chatgpt_account_id": "rotation-target", "chatgpt_user_id": "user"}
	off := rotationAccount(3, group)
	off.Extra[openAIGatewayPoolRotationExtraKey] = false
	source.Priority, target.Priority, off.Priority = 0, 2, 1
	repo := gatewayRotationRepo{schedulerTestOpenAIAccountRepo{accounts: []Account{*source, *target, *off}}}
	svc := withGatewayPoolRowRead(&OpenAIGatewayService{accountRepo: repo, cache: &schedulerTestGatewayCache{}, cfg: &config.Config{},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})})
	ctx := context.WithValue(context.Background(), gatewayPoolRotationKey{}, &gatewayPoolRotation{
		groupID: group, attempted: map[int64]struct{}{1: {}},
	})
	selection, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &group, "", "", "gpt-6-astra", nil,
		OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
	require.NoError(t, err)
	require.Equal(t, int64(2), selection.Account.ID)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
	ctx = context.WithValue(ctx, gatewayPoolRotationKey{}, &gatewayPoolRotation{groupID: group, attempted: map[int64]struct{}{1: {}, 2: {}}})
	_, _, err = svc.SelectAccountWithSchedulerForCapability(ctx, &group, "", "", "gpt-6-astra", nil,
		OpenAIUpstreamTransportHTTPSSE, OpenAIEndpointCapabilityChatCompletions, false, false, true)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
}

func TestGatewayPoolRotationCanceledOrNoGroupDoesNotReadPool(t *testing.T) {
	account := rotationAccount(1, 7)
	svc := rotationService(account)
	failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
	svc.PrepareGatewayPoolAccountRotation(context.Background(), nil, account, failure, nil)
	require.False(t, failure.ShouldRetryNextAccount())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	require.False(t, svc.gatewayPoolNoRemainingRoutes(ctx, account))
}

func TestGatewayPoolRotationDoesNotConfuseDeliveredWithTried(t *testing.T) {
	for _, state := range []string{"in-flight", "live-unverified"} {
		t.Run(state, func(t *testing.T) {
			fake := newGwpoolFakePool(t, "offline-cookie", 150)
			fake.listGateways = []gwpoolFakeGateway{{Name: "unified-71", PairReady: true, UsedByYou: true}}
			account := rotationAccount(40, 2)
			fake.configure(account)
			svc := rotationService(account)
			pair := openAIGatewayPoolPair{cookie: "offline-cookie", gateway: "unified-71", version: "unused"}
			if state == "in-flight" {
				finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
				defer finish()
			} else {
				svc.codexCookies.poolPairs.Store(gwpoolTestIdentity, pair)
				// Acquisition reserves the local attempt before the probe starts.
				svc.codexCookies.gatewayPoolMarkUsed(gwpoolTestIdentity, pair.gateway)
			}
			failure := &UpstreamFailoverError{GatewayPoolRotation: true, NextAccountAction: NextAccountStop}
			group := int64(2)
			svc.PrepareGatewayPoolAccountRotation(context.Background(), &group, account, failure, nil)
			require.False(t, failure.ShouldRetryNextAccount(), "delivered/local-reserved does not prove actually attempted")
		})
	}
}

func TestGatewayPoolRotationRechecksInventoryChangedDuringListing(t *testing.T) {
	fake := newGwpoolFakePool(t, "offline-cookie", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-71", PairReady: true, UsedByYou: true}}
	account := rotationAccount(40, 2)
	fake.configure(account)
	svc := rotationService(account)
	fake.onList = func() {
		// An entire fetch/probe may complete while the remote listing is in
		// flight; the listing predates that work even if active is back to zero.
		finish := svc.codexCookies.gatewayPoolInventoryOperation(gwpoolTestIdentity)
		finish()
	}
	require.False(t, svc.gatewayPoolNoRemainingRoutes(context.Background(), account))
}

func TestGatewayPoolRotationDoesNotSkipConcurrentWarmProbe(t *testing.T) {
	fake := newGwpoolFakePool(t, "offline-cookie", 150)
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-71", PairReady: true, UsedByYou: true}}
	account := rotationAccount(40, 2)
	fake.configure(account)
	svc := rotationService(account)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _, _, _, _ = svc.codexCookies.gatewayPoolWarmVerdict(context.Background(), account, gwpoolTestIdentity,
			OpenAIGatewayPoolApplied{Gateway: "unified-71", Version: "inflight"}, "offline-cookie", 1,
			func(context.Context, string, string) (int, string, error) {
				select {
				case <-started:
				default:
					close(started)
				}
				<-release
				return http.StatusOK, "", nil
			})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("probe did not start")
	}
	blocked := !svc.gatewayPoolNoRemainingRoutes(context.Background(), account)
	close(release)
	<-done
	require.True(t, blocked)
	require.Zero(t, fake.listHits.Load(), "local pending inventory should stop before remote listing")
}
