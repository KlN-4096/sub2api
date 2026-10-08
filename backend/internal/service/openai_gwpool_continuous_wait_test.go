package service

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/stretchr/testify/require"
)

func TestGatewayPoolContinuousWaitConfigAndBudget(t *testing.T) {
	account := gwpoolTestAccount(1)
	require.False(t, account.GatewayPoolContinuousWaitEnabled())
	account.Extra[OpenAIGatewayPoolContinuousWaitKey] = "true"
	require.False(t, account.GatewayPoolContinuousWaitEnabled(), "malformed opt-in cannot remove limits")
	require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra))
	account.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
	svc := &OpenAIGatewayService{}
	ctx := svc.gatewayPoolWaitContext(context.Background(), account)
	state := gatewayPoolWaitFrom(ctx)
	require.NotNil(t, state)
	require.True(t, state.continuous)
	_, hasDeadline := ctx.Deadline()
	require.False(t, hasDeadline, "waiting does not invent an aggregate deadline")
	require.Positive(t, account.gatewayPoolFetchTimeout(), "per-operation limits remain")
	require.Same(t, state, gatewayPoolWaitFrom(svc.gatewayPoolWaitContext(ctx, account)))
}

func TestGatewayPoolContinuousWaitRetriesEmptyPoolWithoutBusinessOrRest(t *testing.T) {
	svc, repo, fake, account, request, _ := ticketWaitFixture(t)
	account.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
	account.Extra[openAIGatewayPoolWaitSecondsExtraKey] = 1
	account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
	account.Extra[openAIGatewayPoolRotationMinGatewaysExtraKey] = 20
	repo.account = *account
	fake.refuseStatus, fake.refuseCode, fake.refuseRetryAfter = 503, gwpool.CodeNoLivePair, 1
	account, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), nil)
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	ctx = svc.gatewayPoolWaitContext(ctx, account)
	state := gatewayPoolWaitFrom(ctx)
	require.True(t, state.continuous)
	sleeps := 0
	state.sleep = func(ctx context.Context, _ time.Duration) error {
		sleeps++
		// Wait longer than this account's ordinary preparation budget.
		if err := gatewayPoolSleep(ctx, 1100*time.Millisecond); err != nil {
			return err
		}
		fake.refuseStatus = 0
		fake.listGateways = []gwpoolFakeGateway{{Name: fake.gateway, PairReady: true}}
		return nil
	}
	shots := 0
	shoot := func(context.Context, string, string) (int, string, error) {
		shots++
		return http.StatusOK, "stable", nil
	}
	require.NoError(t, svc.gatewayPoolWarmUpWith(request.WithContext(ctx), account,
		gwpoolTestIdentity, gwpoolWarmModel, shoot))
	require.Equal(t, 1, sleeps)
	require.Equal(t, 2, shots, "only the normal A/B verification; no business replay")
	require.False(t, readGatewayPoolRest(&repo.account, gatewayPoolLedgerTag(gwpoolTestIdentity)).Active)
	require.Nil(t, repo.account.TempUnschedulableUntil)
}

func TestGatewayPoolContinuousWaitCancellationAndConfigChange(t *testing.T) {
	for _, change := range []string{"cancel", "disable", "auth"} {
		t.Run(change, func(t *testing.T) {
			svc, repo, fake, account, request, _ := ticketWaitFixture(t)
			account.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
			repo.account = *account
			account, err := repo.GetByID(context.Background(), account.ID)
			require.NoError(t, err)
			fake.refuseStatus, fake.refuseCode = 503, gwpool.CodeAllCooling
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx, _ = withOpenAIGatewayPoolSink(ctx, nil)
			ctx = svc.gatewayPoolWaitContext(ctx, account)
			gatewayPoolWaitFrom(ctx).sleep = func(context.Context, time.Duration) error {
				switch change {
				case "cancel":
					cancel()
				case "disable":
					require.NoError(t, repo.UpdateExtra(ctx, account.ID, map[string]any{OpenAIGatewayPoolContinuousWaitKey: false}))
				case "auth":
					repo.mu.Lock()
					until := time.Now().Add(time.Hour)
					repo.account.TempUnschedulableUntil, repo.account.TempUnschedulableReason = &until, "auth"
					repo.mu.Unlock()
				}
				return nil
			}
			err = svc.gatewayPoolWarmUpWith(request.WithContext(ctx), account, gwpoolTestIdentity, gwpoolWarmModel,
				func(context.Context, string, string) (int, string, error) {
					t.Error("empty pool must not probe")
					return 0, "", nil
				})
			require.Error(t, err)
			require.Eventually(t, func() bool { return svc.codexCookies.gatewayPoolPreparationWaiters(gwpoolTestIdentity) == 0 },
				time.Second, time.Millisecond)
		})
	}
}

func TestGatewayPoolContinuousWaitNeverRetriesAuthentication(t *testing.T) {
	for _, code := range []string{"invalid_key", gwpool.CodeRateLimited} {
		t.Run(code, func(t *testing.T) {
			svc, repo, fake, account, _, _ := ticketWaitFixture(t)
			account.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
			repo.account = *account
			fake.refuseStatus, fake.refuseCode = 401, code
			ctx, _ := withOpenAIGatewayPoolSink(context.Background(), nil)
			ctx = svc.gatewayPoolWaitContext(ctx, account)
			gatewayPoolWaitFrom(ctx).sleep = func(context.Context, time.Duration) error {
				t.Fatal("authentication and rate limits are not ticket shortages")
				return nil
			}
			_, err := svc.attachGatewayPoolRouteWithWait(ctx, account, gwpoolTestURL, http.Header{})
			require.Error(t, err)
			require.EqualValues(t, 1, fake.hits.Load())
		})
	}
}

func TestGatewayPoolContinuousWaitDoesNotRecoverOrdinaryClone(t *testing.T) {
	account := rotationAccount(1, 7)
	account.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
	until := time.Now().Add(time.Hour)
	account.TempUnschedulableUntil, account.TempUnschedulableReason = &until, gatewayPoolRestReason(account)
	svc, base := gatewayRuntimeService(account)
	repo := &gatewayLocalRestRepo{base}
	svc.accountRepo = repo
	tag := gatewayPoolLedgerTag(gwpoolTestIdentity)
	rest := gatewayPoolRestState{Tag: tag, Active: true, ChangedAt: time.Now(), ResumeAt: until}
	svc.codexCookies.poolRestState.Store(tag, rest)
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{gatewayPoolRestStateKey: rest}))
	allowed, err := svc.gatewayPoolResumeAllowed(context.Background(), account, true)
	require.NoError(t, err)
	require.True(t, allowed)
	fresh, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	require.Nil(t, fresh.TempUnschedulableUntil)
	require.True(t, readGatewayPoolRest(fresh, tag).Active, "do not overwrite the shared rest history")
	require.False(t, svc.gatewayPoolRestDisplay(fresh, gwpoolTestIdentity, nil).Active)
	ordinary := *fresh
	ordinary.Extra = map[string]any{openAIGatewayPoolExtraKey: true}
	require.True(t, svc.gatewayPoolRestDisplay(&ordinary, gwpoolTestIdentity, nil).Active)
	require.NoError(t, repo.UpdateExtra(context.Background(), account.ID, map[string]any{OpenAIGatewayPoolContinuousWaitKey: false}))
	require.False(t, gatewayPoolWaitAccountMatches(&ordinary, fresh), "the original request snapshot stays in waiting mode")
}

func TestGatewayPoolContinuousWaitSharedDomainAdmissionKeepsN(t *testing.T) {
	var rounds gatewayPoolRounds
	domain := gwpoolTestIdentity
	rounds.setContinuousAccount(7, 2, gwpoolTestIdentity, true)
	require.True(t, rounds.claim(7, gwpoolTestIdentity, 1))
	rounds.rest(7, gwpoolTestIdentity, time.Now().Add(time.Hour))
	excluded, _ := rounds.snapshot(7, map[int64]string{1: domain, 2: domain}, true)
	_, restingClone := excluded[1]
	_, waitingClone := excluded[2]
	require.True(t, restingClone)
	require.False(t, waitingClone)
	require.True(t, rounds.claim(7, gwpoolTestIdentity, 1))
	require.False(t, rounds.claim(7, "other", 1), "waiting still consumes one domain in group N")
	rounds.reconcileContinuous(7, map[int64]string{}, true)
	require.True(t, rounds.blocked(7, gwpoolTestIdentity))
	require.False(t, rounds.claim(7, gwpoolTestIdentity, 1))
}

func TestGatewayPoolContinuousWaitHealthPreservesOtherBlocks(t *testing.T) {
	account := gwpoolTestAccount(1)
	account.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
	until := time.Now().Add(time.Hour)
	account.TempUnschedulableUntil = &until
	account.TempUnschedulableReason = gatewayPoolRestReason(account)
	require.True(t, gatewayPoolWaitHealth(account))
	for _, reason := range []string{"auth", "provider429", strings.TrimPrefix(account.TempUnschedulableReason, "网关候选低于")} {
		account.TempUnschedulableReason = reason
		require.False(t, gatewayPoolWaitHealth(account))
	}
}

func continuousWaitClones(t *testing.T, secondContinuous bool) (*OpenAIGatewayService, *gatewayPoolAccountsRepo, *gwpoolFakePool, []*Account) {
	t.Helper()
	fake := newGwpoolFakePool(t, gwpoolTestPairCookie(t, "unified-142"), 150)
	repo := &gatewayPoolAccountsRepo{rows: map[int64]*gatewayRuntimeRepo{}}
	accounts := []*Account{}
	for _, id := range []int64{1, 2} {
		account := rotationAccount(id, 7)
		fake.configure(account)
		account.Extra[OpenAIGatewayPoolContinuousWaitKey] = id == 1 || secondContinuous
		account.Extra[openAIGatewayPoolGuardEnabledExtraKey] = true
		repo.rows[id] = &gatewayRuntimeRepo{account: *account}
		fresh, err := repo.GetByID(context.Background(), id)
		require.NoError(t, err)
		accounts = append(accounts, fresh)
	}
	svc := &OpenAIGatewayService{accountRepo: repo}
	svc.codexCookies.accountByID = repo.GetByID
	return svc, repo, fake, accounts
}

func TestGatewayPoolContinuousWaitHealthyFollowerSurvivesOwnerSwitch(t *testing.T) {
	svc, repo, fake, accounts := continuousWaitClones(t, true)
	firstCookie, nextCookie := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-143")
	fake.cookieForHit = func(hit int64) string {
		if hit > 1 {
			return nextCookie
		}
		return firstCookie
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	resumeOwner := func() { once.Do(func() { close(resume) }) }
	defer resumeOwner()
	ownerDone, followerDone := make(chan error, 1), make(chan error, 1)
	run := func(account *Account, shoot gatewayPoolWarmShooter, done chan<- error) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, nil)
		work, _ := withOpenAIGatewayPoolSink(ctx, nil)
		done <- svc.gatewayPoolWarmUpWith(request.WithContext(work), account, gwpoolTestIdentity, gwpoolWarmModel, shoot)
	}
	go run(accounts[0], func(ctx context.Context, _, _ string) (int, string, error) {
		close(started)
		select {
		case <-resume:
			return 0, "", errGatewayPoolPreparationOwnerChanged
		case <-ctx.Done():
			return 0, "", ctx.Err()
		}
	}, ownerDone)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("owner never entered probe")
	}
	go run(accounts[1], func(context.Context, string, string) (int, string, error) {
		return http.StatusOK, "same-state", nil
	}, followerDone)
	require.Eventually(t, func() bool { return svc.codexCookies.gatewayPoolPreparationWaiters(gwpoolTestIdentity) == 2 },
		time.Second, time.Millisecond)
	require.NoError(t, repo.UpdateExtra(ctx, 1, map[string]any{OpenAIGatewayPoolContinuousWaitKey: false}))
	resumeOwner()
	require.Error(t, <-ownerDone)
	require.NoError(t, <-followerDone, "a healthy clone must not inherit the leader's disabled policy")
}

func TestGatewayPoolContinuousWaitOrdinaryFollowerReceivesExhaustion(t *testing.T) {
	svc, _, fake, accounts := continuousWaitClones(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	listed, resume, sleeping := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	var first sync.Once
	fake.onList = func() { first.Do(func() { close(listed); <-resume }) }
	fake.refuseStatus, fake.refuseCode = 503, gwpool.CodeAllCooling
	ownerDone, followerDone := make(chan error, 1), make(chan error, 1)
	run := func(account *Account, done chan<- error) {
		work, _ := withOpenAIGatewayPoolSink(ctx, nil)
		work = svc.gatewayPoolWaitContext(work, account)
		gatewayPoolWaitFrom(work).sleep = func(ctx context.Context, _ time.Duration) error {
			select {
			case sleeping <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
		request, _ := http.NewRequestWithContext(work, http.MethodPost, gwpoolTestURL, nil)
		done <- svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel,
			func(context.Context, string, string) (int, string, error) {
				t.Error("no ticket to probe")
				return 0, "", nil
			})
	}
	go run(accounts[0], ownerDone)
	select {
	case <-listed:
	case <-ctx.Done():
		t.Fatal("no list")
	}
	go run(accounts[1], followerDone)
	require.Eventually(t, func() bool { return svc.codexCookies.gatewayPoolPreparationWaiters(gwpoolTestIdentity) == 2 },
		time.Second, time.Millisecond)
	release()
	select {
	case err := <-followerDone:
		require.ErrorIs(t, err, errGatewayPoolWarmAttemptsFinished)
	case <-time.After(time.Second):
		t.Fatal("ordinary follower was held by continuous leader")
	}
	select {
	case <-sleeping:
	case <-time.After(time.Second):
		t.Fatal("continuous caller did not wait")
	}
	cancel()
	require.ErrorIs(t, <-ownerDone, context.Canceled)
}

func TestGatewayPoolContinuousWaitOrdinaryFollowerTriesLastCandidate(t *testing.T) {
	svc, repo, fake, accounts := continuousWaitClones(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	require.NoError(t, repo.UpdateExtra(ctx, accounts[1].ID, map[string]any{openAIGatewayPoolRotationMinGatewaysExtraKey: 2}))
	ordinary, err := repo.GetByID(ctx, accounts[1].ID)
	require.NoError(t, err)
	firstCookie, nextCookie := gwpoolTestPairCookie(t, "unified-142"), gwpoolTestPairCookie(t, "unified-143")
	fake.cookieForHit = func(hit int64) string {
		if hit > 1 {
			return nextCookie
		}
		return firstCookie
	}
	fake.listGateways = []gwpoolFakeGateway{{Name: "unified-142", PairReady: true}, {Name: "unified-143", PairReady: true}}
	firstStarted, resumeFirst, nextStarted, resumeNext := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var firstOnce, nextOnce sync.Once
	unblockFirst := func() { firstOnce.Do(func() { close(resumeFirst) }) }
	unblockNext := func() { nextOnce.Do(func() { close(resumeNext) }) }
	defer unblockFirst()
	defer unblockNext()
	shots := 0
	shoot := func(ctx context.Context, _, _ string) (int, string, error) {
		shots++
		switch shots {
		case 1:
			close(firstStarted)
			select {
			case <-resumeFirst:
				return 200, "first-a", nil
			case <-ctx.Done():
				return 0, "", ctx.Err()
			}
		case 2:
			return 200, "first-b", nil // first candidate is conclusively degraded
		case 3:
			close(nextStarted)
			select {
			case <-resumeNext:
			case <-ctx.Done():
				return 0, "", ctx.Err()
			}
		}
		return 200, "replacement-proof", nil
	}
	run := func(account *Account, done chan<- error) {
		work, _ := withOpenAIGatewayPoolSink(ctx, nil)
		request, _ := http.NewRequestWithContext(work, http.MethodPost, gwpoolTestURL, nil)
		done <- svc.gatewayPoolWarmUpWith(request, account, gwpoolTestIdentity, gwpoolWarmModel, shoot)
	}
	ownerDone, ordinaryDone := make(chan error, 1), make(chan error, 1)
	go run(accounts[0], ownerDone)
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal("first probe did not start")
	}
	go run(ordinary, ordinaryDone)
	require.Eventually(t, func() bool { return svc.codexCookies.gatewayPoolPreparationWaiters(gwpoolTestIdentity) == 2 },
		time.Second, time.Millisecond)
	unblockFirst()
	select {
	case <-nextStarted:
	case <-ctx.Done():
		t.Fatal("preparation did not continue with the remaining candidate")
	}
	select {
	case err := <-ordinaryDone:
		t.Fatalf("ordinary follower ended before the last candidate verified: %v", err)
	default:
	}
	unblockNext()
	require.NoError(t, <-ownerDone)
	require.NoError(t, <-ordinaryDone)
}

func TestGatewayPoolContinuousWaitRefreshesBearerBeforeBusiness(t *testing.T) {
	svc, repo, _, account, _, _ := ticketWaitFixture(t)
	account.Extra[OpenAIGatewayPoolContinuousWaitKey] = true
	account.Credentials["access_token"] = "old-offline-token"
	repo.account = *account
	original, err := repo.GetByID(context.Background(), account.ID)
	require.NoError(t, err)
	ctx, _ := withOpenAIGatewayPoolSink(context.Background(), nil)
	ctx = svc.gatewayPoolWaitContext(ctx, original)
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, strings.NewReader("unchanged-body"))
	request.Header.Set("Authorization", "Bearer old-offline-token")
	repo.mu.Lock()
	repo.account.Credentials["access_token"] = "new-offline-token"
	repo.mu.Unlock()
	require.NoError(t, svc.refreshGatewayPoolWaitingAuth(request, original))
	require.Equal(t, "Bearer new-offline-token", request.Header.Get("Authorization"))
	require.NotNil(t, request.Body)
}
