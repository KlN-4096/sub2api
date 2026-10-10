package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
	"github.com/gin-gonic/gin"
)

const openAIGatewayPoolResumeGatewaysExtraKey = "openai_gwpool_resume_gateways"
const gatewayPoolResumeGatewaysMax = 512
const gatewayPoolResumeGatewaysDefault = 50

func (a *Account) gatewayPoolResumeGateways() int {
	threshold := gatewayPoolResumeGatewaysDefault
	if a != nil {
		if n := a.getExtraInt(openAIGatewayPoolResumeGatewaysExtraKey); n >= 1 && n <= gatewayPoolResumeGatewaysMax {
			threshold = n
		}
	}
	return threshold
}

// Distinguishes completed, conclusive ticket attempts from time-budget expiry.
// Reaching this limit alone still does NOT authorize account rotation.
var errGatewayPoolWarmAttemptsFinished = fmt.Errorf("gateway attempts finished: %w", errOpenAIGatewayPoolWarmExhausted)

type gatewayPoolRotationKey struct{}
type gatewayPoolRetryOnlyKey struct{}
type gatewayPoolRetryOnly struct {
	accountID int64
	groupID   int64
	deadline  time.Time
	failure   *UpstreamFailoverError
	holder    *gatewayPoolWaitHolder // distinguishes a new WS turn from a retry
}

func gatewayPoolRetryOnlyFrom(ctx context.Context) gatewayPoolRetryOnly {
	state, _ := ctx.Value(gatewayPoolRetryOnlyKey{}).(gatewayPoolRetryOnly)
	return state
}

// GatewayPoolRetryFailure checks dispatch admission only. It deliberately does
// not add a context deadline that could cut off a successful response stream.
func GatewayPoolRetryFailure(ctx context.Context) *UpstreamFailoverError {
	retry := gatewayPoolRetryOnlyFrom(ctx)
	if retry.accountID == 0 || retry.deadline.IsZero() || time.Now().Before(retry.deadline) {
		return nil
	}
	failure := UpstreamFailoverError{StatusCode: http.StatusTooManyRequests}
	if retry.failure != nil {
		failure = *retry.failure
	}
	failure.RetryableOnSameAccount, failure.SameAccountRetryOnly = false, false
	failure.NextAccountAction = NextAccountStop
	return &failure
}

type gatewayPoolRotation struct {
	groupID   int64
	attempted map[int64]struct{}
	domains   map[string]struct{}
}

func gatewayPoolRotationFrom(ctx context.Context) *gatewayPoolRotation {
	state, _ := ctx.Value(gatewayPoolRotationKey{}).(*gatewayPoolRotation)
	return state
}

func GatewayPoolAccountRotationActive(ctx context.Context) bool {
	return gatewayPoolRotationFrom(ctx) != nil
}

func gatewayPoolRotationAccount(account *Account, groupID int64) bool {
	return account != nil && groupID > 0 && account.IsOpenAIOAuthLike() &&
		account.UsesGatewayPool() &&
		openAIStickyAccountMatchesGroup(account, &groupID)
}

// Only potentially exhausted candidate sets may be examined further. Even
// all_cooling may describe a partial batch, so a fresh listing must confirm it.
func gatewayPoolRotationFailure(err error) bool {
	if errors.Is(err, errGatewayPoolWarmAttemptsFinished) {
		return true
	}
	var poolErr *gwpool.PoolError
	if errors.As(err, &poolErr) {
		return poolErr.Code == gwpool.CodeAllCooling
	}
	return false
}

// PrepareGatewayPoolAccountRotation must be called only AFTER the handler's
// no-semantic-output/replay guard. It never widens an unrelated stop decision.
func (s *OpenAIGatewayService) PrepareGatewayPoolAccountRotation(ctx context.Context, groupID *int64, source *Account, failure *UpstreamFailoverError, clients ...*gin.Context) context.Context {
	if failure == nil {
		return ctx
	}
	previousRetry := gatewayPoolRetryOnlyFrom(ctx)
	var holder *gatewayPoolWaitHolder
	for _, client := range clients {
		if client != nil {
			if value, exists := client.Get(gatewayPoolWaitGinKey); exists {
				holder, _ = value.(*gatewayPoolWaitHolder)
			}
		}
	}
	if holder != nil && previousRetry.holder != nil && holder != previousRetry.holder {
		// The handler's captured connection context can outlive the ingress
		// turn context. Never carry the previous turn's 429 deadline into the
		// next turn after ingress has installed a new holder.
		previousRetry = gatewayPoolRetryOnly{}
		ctx = context.WithValue(ctx, gatewayPoolWaitKey{}, (*gatewayPoolWaitState)(nil))
	}
	ctx = context.WithValue(ctx, gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{})
	if source != nil && previousRetry.accountID == source.ID && !previousRetry.deadline.IsZero() &&
		previousRetry.deadline.Before(failure.SameAccountRetryDeadline) {
		failure.SameAccountRetryDeadline = previousRetry.deadline
	}
	state := gatewayPoolRotationFrom(ctx)
	if state == nil && (source == nil || !source.IsOpenAIOAuthLike()) {
		return ctx
	}
	if s == nil || s.accountRepo == nil || groupID == nil || source == nil {
		if state != nil || (source != nil && source.UsesGatewayPool()) {
			failure.NextAccountAction = NextAccountStop
		}
		return ctx
	}
	fresh, err := s.accountRepo.GetByID(ctx, source.ID)
	if state == nil && err == nil && fresh != nil && !fresh.UsesGatewayPool() {
		return ctx
	}
	// A confirmed transient 429 may wait on the same credential, but never
	// grants permission to switch credentials. The deadline/reservation was
	// already established by the upstream error classifier.
	retry429 := failure.StatusCode == http.StatusTooManyRequests &&
		failure.RetryableOnSameAccount && !failure.SameAccountRetryDeadline.IsZero() &&
		time.Now().Before(failure.SameAccountRetryDeadline) &&
		failure.SameAccountRetryNotBefore.Before(failure.SameAccountRetryDeadline) &&
		(failure.NextAccountAction != NextAccountStop || failure.SameAccountRetryOnly) && !failure.GatewayPoolRotation
	recoverBeforeOutput := failure.GatewayPoolRetry || (gatewayPoolBusinessRetryStatus(failure.StatusCode) &&
		failure.ShouldRetryNextAccount() && !failure.IsCredentialFailure() &&
		!failure.GatewayPoolRotation && failure.Reason != OpenAIGatewayPoolReason)
	failure.GatewayPoolRetry = false
	failure.RetryableOnSameAccount = false
	failure.SameAccountRetryOnly = false
	failure.NextAccountAction = NextAccountStop
	if err != nil || !gatewayPoolRotationAccount(fresh, *groupID) || !fresh.IsActive() ||
		(state != nil && state.groupID != *groupID) {
		return ctx
	}
	var retryDeadline time.Time
	if previousRetry.accountID == fresh.ID {
		retryDeadline = previousRetry.deadline
		if !retryDeadline.IsZero() && !time.Now().Before(retryDeadline) {
			return ctx // a later HTTP fault cannot restart a spent real 429 window
		}
	}
	if retry429 && fresh.IsSchedulable() {
		failure.RetryableOnSameAccount = true
		failure.SameAccountRetryOnly = true
		return context.WithValue(ctx, gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{
			accountID: fresh.ID, groupID: *groupID, deadline: failure.SameAccountRetryDeadline, failure: failure, holder: holder,
		})
	}
	if recoverBeforeOutput && gatewayPoolWaitHealth(fresh) {
		// Reuse the captured credential policy within the logical request.
		// New WS turns have a new holder; real 429 deadlines remain separate.
		for _, client := range clients {
			if client != nil {
				if value, exists := client.Get(gatewayPoolWaitGinKey); exists {
					if holder, ok := value.(*gatewayPoolWaitHolder); ok {
						holder.mu.Lock()
						budget := holder.state
						holder.mu.Unlock()
						if budget != nil && budget.accountID == fresh.ID &&
							budget.continuous == fresh.GatewayPoolContinuousWaitEnabled() {
							ctx = context.WithValue(ctx, gatewayPoolWaitKey{}, budget)
						}
					}
				}
			}
		}
		ctx = s.gatewayPoolWaitContext(ctx, fresh)
		if gatewayPoolWaitFrom(ctx) != nil && ctx.Err() == nil {
			for _, client := range clients {
				if client != nil && client.Request != nil {
					if value, ok := client.Get(openAIGatewayPoolSinkGinKey); ok {
						if sink, valid := value.(*openAIGatewayPoolSink); valid {
							attemptCtx := context.WithValue(ctx, openAIGatewayPoolSinkCtxKey{}, sink)
							s.retireGatewayPoolBusinessAttempt(client.Request.WithContext(attemptCtx), fresh)
						}
					}
				}
			}
			failure.RetryableOnSameAccount, failure.SameAccountRetryOnly, failure.GatewayPoolRetry = true, true, true
			failure.SameAccountRetryDeadline = retryDeadline
			failure.SameAccountRetryDelay = gatewayPoolVerificationRetryGap
			failure.SameAccountRetryMax = 0
			failure.Reason = OpenAIGatewayPoolReason
			return context.WithValue(ctx, gatewayPoolRetryOnlyKey{}, gatewayPoolRetryOnly{
				accountID: fresh.ID, groupID: *groupID, deadline: retryDeadline, failure: failure, holder: holder,
			})
		}
	}
	if fresh.GatewayPoolContinuousWaitEnabled() {
		return ctx // shortages wait before dispatch; they never authorize credential rotation
	}
	generation := s.codexCookies.poolRounds.generation(*groupID)
	if !failure.GatewayPoolRotation {
		return ctx
	}
	plan := s.gatewayPoolExhaustedRestQuality(ctx, fresh)
	if plan == nil {
		return ctx
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if err != nil {
		return ctx
	}
	// Mark before finding a replacement, including the last account in a round.
	plan.roundGeneration = generation
	if err := s.restGatewayPoolAccount(ctx, fresh, identity, *groupID, *plan); err != nil {
		return ctx
	}
	next := &gatewayPoolRotation{groupID: *groupID, attempted: map[int64]struct{}{source.ID: {}},
		domains: map[string]struct{}{identity: {}}}
	if state != nil {
		for id := range state.attempted {
			next.attempted[id] = struct{}{}
		}
		for domain := range state.domains {
			next.domains[domain] = struct{}{}
		}
	}
	if failure.GatewayPoolRotation {
		failure.NextAccountAction = NextAccountRetry // the existing bounded failover loop owns switching
		// Rest only the credential's ticket acquisition, not account health or
		// global schedulability. Never shorten an existing longer pool backoff.
		if remaining, _ := s.codexCookies.gatewayPoolBackoffFor(identity); remaining < openAIGatewayPoolDefaultBackoff {
			s.codexCookies.poolBackoff.Store(identity, gatewayPoolBackoffEntry{
				Until: time.Now().Add(openAIGatewayPoolDefaultBackoff), Code: gwpool.CodeAllCooling,
			})
		}
	}
	return context.WithValue(ctx, gatewayPoolRotationKey{}, next)
}

func (s *OpenAIGatewayService) gatewayPoolNoRemainingRoutes(ctx context.Context, account *Account) bool {
	return s.gatewayPoolExhaustedCatalog(ctx, account) != nil
}

type gatewayPoolExhaustion struct {
	identity   string
	generation uint64
	gateways   []gwpool.Gateway
}

func (s *OpenAIGatewayService) gatewayPoolExhaustedCatalog(ctx context.Context, account *Account) *gatewayPoolExhaustion {
	if ctx.Err() != nil {
		return nil
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil || s.codexCookies.gatewayPoolVerifiedFull(identity) {
		return nil
	}
	if err := s.codexCookies.hydrateGatewayPoolSharedHistory(ctx, account, identity); err != nil {
		return nil
	}
	generation, active, available := s.codexCookies.gatewayPoolInventoryCandidates(identity)
	if active {
		return nil
	}
	pool, err := s.codexCookies.poolClient(account)
	if err != nil {
		return nil
	}
	listCtx, cancel := context.WithTimeout(ctx, account.gatewayPoolListTimeout())
	defer cancel()
	catalog, err := pool.FreshCatalog(listCtx, gatewayPoolUpstreamAccountID(identity), gatewayPoolAccountTag(account, identity), gatewayPoolProbeModelLuna)
	if err != nil {
		return nil // unreadable state is not a zero inventory
	}
	for _, gateway := range catalog.Gateways {
		if !gateway.ReadyAt(time.Now()) {
			continue
		}
		_, cooling := s.codexCookies.gatewayPoolUsedAt(identity, gateway.Name, account.gatewayPoolGatewayWindow())
		if !cooling {
			available[gateway.Name] = struct{}{}
		}
	}
	after, pending, _ := s.codexCookies.gatewayPoolInventoryCandidates(identity)
	if len(available) != 0 || pending || after != generation || ctx.Err() != nil {
		return nil
	}
	return &gatewayPoolExhaustion{identity: identity, generation: generation, gateways: catalog.Gateways}
}

// Read gateway-pool eligibility from the repository, not scheduler snapshots. An explicit
// allow-list also blocks simple-mode/fallback-group selection from widening scope.
func gatewayPoolRotationExclusions(ctx context.Context, repo AccountRepository, groupID *int64, excluded map[int64]struct{}) (map[int64]struct{}, map[int64]struct{}, error) {
	if retry := gatewayPoolRetryOnlyFrom(ctx); retry.accountID != 0 {
		if failure := GatewayPoolRetryFailure(ctx); failure != nil {
			return nil, nil, failure
		}
		if repo == nil || groupID == nil || *groupID != retry.groupID {
			return nil, nil, ErrNoAvailableAccounts
		}
		accounts, err := repo.ListByPlatform(ctx, PlatformOpenAI)
		if err != nil {
			return nil, nil, err
		}
		omit := cloneExcludedAccountIDs(excluded)
		if omit == nil {
			omit = map[int64]struct{}{}
		}
		allowed := map[int64]struct{}{}
		for i := range accounts {
			account := &accounts[i]
			if account.ID != retry.accountID {
				omit[account.ID] = struct{}{}
			} else if _, skipped := omit[account.ID]; !skipped &&
				gatewayPoolRotationAccount(account, retry.groupID) && account.IsSchedulable() {
				allowed[account.ID] = struct{}{}
			}
		}
		if len(allowed) == 0 {
			return nil, nil, ErrNoAvailableAccounts
		}
		return omit, allowed, nil
	}
	state := gatewayPoolRotationFrom(ctx)
	if state == nil {
		return excluded, nil, nil
	}
	if repo == nil || groupID == nil || *groupID != state.groupID {
		return nil, nil, ErrNoAvailableAccounts
	}
	// Include other OpenAI groups in the exclusion set too: simple mode can
	// otherwise bypass the scheduler's ordinary group filter.
	accounts, err := repo.ListByPlatform(ctx, PlatformOpenAI)
	if err != nil {
		return nil, nil, err
	}
	omit := cloneExcludedAccountIDs(excluded)
	if omit == nil {
		omit = map[int64]struct{}{}
	}
	for id := range state.attempted {
		omit[id] = struct{}{}
	}
	allowed := map[int64]struct{}{}
	for i := range accounts {
		account := &accounts[i]
		if !gatewayPoolRotationAccount(account, state.groupID) || !account.IsSchedulable() {
			omit[account.ID] = struct{}{}
			continue
		}
		if _, skip := omit[account.ID]; !skip {
			allowed[account.ID] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return nil, nil, ErrNoAvailableAccounts
	}
	return omit, allowed, nil
}

func gatewayPoolRotationRecheck(ctx context.Context, repo AccountRepository, allowed map[int64]struct{}, selection *AccountSelectionResult) bool {
	state := gatewayPoolRotationFrom(ctx)
	retry := gatewayPoolRetryOnlyFrom(ctx)
	if state == nil && retry.accountID == 0 {
		return true
	}
	if selection == nil || selection.Account == nil {
		return false
	}
	if _, ok := allowed[selection.Account.ID]; !ok {
		return false
	}
	fresh, err := repo.GetByID(ctx, selection.Account.ID)
	groupID := retry.groupID
	if retry.accountID == 0 {
		groupID = state.groupID
	}
	if err != nil || !gatewayPoolRotationAccount(fresh, groupID) || !fresh.IsSchedulable() {
		return false
	}
	selection.Account = fresh
	return true
}
