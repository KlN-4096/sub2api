package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const openAIGatewayPoolRotationExtraKey = "openai_gwpool_rotation"

// Distinguishes completed, conclusive ticket attempts from time-budget expiry.
// Reaching this limit alone still does NOT authorize account rotation.
var errGatewayPoolWarmAttemptsFinished = fmt.Errorf("gateway attempts finished: %w", errOpenAIGatewayPoolWarmExhausted)

type gatewayPoolRotationKey struct{}
type gatewayPoolRotation struct {
	groupID   int64
	attempted map[int64]struct{}
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
		account.getExtraBool(openAIGatewayPoolExtraKey) && account.getExtraBool(openAIGatewayPoolRotationExtraKey) &&
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
func (s *OpenAIGatewayService) PrepareGatewayPoolAccountRotation(ctx context.Context, groupID *int64, source *Account, failure *UpstreamFailoverError) context.Context {
	if failure == nil {
		return ctx
	}
	state := gatewayPoolRotationFrom(ctx)
	if state == nil && (source == nil || !source.IsOpenAIOAuthLike()) {
		return ctx
	}
	if s == nil || s.accountRepo == nil || groupID == nil || source == nil {
		if state != nil || (source != nil && source.getExtraBool(openAIGatewayPoolRotationExtraKey)) {
			failure.NextAccountAction = NextAccountStop
		}
		return ctx
	}
	fresh, err := s.accountRepo.GetByID(ctx, source.ID)
	if state == nil && err == nil && fresh != nil && !fresh.getExtraBool(openAIGatewayPoolRotationExtraKey) {
		return ctx
	}
	// For opted-in accounts ALL other failures stop here, including ordinary
	// HTTP/auth/transport failures that the general failover loop could replay.
	failure.RetryableOnSameAccount = false
	failure.NextAccountAction = NextAccountStop
	if err != nil || !gatewayPoolRotationAccount(fresh, *groupID) || !fresh.IsActive() ||
		(state != nil && state.groupID != *groupID) {
		return ctx
	}
	if !failure.GatewayPoolRotation || !s.gatewayPoolNoRemainingRoutes(ctx, fresh) {
		return ctx
	}
	next := &gatewayPoolRotation{groupID: *groupID, attempted: map[int64]struct{}{source.ID: {}}}
	if state != nil {
		for id := range state.attempted {
			next.attempted[id] = struct{}{}
		}
	}
	if failure.GatewayPoolRotation {
		failure.NextAccountAction = NextAccountRetry // the existing bounded failover loop owns switching
		// Rest only the credential's ticket acquisition, not account health or
		// global schedulability. Never shorten an existing longer pool backoff.
		if identity, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh); err == nil {
			if remaining, _ := s.codexCookies.gatewayPoolBackoffFor(identity); remaining < openAIGatewayPoolDefaultBackoff {
				s.codexCookies.poolBackoff.Store(gatewayPoolLedgerIdentity(identity), gatewayPoolBackoffEntry{
					Until: time.Now().Add(openAIGatewayPoolDefaultBackoff), Code: gwpool.CodeAllCooling,
				})
			}
		}
	}
	return context.WithValue(ctx, gatewayPoolRotationKey{}, next)
}

func (s *OpenAIGatewayService) gatewayPoolNoRemainingRoutes(ctx context.Context, account *Account) bool {
	if ctx.Err() != nil {
		return false
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil || s.codexCookies.gatewayPoolVerifiedFull(identity) {
		return false
	}
	if err := s.codexCookies.hydrateGatewayPoolSharedHistory(ctx, account, identity); err != nil {
		return false
	}
	generation, pending := s.codexCookies.gatewayPoolInventorySnapshot(identity, account)
	if pending {
		return false
	}
	pool, err := s.codexCookies.poolClient(account)
	if err != nil {
		return false
	}
	listCtx, cancel := context.WithTimeout(ctx, account.gatewayPoolListTimeout())
	defer cancel()
	gateways, err := pool.Gateways(listCtx, gatewayPoolUpstreamAccountID(identity), gatewayPoolAccountTag(account, identity))
	if err != nil || len(gateways) == 0 {
		return false // supply outage / unreadable state is not account-specific exhaustion
	}
	ready := 0
	for _, gateway := range gateways {
		if !gateway.PairReady {
			continue
		}
		ready++
		_, cooling := s.codexCookies.gatewayPoolUsedAt(identity, gateway.Name, account.gatewayPoolGatewayWindow())
		if !cooling && !gateway.UsedByYou {
			return false
		}
	}
	after, pending := s.codexCookies.gatewayPoolInventorySnapshot(identity, account)
	return ready > 0 && !pending && after == generation
}

// Read opt-ins from the repository, not scheduler snapshots. An explicit
// allow-list also blocks simple-mode/fallback-group selection from widening scope.
func gatewayPoolRotationExclusions(ctx context.Context, repo AccountRepository, groupID *int64, excluded map[int64]struct{}) (map[int64]struct{}, map[int64]struct{}, error) {
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
	if state == nil {
		return true
	}
	if selection == nil || selection.Account == nil {
		return false
	}
	if _, ok := allowed[selection.Account.ID]; !ok {
		return false
	}
	fresh, err := repo.GetByID(ctx, selection.Account.ID)
	if err != nil || !gatewayPoolRotationAccount(fresh, state.groupID) || !fresh.IsSchedulable() {
		return false
	}
	selection.Account = fresh
	return true
}
