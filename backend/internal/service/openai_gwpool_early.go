package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	openAIGatewayPoolEarlyEnabledKey = "openai_gwpool_early_probe_enabled"
	openAIGatewayPoolEarlyStateKey   = "openai_gwpool_early_probe_state"
	gatewayPoolEarlyInterval         = 30 * time.Minute
)

// Only a waiting business request can create this intent. Ordinary AttachRoute
// calls (including guard-off traffic) cannot opt into bypassing local quality CD.
type gatewayPoolEarlyIntentKey struct{}
type gatewayPoolEarlyIntent struct {
	ctx     context.Context
	model   string
	attempt *gatewayPoolEarlyAttempt // assigned only inside the identity's poolFetch
}

type gatewayPoolEarlyState struct {
	LedgerTag string    `json:"ledger_tag"`
	At        time.Time `json:"at"`
}

type gatewayPoolEarlyVerdict struct {
	full, conclusive, sent bool
	firstSent              time.Time
	err                    error
}

// Kept on the comparable pair as a pointer. Cache the completed result too:
// singleflight alone would let a slightly later caller run a second A/B.
type gatewayPoolEarlyAttempt struct {
	gateway, model string
	once           sync.Once
	done           chan struct{}
	result         gatewayPoolEarlyVerdict
}

func newGatewayPoolEarlyAttempt(gateway, model string) *gatewayPoolEarlyAttempt {
	return &gatewayPoolEarlyAttempt{gateway: gateway, model: model, done: make(chan struct{})}
}

func (a *Account) gatewayPoolEarlyEnabled() bool {
	return a != nil && a.getExtraBool(openAIGatewayPoolEarlyEnabledKey) &&
		a.getExtraBool(openAIGatewayPoolExtraKey) && a.gatewayPoolGuardEnabled()
}

func readGatewayPoolEarlyState(account *Account, identity string) time.Time {
	if account == nil {
		return time.Time{}
	}
	raw, err := json.Marshal(account.Extra[openAIGatewayPoolEarlyStateKey])
	var state gatewayPoolEarlyState
	if err != nil || json.Unmarshal(raw, &state) != nil || state.LedgerTag != gatewayPoolLedgerTag(identity) {
		return time.Time{}
	}
	return state.At
}

func (s *openAICodexCookieStore) gatewayPoolEarlyDue(ctx context.Context, account *Account, identity string) bool {
	if ctx.Err() != nil || !account.gatewayPoolEarlyEnabled() {
		return false
	}
	fresh, err := s.freshGatewayPoolAccount(ctx, account)
	if err != nil || !fresh.gatewayPoolEarlyEnabled() || !fresh.IsSchedulable() {
		return false
	}
	last := readGatewayPoolEarlyState(fresh, identity)
	if s.historyByTag != nil {
		peers, err := s.historyByTag(ctx, gatewayPoolLedgerTag(identity))
		if err != nil {
			return false
		}
		for i := range peers {
			if at := readGatewayPoolEarlyState(&peers[i], identity); at.After(last) {
				last = at
			}
		}
	}
	if value, ok := s.poolEarlyAt.Load(gatewayPoolLedgerIdentity(identity)); ok {
		if at, ok := value.(time.Time); ok && at.After(last) {
			last = at
		}
	}
	return !time.Now().Before(last.Add(gatewayPoolEarlyInterval))
}

// Reserve durably BEFORE
// fetching or probing; failed/cancelled reservations are deliberately not refunded.
// Cross-instance coordination is outside the existing single-instance contract.
func (s *OpenAIGatewayService) claimGatewayPoolEarly(ctx context.Context, account *Account, identity string, at time.Time) error {
	if s.accountRepo == nil || ctx.Err() != nil {
		return errors.New("early probe budget unavailable")
	}
	lock, _ := s.codexCookies.poolEarlyLocks.LoadOrStore(gatewayPoolLedgerIdentity(identity), &sync.Mutex{})
	mu, ok := lock.(*sync.Mutex)
	if !ok || mu == nil {
		return errors.New("early probe budget lock unavailable")
	}
	mu.Lock()
	defer mu.Unlock()
	unlock := s.codexCookies.gatewayPoolHistoryLock(account.ID)
	defer unlock()
	fresh, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || !fresh.gatewayPoolEarlyEnabled() || !fresh.IsSchedulable() {
		return errors.New("early probe account unavailable")
	}
	currentIdentity, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
	if err != nil || currentIdentity != identity || !s.codexCookies.gatewayPoolEarlyDue(ctx, fresh, identity) {
		return errors.New("early probe budget unavailable")
	}
	if err := s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{
		openAIGatewayPoolEarlyStateKey: gatewayPoolEarlyState{LedgerTag: gatewayPoolLedgerTag(identity), At: at},
		openAIGatewayLedgerTagExtraKey: gatewayPoolLedgerTag(identity),
	}); err != nil {
		return err
	}
	s.codexCookies.poolEarlyAt.Store(gatewayPoolLedgerIdentity(identity), at)
	return nil
}

// No empty-list/UsedByYou/threshold shortcut: there must be zero normal routes
// and at least one pair blocked only by THIS consumer's quality cooldown.
func (s *openAICodexCookieStore) gatewayPoolEarlyCandidates(account *Account, identity string, gateways []gwpool.Gateway) []gwpool.Gateway {
	var cooling []gwpool.Gateway
	for _, gateway := range gateways {
		if !gateway.PairReady || gateway.UsedByYou {
			continue
		}
		if _, used := s.gatewayPoolUsedAt(identity, gateway.Name, account.gatewayPoolGatewayWindow(), account.gatewayPoolUseRecommendation()); !used {
			return nil
		}
		cooling = append(cooling, gateway)
	}
	sort.SliceStable(cooling, func(i, j int) bool {
		a, _ := s.gatewayPoolUsedAt(identity, cooling[i].Name, account.gatewayPoolGatewayWindow(), account.gatewayPoolUseRecommendation())
		b, _ := s.gatewayPoolUsedAt(identity, cooling[j].Name, account.gatewayPoolGatewayWindow(), account.gatewayPoolUseRecommendation())
		return a.Before(b)
	})
	return cooling
}

func (s *openAICodexCookieStore) gatewayPoolPickEarly(ctx context.Context, account *Account, identity string, gateways []gwpool.Gateway) string {
	intent, _ := ctx.Value(gatewayPoolEarlyIntentKey{}).(*gatewayPoolEarlyIntent)
	if intent == nil || intent.attempt != nil || intent.model == "" || intent.ctx.Err() != nil ||
		s.poolEarlyClaim == nil || !s.gatewayPoolEarlyDue(intent.ctx, account, identity) {
		return ""
	}
	candidates := s.gatewayPoolEarlyCandidates(account, identity, gateways)
	if len(candidates) == 0 {
		return ""
	}
	if err := s.poolEarlyClaim(intent.ctx, account, identity, time.Now().UTC()); err != nil {
		slog.Warn("gwpool_early_budget_unavailable", "account_id", account.ID)
		return ""
	}
	intent.attempt = newGatewayPoolEarlyAttempt(candidates[0].Name, intent.model)
	slog.Info("gwpool_early_reserved", "account_id", account.ID, "gateway", candidates[0].Name, "model", intent.model)
	return candidates[0].Name
}

func (s *openAICodexCookieStore) gatewayPoolEarlyModelMatches(identity, model string) bool {
	pair, state := s.cachedPoolPair(identity)
	return state != openAIGatewayPoolPairLive || pair.early == nil || pair.early.model == model ||
		s.gatewayPoolVerifiedFull(identity)
}
