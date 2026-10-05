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
	gatewayPoolEarlyLogInterval      = time.Minute
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
	reservedAt     time.Time
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
	due, _, _ := s.gatewayPoolEarlyBudget(ctx, account, identity)
	return due
}

func (s *openAICodexCookieStore) gatewayPoolEarlyBudget(ctx context.Context, account *Account, identity string) (bool, string, time.Time) {
	if ctx.Err() != nil {
		return false, "cancelled", time.Time{}
	}
	if !account.gatewayPoolEarlyEnabled() {
		return false, "disabled", time.Time{}
	}
	fresh, err := s.freshGatewayPoolAccount(ctx, account)
	if err != nil || fresh == nil {
		return false, "account_unavailable", time.Time{}
	}
	if !fresh.gatewayPoolEarlyEnabled() {
		return false, "disabled", time.Time{}
	}
	if !fresh.IsSchedulable() {
		return false, "account_blocked", time.Time{}
	}
	last := readGatewayPoolEarlyState(fresh, identity)
	if s.historyByTag != nil {
		peers, err := s.historyByTag(ctx, gatewayPoolLedgerTag(identity))
		if err != nil {
			return false, "shared_budget_unavailable", time.Time{}
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
	next := last.Add(gatewayPoolEarlyInterval)
	if time.Now().Before(next) {
		return false, "budget_not_due", next
	}
	return true, "", next
}

type gatewayPoolEarlySkip struct {
	reason string
	at     time.Time
}

func (s *openAICodexCookieStore) gatewayPoolEarlySkipDue(id int64, reason string, now time.Time) bool {
	next := gatewayPoolEarlySkip{reason: reason, at: now}
	for {
		value, loaded := s.poolEarlySkips.LoadOrStore(id, next)
		if !loaded {
			return true
		}
		previous := value.(gatewayPoolEarlySkip)
		if previous.reason == reason && now.Sub(previous.at) < gatewayPoolEarlyLogInterval {
			return false
		}
		if s.poolEarlySkips.CompareAndSwap(id, previous, next) {
			return true
		}
	}
}

func (s *openAICodexCookieStore) noteGatewayPoolEarlySkip(account *Account, reason string, next time.Time) {
	if account == nil || !s.gatewayPoolEarlySkipDue(account.ID, reason, time.Now()) {
		return
	}
	attrs := []any{"account_id", account.ID, "reason", reason, "interval_s", int(gatewayPoolEarlyInterval.Seconds())}
	if !next.IsZero() {
		attrs = append(attrs, "next_at", next.UTC(), "remaining_s", max(0, int(time.Until(next).Seconds())))
	}
	slog.Info("gwpool_early_skipped", attrs...)
}

// Reserve durably BEFORE
// fetching or probing; failed/cancelled reservations are deliberately not refunded.
// Cross-instance coordination is outside the existing single-instance contract.
func (s *OpenAIGatewayService) claimGatewayPoolEarly(ctx context.Context, account *Account, identity string, at time.Time) error {
	if s.accountRepo == nil || ctx.Err() != nil {
		return errors.New("early probe budget unavailable")
	}
	if allowed, err := s.gatewayPoolResumeAllowed(ctx, account, false); err != nil || !allowed {
		return errors.New("early probe account resting")
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

// No empty-list/threshold shortcut: there must be zero normal routes
// and at least one pair blocked only by THIS consumer's quality cooldown.
// UsedByYou is historical delivery information, not the /cookie admission rule.
func (s *openAICodexCookieStore) gatewayPoolEarlyCandidates(account *Account, identity string, gateways []gwpool.Gateway) []gwpool.Gateway {
	var cooling []gwpool.Gateway
	for _, gateway := range gateways {
		if !gateway.PairReady {
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
	if intent == nil || intent.model == "" {
		reason := "no_foreground_intent"
		if !account.gatewayPoolEarlyEnabled() {
			reason = "disabled"
		}
		s.noteGatewayPoolEarlySkip(account, reason, time.Time{})
		return ""
	}
	if intent.attempt != nil {
		s.noteGatewayPoolEarlySkip(account, "already_reserved", intent.attempt.reservedAt.Add(gatewayPoolEarlyInterval))
		return ""
	}
	if s.poolEarlyClaim == nil {
		s.noteGatewayPoolEarlySkip(account, "persistence_unavailable", time.Time{})
		return ""
	}
	if due, reason, next := s.gatewayPoolEarlyBudget(intent.ctx, account, identity); !due {
		s.noteGatewayPoolEarlySkip(account, reason, next)
		return ""
	}
	candidates := s.gatewayPoolEarlyCandidates(account, identity, gateways)
	if len(candidates) == 0 {
		s.noteGatewayPoolEarlySkip(account, "no_exclusively_local_cooling_candidates", time.Time{})
		return ""
	}
	reserved := time.Now().UTC()
	if err := s.poolEarlyClaim(intent.ctx, account, identity, reserved); err != nil {
		slog.Warn("gwpool_early_budget_unavailable", "account_id", account.ID)
		return ""
	}
	intent.attempt = newGatewayPoolEarlyAttempt(candidates[0].Name, intent.model)
	intent.attempt.reservedAt = reserved
	contact, _ := s.poolUsed.Load(gatewayPoolLedgerKey(identity, candidates[0].Name))
	priorContact, _ := contact.(time.Time)
	cooldown, _ := s.cooldownEntry(identity, candidates[0].Name)
	until := cooldown.Until
	if until.IsZero() {
		until = priorContact.Add(account.gatewayPoolGatewayWindow())
	}
	window := cooldown.WindowSeconds
	if window == 0 {
		window = int(account.gatewayPoolGatewayWindow().Seconds())
	}
	slog.Info("gwpool_early_reserved", "account_id", account.ID, "gateway", candidates[0].Name, "model", intent.model,
		"reserved_at", reserved, "next_at", reserved.Add(gatewayPoolEarlyInterval),
		"prior_contact_at", priorContact, "prior_cd_s", window, "prior_until", until,
		"remaining_cd_s", max(0, int(until.Sub(reserved).Seconds())), "sent", false)
	return candidates[0].Name
}

func (s *openAICodexCookieStore) gatewayPoolEarlyModelMatches(identity, model string) bool {
	pair, state := s.cachedPoolPair(identity)
	return state != openAIGatewayPoolPairLive || pair.early == nil || pair.early.model == model ||
		s.gatewayPoolVerifiedFull(identity)
}
