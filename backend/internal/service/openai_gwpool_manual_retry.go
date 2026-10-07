package service

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

type gatewayPoolObservationEpochKey struct{}

var errGatewayPoolGenerationChanged = errors.New("gateway preparation generation changed")

type GatewayPoolRetryResult struct {
	State string `json:"state"`
}

func (s *openAICodexCookieStore) publishGatewayPoolFetchedPair(identity string, pair openAIGatewayPoolPair, epoch time.Time) bool {
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	if !epoch.Equal(s.gatewayPoolCooldownResetAt(identity)) {
		return false
	}
	s.poolPairs.Store(identity, pair)
	return true
}

func (s *openAICodexCookieStore) rememberGatewayPoolBackoff(identity string, epoch time.Time, duration time.Duration, code string) {
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	if code == gwpool.CodeAllCooling && !epoch.Equal(s.gatewayPoolCooldownResetAt(identity)) {
		return
	}
	s.poolBackoff.Store(gatewayPoolLedgerIdentity(identity), gatewayPoolBackoffEntry{Until: time.Now().Add(duration), Code: code})
}

func (c *gatewayPoolCooldown) clearCooldown(at time.Time, base int) bool {
	if at.IsZero() || !at.After(c.ResetAt) {
		return false
	}
	*c = gatewayPoolCooldown{
		ResetAt: at, Cleared: true, SourcesKnown: true, BaseSeconds: base,
		WindowSeconds: base, CycleAt: at, UpdatedAt: at, ScheduleUpdatedAt: at, Until: at,
	}
	return true
}

func (s *openAICodexCookieStore) gatewayPoolCooldownClearAt(identity string) time.Time {
	value, _ := s.poolCooldownClear.Load(gatewayPoolLedgerIdentity(identity))
	at, _ := value.(time.Time)
	return at
}

func (s *openAICodexCookieStore) applyGatewayPoolCooldownClear(identity string, at time.Time, base int) {
	if at.IsZero() || at.After(time.Now().Add(gatewayPoolCooldownGrace)) {
		return
	}
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	if !at.After(s.gatewayPoolCooldownClearAt(identity)) {
		return
	}
	ledger := gatewayPoolLedgerIdentity(identity)
	s.poolCooldownClear.Store(ledger, at)
	if at.After(s.gatewayPoolCooldownResetAt(identity)) {
		s.poolCooldownReset.Store(ledger, at)
	}
	for key, cooldown := range s.poolCooldown {
		if strings.HasPrefix(key, ledger+"\x00") {
			cooldown.clearCooldown(at, base)
			s.poolCooldown[key] = cooldown
		}
	}
	s.poolUsed.Range(func(key, value any) bool {
		name, _ := key.(string)
		touched, _ := value.(time.Time)
		if strings.HasPrefix(name, ledger+"\x00") && !touched.After(at) {
			s.poolUsed.CompareAndDelete(key, value)
		}
		return true
	})
	if value, ok := s.poolBackoff.Load(ledger); ok {
		if entry, valid := value.(gatewayPoolBackoffEntry); valid && entry.Code == gwpool.CodeAllCooling {
			s.poolBackoff.CompareAndDelete(ledger, value)
		}
	}
}

// Commit one durable owner barrier before publishing it in memory. Every
// hydration/ordinary history writer adopts that barrier, including old clones.
func (s *OpenAIGatewayService) clearGatewayPoolCooldown(ctx context.Context, account *Account, identity string) error {
	value, _ := s.codexCookies.poolCooldownResetLocks.LoadOrStore(gatewayPoolLedgerIdentity(identity), &sync.Mutex{})
	lock, ok := value.(*sync.Mutex)
	if !ok || !gatewayPoolLockWithin(ctx, lock) {
		return errors.New("gateway cooldown clear lock unavailable")
	}
	defer lock.Unlock()
	peers, err := s.gatewayPoolHistoryPeers(ctx, gatewayPoolLedgerTag(identity))
	if err != nil {
		return err
	}
	if err := s.codexCookies.hydrateGatewayPoolSharedHistory(ctx, account, identity); err != nil {
		return err
	}
	at := time.Now().UTC()
	if latest := s.codexCookies.gatewayPoolCooldownResetAt(identity); !at.After(latest) {
		at = latest.Add(time.Nanosecond)
	}
	if err := s.writeGatewayPoolCooldownClear(ctx, account.ID, identity, at, true); err != nil {
		return err
	}
	for i := range peers {
		if peers[i].ID != account.ID {
			if err := s.writeGatewayPoolCooldownClear(ctx, peers[i].ID, identity, at, false); err != nil {
				return err
			}
		}
	}
	s.codexCookies.poolCandidateQueues.Range(func(key, value any) bool {
		name, _ := key.(string)
		if gatewayPoolLedgerIdentity(name) == gatewayPoolLedgerIdentity(identity) {
			queue, ok := value.(*gatewayPoolCandidateQueue)
			if !ok || queue == nil {
				panic("invalid gateway pool candidate queue")
			}
			queue.reset()
		}
		return true
	})
	return nil
}

func (s *OpenAIGatewayService) writeGatewayPoolCooldownClear(ctx context.Context, id int64, identity string, at time.Time, owner bool) error {
	value, _ := s.codexCookies.poolHistoryLocks.LoadOrStore(id, &sync.Mutex{})
	lock, ok := value.(*sync.Mutex)
	if !ok || !gatewayPoolLockWithin(ctx, lock) {
		return errors.New("gateway cooldown clear history lock unavailable")
	}
	defer lock.Unlock()
	fresh, err := s.accountRepo.GetByID(ctx, id)
	if err != nil || fresh == nil {
		return errors.New("gateway cooldown clear account unavailable")
	}
	tag := gatewayPoolLedgerTag(identity)
	rec, _ := readOpenAIGatewayHistory(fresh)
	if owner {
		current, err := s.codexCookies.gatewayPoolIdentity(ctx, fresh)
		if err != nil || current != identity || !fresh.UsesGatewayPool() {
			return errors.New("gateway cooldown clear identity changed")
		}
		rec = gatewayPoolHistoryForTag(rec, tag)
		rec.LedgerTag = tag
	}
	view := gatewayPoolCooldownResetHistory(&rec, tag)
	if view == nil {
		return nil
	}
	view.CooldownReset.LastAt, view.CooldownReset.ClearedAt = at, at
	base := gatewayPoolCooldownBase(fresh.gatewayPoolGatewayWindow())
	for name, seen := range view.Seen {
		next := gatewayPoolCooldown{}
		next.clearCooldown(at, base)
		seen.Cooldown = &next
		view.Seen[name] = seen
	}
	previousTag := ""
	if rec.Previous != nil {
		previousTag = rec.Previous.LedgerTag
	}
	if err := s.accountRepo.UpdateExtra(ctx, id, map[string]any{
		openAIGatewayHistoryExtraKey: rec, openAIGatewayLedgerTagExtraKey: rec.LedgerTag,
		openAIGatewayPreviousLedgerTagExtraKey: previousTag,
	}); err != nil {
		return err
	}
	s.codexCookies.applyGatewayPoolCooldownClear(identity, at, base)
	return nil
}

type gatewayPoolClearRestRepository interface {
	ClearGatewayPoolRest(context.Context, int64, map[string]any) error
}

func (s *OpenAIGatewayService) clearGatewayPoolManualRest(ctx context.Context, account *Account, identity string) error {
	state, fresh, err := s.loadGatewayPoolRest(ctx, account, identity)
	if err != nil {
		return err
	}
	// An inactive owner does not prove every clone's block was cleared: a
	// previous attempt may have committed one row before another write failed.
	state.Active, state.NextCheck = false, time.Time{}
	state.advance(time.Now().UTC())
	repo, ok := s.accountRepo.(gatewayPoolClearRestRepository)
	if !ok {
		return errors.New("gateway cooldown clear requires atomic rest support")
	}
	peers, err := s.gatewayPoolHistoryPeers(ctx, state.Tag)
	if err != nil {
		return err
	}
	ids := []int64{fresh.ID}
	seen := map[int64]bool{fresh.ID: true}
	for i := range peers {
		if !seen[peers[i].ID] {
			ids = append(ids, peers[i].ID)
			seen[peers[i].ID] = true
		}
	}
	for _, id := range ids {
		peer, err := s.accountRepo.GetByID(ctx, id)
		if err != nil || peer == nil {
			return errors.New("gateway manual recovery peer unavailable")
		}
		current, err := s.codexCookies.gatewayPoolIdentity(ctx, peer)
		if err != nil {
			return err
		}
		if gatewayPoolLedgerTag(current) != state.Tag || !peer.UsesGatewayPool() {
			continue
		}
		copy := state
		copy.Previous = readGatewayPoolRest(peer, state.Tag).Previous
		previous := ""
		if copy.Previous != nil {
			previous = copy.Previous.Tag
		}
		if err := repo.ClearGatewayPoolRest(ctx, peer.ID, map[string]any{
			gatewayPoolRestStateKey: copy, gatewayPoolRestTagKey: copy.Tag, gatewayPoolRestPreviousTagKey: previous,
		}); err != nil {
			return err
		}
	}
	s.codexCookies.poolRestState.Store(state.Tag, state)
	s.codexCookies.poolRounds.recovered(identity)
	return nil
}

// An explicit administrator action creates one bounded waiter, not a permanent
// background prewarmer. Existing business waiters continue sharing the worker.
func (s *OpenAIGatewayService) RetryGatewayPool(ctx context.Context, id int64) (GatewayPoolRetryResult, error) {
	account, err := s.accountRepo.GetByID(ctx, id)
	if err != nil {
		return GatewayPoolRetryResult{}, err
	}
	if account == nil || !account.UsesGatewayPool() {
		return GatewayPoolRetryResult{}, errors.New("gateway retry requires a pool account")
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return GatewayPoolRetryResult{}, err
	}
	key := gatewayPoolLedgerIdentity(identity)
	if _, running := s.codexCookies.poolManualRetry.LoadOrStore(key, struct{}{}); running {
		return GatewayPoolRetryResult{State: "preparing"}, nil
	}
	started := false
	defer func() {
		if !started {
			s.codexCookies.poolManualRetry.Delete(key)
		}
	}()
	unlock, err := s.lockGatewayPoolRest(ctx, gatewayPoolLedgerTag(identity))
	if err != nil {
		return GatewayPoolRetryResult{}, err
	}
	err = s.clearGatewayPoolCooldown(ctx, account, identity)
	if err == nil {
		err = s.clearGatewayPoolManualRest(ctx, account, identity)
	}
	unlock()
	if err != nil {
		return GatewayPoolRetryResult{}, err
	}
	if s.codexCookies.gatewayPoolVerifiedFull(identity) {
		return GatewayPoolRetryResult{State: "retained"}, nil
	}
	account, err = s.accountRepo.GetByID(ctx, id)
	if err != nil {
		return GatewayPoolRetryResult{}, err
	}
	if account == nil || !account.IsSchedulable() {
		return GatewayPoolRetryResult{State: "blocked"}, nil
	}
	proxyURL := ""
	if account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	if err := requireOpenAIProxyBinding(account, proxyURL); err != nil {
		return GatewayPoolRetryResult{}, err
	}
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), account.gatewayPoolMaxWait())
	request, err := http.NewRequestWithContext(work, http.MethodPost, chatgptCodexAPIURL, nil)
	if err != nil {
		cancel()
		return GatewayPoolRetryResult{}, err
	}
	started = true
	go func() {
		defer s.codexCookies.poolManualRetry.Delete(key)
		defer cancel()
		err := s.gatewayPoolWarmUpWith(request, account, identity, gatewayPoolProbeModelLuna,
			func(ctx context.Context, cookie, state string) (int, string, error) {
				return s.gatewayPoolWarmShot(ctx, account, proxyURL, cookie, gatewayPoolProbeModelLuna, state)
			})
		if err != nil {
			slog.Warn("gwpool_manual_retry_finished", "account_id", id, "ready", false)
		}
	}()
	return GatewayPoolRetryResult{State: "preparing"}, nil
}

func (s *adminServiceImpl) RetryGatewayPool(ctx context.Context, id int64) (GatewayPoolRetryResult, error) {
	if gateway, ok := s.runtimeBlocker.(interface {
		RetryGatewayPool(context.Context, int64) (GatewayPoolRetryResult, error)
	}); ok {
		return gateway.RetryGatewayPool(ctx, id)
	}
	return GatewayPoolRetryResult{}, errors.New("gateway retry unavailable")
}
