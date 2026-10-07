package service

import (
	"context"
	"sort"
	"strings"
	"time"
)

type gatewayPoolPreferenceKey struct{}
type gatewayPoolCandidateDomainsKey struct{}
type gatewayPoolStickyPreferenceKey struct{}
type gatewayPoolExistingBindingKey struct{}

const gatewayPoolCooledUnknown = -1

type gatewayPoolAccountPreference struct {
	verified   bool
	cooled     int
	restFirst  bool
	lastTouch  time.Time
	loadKnown  bool
	loadRate   int
	waiting    int
	throttled  bool
	full       bool
	activeRank int
}

type gatewayPoolAccountPreferences map[int64]gatewayPoolAccountPreference

func (s *OpenAIGatewayService) withGatewayPoolAccountPreferences(ctx context.Context, req OpenAIAccountScheduleRequest) context.Context {
	if s == nil || s.accountRepo == nil || req.GroupID == nil || NormalizeOpenAICompatiblePlatform(req.Platform) != PlatformOpenAI {
		return ctx
	}
	// The candidate list may be a scheduler snapshot, but every preference is
	// derived from a fresh repository row. Missing/failed history is unknown.
	accounts, err := s.listSchedulableAccounts(ctx, req.GroupID, req.Platform)
	if err != nil {
		return ctx
	}
	prefs := gatewayPoolAccountPreferences{}
	domains := map[int64]string{}
	identities := map[int64]string{}
	freshAccounts := map[int64]*Account{}
	poolIDs := map[int64]bool{}
	complete := true
	checker := &defaultOpenAIAccountScheduler{service: s}
	for i := range accounts {
		if !accounts[i].IsOpenAIOAuthLike() {
			continue
		}
		poolIDs[accounts[i].ID] = accounts[i].UsesGatewayPool()
		account, err := s.accountRepo.GetByID(ctx, accounts[i].ID)
		if err != nil {
			complete = false
			continue
		}
		s.codexCookies.poolRotationAccounts.Store(accounts[i].ID, gatewayPoolRotationAccount(account, *req.GroupID))
		if !gatewayPoolRotationAccount(account, *req.GroupID) || !account.IsSchedulable() {
			continue
		}
		poolIDs[account.ID] = true
		identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
		if err != nil {
			complete = false
			continue
		}
		// Membership is group-wide. A model-specific request cannot evict a
		// healthy active account just because it cannot serve this model.
		domains[account.ID] = gatewayPoolLedgerIdentity(identity)
		compatible, _ := checker.isAccountRequestCompatibleReason(ctx, account, req)
		if !compatible || !checker.isAccountTransportCompatible(account, req.RequiredTransport) ||
			(req.RequireCompact && openAICompactSupportTier(account) == 0) {
			continue
		}
		freshAccounts[account.ID] = account
		if allowed, err := s.gatewayPoolResumeAllowed(ctx, account, true); err != nil || !allowed {
			s.codexCookies.poolRounds.rest(*req.GroupID, identity, time.Now().Add(gatewayPoolRestMin))
			continue
		}
		identities[account.ID] = identity
		contacts := readGatewayPoolContacts(account, gatewayPoolLedgerTag(identity))
		for _, seen := range contacts.Seen {
			s.codexCookies.poolRounds.touch(identity, seen.LastAt)
		}
		if account.LastUsedAt != nil {
			s.codexCookies.poolRounds.touch(identity, *account.LastUsedAt)
		}
		if s.codexCookies.hydrateGatewayPoolSharedHistory(ctx, account, identity) != nil {
			continue
		}
		// Merge all matching credential-domain rows. Counting distinct names in
		// the hydrated ledger avoids treating clones as extra gateway capacity.
		prefix := gatewayPoolLedgerIdentity(identity) + "\x00"
		pref := gatewayPoolAccountPreference{verified: s.codexCookies.gatewayPoolVerifiedFull(identity)}
		known := pref.verified
		s.codexCookies.poolUsed.Range(func(key, value any) bool {
			name, validKey := key.(string)
			at, validTime := value.(time.Time)
			if !validKey || !validTime || at.IsZero() {
				return true
			}
			gateway, matches := strings.CutPrefix(name, prefix)
			if !matches || gateway == "" {
				return true
			}
			known = true
			if _, cooling := s.codexCookies.gatewayPoolUsedAt(identity, gateway, account.gatewayPoolGatewayWindow(), account.gatewayPoolUseRecommendation()); !cooling {
				pref.cooled++
			}
			return true
		})
		if !known {
			pref.cooled = gatewayPoolCooledUnknown
		}
		pref.throttled = time.Now().Before(s.openAIOAuth429RetryDeadline(account))
		prefs[account.ID] = pref
	}
	// Live row-level load is deliberately not a new credential-domain hard
	// limit. Clone rows are deduplicated only among new-session candidates.
	if s.concurrencyService != nil {
		var requests []AccountWithConcurrency
		for id, account := range freshAccounts {
			if _, admitted := prefs[id]; admitted {
				requests = append(requests, AccountWithConcurrency{ID: id, MaxConcurrency: account.EffectiveLoadFactor()})
			}
		}
		loadCtx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
		loads, err := s.concurrencyService.GetAccountsLoadBatchFresh(loadCtx, requests)
		cancel()
		if err == nil {
			for id, load := range loads {
				if load == nil {
					continue
				}
				pref, exists := prefs[id]
				if !exists {
					continue
				}
				pref.loadKnown, pref.loadRate, pref.waiting = true, load.LoadRate, load.WaitingCount
				pref.full = freshAccounts[id].Concurrency > 0 && load.CurrentConcurrency >= freshAccounts[id].Concurrency
				prefs[id] = pref
			}
		}
	}
	// Exhaustion is group-wide, just like admission. A model/capability subset
	// must not reset healthy primaries that serve other requests.
	shared, restFirst := s.codexCookies.poolRounds.snapshot(*req.GroupID, domains, complete)
	if state := gatewayPoolRotationFrom(ctx); state != nil {
		attempted := map[string]struct{}{}
		for domain := range state.domains {
			attempted[domain] = struct{}{}
		}
		for id := range state.attempted {
			if domain := domains[id]; domain != "" {
				attempted[domain] = struct{}{}
			}
		}
		for id, domain := range domains {
			if _, used := attempted[domain]; used {
				shared[id] = struct{}{}
			}
		}
	}
	if restFirst {
		for id, pref := range prefs {
			pref.restFirst, pref.lastTouch = true, s.codexCookies.poolRounds.lastTouch(domains[id])
			prefs[id] = pref
		}
	}
	ordered := make([]int64, 0, len(prefs))
	for id := range prefs {
		_, requestExcluded := req.ExcludedIDs[id]
		if _, exhausted := shared[id]; !exhausted && !requestExcluded {
			ordered = append(ordered, id)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := prefs[ordered[i]], prefs[ordered[j]]
		if gatewayPoolPreferenceBetter(a, b) {
			return true
		}
		if gatewayPoolPreferenceBetter(b, a) {
			return false
		}
		return ordered[i] < ordered[j]
	})
	candidates := make([]string, 0, len(ordered))
	stickyID, _ := ctx.Value(gatewayPoolExistingBindingKey{}).(int64)
	if stickyID == 0 && req.SessionHash != "" && s.cache != nil {
		stickyID, _ = s.getStickySessionAccountID(ctx, req.GroupID, req.SessionHash)
	}
	if stickyID != 0 {
		if _, eligible := prefs[stickyID]; eligible {
			if _, exhausted := shared[stickyID]; !exhausted {
				candidates = append(candidates, domains[stickyID])
			}
		}
	}
	for _, id := range ordered {
		candidates = append(candidates, domains[id])
	}
	ranks := s.codexCookies.poolRounds.admit(*req.GroupID, candidates, domains, complete, s.gatewayPoolActiveAccountLimit(ctx))
	for id, poolAccount := range poolIDs {
		if poolAccount && ranks[domains[id]] == 0 && id != stickyID {
			shared[id] = struct{}{}
		}
	}
	for id, pref := range prefs {
		pref.activeRank = ranks[domains[id]]
		prefs[id] = pref
	}
	ctx = context.WithValue(ctx, gatewayPoolRoundExclusionsKey{}, shared)
	ctx = context.WithValue(ctx, gatewayPoolCandidateDomainsKey{}, identities)
	for id := range domains {
		if _, excluded := req.ExcludedIDs[id]; excluded {
			delete(prefs, id)
			continue
		}
		if _, excluded := shared[id]; excluded {
			delete(prefs, id)
			continue
		}
	}
	return context.WithValue(ctx, gatewayPoolPreferenceKey{}, prefs)
}

func gatewayPoolPreferences(ctx context.Context) gatewayPoolAccountPreferences {
	prefs, _ := ctx.Value(gatewayPoolPreferenceKey{}).(gatewayPoolAccountPreferences)
	return prefs
}

func gatewayPoolPreferenceBetter(a, b gatewayPoolAccountPreference) bool {
	if a.throttled != b.throttled {
		return !a.throttled
	}
	if a.full != b.full {
		return !a.full
	}
	if a.activeRank > 0 && b.activeRank > 0 && a.activeRank != b.activeRank {
		return a.activeRank < b.activeRank
	}
	if a.verified != b.verified {
		return a.verified
	}
	if a.restFirst && b.restFirst && !a.lastTouch.Equal(b.lastTouch) {
		return a.lastTouch.Before(b.lastTouch)
	}
	return a.cooled > b.cooled // unknown (-1) is distinct from a measured zero
}

// Only participating slots are reordered. Ordinary accounts retain both their
// relative order and positions; equal/unknown observations retain baseline.
func gatewayPoolOrder[T any](ctx context.Context, values []T, accountOf func(T) *Account) {
	prefs := gatewayPoolPreferences(ctx)
	if len(prefs) < 2 {
		return
	}
	var indices []int
	var cohort []T
	for i, value := range values {
		if account := accountOf(value); account != nil {
			if _, known := prefs[account.ID]; known {
				indices = append(indices, i)
				cohort = append(cohort, value)
			}
		}
	}
	sort.SliceStable(cohort, func(i, j int) bool {
		sticky, _ := ctx.Value(gatewayPoolStickyPreferenceKey{}).(map[int64]bool)
		if sticky[accountOf(cohort[i]).ID] != sticky[accountOf(cohort[j]).ID] {
			return sticky[accountOf(cohort[i]).ID]
		}
		return gatewayPoolPreferenceBetter(prefs[accountOf(cohort[i]).ID], prefs[accountOf(cohort[j]).ID])
	})
	for i, index := range indices {
		values[index] = cohort[i]
	}
}

// Load balancing applies to new sessions, not eviction of a healthy old binding.
// Rest, authentication, compatibility and the existing slot gates decide whether
// a sticky row is still usable; neither 429 scoring nor another row's proof moves it.
func gatewayPoolPreferAlternative(ctx context.Context, accountID int64) bool {
	excluded, _ := ctx.Value(gatewayPoolRoundExclusionsKey{}).(map[int64]struct{})
	_, blocked := excluded[accountID]
	return blocked
}

// Apply the preference BEFORE top-K truncation, then retain the usual weighted
// tie order. Otherwise a high-capacity pool account could be absent from top-K.
func gatewayPoolTopCandidates(ctx context.Context, pool, baseline []openAIAccountCandidateScore) []openAIAccountCandidateScore {
	prefs := gatewayPoolPreferences(ctx)
	if len(prefs) < 2 {
		return baseline
	}
	ordered := selectTopKOpenAICandidates(pool, len(pool))
	gatewayPoolOrder(ctx, ordered, func(candidate openAIAccountCandidateScore) *Account { return candidate.account })
	var cohort []openAIAccountCandidateScore
	for _, candidate := range ordered {
		if _, known := prefs[candidate.account.ID]; known {
			cohort = append(cohort, candidate)
		}
	}
	index := 0
	for i, candidate := range baseline {
		if _, known := prefs[candidate.account.ID]; known && index < len(cohort) {
			baseline[i] = cohort[index]
			index++
		}
	}
	return baseline
}
