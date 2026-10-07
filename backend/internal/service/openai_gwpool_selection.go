package service

import (
	"context"
	"sort"
	"strings"
	"time"
)

type gatewayPoolPreferenceKey struct{}

type gatewayPoolAccountPreference struct {
	verified  bool
	current   bool
	cooled    int
	restFirst bool
	lastTouch time.Time
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
	freshAccounts := map[int64]*Account{}
	complete := true
	checker := &defaultOpenAIAccountScheduler{service: s}
	for i := range accounts {
		if !accounts[i].IsOpenAIOAuthLike() {
			continue
		}
		account, err := s.accountRepo.GetByID(ctx, accounts[i].ID)
		if err != nil {
			complete = false
			continue
		}
		s.codexCookies.poolRotationAccounts.Store(accounts[i].ID, gatewayPoolRotationAccount(account, *req.GroupID))
		if !gatewayPoolRotationAccount(account, *req.GroupID) || !account.IsSchedulable() {
			continue
		}
		compatible, _ := checker.isAccountRequestCompatibleReason(ctx, account, req)
		if !compatible || !checker.isAccountTransportCompatible(account, req.RequiredTransport) ||
			(req.RequireCompact && openAICompactSupportTier(account) == 0) {
			continue
		}
		identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
		if err != nil {
			complete = false
			continue
		}
		domains[account.ID] = gatewayPoolLedgerIdentity(identity)
		freshAccounts[account.ID] = account
		if allowed, err := s.gatewayPoolResumeAllowed(ctx, account, true); err != nil || !allowed {
			s.codexCookies.poolRounds.rest(*req.GroupID, identity, time.Now().Add(gatewayPoolRestMin))
			continue
		}
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
		if known {
			prefs[account.ID] = pref
		}
	}
	shared, restFirst := s.codexCookies.poolRounds.snapshot(*req.GroupID, domains, complete)
	holdDomains := map[int64]string{}
	for id, domain := range domains {
		if _, skip := req.ExcludedIDs[id]; skip {
			continue
		}
		if state := gatewayPoolRotationFrom(ctx); state != nil {
			if _, skip := state.attempted[id]; skip {
				continue
			}
			if _, skip := state.domains[domain]; skip {
				continue
			}
		}
		holdDomains[id] = domain
	}
	current := s.codexCookies.poolRounds.current(*req.GroupID, holdDomains, complete)
	if current != "" {
		for id, domain := range domains {
			if domain == current {
				if s.gatewayPoolNoRemainingRoutes(ctx, freshAccounts[id]) {
					s.codexCookies.poolRounds.exhaust(*req.GroupID, current, s.codexCookies.poolRounds.generation(*req.GroupID))
					s.restGatewayPoolAccount(ctx, freshAccounts[id], current, *req.GroupID)
					current = ""
					for peerID, peerDomain := range domains {
						if peerDomain == domain {
							shared[peerID] = struct{}{}
						}
					}
				}
				break
			}
		}
	}
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
	ctx = context.WithValue(ctx, gatewayPoolRoundExclusionsKey{}, shared)
	for id, domain := range domains {
		if _, excluded := req.ExcludedIDs[id]; excluded {
			delete(prefs, id)
			continue
		}
		if _, excluded := shared[id]; excluded {
			delete(prefs, id)
			continue
		}
		if restFirst {
			pref := prefs[id]
			pref.restFirst, pref.lastTouch = true, s.codexCookies.poolRounds.lastTouch(domain)
			prefs[id] = pref
		}
		if current != "" {
			pref := prefs[id]
			pref.current = domain == current
			prefs[id] = pref
			if domain != current && !pref.verified {
				shared[id] = struct{}{}
			}
		}
	}
	if len(prefs) < 2 {
		for _, pref := range prefs {
			if pref.verified {
				return context.WithValue(ctx, gatewayPoolPreferenceKey{}, prefs)
			}
		}
		return ctx
	}
	return context.WithValue(ctx, gatewayPoolPreferenceKey{}, prefs)
}

func gatewayPoolPreferences(ctx context.Context) gatewayPoolAccountPreferences {
	prefs, _ := ctx.Value(gatewayPoolPreferenceKey{}).(gatewayPoolAccountPreferences)
	return prefs
}

func gatewayPoolPreferenceBetter(a, b gatewayPoolAccountPreference) bool {
	if a.verified != b.verified {
		return a.verified
	}
	if a.current != b.current {
		return a.current
	}
	if a.restFirst && b.restFirst && !a.lastTouch.Equal(b.lastTouch) {
		return a.lastTouch.Before(b.lastTouch)
	}
	return a.cooled > b.cooled
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
		return gatewayPoolPreferenceBetter(prefs[accountOf(cohort[i]).ID], prefs[accountOf(cohort[j]).ID])
	})
	for i, index := range indices {
		values[index] = cohort[i]
	}
}

// Soft session affinity keeps its verified live window; otherwise it may yield.
// Strong previous-response and guardian-parent paths never call this preference gate.
func gatewayPoolPreferAlternative(ctx context.Context, accountID int64) bool {
	prefs := gatewayPoolPreferences(ctx)
	current, known := prefs[accountID]
	if !known || current.verified {
		return false
	}
	for _, alternative := range prefs {
		if gatewayPoolPreferenceBetter(alternative, current) {
			return true
		}
	}
	return false
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
