package service

import (
	"context"
	"sort"
	"time"
)

type GatewayPoolLiveTicket struct {
	Gateway        string    `json:"gateway"`
	Region         string    `json:"region"`
	ExpiresAt      time.Time `json:"expires_at,omitzero"`
	VerifiedAt     time.Time `json:"verified_at,omitzero"`
	VerifiedModels []string  `json:"verified_models"`
}

type GatewayPoolRuntimeView struct {
	ObservedAt           time.Time                          `json:"observed_at"`
	Tickets              []GatewayPoolLiveTicket            `json:"tickets"`
	Rounds               []GatewayPoolUsageRound            `json:"rounds"`
	Archived             map[string]GatewayPoolUsageArchive `json:"archived"`
	Incomplete           bool                               `json:"incomplete,omitempty"`
	History              gatewayPoolHistoryDisplay          `json:"history"`
	Contacts             gatewayPoolContactsDisplay         `json:"contacts"`
	LedgerTag            string                             `json:"ledger_tag"`
	GatewayWindowSeconds int                                `json:"gateway_window_seconds"`
	CurrentConcurrency   *int                               `json:"current_concurrency"`
	ConcurrencyLimit     int                                `json:"concurrency_limit"`
	CooldownEstimate     *GatewayPoolCooldownEstimate       `json:"cooldown_estimate,omitempty"`
	Queues               *GatewayPoolQueueView              `json:"queues"`
	Rest                 GatewayPoolRestView                `json:"rest"`
}

// The account cell reads only these fields of the merged history and contacts,
// so the one-second poll sends nothing else. Tags match the stored records.
// TestGatewayPoolRuntimeDisplayFixtureInSync and AccountGatewayCell.runtimeDisplay.spec.ts
// fail when the cell reads a field these drop.
type gatewayPoolHistoryDisplay struct {
	Current       string                            `json:"current"`
	CurrentRegion string                            `json:"current_region,omitempty"`
	Seen          map[string]gatewayPoolSeenDisplay `json:"seen"`
	UpdatedAt     time.Time                         `json:"updated_at"`
}

type gatewayPoolSeenDisplay struct {
	At         time.Time                   `json:"at"`
	Region     string                      `json:"region,omitempty"`
	Verdict    string                      `json:"verdict,omitempty"`
	FullAt     time.Time                   `json:"full_at,omitzero"`
	FullHeldMs int64                       `json:"full_held_ms,omitempty"`
	Cooldown   *gatewayPoolCooldownDisplay `json:"cooldown,omitempty"`
}

type gatewayPoolCooldownDisplay struct {
	Cleared            bool      `json:"cleared,omitempty"`
	WindowSeconds      int       `json:"window_seconds"`
	FixedSeconds       int       `json:"fixed_seconds,omitempty"`
	Until              time.Time `json:"until,omitzero"`
	RecommendedSeconds int       `json:"recommended_seconds,omitempty"`
}

type gatewayPoolContactsDisplay struct {
	LedgerTag        string                           `json:"ledger_tag"`
	TrackingSince    time.Time                        `json:"tracking_since"`
	HistoryTruncated bool                             `json:"history_truncated,omitempty"`
	Rounds           []gatewayPoolContactRoundDisplay `json:"rounds"`
}

type gatewayPoolContactRoundDisplay struct {
	VersionHash string                          `json:"version_hash"`
	Report      gatewayPoolContactReportDisplay `json:"report"`
}

type gatewayPoolContactReportDisplay struct {
	ID           string    `json:"id"`
	Gateway      string    `json:"gateway"`
	Model        string    `json:"model"`
	Source       string    `json:"source"`
	At           time.Time `json:"at"`
	Outcome      string    `json:"outcome"`
	FullWindowMS int64     `json:"full_window_ms"`
	WindowFinal  bool      `json:"window_final"`
}

func (h openAIGatewayHistory) display() gatewayPoolHistoryDisplay {
	out := gatewayPoolHistoryDisplay{Current: h.Current, CurrentRegion: h.CurrentRegion, UpdatedAt: h.UpdatedAt}
	if h.Seen != nil {
		out.Seen = make(map[string]gatewayPoolSeenDisplay, len(h.Seen))
	}
	for gateway, seen := range h.Seen {
		row := gatewayPoolSeenDisplay{At: seen.At, Region: seen.Region, Verdict: seen.Verdict,
			FullAt: seen.FullAt, FullHeldMs: seen.FullHeldMs}
		if c := seen.Cooldown; c != nil {
			row.Cooldown = &gatewayPoolCooldownDisplay{Cleared: c.Cleared, WindowSeconds: c.WindowSeconds,
				FixedSeconds: c.FixedSeconds, Until: c.Until, RecommendedSeconds: c.LegacyRecommendedSeconds}
		}
		out.Seen[gateway] = row
	}
	return out
}

func (c gatewayPoolContacts) display() gatewayPoolContactsDisplay {
	out := gatewayPoolContactsDisplay{LedgerTag: c.LedgerTag, TrackingSince: c.TrackingSince, HistoryTruncated: c.HistoryTruncated}
	if c.Rounds != nil {
		out.Rounds = make([]gatewayPoolContactRoundDisplay, len(c.Rounds))
	}
	for i, round := range c.Rounds {
		r := round.Report
		out.Rounds[i] = gatewayPoolContactRoundDisplay{VersionHash: round.VersionHash, Report: gatewayPoolContactReportDisplay{
			ID: r.ID, Gateway: r.Gateway, Model: r.Model, Source: r.Source, At: r.At,
			Outcome: r.Outcome, FullWindowMS: r.FullWindowMS, WindowFinal: r.WindowFinal,
		}}
	}
	return out
}

// Read-only local snapshot. This endpoint never fetches tickets, lists gateways,
// ends a round, or probes an upstream model. The cooldown estimate is only
// computed when estimate is set (the temporary-unschedulable dialog).
func (s *OpenAIGatewayService) GatewayPoolRuntimeProgress(ctx context.Context, ids []int64, estimate bool) (map[int64]GatewayPoolProgress, error) {
	if s.accountRepo == nil {
		return s.GatewayPoolProgress(ids), nil
	}
	out := map[int64]GatewayPoolProgress{}
	accounts, err := s.accountRepo.GetByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	var concurrency map[int64]int
	if s.concurrencyService != nil {
		// A failed/missing result stays nil. It must not render as measured zero.
		concurrency, err = s.concurrencyService.GetAccountConcurrencyBatch(ctx, ids)
		if err != nil {
			concurrency = nil
		}
	}
	historyPeers := map[string][]Account{}
	restPeers := map[string][]Account{}
	displayCache := gatewayPoolDisplayCache{}
	for _, account := range accounts {
		if account == nil || !s.codexCookies.gatewayPoolTakeover(account) {
			continue
		}
		identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
		if err != nil {
			return nil, err // an unreadable pool identity is not evidence of recovery
		}
		tag := gatewayPoolLedgerTag(identity)
		peers, loaded := historyPeers[tag]
		if !loaded {
			peers, err = s.gatewayPoolHistoryPeers(ctx, tag)
			if err != nil {
				return nil, err
			}
			historyPeers[tag] = peers
		}
		state := readGatewayPoolUsage(account, tag)
		if cached, ok := s.codexCookies.poolUsageCache.Load(tag); ok {
			if other, valid := cached.(*gatewayPoolUsageLedger); valid && other.UpdatedAt.After(state.UpdatedAt) {
				state = *other
			}
		} else {
			usagePeers, err := s.gatewayPoolStatePeers(ctx, tag, "usage", 0)
			if err != nil {
				return nil, err
			}
			for i := range usagePeers {
				other := readGatewayPoolUsage(&usagePeers[i], tag)
				if other.UpdatedAt.After(state.UpdatedAt) {
					state = other
				}
			}
			s.codexCookies.poolUsageCache.LoadOrStore(tag, &state)
		}
		runtime := &GatewayPoolRuntimeView{ObservedAt: time.Now().UTC(), Tickets: []GatewayPoolLiveTicket{},
			Rounds: make([]GatewayPoolUsageRound, len(state.Rounds)), Archived: state.Archived, Incomplete: state.Incomplete}
		history, contacts := s.gatewayPoolDisplaySnapshot(account, identity, peers, displayCache)
		runtime.History, runtime.Contacts = history.display(), contacts.display()
		var deadlines map[string]time.Time
		cooldownDeadlines := func() map[string]time.Time { // shared by the queue view and the estimate
			if deadlines == nil {
				deadlines = s.codexCookies.gatewayPoolDisplayCooldownDeadlines(identity, account, history, runtime.ObservedAt)
			}
			return deadlines
		}
		runtime.Queues = s.codexCookies.gatewayPoolQueueView(account, identity, contacts, runtime.ObservedAt, cooldownDeadlines)
		restRows, loaded := restPeers[tag]
		if !loaded {
			restRows, err = s.gatewayPoolStatePeers(ctx, tag, "rest", 0)
			if err != nil {
				return nil, err // an unavailable rest snapshot is not a recovered account
			}
			restPeers[tag] = restRows
		}
		var rest gatewayPoolRestState
		if !account.GatewayPoolContinuousWaitEnabled() {
			rest = s.codexCookies.gatewayPoolRestSnapshot(account, identity, restRows)
		}
		runtime.Rest = gatewayPoolRestDisplay(account, rest)
		if estimate {
			cooldown := gatewayPoolCooldownEstimate(account, cooldownDeadlines(), rest)
			runtime.CooldownEstimate = &cooldown
		}
		runtime.LedgerTag, runtime.ConcurrencyLimit = tag, account.Concurrency
		runtime.GatewayWindowSeconds = int(account.gatewayPoolGatewayWindow().Seconds())
		if count, known := concurrency[account.ID]; known {
			runtime.CurrentConcurrency = &count
		}
		activeEvents := s.codexCookies.activeUsageTracker(identity).snapshot()
		session := s.codexCookies.gatewayPoolUsageSession()
		blockedAt := gatewayPoolUsageBlockedAt(account)
		var idleAt time.Time
		inventory := s.codexCookies.gatewayPoolInventory(identity)
		inventory.mu.Lock()
		if inventory.active == 0 && inventory.requests == 0 {
			idleAt = state.idleCutoff(runtime.ObservedAt)
		}
		inventory.mu.Unlock()
		for i := range state.Rounds {
			round := state.Rounds[i] // read-only projection; never mutate cached ticket maps
			if round.Model == gatewayPoolUsageSharedModel && round.EndedAt.IsZero() {
				if !idleAt.IsZero() && !idleAt.Before(round.StartedAt) {
					round.EndedAt, round.EndReason = idleAt, "idle_timeout"
				}
				if !blockedAt.IsZero() && !blockedAt.Before(round.StartedAt) &&
					(round.EndedAt.IsZero() || blockedAt.Before(round.EndedAt)) {
					round.EndedAt, round.EndReason = blockedAt, "temporarily_unschedulable"
				}
			}
			runtime.Rounds[i] = round.fullUsageView(session, runtime.ObservedAt, activeEvents...)
		}
		if pair, live := s.codexCookies.cachedPoolPair(identity); live == openAIGatewayPoolPairLive {
			ticket := GatewayPoolLiveTicket{Gateway: pair.gateway, Region: pair.region, ExpiresAt: pair.routeExpiresAt, VerifiedModels: []string{}}
			if mark, ok := s.codexCookies.gatewayPoolVerifiedMarkOf(identity); ok && mark.version == pair.version && mark.models != nil {
				ticket.VerifiedAt = mark.at
				for model := range *mark.models {
					ticket.VerifiedModels = append(ticket.VerifiedModels, model)
				}
				sort.Strings(ticket.VerifiedModels)
			}
			runtime.Tickets = append(runtime.Tickets, ticket)
		}
		closedBefore := state.ClosedBefore[gatewayPoolUsageSharedModel]
		for _, boundary := range []time.Time{blockedAt, idleAt} {
			if boundary.After(closedBefore) {
				closedBefore = boundary
			}
		}
		// Scope filtering must precede selection and retention: otherwise a
		// late old run can evict the new cycle's last completed progress.
		progressAccount := account.ID
		sharedProgress := false
		if value, ok := s.codexCookies.poolPrepareProgress.Load(identity); ok {
			if run, ok := value.(*gatewayPoolProgressRun); ok {
				progressAccount = run.account
				sharedProgress = true
			}
		}
		progress := s.codexCookies.poolProgress.snapshot([]int64{progressAccount}, runtime.ObservedAt,
			map[int64]gatewayPoolProgressScope{progressAccount: {tag: tag, closedBefore: closedBefore, identity: identity}})[progressAccount]
		if sharedProgress && progress.RunID != "" {
			progress.ActiveRequests = s.codexCookies.gatewayPoolPreparationWaiters(identity)
		}
		if waiting := s.codexCookies.gatewayPoolContinuousWaiters(identity); waiting > 0 && account.GatewayPoolContinuousWaitEnabled() {
			progress.ActiveRequests += waiting
			if s.codexCookies.gatewayPoolPreparationWaiters(identity) == 0 && !s.codexCookies.gatewayPoolVerifiedFull(identity) {
				progress.Phase = "waiting"
				if !progress.StartedAt.IsZero() {
					progress.ElapsedMS = runtime.ObservedAt.Sub(progress.StartedAt).Milliseconds()
				}
			}
		}
		if progress.Phase == "" {
			progress.Phase = "idle"
		}
		if progress.Phase == "ready" && !s.codexCookies.gatewayPoolVerifiedFull(identity) {
			// A completed run is historical evidence, not proof that its ticket
			// is still live. Only active preparation may claim to be fetching.
			progress.Phase = "pending"
		}
		progress.Runtime = runtime
		out[account.ID] = progress
	}
	// Answer every requested ID; the admin page waits for a complete batch.
	for _, id := range ids {
		if _, ok := out[id]; !ok {
			out[id] = GatewayPoolProgress{Inactive: true}
		}
	}
	return out, nil
}

func (s *adminServiceImpl) GatewayPoolRuntimeProgress(ctx context.Context, ids []int64, estimate bool) (map[int64]GatewayPoolProgress, error) {
	if reader, ok := s.runtimeBlocker.(interface {
		GatewayPoolRuntimeProgress(context.Context, []int64, bool) (map[int64]GatewayPoolProgress, error)
	}); ok {
		return reader.GatewayPoolRuntimeProgress(ctx, ids, estimate)
	}
	return s.GatewayPoolProgress(ids), nil
}
