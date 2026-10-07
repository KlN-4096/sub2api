package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	openAIGatewayPoolContactsExtraKey = "openai_gwpool_contacts"
	gatewayPoolContactRoundLimit      = 64
	gatewayPoolContactGatewayLimit    = 512
	gatewayPoolContactAliasLimit      = 8
)

type gatewayPoolProbeModelKey struct{}

type gatewayPoolProbeStep struct {
	Shot          string `json:"shot"`
	Sent          bool   `json:"sent"`
	Status        int    `json:"status"`
	GotState      bool   `json:"got_state"`
	EchoAccepted  bool   `json:"echo_accepted"`
	ActualGateway string `json:"actual_gateway,omitempty"` // empty = unobservable, not the expected gateway
	DurationMS    int64  `json:"duration_ms"`
}

type gatewayPoolContactSeen struct {
	FirstAt time.Time `json:"first_at"`
	LastAt  time.Time `json:"last_at"`
}

type gatewayPoolContactRound struct {
	RoundID          string                 `json:"round_id"`
	Aliases          []string               `json:"aliases,omitempty"`
	VersionHash      string                 `json:"version_hash"`
	Report           gwpool.ContactReport   `json:"report"`
	LastAt           time.Time              `json:"last_at"`
	LastProbeOutcome string                 `json:"last_probe_outcome"`
	LastProbeAt      time.Time              `json:"last_probe_at,omitempty"`
	LastProbeModel   string                 `json:"last_probe_model,omitempty"`
	LastProbeSource  string                 `json:"last_probe_source,omitempty"`
	Steps            []gatewayPoolProbeStep `json:"steps,omitempty"`
}

type gatewayPoolContacts struct {
	LedgerTag        string                            `json:"ledger_tag"`
	TrackingSince    time.Time                         `json:"tracking_since"`
	HistoryTruncated bool                              `json:"history_truncated,omitempty"`
	Seen             map[string]gatewayPoolContactSeen `json:"seen"`
	Rounds           []gatewayPoolContactRound         `json:"rounds"`
}

type gatewayPoolContactEvent struct {
	Applied                          OpenAIGatewayPoolApplied
	Identity, Model, Source, Outcome string
	FirstSent, LastSent              time.Time
	Steps                            []gatewayPoolProbeStep
}

func gatewayPoolContactHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func gatewayPoolContactRoundID(identity, version string, at time.Time) string {
	return gatewayPoolContactHash("contact-round-v1\x00" + gatewayPoolLedgerIdentity(identity) + "\x00" + version + "\x00" + at.UTC().Format(time.RFC3339Nano))
}

func readGatewayPoolContacts(account *Account, tag string) gatewayPoolContacts {
	var state gatewayPoolContacts
	if account != nil {
		raw, _ := json.Marshal(account.Extra[openAIGatewayPoolContactsExtraKey])
		_ = json.Unmarshal(raw, &state)
	}
	if state.LedgerTag != tag {
		state = gatewayPoolContacts{LedgerTag: tag}
	}
	if state.Seen == nil {
		state.Seen = map[string]gatewayPoolContactSeen{}
	}
	pruneGatewayPoolContacts(&state, time.Now())
	return state
}

func pruneGatewayPoolContacts(state *gatewayPoolContacts, now time.Time) {
	rounds := state.Rounds[:0]
	for _, r := range state.Rounds {
		if len(r.RoundID) == 64 && len(r.Report.ID) == 64 &&
			!r.Report.At.Before(now.Add(-gatewayPoolOutboxRetention)) && !r.Report.At.After(now.Add(time.Minute)) {
			if len(r.Steps) > 2 {
				r.Steps = r.Steps[:2]
			}
			if len(r.Aliases) > gatewayPoolContactAliasLimit {
				r.Aliases = r.Aliases[:gatewayPoolContactAliasLimit]
			}
			rounds = append(rounds, r)
		}
	}
	sort.Slice(rounds, func(i, j int) bool { return rounds[i].Report.At.Before(rounds[j].Report.At) })
	if len(rounds) > gatewayPoolContactRoundLimit {
		rounds = rounds[len(rounds)-gatewayPoolContactRoundLimit:]
	}
	state.Rounds = rounds
	for len(state.Seen) > gatewayPoolContactGatewayLimit {
		var oldest string
		for name, seen := range state.Seen {
			if oldest == "" || seen.LastAt.Before(state.Seen[oldest].LastAt) {
				oldest = name
			}
		}
		delete(state.Seen, oldest)
		state.HistoryTruncated = true
	}
}

func mergeGatewayPoolContacts(state *gatewayPoolContacts, other gatewayPoolContacts) {
	if other.LedgerTag != state.LedgerTag {
		return
	}
	if !other.TrackingSince.IsZero() && (state.TrackingSince.IsZero() || other.TrackingSince.Before(state.TrackingSince)) {
		state.TrackingSince = other.TrackingSince
	}
	state.HistoryTruncated = state.HistoryTruncated || other.HistoryTruncated
	for name, seen := range other.Seen {
		prev := state.Seen[name]
		if prev.FirstAt.IsZero() || (!seen.FirstAt.IsZero() && seen.FirstAt.Before(prev.FirstAt)) {
			prev.FirstAt = seen.FirstAt
		}
		if seen.LastAt.After(prev.LastAt) {
			prev.LastAt = seen.LastAt
		}
		state.Seen[name] = prev
	}
	for _, r := range other.Rounds {
		found := false
		for i, prev := range state.Rounds {
			if prev.Report.ID != r.Report.ID {
				continue
			}
			found = true
			target := &state.Rounds[i]
			if r.Report.WindowFinal && !prev.Report.WindowFinal {
				target.Report = r.Report
			}
			if r.LastAt.After(prev.LastAt) {
				target.LastAt = r.LastAt
			}
			if r.LastProbeAt.After(prev.LastProbeAt) {
				target.LastProbeAt, target.LastProbeModel, target.LastProbeSource = r.LastProbeAt, r.LastProbeModel, r.LastProbeSource
				target.Steps, target.LastProbeOutcome = r.Steps, r.LastProbeOutcome
			}
			for _, alias := range r.Aliases {
				target.addAlias(alias)
			}
			break
		}
		if !found {
			state.Rounds = append(state.Rounds, r)
		}
	}
}

func (r *gatewayPoolContactRound) matchesRound(id string) bool {
	return r.RoundID == id || slices.Contains(r.Aliases, id)
}

func (r *gatewayPoolContactRound) addAlias(id string) {
	if len(id) == 64 && !r.matchesRound(id) && len(r.Aliases) < gatewayPoolContactAliasLimit {
		r.Aliases = append(r.Aliases, id)
	}
}

// The identity lock covers clones in this process. Cross-instance exactly-once
// is deliberately not claimed. All persisted times describe locally observed sends.
func (s *OpenAIGatewayService) changeGatewayPoolContacts(ctx context.Context, account *Account, identity string,
	change func(*Account, *gatewayPoolContacts) *gwpool.ContactReport) {
	if s == nil || s.accountRepo == nil || account == nil || identity == "" {
		return
	}
	tag := gatewayPoolLedgerTag(identity)
	value, _ := s.codexCookies.poolContactLocks.LoadOrStore(tag, &sync.Mutex{})
	lock, ok := value.(*sync.Mutex)
	if !ok {
		panic("gwpool contact lock has an invalid type")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer cancel()
	lock.Lock()
	defer lock.Unlock()
	unlock := s.codexCookies.gatewayPoolHistoryLock(account.ID)
	defer unlock()
	fresh, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || fresh == nil {
		slog.Warn("gwpool_contact_read_failed", "account_id", account.ID)
		return
	}
	state := readGatewayPoolContacts(fresh, tag)
	peers, err := s.accountRepo.FindByExtraField(ctx, openAIGatewayLedgerTagExtraKey, tag)
	if err != nil {
		slog.Warn("gwpool_contact_history_incomplete", "account_id", account.ID)
		state.HistoryTruncated = true
	}
	for i := range peers {
		if peers[i].ID != fresh.ID {
			mergeGatewayPoolContacts(&state, readGatewayPoolContacts(&peers[i], tag))
		}
	}
	report := change(fresh, &state)
	if report != nil && len(report.AccountTag) != 64 {
		slog.Warn("gwpool_contact_report_identity_unavailable", "account_id", account.ID)
		report = nil
	}
	pruneGatewayPoolContacts(&state, time.Now())
	patch := map[string]any{openAIGatewayPoolContactsExtraKey: state, openAIGatewayLedgerTagExtraKey: tag}
	if report != nil {
		box := readGatewayPoolOutbox(fresh, time.Now())
		queueGatewayPoolPending(&box, gatewayPoolPendingReport{Kind: "contact", Contact: report,
			Binding: gatewayPoolReportBinding(fresh, report.AccountTag), CreatedAt: time.Now().UTC()})
		patch[openAIGatewayPoolOutboxExtraKey] = box
	}
	// Contact classification and its outbox item commit in the same extra patch.
	if err := s.accountRepo.UpdateExtra(ctx, fresh.ID, patch); err != nil {
		slog.Warn("gwpool_contact_persist_failed", "account_id", account.ID)
		return
	}
	if report != nil {
		s.wakeGatewayPoolReporter()
	}
}

func (s *OpenAIGatewayService) noteGatewayPoolContact(ctx context.Context, account *Account, event gatewayPoolContactEvent) {
	if account == nil || event.Applied.AccountID != account.ID || event.Applied.Gateway == "" ||
		event.Applied.RoundID == "" || event.FirstSent.IsZero() {
		return
	}
	if event.LastSent.IsZero() {
		event.LastSent = event.FirstSent
	}
	if event.Model == "" {
		event.Model = "unknown"
	}
	if event.Outcome == "" {
		event.Outcome = "unknown"
	}
	s.changeGatewayPoolContacts(ctx, account, event.Identity, func(fresh *Account, state *gatewayPoolContacts) *gwpool.ContactReport {
		at, last := event.FirstSent.UTC(), event.LastSent.UTC()
		gateway := event.Applied.Gateway
		seen := state.Seen[gateway]
		if state.TrackingSince.IsZero() {
			state.TrackingSince = at
		}
		version := gatewayPoolContactHash(event.Applied.Version)
		index := -1
		for i, r := range state.Rounds {
			if r.Report.Gateway != gateway {
				continue
			}
			sameVersion := event.Applied.Version != "" && r.VersionHash == version &&
				!at.After(r.LastAt.Add(openAIGatewayFullWindow)) && !at.Before(r.Report.At.Add(-time.Second))
			if r.matchesRound(event.Applied.RoundID) || sameVersion {
				index = i
				break
			}
		}
		var report *gwpool.ContactReport
		if index < 0 {
			first := "tracked_first"
			gapKnown := !seen.LastAt.IsZero() && !at.Before(seen.LastAt)
			gap := int64(0)
			if gapKnown {
				first = "repeat"
				gap = int64(at.Sub(seen.LastAt) / time.Second)
			}
			if gap > 30*24*60*60 {
				gapKnown = false
				gap = 0
			}
			history, _ := readOpenAIGatewayHistory(fresh)
			_, legacy := history.Seen[gateway]
			if !gapKnown && (!seen.FirstAt.IsZero() || legacy || state.HistoryTruncated || event.Source == "business") {
				first = "unknown"
			}
			tag := gatewayPoolAccountTag(fresh, event.Identity)
			r := gatewayPoolContactRound{RoundID: event.Applied.RoundID, VersionHash: version, LastAt: last,
				LastProbeOutcome: event.Outcome, Steps: event.Steps,
				Report: gwpool.ContactReport{ID: gatewayPoolContactHash("contact-report-v1\x00" + tag + "\x00" + event.Applied.RoundID),
					AccountTag: tag, Gateway: gateway, Model: event.Model, Criterion: gwpool.ContactCriterion, Source: event.Source,
					First: first, At: at, GapKnown: gapKnown, ElapsedSeconds: gap, Outcome: event.Outcome}}
			state.Rounds = append(state.Rounds, r)
			state.Rounds[len(state.Rounds)-1].noteProbe(event)
			report = &r.Report
		} else {
			r := &state.Rounds[index]
			r.addAlias(event.Applied.RoundID)
			if last.After(r.LastAt) {
				r.LastAt = last
			}
			r.noteProbe(event)
		}
		if seen.FirstAt.IsZero() || at.Before(seen.FirstAt) {
			seen.FirstAt = at
		}
		if last.After(seen.LastAt) {
			seen.LastAt = last
		}
		state.Seen[gateway] = seen
		return report
	})
}

// Business sends advance LastAt but do not change the latest probe context.
// Keep this separate from the frozen first-contact report used for scoring.
func (r *gatewayPoolContactRound) noteProbe(event gatewayPoolContactEvent) {
	if len(event.Steps) == 0 || !event.LastSent.After(r.LastProbeAt) {
		return
	}
	r.LastProbeAt, r.LastProbeModel, r.LastProbeSource = event.LastSent, event.Model, event.Source
	r.Steps, r.LastProbeOutcome = event.Steps, event.Outcome
}

func (s *OpenAIGatewayService) finishGatewayPoolContact(ctx context.Context, account *Account, identity string,
	applied OpenAIGatewayPoolApplied, held time.Duration) {
	if applied.RoundID == "" || held <= 0 || held > 10*time.Hour {
		return
	}
	s.changeGatewayPoolContacts(ctx, account, identity, func(_ *Account, state *gatewayPoolContacts) *gwpool.ContactReport {
		for i := range state.Rounds {
			r := &state.Rounds[i]
			if !r.matchesRound(applied.RoundID) || r.Report.Gateway != applied.Gateway || r.Report.Outcome != "full" || r.Report.WindowFinal {
				continue
			}
			r.Report.FullWindowMS = held.Milliseconds()
			r.Report.WindowFinal = true
			report := r.Report
			return &report
		}
		return nil
	})
}

func (s *OpenAIGatewayService) noteGatewayPoolProbeAndContact(ctx context.Context, account *Account, observation gatewayPoolProbeObservation) {
	s.noteGatewayPoolProbe(ctx, account, observation)
	if observation.Shots == 0 {
		return
	}
	outcome := "unknown"
	if observation.Conclusive {
		outcome = "refreshed"
		if observation.Full {
			outcome = "full"
		}
	}
	s.noteGatewayPoolContact(ctx, account, gatewayPoolContactEvent{Applied: observation.Applied, Identity: observation.Identity,
		Model: observation.Model, Source: observation.Source, Outcome: outcome, FirstSent: observation.FirstSent,
		LastSent: observation.LastSent, Steps: observation.Steps})
}

func (s *OpenAIGatewayService) noteGatewayPoolBusinessContact(request *http.Request, account *Account, identity string,
	applied OpenAIGatewayPoolApplied, started time.Time) {
	outcome := "unknown"
	snapshot := openAIGatewayPoolSinkFrom(request.Context()).snapshot()
	if snapshot.AccountID == account.ID && snapshot.RoundID == applied.RoundID {
		switch snapshot.Verdict {
		case openAIGatewayVerdictFull:
			outcome = "full"
		case openAIGatewayVerdictDegraded:
			outcome = "refreshed"
		}
	}
	s.noteGatewayPoolContact(request.Context(), account, gatewayPoolContactEvent{Applied: applied, Identity: identity,
		Model: gatewayPoolWarmModel(request), Source: "business", Outcome: outcome, FirstSent: started, LastSent: started})
}
