package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	openAIGatewayPoolOutboxExtraKey = "openai_gwpool_feedback_outbox"
	gatewayPoolOutboxLimit          = 64
	gatewayPoolOutboxRetention      = 7 * 24 * time.Hour
	gatewayPoolOutboxPoll           = time.Minute
	gatewayPoolOutboxMaxBackoff     = time.Hour
)

type gatewayPoolPendingReport struct {
	Kind      string                `json:"kind,omitempty"`
	Contact   *gwpool.ContactReport `json:"contact,omitempty"`
	Report    gwpool.CooldownReport `json:"report"`
	Binding   string                `json:"binding"` // 配置/身份摘要，不存key、URL或原始上游身份。
	CreatedAt time.Time             `json:"created_at"`
	NextAt    time.Time             `json:"next_at"`
	HoldUntil time.Time             `json:"hold_until,omitempty"` // batching only; independent of network retry
	Attempts  int                   `json:"attempts"`
	Permanent bool                  `json:"permanent,omitempty"`
}

func (p gatewayPoolPendingReport) id() string {
	if p.Kind == "contact" {
		if p.Contact == nil {
			return ""
		}
		return p.Contact.ID
	}
	return p.Report.ID
}
func (p gatewayPoolPendingReport) tag() string {
	if p.Kind == "contact" {
		if p.Contact == nil {
			return ""
		}
		return p.Contact.AccountTag
	}
	return p.Report.AccountTag
}
func (p gatewayPoolPendingReport) samePayload(other gatewayPoolPendingReport) bool {
	if p.Kind != other.Kind || p.Binding != other.Binding || p.Report != other.Report {
		return false
	}
	if p.Contact == nil || other.Contact == nil {
		return p.Contact == nil && other.Contact == nil
	}
	return *p.Contact == *other.Contact
}

type gatewayPoolOutbox struct {
	Pending   []gatewayPoolPendingReport           `json:"pending"`
	Policies  map[string]gatewayPoolFeedbackPolicy `json:"policies,omitempty"`
	Sent      int64                                `json:"sent"`
	Discarded int64                                `json:"discarded"`
}

type gatewayPoolReporter struct {
	cancel   context.CancelFunc
	wake     chan struct{}
	done     chan struct{}
	restWake chan struct{}
	restDone chan struct{}
	once     sync.Once
}

func gatewayPoolReportBinding(account *Account, tag string) string {
	sum := sha256.Sum256([]byte(account.gatewayPoolBaseURL() + "\x00" + tag))
	return hex.EncodeToString(sum[:])
}

func readGatewayPoolOutbox(account *Account, now time.Time) gatewayPoolOutbox {
	var box gatewayPoolOutbox
	if account != nil {
		raw, _ := json.Marshal(account.Extra[openAIGatewayPoolOutboxExtraKey])
		_ = json.Unmarshal(raw, &box)
	}
	if box.Sent < 0 {
		box.Sent = 0
	}
	if box.Discarded < 0 {
		box.Discarded = 0
	}
	for gateway, policy := range box.Policies {
		if len(policy.Binding) != 64 || policy.UpdatedAt.After(now.Add(time.Minute)) ||
			!policy.Recommendation.ReportPolicyExpiresAt.After(now) {
			delete(box.Policies, gateway)
		}
	}
	for len(box.Policies) > gatewayPoolFeedbackPolicyLimit {
		var oldest string
		for gateway, policy := range box.Policies {
			if oldest == "" || policy.UpdatedAt.Before(box.Policies[oldest].UpdatedAt) {
				oldest = gateway
			}
		}
		delete(box.Policies, oldest)
	}
	kept := make([]gatewayPoolPendingReport, 0, min(len(box.Pending), gatewayPoolOutboxLimit))
	for _, pending := range box.Pending {
		if (pending.Kind != "" && pending.Kind != "contact") || len(pending.id()) != 64 || len(pending.tag()) != 64 || len(pending.Binding) != 64 ||
			pending.CreatedAt.Before(now.Add(-gatewayPoolOutboxRetention)) || pending.CreatedAt.After(now.Add(time.Minute)) {
			box.Discarded++
			continue
		}
		pending.Attempts = max(0, min(pending.Attempts, 30))
		kept = append(kept, pending)
	}
	if len(kept) > gatewayPoolOutboxLimit {
		box.Discarded += int64(len(kept) - gatewayPoolOutboxLimit)
		kept = kept[len(kept)-gatewayPoolOutboxLimit:]
	}
	box.Pending = kept
	return box
}

func (s *OpenAIGatewayService) changeGatewayPoolOutbox(ctx context.Context, id int64, change func(*Account, *gatewayPoolOutbox)) error {
	if s.accountRepo == nil {
		return errors.New("gateway feedback repository unavailable")
	}
	unlock := s.codexCookies.gatewayPoolHistoryLock(id)
	defer unlock()
	account, err := s.accountRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if account == nil {
		return errors.New("gateway feedback account missing")
	}
	box := readGatewayPoolOutbox(account, time.Now().UTC())
	change(account, &box)
	if len(box.Pending) > gatewayPoolOutboxLimit {
		box.Discarded += int64(len(box.Pending) - gatewayPoolOutboxLimit)
		box.Pending = box.Pending[len(box.Pending)-gatewayPoolOutboxLimit:]
	}
	return s.accountRepo.UpdateExtra(ctx, id, map[string]any{openAIGatewayPoolOutboxExtraKey: box})
}

func (s *OpenAIGatewayService) enqueueGatewayPoolReport(account *Account, report gwpool.CooldownReport) {
	s.enqueueGatewayPoolPending(account, gatewayPoolPendingReport{Report: report})
}

func (s *OpenAIGatewayService) enqueueGatewayPoolPending(account *Account, pending gatewayPoolPendingReport) {
	ctx, cancel := context.WithTimeout(context.Background(), gatewayPoolWarmNoteTimeout)
	defer cancel()
	pending.Binding = gatewayPoolReportBinding(account, pending.tag())
	pending.CreatedAt = time.Now().UTC()
	err := s.changeGatewayPoolOutbox(ctx, account.ID, func(_ *Account, box *gatewayPoolOutbox) {
		s.codexCookies.syncGatewayPoolFeedbackPolicies(box, pending.CreatedAt)
		prepareGatewayPoolReportHold(box, &pending, pending.CreatedAt)
		queueGatewayPoolPending(box, pending)
	})
	if err != nil {
		slog.Warn("gwpool_feedback_persist_failed", "account_id", account.ID)
		return // 持久化失败绝不先发，避免宣称可靠发送却丢重试凭据。
	}
	s.wakeGatewayPoolReporter()
}

func queueGatewayPoolPending(box *gatewayPoolOutbox, pending gatewayPoolPendingReport) {
	for i, old := range box.Pending {
		if old.Kind == pending.Kind && old.id() == pending.id() {
			if pending.Kind == "contact" && old.Contact != nil && pending.Contact != nil && pending.Contact.WindowFinal && !old.Contact.WindowFinal {
				pending.CreatedAt = old.CreatedAt
				box.Pending[i] = pending
			}
			return
		}
	}
	box.Pending = append(box.Pending, pending)
	if len(box.Pending) > gatewayPoolOutboxLimit {
		box.Discarded += int64(len(box.Pending) - gatewayPoolOutboxLimit)
		box.Pending = box.Pending[len(box.Pending)-gatewayPoolOutboxLimit:]
	}
}

func (s *OpenAIGatewayService) wakeGatewayPoolReporter() {
	s.gatewayReporterMu.Lock()
	worker := s.gatewayReporter
	s.gatewayReporterMu.Unlock()
	if worker != nil {
		select {
		case worker.wake <- struct{}{}:
		default:
		}
	}
}

// StartGatewayPoolReporter 在应用依赖装配完毕后调用，重复调用不会创建第二个 worker。
func (s *OpenAIGatewayService) StartGatewayPoolReporter() {
	if s == nil || s.accountRepo == nil {
		return
	}
	s.gatewayReporterMu.Lock()
	defer s.gatewayReporterMu.Unlock()
	if s.gatewayReporter != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker := &gatewayPoolReporter{cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{}),
		restWake: make(chan struct{}, 1), restDone: make(chan struct{})}
	s.gatewayReporter = worker
	go s.runGatewayPoolRestTimer(ctx, worker)
	go func() {
		defer close(worker.done)
		ticker := time.NewTicker(gatewayPoolOutboxPoll)
		defer ticker.Stop()
		for {
			s.flushGatewayPoolReports(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-worker.wake:
			}
		}
	}()
}

func (s *OpenAIGatewayService) StopGatewayPoolReporter() {
	if s == nil {
		return
	}
	s.gatewayReporterMu.Lock()
	worker := s.gatewayReporter
	s.gatewayReporterMu.Unlock()
	if worker == nil {
		return
	}
	worker.once.Do(worker.cancel)
	<-worker.done
	<-worker.restDone
}

func (s *OpenAIGatewayService) flushGatewayPoolReports(ctx context.Context) {
	if s == nil || s.accountRepo == nil || ctx.Err() != nil {
		return
	}
	scanCtx, scanCancel := context.WithTimeout(ctx, gatewayPoolReportTimeout)
	accounts, err := s.accountRepo.FindByExtraField(scanCtx, openAIGatewayPoolExtraKey, true)
	scanCancel()
	if err != nil {
		slog.Warn("gwpool_feedback_scan_failed")
		return
	}
	for i := range accounts {
		if ctx.Err() != nil {
			return
		}
		account := &accounts[i]
		s.maintainGatewayPoolUsage(ctx, account, time.Now().UTC())
		if account.Extra[openAIGatewayPoolOutboxExtraKey] == nil {
			continue
		}
		box := readGatewayPoolOutbox(account, time.Now().UTC())
		if s.codexCookies.syncGatewayPoolFeedbackPolicies(&box, time.Now().UTC()) {
			noteCtx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
			if err := s.changeGatewayPoolOutbox(noteCtx, account.ID, func(_ *Account, fresh *gatewayPoolOutbox) {
				s.codexCookies.syncGatewayPoolFeedbackPolicies(fresh, time.Now().UTC())
			}); err != nil {
				slog.Warn("gwpool_feedback_policy_update_failed", "account_id", account.ID)
			}
			cancel()
		}
		var saved gatewayPoolOutbox
		raw, _ := json.Marshal(account.Extra[openAIGatewayPoolOutboxExtraKey])
		_ = json.Unmarshal(raw, &saved)
		if len(saved.Pending) != len(box.Pending) {
			// 即使只有过期/永久失败项，也将有界留存落实到数据库，而非仅过滤展示。
			pruneCtx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
			if err := s.changeGatewayPoolOutbox(pruneCtx, account.ID, func(_ *Account, _ *gatewayPoolOutbox) {}); err != nil {
				slog.Warn("gwpool_feedback_prune_failed", "account_id", account.ID)
			}
			cancel()
		}
		identity, identityErr := s.codexCookies.gatewayPoolIdentity(ctx, account)
		if identityErr != nil {
			continue
		}
		binding := gatewayPoolReportBinding(account, gatewayPoolAccountTag(account, identity))
		var batch []gatewayPoolPendingReport
		contactSent := false
		for _, pending := range box.Pending {
			if !gatewayPoolReportReady(box, pending, binding, time.Now().UTC()) {
				continue
			}
			if pending.Kind == "contact" {
				if !contactSent {
					s.sendGatewayPoolPending(ctx, account.ID, pending)
					contactSent = true
				}
			} else if len(batch) < gwpool.CooldownBatchLimit {
				batch = append(batch, pending)
			}
		}
		// One contact request and one bounded cooldown batch per account/pass:
		// contact finalization cannot starve behind stable success reports.
		if len(batch) == 1 {
			s.sendGatewayPoolPending(ctx, account.ID, batch[0])
		} else if len(batch) > 1 {
			s.sendGatewayPoolCooldownBatch(ctx, account.ID, batch)
		}
	}
}

func (s *OpenAIGatewayService) sendGatewayPoolPending(ctx context.Context, accountID int64, pending gatewayPoolPendingReport) {
	startedAt := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, gatewayPoolReportTimeout)
	defer cancel()
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil || account == nil || !account.UsesGatewayPool() {
		return
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return
	}
	tag := gatewayPoolAccountTag(account, identity)
	bindingChanged := pending.Binding != gatewayPoolReportBinding(account, tag)
	var recommendation *gwpool.CooldownRecommendation
	if !bindingChanged {
		pool, poolErr := s.codexCookies.poolClient(account)
		err = poolErr
		if err == nil {
			if pending.Kind == "contact" {
				err = pool.ReportContact(ctx, *pending.Contact)
			} else {
				recommendation, err = pool.ReportCooldown(ctx, pending.Report)
			}
		}
	}
	s.completeGatewayPoolPending(ctx, accountID, identity, pending, recommendation, err, bindingChanged, startedAt)
}

func (s *OpenAIGatewayService) sendGatewayPoolCooldownBatch(ctx context.Context, accountID int64, batch []gatewayPoolPendingReport) {
	startedAt := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, gatewayPoolReportTimeout)
	defer cancel()
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil || account == nil || !account.UsesGatewayPool() {
		return
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return
	}
	binding := gatewayPoolReportBinding(account, gatewayPoolAccountTag(account, identity))
	var sending []gatewayPoolPendingReport
	var reports []gwpool.CooldownReport
	for _, pending := range batch {
		if pending.Binding != binding {
			s.completeGatewayPoolPending(ctx, accountID, identity, pending, nil, nil, true, startedAt)
			continue
		}
		sending = append(sending, pending)
		reports = append(reports, pending.Report)
	}
	if len(sending) == 0 {
		return
	}
	pool, err := s.codexCookies.poolClient(account)
	var results []gwpool.CooldownReportResult
	if err == nil {
		results, err = pool.ReportCooldownBatch(ctx, reports)
	}
	byID := map[string]gwpool.CooldownReportResult{}
	for _, result := range results {
		byID[result.ID] = result
	}
	for _, pending := range sending {
		itemErr := err
		result := byID[pending.id()]
		if itemErr == nil && result.Status != http.StatusOK {
			itemErr = &gwpool.PoolError{Status: result.Status}
		}
		s.completeGatewayPoolPending(ctx, accountID, identity, pending, result.Recommendation, itemErr, false, startedAt)
	}
}

func (s *OpenAIGatewayService) completeGatewayPoolPending(ctx context.Context, accountID int64, identity string,
	pending gatewayPoolPendingReport, recommendation *gwpool.CooldownRecommendation, sendErr error, bindingChanged bool, startedAt time.Time) {
	// Network calls hold no history lock; merge ACKs into freshly read state by
	// kind+ID+payload, never overwrite concurrent enqueue or contact finalization.
	noteCtx, noteCancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer noteCancel()
	writeErr := s.changeGatewayPoolOutbox(noteCtx, accountID, func(_ *Account, box *gatewayPoolOutbox) {
		for i := range box.Pending {
			item := &box.Pending[i]
			if item.Kind != pending.Kind || item.id() != pending.id() {
				continue
			}
			if !item.samePayload(pending) {
				return
			} // A later window update arrived while this payload was in flight.
			if bindingChanged || sendErr == nil {
				box.Pending = append(box.Pending[:i], box.Pending[i+1:]...)
				if bindingChanged {
					box.Discarded++
				} else {
					box.Sent++
					if pending.Kind == "" {
						acknowledgeGatewayPoolFeedbackPolicy(box, pending, recommendation, time.Now().UTC(), startedAt)
						s.codexCookies.syncGatewayPoolFeedbackPolicies(box, time.Now().UTC())
					}
				}
			} else {
				item.Attempts = min(item.Attempts+1, 30)
				delay := gatewayPoolOutboxPoll * time.Duration(1<<min(item.Attempts-1, 6))
				item.NextAt = time.Now().UTC().Add(min(delay, gatewayPoolOutboxMaxBackoff))
				var refused *gwpool.PoolError
				if errors.As(sendErr, &refused) && refused.Status >= http.StatusBadRequest &&
					refused.Status < http.StatusInternalServerError && refused.Status != http.StatusTooManyRequests {
					item.Permanent = true // 保留可见失败，不无限重放4xx。
				}
			}
			return
		}
	})
	if writeErr != nil {
		slog.Warn("gwpool_feedback_update_failed", "account_id", accountID)
	}
	if sendErr == nil && !bindingChanged && pending.Kind != "contact" {
		s.codexCookies.noteGatewayPoolRecommendationAt(identity, pending.Report.Gateway, recommendation, startedAt)
	}
}
