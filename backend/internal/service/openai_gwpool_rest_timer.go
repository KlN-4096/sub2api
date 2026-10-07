package service

import (
	"context"
	"log/slog"
	"time"
)

const gatewayPoolRestRetry = time.Second

func (s *OpenAIGatewayService) wakeGatewayPoolRestTimer() {
	s.gatewayReporterMu.Lock()
	worker := s.gatewayReporter
	s.gatewayReporterMu.Unlock()
	if worker != nil {
		select {
		case worker.restWake <- struct{}{}:
		default:
		}
	}
}

// One local worker, independent of feedback network calls. The minute scan
// discovers rest/config changes and restarts; the timer sleeps to the nearest
// exact local deadline between scans. Neither path lists or consumes pool tickets.
func (s *OpenAIGatewayService) runGatewayPoolRestTimer(ctx context.Context, worker *gatewayPoolReporter) {
	defer close(worker.restDone)
	for ctx.Err() == nil {
		delay := s.maintainGatewayPoolRests(ctx)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-worker.restWake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (s *OpenAIGatewayService) maintainGatewayPoolRests(ctx context.Context) time.Duration {
	next := time.Now().Add(gatewayPoolOutboxPoll)
	scanCtx, cancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
	accounts, err := s.accountRepo.FindByExtraField(scanCtx, openAIGatewayPoolExtraKey, true)
	cancel()
	if err != nil {
		slog.Warn("gwpool_local_rest_scan_failed")
		return gatewayPoolRestMin
	}
	for i := range accounts {
		if ctx.Err() != nil {
			break
		}
		account := &accounts[i]
		noteCtx, noteCancel := context.WithTimeout(ctx, gatewayPoolWarmNoteTimeout)
		resetErr := s.maintainGatewayPoolCooldownReset(noteCtx, account, time.Now().UTC())
		noteCancel()
		if resetErr != nil {
			slog.Warn("gwpool_cooldown_reset_failed", "account_id", account.ID)
		}
		// Reset locks have been released: rest -> history -> inventory remains
		// the only nesting order when publishing a recovery.
		_, resumeErr := s.gatewayPoolResumeAllowed(ctx, account, true)
		if resumeErr != nil {
			slog.Warn("gwpool_local_rest_resume_failed", "account_id", account.ID)
			if retry := time.Now().Add(gatewayPoolRestMin); retry.Before(next) {
				next = retry
			}
			continue
		}
		identity, identityErr := s.codexCookies.gatewayPoolIdentity(ctx, account)
		if identityErr != nil {
			continue
		}
		if raw, ok := s.codexCookies.poolRestState.Load(gatewayPoolLedgerTag(identity)); ok {
			if state, valid := raw.(gatewayPoolRestState); valid && state.Active && !state.ResumeAt.IsZero() {
				if state.ResumeAt.Before(next) {
					next = state.ResumeAt
				}
			}
		}
	}
	delay := time.Until(next)
	if delay < gatewayPoolRestRetry {
		delay = gatewayPoolRestRetry
	}
	return delay
}
