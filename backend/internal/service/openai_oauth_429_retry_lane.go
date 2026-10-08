package service

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Only schedules retries of a confirmed transient 429. It never intercepts
// initial business sends, auth/quota failures, or gwpool-local preflight errors.
// A cancelled reservation leaves a small gap rather than releasing a slot into
// a second concurrent request. Reservations cannot exceed the original retry
// deadline; a burst larger than 16 must not terminate merely because its turn
// lies beyond one retry's 8s backoff cap.
// These are best-effort wake-up reservations, not a wire-level rate guarantee:
// downstream scheduling/connection waits can still bunch actual transmissions.
func (s *OpenAIGatewayService) reserveOpenAIOAuth429Retry(account *Account, headers http.Header, deadline time.Time) (time.Duration, bool) {
	return s.reserveOpenAIOAuth429RetryAt(account, headers, deadline, time.Now())
}

func (s *OpenAIGatewayService) reserveOpenAIOAuth429RetryAt(account *Account, headers http.Header, deadline, now time.Time) (time.Duration, bool) {
	delay := openAIOAuth429SameAccountRetryDelay(headers, deadline)
	key := openAIOAuth429RetryIdentity(account)
	if deadline.IsZero() || !now.Before(deadline) {
		return 0, false
	}
	for {
		value, loaded := s.openaiOAuth429RetrySlots.Load(key)
		slot := now.Add(delay)
		if previous, ok := value.(time.Time); ok && previous.Add(openAIOAuth429RetryDelay).After(slot) {
			slot = previous.Add(openAIOAuth429RetryDelay)
		}
		if !slot.Before(deadline) {
			return 0, false
		}
		if !loaded {
			if _, raced := s.openaiOAuth429RetrySlots.LoadOrStore(key, slot); raced {
				continue
			}
		} else if !s.openaiOAuth429RetrySlots.CompareAndSwap(key, value, slot) {
			continue
		}
		return slot.Sub(now), true
	}
}

// Provider 429 reservations retain their existing workspace scope. Gateway
// quality cooldowns are independently partitioned by member credentials.
func openAIOAuth429RetryIdentity(account *Account) string {
	identity := codexAccountIdentityNamespace(account)
	if rest, ok := strings.CutPrefix(identity, "chatgpt:"); ok {
		workspace, _, _ := strings.Cut(rest, ":")
		return "chatgpt:" + workspace
	}
	if identity == "" && account != nil {
		return "id:" + strconv.FormatInt(account.ID, 10)
	}
	return identity
}
