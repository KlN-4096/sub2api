package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

const openAIGatewayPoolMetricsExtraKey = "openai_gwpool_metrics"

type gatewayPoolProbeTraceKey struct{}
type gatewayPoolProbeSourceKey struct{}

type gatewayPoolProbeTrace struct {
	shots         int
	firstSent     time.Time
	onFirst       func(time.Time)
	lastSent      time.Time
	actualGateway string
	mayHaveSent   bool // Used only for conservative ticket return, never contact statistics.
}

func (t *gatewayPoolProbeTrace) markSent(at time.Time) {
	t.shots++
	if at.After(t.lastSent) {
		t.lastSent = at
	}
	if t.firstSent.IsZero() {
		t.firstSent = at
		if t.onFirst != nil {
			t.onFirst(at)
		}
	}
}

func gatewayPoolProbeSource(ctx context.Context) string {
	if source, _ := ctx.Value(gatewayPoolProbeSourceKey{}).(string); source == "background" {
		return source
	}
	return "foreground"
}

func (s *openAICodexCookieStore) gatewayPoolMarkSent(identity, version string, at time.Time) {
	s.poolRounds.touch(identity, at)
	if identity == "" || version == "" || at.IsZero() {
		return
	}
	for {
		value, ok := s.poolPairs.Load(identity)
		pair, valid := value.(openAIGatewayPoolPair)
		if !ok || !valid || pair.version != version || (!pair.firstSent.IsZero() && !at.Before(pair.firstSent)) {
			return
		}
		next := pair
		next.firstSent = at
		if s.poolPairs.CompareAndSwap(identity, pair, next) {
			return
		}
	}
}

type gatewayPoolProbeObservation struct {
	Source              string
	Shots               int
	Full, Conclusive    bool
	DurationMS          int64
	Applied             OpenAIGatewayPoolApplied
	Identity, Model     string
	FirstSent, LastSent time.Time
	Steps               []gatewayPoolProbeStep
}

type gatewayPoolProbeTotals struct {
	Rounds       int64 `json:"rounds"`
	Requests     int64 `json:"requests"`
	Full         int64 `json:"full"`
	Degraded     int64 `json:"degraded"`
	Inconclusive int64 `json:"inconclusive"`
	DurationMS   int64 `json:"duration_ms"`
}

type gatewayPoolProbeMetrics struct {
	Foreground   gatewayPoolProbeTotals `json:"foreground"`
	Background   gatewayPoolProbeTotals `json:"background"`
	Confirmation gatewayPoolProbeTotals `json:"confirmation"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

// 只记录可观测的请求尝试、结果和耗时，不编造 token 或金额；同票单飞只记领头者。
func (s *OpenAIGatewayService) noteGatewayPoolProbe(ctx context.Context, account *Account, observation gatewayPoolProbeObservation) {
	if s == nil || s.accountRepo == nil || account == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), gatewayPoolWarmNoteTimeout)
	defer cancel()
	unlock := s.codexCookies.gatewayPoolHistoryLock(account.ID)
	defer unlock()
	fresh, err := s.accountRepo.GetByID(ctx, account.ID)
	if err != nil || fresh == nil {
		slog.Warn("gwpool_probe_metrics_read_failed", "account_id", account.ID)
		return
	}
	var metrics gatewayPoolProbeMetrics
	raw, _ := json.Marshal(fresh.Extra[openAIGatewayPoolMetricsExtraKey])
	_ = json.Unmarshal(raw, &metrics)
	total := &metrics.Foreground
	switch observation.Source {
	case "background":
		total = &metrics.Background
	case "business":
		total = &metrics.Confirmation
	}
	// 防外部导入的不合理统计溢出；计数上限远大于正常生命周期内可能产生的请求。
	const metricLimit int64 = 1_000_000_000_000
	add := func(value, delta int64) int64 {
		if value < 0 || value > metricLimit {
			value = 0
		}
		if delta < 0 {
			delta = 0
		}
		if delta > metricLimit-value {
			return metricLimit
		}
		return value + delta
	}
	total.Rounds = add(total.Rounds, 1)
	total.Requests = add(total.Requests, int64(observation.Shots))
	switch {
	case observation.Source == "business":
		// Count confirmation traffic only, not an independent quality result.
	case !observation.Conclusive:
		total.Inconclusive = add(total.Inconclusive, 1)
	case observation.Full:
		total.Full = add(total.Full, 1)
	default:
		total.Degraded = add(total.Degraded, 1)
	}
	total.DurationMS = add(total.DurationMS, observation.DurationMS)
	metrics.UpdatedAt = time.Now().UTC()
	if err := s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{openAIGatewayPoolMetricsExtraKey: metrics}); err != nil {
		slog.Warn("gwpool_probe_metrics_write_failed", "account_id", account.ID)
	}
}
