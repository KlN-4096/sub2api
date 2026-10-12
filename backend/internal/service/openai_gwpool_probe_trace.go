package service

import (
	"context"
	"time"
)

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
	Applied             OpenAIGatewayPoolApplied
	Identity, Model     string
	FirstSent, LastSent time.Time
	Steps               []gatewayPoolProbeStep
}
