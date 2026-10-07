package service

import (
	"strconv"
	"sync"
	"time"
)

type gatewayPoolProgressTriedKey struct{}
type gatewayPoolProgressRunKey struct{}

type GatewayPoolProgress struct {
	Runtime        *GatewayPoolRuntimeView `json:"runtime,omitempty"`
	RunID          string                  `json:"run_id"`
	Sequence       uint64                  `json:"sequence"`
	Phase          string                  `json:"phase"`
	Attempt        int                     `json:"attempt"`
	Limit          int                     `json:"limit"`
	Rejected       int                     `json:"rejected"`
	Gateway        string                  `json:"gateway,omitempty"`
	StartedAt      time.Time               `json:"started_at"`
	UpdatedAt      time.Time               `json:"updated_at"`
	ElapsedMS      int64                   `json:"elapsed_ms"`
	ActiveRequests int                     `json:"active_requests"`
}

type gatewayPoolProgressRun struct {
	progress GatewayPoolProgress
	done     bool
	tickets  map[string]struct{}
	account  int64
	order    uint64
	scope    gatewayPoolProgressScope
}

type gatewayPoolProgressScope struct {
	tag            string
	requestStarted time.Time
	sequence       uint64
	closedBefore   time.Time
	identity       string
}

type gatewayPoolProgressTracker struct {
	mu   sync.Mutex
	runs map[int64][]*gatewayPoolProgressRun
	next uint64
}

func (p *gatewayPoolProgressTracker) start(account int64, limit int, scopes ...gatewayPoolProgressScope) *gatewayPoolProgressRun {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.runs == nil {
		p.runs = map[int64][]*gatewayPoolProgressRun{}
	}
	now := time.Now()
	scope := gatewayPoolProgressScope{requestStarted: now}
	if len(scopes) > 0 {
		scope = scopes[0]
	}
	p.next++
	run := &gatewayPoolProgressRun{account: account, order: p.next, scope: scope, tickets: map[string]struct{}{}, progress: GatewayPoolProgress{
		RunID: strconv.FormatUint(p.next, 10), Sequence: scope.sequence, Phase: "fetching", Limit: limit, StartedAt: now, UpdatedAt: now,
	}}
	var active []*gatewayPoolProgressRun
	var latestDone *gatewayPoolProgressRun
	for _, prev := range p.runs[account] {
		if !prev.done {
			active = append(active, prev)
		} else if latestDone == nil || prev.scope.requestStarted.After(latestDone.scope.requestStarted) ||
			(prev.scope.requestStarted.Equal(latestDone.scope.requestStarted) && prev.order > latestDone.order) {
			latestDone = prev
		}
	}
	// A pre-block request may reach warm late. Do not let its start discard
	// the newer cycle's terminal progress before the read-side scope filter.
	if latestDone != nil {
		active = append(active, latestDone)
	}
	p.runs[account] = append(active, run)
	return run
}

func (p *gatewayPoolProgressTracker) resume(run *gatewayPoolProgressRun) {
	p.mu.Lock()
	defer p.mu.Unlock()
	run.done = false
	for _, current := range p.runs[run.account] {
		if current == run {
			return
		}
	}
	p.runs[run.account] = append(p.runs[run.account], run)
}

// Distinct tickets actually sent for verification; neither fetch failures,
// A/B request count nor unused prefetched tickets.
func (p *gatewayPoolProgressTracker) tried(run *gatewayPoolProgressRun, ticket string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ticket == "" || run == nil || run.done {
		return
	}
	if run.tickets == nil {
		run.tickets = map[string]struct{}{}
	}
	run.tickets[ticket] = struct{}{}
	run.progress.Attempt = len(run.tickets)
}

func (p *gatewayPoolProgressTracker) update(run *gatewayPoolProgressRun, phase string, attempt int, gateway string, rejected bool, done bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if run.done {
		return
	}
	run.progress.Phase, run.progress.UpdatedAt = phase, time.Now()
	if attempt > run.progress.Attempt {
		run.progress.Attempt = attempt
	}
	if gateway != "" {
		run.progress.Gateway = gateway
	}
	if rejected {
		run.progress.Rejected++
	}
	run.done = done
}

func (p *gatewayPoolProgressTracker) snapshot(ids []int64, now time.Time, filters ...map[int64]gatewayPoolProgressScope) map[int64]GatewayPoolProgress {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[int64]GatewayPoolProgress{}
	for _, id := range ids {
		var chosen *gatewayPoolProgressRun
		active := 0
		var retained []*gatewayPoolProgressRun
		for _, run := range p.runs[id] {
			if len(filters) > 0 {
				scope, ok := filters[0][id]
				if !ok || run.scope.tag != scope.tag || !run.scope.requestStarted.After(scope.closedBefore) ||
					(scope.identity != "" && run.scope.identity != "" && run.scope.identity != scope.identity) {
					continue
				}
			}
			retained = append(retained, run)
			if !run.done {
				active++
			}
			// Keep the oldest active request on screen until it exits.
			if chosen == nil || (chosen.done && !run.done) ||
				(!chosen.done && !run.done && run.order < chosen.order) ||
				(chosen.done && run.done && run.progress.UpdatedAt.After(chosen.progress.UpdatedAt)) {
				chosen = run
			}
		}
		if chosen != nil && chosen.done {
			retained = []*gatewayPoolProgressRun{chosen}
		}
		if len(retained) == 0 {
			delete(p.runs, id)
		} else {
			p.runs[id] = retained
		}
		if chosen != nil {
			progress := chosen.progress
			end := now
			if chosen.done {
				end = progress.UpdatedAt
			}
			progress.ElapsedMS = end.Sub(progress.StartedAt).Milliseconds()
			progress.ActiveRequests = active
			out[id] = progress
		}
	}
	return out
}

func (s *OpenAIGatewayService) GatewayPoolProgress(ids []int64) map[int64]GatewayPoolProgress {
	return s.codexCookies.poolProgress.snapshot(ids, time.Now())
}

func (s *adminServiceImpl) GatewayPoolProgress(ids []int64) map[int64]GatewayPoolProgress {
	if reader, ok := s.runtimeBlocker.(interface {
		GatewayPoolProgress([]int64) map[int64]GatewayPoolProgress
	}); ok {
		return reader.GatewayPoolProgress(ids)
	}
	return map[int64]GatewayPoolProgress{}
}
