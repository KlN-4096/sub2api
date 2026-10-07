package service

import (
	"sync"
	"time"
)

const gatewayPoolProgressRetention = time.Minute

type GatewayPoolProgress struct {
	Phase          string    `json:"phase"`
	Attempt        int       `json:"attempt"`
	Limit          int       `json:"limit"`
	Rejected       int       `json:"rejected"`
	Gateway        string    `json:"gateway,omitempty"`
	StartedAt      time.Time `json:"started_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	ElapsedMS      int64     `json:"elapsed_ms"`
	ActiveRequests int       `json:"active_requests"`
}

type gatewayPoolProgressRun struct {
	progress GatewayPoolProgress
	done     bool
}

type gatewayPoolProgressTracker struct {
	mu   sync.Mutex
	runs map[int64][]*gatewayPoolProgressRun
}

func (p *gatewayPoolProgressTracker) start(account int64, limit int) *gatewayPoolProgressRun {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.runs == nil {
		p.runs = map[int64][]*gatewayPoolProgressRun{}
	}
	now := time.Now()
	run := &gatewayPoolProgressRun{progress: GatewayPoolProgress{Phase: "fetching", Limit: limit, StartedAt: now, UpdatedAt: now}}
	var active []*gatewayPoolProgressRun
	for _, prev := range p.runs[account] {
		if !prev.done {
			active = append(active, prev)
		}
	}
	p.runs[account] = append(active, run)
	return run
}

func (p *gatewayPoolProgressTracker) update(run *gatewayPoolProgressRun, phase string, attempt int, gateway string, rejected bool, done bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if run.done {
		return
	}
	run.progress.Phase, run.progress.UpdatedAt = phase, time.Now()
	if attempt > 0 {
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

func (p *gatewayPoolProgressTracker) snapshot(ids []int64, now time.Time) map[int64]GatewayPoolProgress {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[int64]GatewayPoolProgress{}
	for _, id := range ids {
		var chosen *gatewayPoolProgressRun
		active := 0
		var retained []*gatewayPoolProgressRun
		for _, run := range p.runs[id] {
			if run.done && now.Sub(run.progress.UpdatedAt) > gatewayPoolProgressRetention {
				continue
			}
			retained = append(retained, run)
			if !run.done {
				active++
			}
			if chosen == nil || (chosen.done && !run.done) ||
				(chosen.done == run.done && run.progress.StartedAt.After(chosen.progress.StartedAt)) {
				chosen = run
			}
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
