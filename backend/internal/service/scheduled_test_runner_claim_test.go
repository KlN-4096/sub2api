//go:build unit

package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type scheduledClaimTestRepo struct {
	ScheduledTestPlanRepository
	mu        sync.Mutex
	deny      bool
	claimed   bool
	released  bool
	finished  bool
	nextRunAt time.Time
}

func (r *scheduledClaimTestRepo) TryClaimDue(ctx context.Context, _ *ScheduledTestPlan, _, next time.Time) (ScheduledTestPlanLease, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if r.deny || r.claimed {
		return nil, false, nil
	}
	r.claimed, r.nextRunAt = true, next
	return r, true, nil
}

func (r *scheduledClaimTestRepo) Release() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = true
	return nil
}

func (r *scheduledClaimTestRepo) MarkRunFinished(_ context.Context, _ int64, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finished = true
	return nil
}

type scheduledClaimAccountRepo struct {
	AccountRepository
	calls  atomic.Int32
	before func()
}

func (r *scheduledClaimAccountRepo) GetByID(context.Context, int64) (*Account, error) {
	if r.before != nil {
		r.before()
	}
	r.calls.Add(1)
	return nil, errors.New("offline test failure")
}

func TestScheduledTestRunnerClaimsBeforeAttempt(t *testing.T) {
	for _, deny := range []bool{false, true} {
		t.Run(map[bool]string{false: "claim_once", true: "conflict_skips"}[deny], func(t *testing.T) {
			plans := &scheduledClaimTestRepo{deny: deny}
			accounts := &scheduledClaimAccountRepo{}
			accounts.before = func() {
				require.True(t, plans.claimed, "claim must precede any account probe")
				require.True(t, plans.nextRunAt.After(time.Now()), "the next occurrence must already be advanced")
			}
			results := &scheduledTestProbeResultRepo{}
			tests := &AccountTestService{accountRepo: accounts}
			scheduled := NewScheduledTestService(plans, results)
			first := NewScheduledTestRunnerService(plans, scheduled, tests, nil, &config.Config{})
			second := NewScheduledTestRunnerService(plans, scheduled, tests, nil, &config.Config{})
			due := time.Now().Add(-time.Minute)
			plan := &ScheduledTestPlan{ID: 7, AccountID: 8, Enabled: true, NextRunAt: &due, CronExpression: "* * * * *", MaxResults: 10}
			first.runOnePlan(context.Background(), plan)
			second.runOnePlan(context.Background(), plan)
			if deny {
				require.Zero(t, accounts.calls.Load(), "failed atomic claim must cause zero probes")
			} else {
				require.Equal(t, int32(1), accounts.calls.Load(), "two runner instances must share one due occurrence")
				require.True(t, plans.claimed)
				require.True(t, plans.released, "failed probe must release its lease")
				require.True(t, plans.finished)
			}
		})
	}
}

func TestScheduledTestRunnerSkipsCanceledAndOverlappingScan(t *testing.T) {
	accounts := &scheduledClaimAccountRepo{}
	runner := NewScheduledTestRunnerService(&scheduledClaimTestRepo{}, nil, &AccountTestService{accountRepo: accounts}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	due := time.Now().Add(-time.Minute)
	runner.runOnePlan(ctx, &ScheduledTestPlan{ID: 7, Enabled: true, NextRunAt: &due, CronExpression: "* * * * *"})
	require.Zero(t, accounts.calls.Load())
	runner.running.Store(true)
	done := make(chan struct{})
	go func() { runner.runScheduled(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("overlapping scan did not return immediately")
	}
}

func TestScheduledTestRunnerSlowAttemptBlocksOtherRunner(t *testing.T) {
	plans := &scheduledClaimTestRepo{}
	entered, unblock, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	defer release()
	accounts := &scheduledClaimAccountRepo{before: func() { close(entered); <-unblock }}
	scheduled := NewScheduledTestService(plans, &scheduledTestProbeResultRepo{})
	testService := &AccountTestService{accountRepo: accounts}
	first := NewScheduledTestRunnerService(plans, scheduled, testService, nil, nil)
	second := NewScheduledTestRunnerService(plans, scheduled, testService, nil, nil)
	due := time.Now().Add(-time.Minute)
	plan := &ScheduledTestPlan{ID: 7, AccountID: 8, Enabled: true, NextRunAt: &due, CronExpression: "* * * * *", MaxResults: 10}
	go func() { first.runOnePlan(context.Background(), plan); close(finished) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first probe did not begin")
	}
	second.runOnePlan(context.Background(), plan)
	plans.mu.Lock()
	released := plans.released
	plans.mu.Unlock()
	require.False(t, released, "second runner must skip while first lease is still held")
	release()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("first probe did not finish")
	}
	second.runOnePlan(context.Background(), plan)
	require.Equal(t, int32(1), accounts.calls.Load(), "released lease must not revive an already-claimed occurrence")
	require.True(t, plans.released)
}
