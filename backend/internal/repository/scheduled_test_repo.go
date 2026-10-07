package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// --- Plan Repository ---

type scheduledTestPlanRepository struct {
	db           *sql.DB
	claimMu      sync.Mutex
	activeClaims int
}

func NewScheduledTestPlanRepository(db *sql.DB) service.ScheduledTestPlanRepository {
	return &scheduledTestPlanRepository{db: db}
}

func (r *scheduledTestPlanRepository) Create(ctx context.Context, plan *service.ScheduledTestPlan) (*service.ScheduledTestPlan, error) {
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO scheduled_test_plans (account_id, model_id, cron_expression, enabled, max_results, auto_recover, next_run_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		RETURNING id, account_id, model_id, cron_expression, enabled, max_results, auto_recover, last_run_at, next_run_at, created_at, updated_at
	`, plan.AccountID, plan.ModelID, plan.CronExpression, plan.Enabled, plan.MaxResults, plan.AutoRecover, plan.NextRunAt)
	return scanPlan(row)
}

func (r *scheduledTestPlanRepository) GetByID(ctx context.Context, id int64) (*service.ScheduledTestPlan, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, account_id, model_id, cron_expression, enabled, max_results, auto_recover, last_run_at, next_run_at, created_at, updated_at
		FROM scheduled_test_plans WHERE id = $1
	`, id)
	return scanPlan(row)
}

func (r *scheduledTestPlanRepository) ListByAccountID(ctx context.Context, accountID int64) ([]*service.ScheduledTestPlan, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, account_id, model_id, cron_expression, enabled, max_results, auto_recover, last_run_at, next_run_at, created_at, updated_at
		FROM scheduled_test_plans WHERE account_id = $1
		ORDER BY created_at DESC
	`, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanPlans(rows)
}

func (r *scheduledTestPlanRepository) ListDue(ctx context.Context, now time.Time) ([]*service.ScheduledTestPlan, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, account_id, model_id, cron_expression, enabled, max_results, auto_recover, last_run_at, next_run_at, created_at, updated_at
		FROM scheduled_test_plans
		WHERE enabled = true AND next_run_at <= $1
		ORDER BY next_run_at ASC
	`, now)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanPlans(rows)
}

func (r *scheduledTestPlanRepository) Update(ctx context.Context, plan *service.ScheduledTestPlan) (*service.ScheduledTestPlan, error) {
	row := r.db.QueryRowContext(ctx, `
		UPDATE scheduled_test_plans
		SET model_id = $2, cron_expression = $3, enabled = $4, max_results = $5, auto_recover = $6, next_run_at = $7, updated_at = NOW()
		WHERE id = $1
		RETURNING id, account_id, model_id, cron_expression, enabled, max_results, auto_recover, last_run_at, next_run_at, created_at, updated_at
	`, plan.ID, plan.ModelID, plan.CronExpression, plan.Enabled, plan.MaxResults, plan.AutoRecover, plan.NextRunAt)
	return scanPlan(row)
}

func (r *scheduledTestPlanRepository) Delete(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM scheduled_test_plans WHERE id = $1`, id)
	return err
}

func (r *scheduledTestPlanRepository) MarkRunFinished(ctx context.Context, id int64, lastRunAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE scheduled_test_plans SET last_run_at = $2 WHERE id = $1
	`, id, lastRunAt)
	return err
}

type scheduledTestPlanLease struct {
	conn  *sql.Conn
	key   string
	owner *scheduledTestPlanRepository
}

const (
	scheduledTestLockReleaseTimeout    = 2 * time.Second
	scheduledTestReservedDBConnections = 1
)

// The session lock spans the test, so a slow test cannot overlap the next cron
// occurrence on another instance. The CAS advances this occurrence before any
// outbound work and rejects administrator edits, even if next_run_at is equal.
func (r *scheduledTestPlanRepository) TryClaimDue(ctx context.Context, plan *service.ScheduledTestPlan, now, nextRunAt time.Time) (out service.ScheduledTestPlanLease, claimed bool, err error) {
	if plan == nil || plan.NextRunAt == nil || !nextRunAt.After(now) {
		return nil, false, fmt.Errorf("invalid scheduled test claim")
	}
	// A lease retains a pool connection while the probe performs ordinary DB
	// work. Never let our own leases consume the entire pool and deadlock it.
	r.claimMu.Lock()
	maxOpen := r.db.Stats().MaxOpenConnections
	if maxOpen > 0 && r.activeClaims >= maxOpen-scheduledTestReservedDBConnections {
		r.claimMu.Unlock()
		if maxOpen <= scheduledTestReservedDBConnections {
			return nil, false, fmt.Errorf("scheduled tests require at least two database connections")
		}
		return nil, false, nil
	}
	r.activeClaims++
	r.claimMu.Unlock()
	defer func() {
		if !claimed {
			r.releaseClaimSlot()
		}
	}()
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	lease := &scheduledTestPlanLease{conn: conn, key: fmt.Sprintf("scheduled_test_plan:%d", plan.ID)}
	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lease.key).Scan(&acquired); err != nil {
		// The server may have acquired the lock before the result was lost.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
		return nil, false, err
	}
	if !acquired {
		return nil, false, conn.Close()
	}
	result, err := conn.ExecContext(ctx, `
		UPDATE scheduled_test_plans
		SET next_run_at = $4, updated_at = NOW()
		WHERE id = $1 AND enabled = true
			AND next_run_at = $2 AND next_run_at <= $3 AND updated_at = $5
	`, plan.ID, *plan.NextRunAt, now, nextRunAt, plan.UpdatedAt)
	if err != nil {
		return nil, false, errors.Join(err, lease.Release())
	}
	count, err := result.RowsAffected()
	if err != nil || count == 0 {
		return nil, false, errors.Join(err, lease.Release())
	}
	lease.owner = r
	return lease, true, nil
}

func (r *scheduledTestPlanRepository) releaseClaimSlot() {
	r.claimMu.Lock()
	r.activeClaims--
	r.claimMu.Unlock()
}

func (l *scheduledTestPlanLease) Release() error {
	if l == nil || l.conn == nil {
		return nil
	}
	conn := l.conn
	l.conn = nil
	if l.owner != nil {
		defer l.owner.releaseClaimSlot()
	}
	ctx, cancel := context.WithTimeout(context.Background(), scheduledTestLockReleaseTimeout)
	defer cancel()
	var released bool
	err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", l.key).Scan(&released)
	if err == nil && !released {
		err = fmt.Errorf("scheduled test lock was not held")
	}
	if err != nil {
		// Never put an ambiguously locked session back into the shared pool.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	closeErr := conn.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	return errors.Join(err, closeErr)
}

// --- Result Repository ---

type scheduledTestResultRepository struct {
	db *sql.DB
}

func NewScheduledTestResultRepository(db *sql.DB) service.ScheduledTestResultRepository {
	return &scheduledTestResultRepository{db: db}
}

func (r *scheduledTestResultRepository) Create(ctx context.Context, result *service.ScheduledTestResult) (*service.ScheduledTestResult, error) {
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO scheduled_test_results (plan_id, status, response_text, error_message, latency_ms, started_at, finished_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		RETURNING id, plan_id, status, response_text, error_message, latency_ms, started_at, finished_at, created_at
	`, result.PlanID, result.Status, result.ResponseText, result.ErrorMessage, result.LatencyMs, result.StartedAt, result.FinishedAt)

	out := &service.ScheduledTestResult{}
	if err := row.Scan(
		&out.ID, &out.PlanID, &out.Status, &out.ResponseText, &out.ErrorMessage,
		&out.LatencyMs, &out.StartedAt, &out.FinishedAt, &out.CreatedAt,
	); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *scheduledTestResultRepository) ListByPlanID(ctx context.Context, planID int64, limit int) ([]*service.ScheduledTestResult, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, plan_id, status, response_text, error_message, latency_ms, started_at, finished_at, created_at
		FROM scheduled_test_results
		WHERE plan_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`, planID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var results []*service.ScheduledTestResult
	for rows.Next() {
		r := &service.ScheduledTestResult{}
		if err := rows.Scan(
			&r.ID, &r.PlanID, &r.Status, &r.ResponseText, &r.ErrorMessage,
			&r.LatencyMs, &r.StartedAt, &r.FinishedAt, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

func (r *scheduledTestResultRepository) PruneOldResults(ctx context.Context, planID int64, keepCount int) error {
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM scheduled_test_results
		WHERE id IN (
			SELECT id FROM (
				SELECT id, ROW_NUMBER() OVER (PARTITION BY plan_id ORDER BY created_at DESC) AS rn
				FROM scheduled_test_results
				WHERE plan_id = $1
			) ranked
			WHERE rn > $2
		)
	`, planID, keepCount)
	return err
}

// --- scan helpers ---

type scannable interface {
	Scan(dest ...any) error
}

func scanPlan(row scannable) (*service.ScheduledTestPlan, error) {
	p := &service.ScheduledTestPlan{}
	if err := row.Scan(
		&p.ID, &p.AccountID, &p.ModelID, &p.CronExpression, &p.Enabled, &p.MaxResults, &p.AutoRecover,
		&p.LastRunAt, &p.NextRunAt, &p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return p, nil
}

func scanPlans(rows *sql.Rows) ([]*service.ScheduledTestPlan, error) {
	var plans []*service.ScheduledTestPlan
	for rows.Next() {
		p, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		plans = append(plans, p)
	}
	return plans, rows.Err()
}
