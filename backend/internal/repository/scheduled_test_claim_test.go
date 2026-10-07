package repository

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestScheduledTestClaimCASAndLease(t *testing.T) {
	for _, outcome := range []string{"claimed", "locked", "edited", "update_error", "lock_error", "unlock_error"} {
		t.Run(outcome, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			repo := &scheduledTestPlanRepository{db: db}
			now := time.Now().UTC().Truncate(time.Microsecond)
			due, updated := now.Add(-time.Minute), now.Add(-time.Hour)
			plan := &service.ScheduledTestPlan{ID: 7, Enabled: true, NextRunAt: &due, UpdatedAt: updated}
			lock := mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")).WithArgs("scheduled_test_plan:7")
			if outcome == "lock_error" {
				lock.WillReturnError(errors.New("lost lock reply"))
			} else {
				lock.WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(outcome != "locked"))
			}
			if outcome != "locked" && outcome != "lock_error" {
				update := mock.ExpectExec(regexp.QuoteMeta(`
					UPDATE scheduled_test_plans
					SET next_run_at = $4, updated_at = NOW()
					WHERE id = $1 AND enabled = true
						AND next_run_at = $2 AND next_run_at <= $3 AND updated_at = $5
				`)).WithArgs(plan.ID, due, now, now.Add(time.Minute), updated)
				switch outcome {
				case "edited":
					update.WillReturnResult(sqlmock.NewResult(0, 0))
				case "update_error":
					update.WillReturnError(errors.New("claim update failed"))
				default:
					update.WillReturnResult(sqlmock.NewResult(0, 1))
				}
				unlock := mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_advisory_unlock(hashtextextended($1, 0))")).WithArgs("scheduled_test_plan:7")
				if outcome == "unlock_error" {
					unlock.WillReturnError(errors.New("lost unlock reply"))
				} else {
					unlock.WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(true))
				}
			}
			lease, claimed, err := repo.TryClaimDue(context.Background(), plan, now, now.Add(time.Minute))
			switch outcome {
			case "lock_error", "update_error":
				require.Error(t, err)
				require.False(t, claimed)
				require.Nil(t, lease)
			case "locked", "edited":
				require.NoError(t, err)
				require.False(t, claimed)
				require.Nil(t, lease)
			default:
				require.NoError(t, err)
				require.True(t, claimed)
				require.Equal(t, 1, db.Stats().InUse, "lease must retain its dedicated session")
				err = lease.Release()
				if outcome == "unlock_error" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.NoError(t, lease.Release(), "release must be idempotent")
			}
			require.Zero(t, db.Stats().InUse)
			require.Zero(t, repo.activeClaims)
			if outcome == "unlock_error" || outcome == "lock_error" {
				require.Zero(t, db.Stats().Idle, "an ambiguously locked physical session must be discarded")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestScheduledTestClaimsReserveDatabaseCapacity(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(2)
	repo := &scheduledTestPlanRepository{db: db, activeClaims: 1}
	now := time.Now()
	plan := &service.ScheduledTestPlan{ID: 7, NextRunAt: &now}
	lease, claimed, err := repo.TryClaimDue(context.Background(), plan, now, now.Add(time.Minute))
	require.NoError(t, err)
	require.False(t, claimed)
	require.Nil(t, lease)
	require.Equal(t, 1, repo.activeClaims)
	db.SetMaxOpenConns(1)
	_, claimed, err = repo.TryClaimDue(context.Background(), plan, now, now.Add(time.Minute))
	require.ErrorContains(t, err, "two database connections")
	require.False(t, claimed)
	require.NoError(t, mock.ExpectationsWereMet(), "capacity exhaustion must not acquire another connection")
}

func TestScheduledTestFinishPreservesAdministratorPlan(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	finished := time.Now()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE scheduled_test_plans SET last_run_at = $2 WHERE id = $1")).
		WithArgs(int64(7), finished).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, (&scheduledTestPlanRepository{db: db}).MarkRunFinished(context.Background(), 7, finished))
	require.NoError(t, mock.ExpectationsWereMet())
}
