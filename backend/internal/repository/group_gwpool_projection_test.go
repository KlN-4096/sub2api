package repository

import (
	"context"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	_ "github.com/Wei-Shaw/sub2api/ent/runtime"
	"github.com/stretchr/testify/require"
)

func TestGroupGatewayPoolLimitSurvivesActualAuthProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery(`FROM "api_keys"`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "group_id", "status"}).AddRow(1, 2, 7, "active"))
	mock.ExpectQuery(`FROM "users"`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "status"}).AddRow(2, "active"))
	mock.ExpectQuery(`JOIN "user_allowed_groups"`).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "group_id"}))
	mock.ExpectQuery(`SELECT .*"groups"."openai_gwpool_active_accounts".*FROM "groups"`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "platform", "status", "openai_gwpool_active_accounts"}).
			AddRow(7, "openai", "active", 3))
	repo := NewAPIKeyRepository(client, db)
	key, err := repo.GetByKeyForAuth(context.Background(), "offline-key")
	require.NoError(t, err)
	require.NotNil(t, key.Group)
	require.Equal(t, 3, key.Group.OpenAIGatewayPoolActiveAccounts)
	require.NoError(t, mock.ExpectationsWereMet())
}
