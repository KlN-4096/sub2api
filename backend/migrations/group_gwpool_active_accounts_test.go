package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGroupGatewayPoolMigrationStartsEveryGroupAtOne(t *testing.T) {
	raw, err := FS.ReadFile("250_add_group_gwpool_active_accounts.sql")
	require.NoError(t, err)
	sql := string(raw)
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS openai_gwpool_active_accounts INTEGER NOT NULL DEFAULT 1")
	require.Contains(t, sql, "CHECK (openai_gwpool_active_accounts BETWEEN 1 AND 64)")
	require.NotContains(t, strings.ToLower(sql), "from settings")
	require.NotContains(t, strings.ToLower(sql), "update groups", "rerunning must not reset user-edited values")
}
