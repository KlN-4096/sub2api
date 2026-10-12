package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// turn-state 形态观测只由网关维护。UpdateAccount 已经剔除并回填它；重授权（UpdateAccountExtra）
// 和批量编辑（BulkUpdateAccounts）是另外两个 extra 写入口，不剔的话一份读数会被写进别的账号。
func turnStateRuntimeExtraPayload() map[string]any {
	return map[string]any{
		"custom":                        "value",
		openAITurnStateObservedExtraKey: map[string]any{"chars": 292},
	}
}

func TestUpdateAccountExtraDropsTurnStateRuntimeKeys(t *testing.T) {
	accountID := int64(154)
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{
		accountID: {ID: accountID, Platform: PlatformOpenAI, Type: AccountTypeOAuth},
	}}

	err := (&adminServiceImpl{accountRepo: repo}).UpdateAccountExtra(context.Background(), accountID, turnStateRuntimeExtraPayload())

	require.NoError(t, err)
	require.Equal(t, "value", repo.accounts[accountID].Extra["custom"])
	require.NotContains(t, repo.accounts[accountID].Extra, openAITurnStateObservedExtraKey)
}

func TestClearOpenAITurnStateRuntimeExtraNullsObservation(t *testing.T) {
	accountID := int64(155)
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{
		accountID: {ID: accountID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
			"custom":                        "value",
			openAITurnStateObservedExtraKey: map[string]any{"chars": 292},
		}},
	}}

	require.NoError(t, (&adminServiceImpl{accountRepo: repo}).ClearOpenAITurnStateRuntimeExtra(context.Background(), accountID))

	extra := repo.accounts[accountID].Extra
	require.Equal(t, "value", extra["custom"])
	// jsonb 顶层合并：写 null 而不是删键，读侧按空处理。
	require.Nil(t, extra[openAITurnStateObservedExtraKey])
	_, ok := readOpenAITurnStateObservation(repo.accounts[accountID])
	require.False(t, ok)
}

func TestOpenAITurnStateIdentityChanged(t *testing.T) {
	oauth := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"chatgpt_account_id": "acc-a"}}
	require.False(t, OpenAITurnStateIdentityChanged(oauth, map[string]any{"chatgpt_account_id": "acc-a"}), "同一个账号续授权不清池")
	require.True(t, OpenAITurnStateIdentityChanged(oauth, map[string]any{"chatgpt_account_id": "acc-b"}))
	require.True(t, OpenAITurnStateIdentityChanged(oauth, map[string]any{}), "新凭据看不出是谁：宁可清")
	unknown := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{}}
	require.True(t, OpenAITurnStateIdentityChanged(unknown, map[string]any{"chatgpt_account_id": "acc-a"}), "旧凭据看不出是谁：宁可清")
	require.False(t, OpenAITurnStateIdentityChanged(&Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}, map[string]any{}), "非 Codex 上游不关心")
}

func TestBulkUpdateAccountsDropsTurnStateRuntimeKeys(t *testing.T) {
	repo := &upstreamBillingProbeAccountRepo{}
	svc := &adminServiceImpl{accountRepo: repo}

	result, err := svc.BulkUpdateAccounts(context.Background(), &BulkUpdateAccountsInput{
		AccountIDs: []int64{1, 2},
		Extra:      turnStateRuntimeExtraPayload(),
	})

	require.NoError(t, err)
	require.Equal(t, 2, result.Success)
	require.Len(t, repo.bulkUpdates, 1)
	require.Equal(t, "value", repo.bulkUpdates[0].Extra["custom"])
	require.NotContains(t, repo.bulkUpdates[0].Extra, openAITurnStateObservedExtraKey)
}
