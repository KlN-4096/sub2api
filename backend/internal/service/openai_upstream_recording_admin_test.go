//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIRecordingAdminConsentValidationAndClone(t *testing.T) {
	for _, platform := range []string{PlatformOpenAI, PlatformAnthropic} {
		for _, value := range []any{true, false, "true", 1, nil} {
			err := validateOpenAIUpstreamRecordingExtra(platform, map[string]any{openAIUpstreamRecordingExtraKey: value})
			_, isBool := value.(bool)
			require.Equal(t, platform == PlatformOpenAI && isBool, err == nil)
		}
	}
	extra, err := duplicateAccountExtra(map[string]any{openAIUpstreamRecordingExtraKey: true, "keep": "value"})
	require.NoError(t, err)
	require.NotContains(t, extra, openAIUpstreamRecordingExtraKey)
	require.Equal(t, "value", extra["keep"])
}

func TestOpenAIRecordingAdminCreateDefaultsOff(t *testing.T) {
	repo := &longContextBillingRepoStub{}
	svc := &adminServiceImpl{accountRepo: repo}
	account, err := svc.CreateAccount(context.Background(), &CreateAccountInput{
		Name: "default-off", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "offline-test"}, SkipDefaultGroupBind: true,
	})
	require.NoError(t, err)
	require.False(t, openAIUpstreamRecordingEnabled(account))
}

func TestOpenAIRecordingAdminUpdatePreservesOmittedAndHonorsFalse(t *testing.T) {
	repo := &longContextBillingRepoStub{account: &Account{
		ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Extra: map[string]any{openAIUpstreamRecordingExtraKey: true},
	}}
	svc := &adminServiceImpl{accountRepo: repo}
	account, err := svc.UpdateAccount(context.Background(), 1, &UpdateAccountInput{Extra: map[string]any{}})
	require.NoError(t, err)
	require.True(t, openAIUpstreamRecordingEnabled(account))
	account, err = svc.UpdateAccount(context.Background(), 1, &UpdateAccountInput{Extra: map[string]any{openAIUpstreamRecordingExtraKey: false}})
	require.NoError(t, err)
	require.False(t, openAIUpstreamRecordingEnabled(account))
}

func TestOpenAIRecordingAdminRejectsInvalidWritesBeforeMutation(t *testing.T) {
	repo := &longContextBillingRepoStub{account: &Account{ID: 1, Platform: PlatformOpenAI}}
	svc := &adminServiceImpl{accountRepo: repo}
	_, err := svc.CreateAccount(context.Background(), &CreateAccountInput{
		Platform: PlatformOpenAI, Extra: map[string]any{openAIUpstreamRecordingExtraKey: "true"},
	})
	require.Error(t, err)
	require.Nil(t, repo.createdAccount)
	err = svc.UpdateAccountExtra(context.Background(), 1, map[string]any{openAIUpstreamRecordingExtraKey: "true"})
	require.Error(t, err)
	require.Zero(t, repo.updateExtraCalls)
	repo.accounts = []*Account{repo.account, {ID: 2, Platform: PlatformAnthropic}}
	_, err = svc.BulkUpdateAccounts(context.Background(), &BulkUpdateAccountsInput{
		AccountIDs: []int64{1, 2}, Extra: map[string]any{openAIUpstreamRecordingExtraKey: true},
	})
	require.Error(t, err)
	require.Zero(t, repo.bulkUpdateCalls)
}
