package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBulkUpdateAccountsGatewayPoolSparsePatchAndTargetValidation(t *testing.T) {
	a, b := gwpoolTestAccount(1), gwpoolTestAccount(2)
	a.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:8099"
	b.Extra[openAIGatewayPoolBaseURLExtraKey] = "http://127.0.0.1:8099"
	a.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "offline-one"
	b.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "offline-two"
	a.Extra[openAIGatewayPoolMetricsExtraKey] = map[string]any{"requests": 8}
	repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{1: a, 2: b}}
	svc := &adminServiceImpl{accountRepo: repo}
	result, err := svc.BulkUpdateAccounts(context.Background(), &BulkUpdateAccountsInput{
		AccountIDs: []int64{1, 2}, Extra: map[string]any{
			OpenAIGatewayPoolConsumerKeyExtraKey:    "",
			openAIGatewayPoolResumeGatewaysExtraKey: 4,
			openAIGatewayPoolProbeTimeoutExtraKey:   10,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, result.Success)
	require.Len(t, repo.bulkUpdates, 1)
	require.Equal(t, map[string]any{
		openAIGatewayPoolResumeGatewaysExtraKey: 4, openAIGatewayPoolProbeTimeoutExtraKey: 10}, repo.bulkUpdates[0].Extra)
	require.Equal(t, "offline-one", a.Extra[OpenAIGatewayPoolConsumerKeyExtraKey])
	require.Equal(t, "offline-two", b.Extra[OpenAIGatewayPoolConsumerKeyExtraKey])
	b.Type = AccountTypeAPIKey
	_, err = svc.BulkUpdateAccounts(context.Background(), &BulkUpdateAccountsInput{
		AccountIDs: []int64{1, 2}, Extra: map[string]any{OpenAIGatewayPoolContinuousWaitKey: true},
	})
	require.Error(t, err)
	require.Len(t, repo.bulkUpdates, 1, "mixed targets must not partially apply the config patch")
}

func TestGatewayPoolConfigValidatesNewNumericAndBooleanFields(t *testing.T) {
	account := gwpoolTestAccount(1)
	for _, key := range []string{openAIGatewayPoolResumeGatewaysExtraKey,
		openAIGatewayPoolFetchTimeoutExtraKey, openAIGatewayPoolListTimeoutExtraKey, openAIGatewayPoolProbeTimeoutExtraKey} {
		for _, value := range []any{-1, 0, 1.5, "3", true, 1_000_000} {
			require.True(t, touchesOpenAIGatewayPoolConfig(map[string]any{key: value}))
			require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, map[string]any{key: value}))
		}
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, map[string]any{key: nil}), "null restores default in sparse merge")
	}
	require.Error(t, validateOpenAIGatewayPoolAccountExtra(account, map[string]any{OpenAIGatewayPoolContinuousWaitKey: "false"}))
}

func TestGatewayPoolRetiredLimitsDoNotRejectUnrelatedAccountUpdates(t *testing.T) {
	fake := newGwpoolFakePool(t, "", 150)
	account := fake.account(1)
	for _, key := range []string{"openai_gwpool_rotation", "openai_gwpool_rotation_min_gateways",
		"openai_gwpool_warm_tickets", "openai_gwpool_auto_wait", "openai_gwpool_max_wait_s"} {
		account.Extra[key] = "legacy-invalid"
		require.NoError(t, validateOpenAIGatewayPoolAccountExtra(account, account.Extra), key)
		require.False(t, touchesOpenAIGatewayPoolConfig(map[string]any{key: "legacy-invalid"}), key)
	}
	account.Extra["openai_gwpool_rotation_min_gateways"] = 100
	account.Extra[openAIGatewayPoolResumeGatewaysExtraKey] = 3
	require.Equal(t, 3, account.gatewayPoolResumeGateways(), "retired stop threshold must not override local recovery count")
}
