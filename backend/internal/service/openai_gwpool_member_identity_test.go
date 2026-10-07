package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func gatewayPoolMemberFixture(t *testing.T, id int64, user string) *Account {
	t.Helper()
	account := gwpoolTestAccount(id)
	account.Extra[OpenAIGatewayPoolConsumerKeyExtraKey] = "offline-consumer"
	account.Credentials["chatgpt_account_id"] = "team-space"
	account.Credentials["chatgpt_user_id"] = user
	payload, err := json.Marshal(map[string]any{
		"aud": "string-audience-is-valid",
		"https://api.openai.com/auth": map[string]string{
			"chatgpt_account_id": "team-space", "chatgpt_user_id": user,
		},
	})
	require.NoError(t, err)
	account.Credentials["access_token"] = "offline." + base64.RawURLEncoding.EncodeToString(payload) + ".offline"
	return account
}

func TestGatewayPoolMemberIsolationOptInPreservesProtocolAndClones(t *testing.T) {
	first := gatewayPoolMemberFixture(t, 1, "user-one")
	clone := gatewayPoolMemberFixture(t, 2, "user-one")
	second := gatewayPoolMemberFixture(t, 3, "user-two")
	protocol := codexAccountIdentityNamespace(first)
	require.Equal(t, gatewayPoolLedgerIdentity(openAIGatewayPoolAccountKey(first)),
		gatewayPoolLedgerIdentity(openAIGatewayPoolAccountKey(second)), "off is the existing policy")
	for _, account := range []*Account{first, clone, second} {
		account.Extra[openAIGatewayPoolMemberIsolationKey] = true
	}
	one, two := openAIGatewayPoolAccountKey(first), openAIGatewayPoolAccountKey(second)
	require.Equal(t, one, openAIGatewayPoolAccountKey(clone))
	require.NotEqual(t, gatewayPoolLedgerIdentity(one), gatewayPoolLedgerIdentity(two))
	require.Equal(t, "team-space/user-one", gatewayPoolUpstreamAccountID(one))
	require.Equal(t, protocol, codexAccountIdentityNamespace(first), "only the pool partition changes")
	require.NotEqual(t, gatewayPoolAccountTag(first, one), gatewayPoolAccountTag(second, two))
	store := &openAICodexCookieStore{}
	store.gatewayPoolMarkUsed(one, "unified-142")
	require.True(t, store.gatewayPoolUsedRecently(openAIGatewayPoolAccountKey(clone), "unified-142", time.Hour))
	require.False(t, store.gatewayPoolUsedRecently(two, "unified-142", time.Hour))
}

func TestGatewayPoolMemberIdentityMalformedOrMismatchedTokenFallsBack(t *testing.T) {
	account := gatewayPoolMemberFixture(t, 1, "user-one")
	account.Extra[openAIGatewayPoolMemberIsolationKey] = true
	old := openAIGatewayPoolAccountKeyLegacy(account)
	for _, token := range []string{"opaque", "a.!.b", "a.e30.b"} {
		account.Credentials["access_token"] = token
		require.Equal(t, old, openAIGatewayPoolAccountKey(account))
	}
	account = gatewayPoolMemberFixture(t, 1, "user-one")
	account.Extra[openAIGatewayPoolMemberIsolationKey] = true
	account.Credentials["chatgpt_account_id"] = "another-space"
	require.Equal(t, openAIGatewayPoolAccountKeyLegacy(account), openAIGatewayPoolAccountKey(account))
}

func TestGatewayPoolHistoryPartitionToggleRestoresOnlyMatchingView(t *testing.T) {
	at := time.Now().UTC()
	original := openAIGatewayHistory{LedgerTag: "old", Current: "unified-1",
		Seen: map[string]openAIGatewaySeen{"unified-1": {At: at}}}
	member := gatewayPoolHistoryForTag(original, "member")
	require.Empty(t, member.Seen)
	require.Equal(t, "old", member.Previous.LedgerTag)
	member.Seen = map[string]openAIGatewaySeen{"unified-2": {At: at}}
	member.LedgerTag = "member"
	restored := gatewayPoolHistoryForTag(member, "old")
	require.Equal(t, original.Seen, restored.Seen)
	require.NotContains(t, restored.Seen, "unified-2")
	require.Nil(t, restored.Previous.Previous, "do not grow an unbounded nested archive")
}

func TestGatewayPoolMemberSwitchRejectsLateOldHistory(t *testing.T) {
	account := gatewayPoolMemberFixture(t, 1, "user-one")
	oldTag := gatewayPoolLedgerTag(openAIGatewayPoolAccountKey(account))
	svc, repo := gatewayRuntimeService(account)
	require.NoError(t, repo.UpdateExtra(context.Background(), 1, map[string]any{openAIGatewayPoolMemberIsolationKey: true}))
	svc.noteOpenAIGatewayUse(context.Background(), account, "g", "", openAIGatewayVerdictFull, true, 0, 0, 0, oldTag)
	fresh, _ := repo.GetByID(context.Background(), 1)
	require.Nil(t, fresh.Extra[openAIGatewayHistoryExtraKey])
}

func TestGatewayPoolMemberHistoryFailsClosedWhenIdentityCannotBeRead(t *testing.T) {
	account := gatewayPoolMemberFixture(t, 1, "user-one")
	svc, repo := gatewayRuntimeService(account)
	svc.codexCookies.identity = func(context.Context, *Account) (string, error) {
		return "", errors.New("credential source unavailable")
	}
	svc.noteOpenAIGatewayUse(context.Background(), account, "g", "", openAIGatewayVerdictFull, true, 0, 0, 0, "old")
	fresh, _ := repo.GetByID(context.Background(), 1)
	require.Nil(t, fresh.Extra[openAIGatewayHistoryExtraKey])
}
