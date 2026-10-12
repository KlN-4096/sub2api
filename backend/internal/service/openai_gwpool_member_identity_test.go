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

func openAIGatewayPoolAccountKey(account *Account) string {
	identity, _ := gatewayPoolMemberIdentity(account)
	return identity
}

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

func TestGatewayPoolMemberIsolationAlwaysPreservesProtocolAndClones(t *testing.T) {
	first := gatewayPoolMemberFixture(t, 1, "user-one")
	clone := gatewayPoolMemberFixture(t, 2, "user-one")
	second := gatewayPoolMemberFixture(t, 3, "user-two")
	protocol := codexAccountIdentityNamespace(first)
	for _, account := range []*Account{first, clone, second} {
		account.Extra["openai_gwpool_member_isolation"] = false // obsolete settings cannot join different members
	}
	one, two := openAIGatewayPoolAccountKey(first), openAIGatewayPoolAccountKey(second)
	require.Equal(t, one, openAIGatewayPoolAccountKey(clone))
	require.NotEqual(t, one, two)
	require.Equal(t, "team-space/user-one", gatewayPoolUpstreamAccountID(one))
	require.Equal(t, protocol, codexAccountIdentityNamespace(first), "only the pool partition changes")
	require.NotEqual(t, gatewayPoolAccountTag(first, one), gatewayPoolAccountTag(second, two))
	store := &openAICodexCookieStore{}
	store.gatewayPoolMarkUsed(one, "unified-142")
	require.True(t, store.gatewayPoolUsedRecently(openAIGatewayPoolAccountKey(clone), "unified-142", time.Hour))
	require.False(t, store.gatewayPoolUsedRecently(two, "unified-142", time.Hour))
}

func TestGatewayPoolMemberIdentityUsesStableMetadataAcrossTokenRefresh(t *testing.T) {
	account := gatewayPoolMemberFixture(t, 1, "user-one")
	want := "gwpool-member:team-space/user-one"
	for _, token := range []string{"opaque", "a.!.b", "a.e30.b"} {
		account.Credentials["access_token"] = token
		identity, err := (&openAICodexCookieStore{}).gatewayPoolIdentity(context.Background(), account)
		require.NoError(t, err)
		require.Equal(t, want, identity, "saved account/user metadata does not depend on bearer encoding or refresh")
	}
	fromToken := gatewayPoolMemberFixture(t, 2, "user-one")
	delete(fromToken.Credentials, "chatgpt_account_id")
	delete(fromToken.Credentials, "chatgpt_user_id")
	require.Equal(t, want, openAIGatewayPoolAccountKey(fromToken), "complete JWT claims establish the same member")
}

func TestGatewayPoolMemberIdentityRejectsMissingOrConflictingMember(t *testing.T) {
	for _, reason := range []string{"missing-user", "workspace-conflict", "member-conflict", "invalid-user"} {
		t.Run(reason, func(t *testing.T) {
			account := gatewayPoolMemberFixture(t, 1, "user-one")
			switch reason {
			case "missing-user":
				delete(account.Credentials, "chatgpt_user_id")
				account.Credentials["access_token"] = "opaque"
			case "workspace-conflict":
				account.Credentials["chatgpt_account_id"] = "other-workspace"
			case "member-conflict":
				account.Credentials["chatgpt_user_id"] = "other-member"
			case "invalid-user":
				account.Credentials["chatgpt_user_id"] = "user/another"
			}
			store := &openAICodexCookieStore{}
			identity, err := store.gatewayPoolIdentity(context.Background(), account)
			require.Error(t, err)
			require.Empty(t, identity)
			require.False(t, gatewayPoolRetryablePreparationError(err), "missing identity is not a recoverable ticket shortage")
		})
	}
}

func TestGatewayPoolHistoryTagSwitchDropsOldLedger(t *testing.T) {
	at := time.Now().UTC()
	original := openAIGatewayHistory{LedgerTag: "old", Current: "unified-1",
		Seen: map[string]openAIGatewaySeen{"unified-1": {At: at}}}
	member := gatewayPoolHistoryForTag(original, "member")
	require.Equal(t, openAIGatewayHistory{LedgerTag: "member"}, member)
	member.Seen = map[string]openAIGatewaySeen{"unified-2": {At: at}}
	require.Equal(t, openAIGatewayHistory{LedgerTag: "old"}, gatewayPoolHistoryForTag(member, "old"),
		"switching back must not resurrect an abandoned ledger")
}

func TestGatewayPoolMemberSwitchRejectsLateOldHistory(t *testing.T) {
	account := gatewayPoolMemberFixture(t, 1, "user-one")
	oldTag := gatewayPoolLedgerTag(openAIGatewayPoolAccountKey(account))
	svc, repo := gatewayRuntimeService(account)
	replacement := gatewayPoolMemberFixture(t, 1, "user-two")
	repo.mu.Lock()
	repo.account.Credentials = replacement.Credentials
	repo.mu.Unlock()
	svc.noteOpenAIGatewayUse(context.Background(), account, "g", "", openAIGatewayVerdictFull, true, 0, 0, 0, oldTag)
	fresh, _ := repo.GetByID(context.Background(), 1)
	require.Nil(t, fresh.Extra[openAIGatewayHistoryExtraKey])
}

func TestGatewayPoolMemberHistoryDoesNotAdoptUnattributedWorkspaceTouches(t *testing.T) {
	account := gatewayPoolMemberFixture(t, 1, "user-one")
	now := time.Now().UTC()
	old := openAIGatewayHistory{
		Current: "unified-142", UpdatedAt: now,
		Seen: map[string]openAIGatewaySeen{"unified-142": {At: now, Verdict: openAIGatewayVerdictFull}},
	}
	account.Extra[openAIGatewayHistoryExtraKey] = old
	identity := openAIGatewayPoolAccountKey(account)
	view := gatewayPoolHistoryForTag(old, gatewayPoolLedgerTag(identity))
	require.Equal(t, openAIGatewayHistory{LedgerTag: gatewayPoolLedgerTag(identity)}, view)
	store := &openAICodexCookieStore{}
	store.gatewayPoolHydrateUsed(account, identity)
	require.False(t, store.gatewayPoolUsedRecently(identity, "unified-142", time.Hour))
}

func TestGatewayPoolMemberSwitchStartsEmptyLedgers(t *testing.T) {
	account := gatewayPoolMemberFixture(t, 1, "user-one")
	tag := gatewayPoolLedgerTag(openAIGatewayPoolAccountKey(account))
	now := time.Now().UTC()
	account.Extra[openAIGatewayPoolContactsExtraKey] = gatewayPoolContacts{LedgerTag: "old-workspace",
		Seen: map[string]gatewayPoolContactSeen{"unified-142": {LastAt: now}}}
	account.Extra[gatewayPoolUsageExtraKey] = gatewayPoolUsageLedger{Tag: "old-workspace",
		Rounds: []GatewayPoolUsageRound{{ID: "unassigned", Attempted: 7, Full: 3}}}
	account.Extra[gatewayPoolRestStateKey] = gatewayPoolRestState{Tag: "old-workspace", Active: true, ChangedAt: now}
	contacts := readGatewayPoolContacts(account, tag)
	require.Equal(t, tag, contacts.LedgerTag)
	require.Empty(t, contacts.Seen)
	require.Empty(t, contacts.Rounds)
	raw, err := json.Marshal(contacts)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "previous")
	require.Equal(t, gatewayPoolUsageLedger{Tag: tag}, readGatewayPoolUsage(account, tag))
	require.Equal(t, gatewayPoolRestState{Tag: tag}, readGatewayPoolRest(account, tag))
}

func TestGatewayPoolLedgerWritesCarryNoPreviousArchive(t *testing.T) {
	account := gatewayPoolMemberFixture(t, 1, "user-one")
	identity := openAIGatewayPoolAccountKey(account)
	tag := gatewayPoolLedgerTag(identity)
	now := time.Now().UTC()
	account.Extra[openAIGatewayHistoryExtraKey] = openAIGatewayHistory{LedgerTag: "old-workspace", Current: "unified-9",
		Seen: map[string]openAIGatewaySeen{"unified-9": {At: now}}}
	account.Extra[openAIGatewayLedgerTagExtraKey] = "old-workspace"
	account.Extra[gatewayPoolUsageExtraKey] = gatewayPoolUsageLedger{Tag: "old-workspace", UpdatedAt: now}
	account.Extra[gatewayPoolUsageTagKey] = "old-workspace"
	svc, repo := gatewayRuntimeService(account)
	ctx := context.Background()
	svc.noteOpenAIGatewayUse(ctx, account, "unified-1", "", openAIGatewayVerdictFull, true, 0, 0, 0, tag)
	require.True(t, svc.changeGatewayPoolUsage(ctx, account, identity, func(*gatewayPoolUsageLedger) bool { return true }))
	fresh, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	raw, err := json.Marshal(fresh.Extra)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "previous")
	require.NotContains(t, string(raw), "old-workspace")
}

func TestGatewayPoolMemberWaitingRejectsCredentialSwitch(t *testing.T) {
	for _, shadow := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "shadow"}[shadow], func(t *testing.T) {
			parent := gatewayPoolMemberFixture(t, 1, "user-one")
			requestAccount := parent
			if shadow {
				requestAccount = &Account{
					ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
					Status: StatusActive, Schedulable: true, ParentAccountID: &parent.ID,
					Extra: map[string]any{openAIGatewayPoolExtraKey: true},
				}
			}
			repo := &upstreamBillingProbeAccountRepo{accounts: map[int64]*Account{
				parent.ID: parent, requestAccount.ID: requestAccount,
			}}
			svc := &OpenAIGatewayService{accountRepo: repo}
			svc.codexCookies.accountByID = repo.GetByID
			svc.codexCookies.identity = svc.codexCredentialIdentity
			ctx := svc.gatewayPoolWaitContext(context.Background(), requestAccount)
			require.NotNil(t, gatewayPoolWaitFrom(ctx))
			parent.Credentials = gatewayPoolMemberFixture(t, parent.ID, "user-two").Credentials
			_, err := svc.freshGatewayPoolPreparationAccount(ctx, requestAccount)
			require.ErrorIs(t, err, errGatewayPoolPreparationOwnerChanged)
			retry, err := svc.waitGatewayPoolRetry(ctx, requestAccount, time.Second)
			require.False(t, retry)
			require.ErrorIs(t, err, errGatewayPoolPreparationOwnerChanged)
		})
	}
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
