package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolFullUseIncludesAttemptInOneDurableMutation(t *testing.T) {
	account := gwpoolTestAccount(1)
	repo := &forkPerformanceRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	identity := openAIGatewayPoolAccountKey(account)
	at := time.Now().UTC()
	applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: "g", Version: "v"}
	svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{
		gateway: "g", version: "v", cookie: "offline", until: at.Add(time.Hour), routeExpiresAt: at.Add(time.Hour),
	})
	require.False(t, svc.noteGatewayPoolFullUse(context.Background(), account, identity, applied, at),
		"unverified sends must still use ordinary attempt accounting")
	require.Zero(t, repo.writes)
	svc.codexCookies.gatewayPoolMarkVerifiedFull(identity, "v", "gpt-6-luna")
	require.True(t, svc.noteGatewayPoolFullUse(context.Background(), account, identity, applied, at))
	require.Equal(t, 1, repo.writes)
	require.Equal(t, 1, repo.reads)
	require.Equal(t, 1, repo.scans)
	state := readGatewayPoolUsage(&repo.account, gatewayPoolLedgerTag(identity))
	require.Len(t, state.Rounds, 1)
	require.Equal(t, 1, state.Rounds[0].Attempted)
	require.Equal(t, 1, state.Rounds[0].Full)
	require.Equal(t, at, state.Rounds[0].FullStartedAt)
	require.True(t, svc.noteGatewayPoolFullUse(context.Background(), account, identity, applied, at.Add(time.Second)))
	require.Equal(t, 1, repo.writes, "checkpoint fast path still avoids duplicate writes")
	applied.Version = "old"
	require.False(t, svc.noteGatewayPoolFullUse(context.Background(), account, identity, applied, at))
}

func TestGatewayPoolBusinessContactDoesNotRetryFailedCombinedMutation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "write-failed"}[fail], func(t *testing.T) {
			account := gwpoolTestAccount(1)
			repo := &forkPerformanceRepo{gatewayRuntimeRepo: &gatewayRuntimeRepo{account: *account, fail: fail}}
			svc := &OpenAIGatewayService{accountRepo: repo}
			identity := openAIGatewayPoolAccountKey(account)
			applied := OpenAIGatewayPoolApplied{AccountID: account.ID, Gateway: "g", Version: "v"}
			svc.codexCookies.poolPairs.Store(identity, openAIGatewayPoolPair{gateway: "g", version: "v", cookie: "offline"})
			ctx, sink := withOpenAIGatewayPoolSink(context.Background(), nil)
			sink.mark(applied)
			sink.noteVerdict("g", openAIGatewayVerdictFull)
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, gwpoolTestURL, nil)
			require.NoError(t, err)
			// Empty RoundID omits unrelated contact-report accounting; the real
			// business completion entry still executes its full usage branch.
			svc.noteGatewayPoolBusinessContact(request, account, identity, applied, time.Now().UTC())
			require.Equal(t, 1, repo.writes, "do not try a second mutation on persistence failure")
			state := readGatewayPoolUsage(&repo.account, gatewayPoolLedgerTag(identity))
			if fail {
				require.Empty(t, state.Rounds)
			} else {
				require.Len(t, state.Rounds, 1)
				require.Equal(t, 1, state.Rounds[0].Attempted)
				require.Equal(t, 1, state.Rounds[0].Full)
			}
		})
	}
}
