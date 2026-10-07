package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGatewayPoolLateVerificationCannotReplaceCurrentProof(t *testing.T) {
	store := &openAICodexCookieStore{}
	store.poolPairs.Store("identity", openAIGatewayPoolPair{
		cookie: "cookie", version: "new", until: time.Now().Add(time.Minute),
	})
	store.gatewayPoolMarkVerifiedFull("identity", "new")
	store.gatewayPoolMarkVerifiedFull("identity", "old")
	mark, ok := store.gatewayPoolVerifiedMarkOf("identity")
	require.True(t, ok)
	require.Equal(t, "new", mark.version)
}

func TestGatewayPoolVerifiedTicketIsSharedAcrossModelsAndKeepsFirstTime(t *testing.T) {
	store := &openAICodexCookieStore{}
	store.poolPairs.Store("identity", openAIGatewayPoolPair{
		cookie: "cookie", version: "v", until: time.Now().Add(time.Minute),
	})
	store.gatewayPoolMarkVerifiedFull("identity", "v", "gpt-6-luna")
	before, _ := store.gatewayPoolVerifiedMarkOf("identity")
	require.True(t, store.gatewayPoolVerifiedFullFor("identity", "gpt-6-luna"))
	require.True(t, store.gatewayPoolVerifiedFullFor("identity", "gpt-6-astra"))
	require.NotContains(t, *before.models, "gpt-6-astra", "shared eligibility does not invent an actual probe")
	store.gatewayPoolMarkVerifiedFull("identity", "v", "gpt-6-astra")
	after, _ := store.gatewayPoolVerifiedMarkOf("identity")
	require.True(t, store.gatewayPoolVerifiedFullFor("identity", "gpt-6-astra"))
	require.True(t, store.gatewayPoolVerifiedFullFor("identity", "gpt-6-luna"))
	require.Equal(t, before.at, after.at)
	require.Len(t, *before.models, 1, "published proof maps must remain immutable")
}
