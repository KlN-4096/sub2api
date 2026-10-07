package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFullWindowSamplesOnlyCountVerifiedPairs(t *testing.T) {
	store := &openAICodexCookieStore{}
	require.Zero(t, store.gatewayPoolNoteFullWindow("identity", "unverified"))
	store.poolVerified.Store("identity", gatewayPoolVerifiedMark{
		version: "verified", at: time.Now().Add(-3 * time.Minute),
	})
	require.Zero(t, store.gatewayPoolNoteFullWindow("identity", "different"))
	require.InDelta(t, 180, store.gatewayPoolNoteFullWindow("identity", "verified").Seconds(), 1)
	store.poolVerified.Store("identity", gatewayPoolVerifiedMark{
		version: "verified", at: time.Now().Add(time.Minute),
	})
	require.Zero(t, store.gatewayPoolNoteFullWindow("identity", "verified"))
}

func TestVerifiedMarkKeepsTheWindowStartForTheSameTicket(t *testing.T) {
	store := &openAICodexCookieStore{}
	before := gatewayPoolVerifiedMark{version: "first", at: time.Now().Add(-time.Minute)}
	store.poolVerified.Store(gwpoolTestIdentity, before)
	store.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "first")
	after, ok := store.gatewayPoolVerifiedMarkOf(gwpoolTestIdentity)
	require.True(t, ok)
	require.Equal(t, before, after)
	store.poolPairs.Store(gwpoolTestIdentity, openAIGatewayPoolPair{cookie: "offline", version: "next", until: time.Now().Add(time.Minute)})
	store.gatewayPoolMarkVerifiedFull(gwpoolTestIdentity, "next")
	after, _ = store.gatewayPoolVerifiedMarkOf(gwpoolTestIdentity)
	require.True(t, after.at.After(before.at))
}
