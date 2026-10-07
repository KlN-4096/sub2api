//go:build unit

package service

import (
	"context"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestResolveUsageBillingRequestID_ForcedWebSearchBeatsClientID(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "client-shared-id")
	got := resolveUsageBillingRequestID(ctx, "web_search:uuid-1")
	require.Equal(t, "web_search:uuid-1", got)
}

func TestResolveUsageBillingRequestID_ClientWinsOverPlainUpstream(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "client-shared-id")
	got := resolveUsageBillingRequestID(ctx, "resp_abc")
	require.Equal(t, "client:client-shared-id", got)
}

func TestIsForcedUsageBillingRequestID(t *testing.T) {
	t.Parallel()
	require.True(t, isForcedUsageBillingRequestID("web_search:x"))
	require.True(t, isForcedUsageBillingRequestID("grok-video:task-1"))
	require.True(t, isForcedUsageBillingRequestID("grok_audio:up-1"))
	require.True(t, isForcedUsageBillingRequestID("grok_realtime:sess-1"))
	require.True(t, isForcedUsageBillingRequestID("gwpool_degraded:client:c-1:0"))
	require.False(t, isForcedUsageBillingRequestID("resp_abc"))
}

// 网关池判降智后被丢弃的那一发，计费幂等键**必须和重试成功那一行不同**。
//
// 同键的后果不是少记一条审计行，而是丢弃行先落库抢占 usage_billing_dedup ⇒ 成功那一发
// claim 失败、Applied=false ⇒ 余额/配额/限流/account_stats 一个都不走，usage_logs 的插入
// 也被 ON CONFLICT 吃掉 ⇒ 真实那一发彻底不计费且无日志。2026-10-02 审计用探针实证过。
func TestResolveUsageBillingRequestID_GwpoolDegradedNeverCollidesWithSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "c-1")

	// 成功那一行用的是 ctx 的 client id。
	success := resolveUsageBillingRequestID(ctx, "resp_upstream")
	require.Equal(t, "client:c-1", success)

	// 丢弃行的键走 forced 前缀，ctx 盖不掉它。
	base := "gwpool_degraded:" + resolveUsageBillingRequestID(ctx, "")
	first := resolveUsageBillingRequestID(ctx, base+":0")
	second := resolveUsageBillingRequestID(ctx, base+":1")

	require.Equal(t, base+":0", first, "丢弃行的键被 ctx 的 client id 盖掉了")
	require.NotEqual(t, success, first)
	// 同一次请求里换票重试了几发就该有几条互不相撞的行。
	require.NotEqual(t, first, second)
	// 可对账：键里带着本次客户端请求的 id。
	require.Contains(t, first, "c-1")
}

func TestStableGrokAudioBillingRequestID(t *testing.T) {
	t.Parallel()
	require.Equal(t, "grok_audio:up-1", StableGrokAudioBillingRequestID("up-1"))
	require.Equal(t, "grok_audio:up-1", StableGrokAudioBillingRequestID("grok_audio:up-1"))
	got := StableGrokAudioBillingRequestID("")
	require.True(t, strings.HasPrefix(got, "grok_audio:"))
	require.Greater(t, len(got), len("grok_audio:"))
}

func TestStableGrokRealtimeBillingRequestID(t *testing.T) {
	t.Parallel()
	require.Equal(t, "grok_realtime:s1", StableGrokRealtimeBillingRequestID("s1"))
	require.Equal(t, "grok_realtime:s1", StableGrokRealtimeBillingRequestID("grok_realtime:s1"))
	got := StableGrokRealtimeBillingRequestID("")
	require.True(t, strings.HasPrefix(got, "grok_realtime:"))
}

func TestResolveUsageBillingRequestID_ForcedGrokAudioBeatsClientID(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), ctxkey.ClientRequestID, "client-shared-id")
	got := resolveUsageBillingRequestID(ctx, StableGrokAudioBillingRequestID("up-9"))
	require.Equal(t, "grok_audio:up-9", got)
}
