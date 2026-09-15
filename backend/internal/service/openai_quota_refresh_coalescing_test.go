package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chatgpt_account_id 只到工作区一级（handler/admin/account_codex_import.go 对同一
// account_id 下的多个成员有专门告警），而 /wham/usage 是按上游用户计的。缺
// chatgpt_user_id 时凭证命名空间退化成工作区级：同工作区的两行会互相读到对方的
// 额度，再被 persistOpenAICodexProbeSnapshot 写进各自的 extra，污染调度判断。
func TestOpenAIQuotaSharedUsageDoesNotMergeWorkspaceSiblings(t *testing.T) {
	newRow := func(id int64, upstreamUserID string) *Account {
		credentials := map[string]any{"access_token": "local-test", "chatgpt_account_id": "workspace-shared"}
		if upstreamUserID != "" {
			credentials["chatgpt_user_id"] = upstreamUserID
		}
		return &Account{ID: id, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: credentials}
	}
	run := func(t *testing.T, rows ...*Account) (int32, []string) {
		t.Helper()
		accounts := make(map[int64]*Account, len(rows))
		for _, row := range rows {
			accounts[row.ID] = row
			key := strconv.FormatInt(row.ID, 10)
			codexWireTimezoneInFlight.Delete(key)
			t.Cleanup(func() { codexWireTimezoneInFlight.Delete(key) })
		}
		var usageCalls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"user_id":"upstream-user-%d"}`, usageCalls.Add(1))
		}))
		defer server.Close()
		repo := &sparkShadowUsageTestRepo{accounts: accounts}
		quota := NewOpenAIQuotaService(repo, nil, NewOpenAITokenProvider(repo, nil, nil),
			newQuotaRedirectingFactory(server))
		seen := make([]string, 0, len(rows))
		for _, row := range rows {
			usage, err := quota.QueryUsageOnly(context.Background(), row.ID, false)
			require.NoError(t, err)
			seen = append(seen, usage.UserID)
		}
		return usageCalls.Load(), seen
	}

	t.Run("same_workspace_different_users_do_not_share", func(t *testing.T) {
		calls, seen := run(t, newRow(9700, ""), newRow(9701, ""))
		require.Equal(t, int32(2), calls, "工作区级 ID 不足以证明两行是同一个上游用户")
		require.Equal(t, []string{"upstream-user-1", "upstream-user-2"}, seen,
			"第二行必须拿到自己的额度，不是第一行的")
	})

	t.Run("same_upstream_user_still_shares", func(t *testing.T) {
		calls, seen := run(t, newRow(9702, "user-a"), newRow(9703, "user-a"))
		require.Equal(t, int32(1), calls, "同一个上游用户仍然只查一次")
		require.Equal(t, []string{"upstream-user-1", "upstream-user-1"}, seen)
	})
}

func TestOpenAIQuotaRefreshSharesCredentialSource(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "background", true: "force"}[force], func(t *testing.T) {
			parent := &Account{ID: 9500, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Credentials: map[string]any{"access_token": "local-test", "chatgpt_account_id": "test-account"}}
			shadow := &Account{ID: 9501, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				ParentAccountID: &parent.ID, QuotaDimension: QuotaDimensionSpark}
			repo := &sparkShadowUsageTestRepo{accounts: map[int64]*Account{parent.ID: parent, shadow.ID: shadow}}
			// 本例中途改 shadow.Extra，而额度查询会顺带起一个后台 goroutine 读同一个
			// Account：生产里 GetByID 每次返回新对象，测试仓库却共享指针，于是变成数据竞争。
			// 时区钩子由 TestOpenAIQuotaSharedUsageKeepsPerRowDetailsAndTimezone 覆盖，
			// 这里直接占住在途窗口挡掉它，顺带避免两个子测试互相继承包级状态。
			for _, id := range []int64{parent.ID, shadow.ID} {
				key := strconv.FormatInt(id, 10)
				codexWireTimezoneInFlight.SetDefault(key, true)
				t.Cleanup(func() { codexWireTimezoneInFlight.Delete(key) })
			}
			var usageCalls, detailsCalls, factoryCalls atomic.Int32
			started, release := make(chan struct{}, 1), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/backend-api/wham/usage" {
					usageCalls.Add(1)
					select {
					case started <- struct{}{}:
					default:
					}
					<-release
					_, _ = w.Write([]byte(`{"rate_limit":{"primary_window":{"used_percent":42,"limit_window_seconds":18000,"reset_after_seconds":3600}},
						"additional_rate_limits":[{"metered_feature":"codex_bengalfox","rate_limit":{"primary_window":{"used_percent":79,"limit_window_seconds":18000,"reset_after_seconds":3600}}}]}`))
				} else {
					detailsCalls.Add(1)
					_, _ = w.Write([]byte(`{"available_count":0,"credits":[]}`))
				}
			}))
			defer func() { unblock(); server.Close() }()
			redirect := newQuotaRedirectingFactory(server)
			quota := NewOpenAIQuotaService(repo, nil, NewOpenAITokenProvider(repo, nil, nil), func(proxyURL string) (*req.Client, error) {
				factoryCalls.Add(1)
				return redirect(proxyURL)
			})
			service := &AccountUsageService{accountRepo: repo, openAIQuotaService: quota, cache: &UsageCache{}}
			type answer struct {
				usage *UsageInfo
				err   error
			}
			first, second := make(chan answer, 1), make(chan answer, 1)
			go func() { u, err := service.getOpenAIUsage(context.Background(), parent, force); first <- answer{u, err} }()
			<-started
			go func() {
				u, err := service.getOpenAIUsage(context.Background(), shadow, force)
				second <- answer{u, err}
			}()
			require.Eventually(t, func() bool { return factoryCalls.Load() == 2 }, time.Second, time.Millisecond)
			// Both callers have prepared their local snapshot; leave the shared
			// request in flight until the second caller has joined it.
			time.Sleep(30 * time.Millisecond)
			unblock()
			parentResult, shadowResult := <-first, <-second
			require.NoError(t, parentResult.err)
			require.NoError(t, shadowResult.err)
			require.NotNil(t, parentResult.usage.FiveHour)
			require.NotNil(t, shadowResult.usage.FiveHour)
			require.Equal(t, float64(42), parentResult.usage.FiveHour.Utilization)
			require.Equal(t, float64(79), shadowResult.usage.FiveHour.Utilization)
			require.Equal(t, int32(1), usageCalls.Load(), "one credential source must have one in-flight usage request")
			require.Zero(t, detailsCalls.Load(), "background window refresh must not fetch reset credits")

			// A stale shadow snapshot must still receive the cached full payload,
			// not be skipped because the parent already consumed the deadline.
			shadow.Extra = nil
			usage, err := service.getOpenAIUsage(context.Background(), shadow, false)
			require.NoError(t, err)
			require.Equal(t, float64(79), usage.FiveHour.Utilization)
			require.Equal(t, int32(1), usageCalls.Load())
			_, err = service.getOpenAIUsage(context.Background(), parent, true)
			require.NoError(t, err)
			require.Equal(t, int32(2), usageCalls.Load(), "force bypasses TTL, not in-flight coalescing")
		})
	}
}

func TestOpenAIQuotaSharedUsageKeepsPerRowDetailsAndTimezone(t *testing.T) {
	parent := &Account{ID: 9600, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{"access_token": "local-test", "chatgpt_account_id": "test-account"},
		Extra:       map[string]any{codexAccountUserAgentExtraKey: "codex-tui/0.153.4 (Windows 10.0.26200; x86_64) WindowsTerminal"}}
	shadow := &Account{ID: 9601, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parent.ID,
		Extra: map[string]any{codexAccountUserAgentExtraKey: "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466"}}
	for _, id := range []int64{parent.ID, shadow.ID} {
		key := strconv.FormatInt(id, 10)
		codexWireTimezoneInFlight.Delete(key)
		t.Cleanup(func() { codexWireTimezoneInFlight.Delete(key) })
	}
	repo := &sparkShadowUsageTestRepo{accounts: map[int64]*Account{parent.ID: parent, shadow.ID: shadow}}
	var factoryCalls atomic.Int32
	started, release, agents := make(chan struct{}, 1), make(chan struct{}), make(chan string, 2)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/backend-api/wham/usage" {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			_, _ = w.Write([]byte(`{"rate_limit":{"allowed":true}}`))
		} else {
			agents <- r.UserAgent()
			_, _ = w.Write([]byte(`{"available_count":0,"credits":[]}`))
		}
	}))
	defer func() { unblock(); server.Close() }()
	redirect := newQuotaRedirectingFactory(server)
	quota := NewOpenAIQuotaService(repo, nil, NewOpenAITokenProvider(repo, nil, nil), func(p string) (*req.Client, error) {
		factoryCalls.Add(1)
		return redirect(p)
	})
	done := make(chan error, 2)
	go func() { _, err := quota.QueryUsage(context.Background(), parent.ID); done <- err }()
	<-started
	go func() { _, err := quota.QueryUsage(context.Background(), shadow.ID); done <- err }()
	require.Eventually(t, func() bool { return factoryCalls.Load() == 2 }, time.Second, time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	unblock()
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	assert.ElementsMatch(t, []string{
		resolveCodexOutboundIdentity(parent.GetOpenAIUserAgent()).userAgent,
		resolveCodexOutboundIdentity(shadow.GetOpenAIUserAgent()).userAgent,
	}, []string{<-agents, <-agents}, "only usage data is shared; per-row credit request identity is not")
	for _, id := range []int64{parent.ID, shadow.ID} {
		_, scheduled := codexWireTimezoneInFlight.Get(strconv.FormatInt(id, 10))
		assert.True(t, scheduled, "timezone hook must run for forwarded row %d, including flight followers", id)
	}
}
