//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetOpenAIUserAgentPrefersExtraOverCredentials(t *testing.T) {
	account := wireProfileTestAccount(true)
	account.Credentials["user_agent"] = "legacy-credential-ua"

	require.Equal(t, "legacy-credential-ua", account.GetOpenAIUserAgent(),
		"没有 extra 值时沿用历史的 credentials.user_agent")

	account.Extra[codexAccountUserAgentExtraKey] = "  "
	require.Equal(t, "legacy-credential-ua", account.GetOpenAIUserAgent(), "空白值不算配置")

	account.Extra[codexAccountUserAgentExtraKey] = "codex-tui/0.153.4 (Windows 10.0.26200; x86_64) WindowsTerminal (codex-tui; 0.153.4)"
	require.Equal(t,
		"codex-tui/0.153.4 (Windows 10.0.26200; x86_64) WindowsTerminal (codex-tui; 0.153.4)",
		account.GetOpenAIUserAgent(), "extra 优先")

	delete(account.Credentials, "user_agent")
	delete(account.Extra, codexAccountUserAgentExtraKey)
	require.Empty(t, account.GetOpenAIUserAgent(), "都没配置时由调用方回落到全局规范身份")

	other := wireProfileTestAccount(true)
	other.Platform = PlatformAnthropic
	other.Extra[codexAccountUserAgentExtraKey] = "should-not-leak"
	require.Empty(t, other.GetOpenAIUserAgent(), "非 OpenAI 账号不读该键")
}

// quotaUserAgentTestRepo 只回答 GetByID：buildCodexQuotaHeaders 对普通 OAuth 账号
// 不会走影子解析与 agent identity 分支。
type quotaUserAgentTestRepo struct {
	AccountRepository
	account *Account
}

func (r *quotaUserAgentTestRepo) GetByID(_ context.Context, _ int64) (*Account, error) {
	return r.account, nil
}

func TestCodexQuotaHeadersFollowAccountUserAgent(t *testing.T) {
	t.Run("account_user_agent_wins", func(t *testing.T) {
		// 管理员填的是一条历史版本的 UA：推理面会把版本段重建成生效版本
		// （resolveCodexOutboundIdentity），额度面必须走同一条解析路径，否则两面反而对不上。
		const configured = "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466 (codex-tui; 0.153.4)"
		account := wireProfileTestAccount(true)
		account.Extra[codexAccountUserAgentExtraKey] = configured
		svc := &OpenAIQuotaService{accountRepo: &quotaUserAgentTestRepo{account: account}}

		headers, _, err := svc.buildCodexQuotaHeaders(context.Background(), account.ID, "token", "acct", false)

		require.NoError(t, err)
		require.Equal(t, resolveCodexOutboundIdentity(configured).userAgent, headers["user-agent"],
			"额度面必须与推理面同一条解析路径")
		require.NotEqual(t, configured, headers["user-agent"], "版本段不能原样发管理员填的历史版本")
		require.NotContains(t, headers["user-agent"], "0.153.4")
		require.Contains(t, headers["user-agent"], "Mac OS 26.2.0", "OS / 终端指纹照旧保留")
	})

	// 遗留的 credentials.user_agent 是改动前就存在的字段，跟着它走会让一批没启用本功能的
	// 老账号在 /wham/usage 上换 UA——本轮不该有的字节变化。
	t.Run("legacy_credentials_user_agent_does_not_change_quota_plane", func(t *testing.T) {
		account := wireProfileTestAccount(false)
		account.Credentials["user_agent"] = "codex-tui/0.99.0 (Ubuntu 24.04; x86_64) tmux/3.4 (codex-tui; 0.99.0)"
		require.NotEmpty(t, account.GetOpenAIUserAgent(), "取值路径确实读得到遗留字段")
		svc := &OpenAIQuotaService{accountRepo: &quotaUserAgentTestRepo{account: account}}

		headers, _, err := svc.buildCodexQuotaHeaders(context.Background(), account.ID, "token", "acct", false)

		require.NoError(t, err)
		require.Equal(t, CodexCanonicalUserAgent(), headers["user-agent"])
	})

	// 影子行自己配了 UA 时，推理面读的是被转发的这一行；额度面若读母账号就又把两面拆开。
	t.Run("shadow_row_uses_its_own_user_agent", func(t *testing.T) {
		parent := wireProfileTestAccount(true)
		parent.ID = 9201
		parent.Extra[codexAccountUserAgentExtraKey] = "codex-tui/0.140.0 (Ubuntu 24.04; x86_64) tmux/3.4 (codex-tui; 0.140.0)"
		shadow := wireProfileTestAccount(true)
		shadow.ID = 9202
		shadow.ParentAccountID = &parent.ID
		const own = "codex-tui/0.141.0 (Mac OS 26.2.0; arm64) Apple_Terminal/466 (codex-tui; 0.141.0)"
		shadow.Extra[codexAccountUserAgentExtraKey] = own
		svc := &OpenAIQuotaService{accountRepo: &quotaUserAgentTestRepo{account: shadow}}

		headers, _, err := svc.buildCodexQuotaHeaders(context.Background(), shadow.ID, "token", "acct", false)

		require.NoError(t, err)
		require.Contains(t, headers["user-agent"], "Mac OS 26.2.0", "取被转发行自己的 UA")
		require.NotContains(t, headers["user-agent"], "Ubuntu")
	})

	// 带控制字符的 UA 会被 net/http 拒绝，该账号每一条出站请求都会以晦涩的错误失败。
	t.Run("control_characters_are_ignored", func(t *testing.T) {
		account := wireProfileTestAccount(true)
		account.Extra[codexAccountUserAgentExtraKey] = "codex-tui/0.140.0 (TestOS 12345; arm64) Terminal\r\nX-Injected: 1"
		require.Empty(t, account.GetOpenAIUserAgent(), "含控制字符的配置整体忽略")
		svc := &OpenAIQuotaService{accountRepo: &quotaUserAgentTestRepo{account: account}}
		headers, _, err := svc.buildCodexQuotaHeaders(context.Background(), account.ID, "token", "acct", false)
		require.NoError(t, err)
		require.Equal(t, CodexCanonicalUserAgent(), headers["user-agent"], "额度面也必须忽略无效配置")
		account.Credentials["user_agent"] = "legacy-credential-ua"
		require.Equal(t, "legacy-credential-ua", account.GetOpenAIUserAgent())
		headers, _, err = svc.buildCodexQuotaHeaders(context.Background(), account.ID, "token", "acct", false)
		require.NoError(t, err)
		require.Equal(t, CodexCanonicalUserAgent(), headers["user-agent"], "无效新配置不能启用遗留 UA 跟随")
	})

	t.Run("falls_back_to_canonical", func(t *testing.T) {
		account := wireProfileTestAccount(true)
		svc := &OpenAIQuotaService{accountRepo: &quotaUserAgentTestRepo{account: account}}

		headers, _, err := svc.buildCodexQuotaHeaders(context.Background(), account.ID, "token", "acct", true)

		require.NoError(t, err)
		require.Equal(t, CodexCanonicalUserAgent(), headers["user-agent"])
		require.Equal(t, "true", headers["x-openai-fedramp"])
	})
}
