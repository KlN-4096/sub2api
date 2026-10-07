//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// x-openai-internal-codex-residency 只在企业托管策略 enforce_residency = "us" 下出现
// （codex-rs login/src/auth/default_client.rs:341-349 读进程级 REQUIREMENTS_RESIDENCY；
// config/src/config_requirements.rs:1418 的枚举只有 Us 一个取值）。个人 Pro 账号的
// Codex 从不发它。
//
// 三条出站路径都不该放行它：客户端侧带进来要剥掉（下游可能是托管安装），我们也不能
// 主动补——给个人账号加一个企业标记，比原样不发是更强的异常信号，与"看起来像一个普通
// 美国用户"的目标相反。这条测试锁死现状，防止它被顺手加进某张白名单。
const codexResidencyHeader = "x-openai-internal-codex-residency"

func TestCodexResidencyHeaderNeverReachesUpstream(t *testing.T) {
	t.Run("http_map", func(t *testing.T) {
		body := wireProfileTestBody(t)
		c := newConvTestContext(t, body)
		c.Request.Header.Set(codexResidencyHeader, "us")
		svc, up := wireProfileTestService()

		_, _ = svc.Forward(context.Background(), c, wireProfileTestAccount(true), body)

		require.NotNil(t, up.lastReq)
		requireNoResidency(t, up.lastReq.Header)
		// 正向对照：白名单内的头确实到达了上游。少了它，常量名一旦写错，下面几条
		// 否定断言会全绿却什么都没证明。
		require.NotEmpty(t, up.lastReq.Header.Get("x-codex-window-id"),
			"前置：入站头确实经白名单到达上游")
	})

	t.Run("http_raw", func(t *testing.T) {
		body := wireProfileTestBody(t)
		c := newConvTestContext(t, body)
		c.Request.Header.Set(codexResidencyHeader, "us")
		account := wireProfileTestAccount(true)
		account.Extra["openai_passthrough"] = true
		svc, up := wireProfileTestService()

		_, _ = svc.Forward(context.Background(), c, account, body)

		require.NotNil(t, up.lastReq)
		requireNoResidency(t, up.lastReq.Header)
	})

	t.Run("websocket", func(t *testing.T) {
		body := wireProfileTestBody(t)
		c := newConvTestContext(t, body)
		c.Request.Header.Set(codexResidencyHeader, "us")
		svc, _ := wireProfileTestService()

		headers, _, err := svc.buildOpenAIWSHeaders(context.Background(), c, wireProfileTestAccount(true), "tok",
			OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2},
			true, "", convTestTurnMetadata(), convTestSession, "", "")

		require.NoError(t, err)
		requireNoResidency(t, headers)
	})

	// 两张 HTTP 白名单是 HTTP 两条路径剥掉它的唯一原因，加进去就静默回归。
	// WS 不看这两张表，它有一份写死的内联名单（openai_ws_forwarder_payload.go:113-126），
	// 上面的 websocket 子用例覆盖的是那条。
	t.Run("不在任何 HTTP 白名单里", func(t *testing.T) {
		require.False(t, openaiAllowedHeaders[codexResidencyHeader])
		require.False(t, openaiPassthroughAllowedHeaders[codexResidencyHeader])
	})
}

func requireNoResidency(t *testing.T, headers http.Header) {
	t.Helper()
	require.Empty(t, headers.Values(codexResidencyHeader),
		"个人账号出站不得携带企业 residency 标记")
}
