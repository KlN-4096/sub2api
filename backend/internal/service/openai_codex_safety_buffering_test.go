package service

import (
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestRelayOpenAICodexSafetyBufferingHeaders_RelaysAndClears(t *testing.T) {
	dst := http.Header{}
	src := http.Header{}
	src.Set("X-Codex-Safety-Buffering-Enabled", "true")
	src.Set("X-Codex-Safety-Buffering-Faster-Model", "gpt-5.6-luna")

	relayOpenAICodexSafetyBufferingHeaders(dst, src)
	require.Equal(t, "true", dst.Get("X-Codex-Safety-Buffering-Enabled"))
	require.Equal(t, "gpt-5.6-luna", dst.Get("X-Codex-Safety-Buffering-Faster-Model"))

	// 上游缺失时清除残留（failover 换号防串扰）
	relayOpenAICodexSafetyBufferingHeaders(dst, http.Header{"Content-Type": []string{"text/event-stream"}})
	require.Empty(t, dst.Get("X-Codex-Safety-Buffering-Enabled"))
	require.Empty(t, dst.Get("X-Codex-Safety-Buffering-Faster-Model"))

	// 手工构造的非规范键也认
	relayOpenAICodexSafetyBufferingHeaders(dst, http.Header{"x-codex-safety-buffering-enabled": []string{"false"}})
	require.Equal(t, "false", dst.Get("X-Codex-Safety-Buffering-Enabled"))
}

func TestStageOpenAICodexSafetyBufferingHeaders(t *testing.T) {
	var staged http.Header
	stageOpenAICodexSafetyBufferingHeaders(&staged, http.Header{})
	require.Nil(t, staged, "上游没带头时不应凭空建出暂存集合")

	src := http.Header{}
	src.Set("X-Codex-Safety-Buffering-Enabled", "true")
	src.Set("X-Codex-Safety-Buffering-Faster-Model", "gpt-6-luna")
	stageOpenAICodexSafetyBufferingHeaders(&staged, src)
	require.Equal(t, "true", staged.Get("X-Codex-Safety-Buffering-Enabled"))
	require.Equal(t, "gpt-6-luna", staged.Get("X-Codex-Safety-Buffering-Faster-Model"))

	stageOpenAICodexSafetyBufferingHeaders(&staged, http.Header{})
	require.Empty(t, staged.Get("X-Codex-Safety-Buffering-Enabled"))
	require.Empty(t, staged.Get("X-Codex-Safety-Buffering-Faster-Model"))
}

func TestUsageCodexSafetyBufferingPtrs(t *testing.T) {
	require.Nil(t, usageCodexSafetyBufferingEnabledPtr(nil))
	require.Nil(t, usageCodexSafetyBufferingFasterModelPtr(nil))
	require.Nil(t, usageCodexSafetyBufferingEnabledPtr(http.Header{}))

	h := http.Header{}
	h.Set("X-Codex-Safety-Buffering-Enabled", " true ")
	h.Set("X-Codex-Safety-Buffering-Faster-Model", " gpt-5.6-luna ")
	enabled := usageCodexSafetyBufferingEnabledPtr(h)
	require.NotNil(t, enabled)
	require.True(t, *enabled)
	faster := usageCodexSafetyBufferingFasterModelPtr(h)
	require.NotNil(t, faster)
	require.Equal(t, "gpt-5.6-luna", *faster)

	h.Set("X-Codex-Safety-Buffering-Enabled", "maybe")
	require.Nil(t, usageCodexSafetyBufferingEnabledPtr(h), "非布尔字面量按缺失处理")
	h.Set("X-Codex-Safety-Buffering-Faster-Model", "   ")
	require.Nil(t, usageCodexSafetyBufferingFasterModelPtr(h))

	// 截断不能切在多字节字符中间（列是 TEXT，PostgreSQL 会拒绝非法 UTF-8）
	h.Set("X-Codex-Safety-Buffering-Faster-Model", strings.Repeat("a", maxUsageSafetyBufferingModelLen-1)+"é")
	faster = usageCodexSafetyBufferingFasterModelPtr(h)
	require.NotNil(t, faster)
	require.True(t, utf8.ValidString(*faster))
	require.LessOrEqual(t, len(*faster), maxUsageSafetyBufferingModelLen)
	require.Equal(t, strings.Repeat("a", maxUsageSafetyBufferingModelLen-1), *faster)

	// 短值里的非法字节同样剔掉，否则整行 INSERT 被 PostgreSQL 拒绝
	h.Set("X-Codex-Safety-Buffering-Faster-Model", "gpt-\xff-luna")
	faster = usageCodexSafetyBufferingFasterModelPtr(h)
	require.NotNil(t, faster)
	require.Equal(t, "gpt--luna", *faster)
}

func TestWriteOpenAIPassthroughResponseHeaders_RelaysSafetyBuffering(t *testing.T) {
	// 与 turn-state 一样不依赖 filter（filter=nil 走兜底分支）
	dst := http.Header{}
	src := http.Header{}
	src.Set("X-Codex-Safety-Buffering-Enabled", "true")
	src.Set("X-Codex-Safety-Buffering-Faster-Model", "gpt-5.6-luna")
	writeOpenAIPassthroughResponseHeaders(dst, src, nil)
	require.Equal(t, "true", dst.Get("X-Codex-Safety-Buffering-Enabled"))
	require.Equal(t, "gpt-5.6-luna", dst.Get("X-Codex-Safety-Buffering-Faster-Model"))

	writeOpenAIPassthroughResponseHeaders(dst, http.Header{"Content-Type": []string{"application/json"}}, nil)
	require.Empty(t, dst.Get("X-Codex-Safety-Buffering-Enabled"))
	require.Empty(t, dst.Get("X-Codex-Safety-Buffering-Faster-Model"))
}
