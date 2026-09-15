//go:build unit

package service

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestRewriteCodexEnvironmentTimezoneKernel(t *testing.T) {
	createdAt := wireTimezoneTestCreatedAt
	source := envBlock("2026-03-01", "Asia/Shanghai")
	message := wireTimezoneTestMessage(source, createdAt)
	body := []byte(`{"input":[` + message + `]}`)
	want := bytes.Replace(body, []byte(source),
		[]byte(envBlock("2026-02-28", "America/Los_Angeles")), 1)

	t.Run("date_comes_from_fixed_message_time", func(t *testing.T) {
		got := rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, body)
		require.Equal(t, want, got, "历史消息使用创建时刻，不使用运行本测试的今天")
		require.Equal(t, got, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, got),
			"投影应幂等")
	})

	t.Run("new_environment_does_not_rebase_history", func(t *testing.T) {
		// 上海同一天内，洛杉矶跨过午夜；新环境因 cwd 变化产生，旧块不得被回写。
		laterText := strings.Replace(source, "<current_date>", "<cwd>/changed</cwd><current_date>", 1)
		later := wireTimezoneTestMessage(laterText, createdAt.Add(6*time.Hour))
		replay := []byte(`{"input":[` + message + `,` + later + `]}`)
		got := rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, replay)
		require.Equal(t, gjson.GetBytes(want, "input.0").Raw, gjson.GetBytes(got, "input.0").Raw)
		require.Contains(t, gjson.GetBytes(got, "input.1.content.0.text").Str,
			"<current_date>2026-03-01</current_date>")
	})

	t.Run("business_text_and_unknown_bytes_are_preserved", func(t *testing.T) {
		sample := wireTimezoneTestMessage(envBlock("2033-01-01", "Etc/UTC"), createdAt)
		sample = strings.Replace(sample, "environments.environment_context", "user.text", 1)
		tool := `{"type":"function_call_output","call_id":"read-doc","output":` +
			strconv.Quote("Document example: "+envBlock("2020-01-01", "Etc/UTC")) + `}`
		raw := []byte("{\n \"huge\":900719925474099312345,\"escaped\":\"\\u4f60\\u597d<>&\", \"input\":[" +
			message + "," + sample + "," + tool + "] }")
		expected := bytes.Replace(raw, []byte(source),
			[]byte(envBlock("2026-02-28", "America/Los_Angeles")), 1)
		require.Equal(t, expected, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, raw),
			"只允许真实环境的两个字段变化，完整文档样例、工具输出和 JSON 拼写都不动")
		before := []byte(`{"input":[` + tool + "," + message + `]}`)
		expected = bytes.Replace(before, []byte(source), []byte(envBlock("2026-02-28", wireTimezoneTestExit)), 1)
		require.Equal(t, expected, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, before),
			"跳过一条工具输出不能跳过后面的真实环境")
	})

	t.Run("classification_tracks_content_index", func(t *testing.T) {
		quoted := `{"type":"input_text","text":` + strconv.Quote(source) + `}`
		raw := []byte(`{"input":[{"type":"message","role":"user","content":[` + quoted + "," + quoted +
			`],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["user.text",` +
			`"environments.environment_context"],"create_time":` + strconv.FormatInt(createdAt.Unix(), 10) + `}}]}`)
		got := rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, raw)
		require.Equal(t, source, gjson.GetBytes(got, "input.0.content.0.text").Str)
		require.Equal(t, envBlock("2026-02-28", "America/Los_Angeles"),
			gjson.GetBytes(got, "input.0.content.1.text").Str)
	})

	t.Run("unproven_metadata_is_a_noop", func(t *testing.T) {
		const meta = "input.0.internal_chat_message_metadata_passthrough."
		for _, tc := range []struct {
			name  string
			path  string
			value any
		}{
			{"missing_metadata", "input.0.internal_chat_message_metadata_passthrough", nil},
			{"wrong_kind", meta + "content_item_kinds", []string{"user.text"}},
			{"malformed_kinds", meta + "content_item_kinds", "environments.environment_context"},
			{"non_string_kind", meta + "content_item_kinds", []any{true}},
			{"unaligned_kinds", meta + "content_item_kinds", []string{"user.text", "environments.environment_context"}},
			{"missing_time", meta + "create_time", nil},
			{"string_time", meta + "create_time", strconv.FormatInt(createdAt.Unix(), 10)},
			{"negative_time", meta + "create_time", -1},
			{"overflow_time", meta + "create_time", 1e100},
			{"assistant_message", "input.0.role", "assistant"},
			{"tool_item", "input.0.type", "function_call_output"},
			{"non_text_content", "input.0.content.0.type", "input_image"},
			{"non_string_text", "input.0.content.0.text", map[string]string{"value": source}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				raw, err := sjson.SetBytes(body, tc.path, tc.value)
				require.NoError(t, err)
				require.Equal(t, raw, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, raw))
			})
		}
	})

	t.Run("duplicate_evidence_is_ambiguous", func(t *testing.T) {
		stamp := strconv.FormatInt(createdAt.Unix(), 10)
		for _, raw := range []string{
			`{"input":[` + message + `],"input":[]}`,
			strings.Replace(string(body), `"type":"message"`, `"type":"message","type":"function_call_output"`, 1),
			strings.Replace(string(body), `"create_time":`+stamp, `"create_time":`+stamp+`,"create_time":0`, 1),
			strings.Replace(string(body), `"content_item_kinds":["environments.environment_context"]`,
				`"content_item_kinds":["environments.environment_context"],"content_item_kinds":["user.text"]`, 1),
			strings.Replace(string(body), `}],"internal_chat_message_metadata_passthrough"`,
				`}],"content":[],"internal_chat_message_metadata_passthrough"`, 1),
			strings.Replace(string(body), `"text":`+strconv.Quote(source),
				`"text":`+strconv.Quote(source)+`,"text":"quoted example"`, 1),
		} {
			require.NotEqual(t, string(body), raw, "夹具必须真的增加了重复字段")
			require.True(t, gjson.Valid(raw))
			require.Equal(t, []byte(raw), rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, []byte(raw)))
		}
	})

	t.Run("invalid_time_cannot_silently_become_epoch", func(t *testing.T) {
		raw := []byte(`{"input":[` + wireTimezoneTestMessage(envBlock("1970-01-01", "Etc/UTC"), time.Unix(0, 0)) + `]}`)
		for _, stamp := range []any{nil, false, "0"} {
			invalid, err := sjson.SetBytes(raw, "input.0.internal_chat_message_metadata_passthrough.create_time", stamp)
			require.NoError(t, err)
			require.Equal(t, invalid, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, invalid))
		}
		negative := []byte(`{"input":[` + wireTimezoneTestMessage(envBlock("1969-12-31", "Etc/UTC"), time.Unix(-1, 0)) + `]}`)
		require.Equal(t, negative, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, negative))
	})

	t.Run("ambiguous_environment_is_a_noop", func(t *testing.T) {
		for _, text := range []string{
			"Document example: " + source,
			source + " is an example.",
			source + source,
			strings.Replace(source, "<environment_context>", "<environment_context><environment_context>", 1),
			strings.Replace(source, "<current_date>", "<current_date>2026-03-01</current_date><current_date>", 1),
			strings.Replace(source, "<timezone>", "<timezone>Europe/Paris</timezone><timezone>", 1),
			envBlock("0001-01-01", "Asia/Shanghai"),
			envBlock("2033-01-01", "Asia/Shanghai"),
			envBlock("2026-02-30", "Asia/Shanghai"),
			envBlock("2026-03-01", "Mars/Olympus"),
			strings.Replace(source, "<timezone>Asia/Shanghai</timezone>", "", 1),
			strings.Replace(source, "</current_date>", "", 1),
			strings.Replace(source, "<current_date>", "", 1),
			strings.Replace(source, "</timezone>", "", 1),
			strings.Replace(source, "<timezone>", "", 1),
		} {
			raw := []byte(`{"input":[` + wireTimezoneTestMessage(text, createdAt) + `]}`)
			require.Equal(t, raw, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, raw), text)
		}
	})

	t.Run("invalid_or_unrecognized_input_is_a_noop", func(t *testing.T) {
		for _, raw := range [][]byte{
			nil, []byte(source), []byte(`{"input":[`), []byte(`{"input":"hello"}`),
			[]byte(`{"input":[]}`),
			[]byte(`{"input":[{"text":"open <environment_context><timezone>"},{"text":"</timezone></environment_context>"}]}`),
			[]byte(strings.ReplaceAll(strings.ReplaceAll(string(body), "<", `\u003c`), ">", `\u003e`)),
			[]byte(strings.Replace(message, `"internal_chat_message_metadata_passthrough"`, `"other_metadata"`, 1)),
		} {
			require.Equal(t, raw, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, raw))
		}
	})

	t.Run("timezone_only_does_not_invent_a_date", func(t *testing.T) {
		text := "<environment_context><timezone>Asia/Shanghai</timezone></environment_context>"
		raw := []byte(`{"input":[` + wireTimezoneTestMessage(text, createdAt) + `]}`)
		expected := bytes.Replace(raw, []byte("Asia/Shanghai"), []byte(wireTimezoneTestExit), 1)
		require.Equal(t, expected, rewriteCodexEnvironmentTimezoneWithName(wireTimezoneTestExit, raw))
	})

	t.Run("target_name_is_validated_and_trimmed", func(t *testing.T) {
		require.Equal(t, want, rewriteCodexEnvironmentTimezoneWithName("  "+wireTimezoneTestExit+"  ", body))
		for _, name := range []string{"", `A"B`, `A\B`, "../etc/passwd", "Asia Shanghai", "亚洲/上海", "Mars/Olympus"} {
			require.Equal(t, body, rewriteCodexEnvironmentTimezoneWithName(name, body), name)
		}
	})
}

func TestCodexEnvironmentCreationTimeHandlesDateBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, created, sourceZone, sourceDate, targetZone, targetDate string
	}{
		{"before_midnight", "2026-03-01T07:59:59Z", "Asia/Shanghai", "2026-03-01", "America/Los_Angeles", "2026-02-28"},
		{"after_midnight", "2026-03-01T08:00:00Z", "Asia/Shanghai", "2026-03-01", "America/Los_Angeles", "2026-03-01"},
		{"dst_spring", "2026-03-08T10:00:00Z", "Asia/Shanghai", "2026-03-08", "America/Los_Angeles", "2026-03-08"},
		{"dst_fall", "2026-11-01T08:59:59Z", "Asia/Shanghai", "2026-11-01", "America/Los_Angeles", "2026-11-01"},
		{"two_day_difference", "2026-03-01T11:30:00Z", "Pacific/Kiritimati", "2026-03-02", "Etc/GMT+12", "2026-02-28"},
		{"year_boundary", "2026-12-31T16:00:00Z", "Asia/Shanghai", "2027-01-01", "America/Los_Angeles", "2026-12-31"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			createdAt, err := time.Parse(time.RFC3339, tc.created)
			require.NoError(t, err)
			raw := []byte(`{"input":[` + wireTimezoneTestMessage(envBlock(tc.sourceDate, tc.sourceZone), createdAt) + `]}`)
			// 原版 create_time 是带小数的 Unix 秒；小数不会被误当毫秒或纳秒。
			raw = bytes.Replace(raw, []byte(strconv.FormatInt(createdAt.Unix(), 10)+"}"),
				[]byte(strconv.FormatInt(createdAt.Unix(), 10)+".125}"), 1)
			got := rewriteCodexEnvironmentTimezoneWithName(tc.targetZone, raw)
			require.Equal(t, envBlock(tc.targetDate, tc.targetZone), gjson.GetBytes(got, "input.0.content.0.text").Str)
		})
	}
	t.Run("target_year_outside_yyyy_is_unchanged", func(t *testing.T) {
		raw := []byte(`{"input":[` + wireTimezoneTestMessage(envBlock("9999-12-31", "Etc/UTC"),
			time.Date(9999, 12, 31, 23, 0, 0, 0, time.UTC)) + `]}`)
		require.Equal(t, raw, rewriteCodexEnvironmentTimezoneWithName("Pacific/Kiritimati", raw))
	})
}

func TestCodexWireTimezoneProxyTagIsOpaqueAndTracksConfiguration(t *testing.T) {
	account := wireProfileTestAccount(true)
	id := int64(7)
	account.ProxyID = &id
	account.Proxy = &Proxy{ID: id, Protocol: "socks5", Host: "proxy.invalid", Port: 1080,
		Username: "test-user", Password: "test-password"}
	tag := codexWireTimezoneProxyTag(account)
	require.Regexp(t, `^proxy:7\|[a-f0-9]{64}$`, tag)
	require.NotContains(t, tag, account.Proxy.URL())
	require.NotContains(t, tag, account.Proxy.Password)
	require.Equal(t, tag, codexWireTimezoneProxyTag(account))

	account.Extra[codexWireTimezoneResolvedExtraKey] = "America/Los_Angeles"
	account.Extra[codexWireTimezoneResolvedProxyExtraKey] = tag
	require.Equal(t, "America/Los_Angeles", codexWireTimezoneName(account))
	account.Proxy.Port++
	require.NotEqual(t, tag, codexWireTimezoneProxyTag(account))
	require.Empty(t, codexWireTimezoneName(account), "新出口不能复用旧出口的时区")
	require.True(t, shouldResolveCodexWireTimezone(account, account, time.Now()))

	account.Extra[codexWireTimezoneExtraKey] = "Asia/Tokyo"
	require.Equal(t, "Asia/Tokyo", codexWireTimezoneName(account), "显式人工覆盖仍优先")
}
