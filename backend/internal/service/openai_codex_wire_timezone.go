package service

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	// 出站时区改写按 IANA 名解析，容器/精简镜像可能没有 /usr/share/zoneinfo，
	// 内嵌一份保证 time.LoadLocation 在任何部署形态下都可用。
	_ "time/tzdata"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// 真客户端把本机时区与当天日期写进请求体的 environment_context：
// core/src/session/turn_context.rs:631-639 local_time_context() =
// iana_time_zone::get_timezone() + Local::now().format("%Y-%m-%d")，
// 渲染见 core/src/context/world_state/environment.rs:290-291 的 push_optional_element
// （<current_date> / <timezone> 两个可选标签，位于 <environment_context> 内）。
//
// 客户端在国内、账号出口在美国时，上游看到的是 Asia/Shanghai 配美国 IP。双开账号按
// 出口时区改写这两个标签，其余账号字节不变。只替换标签内文本，不碰 cwd / shell 与任何
// 其它内容：工具输出里的本地时间仍会漏，这是有意的部分遮盖，不做 shell 语义翻译。
const (
	// codexWireTimezoneExtraKey 手动覆盖（IANA 名）；留空走自动解析。
	codexWireTimezoneExtraKey = "codex_wire_timezone"
	// codexWireTimezoneResolvedExtraKey 自动解析出的出口时区。
	codexWireTimezoneResolvedExtraKey = "codex_wire_timezone_resolved"
	// codexWireTimezoneResolvedAtExtraKey 自动解析时刻（RFC3339）。
	codexWireTimezoneResolvedAtExtraKey = "codex_wire_timezone_resolved_at"
	// codexWireTimezoneResolvedIPExtraKey 解析时看到的出口 IP，仅供人工排查。
	codexWireTimezoneResolvedIPExtraKey = "codex_wire_timezone_resolved_ip"
	// codexWireTimezoneResolvedProxyExtraKey 解析时使用的代理标识；变了立即重解析，
	// 这样换出口（美国 → 亚洲）当轮就能跟上，而不必等 TTL 到期。
	codexWireTimezoneResolvedProxyExtraKey = "codex_wire_timezone_resolved_proxy"
)

// codexWireTimezoneResolveTTL 自动解析的有效期：出口 IP 的归属时区不会频繁变化，
// 一天一次足够；代理变更走独立判定，不受此限制。
const codexWireTimezoneResolveTTL = 24 * time.Hour

// codexWireTimezoneNamePattern 限制可接受的 IANA 名字符集。管理员填的值会进请求体，
// 必须先挡住引号 / 反斜杠等会破坏 JSON 字符串的字节，再交给 time.LoadLocation。
var codexWireTimezoneNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+/-]{0,63}$`)

// codexWireTimezoneDateLayout 是 <current_date> 的格式
// （core/src/session/turn_context.rs:633 用 %Y-%m-%d）。
const (
	codexWireTimezoneDateLayout = "2006-01-02"
	codexEnvironmentContentKind = "environments.environment_context"
	// JSON timestamps outside the representable YYYY-MM-DD range are not evidence.
	codexEnvironmentMaxUnixSeconds = 253402300800 // 10000-01-01T00:00:00Z
)

var (
	codexEnvContextOpen        = []byte("<environment_context>")
	codexEnvContextClose       = []byte("</environment_context>")
	codexEnvTimezonePattern    = regexp.MustCompile(`<timezone>([^<]*)</timezone>`)
	codexEnvCurrentDatePattern = regexp.MustCompile(`<current_date>([^<]*)</current_date>`)
)

// codexWireTimezoneName 返回该账号出站应声明的时区名：手动覆盖优先，其次自动解析结果。
// 返回空串表示不改写（未配置、值非法、或自动解析尚未成功）。
func codexWireTimezoneName(account *Account) string {
	if account == nil {
		return ""
	}
	for _, key := range []string{codexWireTimezoneExtraKey, codexWireTimezoneResolvedExtraKey} {
		if key == codexWireTimezoneResolvedExtraKey &&
			account.GetExtraString(codexWireTimezoneResolvedProxyExtraKey) != codexWireTimezoneProxyTag(account) {
			continue // 旧出口的解析结果不能用于新出口，等下一次解析成功。
		}
		name := strings.TrimSpace(account.GetExtraString(key))
		if name == "" {
			continue
		}
		if _, err := codexWireTimezoneLocation(name); err != nil {
			continue
		}
		return name
	}
	return ""
}

// codexWireTimezoneLocation 校验并加载时区；名字不合字符集或加载失败都算非法。
func codexWireTimezoneLocation(name string) (*time.Location, error) {
	name = strings.TrimSpace(name)
	if !codexWireTimezoneNamePattern.MatchString(name) {
		return nil, errInvalidCodexWireTimezone
	}
	return time.LoadLocation(name)
}

// errInvalidCodexWireTimezone 是所有非法时区名共用的哨兵错误；调用方只判 err != nil。
var errInvalidCodexWireTimezone = errors.New("invalid codex wire timezone name")

// rewriteCodexEnvironmentTimezone 只投影明确分类的环境内容，不猜测用户文本中的 XML。
// 日期按该消息的创建时刻换算，重试、跨午夜和追加历史不能追溯改变已发送的环境。
func rewriteCodexEnvironmentTimezone(c *gin.Context, account *Account, body []byte) []byte {
	if len(body) == 0 || !codexDeviceWireProfileEnabled(c, account) {
		return body
	}
	return rewriteCodexEnvironmentTimezoneWithName(codexWireTimezoneName(account), body)
}

// e763730: context_manager/updates.rs aligns content_item_kinds with content;
// session/mod.rs stamps create_time once, preserving it on later requests.
// Missing/ambiguous evidence is a no-op. GJSON indexes retain the original byte
// offsets, so only the selected text values change; the request is never remarshaled.
func rewriteCodexEnvironmentTimezoneWithName(name string, body []byte) []byte {
	name = strings.TrimSpace(name)
	if name == "" || len(body) == 0 || !gjson.ValidBytes(body) {
		return body
	}
	loc, err := codexWireTimezoneLocation(name)
	if err != nil {
		return body
	}
	input := codexEnvironmentField(gjson.ParseBytes(body), "input")
	if !input.IsArray() {
		return body
	}
	var rewritten []byte
	offset := 0
	input.ForEach(func(_, item gjson.Result) bool {
		if codexEnvironmentField(item, "type").Str != "message" || codexEnvironmentField(item, "role").Str != "user" {
			return true
		}
		metadata := codexEnvironmentField(item, "internal_chat_message_metadata_passthrough")
		stamp := codexEnvironmentField(metadata, "create_time")
		if stamp.Type != gjson.Number || stamp.Num < 0 || stamp.Num >= codexEnvironmentMaxUnixSeconds {
			return true
		}
		content := codexEnvironmentField(item, "content")
		kinds := codexEnvironmentField(metadata, "content_item_kinds")
		if !content.IsArray() || !kinds.IsArray() {
			return true
		}
		entries, classifications := content.Array(), kinds.Array()
		if len(entries) != len(classifications) {
			return true
		}
		createdAt := time.Unix(int64(stamp.Num), 0)
		for i, entry := range entries {
			if classifications[i].Type != gjson.String || classifications[i].Str != codexEnvironmentContentKind ||
				codexEnvironmentField(entry, "type").Str != "input_text" {
				continue
			}
			text := codexEnvironmentField(entry, "text")
			if text.Type != gjson.String {
				continue
			}
			next := rewriteCodexEnvironmentText(text, name, loc, createdAt)
			if next == text.Raw {
				continue
			}
			rewritten = append(rewritten, body[offset:text.Index]...)
			rewritten = append(rewritten, next...)
			offset = text.Index + len(text.Raw)
		}
		return true
	})
	if offset == 0 {
		return body
	}
	return append(rewritten, body[offset:]...)
}

// GJSON reads the first duplicate key, while other JSON decoders may read the
// last. Conflicting classifications/timestamps are not evidence for a rewrite.
func codexEnvironmentField(object gjson.Result, name string) gjson.Result {
	if !object.IsObject() {
		return gjson.Result{}
	}
	var result gjson.Result
	found := false
	object.ForEach(func(key, value gjson.Result) bool {
		if key.Str == name {
			if found {
				result = gjson.Result{}
				return false
			}
			result, found = value, true
		}
		return true
	})
	return result
}

// Work only on a complete, single environment fragment. Each scan is bounded by
// this one string, and only the two ASCII values are replaced in its original JSON
// spelling. HTML-escaped tags and ambiguous date/timezone pairs remain untouched.
func rewriteCodexEnvironmentText(text gjson.Result, name string, loc *time.Location, createdAt time.Time) string {
	block := strings.TrimSpace(text.Str)
	if !strings.HasPrefix(block, string(codexEnvContextOpen)) ||
		!strings.HasSuffix(block, string(codexEnvContextClose)) ||
		strings.Count(block, string(codexEnvContextOpen)) != 1 ||
		strings.Count(block, string(codexEnvContextClose)) != 1 {
		return text.Raw
	}
	zones := codexEnvTimezonePattern.FindAllStringSubmatch(text.Raw, 2)
	dates := codexEnvCurrentDatePattern.FindAllStringSubmatch(text.Raw, 2)
	if len(zones) != 1 || len(dates) > 1 ||
		strings.Count(block, "<timezone>") != 1 || strings.Count(block, "</timezone>") != 1 ||
		strings.Count(block, "<current_date>") != len(dates) || strings.Count(block, "</current_date>") != len(dates) {
		return text.Raw
	}
	source, err := codexWireTimezoneLocation(zones[0][1])
	if err != nil {
		return text.Raw
	}
	if len(dates) == 1 && strings.TrimSpace(dates[0][1]) != createdAt.In(source).Format(codexWireTimezoneDateLayout) {
		return text.Raw // 不用伪造时间、用户粘贴日期或不完整的历史推算环境。
	}
	next := text.Raw
	if len(dates) == 1 {
		date := createdAt.In(loc).Format(codexWireTimezoneDateLayout)
		if len(date) != len(codexWireTimezoneDateLayout) {
			return text.Raw
		}
		next = codexEnvCurrentDatePattern.ReplaceAllString(next, "<current_date>"+date+"</current_date>")
	}
	return codexEnvTimezonePattern.ReplaceAllString(next, "<timezone>"+name+"</timezone>")
}

// codexWireTimezoneProxyURL 与转发侧取代理的表达式逐字相同
// （openai_gateway_forward.go 里的 account.ProxyID != nil && account.Proxy != nil）：
// 解析出口时必须走这条链路真正会用的代理，否则量到的是另一个出口。
func codexWireTimezoneProxyURL(account *Account) string {
	if account == nil || account.ProxyID == nil || account.Proxy == nil {
		return ""
	}
	return account.Proxy.URL()
}

// codexWireTimezoneProxyTag 是代理身份的稳定标识：换了代理行或清空代理都会变。
func codexWireTimezoneProxyTag(account *Account) string {
	if account == nil || account.ProxyID == nil {
		return "none"
	}
	// URL 包含出口配置和可能存在的代理凭据；哈希参与失效判断，不把凭据复制进账号 extra。
	sum := sha256.Sum256([]byte(codexWireTimezoneProxyURL(account)))
	return "proxy:" + strconv.FormatInt(*account.ProxyID, 10) + "|" + hex.EncodeToString(sum[:])
}

// shouldResolveCodexWireTimezone 判断是否需要向出口方向查一次时区。
// 只有双开账号才需要（其它账号不改写请求体）；手动覆盖时完全不查。
// credAccount 是影子行解析后的凭证账号：收敛开关挂在凭证账号上、device 模式挂在被转发的行上，
// 与请求时 codexDeviceWireProfileEnabled 的取值完全一致。两处用同一个谓词，影子行才不会
// 出现"请求时改写、解析时不解析"。
func shouldResolveCodexWireTimezone(account, credAccount *Account, now time.Time) bool {
	if account == nil || !codexDeviceWireProfileEnabledFor(account, credAccount) {
		return false
	}
	if strings.TrimSpace(account.GetExtraString(codexWireTimezoneExtraKey)) != "" {
		return false
	}
	if strings.TrimSpace(account.GetExtraString(codexWireTimezoneResolvedExtraKey)) == "" {
		return true
	}
	if account.GetExtraString(codexWireTimezoneResolvedProxyExtraKey) != codexWireTimezoneProxyTag(account) {
		return true // 换了出口，立刻重解析
	}
	resolvedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(account.GetExtraString(codexWireTimezoneResolvedAtExtraKey)))
	if err != nil {
		return true
	}
	return now.Sub(resolvedAt) >= codexWireTimezoneResolveTTL
}

// codexWireTimezoneExtraUpdates 组装写回 extra 的字段；时区名非法时返回 nil（保留旧值）。
func codexWireTimezoneExtraUpdates(proxyTag, exitIP, timezone string, now time.Time) map[string]any {
	timezone = strings.TrimSpace(timezone)
	if _, err := codexWireTimezoneLocation(timezone); err != nil {
		return nil
	}
	return map[string]any{
		codexWireTimezoneResolvedExtraKey:      timezone,
		codexWireTimezoneResolvedAtExtraKey:    now.UTC().Format(time.RFC3339),
		codexWireTimezoneResolvedIPExtraKey:    strings.TrimSpace(exitIP),
		codexWireTimezoneResolvedProxyExtraKey: proxyTag,
	}
}
