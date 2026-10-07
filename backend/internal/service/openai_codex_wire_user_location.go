package service

import (
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 真客户端把本机配置的大致位置写进 web_search 的 user_location：
// Responses 体里是 tools[].user_location（codex-rs protocol/src/config_types.rs:481
// WebSearchUserLocation，序列化顺序见 tools/src/tool_spec.rs:165-176 的
// type/country/region/city/timezone），/alpha/search 体里是 settings.user_location
// （codex-api/src/search.rs:231-257 SearchSettings.user_location，同一个形状）。
// 两处都只在用户配置了位置时出现，未配置则整个字段缺席。
//
// 这个字段和 environment_context 的时区是同一类信息：客户端在国内、出口在美国时，
// 我们已经把 <timezone> 改成了出口时区，却把"上海"原样发出去，两者自相矛盾。
// 按同一份出口解析结果改写它，让位置、时区、出口 IP 三者同源。
//
// 只改客户端已经发过的字段。真客户端没配 location 时不发它，替它补一个会造出比不改写
// 更强的异常信号——一个"从不报位置的客户端"突然开始报位置。
const (
	// codexWireLocationCityExtraKey 出口所在城市，与时区同一次解析写回。
	codexWireLocationCityExtraKey = "codex_wire_location_city"
	// codexWireLocationRegionExtraKey 出口所在州/省。
	codexWireLocationRegionExtraKey = "codex_wire_location_region"
	// codexWireLocationCountryExtraKey 出口所在国家（ISO 两字母码，ipinfo 的 country 字段）。
	codexWireLocationCountryExtraKey = "codex_wire_location_country"
)

// codexWebSearchToolPrefix 是 web_search 家族的前缀。宽一度匹配带后缀的变体是防御性的：
// 上游给 hosted 工具改过名，命中多余的工具只会让它的位置也对齐到出口，不会泄漏；
// 漏掉一个变体就是泄漏。当前 codex 源码只有 tools/src/tool_spec.rs:39 的 "web_search"。
const codexWebSearchToolPrefix = "web_search"

// codexUserLocationDeleteAttempts 是同一路径重复删除的次数上限。真客户端不会发重复键，
// 这个上限只防病态输入把循环拖住。
const codexUserLocationDeleteAttempts = 8

// codexWireLocation 返回出口地理三元组。三项缺一即返回 false：残缺的位置比原样透传更矛盾。
//
// 两条失效判定：
//   - 手动覆盖了时区就不能再用地理。地理只来自自动解析，而手动覆盖那一支没有（也无法有）
//     出口校验，两者来源不同：会拼出"拉斯维加斯 + Asia/Tokyo"这种单个对象内部的矛盾，
//     且 shouldResolveCodexWireTimezone 在覆盖存在时拒绝重解析，这个状态是永久的。
//   - proxy tag 与时区共用。地理和时区是同一次查询的结果，换了出口必须一起失效，
//     否则会出现"新出口的时区 + 旧出口的城市"。
func codexWireLocation(account *Account) (city, region, country string, ok bool) {
	if account == nil {
		return "", "", "", false
	}
	if name := strings.TrimSpace(account.GetExtraString(codexWireTimezoneExtraKey)); name != "" {
		// 判定必须与 codexWireTimezoneName 逐字相同：那边非法的手动值会被跳过、落回
		// 自动解析结果。这里若只判非空，一个拼错的时区名（"America/Los_Angelas"）就会
		// 在时区仍用自动解析值的同时把同源的地理丢掉，白白删掉所有 user_location。
		if _, err := codexWireTimezoneLocation(name); err == nil {
			return "", "", "", false
		}
	}
	if account.GetExtraString(codexWireTimezoneResolvedProxyExtraKey) != codexWireTimezoneProxyTag(account) {
		return "", "", "", false
	}
	city = strings.TrimSpace(account.GetExtraString(codexWireLocationCityExtraKey))
	region = strings.TrimSpace(account.GetExtraString(codexWireLocationRegionExtraKey))
	country = strings.TrimSpace(account.GetExtraString(codexWireLocationCountryExtraKey))
	if city == "" || region == "" || country == "" {
		return "", "", "", false
	}
	return city, region, country, true
}

// rewriteCodexWebSearchUserLocation 对齐 Responses 体里 tools[].user_location。
func rewriteCodexWebSearchUserLocation(c *gin.Context, account *Account, body []byte) []byte {
	if len(body) == 0 || !codexDeviceWireProfileEnabled(c, account) {
		return body
	}
	return rewriteCodexWebSearchUserLocationWith(account, codexWireTimezoneName(account), body)
}

// rewriteCodexAlphaSearchUserLocation 对齐 /alpha/search 体里 settings.user_location。
// 那条路径的体不是 Responses 结构，没有 tools 数组，位置直接挂在 settings 下。
func rewriteCodexAlphaSearchUserLocation(c *gin.Context, account *Account, body []byte) []byte {
	if len(body) == 0 || !codexDeviceWireProfileEnabled(c, account) {
		return body
	}
	timezone := strings.TrimSpace(codexWireTimezoneName(account))
	if timezone == "" || !gjson.ValidBytes(body) {
		return body
	}
	if !codexUserLocationPresent(gjson.GetBytes(body, "settings.user_location")) {
		return body
	}
	next, ok := rewriteCodexUserLocationAt(body, "settings.user_location", account, timezone)
	if !ok {
		return body
	}
	return next
}

// rewriteCodexWebSearchUserLocationWith 是无 gin 上下文形态，供 WS 帧路径复用。
// 本函数自身不判双开开关，调用方必须先过 codexDeviceWireProfileEnabled。
//
// timezone 为空表示出口时区尚未解析成功——此时 environment_context 整条不改写，位置也
// 不能单独动，否则会出现"本机时区 + 出口城市"这种此前不存在的组合。
func rewriteCodexWebSearchUserLocationWith(account *Account, timezone string, body []byte) []byte {
	timezone = strings.TrimSpace(timezone)
	if timezone == "" || len(body) == 0 || !gjson.ValidBytes(body) {
		return body
	}
	tools := gjson.GetBytes(body, "tools")
	if !tools.IsArray() {
		return body
	}
	// 先收下标再改：gjson 的 Result 带的是改写前的字节偏移，边遍历边写会让后面的项指向
	// 失效位置。下标本身不会移位——动的是工具对象里的一个字段，不是 tools 的元素。
	//
	// 已知边界：重复的**容器**键改不到。顶层两个 tools 时 gjson 取第一个数组（为空就
	// 直接返回），而上游 serde 读最后一个；alpha 路径的 settings 同理——gjson 的
	// settings.user_location 会穿透到后一个 settings，sjson 的删除却定位在前一个上，
	// 删除循环空转到上限后整条放弃。sjson 的路径语法无法定位重复键的后一个，而按路径
	// 删到两个容器都消失会毁掉请求，因此这两处都不处理，只记下来。重复的**叶子**键
	// （user_location / type）是处理了的，见 rewriteCodexUserLocationAt 与
	// codexToolCarriesWebSearchLocation。
	indexes := make([]int, 0, 4)
	tools.ForEach(func(key, tool gjson.Result) bool {
		if codexToolCarriesWebSearchLocation(tool) {
			indexes = append(indexes, int(key.Int()))
		}
		return true
	})
	if len(indexes) == 0 {
		return body
	}
	// 改写累积在 next 上，body 全程不动：任何一处改不动就整条放弃，返回未经改写的原体。
	// 若在 body 上原地累积，这里只能返回"前几个工具已是拉斯维加斯、后几个还是上海"的
	// 半改写体，比不改写更矛盾。
	next := body
	for _, index := range indexes {
		updated, ok := rewriteCodexUserLocationAt(next, "tools."+strconv.Itoa(index)+".user_location", account, timezone)
		if !ok {
			return body
		}
		next = updated
	}
	return next
}

// rewriteCodexUserLocationAt 把 path 处的位置对齐到出口；拿不到出口地理时删除整个字段。
// 返回 false 表示这条路径改不动，调用方据此整体放弃。
func rewriteCodexUserLocationAt(body []byte, path string, account *Account, timezone string) ([]byte, bool) {
	// 先删干净再补。重复键下 gjson 读第一个、serde_json 读最后一个，只改第一个会让后面
	// 那份客户端原值存活并被上游采信（同一个坑的既有解药见 openai_codex_wire_timezone.go
	// 的 codexEnvironmentField）。那边"有歧义就不改"是安全的，这里不改就是泄漏。
	for attempt := 0; gjson.GetBytes(body, path).Exists(); attempt++ {
		if attempt >= codexUserLocationDeleteAttempts {
			return nil, false
		}
		next, err := sjson.DeleteBytes(body, path)
		if err != nil {
			return nil, false
		}
		body = next
	}
	city, region, country, haveGeo := codexWireLocation(account)
	if !haveGeo {
		// 拿不到出口地理（手动覆盖了时区，或上游没返回城市）：留着客户端的城市就成了
		// "上海 + America/Los_Angeles"。删掉整个字段落回"未配置 location 的真客户端"
		// 这一合法形态。
		return body, true
	}
	// 整体重建而不是逐字段 set：客户端可能带上游不认的额外键，逐字段改会把它们留下。
	// 键序按 tools/src/tool_spec.rs:165-176 的声明顺序，serde 就是照这个顺序序列化的。
	// 值用不转义 HTML 的编码器：真客户端出线走 serde_json::to_string，而 encoding/json
	// 默认会把 "Dadra & Nagar Haveli" 写成 &（同一条理由见
	// openai_ws_forwarder_payload.go 的 setCodexWSClientMetadataString）。
	raw := `{"type":"approximate","country":` + codexJSONString(country) +
		`,"region":` + codexJSONString(region) +
		`,"city":` + codexJSONString(city) +
		`,"timezone":` + codexJSONString(timezone) + `}`
	next, err := sjson.SetRawBytes(body, path, []byte(raw))
	if err != nil {
		return nil, false
	}
	return next, true
}

// codexToolCarriesWebSearchLocation 判断该工具是否带着要对齐的位置字段。
//
// 用 ForEach 扫全部 type 而不是 tool.Get("type")：重复键下 gjson 读第一个而上游读最后
// 一个，只看第一个等于让客户端用一个假的首个 type（"function"）把真正的 web_search
// 藏过去。任一个 type 命中就算命中——真客户端不产生重复键，多扫一个工具只是把它的
// 位置也对齐到出口，代价远小于漏一个。
func codexToolCarriesWebSearchLocation(tool gjson.Result) bool {
	if !tool.IsObject() || !codexUserLocationPresent(tool.Get("user_location")) {
		return false
	}
	matched := false
	tool.ForEach(func(key, value gjson.Result) bool {
		if key.Str != "type" || value.Type != gjson.String {
			return true
		}
		if value.Str == codexWebSearchToolPrefix ||
			strings.HasPrefix(value.Str, codexWebSearchToolPrefix+"_") {
			matched = true
		}
		return true
	})
	return matched
}

// codexUserLocationPresent 判断客户端是否真的报了位置。
//
// 显式的 null 不算：gjson 的 Exists() 对 JSON null 返回 true，但上游两处字段都是
// Option（tools/src/tool_spec.rs:47、codex-api/src/search.rs:232），null 反序列化就是
// None。把 null 当成"发过"会让我们删掉它再写进一个完整的出口位置——也就是替一个
// 没报位置的客户端补一个，正是本文件开头明令禁止的那件事。
func codexUserLocationPresent(value gjson.Result) bool {
	return value.Exists() && value.Type != gjson.Null
}

// codexJSONString 把值编成不转义 HTML 的 JSON 字符串字面量。
func codexJSONString(value string) string {
	raw, err := marshalOpenAIUpstreamJSON(value)
	if err != nil {
		return `""`
	}
	return string(raw)
}
