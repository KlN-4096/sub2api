package service

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
)

// Codex 推理面的路由对读数：__cflb / __oailb。
//
// __oailb 是 ES256 JWT，载荷形如
// {"host":"chat.gateway.unified-101.api.openai.com","iss":"edge-gateway","aud":["chatgpt.com"],...}，
// 2026-09-27 直连解包实测：里面只有网关主机、签发方、受众与有效期，**没有账号身份、也没有任何
// 能力声明**——它钉的是"这一发走哪台后端网关"，不是"这一发是不是满血"。__cflb 是 Cloudflare
// 负载均衡的不透明短串。两者由 openAICodexCookieStore 按账号分罐回放（openai_codex_cookies.go）。
//
// 落 usage_logs 只为观测：同一账号在不同网关上的表现能不能对上，以及票（x-codex-turn-state）
// 与路由对是不是同进同退。不做任何判据。
const (
	maxUsageRoutePairLen    = 512
	maxUsageRouteGatewayLen = 64
	// maxUsageRoutePairVersionLen 与 gwpool 侧的 cookie_version 上限同值。
	maxUsageRoutePairVersionLen = 128
)

const (
	// openAICodexRouteCFLBCookie 是真正钉住路由的那一项（Cloudflare 负载均衡的不透明短串）。
	// openAICodexRouteOAILBCookie 只是让响应**暴露落点**的那张 JWT。
	openAICodexRouteCFLBCookie  = "__cflb"
	openAICodexRouteOAILBCookie = "__oailb"
)

// 顺序即 routePairOf 的输出顺序，改了会让同一组路由对在不同请求里落库成不同字符串。
var openAICodexRouteCookieNames = [...]string{openAICodexRouteCFLBCookie, openAICodexRouteOAILBCookie}

// routePairOf 从一组 "name=value" 候选里挑出这两个名字，拼成稳定顺序的 "name=value; name=value"。
func routePairOf(candidates []string) string {
	found := map[string]string{}
	for _, raw := range candidates {
		name, value, _ := strings.Cut(strings.TrimSpace(raw), "=")
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if value == "" {
			continue
		}
		for _, want := range openAICodexRouteCookieNames {
			if name == want {
				if _, seen := found[name]; !seen {
					found[name] = value
				}
			}
		}
	}
	var out []string
	for _, name := range openAICodexRouteCookieNames {
		if v, ok := found[name]; ok {
			out = append(out, name+"="+v)
		}
	}
	return strings.Join(out, "; ")
}

// routePairItem 从 routePairOf 的产物里取出一项（"name=value"，没有这一项就返回空串）。
func routePairItem(pair, name string) string {
	for _, item := range strings.Split(pair, ";") {
		item = strings.TrimSpace(item)
		if itemName, _, _ := strings.Cut(item, "="); itemName == name {
			return item
		}
	}
	return ""
}

// Set-Cookie 一行一对，属性段（Path/Max-Age/...）在第一个 ";" 之后，丢掉。
func openAICodexRoutePairFromSetCookie(h http.Header) string {
	var candidates []string
	for _, line := range headerValuesFold(h, "set-cookie") {
		first, _, _ := strings.Cut(line, ";")
		candidates = append(candidates, first)
	}
	return routePairOf(candidates)
}

// Cookie 一行多对，用 ";" 分隔。
func openAICodexRoutePairFromCookie(h http.Header) string {
	var candidates []string
	for _, line := range headerValuesFold(h, "cookie") {
		candidates = append(candidates, strings.Split(line, ";")...)
	}
	return routePairOf(candidates)
}

// routePairInUse 取这一发实际生效的路由对：上游在响应里新下发就用新的（那是改派后的路由），
// 否则回读罐里当前的那一组（Attach 到一个临时头上再读回来，不写罐、不改任何状态）。
//
// 后两个返回值是「出站那一组由网关池下发」与**池子交付时说的那个网关**。它们与第一个返回值
// 刻意可以不同源：上游改派时 Set-Cookie 里是新落点（第一个返回值记它），而出站带的确实是池子
// 那张。页面据此比对——
//
//	poolGateway == 实际落点 ⇒ 注入生效
//	poolGateway != 实际落点 ⇒ 上游下发了新的 __oailb，这一发被改派走了 = 注入被拒
//
// 判据只比**落点**，不看「上游有没有下发 Set-Cookie」—— 三格都是已知常态：
//
//	没有 Set-Cookie                  ⇒ 已覆写（两件齐发时的常态：借来的活票上游什么都不回）
//	有新 __oailb，落点 == 交付的网关  ⇒ 已覆写（续期那一发的常态：它刻意只送 __cflb，
//	                                  上游必然补发一套，但路由没变 —— 见 gatewayPoolRenew）
//	有新 __oailb，落点 != 交付的网关  ⇒ 被改派（真漂移 = 注入被拒）
//
// 「有 Set-Cookie 就是被改派」是 2026-10-02 续期上线前的老判据，会把第二格误读成漂移。
// 第四个返回值是池子给这张票的身份（cookie_version），落库只为**和池子的日志对上账**：
// 徽标说「被改派」时，拿它去池子那边看这张票的交付与验证记录。它不是 cookie 本体。
func (s *OpenAIGatewayService) routePairInUse(
	account *Account,
	upstream http.Header,
	applied OpenAIGatewayPoolApplied,
) (pair string, fromPool bool, poolGateway string, poolVersion string) {
	fresh := openAICodexRoutePairFromSetCookie(upstream)
	if account == nil || !openAICodexCookiesApply(account) {
		return fresh, false, "", ""
	}
	// 网关池接管时罐不参与出站，这一发带的是池子那张 pair（openai_gwpool.go）。
	// 只认 per-request 标记：回读 pair 缓存认不出这一发是不是推理面，会把每一发落回罐回放的
	// 非推理面请求都记成「已覆写」（见 OpenAIGatewayPoolApplied）。
	// 账号必须对得上：故障转移在同一个 ctx 里换号重试，标记留的是前一个号的。
	fromPool = applied.Cookie != "" && applied.AccountID == account.ID
	if fromPool {
		poolGateway, poolVersion = applied.Gateway, applied.Version
	}
	switch {
	case fresh != "":
		return fresh, fromPool, poolGateway, poolVersion
	case fromPool:
		return applied.Cookie, true, poolGateway, poolVersion
	}
	probe := http.Header{}
	s.codexCookies.Attach(account, openAITurnStatePairCookieURL, probe)
	return openAICodexRoutePairFromCookie(probe), false, "", ""
}

// openAICodexRouteGateway 解 __oailb 的 JWT 载荷取 host 里的 unified-N 段。只解不验签：
// 这是观测读数，签名由上游自己校验，我们既不签发也不据此放行任何东西。
func openAICodexRouteGateway(pair string) string {
	for _, item := range strings.Split(pair, ";") {
		name, value, _ := strings.Cut(strings.TrimSpace(item), "=")
		if strings.TrimSpace(name) != "__oailb" {
			continue
		}
		segments := strings.Split(strings.TrimSpace(value), ".")
		if len(segments) < 2 {
			return ""
		}
		payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(segments[1], "="))
		if err != nil {
			return ""
		}
		var claims struct {
			Host string `json:"host"`
		}
		if json.Unmarshal(payload, &claims) != nil {
			return ""
		}
		for _, chunk := range strings.Split(claims.Host, ".") {
			if strings.HasPrefix(chunk, "unified-") {
				return chunk
			}
		}
		return ""
	}
	return ""
}

// usageCodexRoutePairPtr / usageCodexRouteGatewayPtr：写进使用记录的两个读数，空值给 NULL。
// 列是 TEXT：截断不切多字节字符，非法字节剔掉，否则整行 INSERT 被 PostgreSQL 拒绝。
func usageCodexRoutePairPtr(pair string) *string {
	pair = strings.TrimSpace(pair)
	if pair == "" {
		return nil
	}
	v := strings.ToValidUTF8(truncateUTF8(pair, maxUsageRoutePairLen), "")
	return &v
}

// usageCodexRoutePairOverriddenPtr 记录这一发出站的路由对是不是网关池下发的
// （使用记录里那张小卡片的「已覆写」徽标）。与 usageCodexTurnStateOverriddenPtr 同口径：
// 账号类型不适用时返回 nil（列保持 NULL），与「接管着但这一发没覆写」区分开。
//
// 只记「覆写了没有」和两个网关名，**不记 cookie 本体是从哪来的**——pair 全文已经在
// route_pair 列里，卡片不展示它。
func usageCodexRoutePairOverriddenPtr(account *Account, fromPool bool) *bool {
	if account == nil || !account.TargetsChatGPTCodexUpstream() {
		return nil
	}
	return &fromPool
}

// usageCodexRoutePairPoolGatewayPtr 记池子交付时说的那个网关：与 route_gateway 比对就知道注入
// 被不被上游接受（见 routePairInUse）。没走池子 / 池子没报出网关名时为 nil。
func usageCodexRoutePairPoolGatewayPtr(gateway string) *string {
	gateway = strings.TrimSpace(gateway)
	if gateway == "" {
		return nil
	}
	v := strings.ToValidUTF8(truncateUTF8(gateway, maxUsageRouteGatewayLen), "")
	return &v
}

// usageCodexRoutePairPoolVersionPtr 记池子给这张票的身份（cookie_version），用来和池子侧的
// 交付/验证日志对上账。池子没报时为 nil。长度上限与网关名同口径（池子侧已钳到 128 内）。
func usageCodexRoutePairPoolVersionPtr(version string) *string {
	version = strings.TrimSpace(version)
	if version == "" {
		return nil
	}
	v := strings.ToValidUTF8(truncateUTF8(version, maxUsageRoutePairVersionLen), "")
	return &v
}

func usageCodexRouteGatewayPtr(pair string) *string {
	gw := openAICodexRouteGateway(pair)
	if gw == "" {
		return nil
	}
	v := strings.ToValidUTF8(truncateUTF8(gw, maxUsageRouteGatewayLen), "")
	return &v
}
