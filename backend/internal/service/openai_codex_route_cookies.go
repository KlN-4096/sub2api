package service

import (
	"context"
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
)

var openAICodexRouteCookieNames = [...]string{"__cflb", "__oailb"}

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
func (s *OpenAIGatewayService) routePairInUse(ctx context.Context, account *Account, upstream http.Header) string {
	if fresh := openAICodexRoutePairFromSetCookie(upstream); fresh != "" {
		return fresh
	}
	if account == nil || !openAICodexCookiesApply(account) {
		return ""
	}
	// 网关池接管时罐不参与出站，这一发带的是池子那张 pair（openai_gwpool.go）。
	if pair, ok := s.codexCookies.gatewayPoolPairInUse(ctx, account); ok {
		return pair
	}
	probe := http.Header{}
	s.codexCookies.Attach(account, openAITurnStatePairCookieURL, probe)
	return openAICodexRoutePairFromCookie(probe)
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

func usageCodexRouteGatewayPtr(pair string) *string {
	gw := openAICodexRouteGateway(pair)
	if gw == "" {
		return nil
	}
	v := strings.ToValidUTF8(truncateUTF8(gw, maxUsageRouteGatewayLen), "")
	return &v
}
