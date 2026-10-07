package service

import (
	"fmt"
	"strings"
)

// codex-proxy-rs（下称 CPR）中继账号。
//
// 与 OAuth/apikey 的根本差别：出站的 TLS 指纹、身份头、environment_context 与
// user_location 全部由 CPR 负责，sub2api 只做用户体系、调度与展示。因此
// UsesOpenAICodexProtocol() 对本类型必须保持 false —— 那一整条指纹链
// （UA 强制、时区改写、zstd、chatgpt-account-id、req.Host=chatgpt.com）
// 都挂在它下面，一旦为真就会与 CPR 自己的改写打架。
//
// 约定：一个 cpr 账号恰好对应 CPR 里的一个 OpenAI 账号。做法是给该账号单独建
// 一个分组，再建一把只绑定该分组的 client key —— CPR 的选号器在
// scheduling_blocker 第一道闸就做 scope 过滤，候选集被压成 1 个元素后所有轮换
// 策略等价，必然选中它，且不会 fallback 到组外账号。
const (
	cprCredentialBaseURL      = "base_url"       // CPR 网关地址，如 http://127.0.0.1:18081
	cprCredentialClientKey    = "api_key"        // CPR client key，Authorization: Bearer
	cprCredentialAdminBaseURL = "admin_base_url" // admin API 地址；留空回落到 base_url
	cprCredentialAdminAPIKey  = "admin_api_key"  // CPR admin key（x-api-key），用于拉额度
	cprCredentialAccountID    = "cpr_account_id" // CPR 侧账号 id，形如 acct_xxx
)

// IsCPR 报告账号是否为 CPR 中继类型。
func (a *Account) IsCPR() bool {
	return a != nil && a.Type == AccountTypeCPR && a.Platform == PlatformOpenAI
}

// validateCPRAccountShape 拒绝 platform ≠ openai 的 cpr 账号。
//
// IsCPR() 要求 platform 为 openai，但 GetCPRClientKey() 与 GetAccessToken 的
// case 分支都不要求。不拦的话可以造出 anthropic + cpr 这种账号：它在 anthropic
// 网关侧会 fail closed，但属于能落库的无效状态，排查时很费解。
func validateCPRAccountShape(platform, accountType string) error {
	if accountType == AccountTypeCPR && platform != PlatformOpenAI {
		return fmt.Errorf("cpr account type requires platform %q, got %q", PlatformOpenAI, platform)
	}
	return nil
}

// GetCPRGatewayBaseURL 返回 CPR 网关地址。
//
// 故意不提供默认值：这里回落到 api.openai.com 会把 CPR 的 client key 当成
// OpenAI API key 发给官方，是凭据泄漏。调用方拿到空串必须报错而不是兜底。
func (a *Account) GetCPRGatewayBaseURL() string {
	if a == nil {
		return ""
	}
	return strings.TrimSpace(a.GetCredential(cprCredentialBaseURL))
}

// GetCPRClientKey 返回用于 Authorization: Bearer 的 CPR client key。
func (a *Account) GetCPRClientKey() string {
	if a == nil {
		return ""
	}
	return strings.TrimSpace(a.GetCredential(cprCredentialClientKey))
}

// GetCPRAdminBaseURL 返回 admin API 地址；未单独配置时与网关同址。
func (a *Account) GetCPRAdminBaseURL() string {
	if a == nil {
		return ""
	}
	if adminURL := strings.TrimSpace(a.GetCredential(cprCredentialAdminBaseURL)); adminURL != "" {
		return adminURL
	}
	return a.GetCPRGatewayBaseURL()
}

// GetCPRAdminAPIKey 返回 CPR 的 admin key（x-api-key）。
func (a *Account) GetCPRAdminAPIKey() string {
	if a == nil {
		return ""
	}
	return strings.TrimSpace(a.GetCredential(cprCredentialAdminAPIKey))
}

// GetCPRAccountID 返回该 sub2api 账号绑定的 CPR 账号 id。
func (a *Account) GetCPRAccountID() string {
	if a == nil {
		return ""
	}
	return strings.TrimSpace(a.GetCredential(cprCredentialAccountID))
}
