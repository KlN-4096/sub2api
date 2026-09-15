package service

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	gocache "github.com/patrickmn/go-cache"
)

// 出口时区解析：搭在额度刷新上（每账号 10–30 分钟一次），再由
// shouldResolveCodexWireTimezone 节流到一天一次或换代理时。请求走账号自己的代理，
// 返回的 ip 即该账号出站时上游看到的地址，其归属时区就是要写进 environment_context 的值。
// 任何失败都只记日志并保留旧值：额度查询的结果不受影响，改写侧没有值时也只是不改写。
const (
	codexWireTimezoneLookupURL     = "https://ipinfo.io/json"
	codexWireTimezoneLookupTimeout = 10 * time.Second
	// codexWireTimezoneRefreshTimeout 整段后台工作的上限。脱离请求 ctx 之后 DB / Redis 调用
	// 就没有 deadline 了，卡住会让每次成功的额度查询泄一个 goroutine。
	codexWireTimezoneRefreshTimeout = 30 * time.Second
)

// codexWireTimezoneInFlight 按账号收口并发解析。额度查询有多个入口（管理端账号详情、
// 定时用量刷新、自动重置流程），同一个未解析过的账号可能被同时点到：闸门读的是库里的
// 旧值，两条都会通过，各跑一次经账号代理的外部查询。
var codexWireTimezoneInFlight = gocache.New(5*time.Minute, time.Minute)

// refreshCodexWireTimezone 在额度查询成功后顺带解析并写回出口时区。
//
// 整段在后台跑：闸门判定要读账号（还可能解析影子行），命中后还有一次最长 10 秒的外部查询，
// 挂在额度查询的同步路径上会把管理端的账号详情和用量刷新一起拖住。额度结果不依赖它。
func (s *OpenAIQuotaService) refreshCodexWireTimezone(ctx context.Context, accountID int64) {
	if s == nil || s.accountRepo == nil || s.privacyClientFactory == nil {
		return
	}
	if err := codexWireTimezoneInFlight.Add(
		strconv.FormatInt(accountID, 10), true, gocache.DefaultExpiration); err != nil {
		return // 这个账号刚跑过或正在跑
	}
	// 脱离请求生命周期但保留链路上的值（日志字段等）：请求返回后这段仍要跑完。
	// WithoutCancel 同时去掉了 deadline，必须自己补一个，否则下面的 DB 调用无超时。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), codexWireTimezoneRefreshTimeout)
	go func() {
		defer cancel()
		defer func() { _ = recover() }()
		account, err := s.accountRepo.GetByID(ctx, accountID)
		if err != nil || account == nil {
			return
		}
		// 收敛开关挂在凭证账号上，device 模式挂在被转发的行上——与请求时同一套解析，
		// 否则影子行会出现"请求时改写、解析时不解析"。
		credAccount := account
		if account.IsShadow() {
			if resolved, resolveErr := resolveCredentialAccount(ctx, s.accountRepo, account); resolveErr == nil && resolved != nil {
				credAccount = resolved
			}
		}
		now := time.Now()
		if !shouldResolveCodexWireTimezone(account, credAccount, now) {
			return
		}
		// 量的必须是这条链路真正会用的出口：代理取值与转发侧同一个表达式。
		exitIP, timezone, err := s.lookupCodexWireTimezone(ctx, codexWireTimezoneProxyURL(account))
		if err != nil {
			slog.Warn("codex_wire_timezone_lookup_failed", "account_id", accountID, "error", err)
			return
		}
		updates := codexWireTimezoneExtraUpdates(codexWireTimezoneProxyTag(account), exitIP, timezone, now)
		if updates == nil {
			slog.Warn("codex_wire_timezone_lookup_invalid", "account_id", accountID, "timezone", timezone)
			return
		}
		if err := s.accountRepo.UpdateExtra(ctx, accountID, updates); err != nil {
			slog.Warn("codex_wire_timezone_persist_failed", "account_id", accountID, "error", err)
			return
		}
		slog.Info("codex_wire_timezone_resolved",
			"account_id", accountID, "timezone", timezone, "exit_ip", exitIP)
	}()
}

// lookupCodexWireTimezone 经账号代理查一次出口 IP 与其归属时区。
func (s *OpenAIQuotaService) lookupCodexWireTimezone(ctx context.Context, proxyURL string) (string, string, error) {
	client, err := s.privacyClientFactory(proxyURL)
	if err != nil {
		return "", "", err
	}
	lookupCtx, cancel := context.WithTimeout(ctx, codexWireTimezoneLookupTimeout)
	defer cancel()
	var payload struct {
		IP       string `json:"ip"`
		Timezone string `json:"timezone"`
	}
	resp, err := client.R().
		SetContext(lookupCtx).
		SetSuccessResult(&payload).
		Get(codexWireTimezoneLookupURL)
	if err != nil {
		return "", "", err
	}
	if !resp.IsSuccessState() {
		return "", "", fmt.Errorf("exit timezone lookup returned %d", resp.StatusCode)
	}
	return strings.TrimSpace(payload.IP), strings.TrimSpace(payload.Timezone), nil
}
