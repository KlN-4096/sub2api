package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/imroc/req/v3"
	gocache "github.com/patrickmn/go-cache"
)

// 出口时区解析：搭在额度刷新上（每账号 10–30 分钟一次），再由
// shouldResolveCodexWireTimezone 节流到一天一次或换代理时。请求走账号自己的代理，
// 返回的 ip 即该账号出站时上游看到的地址，其归属时区就是要写进 environment_context 的值。
// 任何失败都只记日志并保留旧值：额度查询的结果不受影响，改写侧没有值时也只是不改写。
const (
	codexWireTimezoneLookupTimeout = 10 * time.Second
	// codexWireTimezoneRefreshTimeout 整段后台工作的上限。脱离请求 ctx 之后 DB / Redis 调用
	// 就没有 deadline 了，卡住会让每次成功的额度查询泄一个 goroutine。
	// 名单有两条，最坏情况是两次查询各耗满 10 秒；余量必须够后面那次 UpdateExtra 写回，
	// 否则解析成功的结果会因为 ctx 过期被丢掉，白跑两次经代理的外部查询。
	codexWireTimezoneRefreshTimeout = 45 * time.Second
)

// codexWireTimezoneLookupURLs 按顺序尝试，第一条成功即止。
//
// ipinfo.io 只有 A 记录没有 AAAA，IPv6-only 出口根本连不到它：SOCKS 代理直接失败，
// 该账号永远解析不出时区，于是静默退化成"不改写"——也就是把客户端本机时区原样发给
// 上游，正是本功能要防的那件事。v6.ipinfo.io 是同厂的纯 IPv6 端点（只有 AAAA），
// 响应结构与字段名完全一致，只在第一条失败后才可能成功，不引入新的第三方。
// IPv4 出口仍然走第一条，行为不变。
var codexWireTimezoneLookupURLs = []string{
	"https://ipinfo.io/json",
	"https://v6.ipinfo.io/json",
}

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
		proxyURL, err := resolveConfiguredProxyURL(ctx, s.proxyRepo, account.ProxyID, account.Proxy)
		if err != nil {
			slog.Warn("codex_wire_timezone_proxy_failed", "account_id", accountID, "error", err)
			return
		}
		exit, err := s.lookupCodexWireTimezone(ctx, proxyURL)
		if err != nil {
			slog.Warn("codex_wire_timezone_lookup_failed", "account_id", accountID, "error", err)
			return
		}
		updates := codexWireTimezoneExtraUpdates(codexWireTimezoneProxyTag(account), exit, now)
		if updates == nil {
			slog.Warn("codex_wire_timezone_lookup_invalid", "account_id", accountID, "timezone", exit.timezone)
			return
		}
		if err := s.accountRepo.UpdateExtra(ctx, accountID, updates); err != nil {
			slog.Warn("codex_wire_timezone_persist_failed", "account_id", accountID, "error", err)
			return
		}
		slog.Info("codex_wire_timezone_resolved",
			"account_id", accountID, "timezone", exit.timezone, "exit_ip", exit.ip,
			"city", exit.city, "region", exit.region, "country", exit.country)
	}()
}

// codexWireExit 是一次出口解析的结果。地理三项供 web_search 的 user_location 对齐
// （openai_codex_wire_user_location.go），和时区来自同一个响应，绝不会互相错配。
type codexWireExit struct {
	ip       string
	timezone string
	city     string
	region   string
	country  string
}

// lookupCodexWireTimezone 经账号代理查一次出口 IP、归属时区与大致地理位置。
func (s *OpenAIQuotaService) lookupCodexWireTimezone(ctx context.Context, proxyURL string) (codexWireExit, error) {
	client, err := s.privacyClientFactory(proxyURL)
	if err != nil {
		return codexWireExit{}, err
	}
	var errs []error
	for _, url := range codexWireTimezoneLookupURLs {
		exit, err := lookupCodexWireTimezoneAt(ctx, client, url)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		return exit, nil
	}
	if len(errs) == 0 {
		// 名单被改空：errors.Join 会返回 nil，调用方就把"什么都没查"当成查到了空时区。
		return codexWireExit{}, errors.New("no exit timezone lookup endpoint configured")
	}
	return codexWireExit{}, errors.Join(errs...)
}

func lookupCodexWireTimezoneAt(ctx context.Context, client *req.Client, url string) (codexWireExit, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, codexWireTimezoneLookupTimeout)
	defer cancel()
	var payload struct {
		IP       string `json:"ip"`
		Timezone string `json:"timezone"`
		City     string `json:"city"`
		Region   string `json:"region"`
		Country  string `json:"country"`
	}
	resp, err := client.R().
		SetContext(lookupCtx).
		SetSuccessResult(&payload).
		Get(url)
	if err != nil {
		return codexWireExit{}, err
	}
	if !resp.IsSuccessState() {
		return codexWireExit{}, fmt.Errorf("exit timezone lookup returned %d", resp.StatusCode)
	}
	// 200 但没有时区，等同失败，否则会吃掉后面那条兜底。这条守卫救的是能解析成
	// 功却无值的响应：`{}`、ipinfo 对私有地址返回的 bogon 体、以及任何缺 timezone
	// 的 200 JSON——空体和 HTML 错误页在上面的反序列化就已经报错了。
	// 只判时区：exit IP 仅写进 extra 供人工排查；地理三项缺失只是不改写 user_location，
	// 不能让整条查询失败——时区才是这次查询的目的，为地理去打第二家反而放大暴露面。
	timezone := strings.TrimSpace(payload.Timezone)
	if timezone == "" {
		return codexWireExit{}, fmt.Errorf("exit timezone lookup returned no timezone")
	}
	return codexWireExit{
		ip:       strings.TrimSpace(payload.IP),
		timezone: timezone,
		city:     strings.TrimSpace(payload.City),
		region:   strings.TrimSpace(payload.Region),
		country:  strings.TrimSpace(payload.Country),
	}, nil
}
