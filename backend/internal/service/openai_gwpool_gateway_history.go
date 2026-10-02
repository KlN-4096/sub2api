package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// 账号打过哪些网关、现在落在哪个。
//
// (账号 × 网关) 是降智的作用单位，但这本账以前只有两个形态，两个都答不了「这个号碰过
// 哪些落点」：usage_logs 一请求一行（没有按账号的聚合查询，也没索引），以及进程内存里的
// poolUsed（重启即失，而且键是凭据身份不是账号行）。账号列表那张卡要的是账号维度的读数，
// 所以在 extra 上留一条**每账号一条**的记录，走 turn-state 观测那套管线：节流写 + 登记成
// 调度中立键。
//
// **口径注意**：这条记录挂在账号行上，而真正被烧掉的单位是上游账号——同一份 Codex 凭据
// 可能挂在多个行上（克隆行、影子行，见 gatewayPoolLedgerKey 的注释），各行只看得见自己
// 发出去的那些。所以它只能用来展示，一个判定都不许接；要判「这个网关还能不能用」仍然走
// gatewayPoolUsedRecently。
const openAIGatewayHistoryExtraKey = "openai_gwpool_gateways"

const (
	// openAIGatewayHistoryMax 是留多少个网关。池子现在摸到的总共几十个，24 条足够看出
	// 「这个号在哪些落点上烧过」，又不会把 extra 撑成一本日志。
	openAIGatewayHistoryMax = 24
	// openAIGatewayHistoryWriteInterval 是同一个网关的写节流窗口。没有它的话一个会话里
	// 每一发请求都要 UPDATE 一次账号行（UpdateExtra 对中性键仍会连带 GetByID + Redis 写）。
	// 换网关要立刻写——那正是这张卡要看的事，不该被节流窗口压住。
	openAIGatewayHistoryWriteInterval = 5 * time.Minute
)

// openAIGatewayHistory 是那条记录。
//
// Seen 用 map 而不是数组：同一个网关会被反复碰到，按名字原地覆盖时间戳，条目数恒等于
// 碰过的网关数。顺序在读的时候按时间排（readOpenAIGatewayHistoryRows）。
type openAIGatewayHistory struct {
	// Current 是最近一发请求实际落在的网关。空 = 这个号还没拿到过能读出落点的路由。
	Current string `json:"current"`
	// CurrentRegion 是 Current 那个网关所属的大区（池子报的）。空 = 不知道。
	// 单独存一份而不是现查 Seen：卡片第一行要的就是「当前大区 · 当前网关」这一对。
	CurrentRegion string `json:"current_region,omitempty"`
	// Seen 是「网关名 → 最近一次落在它上面的时刻 + 它属于哪个大区」。
	//
	// **换过值的形状**（2026-10-02）：以前是 map[string]time.Time。老记录解不出来 ⇒
	// readOpenAIGatewayHistory 按「没有」处理 ⇒ 下一发请求重写一条。这条记录是读数不是
	// 账本（判「这个网关还能不能用」走 gatewayPoolUsedRecently），丢了只是卡片空一会儿，
	// 所以不写迁移代码。
	Seen map[string]openAIGatewaySeen `json:"seen"`
	// UpdatedAt 是写下这条记录的时刻，只用于展示「这份读数有多新」。
	UpdatedAt time.Time `json:"updated_at"`
}

// openAIGatewaySeen 是一个网关的一条记录。
type openAIGatewaySeen struct {
	// At 是最近一次落在这个网关上的时刻。
	At time.Time `json:"at"`
	// Region 是池子说的「这张票是哪个大区铸的」。空 = 池子没报 / 这一发被上游改派走了
	// （那时池子说的大区对不上实际落点，记上去会把落点归到错的大区里，宁可留空）。
	Region string `json:"region,omitempty"`
}

// readOpenAIGatewayHistory 读这条记录。解析失败按「没有」处理。
//
// 走 marshal/unmarshal 而不是裸类型断言：extra 是 JSONB，同一个键在「刚写进去」和
// 「从 DB / Redis 读回来」两条路径上的具体 Go 类型不保证相同，裸断言失败是静默的
// （照 readOpenAITurnStateObservation）。
func readOpenAIGatewayHistory(a *Account) (openAIGatewayHistory, bool) {
	if a == nil {
		return openAIGatewayHistory{}, false
	}
	raw, ok := a.Extra[openAIGatewayHistoryExtraKey]
	if !ok || raw == nil {
		return openAIGatewayHistory{}, false
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return openAIGatewayHistory{}, false
	}
	var rec openAIGatewayHistory
	if err := json.Unmarshal(encoded, &rec); err != nil {
		return openAIGatewayHistory{}, false
	}
	return rec, true
}

// noteOpenAIGatewayUse 记一发请求落在了哪个网关。gateway 为空（读不出落点）时什么都不做：
// 「这一发没能读出网关」和「这一发没有网关」是两回事，记空值会把 Current 擦掉。
func (s *OpenAIGatewayService) noteOpenAIGatewayUse(ctx context.Context, account *Account, gateway, region string) {
	if s == nil || s.accountRepo == nil || account == nil {
		return
	}
	gateway = strings.TrimSpace(gateway)
	if gateway == "" {
		return
	}
	region = strings.TrimSpace(region)
	now := time.Now().UTC()

	rec, _ := readOpenAIGatewayHistory(account)
	prev := rec.Seen[gateway]
	// 没换网关、这个网关刚写过、而且大区也没新消息 ⇒ 不写。换了就立刻写。
	if rec.Current == gateway && (region == "" || region == prev.Region) {
		if !prev.At.IsZero() && now.Before(prev.At.Add(openAIGatewayHistoryWriteInterval)) {
			return
		}
	}
	if rec.Seen == nil {
		rec.Seen = map[string]openAIGatewaySeen{}
	}
	rec.Current = gateway
	// 大区读不出来时**留着上一次记的那个**：同一个网关的大区不会变（网关 = 大区 × 账号），
	// 一发改派就把它擦掉等于白丢一格信息。
	if region == "" {
		region = prev.Region
	}
	rec.CurrentRegion = region
	rec.Seen[gateway] = openAIGatewaySeen{At: now, Region: region}
	rec.UpdatedAt = now
	pruneOpenAIGatewayHistory(&rec)

	encoded, err := json.Marshal(rec)
	if err != nil {
		return
	}
	var generic map[string]any
	if err := json.Unmarshal(encoded, &generic); err != nil {
		return
	}
	if account.Extra == nil {
		account.Extra = map[string]any{}
	}
	account.Extra[openAIGatewayHistoryExtraKey] = generic
	// 不随请求取消：用量是在响应收尾之后记的，跟着请求 ctx 一起死就等于这条读数永远写不进去。
	if err := s.accountRepo.UpdateExtra(context.WithoutCancel(ctx), account.ID, map[string]any{
		openAIGatewayHistoryExtraKey: generic,
	}); err != nil {
		slog.Debug("gwpool_gateway_history_persist_failed", "account_id", account.ID, "error", err)
	}
}

// pruneOpenAIGatewayHistory 裁到 openAIGatewayHistoryMax 条，丢最早的。
//
// Current 不许被裁掉：它是这张卡最要紧的那一格，而「当前网关」恰好可能是刚加进来的那条
// （加进来时它是最新的，裁的是最旧的，所以这里实际裁不到它——留着这个判断是为了让
// 以后改排序规则的人撞上它）。
func pruneOpenAIGatewayHistory(rec *openAIGatewayHistory) {
	if rec == nil || len(rec.Seen) <= openAIGatewayHistoryMax {
		return
	}
	type seen struct {
		name string
		at   time.Time
	}
	all := make([]seen, 0, len(rec.Seen))
	for name, s := range rec.Seen {
		all = append(all, seen{name, s.At})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].at.Equal(all[j].at) {
			return all[i].name < all[j].name
		}
		return all[i].at.After(all[j].at)
	})
	for _, s := range all[openAIGatewayHistoryMax:] {
		if s.name == rec.Current {
			continue
		}
		delete(rec.Seen, s.name)
	}
}
