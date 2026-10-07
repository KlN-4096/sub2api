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
// 发出去的那些，所以它报的「碰过」是真相的**子集**。
//
// 子集这件事决定了它能接什么判定：**可以喂「别再碰这个落点」**（少避不会错避，漏避的代价是
// 一次降智，而那条路本来就有 state-echo 兜着），**不许反过来当「这个落点还能用」**（会把
// 别的行烧掉的窗口当成没烧）。前者就是 gatewayPoolHydrateUsed；后者仍然只信
// gatewayPoolUsedRecently 那本内存账。
const openAIGatewayHistoryExtraKey = "openai_gwpool_gateways"

const (
	// openAIGatewayHistoryMax 是留多少个网关。
	//
	// 24 太小：池子 2026-10-02 已经摸到 99 个落点（而网关名到 unified-215），一个跑了几小时的
	// 账号碰过的网关远超 24 个 ⇒ pruneOpenAIGatewayHistory 按时间裁掉最旧的 ⇒ 九宫格里那些格子
	// 直接消失，而「这个大区我没碰过」和「碰过但被裁了」在页面上长得一模一样。更要紧的是这份
	// 记录现在要给 /cookie 的 exclude 补账本（gatewayPoolHydrateUsed），裁掉一条就等于重启后
	// 把那个落点重新放出来。
	//
	// 201 = 把目前见过的网关号段整段盖住，还留了余量。条目很小（名字 + 两个时间戳 + 一个判定），
	// 201 条序列化后是十几 KB 量级的 JSONB，和这个 extra 里别的键同数量级。
	openAIGatewayHistoryMax = 201
	// openAIGatewayHistoryWriteInterval 是同一个网关的写节流窗口。没有它的话一个会话里
	// 每一发请求都要 UPDATE 一次账号行（UpdateExtra 对中性键仍会连带 GetByID + Redis 写）。
	// 换网关要立刻写——那正是这张卡要看的事，不该被节流窗口压住。
	openAIGatewayHistoryWriteInterval = 5 * time.Minute
	// openAIGatewayFullWindow 是前端把「验过满血」那一格显示成绿色的时长，也是 FullAt 必须
	// 刷新的节奏（见下面的节流穿透）。
	//
	// 183 秒不是我们测出来的，是**取两边最保守的那个**：实测窗口是 200–300 秒，而池子自己的
	// types.FullWindow 就是 183 秒、DeliverTTL 只有 150 秒（gwpool/internal/types）。取大的
	// 会让格子在池子和后端都认为窗口已关之后还绿着 —— 而运营方正照着这一格挑落点。
	// 前端 AccountGatewayCell.vue 的 FULL_WINDOW_MS 必须和它同值（跨语言，只能靠这条注释）。
	openAIGatewayFullWindow = 183 * time.Second
)

// state-echo 的两个结论（openai_gwpool_state_echo.go 产出，这里只存与展示）。
// 空串是第三态「没判过」，刻意不给它名字：零值就该是它。
const (
	openAIGatewayVerdictFull     = "full"
	openAIGatewayVerdictDegraded = "degraded"
)

// 满血分钟预测**刻意不在这里算**，在前端（AccountGatewayCell.vue 的 forecastUnits）。
//
// 它是个随时间衰减的值，而这条记录有 5 分钟写节流（openAIGatewayHistoryWriteInterval）
// ⇒ 存进去的预测立刻就过期。前端那边 now 跟着 ticker 走，而 seen[].at / seen[].region
// 和冷却窗口（openai_gwpool_gateway_window_s）本来就全在 extra 里，算得出来。
//
// 单位是 (账号 × 大区) 不是 (账号 × 网关)：一个号在一个大区同一时间只有一个网关，
// 同一大区下的多个网关名是同一个单位（现网 us-west 一个大区有 20 个不同网关名）。

// **这是上界不是承诺**，两个方向都偏乐观，前端那边的 tooltip 必须把它们说出来：
//   - 这本账挂在**账号行**上，而单位是**上游账号**的 —— 同一份凭据的克隆行/影子行各自
//     只看得见自己发出去的那些，所以「烧过」记少了（见本文件开头那段口径）。
//   - 冷却时长本身没测准（openAIGatewayPoolGatewayWindow 的注释：静置 30 分钟到 4 小时
//     命中率恒定，零相关），4 小时是工程保守取值。
//   - openAIGatewayHistoryMax 从 24 提到 201 之前写下的行，历史被按时间裁过 ⇒ 那些被裁掉
//     的落点看起来「没碰过」。
//
// 所以它答的是「最多」，用来回答「现在值不值得发请求」，**不能**反过来当调度闸 ——
// 那条仍然只信 gatewayPoolUsedRecently 那本内存账。

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
	// PoolLive / PoolFree 是最近一次问到的池子清单读数：此刻能交付几个网关，其中这个号
	// 还没烧过几个。两个数由 gatewayPoolPick 当场数出来（见 OpenAIGatewayPoolApplied）。
	//
	// **PoolFree 不是算出来的**：拿「PoolLive − Seen 里窗口内的条目数」去减是错的，账本
	// 装的是过去一个窗口里碰过的网关名、清单是此刻还有活票的，两者不是包含关系，相减会出
	// 负数，夹到 0 就成了「池子用完了」（2026-10-03 现网：账本 67、可交付 62，卡片报成 0）。
	//
	// PoolLive=0 = 还没问到过：关了 steering 的号不取清单（gatewayPoolPick 直接返回），
	// 列表打不开时也不覆盖旧值。卡片在那时只报已用，不编分母。PoolLive>0 时 PoolFree=0
	// 是**真的 0**（可交付的全烧过了）。
	//
	// 存在账号行上是搭车：池子全局的读数每一行各存一份。新鲜度跟着这一行自己的流量走，
	// 而那正是要看它的时候。
	PoolLive int `json:"pool_live,omitempty"`
	// PoolFree **不能带 omitempty**：带了的话真实的 0（可交付的全烧过了）会被整条省掉，
	// 和「这条记录是旧版本写的、压根没这个字段」在消费端长得一模一样 —— 而那正是这个字段
	// 存在的意义。消费端按「两个字段都在」判「这一对测到了」。
	PoolFree int `json:"pool_free"`
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
	// Verdict 是这个 (账号 × 网关) 上**最后一次** state-echo 判成什么：
	// "" = 没判过 / full / degraded。
	Verdict string `json:"verdict,omitempty"`
	// FullAt 是最后一次**判成满血**的时刻，唯一有意义的「回归」起点。
	//
	// 只能当排序权重，**不能当闸**：回归的触发变量未知（10-01 的 11 点测试里休息 30 分钟到
	// 4 小时的命中率是常数，零相关），所以「等 N 小时就算恢复」没有实测依据。判「这个网关
	// 还能不能用」仍然走 gatewayPoolUsedRecently 那个工程兜底窗口。
	// omitzero 而不是 omitempty：omitempty 对 struct 不生效，零值会落库成
	// "0001-01-01T00:00:00Z"（同 openai_turn_state_recovery.go 的两个时间字段）。
	FullAt time.Time `json:"full_at,omitzero"`
	// FullHeldMs 是这个 (账号 × 网关) 上量到的**满血持续了多久**（毫秒），由判降智那一刻
	// 从「这张票验出满血」算到「判成降智」—— 两头都在同一张票的生命里，有界。
	// 0 = 没量到（没验过满血、或者这一格的降智不是从本进程这条路判出来的）。
	//
	// **不能用 At − FullAt 代替**：FullAt 是粘滞的，而降智判定要等下一次真的打到这个网关
	// 才会写，中间的空闲全算进去。2026-10-03 现网照这个减法渲染出 22655 秒。
	FullHeldMs int64 `json:"full_held_ms,omitempty"`
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

// noteOpenAIGatewayUse 记一发请求落在了哪个网关，以及这一发的 state-echo 读数
// （verdict 为空 = 这一发没判据，照旧只记落点）。
//
// gateway 为空（读不出落点）时什么都不做：「这一发没能读出网关」和「这一发没有网关」是两回事，
// 记空值会把 Current 擦掉。
//
// advanceCurrent=false 只更新 Seen，不动 `Current`/`CurrentRegion`：queue 档的预热在业务请求
// **之前**判死一批落点，那些落点上永远不会有业务请求，推进「当前网关」会把卡片第一行写成最后
// 一个被判死的落点（见 openai_gwpool_warm.go 的 noteWarmVerdict）。
// poolLive/poolFree 是池子这一发清单的两个读数，poolLive=0 = 没问到（关了 steering /
// 列表打不开）⇒ 整对留旧值。它蹭的是这条已有的写路径：另起一条写 extra 的路就是两个写者
// 抢同一个键。
func (s *OpenAIGatewayService) noteOpenAIGatewayUse(
	ctx context.Context, account *Account, gateway, region, verdict string, advanceCurrent bool,
	poolLive, poolFree int, fullHeldMs int64,
) {
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
	// 没换网关、这个网关刚写过、大区没新消息、**判定也没变** ⇒ 不写。换了就立刻写。
	// 判定变了必须穿过节流：「这个落点刚被判降智」正是这张卡要看的事，压住它等于不记。
	//
	// 判成满血、而上一次判成满血已经比满血窗口还旧 ⇒ **也要穿过**：节流窗口（5 分钟）比满血
	// 窗口（183 秒）长，不穿的话一个持续被验成满血的落点，它的 FullAt 会停在第一次那一刻，
	// 格子在 183 秒后掉成琥珀 —— 而它可能几十秒前刚验过。穿过的频率上界就是满血窗口，
	// 不比原来的节流多出数量级。
	staleFull := verdict == openAIGatewayVerdictFull &&
		now.After(prev.FullAt.Add(openAIGatewayFullWindow))
	if (!advanceCurrent || rec.Current == gateway) && (region == "" || region == prev.Region) &&
		(verdict == "" || verdict == prev.Verdict) && !staleFull {
		if !prev.At.IsZero() && now.Before(prev.At.Add(openAIGatewayHistoryWriteInterval)) {
			return
		}
	}
	if rec.Seen == nil {
		rec.Seen = map[string]openAIGatewaySeen{}
	}
	// 大区读不出来时**留着上一次记的那个**：同一个网关的大区不会变（网关 = 大区 × 账号），
	// 一发改派就把它擦掉等于白丢一格信息。
	if region == "" {
		region = prev.Region
	}
	if advanceCurrent {
		rec.Current = gateway
		rec.CurrentRegion = region
	}
	next := openAIGatewaySeen{At: now, Region: region, Verdict: prev.Verdict, FullAt: prev.FullAt,
		FullHeldMs: prev.FullHeldMs}
	// 这一发量到了就刷新，没量到留着上一次的：满血时长是「上一个窗口有多长」，没新读数时
	// 旧读数仍然是关于这一格最新的事实。
	if fullHeldMs > 0 {
		next.FullHeldMs = fullHeldMs
	}
	// 这一发没判据时**留着上一次的判定**，和大区同一个道理：没判 ≠ 判不出来。
	if verdict != "" {
		next.Verdict = verdict
		if verdict == openAIGatewayVerdictFull {
			next.FullAt = now
		}
	}
	rec.Seen[gateway] = next
	// 成对写、0 不覆盖：没问到清单的那些发（关了 steering、列表超时）该留着上一次问到的
	// 那一对，写 0 会让卡片说「池子一个落点都没有」。拆开写会出现新 free 配旧 live 的组合，
	// 而那个组合从来没有同时成立过。跟着这条写路径走、不单独穿过节流 —— 它只是展示用的
	// 读数，下一次正常写就会刷新。
	if poolLive > 0 {
		rec.PoolLive, rec.PoolFree = poolLive, poolFree
	}
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
