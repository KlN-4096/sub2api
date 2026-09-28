//go:build unit

package service

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// pair 模式全部离线：上游是 hunterUpstream，零真实请求。

const (
	// 2026-09-23 起上游铸的形态：33 块密文 → 780 字符。按长度判健康在它身上恒 false，
	// 所以 pair 模式的判据只能是做题（openai_turn_state_pair.go 文件头）。
	openAIPairTurnStateBlocks = 33
	openAIPairTurnStateLen    = 780
)

// pairHunterAccount 造一个开着 pair 模式猎手的账号。lead_minutes 必须比票寿命短，
// 否则「任何票都永远不够用」（handler 会拒，applyDefaults 也会兜底）。
func pairHunterAccount(overrides map[string]any) *Account {
	cfg := map[string]any{"pair_mode": true, "lead_minutes": 1, "ticket_ttl_seconds": 300}
	for k, v := range overrides {
		cfg[k] = v
	}
	return hunterTestAccount(hunterConfig(cfg))
}

// pairResp 造一条铸票响应：780 票 + 路由 cookie + 完整 SSE 回答体。
func pairResp(minted time.Time, answer string, cookies ...string) *http.Response {
	resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(minted, openAIPairTurnStateBlocks), recoverySSE(answer))
	for _, cookie := range cookies {
		resp.Header.Add("Set-Cookie", cookie)
	}
	return resp
}

const (
	pairCFLB  = "__cflb=0H28vzvP8q1Xy; Path=/; HttpOnly; Secure; SameSite=None"
	pairOAILB = "__oailb=eyJhbGciOiJFUzI1NiJ9.payload.sig; Path=/; Secure"
)

// TestOpenAITurnStatePairProbePoolsOnCorrectAnswer 钉住 pair 模式主流程：
// 一次请求（不是「先发 hi 再验票」）、直接做糖果题、答对才入池，票连带 __cflb / __oailb
// 与自己的短有效期一起存下来。
func TestOpenAITurnStatePairProbePoolsOnCorrectAnswer(t *testing.T) {
	now := time.Now().UTC()
	// 先把前提钉住：780 的票按长度判是「不健康」，所以入池只可能来自做题判据。
	blob := turnStateFernetBlob(now, openAIPairTurnStateBlocks)
	require.Len(t, blob, openAIPairTurnStateLen)
	require.False(t, openAITurnStateHealthy(blob), "780 时代按长度判健康已死")

	h := newHunterHarness(pairHunterAccount(nil), hunterWebshareProxy)
	h.up.queue = []*http.Response{pairResp(now, "21", pairCFLB, pairOAILB, "__cf_bm=bm; Path=/")}

	h.run(t)

	require.Len(t, h.up.requests, 1, "pair 探测一次请求就拿到票、pair 和判据")
	require.True(t, h.up.requests[0].Close, "铸票要新鲜出口：不关连接就一直从同一个出口发")
	require.Empty(t, h.up.requests[0].Header.Get("x-codex-turn-state"), "铸票必须自然铸造，不能带票")

	body := h.up.bodies[0]
	require.Equal(t, openAITurnStateRecoveryPrompt, gjson.GetBytes(body, "input.0.content.0.text").String(),
		"pair 探测直接出糖果题，不再发 hi")
	require.Equal(t, hunterTestModel, gjson.GetBytes(body, "model").String(), "按要猎的模型出题（票绑死在模型上）")

	pool := readOpenAITurnStatePool(h.account)
	require.Len(t, pool, 1, "答对了才入池")
	require.Equal(t, blob, pool[0].Blob)
	require.Equal(t, hunterTestModel, pool[0].Model)
	require.Equal(t, 300, pool[0].TTLSeconds, "候选上的有效期必须是用户填的那个值，不是默认值")
	require.Equal(t, []string{"__cflb=0H28vzvP8q1Xy", "__oailb=eyJhbGciOiJFUzI1NiJ9.payload.sig"}, pool[0].Cookies,
		"只存钉路由的那两个：__cf_bm 是按 TLS 指纹 + IP 发的，跨出口回放只会自相矛盾")
	require.Zero(t, pool[0].Reminted)

	st := h.state()
	require.Len(t, st.Last, 1)
	require.True(t, st.Last[0].Healthy)
	require.Equal(t, "21", st.Last[0].Answer, "页面要能看到模型答了什么")
	require.Equal(t, 2, st.Last[0].Cookies, "拿到几个 pair cookie 也是读数")
	require.Equal(t, openAIPairTurnStateLen, st.Last[0].Chars, "票长只作信息记录")
	require.Empty(t, st.Last[0].Error)
	require.True(t, st.NextAt.IsZero(), "命中后不退避")
}

// TestOpenAITurnStatePairProbeDiscardsWrongAnswer 钉住判据：答错就不入池——780 的票长
// 在答对答错时一模一样，只有答案能区分。
func TestOpenAITurnStatePairProbeDiscardsWrongAnswer(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(pairHunterAccount(map[string]any{"max_per_hour": 1}), hunterWebshareProxy)
	h.up.queue = []*http.Response{pairResp(now, "29", pairCFLB, pairOAILB)}

	h.run(t)

	require.Len(t, h.up.requests, 1)
	require.Empty(t, readOpenAITurnStatePool(h.account), "答错的票不许入池")
	st := h.state()
	require.False(t, st.Last[0].Healthy)
	require.Equal(t, "29", st.Last[0].Answer)
	require.Equal(t, openAIPairTurnStateLen, st.Last[0].Chars, "答错的票也记票长：它和答对的一样长")
	require.Empty(t, st.Last[0].Error, "答错是判定结果，不是探测出错")
}

// TestOpenAITurnStatePairLengthPathStaysOut 钉住 pair 模式下**票长不是判据**：上游铸出一张
// 认得出的健康形态（292）而模型答错时，按票长入池那条路（observeOpenAITurnStateMint）必须让开，
// 否则答错的票会进池——不带 pair、还吃账号级 1 小时有效期，把猎手的开窗判定压住 50 分钟。
func TestOpenAITurnStatePairLengthPathStaysOut(t *testing.T) {
	now := time.Now().UTC()
	healthyShaped := turnStateFernetBlob(now, openAIHealthyTurnStateBlocks)
	require.True(t, openAITurnStateHealthy(healthyShaped), "前提：这张票按长度判是健康的")

	h := newHunterHarness(pairHunterAccount(map[string]any{"max_per_hour": 1}), hunterWebshareProxy)
	resp, _ := hunterResp(http.StatusOK, healthyShaped, recoverySSE("29"))
	h.up.queue = []*http.Response{resp}

	h.run(t)

	require.Empty(t, readOpenAITurnStatePool(h.account), "答错就不入池，票长说什么都不算")
	require.False(t, h.state().Last[0].Healthy)

	// 真实流量那条路同样让开：本出口自己铸的 292 也不入池（实测这种票 0/2 被上游接受）。
	c := turnStateAutoCtxModel("real", hunterTestModel)
	h.gw.observeOpenAITurnStateMint(c, h.account, turnStateFernetBlob(now.Add(time.Second), openAIHealthyTurnStateBlocks))
	require.Empty(t, readOpenAITurnStatePool(h.account), "pair 模式下真实流量的自然铸造也不按票长入池")
}

// TestOpenAITurnStatePairMissDoesNotCoolExitForAWeek 钉住 S1：单次答错不许把一条固定出口封一周。
// 冷却 7 天的理由是「铸什么由出口权重定」，那是对票长这个稳定读数说的；单次答案是噪声。
func TestOpenAITurnStatePairMissDoesNotCoolExitForAWeek(t *testing.T) {
	now := time.Now().UTC()
	// 固定出口（不是 -rotate）：这种代理才会解析出口 IP 并按 IP 冷却。
	h := newHunterHarness(pairHunterAccount(map[string]any{"max_per_hour": 1, "proxy_ids": []any{float64(8)}}), hunterCoxProxy)
	h.up.queue = []*http.Response{pairResp(now, "29", pairCFLB, pairOAILB)}

	h.run(t)

	st := h.state()
	require.Equal(t, "29", st.Last[0].Answer)
	require.NotEmpty(t, st.Last[0].Exit, "固定出口要解析出 IP，否则这条用例什么都没测")
	require.Empty(t, st.Exits, "按做题判的失败不记出口冷却")
	require.False(t, st.exitCoolingDown(st.Last[0].Exit, now))
}

// TestOpenAITurnStatePairProbeWithoutTicketFails 钉住：答对了但上游没铸票 → 这次探测白跑，
// 要算失败（pair 模式要的就是那张票）。恢复探测只要回答，没票无所谓。
func TestOpenAITurnStatePairProbeWithoutTicketFails(t *testing.T) {
	h := newHunterHarness(pairHunterAccount(map[string]any{"max_per_hour": 1}), hunterWebshareProxy)
	noTicket, _ := hunterResp(http.StatusOK, "", recoverySSE("21"))
	h.up.queue = []*http.Response{noTicket}

	h.run(t)

	require.Empty(t, readOpenAITurnStatePool(h.account))
	st := h.state()
	require.Equal(t, "no turn-state in response", st.Last[0].Error)
	require.Equal(t, "21", st.Last[0].Answer, "答案照记：它说明这个出口当时是好的")
}

// TestOpenAITurnStatePairCandidateUsesOwnTTL 钉住 pair 票按自己的有效期过期，不按账号级的
// 1 小时——否则一张 4 分钟前就失效的票会一直压着猎手不补票。
func TestOpenAITurnStatePairCandidateUsesOwnTTL(t *testing.T) {
	now := time.Now().UTC()
	const accountTTL = time.Hour

	stale := openAITurnStateCandidate{
		Blob: "b1", Model: turnStateTestModel, MintedAt: now.Add(-5 * time.Minute), TTLSeconds: 240,
	}
	require.False(t, stale.usable(turnStateTestModel, accountTTL, now), "pair 票 4 分钟就过期")

	legacy := stale
	legacy.TTLSeconds = 0
	require.True(t, legacy.usable(turnStateTestModel, accountTTL, now), "老候选仍按账号级有效期")

	fresh := openAITurnStateCandidate{
		Blob: "b2", Model: turnStateTestModel, MintedAt: now.Add(-time.Minute), TTLSeconds: 240,
	}
	expires, ok := openAITurnStateNewestUsableExpiry([]openAITurnStateCandidate{fresh}, turnStateTestModel, accountTTL, now)
	require.True(t, ok)
	require.Equal(t, fresh.MintedAt.Add(240*time.Second), expires, "开窗判定也要按票自己的有效期算")
}

// TestOpenAITurnStatePairInjectionSeedsCookies 钉住注入形态：票出站的同时，铸票那次的
// __cflb / __oailb 被种回账号罐，由 codexCookies.Attach 写成 Cookie 头随票一起回放。
func TestOpenAITurnStatePairInjectionSeedsCookies(t *testing.T) {
	account := pairHunterAccount(nil)
	repo := newTurnStateAutoRepo()
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo}

	blob := turnStateFernetBlob(time.Now(), openAIPairTurnStateBlocks)
	gw.pushOpenAITurnStateCandidate(turnStateAutoCtxModel("seed", hunterTestModel), account, blob,
		&openAITurnStatePair{Cookies: []string{"__cflb=lb1", "__oailb=jwt1"}, TTLSeconds: 300})

	got, source := gw.resolveOpenAITurnStateOverride(turnStateAutoCtxModel("s", hunterTestModel), account)
	require.Equal(t, blob, got)
	require.Equal(t, turnStateSourceAuto, source)

	header := http.Header{}
	gw.codexCookies.Attach(account, openAITurnStatePairCookieURL, header)
	cookie := header.Get("Cookie")
	require.Contains(t, cookie, "__cflb=lb1")
	require.Contains(t, cookie, "__oailb=jwt1")
	require.Len(t, strings.Split(cookie, "; "), 2, "只回放这一对，别把别的东西带出去")

	// 种进罐里的必须对整个主机有效。属性在入池时被丢掉了，不显式补 Path=/ 的话罐会按「铸票
	// 地址的目录」当作用域（/backend-api/codex），同账号打在那个前缀之外的出站——额度侧信道
	// 的 /backend-api/wham/settings/user 就是一条——会静默漏掉 pair。
	sideband := http.Header{}
	gw.codexCookies.Attach(account, chatGPTSettingsUserURL, sideband)
	require.Equal(t, cookie, sideband.Get("Cookie"), "pair 要对整个主机有效，不只是铸票那条路径")

	// 池里的脏数据不许进罐（手改过 extra、旧版本写下的别的 cookie）。
	dirty := pairHunterAccount(nil)
	dirty.ID = account.ID + 1
	gw.seedOpenAITurnStatePairCookies(dirty, []string{"session=stolen", "__cf_bm=bm"}, 300)
	dirtyHeader := http.Header{}
	gw.codexCookies.Attach(dirty, openAITurnStatePairCookieURL, dirtyHeader)
	require.Empty(t, dirtyHeader.Get("Cookie"))
}

// TestOpenAITurnStatePairRemintIsReadOnly 钉住重铸只是读数：注入后上游又铸新票 → 候选上记
// 一次计数，**不判失效、不停号**。那条判据 2026-09-19 已经错过一次（铸什么由账号权重定）。
func TestOpenAITurnStatePairRemintIsReadOnly(t *testing.T) {
	now := time.Now().UTC()
	account := pairHunterAccount(nil)
	repo := newTurnStateAutoRepo()
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo}

	blob := turnStateFernetBlob(now, openAIPairTurnStateBlocks)
	gw.pushOpenAITurnStateCandidate(turnStateAutoCtxModel("seed", hunterTestModel), account, blob,
		&openAITurnStatePair{Cookies: []string{"__cflb=lb1"}, TTLSeconds: 240})

	c := turnStateAutoCtxModel("s", hunterTestModel)
	markOpenAITurnStateInjected(c, blob, turnStateSourceAuto)
	remint := turnStateFernetBlob(now.Add(time.Second), openAIPairTurnStateBlocks)
	gw.observeOpenAITurnStateMint(c, account, remint)

	pool := readOpenAITurnStatePool(account)
	require.Len(t, pool, 1, "重铸不入池：新票没经过做题判定")
	require.Equal(t, 1, pool[0].Reminted)
	require.False(t, pool[0].Failed, "重铸不是票坏了的证据")
	require.Empty(t, repo.schedulable, "重铸不停号")

	// 同一个上下文里观测两次只算一次：applyAttemptResponseHeaders 有两个调用点。
	gw.observeOpenAITurnStateMint(c, account, remint)
	require.Equal(t, 1, readOpenAITurnStatePool(account)[0].Reminted)

	// 上游把同一条票原样回带（带票请求 92% 是这样）不算重铸。
	echo := turnStateAutoCtxModel("s2", hunterTestModel)
	markOpenAITurnStateInjected(echo, blob, turnStateSourceAuto)
	gw.observeOpenAITurnStateMint(echo, account, blob)
	require.Equal(t, 1, readOpenAITurnStatePool(account)[0].Reminted)
}

// TestOpenAITurnStatePairExhaustionKeepsAccountRunning 钉住 pair 模式的耗尽口径：票被上游以
// invalid_encrypted_content 拒掉、该模型一张不剩时只记日志，**不停调度**——票本来就活得短、
// 每张都是探测现摇的，一轮全灭是常态，停号会把实验性功能升级成线上事故。老路径照旧停。
func TestOpenAITurnStatePairExhaustionKeepsAccountRunning(t *testing.T) {
	reject := func(pairMode bool) *turnStateAutoRepo {
		overrides := map[string]any{}
		if !pairMode {
			overrides["pair_mode"] = false
		}
		account := pairHunterAccount(overrides)
		repo := newTurnStateAutoRepo()
		repo.latest = account
		gw := &OpenAIGatewayService{accountRepo: repo}

		blob := turnStateFernetBlob(time.Now(), openAIPairTurnStateBlocks)
		gw.pushOpenAITurnStateCandidate(turnStateAutoCtxModel("seed", hunterTestModel), account, blob,
			&openAITurnStatePair{Cookies: []string{"__cflb=lb1"}, TTLSeconds: 240})

		c := turnStateAutoCtxModel("s", hunterTestModel)
		markOpenAITurnStateInjected(c, blob, turnStateSourceAuto)
		gw.noteOpenAITurnStateRejected(c, account)

		pool := readOpenAITurnStatePool(account)
		require.True(t, pool[0].Failed, "上游明确拒绝仍然是失效的硬证据")
		return repo
	}

	require.Empty(t, reject(true).schedulable, "pair 模式耗尽不停号")
	// 开关关掉之后池里还躺着只活 4 分钟的 pair 票，那几分钟里撞一次拒绝也不该停号：
	// 判据看被拒的那张票自己带不带 pair，不看开关此刻开没开。
	require.Empty(t, reject(false).schedulable, "关了开关，残留的 pair 票被拒也不停号")

	// 老候选（不带 pair）被拒到耗尽仍然停号：那条路没被这次改动改掉。
	legacyAccount := pairHunterAccount(map[string]any{"pair_mode": false})
	legacyRepo := newTurnStateAutoRepo()
	legacyRepo.latest = legacyAccount
	legacyGW := &OpenAIGatewayService{accountRepo: legacyRepo}
	legacyBlob := turnStateFernetBlob(time.Now(), openAIHealthyTurnStateBlocks)
	legacyGW.pushOpenAITurnStateCandidate(turnStateAutoCtxModel("seed", hunterTestModel), legacyAccount, legacyBlob, nil)
	legacyCtx := turnStateAutoCtxModel("s", hunterTestModel)
	markOpenAITurnStateInjected(legacyCtx, legacyBlob, turnStateSourceAuto)
	legacyGW.noteOpenAITurnStateRejected(legacyCtx, legacyAccount)
	require.Equal(t, []bool{false}, legacyRepo.schedulable, "老路径耗尽照旧停号")
}

// TestOpenAITurnStatePairCookiesFilters 钉住 cookie 提取：只收钉路由的那两个、只留
// name=value、空值与超长值不要。
func TestOpenAITurnStatePairCookiesFilters(t *testing.T) {
	h := http.Header{}
	h.Add("Set-Cookie", pairCFLB)
	h.Add("Set-Cookie", pairOAILB)
	h.Add("Set-Cookie", "__cf_bm=bm; Path=/") // 按 TLS 指纹 + IP 发，跨出口回放自相矛盾
	h.Add("Set-Cookie", "oai-sc=sess; Path=/")
	h.Add("Set-Cookie", "__cfruid=ruid; Path=/") // 在罐的白名单里，但不在 pair 白名单里
	require.Equal(t, []string{"__cflb=0H28vzvP8q1Xy", "__oailb=eyJhbGciOiJFUzI1NiJ9.payload.sig"},
		openAITurnStatePairCookies(h), "属性一律丢掉，只留 name=value")

	require.Nil(t, openAITurnStatePairCookies(http.Header{}))
	require.Nil(t, openAITurnStatePairCookies(nil))

	empty := http.Header{}
	empty.Add("Set-Cookie", "__cflb=; Path=/")
	require.Nil(t, openAITurnStatePairCookies(empty), "空值回放不了")

	long := http.Header{}
	long.Add("Set-Cookie", "__oailb="+strings.Repeat("x", openAITurnStatePairCookieValueKeep+1))
	require.Nil(t, openAITurnStatePairCookies(long), "超长的不收：pair 要进 extra，得有界")
}

// TestOpenAITurnStatePairConfigValidation 钉住配置校验：开窗提前量必须比票寿命短，
// 否则「任何票都永远不够用」、猎手每小时打满上限也停不下来。
func TestOpenAITurnStatePairConfigValidation(t *testing.T) {
	extra := func(overrides map[string]any) map[string]any {
		return map[string]any{openAITurnStateHunterExtraKey: hunterConfig(overrides)}
	}

	require.Error(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{"pair_mode": "true"})),
		"pair_mode 写成字符串时开关会静默失效，要当场拒")
	// 没填 lead_minutes 就是「按默认来」，交给 applyDefaults 削——不能拒，否则在编辑弹窗里
	// 勾一下 pair 模式（那个输入框留空 → 前端不写这个键）就存不进去。
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{"pair_mode": true})),
		"只勾 pair、不动开窗提前量必须存得下去")
	require.Error(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{
		"pair_mode": true, "lead_minutes": float64(10),
	})), "显式填的 10 分钟比默认票寿命 120 秒长，当场拒")
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{
		"pair_mode": true, "lead_minutes": float64(1),
	})))
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{
		"pair_mode": true, "ticket_ttl_seconds": float64(900),
	})), "票寿命放到 15 分钟，默认 10 分钟开窗就合法了")
	// 低于下限是配错，不是「取默认」：与开窗提前量那条约束分开测，否则谁在拦它分不清。
	require.Error(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{
		"ticket_ttl_seconds": float64(10),
	})), "票寿命低于下限是配错，不是取默认")
	require.Error(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{
		"pair_mode": true, "lead_minutes": float64(1), "ticket_ttl_seconds": float64(10),
	})))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{
		"pair_mode": true, "lead_minutes": float64(1), "ticket_ttl_seconds": float64(openAITurnStateHunterMaxTicketTTL + 1),
	})))
	// pair 关着时这两条约束都不该出现（老路径的票寿命是账号级的 1 小时）。
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(extra(map[string]any{"ticket_ttl_seconds": float64(240)})))
}

// TestOpenAITurnStatePairDefaultsClampLead 钉住绕过 handler 校验落库的行（数据导入）也不会
// 让猎手空转：applyDefaults 把开窗提前量削到票寿命以内。
func TestOpenAITurnStatePairDefaultsClampLead(t *testing.T) {
	cfg := openAITurnStateHunterConfig{Enabled: true, PairMode: true, LeadMinutes: 10}
	cfg.applyDefaults()
	require.Equal(t, defaultOpenAITurnStateHuntTicketTTL, cfg.TicketTTLSeconds)
	require.Equal(t, 1, cfg.LeadMinutes)
	require.Less(t, int(cfg.lead().Seconds()), cfg.TicketTTLSeconds, "开窗提前量必须比票寿命短")

	legacy := openAITurnStateHunterConfig{Enabled: true, LeadMinutes: 10}
	legacy.applyDefaults()
	require.Equal(t, defaultOpenAITurnStateHuntLeadMinutes, legacy.LeadMinutes, "老路径的提前量不受影响")

	tiny := openAITurnStateHunterConfig{Enabled: true, PairMode: true, LeadMinutes: 10, TicketTTLSeconds: 5}
	tiny.applyDefaults()
	require.Equal(t, openAITurnStateHunterMinTicketTTL, tiny.TicketTTLSeconds, "票寿命有下限")
	require.Less(t, int(tiny.lead().Seconds()), tiny.TicketTTLSeconds)
}
