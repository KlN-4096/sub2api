//go:build unit

package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// recoveryConfig 造一份恢复探测配置：默认区间 30–90 分钟、总次数 5、成功次数 4、冷却 16 小时。
func recoveryConfig(overrides map[string]any) map[string]any {
	cfg := map[string]any{"enabled": true, "model": hunterTestModel}
	for k, v := range overrides {
		cfg[k] = v
	}
	return cfg
}

// recoveryAccount 造一个只开恢复探测、**不开猎手**的账号：这个功能是独立开关。
func recoveryAccount(recovery map[string]any) *Account {
	a := hunterTestAccount(map[string]any{"enabled": false})
	a.Extra[openAITurnStateRecoveryExtraKey] = recovery
	proxy := hunterCoxProxy
	a.ProxyID, a.Proxy = &proxy.ID, &proxy
	return a
}

func recoveryState(a *Account) openAITurnStateRecoveryState {
	return readOpenAITurnStateRecoveryState(a)
}

// setRecoveryState 按 JSONB 读回来的形态写运行态（time.Time 变字符串），与生产落库一致。
func setRecoveryState(a *Account, st openAITurnStateRecoveryState) {
	encoded, err := json.Marshal(st)
	if err != nil {
		panic(err)
	}
	var generic map[string]any
	if err := json.Unmarshal(encoded, &generic); err != nil {
		panic(err)
	}
	a.Extra[openAITurnStateRecoveryStateExtraKey] = generic
}

// recoverySSE 造一份 Codex 后端真实形态的探测响应体（2026-09-18 实抓）：正文只在
// output_text.delta / output_item.done 里，**completed 的 output 是 []**，只带 usage。
func recoverySSE(answer string) string {
	text, _ := json.Marshal(answer)
	return strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_probe","model":"gpt-5.6-sol","status":"in_progress","output":[]}}`,
		``,
		`event: response.output_item.done`,
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","summary":[]}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":` + string(text) + `}`,
		``,
		`event: response.output_item.done`,
		`data: {"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":` + string(text) + `,"annotations":[]}]}}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"id":"resp_probe","model":"gpt-5.6-sol","status":"completed","output":[],"usage":{"input_tokens":1200,"output_tokens":300}}}`,
		``, ``,
	}, "\n")
}

// recoverySSEInlineOutput 是 Responses API 标准形态：completed 的 output 里自带 message。
func recoverySSEInlineOutput(answer string) string {
	text, _ := json.Marshal(answer)
	return strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_probe","model":"gpt-5.6-sol"}}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"id":"resp_probe","model":"gpt-5.6-sol","status":"completed","output":[{"type":"reasoning","summary":[]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":` + string(text) + `}]}],"usage":{"input_tokens":1200,"output_tokens":300}}}`,
		``, ``,
	}, "\n")
}

// queueAnswers 往替身上游排一串探测响应：每个 answer 一条 200 + 292 票 + 完整 SSE 体。
func queueAnswers(h *hunterHarness, minted time.Time, answers ...string) {
	for _, answer := range answers {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(minted, openAIHealthyTurnStateBlocks), recoverySSE(answer))
		h.up.queue = append(h.up.queue, resp)
	}
}

// TestOpenAITurnStateRecoveryMarksAtSuccessRate 钉住主判据：最近 5 次里答对 4 次 → 标记已恢复；
// 猎手关着也照跑；每次只探一次、间隔落在配置区间内；标记之后不再探。
func TestOpenAITurnStateRecoveryMarksAtSuccessRate(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	clock := now
	h.svc.now = func() time.Time { return clock }
	answers := []string{"21", "21", "29", "21", "21"}
	queueAnswers(h, now, append(answers, "21")...)

	for i, answer := range answers {
		h.run(t)
		st := recoveryState(h.account)
		require.Len(t, h.up.requests, i+1, "一个 tick 只探一次")
		require.Equal(t, answer, st.Last[0].Answer)
		require.Equal(t, answer == "21", st.Last[0].Healthy)
		if i < len(answers)-1 {
			require.True(t, st.RecoveredAt.IsZero(), "没攒够 4 次不许判定恢复")
			wait := st.NextAt.Sub(clock)
			require.GreaterOrEqual(t, wait, time.Duration(defaultOpenAITurnStateRecoveryMinMinutes)*time.Minute)
			require.LessOrEqual(t, wait, time.Duration(defaultOpenAITurnStateRecoveryMaxMinutes)*time.Minute)
			h.run(t)
			require.Len(t, h.up.requests, i+1, "没到点不探")
			clock = st.NextAt
		}
	}

	st := recoveryState(h.account)
	require.False(t, st.RecoveredAt.IsZero(), "5 次里答对 4 次 = 降智恢复")
	require.Equal(t, []bool{true, true, false, true, true}, st.Results)
	require.Equal(t, 4, st.successes())

	// 判定之后不再探：结论已经有了，继续探只是白付额度。
	clock = clock.Add(24 * time.Hour)
	h.run(t)
	require.Len(t, h.up.requests, len(answers))
}

// TestOpenAITurnStateRecoveryWindowSlides 钉住窗口语义：答错不清零，只是把窗口往前推；成功次数
// 不够就继续探，够了才判。
func TestOpenAITurnStateRecoveryWindowSlides(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	clock := now
	h.svc.now = func() time.Time { return clock }
	queueAnswers(h, now, "21", "29", "21", "29", "21", "21", "21")

	advance := func() {
		h.run(t)
		clock = recoveryState(h.account).NextAt
	}
	for range 5 {
		advance()
	}
	st := recoveryState(h.account)
	require.Equal(t, 3, st.successes(), "5 次里 3 对")
	require.True(t, st.RecoveredAt.IsZero())
	require.Zero(t, st.FailStreak, "最后一次答对，连续失败清零")

	advance() // 第 6 次：窗口变成 21,21,29,21,29 → 仍 3 对
	st = recoveryState(h.account)
	require.Equal(t, []bool{true, true, false, true, false}, st.Results)
	require.True(t, st.RecoveredAt.IsZero())

	advance() // 第 7 次：21,21,21,29,21 → 4 对
	st = recoveryState(h.account)
	require.Equal(t, 4, st.successes())
	require.False(t, st.RecoveredAt.IsZero(), "窗口滑到 4/5 就判恢复")
}

// TestOpenAITurnStateRecoveryCooldownAfterFailures 钉住用户要的 CD：连续 5 次失败 → 冷却 16 小时，
// 冷却期内不探、这一批结果作废；醒来后失败计数从零开始。传输错误与答错同样算失败。
func TestOpenAITurnStateRecoveryCooldownAfterFailures(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	clock := now
	h.svc.now = func() time.Time { return clock }
	queueAnswers(h, now, "29", "36", "", "29")
	h.up.queue = append(h.up.queue, nil) // 第五次：传输错误
	h.up.errOnNil = errors.New("dial tcp: proxy refused")

	lastProbeAt := clock
	for i := 1; i <= defaultOpenAITurnStateRecoveryStreak; i++ {
		lastProbeAt = clock
		h.run(t)
		require.Len(t, h.up.requests, i)
		clock = recoveryState(h.account).NextAt
	}

	st := recoveryState(h.account)
	require.Zero(t, st.FailStreak, "进冷却时计数归零，醒来不能一探就又满")
	require.Empty(t, st.Results, "进冷却时这一批结果作废")
	require.True(t, st.RecoveredAt.IsZero())
	require.Equal(t, lastProbeAt.Add(defaultOpenAITurnStateRecoveryCooldownHours*time.Hour), st.CoolingUntil,
		"连续失败够数 → 从最后一次探测起冷却 16 小时")
	require.Equal(t, st.CoolingUntil, st.NextAt)

	// 冷却期内不探。
	clock = st.CoolingUntil.Add(-time.Minute)
	h.run(t)
	require.Len(t, h.up.requests, defaultOpenAITurnStateRecoveryStreak)

	// 醒来接着探。
	queueAnswers(h, now, "21")
	h.up.errOnNil = nil
	clock = st.CoolingUntil
	h.run(t)
	require.Len(t, h.up.requests, defaultOpenAITurnStateRecoveryStreak+1)
	require.Equal(t, []bool{true}, recoveryState(h.account).Results)
}

// TestOpenAITurnStateRecoveryUsesAccountExit 钉住「走账号自己的出口」：不是猎手代理、不关连接
// （关了会把真实流量正在用的那条隧道带走），铸出的 292 照常入池。
func TestOpenAITurnStateRecoveryUsesAccountExit(t *testing.T) {
	now := time.Now().UTC()
	account := recoveryAccount(recoveryConfig(nil))
	account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"proxy_ids": []any{float64(20)}})
	h := newHunterHarness(account, hunterWebshareProxy, hunterCoxProxy)
	h.svc.now = func() time.Time { return now }
	queueAnswers(h, now, "21")

	h.run(t)

	require.NotEmpty(t, h.up.requests)
	require.Contains(t, h.up.proxyURLs[0], hunterCoxProxy.Host, "走账号绑定的出口，不是猎手代理")
	require.NotContains(t, h.up.proxyURLs[0], hunterWebshareProxy.Host)
	require.False(t, h.up.requests[0].Close, "复用连接：关连接会踢掉真实流量的 HTTP/2 隧道")
	require.Empty(t, h.prober.calls, "自己的出口不需要回声")
	require.Equal(t, hunterCoxProxy.ID, recoveryState(h.account).Last[0].ProxyID)
	require.Len(t, readOpenAITurnStatePool(h.account), 1, "同出口铸的 292 照常入池")
}

// TestOpenAITurnStateRecoveryDirectAccount 钉住没绑代理的账号也能探（直连，空代理 URL）。
func TestOpenAITurnStateRecoveryDirectAccount(t *testing.T) {
	now := time.Now().UTC()
	account := recoveryAccount(recoveryConfig(nil))
	account.ProxyID, account.Proxy = nil, nil
	h := newHunterHarness(account)
	h.svc.now = func() time.Time { return now }
	queueAnswers(h, now, "21")

	h.run(t)

	require.Len(t, h.up.requests, 1)
	require.Empty(t, h.up.proxyURLs[0], "直连")
	require.Equal(t, []bool{true}, recoveryState(h.account).Results)
}

// TestOpenAITurnStateRecoveryProbeShape 钉住探测本身：默认探 gpt-5.6-sol@medium、用户消息是糖果题、
// 读完整个体并按回答判定；用量按上游 completed 里的真实数记，不再是「输入估算 + 输出 0」。
func TestOpenAITurnStateRecoveryProbeShape(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(map[string]any{"model": "", "usage_api_key_id": float64(77)})), hunterCoxProxy)
	keys := &hunterAPIKeys{key: &APIKey{ID: 77, User: &User{ID: 5}, Group: &Group{ID: 3, RateMultiplier: 1}}}
	capture := &probeUsageCapture{}
	h.svc.SetAPIKeys(keys)
	h.svc.recordUsage = capture.record
	h.svc.now = func() time.Time { return now }
	queueAnswers(h, now, "21。")

	h.run(t)

	require.Len(t, h.up.bodies, 1)
	body := h.up.bodies[0]
	require.Equal(t, defaultOpenAITurnStateRecoveryModel, gjson.GetBytes(body, "model").String())
	require.Equal(t, defaultOpenAITurnStateRecoveryEffort, gjson.GetBytes(body, "reasoning.effort").String())
	require.Equal(t, openAITurnStateRecoveryPrompt, gjson.GetBytes(body, "input.0.content.0.text").String())
	require.True(t, gjson.GetBytes(body, "stream").Bool())

	st := recoveryState(h.account)
	require.Equal(t, defaultOpenAITurnStateRecoveryModel, st.Last[0].Model)
	require.Equal(t, "21", st.Last[0].Answer, "句末标点不算答错")
	require.True(t, st.Last[0].Healthy)

	require.Len(t, capture.inputs, 1)
	require.Equal(t, RequestTypeTurnStateProbe, capture.inputs[0].RequestType)
	require.Equal(t, 1200, capture.inputs[0].Result.Usage.InputTokens, "用上游报的输入 token")
	require.Equal(t, 300, capture.inputs[0].Result.Usage.OutputTokens, "读完了流，输出 token 不再记 0")
}

// TestOpenAITurnStateRecoveryAnswerNormalization 钉住回答的归一化、「含 21 即答对」（用户 2026-09-25 定）
// 与终态缺失。
func TestOpenAITurnStateRecoveryAnswerNormalization(t *testing.T) {
	type want struct {
		answer  string
		healthy bool
	}
	for raw, w := range map[string]want{
		"21": {"21", true}, " 21。": {"21", true}, "**21**": {"21", true}, "\"21\"": {"21", true}, "21.": {"21", true},
		"**21**。": {"21", true}, "「21」": {"21", true}, "21颗": {"21颗", true}, "答案是 21": {"答案是 21", true},
		"一共 21 颗糖": {"一共 21 颗糖", true}, "21.5": {"21.5", true}, "210": {"210", true},
		"29": {"29", false}, "36": {"36", false}, "12": {"12", false}, "2 1": {"2 1", false},
	} {
		answer, healthy, usage, ok := openAITurnStateRecoveryAnswer([]byte(recoverySSE(raw)))
		require.True(t, ok, raw)
		require.Equal(t, w.answer, answer, raw)
		require.Equal(t, w.healthy, healthy, raw)
		require.NotNil(t, usage, raw)
		require.Equal(t, 1200, usage.InputTokens)
		require.Equal(t, 300, usage.OutputTokens)
	}

	// 长篇解释里 21 出现在展示截断（200 字节）之后也算答对：判定看整段，截断只影响展示。
	long := strings.Repeat("先算每人拿到的糖数再相加。", 30) + "所以一共是 21 颗。"
	answer, healthy, _, ok := openAITurnStateRecoveryAnswer([]byte(recoverySSE(long)))
	require.True(t, ok)
	require.True(t, healthy)
	require.LessOrEqual(t, len(answer), openAITurnStateRecoveryAnswerKeep)
	require.NotContains(t, answer, "21", "截断后的展示串里已经没有 21，说明判定不是看它")

	_, _, _, ok = openAITurnStateRecoveryAnswer([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"x\"}}\n\n"))
	require.False(t, ok, "没有 completed 终态就不算有回答")
	_, _, _, ok = openAITurnStateRecoveryAnswer(nil)
	require.False(t, ok)

	answer, healthy, usage, ok := openAITurnStateRecoveryAnswer([]byte(recoverySSENoUsage("21")))
	require.True(t, ok)
	require.True(t, healthy)
	require.Equal(t, "21", answer)
	require.Nil(t, usage, "终态没带 usage 时不能当 0 token 记账")

	// Responses 标准形态（completed 自带 output）同样认。
	answer, healthy, _, ok = openAITurnStateRecoveryAnswer([]byte(recoverySSEInlineOutput("29")))
	require.True(t, ok)
	require.False(t, healthy)
	require.Equal(t, "29", answer)
}

// TestOpenAITurnStateRecoveryEmptyAnswerIsDescribed 钉住用户 09-25 的要求：日志要写明模型答了什么，
// 不能只有一个 ✗。模型交白卷时回答写成形态说明（status / output 项类型 / refusal），且判为答错；
// 09-25 首次线上探测正是 completed output=[] 让回答落成空、页面只显示「答 -✗」。
func TestOpenAITurnStateRecoveryEmptyAnswerIsDescribed(t *testing.T) {
	answer, healthy, usage, ok := openAITurnStateRecoveryAnswer([]byte(recoverySSE("")))
	require.True(t, ok)
	require.False(t, healthy)
	require.Equal(t, "∅ status=completed output=[reasoning,message]", answer)
	require.NotNil(t, usage)

	refused := strings.Join([]string{
		`event: response.output_item.done`,
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"I can't help with that."}]}}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"id":"resp_probe","status":"completed","output":[],"incomplete_details":null}}`,
		``, ``,
	}, "\n")
	answer, healthy, _, ok = openAITurnStateRecoveryAnswer([]byte(refused))
	require.True(t, ok)
	require.False(t, healthy)
	require.Equal(t, "∅ status=completed refusal=I can't help with that. output=[message]", answer)

	// 走完整探测链路：白卷要落进 attempt.answer 与 results=false。
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	h.svc.now = func() time.Time { return now }
	queueAnswers(h, now, "")
	h.run(t)
	st := recoveryState(h.account)
	require.False(t, st.Last[0].Healthy)
	require.Equal(t, "∅ status=completed output=[reasoning,message]", st.Last[0].Answer)
	require.Empty(t, st.Last[0].Error, "白卷是答错，不是探测出错")
	raw := h.account.Extra[openAITurnStateRecoveryStateExtraKey].(map[string]any)
	require.Contains(t, raw["last"].([]any)[0].(map[string]any), "answer", "落库里要有 answer 键，页面才能显示")
}

// recoverySSENoUsage 是 completed 里不带 usage 的探测响应体。
func recoverySSENoUsage(answer string) string {
	return strings.Replace(recoverySSE(answer), `,"usage":{"input_tokens":1200,"output_tokens":300}`, "", 1)
}

// TestOpenAITurnStateRecoveryBillsEstimateWithoutUsage 钉住第二轮评审 S4：终态没带 usage 时按
// 输入估算记账（与猎手同一套），不能记成 0。
func TestOpenAITurnStateRecoveryBillsEstimateWithoutUsage(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(map[string]any{"usage_api_key_id": float64(77)})), hunterCoxProxy)
	keys := &hunterAPIKeys{key: &APIKey{ID: 77, User: &User{ID: 5}, Group: &Group{ID: 3, RateMultiplier: 1}}}
	capture := &probeUsageCapture{}
	h.svc.SetAPIKeys(keys)
	h.svc.recordUsage = capture.record
	h.svc.now = func() time.Time { return now }
	resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), recoverySSENoUsage("21"))
	h.up.queue = append(h.up.queue, resp)

	h.run(t)

	require.True(t, recoveryState(h.account).Last[0].Healthy)
	require.Len(t, capture.inputs, 1)
	require.Equal(t, openAITurnStateProbeInputTokens(hunterTestModel, defaultOpenAITurnStateRecoveryEffort, openAITurnStateRecoveryPrompt),
		capture.inputs[0].Result.Usage.InputTokens)
	require.Zero(t, capture.inputs[0].Result.Usage.OutputTokens)
}

// TestOpenAITurnStateRecoveryLegacyMarkerReprobes 钉住第二轮评审 S2：旧判据（连续 292）打下的
// 「已恢复」没有判定窗口，不能让它永久停探——清掉标记照常探，判定从头攒。
func TestOpenAITurnStateRecoveryLegacyMarkerReprobes(t *testing.T) {
	now := time.Now().UTC()
	account := recoveryAccount(recoveryConfig(nil))
	account.Extra[openAITurnStateRecoveryStateExtraKey] = map[string]any{
		"streak":       float64(5),
		"recovered_at": now.Add(-48 * time.Hour).Format(time.RFC3339),
		"next_at":      now.Add(-47 * time.Hour).Format(time.RFC3339),
		"updated_at":   now.Add(-48 * time.Hour).Format(time.RFC3339),
	}
	h := newHunterHarness(account, hunterCoxProxy)
	h.svc.now = func() time.Time { return now }
	queueAnswers(h, now, "29")

	h.run(t)

	require.Len(t, h.up.requests, 1, "老标记不算数，照常探")
	st := recoveryState(h.account)
	require.True(t, st.RecoveredAt.IsZero(), "老标记清掉，落库")
	require.Equal(t, []bool{false}, st.Results)
	require.Equal(t, 1, st.FailStreak)

	// 老行的 next_at 还在未来：不探，但标记要立刻清掉落库，页面不能再写「已恢复」。
	waiting := recoveryAccount(recoveryConfig(nil))
	waiting.Extra[openAITurnStateRecoveryStateExtraKey] = map[string]any{
		"streak":       float64(5),
		"recovered_at": now.Add(-time.Hour).Format(time.RFC3339),
		"next_at":      now.Add(time.Hour).Format(time.RFC3339),
	}
	h = newHunterHarness(waiting, hunterCoxProxy)
	h.svc.now = func() time.Time { return now }

	h.run(t)

	require.Empty(t, h.up.requests, "没到点不探")
	require.True(t, recoveryState(h.account).RecoveredAt.IsZero())
	require.Len(t, h.repo.extraWrites, 1, "清老标记写一次库")

	h.run(t)
	require.Len(t, h.repo.extraWrites, 1, "清过就不再重复写")
}

// TestOpenAITurnStateRecoveryTruncatedStreamIsFailure 钉住流被截断（没有终态）按失败记，不按答对也不按答错。
func TestOpenAITurnStateRecoveryTruncatedStreamIsFailure(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	h.svc.now = func() time.Time { return now }
	resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks),
		"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"x\"}}\n\n")
	h.up.queue = append(h.up.queue, resp)

	h.run(t)

	st := recoveryState(h.account)
	require.Equal(t, []bool{false}, st.Results)
	require.Equal(t, 1, st.FailStreak)
	require.Equal(t, "no completed response", st.LastError)
}

// TestOpenAITurnStateRecoveryResetOnNatural312 钉住标记的失效：真实流量又自然铸出 312 →
// 清掉「已恢复」与判定窗口，从头攒；注入回声（带票的请求）不算。
func TestOpenAITurnStateRecoveryResetOnNatural312(t *testing.T) {
	now := time.Now().UTC()
	repo := newTurnStateAutoRepo()
	account := recoveryAccount(recoveryConfig(nil))
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo}
	st := openAITurnStateRecoveryState{Results: []bool{true, true, true, true}, RecoveredAt: now}
	setRecoveryState(account, st)

	// 注入过的响应不算数：92% 的带票请求上游原样回带，拿它判定就是拿自己的票当证据。
	c := turnStateAutoCtxModel("echo", hunterTestModel)
	c.Set(ctxKeyTurnStateInjected, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks))
	gw.observeOpenAITurnStateMint(c, account, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	require.False(t, recoveryState(account).RecoveredAt.IsZero(), "回声不清标记")

	// 自然铸造的 312 才算。
	gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("natural", hunterTestModel), account,
		turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	got := recoveryState(account)
	require.True(t, got.RecoveredAt.IsZero(), "又铸 312 = 之前判定的恢复不作数")
	require.Empty(t, got.Results)

	// 标记已经清掉之后不再重复写库（响应热路径）。
	before := repo.extraWrites
	gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("natural-2", hunterTestModel), account,
		turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	require.Len(t, repo.extraWrites, len(before), "标记不在时不写库")
}

// TestOpenAITurnStateRecoverySkips 钉住不该探的场景。
func TestOpenAITurnStateRecoverySkips(t *testing.T) {
	now := time.Now().UTC()
	cases := map[string]func(*Account){
		"关着": func(a *Account) {
			a.Extra[openAITurnStateRecoveryExtraKey] = recoveryConfig(map[string]any{"enabled": false})
		},
		"没配置":         func(a *Account) { delete(a.Extra, openAITurnStateRecoveryExtraKey) },
		"账号不是 active": func(a *Account) { a.Status = StatusError },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			account := recoveryAccount(recoveryConfig(nil))
			mutate(account)
			h := newHunterHarness(account, hunterCoxProxy)
			h.svc.now = func() time.Time { return now }
			queueAnswers(h, now, "21")

			h.run(t)

			require.Empty(t, h.up.requests)
		})
	}
}

// TestOpenAITurnStateRecoveryDisableClearsState 钉住「关掉开关即清」：残留的标记与窗口在下一个
// tick 清空（只写一次库），再开从头攒。
func TestOpenAITurnStateRecoveryDisableClearsState(t *testing.T) {
	now := time.Now().UTC()
	account := recoveryAccount(recoveryConfig(map[string]any{"enabled": false}))
	setRecoveryState(account, openAITurnStateRecoveryState{Results: []bool{true, true, true, true}, RecoveredAt: now, FailStreak: 0})
	h := newHunterHarness(account, hunterCoxProxy)
	h.svc.now = func() time.Time { return now }

	h.run(t)

	st := recoveryState(h.account)
	require.True(t, st.RecoveredAt.IsZero(), "关掉开关，标记随之清空")
	require.Empty(t, st.Results)
	require.Empty(t, h.up.requests, "关着不探")
	writes := len(h.repo.extraWrites)
	require.Positive(t, writes)

	h.run(t)
	require.Len(t, h.repo.extraWrites, writes, "清空之后不再重复写库")

	// 再开：从头攒。
	h.account.Extra[openAITurnStateRecoveryExtraKey] = recoveryConfig(nil)
	queueAnswers(h, now, "21")
	h.run(t)
	require.Equal(t, []bool{true}, recoveryState(h.account).Results)
}

// TestOpenAITurnStateRecoveryConfigBounds 钉住配置兜底：区间填反按固定间隔走，非法值取默认，
// 成功次数不能超过总次数，模型 / effort 留空走 gpt-5.6-sol@medium。
func TestOpenAITurnStateRecoveryConfigBounds(t *testing.T) {
	account := recoveryAccount(recoveryConfig(map[string]any{
		"model": "", "min_minutes": float64(90), "max_minutes": float64(30),
		"streak_target": float64(0), "success_target": float64(0), "cooldown_hours": float64(0), "reasoning_effort": "bogus",
	}))
	cfg, ok := readOpenAITurnStateRecoveryConfig(account)
	require.True(t, ok)
	require.Equal(t, 90, cfg.MinMinutes)
	require.Equal(t, 90, cfg.MaxMinutes, "填反了按固定间隔走，别让区间变负数")
	require.Equal(t, 90*time.Minute, cfg.interval())
	require.Equal(t, defaultOpenAITurnStateRecoveryStreak, cfg.StreakTarget)
	require.Equal(t, defaultOpenAITurnStateRecoverySuccess, cfg.SuccessTarget)
	require.Equal(t, defaultOpenAITurnStateRecoveryCooldownHours*time.Hour, cfg.cooldown())
	require.Equal(t, defaultOpenAITurnStateRecoveryEffort, cfg.ReasoningEffort)
	require.Equal(t, defaultOpenAITurnStateRecoveryModel, cfg.Model)

	capped := recoveryAccount(recoveryConfig(map[string]any{
		"min_minutes": float64(99999), "max_minutes": float64(99999), "cooldown_hours": float64(99999),
		"streak_target": float64(3), "success_target": float64(9),
	}))
	cfg, _ = readOpenAITurnStateRecoveryConfig(capped)
	require.Equal(t, openAITurnStateRecoveryMaxMinutes, cfg.MaxMinutes)
	require.Equal(t, openAITurnStateRecoveryMaxCooldownHours, cfg.CooldownHours)
	require.Equal(t, 3, cfg.StreakTarget)
	require.Equal(t, 3, cfg.SuccessTarget, "成功次数压到总次数")

	small := recoveryAccount(recoveryConfig(map[string]any{"streak_target": float64(2)}))
	cfg, _ = readOpenAITurnStateRecoveryConfig(small)
	require.Equal(t, 2, cfg.SuccessTarget, "默认 4 也不能超过总次数 2")
}

// TestValidateOpenAITurnStateRecoveryExtra 钉住管理员配置的校验（随猎手配置一起在 handler 入口调）。
func TestValidateOpenAITurnStateRecoveryExtra(t *testing.T) {
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"success_target": float64(4)})}))
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"streak_target": float64(6), "success_target": float64(6)})}),
		"成功次数等于总次数是合法的（全对才算恢复）")

	nulled := map[string]any{openAITurnStateRecoveryExtraKey: nil}
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(nulled))
	require.NotContains(t, nulled, openAITurnStateRecoveryExtraKey, "null 等价于未配置")

	for _, bad := range []map[string]any{
		{openAITurnStateRecoveryExtraKey: "on"},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"enabled": "yes"})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"model": float64(1)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"streak_target": float64(0)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"success_target": float64(0)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"success_target": float64(6)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"streak_target": float64(3), "success_target": float64(4)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"cooldown_hours": float64(1.5)})},
		{openAITurnStateRecoveryExtraKey: recoveryConfig(map[string]any{"reasoning_effort": "bogus"})},
	} {
		require.Error(t, ValidateOpenAITurnStateHunterExtra(bad))
	}
}

// TestOpenAITurnStateRecoveryNotResetByHunterOrEcho 钉住第一轮评审 B1：清标记只认真实流量自己
// 铸出的 312。猎手走的是**别的出口**，恢复探测的失败自己记；带票请求 92% 是上游原样回带——
// 三者都清一次窗口的话，降智账号永远攒不满。
func TestOpenAITurnStateRecoveryNotResetByHunterOrEcho(t *testing.T) {
	now := time.Now().UTC()
	degraded := turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1)

	t.Run("猎手探测的 312 不清窗口", func(t *testing.T) {
		h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy, hunterWebshareProxy)
		// 猎手猎另一个模型：否则恢复探测铸出的 292 入池后，猎手看到票新鲜就不再探了。
		h.account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{
			"proxy_ids": []any{float64(20)}, "models": []any{"gpt-5.6-luna"},
		})
		h.svc.now = func() time.Time { return now }
		queueAnswers(h, now, "21")                         // 恢复探测：答对
		miss, _ := hunterResp(http.StatusOK, degraded, "") // 猎手：312
		h.up.queue = append(h.up.queue, miss)

		h.run(t)

		require.GreaterOrEqual(t, len(h.up.requests), 2, "同一个 tick 里两种探测都跑了")
		require.Equal(t, []bool{true}, recoveryState(h.account).Results, "猎手在别的出口上撞 312，不是账号自己出口的证据")
	})

	t.Run("回带的 312 不清标记", func(t *testing.T) {
		repo := newTurnStateAutoRepo()
		account := recoveryAccount(recoveryConfig(nil))
		repo.latest = account
		gw := &OpenAIGatewayService{accountRepo: repo}
		setRecoveryState(account, openAITurnStateRecoveryState{Results: []bool{true, true, true}})

		c := turnStateAutoCtxModel("carried", hunterTestModel)
		markOpenAITurnStateSent(c, account, degraded) // 出站带了票（客户端自带透传，非我们注入）
		gw.observeOpenAITurnStateMint(c, account, degraded)

		require.Equal(t, 3, recoveryState(account).successes(), "回声不是证据")
	})

	t.Run("裸请求的 312 才清", func(t *testing.T) {
		repo := newTurnStateAutoRepo()
		account := recoveryAccount(recoveryConfig(nil))
		repo.latest = account
		gw := &OpenAIGatewayService{accountRepo: repo}
		setRecoveryState(account, openAITurnStateRecoveryState{Results: []bool{true, true, true}})

		gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("bare", hunterTestModel), account, degraded)

		require.Empty(t, recoveryState(account).Results)
	})
}

// TestOpenAITurnStateRecoveryStateOmitsZeroTimes 钉住第一轮评审 B2：零值时间不能落库成
// 0001-01-01（JS 的 Date 认这个字符串，页面会从第一次探测起就写「已恢复」）。
func TestOpenAITurnStateRecoveryStateOmitsZeroTimes(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(recoveryAccount(recoveryConfig(nil)), hunterCoxProxy)
	h.svc.now = func() time.Time { return now }
	queueAnswers(h, now, "21")

	h.run(t)

	raw, ok := h.account.Extra[openAITurnStateRecoveryStateExtraKey].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, raw, "recovered_at", "没判定恢复就不该有这个键")
	require.NotContains(t, raw, "cooling_until")
	require.Contains(t, raw, "next_at")
	require.Contains(t, raw, "results")
}

// TestOpenAITurnStateRecoveryOneAccountPerTick 钉住第一轮评审 S4：恢复探测一个 tick 只做一个账号，
// 不能几十个账号一起在收集阶段各阻塞几分钟、把猎手的 15 分钟预算吃光。
func TestOpenAITurnStateRecoveryOneAccountPerTick(t *testing.T) {
	now := time.Now().UTC()
	first := recoveryAccount(recoveryConfig(nil))
	second := recoveryAccount(recoveryConfig(nil))
	second.ID = 9202
	second.Credentials = map[string]any{"access_token": "second-token", "chatgpt_account_id": "second-account"}
	h := newHunterHarness(first, hunterCoxProxy)
	h.repo.others = []*Account{second}
	h.svc.now = func() time.Time { return now }
	queueAnswers(h, now, "21", "21")

	h.run(t)
	require.Len(t, h.up.requests, 1, "一个 tick 只探一个账号")

	h.run(t)
	require.Len(t, h.up.requests, 2, "下个 tick 轮到另一个账号")
	require.Equal(t, "Bearer second-token", h.up.requests[1].Header.Get("Authorization"))
}

// TestOpenAITurnStateRecoveryIntervalIsRandom 钉住「间隔不固定」：同一份配置多次取值不能恒等。
func TestOpenAITurnStateRecoveryIntervalIsRandom(t *testing.T) {
	cfg := openAITurnStateRecoveryConfig{MinMinutes: 30, MaxMinutes: 90}
	seen := map[time.Duration]bool{}
	for range 30 {
		d := cfg.interval()
		require.GreaterOrEqual(t, d, 30*time.Minute)
		require.LessOrEqual(t, d, 90*time.Minute)
		seen[d] = true
	}
	require.Greater(t, len(seen), 1, "间隔要随机，不能每次都一样")
}
