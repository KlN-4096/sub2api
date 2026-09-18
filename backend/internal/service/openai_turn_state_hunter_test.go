//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

// 猎手单测全部离线：上游是 hunterUpstream，零真实请求。

const hunterTestModel = "gpt-6-astra"

var (
	hunterWebshareProxy = Proxy{ID: 20, Name: "webshare", Protocol: "socks5", Host: "p.webshare.io", Port: 1080, Username: "user-us-rotate", Password: "pw", Status: "active"}
	hunterCoxProxy      = Proxy{ID: 8, Name: "cox-a", Protocol: "socks5", Host: "24.120.102.167", Port: 35444, Username: "cox0", Password: "pw", Status: "active"}
	hunterCox2Proxy     = Proxy{ID: 9, Name: "cox-b", Protocol: "socks5", Host: "24.120.102.167", Port: 35445, Username: "cox1", Password: "pw", Status: "active"}
)

// hunterProbeBody 记录响应体有没有被读、有没有被关：头到手即断的判据。
type hunterProbeBody struct {
	mu     sync.Mutex
	reads  int
	closed bool
}

func (b *hunterProbeBody) Read([]byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	return 0, io.EOF
}

func (b *hunterProbeBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

type hunterUpstream struct {
	mu        sync.Mutex
	proxyURLs []string
	requests  []*http.Request
	bodies    [][]byte
	queue     []*http.Response
	err       error
}

func (u *hunterUpstream) Do(req *http.Request, proxyURL string, _ int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.proxyURLs = append(u.proxyURLs, proxyURL)
	u.requests = append(u.requests, req)
	if req != nil && req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		u.bodies = append(u.bodies, codexTestDecodeUpstreamBody(req.Header, raw))
	}
	if u.err != nil {
		return nil, u.err
	}
	if len(u.queue) == 0 {
		return nil, errors.New("hunter upstream: no queued response")
	}
	resp := u.queue[0]
	u.queue = u.queue[1:]
	return resp, nil
}

func (u *hunterUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, concurrency)
}

func hunterResp(status int, blob string, body string) (*http.Response, *hunterProbeBody) {
	b := &hunterProbeBody{}
	h := http.Header{"Content-Type": []string{"text/event-stream"}}
	if blob != "" {
		h.Set("X-Codex-Turn-State", blob)
	}
	var rc io.ReadCloser = b
	if body != "" {
		rc = io.NopCloser(strings.NewReader(body))
	}
	return &http.Response{StatusCode: status, Header: h, Body: rc}, b
}

type hunterAccountRepo struct {
	*turnStateAutoRepo
	// others 是多账号用例里 latest 之外的账号；GetByID / UpdateExtra 按 ID 路由。
	others []*Account
	// onUpdateExtra 在每次落库时回调：用来断言「落库那一刻上游连接已经挂断」这类顺序。
	onUpdateExtra func()
}

func (r *hunterAccountRepo) byID(id int64) *Account {
	for _, a := range r.others {
		if a.ID == id {
			return a
		}
	}
	return r.latest
}

func (r *hunterAccountRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getByIDCalls++
	a := r.byID(id)
	if a == nil {
		return nil, errors.New("not found")
	}
	return a, nil
}

// ListByPlatform 返回每个账号的**深拷贝**：生产里每个 tick 都是从 DB 重新读的行，内存里改了
// 没落库的东西下个 tick 就没了。浅拷贝共享同一张 Extra，删掉生产代码里的 UpdateExtra 调用
// 测试照样全绿——持久化那一层等于没测。
func (r *hunterAccountRepo) ListByPlatform(_ context.Context, _ string) ([]Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.latest == nil {
		return nil, nil
	}
	out := make([]Account, 0, 1+len(r.others))
	for _, a := range append([]*Account{r.latest}, r.others...) {
		out = append(out, hunterCloneAccount(a))
	}
	return out, nil
}

// UpdateExtra 模拟 jsonb 顶层键合并写回 DB 侧账号；断言读 h.account 看到的就是落库后的值。
func (r *hunterAccountRepo) UpdateExtra(_ context.Context, id int64, updates map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extraWrites = append(r.extraWrites, updates)
	r.getByIDCalls++
	if r.onUpdateExtra != nil {
		r.onUpdateExtra()
	}
	a := r.byID(id)
	if a == nil {
		return errors.New("not found")
	}
	if a.Extra == nil {
		a.Extra = map[string]any{}
	}
	for k, v := range updates {
		a.Extra[k] = v
	}
	return nil
}

// hunterCloneAccount 走一遍 JSON：与 DB 读出来的行同一形态（time.Time 变字符串）。
func hunterCloneAccount(a *Account) Account {
	c := *a
	raw, err := json.Marshal(a.Extra)
	if err != nil {
		panic(err) // 用例往 Extra 里放了不可序列化的值，当场炸比静默变 nil 好
	}
	var extra map[string]any
	if err := json.Unmarshal(raw, &extra); err != nil {
		panic(err)
	}
	c.Extra = extra
	return c
}

// hunterProber 是出口回声的替身：按代理 URL 里的端口返回固定 IP（同端口同出口），可覆盖。
type hunterProber struct {
	mu    sync.Mutex
	calls []string
	ips   map[string]string // 代理 URL 子串 → IP；不命中就按端口派生
	err   error
}

func (p *hunterProber) ProbeProxy(_ context.Context, proxyURL string) (*ProxyExitInfo, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, proxyURL)
	if p.err != nil {
		return nil, 0, p.err
	}
	for needle, ip := range p.ips {
		if strings.Contains(proxyURL, needle) {
			return &ProxyExitInfo{IP: ip}, 1, nil
		}
	}
	return &ProxyExitInfo{IP: "198.51.100." + proxyURL[strings.LastIndex(proxyURL, ":")+1:]}, 1, nil
}

type hunterProxyRepo struct{ proxies []Proxy }

func (r *hunterProxyRepo) ListByIDs(_ context.Context, ids []int64) ([]Proxy, error) {
	out := make([]Proxy, 0, len(ids))
	for _, id := range ids {
		for _, p := range r.proxies {
			if p.ID == id {
				out = append(out, p)
			}
		}
	}
	return out, nil
}

func hunterConfig(overrides map[string]any) map[string]any {
	cfg := map[string]any{
		"enabled": true, "models": []any{hunterTestModel}, "proxy_ids": []any{float64(20)},
		"gap_seconds": 1, "idle_minutes": -1,
	}
	for k, v := range overrides {
		cfg[k] = v
	}
	return cfg
}

func hunterTestAccount(hunter map[string]any) *Account {
	a := newTestOAuthAccount(9201, map[string]any{
		codexFingerprintModeExtraKey:        "device",
		codexFingerprintConvergenceExtraKey: true,
		openAITurnStateAutoExtraKey:         true,
		openAITurnStateHunterExtraKey:       hunter,
	})
	a.Status, a.Schedulable, a.Concurrency = StatusActive, true, 1
	a.Credentials = map[string]any{"access_token": "offline-token", "chatgpt_account_id": "offline-account"}
	return a
}

type hunterHarness struct {
	svc     *OpenAITurnStateHunterService
	gw      *OpenAIGatewayService
	up      *hunterUpstream
	repo    *hunterAccountRepo
	prober  *hunterProber
	account *Account
	sleeps  []time.Duration
}

func newHunterHarness(account *Account, proxies ...Proxy) *hunterHarness {
	up := &hunterUpstream{}
	repo := &hunterAccountRepo{turnStateAutoRepo: newTurnStateAutoRepo()}
	repo.latest = account
	prober := &hunterProber{}
	gw := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: up, toolCorrector: NewCodexToolCorrector(), accountRepo: repo}
	svc := NewOpenAITurnStateHunterService(gw, repo, &hunterProxyRepo{proxies: proxies}, prober, time.Hour)
	h := &hunterHarness{svc: svc, gw: gw, up: up, repo: repo, prober: prober, account: account}
	svc.sleep = func(_ context.Context, d time.Duration) error {
		h.sleeps = append(h.sleeps, d)
		return nil
	}
	return h
}

func (h *hunterHarness) run(t *testing.T) {
	t.Helper()
	h.svc.runOnce(context.Background())
}

func (h *hunterHarness) state() openAITurnStateHuntState {
	return readOpenAITurnStateHuntState(h.account)
}

// TestOpenAITurnStateHunterHitPoolsAndHangsUp 钉住主流程：312 → 换出口再摇 → 292 入池即停；
// 探测不读响应体；每次探测一条新代理连接（-rotate 按连接换出口）；请求沿真实管线出站。
func TestOpenAITurnStateHunterHitPoolsAndHangsUp(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	degraded, degradedBody := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	healthy, healthyBody := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{degraded, healthy}
	// 第一次落库发生在哪一步不重要，重要的是那一刻正在处理的响应体已经关了。
	bodyClosedAtFirstWrite, sawWrite := false, false
	h.repo.onUpdateExtra = func() {
		if sawWrite {
			return
		}
		sawWrite = true
		bodyClosedAtFirstWrite = degradedBody.closed
	}

	h.run(t)

	require.Len(t, h.up.requests, 2, "第二次摇到 292 就停")
	require.Len(t, h.sleeps, 1, "两次探测之间睡一次随机间隔")
	require.Equal(t, 0, degradedBody.reads, "头到手即断：不读 SSE")
	require.True(t, degradedBody.closed && healthyBody.closed, "响应体必须关掉，否则连接池计数不回落")
	require.True(t, bodyClosedAtFirstWrite, "入池读写库之前就得挂断：数据库慢不能让上游多生成一秒")

	require.Equal(t, h.up.proxyURLs[0], h.up.proxyURLs[1], "-rotate 端点原样用，不改写用户名")
	for i, u := range h.up.proxyURLs {
		require.Contains(t, u, "user-us-rotate:")
		require.True(t, h.up.requests[i].Close, "每次探测一条新代理连接：不关连接就一直从同一个出口发")
	}
	require.Empty(t, h.prober.calls, "轮换端点不回声：回声那条连接的出口不是探测会用的")
	require.Empty(t, h.state().Exits)

	req := h.up.requests[0]
	require.Equal(t, "chatgpt.com", req.Host)
	require.Empty(t, req.Header.Get("x-codex-turn-state"), "探测必须自然铸造，不能带票")
	require.Equal(t, resolveCodexOutboundIdentity("").originator, req.Header.Get("originator"))
	require.Equal(t, "text/event-stream", req.Header.Get("accept"))
	body := h.up.bodies[0]
	require.Equal(t, hunterTestModel, gjson.GetBytes(body, "model").String())
	require.True(t, gjson.GetBytes(body, "stream").Bool())
	require.False(t, gjson.GetBytes(body, "store").Bool())
	require.Equal(t, defaultOpenAITurnStateHuntReasoningEffort, gjson.GetBytes(body, "reasoning.effort").String())
	require.NotEmpty(t, gjson.GetBytes(body, "instructions").String(), "带该模型的真实 base prompt")
	require.NotEmpty(t, gjson.GetBytes(body, "prompt_cache_key").String())
	body2 := h.up.bodies[1]
	require.NotEqual(t, gjson.GetBytes(body, "prompt_cache_key").String(), gjson.GetBytes(body2, "prompt_cache_key").String(), "每次探测是一个新会话")

	pool := readOpenAITurnStatePool(h.account)
	require.Len(t, pool, 1, "只有 292 入池")
	require.Equal(t, hunterTestModel, pool[0].Model)
	require.True(t, openAITurnStateHealthy(pool[0].Blob))

	obs, ok := readOpenAITurnStateObservation(h.account)
	require.True(t, ok, "探测是自然铸造，要写形态观测")
	require.Equal(t, openAIHealthyTurnStateLen, obs.Chars)
	require.True(t, obs.Healthy)

	st := h.state()
	require.Equal(t, 2, st.HourCount)
	require.Len(t, st.Last, 2)
	require.True(t, st.Last[0].Healthy)
	require.Equal(t, openAIHealthyTurnStateLen, st.Last[0].Chars)
	require.Equal(t, openAIDegradedTurnStateLen, st.Last[1].Chars)
	require.Equal(t, hunterWebshareProxy.ID, st.Last[0].ProxyID)
	require.True(t, st.NextAt.IsZero(), "命中后不退避")
	require.Empty(t, st.LastError)
}

// TestOpenAITurnStateHunterSkipsWhenTicketFresh 钉住开窗时机：票还够用就不猎，剩余不足
// lead_minutes 才开始。
func TestOpenAITurnStateHunterSkipsWhenTicketFresh(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	setPool := func(minted time.Time) {
		h.account.Extra[openAITurnStatePoolExtraKey] = []any{map[string]any{
			"blob": turnStateFernetBlob(minted, openAIHealthyTurnStateBlocks), "model": hunterTestModel, "minted_at": minted,
		}}
	}

	setPool(now.Add(-5 * time.Minute)) // 还剩 55 分钟
	h.run(t)
	require.Empty(t, h.up.requests, "票还够用，不探测")
	require.Equal(t, openAITurnStateHuntGateFresh, h.state().Gate, "被门槛挡住要留痕，页面才分得清「票还新鲜」和「没在跑」")
	writes := len(h.repo.extraWrites)
	h.run(t)
	require.Len(t, h.repo.extraWrites, writes, "原因没变就不再落库")

	setPool(now.Add(-52 * time.Minute)) // 只剩 8 分钟 < 10
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}
	h.run(t)
	require.Len(t, h.up.requests, 1, "到期前 10 分钟开窗")
	require.Empty(t, h.up.requests[0].Header.Get("x-codex-turn-state"), "池里有票、猎手又开着：探测仍必须裸发，自然铸造")
	require.Empty(t, h.state().Gate, "开猎就清掉门槛痕迹")
}

// TestOpenAITurnStateHunterZeroMintedAtStillHunts 钉住：铸造戳解不出来的票到期时刻未知，
// 不能永远压住猎手。
func TestOpenAITurnStateHunterZeroMintedAtStillHunts(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	opaque := map[string]any{"blob": "opaque-not-fernet", "model": hunterTestModel, "minted_at": time.Time{}}
	fresh := map[string]any{"blob": turnStateFernetBlob(now.Add(-5*time.Minute), openAIHealthyTurnStateBlocks), "model": hunterTestModel, "minted_at": now.Add(-5 * time.Minute)}
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}

	// 零戳条目 + 一张还剩 55 分钟的真票：真票说了算，不探测（零戳条目既不压猎手也不抬 newest）。
	h.account.Extra[openAITurnStatePoolExtraKey] = []any{opaque, fresh}
	h.run(t)
	require.Empty(t, h.up.requests)

	// 只有零戳条目：到期时刻未知，照常猎。
	h.account.Extra[openAITurnStatePoolExtraKey] = []any{opaque}
	h.run(t)
	require.Len(t, h.up.requests, 1)
}

// TestOpenAITurnStateHunterRoundRobinModels 钉住多模型轮流：每次探测换下一个缺票的模型，
// 命中的出列，第一个模型不能吃光额度。
func TestOpenAITurnStateHunterRoundRobinModels(t *testing.T) {
	now := time.Now().UTC()
	const second = "gpt-6"
	cfg := hunterConfig(map[string]any{"models": []any{hunterTestModel, second}})
	h := newHunterHarness(hunterTestAccount(cfg), hunterWebshareProxy)
	blob := func(blocks int, minted time.Time) *http.Response {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(minted, blocks), "")
		return resp
	}
	h.up.queue = []*http.Response{
		blob(openAIHealthyTurnStateBlocks+1, now),                // astra 312
		blob(openAIHealthyTurnStateBlocks, now),                  // gpt-6 292 → 出列
		blob(openAIHealthyTurnStateBlocks, now.Add(time.Second)), // astra 292 → 全部命中（blob 不同，池不去重）
	}

	h.run(t)

	require.Len(t, h.up.requests, 3)
	models := make([]string, 0, 3)
	for _, body := range h.up.bodies {
		models = append(models, gjson.GetBytes(body, "model").String())
	}
	require.Equal(t, []string{hunterTestModel, second, hunterTestModel}, models)
	require.Len(t, readOpenAITurnStatePool(h.account), 2, "两个模型各入一张 292")
	require.True(t, h.state().NextAt.IsZero(), "全部命中不退避")
}

// TestOpenAITurnStateHunterHourWindowRollsMidCycle 钉住上限按小时窗算：一轮跨过小时边界后
// 计数归零、继续探测，而不是把上一小时的计数带进来提前停。
func TestOpenAITurnStateHunterHourWindowRollsMidCycle(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"max_per_hour": 2})), hunterWebshareProxy)
	// 上一小时窗还剩 1 分钟、已用 1 次：第 1 次探测后窗口就过期。
	h.account.Extra[openAITurnStateHuntExtraKey] = map[string]any{"hour_start": now.Add(-59 * time.Minute), "hour_count": 1}
	clock := now
	h.svc.now = func() time.Time { return clock }
	h.svc.sleep = func(context.Context, time.Duration) error {
		clock = clock.Add(2 * time.Minute)
		return nil
	}
	for range 3 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}

	h.run(t)

	require.Len(t, h.up.requests, 3, "第 2 次探测时已进入新的小时窗，上一窗的计数不能带过来")
	st := h.state()
	require.Equal(t, 2, st.HourCount, "新窗口里只算后两次")
	require.True(t, st.HourStart.After(now), "小时窗已滚动")
	require.Equal(t, st.HourStart.Add(time.Hour), st.NextAt, "新窗口到顶，等到它结束")
}

// TestOpenAITurnStateHunterCycleBudget 钉住一轮的软预算：超过预算就收手、NextAt 不动，
// 下个 tick 重新拿锁接着猎——锁 TTL 不续期，靠这个保证锁不会在探测进行中过期。
func TestOpenAITurnStateHunterCycleBudget(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	clock := now
	h.svc.now = func() time.Time { return clock }
	h.svc.sleep = func(context.Context, time.Duration) error {
		clock = clock.Add(openAITurnStateHunterCycleBudget + time.Second)
		return nil
	}
	for range 2 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}

	h.run(t)
	require.Len(t, h.up.requests, 1, "预算用完就收手")
	require.True(t, h.state().NextAt.IsZero(), "收手不是退避")

	h.run(t)
	require.Len(t, h.up.requests, 2, "下个 tick 接着猎")
}

// TestOpenAITurnStateHunterBudgetRotatesAccounts 钉住多账号：一轮预算被前一个账号吃光后，
// 下个 tick 从没轮到的账号开始，而不是每次都从头数（否则后面的账号永远轮不到）。
func TestOpenAITurnStateHunterBudgetRotatesAccounts(t *testing.T) {
	now := time.Now().UTC()
	first := hunterTestAccount(hunterConfig(nil))
	second := hunterTestAccount(hunterConfig(nil))
	second.ID = 9202
	second.Credentials = map[string]any{"access_token": "second-token", "chatgpt_account_id": "second-account"}
	h := newHunterHarness(first, hunterWebshareProxy)
	h.repo.others = []*Account{second}
	clock := now
	h.svc.now = func() time.Time { return clock }
	h.svc.sleep = func(context.Context, time.Duration) error {
		clock = clock.Add(openAITurnStateHunterCycleBudget + time.Second)
		return nil
	}
	for range 3 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}

	h.run(t)
	require.Len(t, h.up.requests, 1, "第一个账号一次探测后预算就用完")
	require.Equal(t, "Bearer offline-token", h.up.requests[0].Header.Get("Authorization"))

	h.run(t)
	require.Len(t, h.up.requests, 2)
	require.Equal(t, "Bearer second-token", h.up.requests[1].Header.Get("Authorization"), "下个 tick 从第二个账号开始")
	require.Len(t, readOpenAITurnStateHuntState(second).Last, 1, "第二个账号的运行态落在它自己的行上")
	require.Len(t, h.state().Last, 1)
}

// TestOpenAITurnStateHunterNoTurnStateInResponse 钉住 200 却没有 turn-state 头：算错误、
// 退避 15 分钟、不入池。
func TestOpenAITurnStateHunterNoTurnStateInResponse(t *testing.T) {
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	bare, _ := hunterResp(http.StatusOK, "", "")
	h.up.queue = []*http.Response{bare}

	h.run(t)

	require.Len(t, h.up.requests, 1)
	st := h.state()
	require.Equal(t, "no turn-state in response", st.LastError)
	require.InDelta(t, 15, st.NextAt.Sub(time.Now()).Minutes(), 1)
	require.Empty(t, readOpenAITurnStatePool(h.account))
}

// TestOpenAITurnStateHunterIdleGate 钉住空闲门槛：没人用的模型不猎；探测自己不算流量。
func TestOpenAITurnStateHunterIdleGate(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"idle_minutes": 0})), hunterWebshareProxy) // 0 = 默认 60
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}

	h.run(t)
	require.Empty(t, h.up.requests, "60 分钟内没有真实请求就不猎")
	require.Equal(t, openAITurnStateHuntGateIdle, h.state().Gate)
	// stub 的 UpdateExtra 也计入 getByIDCalls（真实实现尾部会同步快照）：两者相等 = 零次纯读池。
	require.Len(t, h.repo.extraWrites, 1, "只有留痕那一次落库")
	require.Equal(t, len(h.repo.extraWrites), h.repo.getByIDCalls, "全空闲就不读池")

	seen := now.Add(-time.Minute)
	h.gw.noteOpenAITurnStateTraffic(h.account.ID, hunterTestModel, seen)
	h.run(t)
	require.Len(t, h.up.requests, 1)
	require.False(t, h.gw.openAITurnStateTrafficSince(h.account.ID, hunterTestModel, seen), "探测不能把自己记成真实流量")
	require.Empty(t, h.state().Gate)
}

// TestOpenAITurnStateTrafficOnlyFromRealRequests 钉住水位入口：真实请求**出站**就记（与上游
// 响应里有没有 turn-state 头无关），探测上下文不记。
func TestOpenAITurnStateTrafficOnlyFromRealRequests(t *testing.T) {
	repo := newTurnStateAutoRepo()
	account := hunterTestAccount(hunterConfig(nil))
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo}
	since := time.Now().Add(-time.Minute)

	probe := turnStateAutoCtxModel("probe", hunterTestModel)
	probe.Set(ctxKeyTurnStateProbe, true)
	gw.applyOpenAICodexTurnStateOverrideHeader(probe, account, http.Header{})
	require.False(t, gw.openAITurnStateTrafficSince(account.ID, hunterTestModel, since))

	// 上游没铸 turn-state 的成功请求也是流量：只在响应头里记的话，缺票的模型会被空闲门槛挡住。
	gw.applyOpenAICodexTurnStateOverrideHeader(turnStateAutoCtxModel("real", hunterTestModel), account, http.Header{})
	require.True(t, gw.openAITurnStateTrafficSince(account.ID, hunterTestModel, since))
	require.False(t, gw.openAITurnStateTrafficSince(account.ID, "gpt-6-other", since), "按模型分桶")
}

// TestOpenAITurnStateHunterFixedProxiesUsedOnce 钉住「一轮内每个固定出口只用一次」：
// 同一个 IP 铸出 312 后再试它是白付额度；用完就退避。
func TestOpenAITurnStateHunterFixedProxiesUsedOnce(t *testing.T) {
	now := time.Now().UTC()
	cfg := hunterConfig(map[string]any{"proxy_ids": []any{float64(8), float64(9)}})
	h := newHunterHarness(hunterTestAccount(cfg), hunterCoxProxy, hunterCox2Proxy)
	for range 3 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}

	h.run(t)

	require.Len(t, h.up.requests, 2, "两个固定出口各试一次就停")
	require.Contains(t, h.up.proxyURLs[0], ":35444")
	require.Contains(t, h.up.proxyURLs[1], ":35445")
	st := h.state()
	require.Equal(t, 2, st.HourCount)
	require.InDelta(t, defaultOpenAITurnStateHuntRetryMinutes, st.NextAt.Sub(time.Now()).Minutes(), 1, "没摇到就退避 retry_minutes")

	h.run(t)
	require.Len(t, h.up.requests, 2, "退避期内不探测")
}

// TestOpenAITurnStateHunterSameExitProbedOnce 钉住「同一出口只探一次」：两个固定代理回声
// 出同一个 IP，第二个跳过（不算额度、不睡）；铸出 312 的出口一小时内下一轮也不再探，且
// 同一代理靠上次记录就能跳过、不再回声；冷却过了照常探。
func TestOpenAITurnStateHunterSameExitProbedOnce(t *testing.T) {
	now := time.Now().UTC()
	cfg := hunterConfig(map[string]any{"proxy_ids": []any{float64(8), float64(9)}})
	h := newHunterHarness(hunterTestAccount(cfg), hunterCoxProxy, hunterCox2Proxy)
	h.prober.ips = map[string]string{":35444": "203.0.113.7", ":35445": "203.0.113.7"}
	for range 3 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}

	h.run(t)

	require.Len(t, h.up.requests, 1, "第二个代理是同一个出口，跳过")
	require.Contains(t, h.up.proxyURLs[0], ":35444")
	require.Len(t, h.prober.calls, 2, "两个代理各回声一次")
	require.Len(t, h.sleeps, 1, "只有探测之后睡一次；跳过本身不睡")
	st := h.state()
	require.Equal(t, 1, st.HourCount, "跳过不算额度")
	require.Equal(t, "203.0.113.7", st.Last[0].Exit)
	require.Len(t, st.Exits, 2, "代理 9 回声到同一出口后记成别名：两个代理各一条")
	for _, e := range st.Exits {
		require.Equal(t, "203.0.113.7", e.IP)
		require.False(t, e.Healthy)
	}
	require.InDelta(t, defaultOpenAITurnStateHuntRetryMinutes, st.NextAt.Sub(time.Now()).Minutes(), 1)

	// 下一轮（退避已过）：出口还在冷却，一次都不探、也不再回声（两个代理都靠上次记录跳过）；
	// 页面要能看出「出口全在冷却」。
	st.NextAt = time.Time{}
	h.account.Extra[openAITurnStateHuntExtraKey] = hunterStateMap(t, st)
	h.run(t)
	require.Len(t, h.up.requests, 1)
	require.Len(t, h.prober.calls, 2, "别名记录让代理 9 也不用回声")
	require.Equal(t, "all hunt exits cooling", h.state().LastError)

	// 冷却过了：照常探。
	st = h.state()
	st.NextAt = time.Time{}
	for i := range st.Exits {
		st.Exits[i].At = now.Add(-openAITurnStateHuntExitCooldown - time.Minute)
	}
	h.account.Extra[openAITurnStateHuntExtraKey] = hunterStateMap(t, st)
	h.run(t)
	require.Len(t, h.up.requests, 2)
	require.Empty(t, h.state().LastError, "探测过就清掉冷却提示")
}

// TestOpenAITurnStateHunterExitEchoFailureStillProbes 钉住回声失败不挡探测：退化成按代理 ID
// 去重，记录里没有出口。
func TestOpenAITurnStateHunterExitEchoFailureStillProbes(t *testing.T) {
	now := time.Now().UTC()
	cfg := hunterConfig(map[string]any{"proxy_ids": []any{float64(8)}})
	h := newHunterHarness(hunterTestAccount(cfg), hunterCoxProxy)
	h.prober.err = errors.New("echo down")
	resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	h.up.queue = []*http.Response{resp}

	h.run(t)

	require.Len(t, h.up.requests, 1)
	require.Len(t, h.prober.calls, 1)
	st := h.state()
	require.Empty(t, st.Last[0].Exit)
	require.Empty(t, st.Exits)
}

// TestOpenAITurnStateHunterHealthyExitNotCooled 钉住：铸出 292 的出口不冷却——它对别的模型
// 也大概率是好出口。
func TestOpenAITurnStateHunterHealthyExitNotCooled(t *testing.T) {
	now := time.Now().UTC()
	const second = "gpt-6"
	cfg := hunterConfig(map[string]any{"proxy_ids": []any{float64(8)}, "models": []any{hunterTestModel, second}})
	h := newHunterHarness(hunterTestAccount(cfg), hunterCoxProxy)
	h.account.Extra[openAITurnStateHuntExtraKey] = map[string]any{"exits": []any{map[string]any{
		"ip": "198.51.100.35444", "proxy_id": 8, "at": now.Add(-time.Minute), "healthy": true,
	}}}
	resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{resp}

	h.run(t)

	require.Len(t, h.up.requests, 1, "好出口不冷却")
}

func hunterStateMap(t *testing.T, st openAITurnStateHuntState) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(st)
	require.NoError(t, err)
	var generic map[string]any
	require.NoError(t, json.Unmarshal(encoded, &generic))
	return generic
}

// TestOpenAITurnStateHunterHourlyCap 钉住每小时上限：到顶就等下一个小时窗。
func TestOpenAITurnStateHunterHourlyCap(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"max_per_hour": 2})), hunterWebshareProxy)
	for range 3 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}

	h.run(t)
	require.Len(t, h.up.requests, 2)
	st := h.state()
	require.Equal(t, st.HourStart.Add(time.Hour), st.NextAt, "到顶等到小时窗结束")

	h.run(t)
	require.Len(t, h.up.requests, 2)
}

// TestOpenAITurnStateHunterCapRaisedResumesImmediately 钉住：撞上限的等待在上限调高后立刻
// 解除；出错退避不受上限影响。
func TestOpenAITurnStateHunterCapRaisedResumesImmediately(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"max_per_hour": 2})), hunterWebshareProxy)
	for range 4 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}

	h.run(t)
	require.Len(t, h.up.requests, 2)
	require.True(t, h.state().CapWait, "撞上限等窗")

	h.run(t)
	require.Len(t, h.up.requests, 2, "上限没变：等窗")

	h.account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"max_per_hour": 4})
	h.run(t)
	require.Len(t, h.up.requests, 4, "上限调到 4：不等到点，本窗余量立刻用上")
	st := h.state()
	require.True(t, st.CapWait, "再次撞上限")
	require.Equal(t, st.HourStart.Add(time.Hour), st.NextAt)

	// 出错退避：上限调高也不解除。
	h.account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"max_per_hour": 10})
	h.up.err = errors.New("dial tcp: proxy refused")
	h.run(t)
	require.Len(t, h.up.requests, 5)
	st = h.state()
	require.False(t, st.CapWait)
	h.account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"max_per_hour": 100})
	h.run(t)
	require.Len(t, h.up.requests, 5, "退避期内上限再高也不探")
}

// TestOpenAITurnStateHunterCapWaitSurvivesGatePersist 钉住：等窗期间被「票未到期」挡住时
// 会落库留痕，这次落库不能把 CapWait 抹掉——否则上限调高后本窗余量就用不上了，死等到点。
func TestOpenAITurnStateHunterCapWaitSurvivesGatePersist(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"max_per_hour": 2})), hunterWebshareProxy)
	setPool := func(minted time.Time) {
		h.account.Extra[openAITurnStatePoolExtraKey] = []any{map[string]any{
			"blob": turnStateFernetBlob(minted, openAIHealthyTurnStateBlocks), "model": hunterTestModel, "minted_at": minted,
		}}
	}
	for range 2 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}
	h.run(t)
	require.Len(t, h.up.requests, 2)
	require.True(t, h.state().CapWait, "撞上限等窗")

	// 上限调到 4，但此刻票还新鲜：被门槛挡住并留痕，CapWait 要原样留着。
	h.account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"max_per_hour": 4})
	setPool(now.Add(-5 * time.Minute))
	h.run(t)
	require.Len(t, h.up.requests, 2)
	st := h.state()
	require.Equal(t, openAITurnStateHuntGateFresh, st.Gate)
	require.True(t, st.CapWait, "留痕不能抹掉等窗标记")

	// 票快到期：本窗余量必须立刻用上，而不是等到 NextAt。
	setPool(now.Add(-52 * time.Minute))
	healthy, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{healthy}
	h.run(t)
	require.Len(t, h.up.requests, 3, "上限已调高、票又快到期：立刻猎")
	require.Empty(t, h.state().Gate)
}

// TestOpenAITurnStateHunterBackoffWithoutTouchingAccount 钉住「出错只退避，不改账号状态」：
// 401 不 SetError、不停号；退避 6 小时；错误信息进运行态。
func TestOpenAITurnStateHunterBackoffWithoutTouchingAccount(t *testing.T) {
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	denied, _ := hunterResp(http.StatusUnauthorized, "", `{"error":{"message":"token expired"}}`)
	h.up.queue = []*http.Response{denied}

	h.run(t)

	require.Len(t, h.up.requests, 1)
	st := h.state()
	require.Equal(t, http.StatusUnauthorized, st.Last[0].Status)
	require.Contains(t, st.LastError, "token expired")
	require.InDelta(t, 6*60, st.NextAt.Sub(time.Now()).Minutes(), 1)
	require.Empty(t, h.repo.errors, "探测的 401 不能给账号记错误")
	require.Empty(t, h.repo.schedulable, "探测的失败不能停号")
	require.Empty(t, readOpenAITurnStatePool(h.account))

	h.run(t)
	require.Len(t, h.up.requests, 1, "退避期内不探测")
}

// TestOpenAITurnStateHunterTransportErrorBacksOff 钉住传输层错误（hunt 代理坏了）：
// 退避 15 分钟，账号不受影响。
func TestOpenAITurnStateHunterTransportErrorBacksOff(t *testing.T) {
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	h.up.err = errors.New("dial tcp: proxy refused")

	h.run(t)

	require.Len(t, h.up.requests, 1)
	st := h.state()
	require.Equal(t, 0, st.Last[0].Status)
	require.Contains(t, st.LastError, "proxy refused")
	require.InDelta(t, 15, st.NextAt.Sub(time.Now()).Minutes(), 1)
	require.Empty(t, h.repo.errors)
	require.Empty(t, h.repo.schedulable)
}

// TestOpenAITurnStateHunterGates 钉住三道前置门禁：猎手关、自动接管关、代理配了但不可用。
func TestOpenAITurnStateHunterGates(t *testing.T) {
	now := time.Now().UTC()
	healthy := func() *http.Response {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
		return resp
	}

	t.Run("hunter disabled", func(t *testing.T) {
		h := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"enabled": false})), hunterWebshareProxy)
		h.up.queue = []*http.Response{healthy()}
		h.run(t)
		require.Empty(t, h.up.requests)
	})
	t.Run("auto takeover disabled", func(t *testing.T) {
		account := hunterTestAccount(hunterConfig(nil))
		delete(account.Extra, openAITurnStateAutoExtraKey)
		h := newHunterHarness(account, hunterWebshareProxy)
		h.up.queue = []*http.Response{healthy()}
		h.run(t)
		require.Empty(t, h.up.requests, "票只入池不注入等于白猎")
	})
	t.Run("proxy inactive", func(t *testing.T) {
		inactive := hunterWebshareProxy
		inactive.Status = "inactive"
		h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), inactive)
		h.up.queue = []*http.Response{healthy()}
		h.run(t)
		require.Empty(t, h.up.requests, "绑了代理就不许回落到别的出口")
		st := h.state()
		require.Contains(t, st.LastError, "no usable hunt proxy")
		require.False(t, st.NextAt.IsZero())
	})
	t.Run("cpr account never hunts", func(t *testing.T) {
		account := hunterTestAccount(hunterConfig(nil))
		account.Type = AccountTypeCPR
		require.False(t, account.IsOpenAITurnStateHunterEnabled(), "cpr 的出口由 codex-proxy-rs 决定")
	})
}

// TestOpenAITurnStateHuntProxyRotating 钉住轮换端点的判定：只认 webshare 主机上以 -rotate
// 结尾的用户名；{user}-{country}-{N} 是固定出口；别的供应商即使叫 -rotate 也按固定出口。
func TestOpenAITurnStateHuntProxyRotating(t *testing.T) {
	require.True(t, openAITurnStateHuntProxyRotating(hunterWebshareProxy))
	plain := hunterWebshareProxy
	plain.Username = "user-rotate"
	require.True(t, openAITurnStateHuntProxyRotating(plain), "不带国家段的 -rotate 也是轮换端点")
	upper := hunterWebshareProxy
	upper.Username = "user-US-ROTATE"
	require.True(t, openAITurnStateHuntProxyRotating(upper))

	fixed := hunterWebshareProxy
	fixed.Username = "user-US-1"
	require.False(t, openAITurnStateHuntProxyRotating(fixed), "{user}-{country}-{N} 两次连接同一 IP")
	require.False(t, openAITurnStateHuntProxyRotating(hunterCoxProxy))
	other := hunterCoxProxy
	other.Username = "someone-rotate"
	require.False(t, openAITurnStateHuntProxyRotating(other))
}

// TestOpenAITurnStateInjectsEverySessionWhenHunterEnabled 钉住注入策略：开了猎手，池里
// 有票就对未判定的新会话也注入；没开猎手保持「只注已判定降智的会话」。
func TestOpenAITurnStateInjectsEverySessionWhenHunterEnabled(t *testing.T) {
	repo := newTurnStateAutoRepo()
	account := hunterTestAccount(hunterConfig(nil))
	repo.latest = account
	gw := &OpenAIGatewayService{accountRepo: repo}
	blob := turnStateFernetBlob(time.Now(), openAIHealthyTurnStateBlocks)
	gw.pushOpenAITurnStateCandidate(turnStateAutoCtxModel("seed", hunterTestModel), account, blob)

	got, source := gw.resolveOpenAITurnStateOverride(turnStateAutoCtxModel("fresh-session", hunterTestModel), account)
	require.Equal(t, blob, got, "开了猎手：新会话第一回合就注")
	require.Equal(t, turnStateSourceAuto, source)

	gw.pushOpenAITurnStateCandidate(turnStateAutoCtxModel("seed-other", "gpt-6"), account, turnStateFernetBlob(time.Now().Add(time.Second), openAIHealthyTurnStateBlocks))
	got, _ = gw.resolveOpenAITurnStateOverride(turnStateAutoCtxModel("fresh-session-other", "gpt-6"), account)
	require.Empty(t, got, "猎手不管的模型：仍要先判定降智才注")

	account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"enabled": false})
	got, _ = gw.resolveOpenAITurnStateOverride(turnStateAutoCtxModel("fresh-session-2", hunterTestModel), account)
	require.Empty(t, got, "没开猎手：未判定的会话保持真客户端形态")
}

func TestValidateOpenAITurnStateHunterExtra(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{openAITurnStateHunterExtraKey: hunterConfig(nil)}
	}
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(nil))
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{}))
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(valid()))

	nulled := map[string]any{openAITurnStateHunterExtraKey: nil}
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(nulled))
	require.NotContains(t, nulled, openAITurnStateHunterExtraKey, "null 等价于未配置")

	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: "on"}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"enabled": "true"})}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"models": []any{}})}), "开着却没模型")
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"proxy_ids": []any{}})}), "开着却没代理")
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"enabled": false, "models": []any{}})}), "关着可以空")
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"proxy_ids": []any{"20"}})}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"proxy_ids": []any{1.5}})}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"models": []any{" "}})}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"max_per_hour": 601})}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"lead_minutes": 60})}), "开窗不能早于票的寿命")
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"reasoning_effort": "max"})}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{
		openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"lead_minutes": 30}), openAITurnStateStaleMinExtraKey: 30,
	}), "开窗提前量必须小于票的寿命")
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{
		openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"lead_minutes": 29}), openAITurnStateStaleMinExtraKey: 30,
	}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{
		openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"lead_minutes": 30}), openAITurnStateStaleMinExtraKey: "30",
	}), "stale_after 写成数字串也要比：运行时 parseExtraFloat64 就收字符串")
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{
		openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"lead_minutes": 10}), openAITurnStateStaleMinExtraKey: 10.5,
	}), "运行时按 int 截断成 10，10 >= 10 就是永远够不上的窗")
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"reasoning_effort": "xhigh", "idle_minutes": -1})}))

	cfg, ok := readOpenAITurnStateHunterConfig(&Account{Extra: valid()})
	require.True(t, ok)
	require.Equal(t, []int64{20}, cfg.ProxyIDs)
	require.Equal(t, defaultOpenAITurnStateHuntMaxPerHour, cfg.MaxPerHour, "0 取默认")
	require.Equal(t, -1, cfg.IdleMinutes, "显式 -1 = 不设空闲门槛")
	require.Equal(t, 1, cfg.GapSeconds)

	// 数据导入不过 handler 校验：运行侧自己压上限。
	wild := hunterConfig(map[string]any{
		"max_per_hour": 5000, "lead_minutes": 600, "retry_minutes": 99999, "idle_minutes": 99999,
		"gap_seconds": 86400, "reasoning_effort": "bogus",
		"models": []any{"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"},
	})
	cfg, ok = readOpenAITurnStateHunterConfig(&Account{Extra: map[string]any{openAITurnStateHunterExtraKey: wild}})
	require.True(t, ok)
	require.Equal(t, openAITurnStateHunterMaxPerHourCap, cfg.MaxPerHour)
	require.Equal(t, openAITurnStateHunterMaxLeadMinutes, cfg.LeadMinutes)
	require.Equal(t, openAITurnStateHunterMaxMinutes, cfg.RetryMinutes)
	require.Equal(t, openAITurnStateHunterMaxMinutes, cfg.IdleMinutes)
	require.Equal(t, openAITurnStateHunterMaxGapSeconds, cfg.GapSeconds)
	require.Equal(t, defaultOpenAITurnStateHuntReasoningEffort, cfg.ReasoningEffort)
	require.Len(t, cfg.Models, openAITurnStateHunterMaxModels)
}
