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
	// errOnNil 是队列里 nil 条目要返回的错误：用来在一条队列里混排「传输错误 → 正常响应」。
	errOnNil error
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
	if resp == nil {
		return nil, u.errOnNil
	}
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

// SetModelRateLimit 按 ID 路由：多账号用例里暂停/放回落错账号必须能被发现（第二轮评审 S4）。
func (r *hunterAccountRepo) SetModelRateLimit(ctx context.Context, id int64, scope string, resetAt time.Time, reason ...string) error {
	if r.byID(id) == r.latest {
		return r.turnStateAutoRepo.SetModelRateLimit(ctx, id, scope, resetAt, reason...)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	why := ""
	if len(reason) > 0 {
		why = reason[0]
	}
	if resetAt.After(time.Now()) {
		r.holds = append(r.holds, scope)
	} else {
		r.clears++
	}
	setAccountModelRateLimitSnapshot(r.byID(id), scope, resetAt, why, time.Now())
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

// TestOpenAITurnStateHunterRestartsRoundOnConfigChange 钉住「改配置立刻生效」：轮次中管理员改了猎手
// 配置 → 本轮当场结束（NextAt 不动），下个 tick 按新配置从头开轮；关掉猎手 → 下个 tick 不再探
// （2026-09-19 用户反馈：填了记账 key 十几分钟不见用量行——配置是轮次开头读一次的）。
func TestOpenAITurnStateHunterRestartsRoundOnConfigChange(t *testing.T) {
	now := time.Now().UTC()
	// onFirstWrite 在第一次落库（= 第一次探测之后）时模拟管理员保存：改 DB 侧账号，轮次手里的是旧快照。
	onFirstWrite := func(h *hunterHarness, cfg map[string]any) {
		done := false
		h.repo.onUpdateExtra = func() {
			if done {
				return
			}
			done = true
			h.account.Extra[openAITurnStateHunterExtraKey] = cfg
		}
	}
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	miss, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	hit, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{miss, hit}
	onFirstWrite(h, hunterConfig(map[string]any{"usage_api_key_id": 12}))

	h.run(t)
	require.Len(t, h.up.requests, 1, "配置变了：本轮到此为止")
	require.True(t, h.state().NextAt.IsZero(), "结束本轮不是退避")
	require.Empty(t, h.sleeps, "探完就发现变了，不白睡一个 gap")

	h.run(t)
	require.Len(t, h.up.requests, 2, "下个 tick 按新配置从头开轮")
	require.True(t, h.state().Last[0].Healthy)

	// 关掉猎手：本轮当场结束，下个 tick 也不探。
	off := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	for range 3 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		off.up.queue = append(off.up.queue, resp)
	}
	onFirstWrite(off, hunterConfig(map[string]any{"enabled": false}))
	off.run(t)
	require.Len(t, off.up.requests, 1, "关掉开关：本轮到此为止")
	off.run(t)
	require.Len(t, off.up.requests, 1, "下个 tick 不再探")
}

// TestOpenAITurnStateHunterInterleavesAccounts 钉住多账号交错：一个账号一直 312 不能把整轮预算
// 吃光让别的账号排队（2026-09-19 反馈：账号 21 探满 15 分钟，账号 22 开窗后只抢到 1 次探测，票在
// 排队里过期）。每个账号各守各的 gap：两个账号的第一次探测背靠背，之后按各自的 readyAt 交替。
func TestOpenAITurnStateHunterInterleavesAccounts(t *testing.T) {
	now := time.Now().UTC()
	first := hunterTestAccount(hunterConfig(nil)) // gap 1s
	second := hunterTestAccount(hunterConfig(map[string]any{"gap_seconds": 10}))
	second.ID = 9202
	second.Credentials = map[string]any{"access_token": "second-token", "chatgpt_account_id": "second-account"}
	h := newHunterHarness(first, hunterWebshareProxy)
	h.repo.others = []*Account{second}
	clock := now
	h.svc.now = func() time.Time { return clock }
	h.svc.sleep = func(_ context.Context, d time.Duration) error {
		h.sleeps = append(h.sleeps, d)
		clock = clock.Add(d)
		return nil
	}
	miss := func() *http.Response {
		r, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		return r
	}
	hit := func() *http.Response {
		r, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
		return r
	}
	// 请求顺序：first 312、second 312、first 292（1s 后）、second 292（10s 后）。
	h.up.queue = []*http.Response{miss(), miss(), hit(), hit()}

	h.run(t)

	require.Len(t, h.up.requests, 4)
	var order []string
	for _, r := range h.up.requests {
		order = append(order, r.Header.Get("Authorization"))
	}
	require.Equal(t, []string{"Bearer offline-token", "Bearer second-token", "Bearer offline-token", "Bearer second-token"}, order,
		"两个账号交替探测，不是第一个探完才轮到第二个")
	require.Len(t, h.sleeps, 2)
	require.Less(t, h.sleeps[0], 2*time.Second, "第一个账号守自己的 1s 间隔")
	require.Greater(t, h.sleeps[1], 3*time.Second, "第二个账号守自己的 10s 间隔，不被第一个账号的节奏带跑")
	require.Len(t, h.state().Last, 2, "运行态各落各的行")
	require.True(t, h.state().Last[0].Healthy)
	require.Len(t, readOpenAITurnStateHuntState(second).Last, 2)
	require.True(t, readOpenAITurnStateHuntState(second).Last[0].Healthy)
	require.True(t, h.state().NextAt.IsZero() && readOpenAITurnStateHuntState(second).NextAt.IsZero(), "命中不退避")
}

// TestOpenAITurnStateHunterNoTurnStateInResponse 钉住 200 却没有 turn-state 头：算错误、
// 退避 15 分钟、不入池。
func TestOpenAITurnStateHunterNoTurnStateInResponse(t *testing.T) {
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	// 失败会重试，队列要够 strike 次：少一条的话第二次拿到的是 harness 的「没有排队响应」。
	for range openAITurnStateHuntFailureStrikes {
		bare, _ := hunterResp(http.StatusOK, "", "")
		h.up.queue = append(h.up.queue, bare)
	}

	h.run(t)

	require.Len(t, h.up.requests, openAITurnStateHuntFailureStrikes, "拿不到票也算失败：同一出口连试 3 次")
	st := h.state()
	require.Equal(t, "no turn-state in response", st.LastError)
	require.InDelta(t, openAITurnStateHuntFailureBackoff.Minutes(), st.NextAt.Sub(time.Now()).Minutes(), 1)
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

// TestOpenAITurnStateHunterHTTPErrorRetriesInsteadOfEndingRound 钉住现网那次事故：轮换端点
// 摇到一个被边缘拒掉的出口回 403，上一分钟同一个代理还在出 292。旧行为是「401/403 = 凭据坏了」
// 直接退避 6 小时、整轮结束，手里另外几条好代理连试都没试。新行为：换下一个代理接着猎。
func TestOpenAITurnStateHunterHTTPErrorRetriesInsteadOfEndingRound(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{
		"proxy_ids": []any{float64(8), float64(20)}, "rotating_proxy_ids": []any{float64(8)}, "max_per_hour": 10,
	})), hunterCoxProxy, hunterWebshareProxy)
	denied, _ := hunterResp(http.StatusForbidden, "", "")
	hit, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{denied, hit}

	h.run(t)

	require.Len(t, h.up.requests, 2, "403 只让这个代理记一次 strike，换下一个继续")
	require.Equal(t, http.StatusForbidden, h.state().Last[1].Status)
	require.True(t, h.state().Last[0].Healthy, "同一轮里就猎到了票")
	require.Len(t, readOpenAITurnStatePool(h.account), 1)
	require.False(t, time.Now().Add(5*time.Minute).Before(h.state().NextAt), "命中后不退避，更不是 6 小时")
}

// TestOpenAITurnStateHunterAllProxiesFailingBacksOffOneHour 钉住退避口径：每条代理都连续失败
// 到出局才等一小时（不再有 6 小时那一档），中途有代理没出局就照旧走 retry_minutes。
func TestOpenAITurnStateHunterAllProxiesFailingBacksOffOneHour(t *testing.T) {
	h := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{
		"proxy_ids": []any{float64(8), float64(20)}, "rotating_proxy_ids": []any{float64(8)}, "max_per_hour": 100,
	})), hunterCoxProxy, hunterWebshareProxy)
	for range openAITurnStateHuntFailureStrikes * 2 {
		denied, _ := hunterResp(http.StatusForbidden, "", "")
		h.up.queue = append(h.up.queue, denied)
	}

	h.run(t)

	require.Len(t, h.up.requests, openAITurnStateHuntFailureStrikes*2, "两条代理各连试 3 次")
	require.InDelta(t, openAITurnStateHuntFailureBackoff.Minutes(), h.state().NextAt.Sub(time.Now()).Minutes(), 1)
	require.Empty(t, h.repo.errors, "探测的失败不给账号记错误")
	require.Empty(t, h.repo.schedulable, "探测的失败不停号")
}

// TestOpenAITurnStateHuntExitCoolingDownOnlyAfter312 钉住 7 天冷却只罚铸出 312 的出口：
// 铸出 292 的出口不冷却（能拿到 292 的出口下一次大概率还能拿到），没到上游的失败尝试
// 连记录都不写（noteExit 跳过带 Error 的尝试），不会误判成 312。
func TestOpenAITurnStateHuntExitCoolingDownOnlyAfter312(t *testing.T) {
	now := time.Now().UTC()
	st := &openAITurnStateHuntState{}

	st.noteExit(openAITurnStateHuntAttempt{Exit: "1.1.1.1", ProxyID: 8, At: now.Add(-time.Hour), Healthy: false})
	st.noteExit(openAITurnStateHuntAttempt{Exit: "2.2.2.2", ProxyID: 9, At: now.Add(-time.Hour), Healthy: true})
	st.noteExit(openAITurnStateHuntAttempt{Exit: "3.3.3.3", ProxyID: 10, At: now.Add(-time.Hour), Error: "dial tcp: timeout"})

	require.True(t, st.exitCoolingDown("1.1.1.1", now), "312 的出口冷却")
	require.False(t, st.exitCoolingDown("2.2.2.2", now), "292 的出口不冷却")
	require.False(t, st.exitCoolingDown("3.3.3.3", now), "没到上游的尝试不记录，也就谈不上冷却")

	// 同一出口先 312 后 292：最新一条说了算，立刻解冻。
	st.noteExit(openAITurnStateHuntAttempt{Exit: "1.1.1.1", ProxyID: 8, At: now, Healthy: true})
	require.False(t, st.exitCoolingDown("1.1.1.1", now))

	// 312 满 7 天后自然解冻。
	st.noteExit(openAITurnStateHuntAttempt{Exit: "4.4.4.4", ProxyID: 11, At: now.Add(-openAITurnStateHuntExitCooldown - time.Minute), Healthy: false})
	require.False(t, st.exitCoolingDown("4.4.4.4", now))
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

	// 出错等待：上限调高也不解除。（唯一的出口连不上：同一代理连续 3 次才出局，4 + 3 = 7）
	h.account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"max_per_hour": 10})
	h.up.err = errors.New("dial tcp: proxy refused")
	h.run(t)
	require.Len(t, h.up.requests, 4+openAITurnStateHuntFailureStrikes)
	st = h.state()
	require.False(t, st.CapWait)
	h.account.Extra[openAITurnStateHunterExtraKey] = hunterConfig(map[string]any{"max_per_hour": 100})
	h.run(t)
	require.Len(t, h.up.requests, 4+openAITurnStateHuntFailureStrikes, "等待期内上限再高也不探")
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
	for range openAITurnStateHuntFailureStrikes {
		denied, _ := hunterResp(http.StatusUnauthorized, "", `{"error":{"message":"token expired"}}`)
		h.up.queue = append(h.up.queue, denied)
	}

	h.run(t)

	require.Len(t, h.up.requests, openAITurnStateHuntFailureStrikes, "401 不再特判：与别的失败一样连试 3 次")
	st := h.state()
	require.Equal(t, http.StatusUnauthorized, st.Last[0].Status)
	require.Contains(t, st.LastError, "token expired")
	require.InDelta(t, openAITurnStateHuntFailureBackoff.Minutes(), st.NextAt.Sub(time.Now()).Minutes(), 1)
	require.Empty(t, h.repo.errors, "探测的 401 不能给账号记错误")
	require.Empty(t, h.repo.schedulable, "探测的失败不能停号")
	require.Empty(t, readOpenAITurnStatePool(h.account))

	h.run(t)
	require.Len(t, h.up.requests, openAITurnStateHuntFailureStrikes, "退避期内不探测")
}

// TestOpenAITurnStateHunterTransportErrorBacksOff 钉住传输层错误（hunt 代理坏了）：唯一的出口
// 一轮只试一次，然后按 retry 等下一轮（不是 15 分钟的出错退避，也不会一直撞到小时封顶），
// 账号不受影响。
func TestOpenAITurnStateHunterTransportErrorBacksOff(t *testing.T) {
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	h.up.err = errors.New("dial tcp: proxy refused")

	h.run(t)

	require.Len(t, h.up.requests, openAITurnStateHuntFailureStrikes, "同一出口隔一个 gap 重试，连续 3 次才放弃")
	require.Len(t, h.sleeps, openAITurnStateHuntFailureStrikes-1, "每次重试前都守探测间隔")
	st := h.state()
	require.Equal(t, 0, st.Last[0].Status)
	require.Contains(t, st.LastError, "proxy refused")
	require.Zero(t, st.HourCount, "连代理都没连上不算上游花费：死代理不能白吃小时额度")
	require.InDelta(t, openAITurnStateHuntFailureBackoff.Minutes(), st.NextAt.Sub(time.Now()).Minutes(), 1)
	require.Empty(t, h.repo.errors)
	require.Empty(t, h.repo.schedulable)
}

// TestOpenAITurnStateHunterTransportErrorRetriesSameExit 钉住偶发传输错误的重试：唯一出口一次
// `http2: client connection lost` 之后隔一个 gap 重试同一出口即命中，不等一轮 retry_minutes
// （2026-09-19 用户反馈：一次偶发错误让整个出口 10 分钟不可用）。
func TestOpenAITurnStateHunterTransportErrorRetriesSameExit(t *testing.T) {
	now := time.Now().UTC()
	h := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	hit, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = []*http.Response{nil, hit}
	h.up.errOnNil = errors.New("Post \"https://chatgpt.com/backend-api/codex/responses\": http2: client connection lost")

	h.run(t)

	require.Len(t, h.up.requests, 2)
	require.Equal(t, h.up.proxyURLs[0], h.up.proxyURLs[1], "重试的是同一个出口")
	require.Len(t, h.sleeps, 1)
	st := h.state()
	require.True(t, st.Last[0].Healthy)
	require.Contains(t, st.Last[1].Error, "connection lost", "传输错误照样留在最近记录里")
	require.Equal(t, 1, st.HourCount, "只有真到上游的那一次计额度")
	require.Empty(t, st.LastError)
	require.False(t, time.Now().Add(time.Minute).Before(st.NextAt), "命中后不退避")
	require.Len(t, readOpenAITurnStatePool(h.account), 1)

	// 固定出口一样：选中即标「用过」，但没到上游的那次得放回去重试（第二轮评审 S2）。
	fixed := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"proxy_ids": []any{float64(8)}})), hunterCoxProxy)
	hit2, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	fixed.up.queue = []*http.Response{nil, hit2}
	fixed.up.errOnNil = errors.New("read tcp: connection reset by peer")
	fixed.run(t)
	require.Len(t, fixed.up.requests, 2)
	for _, u := range fixed.up.proxyURLs {
		require.Contains(t, u, hunterCoxProxy.Host)
	}
	require.True(t, fixed.state().Last[0].Healthy)
	require.Equal(t, 1, fixed.state().HourCount)
	require.Len(t, fixed.state().Exits, 1, "只记成功探测的出口")

	// 预算里等不到重试（gap 配到 600s）：按 retry 等下一轮，别下个 tick 再从 strike 1 数起、每 60 秒拨一次
	// 死代理（第二轮评审 1）。
	wide := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"gap_seconds": 600})), hunterWebshareProxy)
	wide.up.err = errors.New("dial tcp: proxy refused")
	wide.run(t)
	require.Len(t, wide.up.requests, 1, "等不到重试就不再试")
	require.Empty(t, wide.sleeps)
	require.InDelta(t, defaultOpenAITurnStateHuntRetryMinutes, wide.state().NextAt.Sub(time.Now()).Minutes(), 1)
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

// TestOpenAITurnStateHuntProxyRotating 钉住轮换端点的判定：webshare 主机上以 -rotate 结尾的
// 用户名自动识别；{user}-{country}-{N} 是固定出口；别的供应商即使叫 -rotate 也按固定出口——
// 除非显式勾进 rotating_proxy_ids（B2Proxy 这类轮换/粘性从用户名看不出来）。
func TestOpenAITurnStateHuntProxyRotating(t *testing.T) {
	var none openAITurnStateHunterConfig
	require.True(t, openAITurnStateHuntProxyRotating(none, hunterWebshareProxy))
	plain := hunterWebshareProxy
	plain.Username = "user-rotate"
	require.True(t, openAITurnStateHuntProxyRotating(none, plain), "不带国家段的 -rotate 也是轮换端点")
	upper := hunterWebshareProxy
	upper.Username = "user-US-ROTATE"
	require.True(t, openAITurnStateHuntProxyRotating(none, upper))

	fixed := hunterWebshareProxy
	fixed.Username = "user-US-1"
	require.False(t, openAITurnStateHuntProxyRotating(none, fixed), "{user}-{country}-{N} 两次连接同一 IP")
	require.False(t, openAITurnStateHuntProxyRotating(none, hunterCoxProxy))
	other := hunterCoxProxy
	other.Username = "someone-rotate"
	require.False(t, openAITurnStateHuntProxyRotating(none, other))

	explicit := openAITurnStateHunterConfig{RotatingProxyIDs: []int64{hunterCoxProxy.ID}}
	require.True(t, openAITurnStateHuntProxyRotating(explicit, hunterCoxProxy), "显式勾了就按轮换")
	require.False(t, openAITurnStateHuntProxyRotating(explicit, hunterCox2Proxy))
}

// TestOpenAITurnStateHunterExplicitRotatingProxyReused 钉住 2026-09-19 反馈：B2Proxy 这类轮换
// 代理被当成固定出口后「一轮只探一次 → 等 10 分钟 → 312 冷却 7 天」。勾进 rotating_proxy_ids
// 后一轮内反复用、不回声、312 不按 IP 冷却。
func TestOpenAITurnStateHunterExplicitRotatingProxyReused(t *testing.T) {
	now := time.Now().UTC()
	cfg := hunterConfig(map[string]any{"proxy_ids": []any{float64(8)}, "rotating_proxy_ids": []any{float64(8)}})
	h := newHunterHarness(hunterTestAccount(cfg), hunterCoxProxy)
	for range 2 {
		resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
		h.up.queue = append(h.up.queue, resp)
	}
	hit, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = append(h.up.queue, hit)

	h.run(t)

	require.Len(t, h.up.requests, 3, "同一条轮换代理一轮内反复探，直到命中")
	require.Zero(t, h.prober.calls, "轮换端点不回声")
	require.Empty(t, h.state().Exits, "轮换端点不记出口冷却")
	require.Equal(t, 1, len(readOpenAITurnStatePool(h.account)))
}

// TestOpenAITurnStateHunterTransportErrorSkipsToNextProxy 钉住 2026-09-19 反馈：一条代理连不上
// 不能把整轮掐掉——本轮不再用它，换下一条继续，坏几条都一样；出错的尝试不记出口冷却。
func TestOpenAITurnStateHunterTransportErrorSkipsToNextProxy(t *testing.T) {
	now := time.Now().UTC()
	cfg := hunterConfig(map[string]any{"proxy_ids": []any{float64(8), float64(9)}})
	h := newHunterHarness(hunterTestAccount(cfg), hunterCoxProxy, hunterCox2Proxy)
	h.up.queue = []*http.Response{nil} // 第一条：传输错误
	hit, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = append(h.up.queue, hit)
	h.up.errOnNil = errors.New("read tcp: connection reset by peer")

	h.run(t)

	require.Len(t, h.up.requests, 2, "cox-a 连不上就换 cox-b")
	require.True(t, h.state().Last[0].Healthy)
	require.Equal(t, 1, len(readOpenAITurnStatePool(h.account)))
	require.False(t, time.Now().Add(5*time.Minute).Before(h.state().NextAt), "命中后不退避")
	require.Len(t, h.state().Exits, 1, "只记成功探测的出口")
	st := h.state()
	require.Equal(t, "", st.lastExitOf(8), "连接被重置不是 312，cox-a 的出口不能进 7 天冷却")

	// 坏几条都一样：三条里前两条连不上，第三条照样轮到。
	many := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"proxy_ids": []any{float64(8), float64(9), float64(20)}})), hunterCoxProxy, hunterCox2Proxy, hunterWebshareProxy)
	hit2, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	many.up.queue = []*http.Response{nil, nil, hit2}
	many.up.errOnNil = errors.New("dial tcp: i/o timeout")
	many.run(t)
	require.Len(t, many.up.requests, 3)
	require.True(t, many.state().Last[0].Healthy)
	require.Equal(t, 1, many.state().HourCount, "只有真到上游的那一次计额度")

	// 坏的轮换端点连续 3 次失败才本轮不再用：别的端点成功不「洗白」它的连续计数。
	rot := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{
		"proxy_ids": []any{float64(8), float64(20)}, "rotating_proxy_ids": []any{float64(8)}, "max_per_hour": 10,
	})), hunterCoxProxy, hunterWebshareProxy)
	miss1, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	miss2, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	hit3, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	miss3, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1), "")
	rot.up.queue = []*http.Response{nil, miss1, nil, miss2, nil, miss3, hit3}
	rot.up.errOnNil = errors.New("dial tcp: proxy refused")
	rot.run(t)
	require.Len(t, rot.up.requests, 7)
	for _, i := range []int{0, 2, 4} {
		require.Contains(t, rot.up.proxyURLs[i], hunterCoxProxy.Host, "轮换端点连试 3 次才出局（别的端点成功不洗白它的连续计数）")
	}
	for _, u := range rot.up.proxyURLs[5:] {
		require.Contains(t, u, hunterWebshareProxy.Host, "连续 3 次连不上的轮换端点本轮不再用")
	}
	require.True(t, rot.state().Last[0].Healthy, "出局一条不影响另一条继续猎到票")

	// 全都不通：代理全部 strike 出局后退避一小时，别一直撞到小时封顶。
	dead := newHunterHarness(hunterTestAccount(hunterConfig(nil)), hunterWebshareProxy)
	dead.up.err = errors.New("dial tcp: proxy refused")
	dead.run(t)
	require.Len(t, dead.up.requests, openAITurnStateHuntFailureStrikes)
	require.InDelta(t, openAITurnStateHuntFailureBackoff.Minutes(), dead.state().NextAt.Sub(time.Now()).Minutes(), 1)

	// URL 都拼不出来的代理不是瞬时故障：根本不进轮次，别的代理照常。
	badProxy := hunterCoxProxy
	badProxy.Protocol = "ftp"
	unusable := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"proxy_ids": []any{float64(8), float64(9)}})), badProxy, hunterCox2Proxy)
	hit4, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	unusable.up.queue = []*http.Response{hit4}
	unusable.run(t)
	require.Len(t, unusable.up.requests, 1)
	require.Contains(t, unusable.up.proxyURLs[0], hunterCox2Proxy.Host)
	require.True(t, unusable.state().Last[0].Healthy)
	require.Empty(t, unusable.state().LastError)

	// 构造探测失败（拿不到 token）是账号的问题，不是出口的问题：不换出口重试，直接退避。
	broken := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"proxy_ids": []any{float64(8), float64(9)}})), hunterCoxProxy, hunterCox2Proxy)
	broken.account.Credentials = map[string]any{"chatgpt_account_id": "offline-account"} // 没有 access_token
	broken.run(t)
	require.Empty(t, broken.up.requests, "构造阶段就失败，请求根本没发出")
	require.Len(t, broken.state().Last, 1, "只记一次，不按出口重试")
	require.Contains(t, broken.state().LastError, "build probe")
	require.InDelta(t, defaultOpenAITurnStateHuntRetryMinutes, broken.state().NextAt.Sub(time.Now()).Minutes(), 1)
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
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"hold_when_degraded": "yes"})}), "暂停开关只能是 bool")
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"usage_api_key_id": -1})}), "记账 key 不能是负数")
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"usage_api_key_id": "7"})}), "记账 key 必须是数字")
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"usage_api_key_id": float64(7)})}))
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"hold_when_degraded": true})}))
	require.Error(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"auto_models": 1})}), "自动定模型只能是 bool")
	require.NoError(t, ValidateOpenAITurnStateHunterExtra(map[string]any{openAITurnStateHunterExtraKey: hunterConfig(map[string]any{"auto_models": true, "models": []any{}})}), "自动定模型时手选可以为空")
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

// TestOpenAITurnStateHunterAutoModelsFollowTraffic 钉住「按真实请求自动定模型」：手选列表忽略，
// 空闲窗口内有真实流量的模型都猎；注入点对任何模型都按「猎手在补票」处理。
func TestOpenAITurnStateHunterAutoModelsFollowTraffic(t *testing.T) {
	now := time.Now().UTC()
	cfg := hunterConfig(map[string]any{"auto_models": true, "models": []any{"gpt-6-manual"}, "idle_minutes": 60})
	h := newHunterHarness(hunterTestAccount(cfg), hunterWebshareProxy)
	require.False(t, h.gw.openAITurnStateHuntedModel(h.account, "gpt-6-anything"), "自动模式：上游没给它铸过票的模型不算在管")

	h.run(t)
	require.Empty(t, h.up.requests, "还没有真实流量：没有模型可猎")
	require.Equal(t, openAITurnStateHuntGateIdle, h.state().Gate)

	// 铸造记忆走真实响应的观测入口（未注入的响应才算）。
	h.gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("s-luna", "gpt-5.6-Luna"), h.account, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	h.gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("s-astra", hunterTestModel), h.account, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	h.gw.observeOpenAITurnStateMint(turnStateAutoCtxModel("s-img", "gpt-image-2"), h.account, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	require.True(t, h.gw.openAITurnStateHuntedModel(h.account, "gpt-5.6-Luna"))
	require.False(t, h.gw.openAITurnStateHuntedModel(h.account, "gpt-image-2"), "画图模型不参与，铸过 312 也不算")
	require.False(t, h.gw.openAITurnStateHuntedModel(h.account, "codex-auto-review"), "从不铸票的模型不算")

	h.gw.noteOpenAITurnStateTraffic(h.account.ID, "gpt-5.6-Luna", now.Add(-5*time.Minute))
	h.gw.noteOpenAITurnStateTraffic(h.account.ID, hunterTestModel, now.Add(-2*time.Hour)) // 超出空闲窗口
	h.gw.noteOpenAITurnStateTraffic(h.account.ID, "gpt-image-2", now.Add(-5*time.Minute))
	h.gw.noteOpenAITurnStateTraffic(h.account.ID, "codex-auto-review", now.Add(-5*time.Minute))
	resp, _ := hunterResp(http.StatusOK, turnStateFernetBlob(now, openAIHealthyTurnStateBlocks), "")
	h.up.queue = append(h.up.queue, resp)
	h.run(t)
	require.Len(t, h.up.requests, 1)
	require.Equal(t, "gpt-5.6-Luna", h.state().Last[0].Model, "探测用水位里记的原样模型名，手选的 gpt-6-manual 忽略")
	require.Equal(t, []string{"gpt-5.6-Luna"}, h.gw.openAITurnStateTrafficModels(h.account, now.Add(-time.Hour)))
	require.Equal(t, []string{"gpt-5.6-Luna", hunterTestModel}, h.gw.openAITurnStateTrafficModels(h.account, time.Time{}), "since 零值 = 进程内见过的全部够格模型，按名排序")

	// 已被停调度的模型即使铸造记忆清零（重启）也算在管：不能因此把暂停放回。
	held := newHunterHarness(hunterTestAccount(holdHunterConfig(map[string]any{"auto_models": true})), hunterWebshareProxy)
	markHeld(held.account, now.Add(20*time.Hour))
	require.True(t, held.gw.openAITurnStateHuntedModel(held.account, hunterTestModel))
	require.True(t, held.gw.openAITurnStateHoldEnabled(held.account, hunterTestModel))
	require.False(t, held.gw.openAITurnStateHuntedModel(held.account, "gpt-5.6-Luna"))

	// 画图模型排在兜底之前：手选模式下停了 gpt-image-2 再切自动，不能靠兜底继续猎它。
	imgHeld := newHunterHarness(hunterTestAccount(holdHunterConfig(map[string]any{"auto_models": true})), hunterWebshareProxy)
	setAccountModelRateLimitSnapshot(imgHeld.account, "gpt-image-2", now.Add(30*time.Minute), openAITurnStateHoldLimitReason, now)
	require.False(t, imgHeld.gw.openAITurnStateHuntedModel(imgHeld.account, "gpt-image-2"))
	imgHeld.run(t)
	require.Empty(t, imgHeld.up.requests, "不拿文本探测体去打画图模型")
	require.Equal(t, 1, imgHeld.repo.clears, "hold 已不再成立：放回")

	// 重启后铸造记忆为空：池里有它的票就是铸过的证据，立刻够格（否则有票也不注入、也不猎）。
	pooled := newHunterHarness(hunterTestAccount(hunterConfig(map[string]any{"auto_models": true})), hunterWebshareProxy)
	require.False(t, pooled.gw.openAITurnStateHuntedModel(pooled.account, hunterTestModel))
	pooled.account.Extra[openAITurnStatePoolExtraKey] = []any{map[string]any{
		"blob": turnStateFernetBlob(now.Add(-2*time.Hour), openAIHealthyTurnStateBlocks), "model": hunterTestModel,
		"minted_at": now.Add(-2 * time.Hour).Format(time.RFC3339),
	}}
	require.True(t, pooled.gw.openAITurnStateHuntedModel(pooled.account, hunterTestModel), "过期的票也算证据")
	require.False(t, pooled.gw.openAITurnStateHuntedModel(pooled.account, "gpt-5.6-Luna"))
}
