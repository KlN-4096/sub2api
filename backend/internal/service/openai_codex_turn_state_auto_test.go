//go:build unit

package service

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// turnStateAutoRepo 只实现自动接管真正会调到的三个方法，其余继承 accountRepoStub 的
// panic 实现——多调一个方法就会当场炸出来。
type turnStateAutoRepo struct {
	*accountRepoStub

	mu sync.Mutex
	// latest 模拟 DB 侧的账号当前态：候选池维护会在锁内重新读它。
	latest      *Account
	extraWrites []map[string]any
	schedulable []bool
	errors      []string
	// getByIDCalls 数同步读库次数。入池那条路每次要 GetByID（生产实现还连带
	// loadProxies / loadAccountGroups 共 3 条 SELECT），而它在响应首字节之前、
	// 持着账号锁——所以「接管关着的账号一次都不该读」是条要守的不变量。
	getByIDCalls int
	// holds / clears 记降智暂停的停调度与放回；stub 同时按生产语义改 latest（until 只能往后推）。
	holds  []string
	clears int
}

// SetModelRateLimit 模拟降智暂停的落库：未来的 reset 记一次 hold（scope），过去的记一次 clear
// （放回就是写成已到期），并同步 DB 侧账号的 model_rate_limits。
func (r *turnStateAutoRepo) SetModelRateLimit(_ context.Context, _ int64, scope string, resetAt time.Time, reason ...string) error {
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
	if r.latest != nil {
		setAccountModelRateLimitSnapshot(r.latest, scope, resetAt, why, time.Now())
	}
	return nil
}

func (r *turnStateAutoRepo) GetByID(_ context.Context, _ int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getByIDCalls++
	if r.latest == nil {
		return nil, errors.New("not found")
	}
	return r.latest, nil
}

func newTurnStateAutoRepo() *turnStateAutoRepo {
	return &turnStateAutoRepo{accountRepoStub: &accountRepoStub{}}
}

// UpdateExtra 也自增 getByIDCalls：真实的 accountRepository.UpdateExtra 尾部无条件调
// syncSchedulerAccountSnapshot → GetByID（连带 loadProxies / loadAccountGroups 共 3 条
// SELECT）+ 一次 Redis SetAccount。stub 里只 append 的话，「响应路径上读了几次库」这个
// 断言看不到这一层，会恒绿。
func (r *turnStateAutoRepo) UpdateExtra(_ context.Context, _ int64, updates map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extraWrites = append(r.extraWrites, updates)
	r.getByIDCalls++
	return nil
}

func (r *turnStateAutoRepo) SetSchedulable(_ context.Context, _ int64, schedulable bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schedulable = append(r.schedulable, schedulable)
	return nil
}

func (r *turnStateAutoRepo) SetError(_ context.Context, _ int64, msg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, msg)
	return nil
}

const turnStateTestModel = "gpt-5.6-luna"

// 实测的 individual 号形态，只有测试用它们造样本——生产判据是 openAITurnStateShapes
// 那张表（含 team 形态）。放在测试侧而不是生产侧：留在生产侧就是 unused，而 292 / 312 /
// 10 这几个数在测试里比 openAITurnStateShapes[0].Chars 直观得多。
// 与 openAITurnStateShapes 的一致性由 TestOpenAITurnStateShapeTable 钉住。
const (
	openAIHealthyTurnStateLen    = 292
	openAIDegradedTurnStateLen   = 312
	openAIHealthyTurnStateBlocks = 10
)

func turnStateAutoCtx(sessionID string) *gin.Context {
	return turnStateAutoCtxModel(sessionID, turnStateTestModel)
}

// turnStateAutoCtxModel 造一个带模型归属的请求上下文。出站各路径都会在分发前
// SetOpsUpstreamModel，自动接管就是从那里读本次模型的。
func turnStateAutoCtxModel(sessionID, model string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	if sessionID != "" {
		c.Request.Header.Set("Session-Id", sessionID)
	}
	SetOpsUpstreamModel(c, model)
	return c
}

func turnStateAutoAccount() *Account {
	return &Account{
		ID:       7,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{openAITurnStateAutoExtraKey: true},
	}
}

// healthy/degraded 只用长度说话——判定逻辑只看 len == 292。
func turnStateBlob(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(i%26)
	}
	return string(b)
}

// TestPickOpenAITurnStateCandidate 钉住候选选取：跳过失效、跳过过期、只取同模型的。
func TestPickOpenAITurnStateCandidate(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	ttl := time.Hour
	const m = turnStateTestModel
	pick := func(pool []openAITurnStateCandidate, model string) (openAITurnStateCandidate, string, bool) {
		return pickOpenAITurnStateCandidate(pool, model, ttl, now)
	}
	fresh := func(blob, model string) openAITurnStateCandidate {
		return openAITurnStateCandidate{Blob: blob, Model: model, MintedAt: now.Add(-time.Minute)}
	}

	_, _, ok := pick(nil, m)
	require.False(t, ok, "空池不注入")

	pool := []openAITurnStateCandidate{
		{Blob: "failed", Model: m, MintedAt: now.Add(-time.Minute), Failed: true},
		fresh("fresh", m),
		{Blob: "old", Model: m, MintedAt: now.Add(-3 * time.Hour)},
	}
	got, source, ok := pick(pool, m)
	require.True(t, ok)
	require.Equal(t, "fresh", got.Blob, "失效候选必须跳过")
	require.Equal(t, turnStateSourceAuto, source)

	// 模型是硬门槛：turn-state 与模型强绑定，别的模型的票注进来只会白撞一次 400。
	_, _, ok = pick(pool, "gpt-6-astra")
	require.False(t, ok, "别的模型的候选不得注入")
	got, _, ok = pick([]openAITurnStateCandidate{fresh("luna", m), fresh("astra", "gpt-6-astra")}, "gpt-6-astra")
	require.True(t, ok)
	require.Equal(t, "astra", got.Blob, "必须取本模型的那条")
	_, _, ok = pick(pool, "")
	require.False(t, ok, "取不到本次模型时不注入")
	_, _, ok = pick([]openAITurnStateCandidate{fresh("legacy", "")}, m)
	require.False(t, ok, "没有模型归属的历史条目不得注入")

	// 有效期是硬门槛：过期就不注入，等下一条自然铸出的 292（对家实时池六张卡实测
	// 到期 = Fernet 铸造戳 + 1 小时）。
	_, _, ok = pick([]openAITurnStateCandidate{{Blob: "old", Model: m, MintedAt: now.Add(-3 * time.Hour)}}, m)
	require.False(t, ok, "过期候选不得注入")

	// 边界：正好满 1 小时算过期，差 1 纳秒还能用。
	_, _, ok = pick([]openAITurnStateCandidate{{Blob: "edge", Model: m, MintedAt: now.Add(-ttl)}}, m)
	require.False(t, ok, "铸造后满 ttl 即过期")
	_, _, ok = pick([]openAITurnStateCandidate{{Blob: "edge", Model: m, MintedAt: now.Add(-ttl + time.Nanosecond)}}, m)
	require.True(t, ok, "未满 ttl 仍可用")

	// 信封解不出铸造时刻（MintedAt 零值）时按不过期处理，不要静默停掉功能。
	_, _, ok = pick([]openAITurnStateCandidate{{Blob: "undecodable", Model: m}}, m)
	require.True(t, ok, "解不出铸造时刻的候选照常可用")

	allFailed := []openAITurnStateCandidate{{Blob: "a", Model: m, Failed: true}, {Blob: "", Model: m, MintedAt: now}}
	_, _, ok = pick(allFailed, m)
	require.False(t, ok, "全失效（空 blob 也算不可用）= 耗尽")

	// 耗尽判定不看有效期：过期只是等新票，不该把账号停掉。
	require.False(t, openAITurnStateModelAlive(allFailed, m))
	require.True(t, openAITurnStateModelAlive(
		[]openAITurnStateCandidate{{Blob: "old", Model: m, MintedAt: now.Add(-3 * time.Hour)}}, m),
		"过期但未失效的候选仍算「降级链没走完」")
	require.False(t, openAITurnStateModelAlive([]openAITurnStateCandidate{fresh("x", m)}, "gpt-6-astra"))
}

// TestOpenAITurnStateAutoOnlyInjectsDegradedSessions 钉住方案 B：
// 只有被判定落在 312 的 session 才注入，其余保持真客户端形态。
func TestOpenAITurnStateAutoOnlyInjectsDegradedSessions(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
	}

	// 没有 session 判定记录 → 不注入
	override, source := svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("sess-A"), account)
	require.Empty(t, override, "未判定降智的 session 不注入")
	require.Empty(t, source)

	// 该 session 自然铸出 312 → 判定降智
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess-A"), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	override, source = svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("sess-A"), account)
	require.Equal(t, healthy, override, "判定降智后必须注入健康候选")
	require.Equal(t, turnStateSourceAuto, source)

	// 另一个 session 不受影响
	override, _ = svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("sess-B"), account)
	require.Empty(t, override, "降智判定按 session 分域，不外溢")

	// 没带 session-id 的请求走占位域，它自己还没被判降智，所以也不注入
	//（占位域本身能被判定，见 TestOpenAITurnStateSessionlessClientIsCovered）
	override, _ = svc.resolveOpenAITurnStateOverride(turnStateAutoCtx(""), account)
	require.Empty(t, override)
}

// TestOpenAITurnStateAutoBeatsManual 钉住「系统接管」：开了自动，手填值一个字节都不生效。
func TestOpenAITurnStateAutoBeatsManual(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStateOverrideExtraKey] = map[string]any{turnStateTestModel: "手填的值"}
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
	}
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	override, source := svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("sess"), account)
	require.Equal(t, healthy, override, "自动接管优先于手填")
	require.Equal(t, turnStateSourceAuto, source)

	// 候选池空时也不回退到手填——接管就是接管，回退会让「已接管」的说明变成谎话
	account.Extra[openAITurnStatePoolExtraKey] = []any{}
	override, source = svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("sess"), account)
	require.Empty(t, override, "没有候选时不得回落到手填值")
	require.Empty(t, source)

	// 关掉开关，手填立刻恢复生效
	delete(account.Extra, openAITurnStateAutoExtraKey)
	override, source = svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("sess"), account)
	require.Equal(t, "手填的值", override)
	require.Equal(t, turnStateSourceManual, source)
}

// TestOpenAITurnStateAutoInjectedMintDoesNotResetSession 钉住防跳变：
// 注入请求铸出的 blob 不回写 session 判定，否则注入一生效就把降智标记抹掉，来回跳。
func TestOpenAITurnStateAutoInjectedMintDoesNotResetSession(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
	}

	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	c := turnStateAutoCtx("sess")
	override, _ := svc.resolveOpenAITurnStateOverride(c, account)
	require.Equal(t, healthy, override)
	// 注入生效：上游改铸出另一条 292。它入池（见 TestOpenAITurnStateInjectedMintRefillsPool），
	// 但不回写 session 判定——这两件事分开，本用例只钉后者。
	fresher := turnStateBlob(openAIHealthyTurnStateLen-1) + "z"
	require.Len(t, fresher, openAIHealthyTurnStateLen)
	svc.observeOpenAITurnStateMint(c, account, fresher)

	// 该 session 仍被判定为降智：若拿注入后的结果回写判定，下一轮就不注入、
	// 上游又铸回 312，两个状态来回跳。注入的换成了刚入池的新票（栈顶优先）。
	require.Equal(t, fresher,
		mustResolve(t, svc, turnStateAutoCtx("sess"), account), "注入成功不得清掉降智判定")
}

// TestOpenAITurnStateInjectedMintRefillsPool 复现「候选池饿死」。
//
// 一个 session 一旦被判降智，之后每一条请求都带注入，于是永远走不到「未注入」那一支。
// 入池若也锁在那一支里，池子就只出不进：候选 1 小时到期后自动接管静默停摆，表现为
// 「这个号一直降智，接管好像没工作」，且日志里一个字都没有。
func TestOpenAITurnStateInjectedMintRefillsPool(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
	}
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	c := turnStateAutoCtx("sess")
	require.Equal(t, healthy, mustResolve(t, svc, c, account), "判定降智后应注入")

	// 注入生效：上游改铸出另一条健康 292。它必须入池，这是降智账号唯一的补票来源。
	fresher := turnStateBlob(openAIHealthyTurnStateLen-1) + "z"
	require.Len(t, fresher, openAIHealthyTurnStateLen)
	svc.observeOpenAITurnStateMint(c, account, fresher)

	pool := readOpenAITurnStatePool(account)
	require.Len(t, pool, 2, "注入请求铸出的健康票必须入池，否则池子只出不进")
	require.Equal(t, fresher, pool[0].Blob, "新票压栈顶")
}

// TestOpenAITurnStateSessionlessClientIsCovered 钉住：客户端不发 session-id 时，
// 判定退化成「账号 + 模型」粒度，而不是整条链路静默熄火。
//
// 熄火的形态很隐蔽：key 为空 → 既查不到判定、也记不下判定，于是自动接管对 curl 和
// 不发该头的第三方客户端等于根本没开，还不报错不打日志。
func TestOpenAITurnStateSessionlessClientIsCovered(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
	}

	require.Empty(t, mustResolve(t, svc, turnStateAutoCtx(""), account), "还没判定降智，先不注入")

	svc.observeOpenAITurnStateMint(turnStateAutoCtx(""), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	require.Equal(t, healthy, mustResolve(t, svc, turnStateAutoCtx(""), account),
		"无 session-id 的请求也要能被接管")
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("sess-real"), account),
		"占位域不得外溢到真实 session")
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtxModel("", "other-model"), account),
		"占位域仍按模型分域")
}

func mustResolve(t *testing.T, svc *OpenAIGatewayService, c *gin.Context, account *Account) string {
	t.Helper()
	override, _ := svc.resolveOpenAITurnStateOverride(c, account)
	return override
}

// TestOpenAITurnStateAutoDegradesThenDisables 钉住失效链路：
// 注入的票被上游以 invalid_encrypted_content 拒绝 → 该候选失效 → 降级到下一条 →
// 全部失效 → 停调度并写明原因。
//
// 驱动失效的**只能**是这条硬证据。曾经「注入 292、上游仍铸 312」也会记一次失败，
// 那个判据是错的，已移除——见 TestOpenAITurnStateInjectedTicketSurvivesDegradedMint。
func TestOpenAITurnStateAutoDegradesThenDisables(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	first := turnStateBlob(openAIHealthyTurnStateLen)
	second := turnStateBlob(openAIHealthyTurnStateLen-1) + "z"
	require.NotEqual(t, first, second)

	minted := time.Now().UTC().Format(time.RFC3339)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": first, "minted_at": minted},
		map[string]any{"model": turnStateTestModel, "blob": second, "minted_at": minted},
	}
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	// 第 1 轮：注入 first，上游拒绝这条 blob → 默认阈值 1，first 立即失效
	c := turnStateAutoCtx("sess")
	require.Equal(t, first, mustResolve(t, svc, c, account))
	svc.noteOpenAITurnStateRejected(c, account)
	require.Empty(t, repo.schedulable, "还有候选就不该停账号")

	// 第 2 轮：降级到 second
	c = turnStateAutoCtx("sess")
	require.Equal(t, second, mustResolve(t, svc, c, account), "必须降级到下一条候选")
	svc.noteOpenAITurnStateRejected(c, account)

	require.Equal(t, []bool{false}, repo.schedulable, "候选耗尽必须停调度")
	require.Len(t, repo.errors, 1)
	// 原因里必须写清失效的真实来源，别再写成「上游仍铸出更长的值」——那个判据已移除。
	require.Contains(t, repo.errors[0], "invalid_encrypted_content")
	require.Contains(t, repo.errors[0], "候选已全部失效")
	require.False(t, account.Schedulable)

	// 停掉之后不再注入（池里已无可用候选）
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("sess"), account))
}

// TestOpenAITurnStateWSClearsInjectionMarkerAcrossAttempts 钉住 failover 串账：
// attempt 1 走 HTTP 注入过、attempt 2 换号走 WS 时必须清掉注入标记，否则第二个账号
// 的使用记录会记成 overridden=true / source=manual，而它这一轮根本没注入。
func TestOpenAITurnStateWSClearsInjectionMarkerAcrossAttempts(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	c := turnStateAutoCtx("sess")

	// attempt 1：账号 A 手填覆写生效，标记写进上下文。
	accountA := turnStateAutoAccount()
	delete(accountA.Extra, openAITurnStateAutoExtraKey)
	accountA.Extra[openAITurnStateOverrideExtraKey] = map[string]any{turnStateTestModel: "manual-blob-a"}
	require.Equal(t, "manual-blob-a",
		svc.applyOpenAICodexTurnStateOverrideWSManualOnly(c, accountA, ""))
	require.Equal(t, turnStateSourceManual, OpenAITurnStateUsageSource(c))

	// attempt 2：failover 到没有覆写的账号 B，同一个 gin.Context。
	accountB := turnStateAutoAccount()
	accountB.ID = 8
	delete(accountB.Extra, openAITurnStateAutoExtraKey)
	require.Equal(t, "echoed",
		svc.applyOpenAICodexTurnStateOverrideWSManualOnly(c, accountB, "echoed"))
	require.Empty(t, OpenAITurnStateUsageSource(c), "上一个账号的注入标记必须被清掉")
	require.False(t, *usageCodexTurnStateOverriddenPtr(accountB, OpenAITurnStateUsageSource(c)),
		"没注入就不该记成覆写")
}

// TestOpenAITurnStateExpiredCandidatesStayInPool 钉住「过期 ≠ 降级链被消耗」：
// 入池时不得把过期候选物理删掉，否则同模型下最后一条新鲜候选失败时，
// 本该还剩的格子已经没了，账号会被提前停掉。
func TestOpenAITurnStateExpiredCandidatesStayInPool(t *testing.T) {
	const m = turnStateTestModel
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolSizeExtraKey] = 3

	// 必须种成数组形态：readOpenAITurnStatePool 走 json.Marshal(raw) → Unmarshal，
	// 种成 JSON 字符串会解成 nil，断言就变成空跑。
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{
			"blob":      "old",
			"model":     m,
			"minted_at": time.Now().UTC().Add(-3 * time.Hour).Format(time.RFC3339),
		},
	}

	fresh := turnStateBlob(openAIHealthyTurnStateLen)
	svc.pushOpenAITurnStateCandidate(turnStateAutoCtx("s"), account, fresh)

	pool := readOpenAITurnStatePool(account)
	require.Len(t, pool, 2, "过期候选不得在入池时被删掉")
	require.True(t, openAITurnStateModelAlive(pool, m))

	// 新鲜的那条失败 → 池里还剩过期但未失效的一条 → 不该停账号。
	c := turnStateAutoCtx("s")
	markOpenAITurnStateInjected(c, fresh, turnStateSourceAuto)
	svc.recordOpenAITurnStateFailure(c, account, fresh)
	require.Empty(t, repo.schedulable, "降级链还剩一格就不该停账号")

	// 超出本模型配额时过期条目才出局，数量有界。
	for i := 1; i <= 3; i++ {
		svc.pushOpenAITurnStateCandidate(turnStateAutoCtx("s"), account,
			turnStateBlob(openAIHealthyTurnStateLen-i)+strings.Repeat("y", i))
	}
	require.Len(t, readOpenAITurnStatePool(account), 3, "限深仍按模型生效")
}

// turnStateFernetBlob 造一条真 Fernet 信封：0x80 | 8B 大端铸造戳 | 16B IV |
// blocks×16B 密文 | 32B HMAC。turnStateBlob 造的是纯长度样本，解不出信封，
// 凡是要测有效期的地方都必须用真的。
func turnStateFernetBlob(minted time.Time, blocks int) string {
	raw := make([]byte, 1+8+16+blocks*16+32)
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(minted.Unix()))
	return base64.URLEncoding.EncodeToString(raw)
}

// TestOpenAITurnStateManualOverrideExpires 钉住手填覆写也有 1 小时有效期。
//
// 不加这道闸的话：手填只在自动接管关闭时生效，而失效归因要求自动接管开着，
// 所以过期的手填票会每次注入、每次撞 400，并且永远不会被发现。
func TestOpenAITurnStateManualOverrideExpires(t *testing.T) {
	now := time.Now().UTC()
	account := turnStateAutoAccount()
	delete(account.Extra, openAITurnStateAutoExtraKey)
	setOverride := func(blob string) {
		account.Extra[openAITurnStateOverrideExtraKey] = map[string]any{turnStateTestModel: blob}
	}

	fresh := turnStateFernetBlob(now.Add(-5*time.Minute), openAIHealthyTurnStateBlocks)
	setOverride(fresh)
	require.Equal(t, fresh, account.OpenAICodexTurnStateOverride(turnStateTestModel), "未过期照常生效")

	expired := turnStateFernetBlob(now.Add(-2*time.Hour), openAIHealthyTurnStateBlocks)
	setOverride(expired)
	require.Empty(t, account.OpenAICodexTurnStateOverride(turnStateTestModel), "过期的手填值不得再注入")

	// 有效期跟随账号配置。
	account.Extra[openAITurnStateStaleMinExtraKey] = 180
	require.Equal(t, expired, account.OpenAICodexTurnStateOverride(turnStateTestModel), "有效期应跟随配置")
	delete(account.Extra, openAITurnStateStaleMinExtraKey)

	// 解不出信封的值按不过期处理，别因为解码失败静默关掉功能。
	setOverride(turnStateBlob(openAIHealthyTurnStateLen))
	require.NotEmpty(t, account.OpenAICodexTurnStateOverride(turnStateTestModel))

	// 两条出站路径都要挡住，不能只挡一条。
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	setOverride(expired)
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("s"), account), "HTTP 路径")
	require.Equal(t, "echoed",
		svc.applyOpenAICodexTurnStateOverrideWSManualOnly(turnStateAutoCtx("s"), account, "echoed"),
		"WS 路径：过期就当没配，保留客户端回带值")
}

// TestOpenAITurnStateAutoIsModelScoped 钉住「turn-state 与模型强绑定」：
// 候选池、session 降智判定都按 (账号, 模型) 分桶，A 模型的票不会注给 B 模型。
func TestOpenAITurnStateAutoIsModelScoped(t *testing.T) {
	const luna, astra = turnStateTestModel, "gpt-6-astra"
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	account := turnStateAutoAccount()
	healthy := turnStateBlob(openAIHealthyTurnStateLen)

	// luna 上自然铸出一条健康票，入池时带上 luna 的归属。
	svc.observeOpenAITurnStateMint(turnStateAutoCtxModel("s1", luna), account, healthy)
	pool := readOpenAITurnStatePool(account)
	require.Len(t, pool, 1)
	require.Equal(t, luna, pool[0].Model, "候选必须记下铸造它的模型")

	// 同一个 session 在 luna 上被判降智 → luna 注入，astra 不注入。
	svc.observeOpenAITurnStateMint(turnStateAutoCtxModel("s1", luna), account,
		turnStateBlob(openAIDegradedTurnStateLen))
	require.Equal(t, healthy, mustResolve(t, svc, turnStateAutoCtxModel("s1", luna), account))
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtxModel("s1", astra), account),
		"换模型就是另一张票，不该沿用 luna 的降智判定")

	// astra 自己被判降智，但池里没有 astra 的票 → 仍然不注入（等它自己铸出 292）。
	svc.observeOpenAITurnStateMint(turnStateAutoCtxModel("s1", astra), account,
		turnStateBlob(openAIDegradedTurnStateLen))
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtxModel("s1", astra), account),
		"没有本模型的候选就不注入")

	// 取不到本次模型时既不注入也不入池——归不到模型的票没法用。
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtxModel("s1", ""), account))
	svc.observeOpenAITurnStateMint(turnStateAutoCtxModel("s2", ""), account,
		turnStateBlob(openAIHealthyTurnStateLen-1)+"z")
	require.Len(t, readOpenAITurnStatePool(account), 1, "没有模型归属的票不入池")
}

// TestOpenAITurnStatePoolDepthIsPerModel 钉住限深按模型算：
// 全局截断会让活跃模型把冷门模型的票挤光，那个模型就永远补不上。
func TestOpenAITurnStatePoolDepthIsPerModel(t *testing.T) {
	const luna, astra = turnStateTestModel, "gpt-6-astra"
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolSizeExtraKey] = 1

	astraBlob := turnStateBlob(openAIHealthyTurnStateLen)
	svc.pushOpenAITurnStateCandidate(turnStateAutoCtxModel("s", astra), account, astraBlob)
	for i := 1; i <= 3; i++ {
		svc.pushOpenAITurnStateCandidate(turnStateAutoCtxModel("s", luna), account,
			turnStateBlob(openAIHealthyTurnStateLen-i)+strings.Repeat("z", i))
	}

	pool := readOpenAITurnStatePool(account)
	byModel := map[string]int{}
	for _, c := range pool {
		byModel[c.Model]++
	}
	require.Equal(t, map[string]int{luna: 1, astra: 1}, byModel, "限深必须按模型各算各的")
	_, _, ok := pickOpenAITurnStateCandidate(pool, astra, time.Hour, time.Now())
	require.True(t, ok, "冷门模型的票不该被活跃模型挤掉")
}

// TestOpenAITurnStateOverridesSurviveJSONRoundTrip 钉住读取走 JSON 往返而不是裸
// 类型断言。extra 是 JSONB：同一个键在「刚从请求体解出来」和「从 DB 读回来」两条
// 路径上的具体 Go 类型不保证相同，裸断言失败是静默的——覆写永不生效且没有线索。
func TestOpenAITurnStateOverridesSurviveJSONRoundTrip(t *testing.T) {
	const model, blob = turnStateTestModel, "gAAAAAB-luna"
	account := turnStateAutoAccount()
	delete(account.Extra, openAITurnStateAutoExtraKey)

	for name, stored := range map[string]any{
		"map[string]any":    map[string]any{model: blob},
		"map[string]string": map[string]string{model: blob},
		"json.RawMessage":   json.RawMessage(`{"` + model + `":"` + blob + `"}`),
	} {
		t.Run(name, func(t *testing.T) {
			account.Extra[openAITurnStateOverrideExtraKey] = stored
			require.Equal(t, blob, account.OpenAICodexTurnStateOverride(model))
		})
	}

	// 旧的单字符串形态没有模型归属，按未配置处理——不能静默注给所有模型。
	account.Extra[openAITurnStateOverrideExtraKey] = blob
	require.Empty(t, account.OpenAICodexTurnStateOverride(model))
}

// TestOpenAITurnStateManualOverrideIsPerModel 钉住手填覆写按模型取票。
//
// blob 是密文，系统无从得知它来自哪个模型，只能由管理员填的时候指定。取不到本次
// 模型、或本次模型没配票时一律不注入——不能再有「不限模型」这种兜底：注给别的模型
// 只会白撞一次 invalid_encrypted_content。
func TestOpenAITurnStateManualOverrideIsPerModel(t *testing.T) {
	const luna, astra = turnStateTestModel, "gpt-6-astra"
	lunaBlob, astraBlob := turnStateBlob(openAIHealthyTurnStateLen), turnStateBlob(openAIHealthyTurnStateLen-1)
	account := turnStateAutoAccount()
	delete(account.Extra, openAITurnStateAutoExtraKey)
	account.Extra[openAITurnStateOverrideExtraKey] = map[string]any{
		"GPT-5.6-Luna": lunaBlob,
		astra:          astraBlob,
	}

	require.Equal(t, lunaBlob, account.OpenAICodexTurnStateOverride(luna), "大小写不敏感")
	require.Equal(t, astraBlob, account.OpenAICodexTurnStateOverride(astra), "各模型取各自的票")
	require.Empty(t, account.OpenAICodexTurnStateOverride("gpt-5.6-sol"), "没配票的模型不得注入")
	require.Empty(t, account.OpenAICodexTurnStateOverride(""), "取不到模型时不注入")

	// HTTP 与 WS 两条出站路径读的是同一张表。
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	require.Equal(t, lunaBlob, mustResolve(t, svc, turnStateAutoCtxModel("s", luna), account))
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtxModel("s", "gpt-5.6-sol"), account))
	require.Equal(t, astraBlob,
		svc.applyOpenAICodexTurnStateOverrideWSManualOnly(turnStateAutoCtxModel("s", astra), account, "echoed"))
	require.Equal(t, "echoed",
		svc.applyOpenAICodexTurnStateOverrideWSManualOnly(turnStateAutoCtxModel("s", "gpt-5.6-sol"), account, "echoed"),
		"没配票就保留客户端回带值")
}

// TestOpenAITurnStateAutoPushesHealthyMintIntoPool 钉住入池：健康 blob 自动收集、去重、限深。
func TestOpenAITurnStateAutoPushesHealthyMintIntoPool(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolSizeExtraKey] = 2

	blobs := []string{
		turnStateBlob(openAIHealthyTurnStateLen),
		turnStateBlob(openAIHealthyTurnStateLen-1) + "z",
		turnStateBlob(openAIHealthyTurnStateLen-2) + "yz",
	}
	for _, b := range blobs {
		svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account, b)
		svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account, b) // 同一条回带多次
	}
	// 312 不入池
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	pool := readOpenAITurnStatePool(account)
	require.Len(t, pool, 2, "池深必须按配置截断")
	require.Equal(t, blobs[2], pool[0].Blob, "最新的在栈顶")
	require.Equal(t, blobs[1], pool[1].Blob)
	// 每次写库只碰一个键：候选池与形态观测各写各的，绝不把两者合成一条 UPDATE——
	// 合并写会让候选池跟着观测的节流走，反过来也一样。
	require.NotEmpty(t, repo.extraWrites)
	poolWrites := 0
	for _, w := range repo.extraWrites {
		require.Len(t, w, 1, "一次 UpdateExtra 只写一个键")
		if _, ok := w[openAITurnStatePoolExtraKey]; ok {
			poolWrites++
			continue
		}
		require.Contains(t, w, openAITurnStateObservedExtraKey, "除候选池外只允许形态观测")
	}
	require.Equal(t, 3, poolWrites, "三条不同的健康 blob 各入池一次，回带重复的不重复写")
}

// TestOpenAITurnStateAutoIgnoresNonCodexAccounts 钉住适用范围：非 Codex 上游一个字节都不碰。
func TestOpenAITurnStateAutoIgnoresNonCodexAccounts(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	apikey := &Account{
		ID: 9, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Extra: map[string]any{openAITurnStateAutoExtraKey: true},
	}
	require.False(t, apikey.IsOpenAITurnStateAutoEnabled())

	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), apikey,
		turnStateBlob(openAIHealthyTurnStateLen))
	require.Empty(t, repo.extraWrites, "非 Codex 账号不得写候选池")

	override, source := svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("sess"), apikey)
	require.Empty(t, override)
	require.Empty(t, source)
}

// TestOpenAITurnStateAutoSkippedOnWSContext 钉住 B 类泄漏：WS 入口一旦经手这个上下文，
// 后续任何 HTTP 出站构建（WS ingress 的 HTTP 桥就是拿同一个 c 走 passthrough 的）
// 都不得再做自动接管——它的判定按「一次请求」设计，套到长连接上会失真。
func TestOpenAITurnStateAutoSkippedOnWSContext(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
	}
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	// 同一个 c 先被 WS 入口经手，再走 HTTP 出站头构建
	c := turnStateAutoCtx("sess")
	require.Equal(t, "原值", svc.applyOpenAICodexTurnStateOverrideWSManualOnly(c, account, "原值"),
		"开了自动接管时 WS 不应用手填值")

	h := http.Header{}
	svc.applyOpenAICodexTurnStateOverrideHeader(c, account, h)
	require.Empty(t, h.Get(openAICodexTurnStateHeader), "WS 上下文里不得自动接管")
	require.Empty(t, OpenAITurnStateUsageSource(c))

	// 干净的 HTTP 上下文照常接管，证明上面的空不是因为别的原因
	fresh := turnStateAutoCtx("sess")
	svc.applyOpenAICodexTurnStateOverrideHeader(fresh, account, http.Header{})
	require.Equal(t, turnStateSourceAuto, OpenAITurnStateUsageSource(fresh))
}

// TestOpenAITurnStateWSManualRecordsUsageSource 钉住 WS 手填覆写仍然记进使用记录。
func TestOpenAITurnStateWSManualRecordsUsageSource(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	account := turnStateAutoAccount()
	delete(account.Extra, openAITurnStateAutoExtraKey)
	account.Extra[openAITurnStateOverrideExtraKey] = map[string]any{turnStateTestModel: "手填的值"}

	c := turnStateAutoCtx("sess")
	require.Equal(t, "手填的值", svc.applyOpenAICodexTurnStateOverrideWSManualOnly(c, account, "客户端自带"))
	require.Equal(t, turnStateSourceManual, OpenAITurnStateUsageSource(c))
	require.Equal(t, "手填的值", openAITurnStateInjectedFromContext(c),
		"帧填充据此判断是否强制覆盖客户端自带 blob")

	// 没配手填 = 功能不存在，一个标记都不留
	plain := turnStateAutoCtx("sess")
	bare := &Account{ID: 8, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	require.Equal(t, "客户端自带", svc.applyOpenAICodexTurnStateOverrideWSManualOnly(plain, bare, "客户端自带"))
	require.Empty(t, OpenAITurnStateUsageSource(plain))
	require.Empty(t, openAITurnStateInjectedFromContext(plain))
}

// TestOpenAITurnStateInjectionMarkerClearedPerAttempt 钉住 failover：c 在整个重试循环里
// 是同一个，上一个账号的注入标记不清掉，会把失效判定记到换号后的账号头上。
func TestOpenAITurnStateInjectionMarkerClearedPerAttempt(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	first := &Account{
		ID: 11, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Extra: map[string]any{openAITurnStateOverrideExtraKey: map[string]any{
			turnStateTestModel: "第一个账号的手填值",
		}},
	}
	second := &Account{ID: 12, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	c := turnStateAutoCtx("sess")
	svc.applyOpenAICodexTurnStateOverrideHeader(c, first, http.Header{})
	require.Equal(t, "第一个账号的手填值", openAITurnStateInjectedFromContext(c))

	// 换号重试：新账号没配覆写，标记必须清干净
	h := http.Header{}
	svc.applyOpenAICodexTurnStateOverrideHeader(c, second, h)
	require.Empty(t, h.Get(openAICodexTurnStateHeader))
	require.Empty(t, openAITurnStateInjectedFromContext(c), "上个账号的注入标记必须清掉")
	require.Empty(t, OpenAITurnStateUsageSource(c))
}

// TestOpenAITurnStateAutoDefaults 钉住用户确认过的三个默认值。
// 全部测试都用常量构造数据，改了常量测试会跟着改——这里直接钉字面量。
func TestOpenAITurnStateAutoDefaults(t *testing.T) {
	// 真实捕获样本（pro3），运维口径「292 = 不降智」的由来
	const realHealthy = "gAAAAABqqrNHYSOlO_EUJI-hlduVBqJ8slR-floDb7J-ZYvvLXj7WV7dOZ_zk10RDMl_N4dRvG0UqxWR19XdSGbeHFUEAzwv7yQBADQrB1QhpOKkfcUPeSy2qsvZIvq__OHHoF2yCZfSTPq6YvkKahwLUxkeORhQZ9Ug86sMJwkrJXUefsa6fTpRqzZSN7SLphKU-6Ys6FV3GveSXjgk0UcCaKvfShFj4_EmGriyCb-JVoU0D8LJbjsClcivKgDNu1jfZfF-6q8VXHGF1Uck7vDVXdiNh1kRXw=="
	require.Len(t, realHealthy, openAIHealthyTurnStateLen, "健康长度必须与真实样本一致")
	require.Equal(t, 292, openAIHealthyTurnStateLen)
	require.Equal(t, 312, openAIDegradedTurnStateLen)

	bare := turnStateAutoAccount()
	require.Equal(t, 3, bare.openAITurnStatePoolSize(), "候选池深度默认 3")
	require.Equal(t, 1, bare.openAITurnStateFailThreshold(), "失效阈值默认 1 次")
	require.Equal(t, time.Hour, bare.openAITurnStateStaleAfter(), "保鲜期默认 60 分钟")

	// 三个都是后端配置项（不开放前端），能被 extra 覆盖
	bare.Extra[openAITurnStatePoolSizeExtraKey] = 5
	bare.Extra[openAITurnStateFailThreshExtraKey] = 2
	bare.Extra[openAITurnStateStaleMinExtraKey] = 30
	require.Equal(t, 5, bare.openAITurnStatePoolSize())
	require.Equal(t, 2, bare.openAITurnStateFailThreshold())
	require.Equal(t, 30*time.Minute, bare.openAITurnStateStaleAfter())
}

// TestOpenAITurnStateSessionKeyIsAccountScoped 钉住会话分域：
// 同一个 session id 在不同凭证域下是两段独立上游会话，混用会让 A 的降智判定作用到 B。
func TestOpenAITurnStateSessionKeyIsAccountScoped(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	minted := time.Now().UTC().Format(time.RFC3339)
	pool := []any{map[string]any{"model": turnStateTestModel, "blob": healthy, "minted_at": minted}}

	a := turnStateAutoAccount()
	a.Extra[openAITurnStatePoolExtraKey] = pool
	b := turnStateAutoAccount()
	b.ID = 99
	b.Extra = map[string]any{openAITurnStateAutoExtraKey: true, openAITurnStatePoolExtraKey: pool}

	require.NotEqual(t,
		openAITurnStateSessionKey(turnStateAutoCtx("same-sess"), a, "same-sess"),
		openAITurnStateSessionKey(turnStateAutoCtx("same-sess"), b, "same-sess"),
		"不同账号的同名 session 必须是不同的键")

	// A 判定降智，B 的同名 session 不受影响
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("same-sess"), a,
		turnStateBlob(openAIDegradedTurnStateLen))
	require.Equal(t, healthy, mustResolve(t, svc, turnStateAutoCtx("same-sess"), a))
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("same-sess"), b),
		"A 的降智判定不得外溢到 B")
}

// TestOpenAITurnStateSessionIDAcceptsUnderscoreHeader 钉住两种会话头形态都认。
func TestOpenAITurnStateSessionIDAcceptsUnderscoreHeader(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("session_id", "下划线形态")
	require.Equal(t, "下划线形态", openAITurnStateRequestSessionID(c),
		"只发 session_id 的客户端不能静默失去这个功能")
}

// TestOpenAITurnStateFailureReadsFreshPool 钉住「不拿陈旧快照做读-改-写」：
// 请求手里的 *Account 是选号时刻的，并发请求可能已经把别的候选标失效了。
func TestOpenAITurnStateFailureReadsFreshPool(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	injected := turnStateBlob(openAIHealthyTurnStateLen)
	other := turnStateBlob(openAIHealthyTurnStateLen-1) + "z"
	minted := time.Now().UTC().Format(time.RFC3339)

	// 请求快照：两条候选都还健康
	stale := turnStateAutoAccount()
	stale.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": injected, "minted_at": minted},
		map[string]any{"model": turnStateTestModel, "blob": other, "minted_at": minted},
	}
	// DB 侧最新：并发请求已经把 other 标失效了
	fresh := turnStateAutoAccount()
	fresh.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": injected, "minted_at": minted},
		map[string]any{"model": turnStateTestModel, "blob": other, "minted_at": minted, "failed": true},
	}
	repo.latest = fresh

	c := turnStateAutoCtx("sess")
	svc.observeOpenAITurnStateMint(c, stale, turnStateBlob(openAIDegradedTurnStateLen))
	require.Equal(t, injected, mustResolve(t, svc, c, stale))
	// 注入的这条也被上游拒绝 → 两条都失效 → 必须停号
	svc.noteOpenAITurnStateRejected(c, stale)

	require.Equal(t, []bool{false}, repo.schedulable,
		"拿陈旧快照的话 other 的 Failed 位会被抹掉，永远判不出耗尽")
}

// TestOpenAITurnStateInjectionReadsFreshPool 钉住注入决策也读新鲜池。
//
// 请求手里的 *Account 来自调度器的 Redis 副本，而 openai_turn_state_pool 在
// schedulerNeutralExtraKeys 里——池写入刻意不触发快照重建，副本最多陈旧一整个
// full_rebuild_interval_seconds（默认 300s）。读快照的两个后果都不能忍：刚补进池的
// 新票要等下一轮 rebuild 才注得出去；已判 Failed 的候选在副本里仍 alive，会被反复
// 注入，而 record 侧从新鲜池里找不到它 → 耗尽判定和停号整段都走不到。
func TestOpenAITurnStateInjectionReadsFreshPool(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	minted := time.Now().UTC().Format(time.RFC3339)
	refilled := turnStateBlob(openAIHealthyTurnStateLen)

	// 请求快照：陈旧的 Redis 副本，池子是空的
	stale := turnStateAutoAccount()
	stale.Extra[openAITurnStatePoolExtraKey] = []any{}
	// DB 侧最新：别的请求刚补进来一条健康票
	fresh := turnStateAutoAccount()
	fresh.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": refilled, "minted_at": minted},
	}
	repo.latest = fresh

	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), stale,
		turnStateBlob(openAIDegradedTurnStateLen))

	require.Equal(t, refilled, mustResolve(t, svc, turnStateAutoCtx("sess"), stale),
		"读陈旧快照的话新补的票要等一轮 rebuild 才注得出去")
}

// TestOpenAITurnStateObservationDedupedPerContext 钉住 ctxKeyTurnStateObserved 这道闸：
// applyAttemptResponseHeaders 有两个调用点，幂等只靠 c.Writer.Written()，同一条 blob
// 会被观测两次。
//
// 判据取读库次数而不是写库次数：入池自己按 blob 去重、形态观测自己带节流，重复观测
// 最终都不会写错值——白付的是那几趟库操作，全在响应首字节之前。一次观测该有 3 次：
// 形态写入、入池前的 loadOpenAITurnStatePoolFresh、入池写入（后两者各自连带一次
// GetByID，共 3 条 SELECT）。闸门没了这里会变成 6，而那正是它存在的全部理由。
func TestOpenAITurnStateObservationDedupedPerContext(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateAutoAccount()
	blob := turnStateBlob(openAIHealthyTurnStateLen)

	c := turnStateAutoCtx("sess")
	svc.observeOpenAITurnStateMint(c, account, blob)
	svc.observeOpenAITurnStateMint(c, account, blob) // 第二个调用点

	require.Equal(t, 3, repo.getByIDCalls, "同一条 blob 在一个上下文里不该把读库付两遍")
	require.Len(t, readOpenAITurnStatePool(account), 1)
}

// TestOpenAITurnStateRejectionDedupedPerContext 钉住同一条 blob 不重复计失败：
// 非 WSv2 路径会在剥掉 encrypted reasoning items 后重试一次，同一次注入可能撞两回 400。
func TestOpenAITurnStateRejectionDedupedPerContext(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateAutoAccount()
	account.Extra[openAITurnStateFailThreshExtraKey] = 2
	minted := time.Now().UTC().Format(time.RFC3339)
	first := turnStateBlob(openAIHealthyTurnStateLen)
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": first, "minted_at": minted},
	}

	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateBlob(openAIDegradedTurnStateLen))

	c := turnStateAutoCtx("sess")
	require.Equal(t, first, mustResolve(t, svc, c, account))
	svc.noteOpenAITurnStateRejected(c, account)
	svc.noteOpenAITurnStateRejected(c, account) // 第二个调用点

	pool := readOpenAITurnStatePool(account)
	require.Len(t, pool, 1)
	require.Equal(t, 1, pool[0].FailStreak, "同一条 blob 在一个上下文里被拒两次只能记一次失败")
	require.False(t, pool[0].Failed, "阈值 2 时一次失败还不该判失效")
	require.Empty(t, repo.schedulable)
}

// TestOpenAITurnStateShapeTable 钉住正常形态表：individual 10 块/292，team 12 块/332。
//
// 降智一律是各自基线上多出恰好一块（11/312、13/356），两种形态各有各的基线，不能拿
// 一个阈值切。只认 individual 的话，team 号铸出来的每一条都会被判降智：自动接管会
// 一直注入、一直判失效，最后把号停掉，而那个号从头到尾都是正常的。
func TestOpenAITurnStateShapeTable(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name    string
		blocks  int
		healthy bool
	}{
		{"individual 正常", 10, true},
		{"individual 降智", 11, false},
		{"team 正常", 12, true},
		{"team 降智", 13, false},
	} {
		require.Equal(t, tc.healthy,
			openAITurnStateHealthy(turnStateFernetBlob(now, tc.blocks)), tc.name)
	}

	// 信封解不开时退回字符长度，同样要认两种形态。
	require.True(t, openAITurnStateHealthy(turnStateBlob(292)), "individual 长度兜底")
	require.True(t, openAITurnStateHealthy(turnStateBlob(332)), "team 长度兜底")
	require.False(t, openAITurnStateHealthy(turnStateBlob(312)), "312 是 individual 的降智值")
	require.False(t, openAITurnStateHealthy(turnStateBlob(356)), "356 是 team 的降智值")
}

// TestOpenAITurnStateUnknownShapeNeverEntersPool 钉住：2026-09-23 起上游铸出的 780 字符 / 33 块
// 判不了是否降智（前端标黄、账号页照常展示），但它不是 292/332，不能进候选池、不能被注入。
func TestOpenAITurnStateUnknownShapeNeverEntersPool(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateAutoAccount()

	unknown := turnStateFernetBlob(time.Now().UTC(), 33)
	require.Len(t, unknown, 780)
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account, unknown)
	require.Empty(t, readOpenAITurnStatePool(account), "780 不进池")

	// 对照：同一条路径上 292 照常入池，证明上面的空池不是路径没走通。
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account, turnStateFernetBlob(time.Now().UTC(), 10))
	require.Len(t, readOpenAITurnStatePool(account), 1)
}

// TestOpenAITurnStateObservedWithAutoDisabled 钉住：自动接管关着时，
// **记形态、不入池、不读库**。
//
// 三条缺一不可：
//   - 记形态：账号页上「没开接管」「开了但池空」「正在铸 312」长得一模一样的话，
//     运维没法判断该不该开接管——而这个决策恰恰只在铸 312 的时候才需要做。
//     2026-09-18 pro1-cpr 上游确实铸出过两条 292，就是被旧门禁整个丢掉的。
//   - 不入池：入池是同步读库+写库，在响应首字节之前、持着账号锁。而不带 turn-state
//     的请求 87.1% 会铸出新值，放开门禁等于给每个 Codex 账号的每条响应都加上
//     3 条 SELECT + 1 条 UPDATE，同一行 accounts 每请求一次 UPDATE。
//   - 不入池不代表不写库：观测本身要写一次，那是这条记录存在的代价，用节流兜住
//     （见 TestOpenAITurnStateObservationIsThrottled）。这里钉的是「除此之外没有别的
//     库操作」——stub 的 UpdateExtra 也会自增 getByIDCalls，模拟真实 repo 尾部那次
//     syncSchedulerAccountSnapshot 连带的 GetByID。
func TestOpenAITurnStateObservedWithAutoDisabled(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateAutoAccount()
	delete(account.Extra, openAITurnStateAutoExtraKey)
	require.False(t, account.IsOpenAITurnStateAutoEnabled(), "前提：自动接管是关的")

	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateFernetBlob(time.Now().UTC(), openAIHealthyTurnStateBlocks))

	observed, ok := readOpenAITurnStateObservation(account)
	require.True(t, ok, "接管关着也要记形态")
	require.Equal(t, 10, observed.Blocks)
	require.True(t, observed.Healthy)
	require.Equal(t, turnStateTestModel, observed.Model)

	require.Empty(t, readOpenAITurnStatePool(account), "接管关着不得入池")
	require.Equal(t, 1, repo.getByIDCalls, "接管关着时，一次观测只该带来那一次写库")

	// 312 同样要记 —— 这正是「该不该开接管」需要看到的那一半。旧实现只在 healthy 时
	// 采集，于是铸 312 的账号页面永远是空的，需求在它自己的动机场景里失效。
	svc.observeOpenAITurnStateMint(turnStateAutoCtxModel("sess2", "other-model"), account,
		turnStateFernetBlob(time.Now().UTC(), openAIHealthyTurnStateBlocks+1))
	observed, ok = readOpenAITurnStateObservation(account)
	require.True(t, ok)
	require.Equal(t, 11, observed.Blocks)
	require.False(t, observed.Healthy, "312 要记成不健康，但必须记")
	require.Equal(t, "other-model", observed.Model, "单条记录：新读数直接覆盖，不按模型建表")
	require.Empty(t, readOpenAITurnStatePool(account), "降智值照旧不入池")

	// 断言「不注入」之前要先把另外两个可能的原因排掉，否则它是双因空转：把 auto 门禁
	// 删掉照样绿。这里同时喂上 session 降智判定和一张可用票，让 auto 门禁成为唯一变量。
	// 放在最后做，因为往池里塞票会污染上面那几条「不得入池」的断言。
	injectCtx := turnStateAutoCtx("sess")
	key := openAITurnStateSessionKey(injectCtx, account, openAITurnStateRequestSessionID(injectCtx))
	require.NotEmpty(t, key)
	svc.setSessionTurnStateNeedsInjection(key, true)
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": turnStateBlob(openAIHealthyTurnStateLen),
			"minted_at": time.Now().UTC().Format(time.RFC3339)},
	}
	require.Empty(t, mustResolve(t, svc, injectCtx, account),
		"接管关着就不注入——哪怕 session 判过降智、池里也有可用的票")
}

// TestOpenAITurnStateObservationIsThrottled 钉住形态观测的节流。
//
// 不节流的话这条路就是每响应一次 UPDATE（不带 turn-state 的请求 87.1% 会铸新值）。
// 形态没变且上一条还在有效期内就不写；形态变了要写（那正是要看的事），上一条过期了
// 也要写（否则页面的到期倒计时会停在一个早该消失的值上）。
func TestOpenAITurnStateObservationIsThrottled(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateAutoAccount()
	delete(account.Extra, openAITurnStateAutoExtraKey)
	now := time.Now().UTC()

	countObservationWrites := func() int {
		n := 0
		for _, w := range repo.extraWrites {
			if _, ok := w[openAITurnStateObservedExtraKey]; ok {
				n++
			}
		}
		return n
	}

	// 同一形态连观测三次（每次都是不同的 blob，只是块数相同）：只该写一次。
	for i := 0; i < 3; i++ {
		svc.observeOpenAITurnStateMint(turnStateAutoCtx("s"), account,
			turnStateFernetBlob(now.Add(time.Duration(i)*time.Second), openAIHealthyTurnStateBlocks))
	}
	require.Equal(t, 1, countObservationWrites(), "形态没变、上一条没过期，不该反复写库")

	// 形态变了：必须写。
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("s"), account,
		turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	require.Equal(t, 2, countObservationWrites(), "形态变化是要看的事，必须写")

	// 节流窗口过去了：形态相同也要续写一条，页面上的「最近观测于」才不会一直停在
	// 一个很久以前的时刻。把上一条的 observed_at 改老来模拟。
	account.Extra[openAITurnStateObservedExtraKey] = map[string]any{
		"model": turnStateTestModel, "blocks": openAIHealthyTurnStateBlocks + 1,
		"chars": openAIDegradedTurnStateLen, "healthy": false,
		"minted_at":   now.Format(time.RFC3339),
		"observed_at": now.Add(-2 * openAITurnStateObserveInterval).Format(time.RFC3339),
	}
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("s"), account,
		turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	require.Equal(t, 3, countObservationWrites(), "过了节流窗口要续写")

	// 节流基准必须是「上次写库时刻」，不是 blob 的铸造时刻。观测到一条已经很老的 blob
	// 时，若拿 minted_at 判，写进去的还是那个老时间戳，下一条响应再判一次还是过期
	// —— 退化成每响应一次 UPDATE。这里连喂 4 条同形态的老 blob，只应有第一条写进去。
	before := countObservationWrites()
	stale := now.Add(-3 * time.Hour)
	for i := 0; i < 4; i++ {
		svc.observeOpenAITurnStateMint(turnStateAutoCtx("s"), account,
			turnStateFernetBlob(stale.Add(time.Duration(i)*time.Second), openAIHealthyTurnStateBlocks))
	}
	require.Equal(t, before+1, countObservationWrites(),
		"老 blob 连续回带时不得每响应写一次库")
}

// TestOpenAITurnStateObservationIgnoresEchoedInjection 钉住：注入生效时不记形态观测。
//
// 这个入口是「上游响应里带了这个头就调」，而带 turn-state 的请求只有 8.0% 会重铸、
// 另外 92% 上游原样回带。不设闸的话，接管一开记下来的就是我们自己注进去那张 292 的
// 回声：页面在账号仍然铸 312 的时候报绿，运维看绿关掉接管立刻又吃 312，而「正在铸
// 312」和「接管正在生效」被合并成同一种显示——恰好毁掉这条记录存在的理由。
func TestOpenAITurnStateObservationIgnoresEchoedInjection(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	account := turnStateAutoAccount()
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": healthy,
			"minted_at": time.Now().UTC().Format(time.RFC3339)},
	}

	// 先让这个 session 被判降智：一次自然铸造的 312，这条要记。
	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateFernetBlob(time.Now().UTC(), openAIHealthyTurnStateBlocks+1))
	observed, ok := readOpenAITurnStateObservation(account)
	require.True(t, ok)
	require.Equal(t, 11, observed.Blocks, "前提：自然铸造的 312 记下来了")

	c := turnStateAutoCtx("sess")
	require.Equal(t, healthy, mustResolve(t, svc, c, account), "前提：这张 292 正在注入")

	// 上游把注入的那张 292 原样回带。形态观测不得把它当成「这个号现在铸 292」。
	svc.observeOpenAITurnStateMint(c, account, healthy)

	observed, ok = readOpenAITurnStateObservation(account)
	require.True(t, ok)
	require.Equal(t, 11, observed.Blocks,
		"注入的回声不是这个号铸出来的——记下去页面就会在仍铸 312 时报绿")
	require.False(t, observed.Healthy)
}

// TestOpenAITurnStateInjectedTicketSurvivesDegradedMint 钉住：注入后上游仍铸出 312，
// 不得判这张票失效。
//
// 实测结论（memory turn-state-remint-rules，578 条现网样本）：新铸的块数由账号当时的
// 权重决定，与请求里带的那张票无关——「10 块 → 11 块」从未发生，而「注入有效期内的
// 292、上游仍铸 312」是常态。把后者当成「这张票坏了」，而 fail_threshold 默认又是 1，
// 于是每注入一次就烧掉一张票，池子几分钟见底，接着客户端的 312 原样裸奔出站。
// 2026-09-18 用户反馈「这个头才拿到半分钟呀，这么快就过期了吗」就是这条——不是过期，
// 是被误判失效。
//
// 票坏了的硬证据只有一个：上游回 invalid_encrypted_content，那条走
// noteOpenAITurnStateRejected，不在这里。
func TestOpenAITurnStateInjectedTicketSurvivesDegradedMint(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	account := turnStateAutoAccount()
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"model": turnStateTestModel, "blob": healthy,
			"minted_at": time.Now().UTC().Format(time.RFC3339)},
	}

	svc.observeOpenAITurnStateMint(turnStateAutoCtx("sess"), account,
		turnStateBlob(openAIDegradedTurnStateLen))
	c := turnStateAutoCtx("sess")
	require.Equal(t, healthy, mustResolve(t, svc, c, account), "前提：这张票正在注入")

	// 注入生效的这一轮，上游仍铸出 312 —— 账号权重低的读数，不是票的失效证据。
	svc.observeOpenAITurnStateMint(c, account, turnStateBlob(openAIDegradedTurnStateLen))

	pool := readOpenAITurnStatePool(account)
	var ticket *openAITurnStateCandidate
	for i := range pool {
		if pool[i].Blob == healthy {
			ticket = &pool[i]
		}
	}
	require.NotNil(t, ticket, "票必须还在池子里")
	require.False(t, ticket.Failed, "上游铸 312 不是这张票的失效证据")
	require.Zero(t, ticket.FailStreak, "更不该记失败计数")

	require.Equal(t, healthy, mustResolve(t, svc, turnStateAutoCtx("sess"), account),
		"票没坏就该继续用到自然过期")
}

// TestOpenAITurnStateShapeTableIsSelfConsistent 把形态表的两条性质变成被守住的不变量。
//
// 表里的 Blocks 与 Chars 现在只是两个并排写死的数,没人拦它们对不上:Chars 打错只会让
// 「信封解不开时退回字符长度」那条路静默失效,而新增一个形态时若它的降智值(blocks+1)
// 撞进正常集合,整张表就废了。两条断言成本近乎为零,加一个形态时会立刻报警。
//
// 算术关系:base64 长度 = 4*ceil((57 + 16*blocks)/3)。
func TestOpenAITurnStateShapeTableIsSelfConsistent(t *testing.T) {
	normal := make(map[int]bool, len(openAITurnStateShapes))
	for _, shape := range openAITurnStateShapes {
		raw := openAITurnStateFernetOverhead + openAITurnStateAESBlockBytes*shape.Blocks
		want := 4 * ((raw + 2) / 3)
		require.Equal(t, want, shape.Chars,
			"形态 %d 块的字符数对不上 base64 长度", shape.Blocks)
		normal[shape.Blocks] = true
	}
	for _, shape := range openAITurnStateShapes {
		require.False(t, normal[shape.Blocks+1],
			"形态 %d 块的降智值(%d 块)撞进了正常集合,整张表的判据就废了",
			shape.Blocks, shape.Blocks+1)
	}
	// 与前端 frontend/src/utils/turnState.ts 的 TURN_STATE_SHAPES 是两份拷贝,
	// 唯一的约束是注释里那句「改一边要改两边」。这里至少钉住本侧的自洽。
	require.Len(t, openAITurnStateShapes, 2, "改形态表时记得同步前端 TURN_STATE_SHAPES")
}

// TestCPRTurnStateObservedButNeverReplaced 钉住 2026-09-23 的取舍：cpr 走原样中继，
// 残留的手填覆写 / 自动接管配置一律不生效；但用量表「出站」列与形态观测照旧。
func TestCPRTurnStateObservedButNeverReplaced(t *testing.T) {
	repo := newTurnStateAutoRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	cpr := &Account{ID: 21, Platform: PlatformOpenAI, Type: AccountTypeCPR, Extra: map[string]any{
		openAITurnStateAutoExtraKey:     true,
		openAITurnStateOverrideExtraKey: map[string]any{turnStateTestModel: "残留的手填值"},
	}}
	require.False(t, cpr.IsOpenAITurnStateAutoEnabled())
	require.Empty(t, cpr.OpenAICodexTurnStateOverride(turnStateTestModel))

	c := turnStateAutoCtx("sess")
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, "客户端自带")
	svc.applyOpenAICodexTurnStateOverrideHeader(c, cpr, h)
	require.Equal(t, "客户端自带", h.Get(openAICodexTurnStateHeader))
	require.Empty(t, openAITurnStateInjectedFromContext(c))
	require.Empty(t, OpenAITurnStateUsageSource(c))
	require.Equal(t, "客户端自带", OpenAITurnStateUsageSent(c), "出站列照记")

	svc.observeOpenAITurnStateMint(c, cpr, turnStateFernetBlob(time.Now().UTC(), openAIHealthyTurnStateBlocks+1))
	observed, ok := readOpenAITurnStateObservation(cpr)
	require.True(t, ok, "形态观测照记")
	require.Equal(t, openAIHealthyTurnStateBlocks+1, observed.Blocks)
}
