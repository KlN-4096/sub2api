//go:build unit

package service

import (
	"context"
	"encoding/base64"
	"encoding/binary"
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
}

func (r *turnStateAutoRepo) GetByID(_ context.Context, _ int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.latest == nil {
		return nil, errors.New("not found")
	}
	return r.latest, nil
}

func newTurnStateAutoRepo() *turnStateAutoRepo {
	return &turnStateAutoRepo{accountRepoStub: &accountRepoStub{}}
}

func (r *turnStateAutoRepo) UpdateExtra(_ context.Context, _ int64, updates map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extraWrites = append(r.extraWrites, updates)
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

// openAIDegradedTurnStateLen 是实测的「降智」字符长度，只有测试用它造样本：
// 生产判据是密文块数（openai_codex_turn_state_envelope.go），不是长度。
const openAIDegradedTurnStateLen = 312

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
		Type:     AccountTypeCPR,
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

	// 没带 session-id 的请求也不注入（没有可判定的域）
	override, _ = svc.resolveOpenAITurnStateOverride(turnStateAutoCtx(""), account)
	require.Empty(t, override)
}

// TestOpenAITurnStateAutoBeatsManual 钉住「系统接管」：开了自动，手填值一个字节都不生效。
func TestOpenAITurnStateAutoBeatsManual(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStateOverrideExtraKey] = "手填的值"
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
	// 注入生效：上游改铸出另一条 292。它不入池——注入请求铸出的 blob 会把刚投进去
	// 的候选挤出定深池，失效判定还没攒够就没了。
	fresher := turnStateBlob(openAIHealthyTurnStateLen-1) + "z"
	require.Len(t, fresher, openAIHealthyTurnStateLen)
	svc.observeOpenAITurnStateMint(c, account, fresher)
	require.Len(t, readOpenAITurnStatePool(account), 1, "注入请求铸出的 blob 不入池")

	// 该 session 仍被判定为降智：若拿注入后的结果回写判定，下一轮就不注入、
	// 上游又铸回 312，两个状态来回跳。
	require.Equal(t, healthy,
		mustResolve(t, svc, turnStateAutoCtx("sess"), account), "注入成功不得清掉降智判定")
}

func mustResolve(t *testing.T, svc *OpenAIGatewayService, c *gin.Context, account *Account) string {
	t.Helper()
	override, _ := svc.resolveOpenAITurnStateOverride(c, account)
	return override
}

// TestOpenAITurnStateAutoDegradesThenDisables 钉住失效链路：
// 注入 292 回来仍是 312 → 该候选失效 → 降级到下一条 → 全部失效 → 停调度并写明原因。
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

	// 第 1 轮：注入 first，上游仍铸 312 → 默认阈值 1，first 立即失效
	c := turnStateAutoCtx("sess")
	require.Equal(t, first, mustResolve(t, svc, c, account))
	svc.observeOpenAITurnStateMint(c, account, turnStateBlob(openAIDegradedTurnStateLen))
	require.Empty(t, repo.schedulable, "还有候选就不该停账号")

	// 第 2 轮：降级到 second
	c = turnStateAutoCtx("sess")
	require.Equal(t, second, mustResolve(t, svc, c, account), "必须降级到下一条候选")
	svc.observeOpenAITurnStateMint(c, account, turnStateBlob(openAIDegradedTurnStateLen))

	require.Equal(t, []bool{false}, repo.schedulable, "候选耗尽必须停调度")
	require.Len(t, repo.errors, 1)
	require.Contains(t, repo.errors[0], "疑似降智")
	// 判据是密文块数，不是字符长度——原因里必须写清 10 块这个健康基线。
	require.Contains(t, repo.errors[0], "10 块")
	require.Contains(t, repo.errors[0], "疑似判据，不是确证")
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
	accountA.Extra[openAITurnStateOverrideExtraKey] = "manual-blob-a"
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
	svc.recordOpenAITurnStateOutcome(c, account, fresh, false)
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

	fresh := turnStateFernetBlob(now.Add(-5*time.Minute), openAIHealthyTurnStateBlocks)
	account.Extra[openAITurnStateOverrideExtraKey] = fresh
	require.Equal(t, fresh, account.OpenAICodexTurnStateOverride(), "未过期照常生效")

	expired := turnStateFernetBlob(now.Add(-2*time.Hour), openAIHealthyTurnStateBlocks)
	account.Extra[openAITurnStateOverrideExtraKey] = expired
	require.Empty(t, account.OpenAICodexTurnStateOverride(), "过期的手填值不得再注入")

	// 有效期跟随账号配置。
	account.Extra[openAITurnStateStaleMinExtraKey] = 180
	require.Equal(t, expired, account.OpenAICodexTurnStateOverride(), "有效期应跟随配置")
	delete(account.Extra, openAITurnStateStaleMinExtraKey)

	// 解不出信封的值按不过期处理，别因为解码失败静默关掉功能。
	account.Extra[openAITurnStateOverrideExtraKey] = turnStateBlob(openAIHealthyTurnStateLen)
	require.NotEmpty(t, account.OpenAICodexTurnStateOverride())

	// 两条出站路径都要挡住，不能只挡一条。
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	account.Extra[openAITurnStateOverrideExtraKey] = expired
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("s"), account), "HTTP 路径")
	require.Equal(t, "echoed",
		svc.applyOpenAICodexTurnStateOverrideWSManualOnly(turnStateAutoCtx("s"), account, "echoed"),
		"WS 路径：过期就当没配，保留客户端回带值")
}

// TestOpenAITurnStateColdStartSeed 钉住冷启动引子：
// 池子空时用引子换一条上游新铸的 292 入池，然后把引子消费掉，不再复用。
func TestOpenAITurnStateColdStartSeed(t *testing.T) {
	now := time.Now().UTC()
	seed := turnStateFernetBlob(now.Add(-2*time.Minute), openAIHealthyTurnStateBlocks)

	newAccount := func() *Account {
		a := turnStateAutoAccount()
		a.Extra[openAITurnStateSeedExtraKey] = seed
		return a
	}
	degrade := func(svc *OpenAIGatewayService, a *Account) {
		svc.observeOpenAITurnStateMint(turnStateAutoCtx("s"), a, turnStateBlob(openAIDegradedTurnStateLen))
	}

	t.Run("换回健康值就入池并消费引子", func(t *testing.T) {
		repo := newTurnStateAutoRepo()
		svc := &OpenAIGatewayService{accountRepo: repo}
		account := newAccount()
		degrade(svc, account)

		c := turnStateAutoCtx("s")
		override, source := svc.resolveOpenAITurnStateOverride(c, account)
		require.Equal(t, seed, override, "池子空时必须拿引子顶上")
		require.Equal(t, turnStateSourceSeed, source, "来源要与 auto 分开记")

		minted := turnStateFernetBlob(now, openAIHealthyTurnStateBlocks)
		svc.observeOpenAITurnStateMint(c, account, minted)

		pool := readOpenAITurnStatePool(account)
		require.Len(t, pool, 1, "引子换回来的健康值必须入池")
		require.Equal(t, minted, pool[0].Blob)
		require.Empty(t, account.openAITurnStateSeed(), "引子用完即消费")

		// 之后走自己铸的票，不再复用引子。
		next, source := svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("s"), account)
		require.Equal(t, minted, next)
		require.Equal(t, turnStateSourceAuto, source)
	})

	t.Run("换回降级值就丢掉，别拿它继续烧请求", func(t *testing.T) {
		svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
		account := newAccount()
		degrade(svc, account)

		c := turnStateAutoCtx("s")
		require.Equal(t, seed, mustResolve(t, svc, c, account))
		svc.observeOpenAITurnStateMint(c, account, turnStateBlob(openAIDegradedTurnStateLen))

		require.Empty(t, account.openAITurnStateSeed())
		require.Empty(t, readOpenAITurnStatePool(account), "降级值不得入池")
		require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("s"), account))
	})

	t.Run("撞 400 也丢掉", func(t *testing.T) {
		svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
		account := newAccount()
		degrade(svc, account)

		c := turnStateAutoCtx("s")
		require.Equal(t, seed, mustResolve(t, svc, c, account))
		svc.noteOpenAITurnStateRejected(c, account)
		require.Empty(t, account.openAITurnStateSeed())
	})

	t.Run("池里有可用候选时不动引子", func(t *testing.T) {
		svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
		account := newAccount()
		healthy := turnStateFernetBlob(now, openAIHealthyTurnStateBlocks)
		svc.observeOpenAITurnStateMint(turnStateAutoCtx("s"), account, healthy)
		degrade(svc, account)

		override, source := svc.resolveOpenAITurnStateOverride(turnStateAutoCtx("s"), account)
		require.Equal(t, healthy, override, "有候选就用候选")
		require.Equal(t, turnStateSourceAuto, source)
		require.Equal(t, seed, account.openAITurnStateSeed(), "引子留着备用")
	})

	t.Run("过期引子不注入", func(t *testing.T) {
		svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
		account := turnStateAutoAccount()
		account.Extra[openAITurnStateSeedExtraKey] = turnStateFernetBlob(now.Add(-2*time.Hour), openAIHealthyTurnStateBlocks)
		degrade(svc, account)
		require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("s"), account))
	})

	t.Run("名单外的模型不动引子", func(t *testing.T) {
		svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
		account := newAccount()
		account.Extra[openAITurnStateModelsExtraKey] = "gpt-6*"
		degrade(svc, account)
		require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("s"), account))
		require.Equal(t, seed, account.openAITurnStateSeed())
	})
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

// TestOpenAITurnStateModelAllowlist 钉住生效模型名单：逗号分隔、大小写不敏感、
// 结尾 * 前缀匹配；留空或识别不出模型时放行。
func TestOpenAITurnStateModelAllowlist(t *testing.T) {
	a := turnStateAutoAccount()
	require.True(t, a.openAITurnStateModelAllowed("anything"), "留空 = 不限模型")

	a.Extra[openAITurnStateModelsExtraKey] = " GPT-5.6-Luna , gpt-6* "
	require.True(t, a.openAITurnStateModelAllowed("gpt-5.6-luna"), "大小写不敏感")
	require.True(t, a.openAITurnStateModelAllowed("gpt-6-astra"), "结尾 * 前缀匹配")
	require.False(t, a.openAITurnStateModelAllowed("gpt-5.6-sol"), "名单外必须拦住")
	require.True(t, a.openAITurnStateModelAllowed(""), "识别不出模型时放行，别静默失效")

	// 名单拦住的模型连自动接管也不注入。
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	svc.observeOpenAITurnStateMint(turnStateAutoCtxModel("s", "gpt-5.6-sol"), a, healthy)
	svc.observeOpenAITurnStateMint(turnStateAutoCtxModel("s", "gpt-5.6-sol"), a,
		turnStateBlob(openAIDegradedTurnStateLen))
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtxModel("s", "gpt-5.6-sol"), a))
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
	require.NotEmpty(t, repo.extraWrites)
	for _, w := range repo.extraWrites {
		require.Contains(t, w, openAITurnStatePoolExtraKey, "只写候选池这一个键")
		require.Len(t, w, 1)
	}
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
	account.Extra[openAITurnStateOverrideExtraKey] = "手填的值"

	c := turnStateAutoCtx("sess")
	require.Equal(t, "手填的值", svc.applyOpenAICodexTurnStateOverrideWSManualOnly(c, account, "客户端自带"))
	require.Equal(t, turnStateSourceManual, OpenAITurnStateUsageSource(c))
	require.Equal(t, "手填的值", openAITurnStateInjectedFromContext(c),
		"帧填充据此判断是否强制覆盖客户端自带 blob")

	// 没配手填 = 功能不存在，一个标记都不留
	plain := turnStateAutoCtx("sess")
	bare := &Account{ID: 8, Platform: PlatformOpenAI, Type: AccountTypeCPR}
	require.Equal(t, "客户端自带", svc.applyOpenAICodexTurnStateOverrideWSManualOnly(plain, bare, "客户端自带"))
	require.Empty(t, OpenAITurnStateUsageSource(plain))
	require.Empty(t, openAITurnStateInjectedFromContext(plain))
}

// TestOpenAITurnStateInjectionMarkerClearedPerAttempt 钉住 failover：c 在整个重试循环里
// 是同一个，上一个账号的注入标记不清掉，会把失效判定记到换号后的账号头上。
func TestOpenAITurnStateInjectionMarkerClearedPerAttempt(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	first := &Account{
		ID: 11, Platform: PlatformOpenAI, Type: AccountTypeCPR,
		Extra: map[string]any{openAITurnStateOverrideExtraKey: "第一个账号的手填值"},
	}
	second := &Account{ID: 12, Platform: PlatformOpenAI, Type: AccountTypeCPR}

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

// TestOpenAITurnStateOutcomeReadsFreshPool 钉住「不拿陈旧快照做读-改-写」：
// 请求手里的 *Account 是选号时刻的，并发请求可能已经把别的候选标失效了。
func TestOpenAITurnStateOutcomeReadsFreshPool(t *testing.T) {
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
	// 注入的这条也回 312 → 两条都失效 → 必须停号
	svc.observeOpenAITurnStateMint(c, stale, turnStateBlob(openAIDegradedTurnStateLen)+"x")

	require.Equal(t, []bool{false}, repo.schedulable,
		"拿陈旧快照的话 other 的 Failed 位会被抹掉，永远判不出耗尽")
}

// TestOpenAITurnStateObservationDedupedPerContext 钉住同一条 blob 不重复计账：
// applyAttemptResponseHeaders 有两个调用点，幂等只靠 c.Writer.Written()。
func TestOpenAITurnStateObservationDedupedPerContext(t *testing.T) {
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
	degraded := turnStateBlob(openAIDegradedTurnStateLen)
	svc.observeOpenAITurnStateMint(c, account, degraded)
	svc.observeOpenAITurnStateMint(c, account, degraded) // 第二个调用点

	pool := readOpenAITurnStatePool(account)
	require.Len(t, pool, 1)
	require.Equal(t, 1, pool[0].FailStreak, "同一条 blob 观测两次只能记一次失败")
	require.False(t, pool[0].Failed, "阈值 2 时一次失败还不该判失效")
	require.Empty(t, repo.schedulable)
}
