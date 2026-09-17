//go:build unit

package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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

func turnStateAutoCtx(sessionID string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	if sessionID != "" {
		c.Request.Header.Set("Session-Id", sessionID)
	}
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

// TestPickOpenAITurnStateCandidate 钉住候选选取：跳过失效、过保鲜期仍可用但换来源。
func TestPickOpenAITurnStateCandidate(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	stale := time.Hour

	_, _, ok := pickOpenAITurnStateCandidate(nil, stale, now)
	require.False(t, ok, "空池不注入")

	pool := []openAITurnStateCandidate{
		{Blob: "failed", MintedAt: now.Add(-time.Minute), Failed: true},
		{Blob: "fresh", MintedAt: now.Add(-time.Minute)},
		{Blob: "old", MintedAt: now.Add(-3 * time.Hour)},
	}
	got, source, ok := pickOpenAITurnStateCandidate(pool, stale, now)
	require.True(t, ok)
	require.Equal(t, "fresh", got.Blob, "失效候选必须跳过")
	require.Equal(t, turnStateSourceAuto, source)

	// 过保鲜期照用不删：292 太稀缺，只是把来源标成 auto_stale 以便事后统计
	onlyOld := []openAITurnStateCandidate{{Blob: "old", MintedAt: now.Add(-3 * time.Hour)}}
	got, source, ok = pickOpenAITurnStateCandidate(onlyOld, stale, now)
	require.True(t, ok, "过期候选仍然可用")
	require.Equal(t, "old", got.Blob)
	require.Equal(t, turnStateSourceAutoStale, source)

	allFailed := []openAITurnStateCandidate{{Blob: "a", Failed: true}, {Blob: "", MintedAt: now}}
	_, _, ok = pickOpenAITurnStateCandidate(allFailed, stale, now)
	require.False(t, ok, "全失效（空 blob 也算不可用）= 耗尽")
}

// TestOpenAITurnStateAutoOnlyInjectsDegradedSessions 钉住方案 B：
// 只有被判定落在 312 的 session 才注入，其余保持真客户端形态。
func TestOpenAITurnStateAutoOnlyInjectsDegradedSessions(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: newTurnStateAutoRepo()}
	healthy := turnStateBlob(openAIHealthyTurnStateLen)
	account := turnStateAutoAccount()
	account.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
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
		map[string]any{"blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
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
		map[string]any{"blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
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
		map[string]any{"blob": first, "minted_at": minted},
		map[string]any{"blob": second, "minted_at": minted},
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
	require.Contains(t, repo.errors[0], "312")
	require.False(t, account.Schedulable)

	// 停掉之后不再注入（池里已无可用候选）
	require.Empty(t, mustResolve(t, svc, turnStateAutoCtx("sess"), account))
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
		map[string]any{"blob": healthy, "minted_at": time.Now().UTC().Format(time.RFC3339)},
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
	pool := []any{map[string]any{"blob": healthy, "minted_at": minted}}

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
		map[string]any{"blob": injected, "minted_at": minted},
		map[string]any{"blob": other, "minted_at": minted},
	}
	// DB 侧最新：并发请求已经把 other 标失效了
	fresh := turnStateAutoAccount()
	fresh.Extra[openAITurnStatePoolExtraKey] = []any{
		map[string]any{"blob": injected, "minted_at": minted},
		map[string]any{"blob": other, "minted_at": minted, "failed": true},
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
		map[string]any{"blob": first, "minted_at": minted},
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
