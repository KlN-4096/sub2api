//go:build unit

package service

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// turnStateTestRepo 只实现形态观测与网关池账本测试真正会调到的方法，其余继承
// accountRepoStub 的 panic 实现——多调一个方法就会当场炸出来。
type turnStateTestRepo struct {
	*accountRepoStub

	mu          sync.Mutex
	latest      *Account
	extraWrites []map[string]any
	// block 非空时 UpdateExtra 先等它关闭：模拟卡住的库。
	block chan struct{}
}

func (r *turnStateTestRepo) GetByID(_ context.Context, _ int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.latest == nil {
		return nil, errors.New("not found")
	}
	return r.latest, nil
}

func newTurnStateTestRepo() *turnStateTestRepo {
	return &turnStateTestRepo{accountRepoStub: &accountRepoStub{}}
}

func (r *turnStateTestRepo) UpdateExtra(_ context.Context, _ int64, updates map[string]any) error {
	if r.block != nil {
		<-r.block
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extraWrites = append(r.extraWrites, updates)
	return nil
}

// waitObservationWrites 等后台落库到 want 条，再确认短时间内不会多出来——多出来说明节流
// 失效。是否落库在调用点同步决定，goroutine 只负责写，所以先等后看是确定的。
func (r *turnStateTestRepo) waitObservationWrites(t *testing.T, want int) {
	t.Helper()
	count := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()
		n := 0
		for _, w := range r.extraWrites {
			if _, ok := w[openAITurnStateObservedExtraKey]; ok {
				n++
			}
		}
		return n
	}
	require.Eventually(t, func() bool { return count() >= want }, 2*time.Second, time.Millisecond)
	require.Never(t, func() bool { return count() > want }, 30*time.Millisecond, time.Millisecond,
		"形态观测写库次数超过 %d", want)
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

func turnStateTestCtx(sessionID string) *gin.Context {
	return turnStateTestCtxModel(sessionID, turnStateTestModel)
}

// turnStateTestCtxModel 造一个带模型归属的请求上下文。出站各路径都会在分发前
// SetOpsUpstreamModel，形态观测从那里读本次模型。
func turnStateTestCtxModel(sessionID, model string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	if sessionID != "" {
		c.Request.Header.Set("Session-Id", sessionID)
	}
	SetOpsUpstreamModel(c, model)
	return c
}

func turnStateTestAccount() *Account {
	return &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{}}
}

// healthy/degraded 只用长度说话——信封解不开时判定只看字符数。
func turnStateBlob(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(i%26)
	}
	return string(b)
}

// turnStateFernetBlob 造一条真 Fernet 信封：0x80 | 8B 大端铸造戳 | 16B IV |
// blocks×16B 密文 | 32B HMAC。turnStateBlob 造的是纯长度样本，解不出信封。
func turnStateFernetBlob(minted time.Time, blocks int) string {
	raw := make([]byte, 1+8+16+blocks*16+32)
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(minted.Unix()))
	return base64.URLEncoding.EncodeToString(raw)
}

// openAITurnStateHealthy 按 blob 直接问健康判定。生产在 noteOpenAITurnStateObservation 里一次解析
// 同时取块数、铸造时刻与判定（openAITurnStateShape），只有测试需要这个单项形态，留在生产侧就是 unused。
func openAITurnStateHealthy(blob string) bool {
	_, _, healthy := openAITurnStateShape(blob)
	return healthy
}

// TestOpenAITurnStateHealthyLengthMatchesRealSample 钉住「292 = 不降智」这个口径的真实样本。
func TestOpenAITurnStateHealthyLengthMatchesRealSample(t *testing.T) {
	// 真实捕获样本（pro3），运维口径「292 = 不降智」的由来
	const realHealthy = "gAAAAABqqrNHYSOlO_EUJI-hlduVBqJ8slR-floDb7J-ZYvvLXj7WV7dOZ_zk10RDMl_N4dRvG0UqxWR19XdSGbeHFUEAzwv7yQBADQrB1QhpOKkfcUPeSy2qsvZIvq__OHHoF2yCZfSTPq6YvkKahwLUxkeORhQZ9Ug86sMJwkrJXUefsa6fTpRqzZSN7SLphKU-6Ys6FV3GveSXjgk0UcCaKvfShFj4_EmGriyCb-JVoU0D8LJbjsClcivKgDNu1jfZfF-6q8VXHGF1Uck7vDVXdiNh1kRXw=="
	require.Len(t, realHealthy, openAIHealthyTurnStateLen, "健康长度必须与真实样本一致")
	require.Equal(t, 292, openAIHealthyTurnStateLen)
	require.Equal(t, 312, openAIDegradedTurnStateLen)
	require.True(t, openAITurnStateHealthy(realHealthy))
}

// TestOpenAITurnStateShapeTable 钉住正常形态表：individual 10 块/292，team 12 块/332。
//
// 降智一律是各自基线上多出恰好一块（11/312、13/356），两种形态各有各的基线，不能拿
// 一个阈值切。只认 individual 的话，team 号铸出来的每一条都会被判降智，而那个号从头
// 到尾都是正常的。
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

// TestOpenAITurnStateObservedForEveryCodexAccount 钉住：每个 Codex 账号都记形态，健康与否
// 都记；读数同步写回请求手里的账号快照，落库在后台、只有那一次 UpdateExtra（stub 的
// 其余方法会 panic）。写频由节流兜住（见 TestOpenAITurnStateObservationIsThrottled）。
func TestOpenAITurnStateObservedForEveryCodexAccount(t *testing.T) {
	repo := newTurnStateTestRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateTestAccount()

	svc.observeOpenAITurnStateMint(turnStateTestCtx("sess"), account,
		turnStateFernetBlob(time.Now().UTC(), openAIHealthyTurnStateBlocks))

	observed, ok := readOpenAITurnStateObservation(account)
	require.True(t, ok)
	require.Equal(t, 10, observed.Blocks)
	require.True(t, observed.Healthy)
	require.Equal(t, turnStateTestModel, observed.Model)
	repo.waitObservationWrites(t, 1)

	// 312 同样要记：旧实现只在 healthy 时采集，于是铸 312 的账号页面永远是空的。
	svc.observeOpenAITurnStateMint(turnStateTestCtxModel("sess2", "other-model"), account,
		turnStateFernetBlob(time.Now().UTC(), openAIHealthyTurnStateBlocks+1))
	observed, ok = readOpenAITurnStateObservation(account)
	require.True(t, ok)
	require.Equal(t, 11, observed.Blocks)
	require.False(t, observed.Healthy, "312 要记成不健康，但必须记")
	require.Equal(t, "other-model", observed.Model, "单条记录：新读数直接覆盖，不按模型建表")
	repo.waitObservationWrites(t, 2)
}

// TestOpenAITurnStateObservationWritesInBackground 钉住落库不在响应路径上：调用点在响应
// 首字节之前，库卡住时观测也要立刻返回，读数照样先写回请求手里的账号快照。
func TestOpenAITurnStateObservationWritesInBackground(t *testing.T) {
	repo := newTurnStateTestRepo()
	repo.block = make(chan struct{})
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateTestAccount()

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.observeOpenAITurnStateMint(turnStateTestCtx("s"), account,
			turnStateFernetBlob(time.Now().UTC(), openAIHealthyTurnStateBlocks))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("库卡住时形态观测挡住了响应")
	}
	_, ok := readOpenAITurnStateObservation(account)
	require.True(t, ok)

	close(repo.block)
	repo.waitObservationWrites(t, 1)
}

// TestOpenAITurnStateObservationIsThrottled 钉住形态观测的节流。
//
// 不节流的话这条路就是每响应一次 UPDATE（不带 turn-state 的请求 87.1% 会铸新值）。
// 形态没变且上一条还在有效期内就不写；形态变了要写（那正是要看的事），上一条过期了
// 也要写（否则页面的「最近观测于」会停在一个早该刷新的值上）。同一条 blob 在一次响应里
// 可能被送两次，也只该写一次。
func TestOpenAITurnStateObservationIsThrottled(t *testing.T) {
	repo := newTurnStateTestRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := turnStateTestAccount()
	now := time.Now().UTC()

	// 同一条 blob 送两次（两个调用点）：只该写一次。
	c := turnStateTestCtx("s")
	blob := turnStateFernetBlob(now, openAIHealthyTurnStateBlocks)
	svc.observeOpenAITurnStateMint(c, account, blob)
	svc.observeOpenAITurnStateMint(c, account, blob)
	repo.waitObservationWrites(t, 1)

	// 同一形态再来两条（不同的 blob，块数相同）：仍然只有那一次。
	for i := 1; i < 3; i++ {
		svc.observeOpenAITurnStateMint(turnStateTestCtx("s"), account,
			turnStateFernetBlob(now.Add(time.Duration(i)*time.Second), openAIHealthyTurnStateBlocks))
	}
	repo.waitObservationWrites(t, 1)

	// 形态变了：必须写。
	svc.observeOpenAITurnStateMint(turnStateTestCtx("s"), account,
		turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	repo.waitObservationWrites(t, 2)

	// 节流窗口过去了：形态相同也要续写一条。把上一条的 observed_at 改老来模拟。
	account.Extra[openAITurnStateObservedExtraKey] = map[string]any{
		"model": turnStateTestModel, "blocks": openAIHealthyTurnStateBlocks + 1,
		"chars": openAIDegradedTurnStateLen, "healthy": false,
		"minted_at":   now.Format(time.RFC3339),
		"observed_at": now.Add(-2 * openAITurnStateObserveInterval).Format(time.RFC3339),
	}
	svc.observeOpenAITurnStateMint(turnStateTestCtx("s"), account,
		turnStateFernetBlob(now, openAIHealthyTurnStateBlocks+1))
	repo.waitObservationWrites(t, 3)

	// 节流基准必须是「上次写库时刻」，不是 blob 的铸造时刻。观测到一条已经很老的 blob
	// 时，若拿 minted_at 判，写进去的还是那个老时间戳，下一条响应再判一次还是过期
	// —— 退化成每响应一次 UPDATE。这里连喂 4 条同形态的老 blob，只应有第一条写进去。
	stale := now.Add(-3 * time.Hour)
	for i := 0; i < 4; i++ {
		svc.observeOpenAITurnStateMint(turnStateTestCtx("s"), account,
			turnStateFernetBlob(stale.Add(time.Duration(i)*time.Second), openAIHealthyTurnStateBlocks))
	}
	repo.waitObservationWrites(t, 4)
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

// TestCPRTurnStateSentAndObserved 钉住 cpr 账号（原样中继）的用量表「出站」列与形态观测照记。
func TestCPRTurnStateSentAndObserved(t *testing.T) {
	repo := newTurnStateTestRepo()
	svc := &OpenAIGatewayService{accountRepo: repo}
	cpr := &Account{ID: 21, Platform: PlatformOpenAI, Type: AccountTypeCPR, Extra: map[string]any{}}

	c := turnStateTestCtx("sess")
	markOpenAITurnStateSent(c, cpr, " 客户端自带 ")
	require.Equal(t, "客户端自带", OpenAITurnStateUsageSent(c), "出站列照记")

	svc.observeOpenAITurnStateMint(c, cpr, turnStateFernetBlob(time.Now().UTC(), openAIHealthyTurnStateBlocks+1))
	observed, ok := readOpenAITurnStateObservation(cpr)
	require.True(t, ok, "形态观测照记")
	require.Equal(t, openAIHealthyTurnStateBlocks+1, observed.Blocks)
	repo.waitObservationWrites(t, 1)
}

// TestExtraJSONRecordReadsAreStrict 钉住 extra 上两条 JSON 记录的严格读：账号为 nil、缺键、
// 值为 null、形状对不上都按「没有」处理，并且返回零值而不是解了一半的记录。
func TestExtraJSONRecordReadsAreStrict(t *testing.T) {
	_, ok := readOpenAITurnStateObservation(nil)
	require.False(t, ok)
	_, ok = readOpenAIGatewayHistory(nil)
	require.False(t, ok)

	account := &Account{Extra: map[string]any{openAITurnStateObservedExtraKey: nil}}
	_, ok = readOpenAITurnStateObservation(account)
	require.False(t, ok, "值为 null")
	_, ok = readOpenAIGatewayHistory(account)
	require.False(t, ok, "缺键")

	// 形状对不上时 json.Unmarshal 仍会填上能解的字段再报错，读侧必须整条丢掉。
	account.Extra[openAITurnStateObservedExtraKey] = map[string]any{"model": "gpt-5", "blocks": "ten"}
	observed, ok := readOpenAITurnStateObservation(account)
	require.False(t, ok)
	require.Zero(t, observed)
	account.Extra[openAIGatewayHistoryExtraKey] = map[string]any{
		"current": "unified-1",
		"seen":    map[string]any{"unified-1": "2026-10-01T00:00:00Z"}, // 2026-10-02 之前的老形状
	}
	history, ok := readOpenAIGatewayHistory(account)
	require.False(t, ok)
	require.Zero(t, history)

	account.Extra[openAITurnStateObservedExtraKey] = map[string]any{"model": "gpt-5", "blocks": 10, "chars": 292}
	observed, ok = readOpenAITurnStateObservation(account)
	require.True(t, ok)
	require.Equal(t, openAITurnStateObservation{Model: "gpt-5", Blocks: 10, Chars: 292}, observed)
}
