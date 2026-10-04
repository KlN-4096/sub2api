package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

const (
	gatewayPoolCooldownCeiling = time.Duration(gwpool.CooldownMaxSeconds) * time.Second
	// 调度/取票有毫秒级延迟，不能因此把准时的 1 小时观察归到 2 小时档。
	gatewayPoolCooldownGrace   = time.Minute
	gatewayPoolCooldownLocks   = 2
	gatewayPoolMaxObservedIdle = 7 * 24 * time.Hour
)

// gatewayPoolCooldown 持久化在已有的 seen[gateway] 中。Until 是本地下一次允许试用的时刻，
// 不是上游保证恢复的时刻；成功样本只在不同尝试对应的独立冷却周期里计一次。
type gatewayPoolCooldown struct {
	WindowSeconds        int         `json:"window_seconds"`
	FixedSeconds         int         `json:"fixed_seconds,omitempty"`
	Until                time.Time   `json:"until,omitzero"`
	UpdatedAt            time.Time   `json:"updated_at,omitzero"`
	AttemptAt            time.Time   `json:"attempt_at,omitzero"`
	AttemptSeconds       int         `json:"attempt_seconds,omitempty"`
	ElapsedSeconds       int         `json:"elapsed_seconds,omitempty"`
	Outcome              string      `json:"outcome,omitempty"`
	Successes            map[int]int `json:"successes,omitempty"`
	LastSuccessSeconds   int         `json:"last_success_seconds,omitempty"`
	LastSuccessAt        time.Time   `json:"last_success_at,omitzero"`
	RecommendedSeconds   int         `json:"recommended_seconds,omitempty"`
	RecommendationSource string      `json:"recommendation_source,omitempty"`
}

type gatewayPoolCooldownSample struct {
	Gateway        string
	AttemptAt      time.Time
	WindowSeconds  int
	ElapsedSeconds int
	SuccessSeconds int
	Full           bool
}

func gatewayPoolCooldownBase(window time.Duration) int {
	if window < openAIGatewayPoolGatewayWindow || window > gatewayPoolCooldownCeiling {
		window = openAIGatewayPoolGatewayWindow
	}
	return int(window.Seconds())
}

func nextGatewayPoolCooldown(seconds int) int {
	return gwpool.NextCooldownSeconds(seconds)
}

func successfulGatewayPoolCooldown(planned, elapsed int) int {
	if planned > 0 && elapsed >= planned-int(gatewayPoolCooldownGrace.Seconds()) &&
		elapsed <= planned+int(gatewayPoolCooldownGrace.Seconds()) {
		return planned
	}
	return 0 // 稀疏流量只记录实际静置时间，不能冒充某个未实际等待验证的档位。
}

func (c gatewayPoolCooldown) clone() gatewayPoolCooldown {
	out := c
	if c.Successes != nil {
		out.Successes = make(map[int]int, len(c.Successes))
		for seconds, n := range c.Successes {
			out.Successes[seconds] = n
		}
	}
	return out
}

func (c *gatewayPoolCooldown) resetWindow(base int) {
	c.WindowSeconds = base
	if c.FixedSeconds > 0 {
		c.WindowSeconds = c.FixedSeconds
	}
}

// begin 在真正取出一张新票时调用。旧的 Until 已到才允许开始，重复/并发取票不会穿过冷却。
func (c *gatewayPoolCooldown) begin(now, usedAt time.Time, base int) bool {
	if c.WindowSeconds < int(openAIGatewayPoolGatewayWindow.Seconds()) || c.WindowSeconds > int(gatewayPoolCooldownCeiling.Seconds()) {
		if c.FixedSeconds < int(openAIGatewayPoolGatewayWindow.Seconds()) {
			c.FixedSeconds = 0
		}
		c.resetWindow(base)
	}
	until := c.Until
	if !usedAt.IsZero() {
		if touchedUntil := usedAt.Add(time.Duration(c.WindowSeconds) * time.Second); touchedUntil.After(until) {
			until = touchedUntil
		}
	}
	if now.Before(until) {
		return false
	}
	c.AttemptSeconds, c.ElapsedSeconds = 0, 0
	if !until.IsZero() {
		c.AttemptSeconds = c.WindowSeconds
		start := until.Add(-time.Duration(c.WindowSeconds) * time.Second)
		c.ElapsedSeconds = int(math.Ceil(now.Sub(start).Seconds()))
		if c.ElapsedSeconds > int(gatewayPoolMaxObservedIdle.Seconds()) {
			c.AttemptSeconds, c.ElapsedSeconds = 0, 0
		}
	}
	// 上面计分的是已完成的旧窗口；推荐只安排下一窗口，不能冒充已等待的时长。
	// 失败后的本地退避不能被较短推荐降档，已学到的固定值仍优先。
	if c.FixedSeconds > 0 {
		c.WindowSeconds = c.FixedSeconds
	} else if c.Outcome == openAIGatewayVerdictFull {
		c.WindowSeconds = base
	} else {
		c.WindowSeconds = max(c.WindowSeconds, base)
	}
	c.AttemptAt, c.UpdatedAt, c.Outcome = now, now, ""
	c.Until = now.Add(time.Duration(c.WindowSeconds) * time.Second)
	return true
}

// observe 不接收网络错误/未知读数。一次已确认满血之后正常耗尽，开始新周期而不是升级；
// 只有真正等完了上一档、再次尝试仍明确降级，才升级并解除固定。
func (c *gatewayPoolCooldown) observe(now time.Time, verdict string, base int) (*gatewayPoolCooldownSample, bool) {
	if verdict != openAIGatewayVerdictFull && verdict != openAIGatewayVerdictDegraded {
		return nil, false
	}
	if c.Outcome == verdict {
		return nil, false
	}
	if c.WindowSeconds <= 0 {
		c.resetWindow(base)
	}
	var sample *gatewayPoolCooldownSample
	if c.AttemptSeconds > 0 && c.Outcome == "" {
		sample = &gatewayPoolCooldownSample{
			AttemptAt: c.AttemptAt, WindowSeconds: c.AttemptSeconds,
			ElapsedSeconds: c.ElapsedSeconds, Full: verdict == openAIGatewayVerdictFull,
		}
	}
	if verdict == openAIGatewayVerdictFull {
		if sample != nil {
			success := successfulGatewayPoolCooldown(c.AttemptSeconds, c.ElapsedSeconds)
			sample.SuccessSeconds = success
			c.LastSuccessAt, c.LastSuccessSeconds = now, c.ElapsedSeconds
			if success > 0 {
				if c.Successes == nil {
					c.Successes = map[int]int{}
				}
				if c.Successes[success] < gatewayPoolCooldownLocks {
					c.Successes[success]++
				}
				if c.Successes[success] >= gatewayPoolCooldownLocks {
					c.FixedSeconds = success
				}
			}
		}
		c.resetWindow(base)
		// 这个窗口仍可被缓存复用；下一张新票才受 Until 约束。
		c.Until = now.Add(time.Duration(c.WindowSeconds) * time.Second)
	} else {
		if sample != nil {
			delete(c.Successes, c.AttemptSeconds)
			c.FixedSeconds = 0
			c.WindowSeconds = min(int(gatewayPoolCooldownCeiling.Seconds()),
				max(nextGatewayPoolCooldown(c.AttemptSeconds), max(base, c.WindowSeconds)))
		} else {
			c.resetWindow(base)
		}
		c.Until = now.Add(time.Duration(c.WindowSeconds) * time.Second)
	}
	c.Outcome, c.UpdatedAt = verdict, now
	return sample, true
}

func (s *openAICodexCookieStore) cooldownEntry(identity, gateway string) (gatewayPoolCooldown, bool) {
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	c, ok := s.poolCooldown[gatewayPoolLedgerKey(identity, gateway)]
	return c.clone(), ok
}

func validGatewayPoolCooldown(c *gatewayPoolCooldown, now time.Time, base int) bool {
	validStep := func(seconds int) bool {
		for step := base; step > 0 && step <= int(gatewayPoolCooldownCeiling.Seconds()); {
			if step == seconds {
				return true
			}
			next := nextGatewayPoolCooldown(step)
			if step == next {
				break
			}
			step = next
		}
		// 账号改过初始值时，原先标准档的学习仍然合法。
		return gwpool.IsCooldownStep(seconds)
	}
	if c == nil || c.UpdatedAt.IsZero() || c.UpdatedAt.After(now.Add(gatewayPoolCooldownGrace)) ||
		!validStep(c.WindowSeconds) || c.Until.After(c.UpdatedAt.Add(time.Duration(c.WindowSeconds)*time.Second+gatewayPoolCooldownGrace)) ||
		c.Until.After(now.Add(gatewayPoolCooldownCeiling+gatewayPoolCooldownGrace)) ||
		c.AttemptAt.After(now.Add(gatewayPoolCooldownGrace)) ||
		c.AttemptAt.After(c.UpdatedAt.Add(gatewayPoolCooldownGrace)) ||
		c.LastSuccessAt.After(now.Add(gatewayPoolCooldownGrace)) ||
		(c.FixedSeconds != 0 && !validStep(c.FixedSeconds)) ||
		(c.AttemptSeconds != 0 && (!validStep(c.AttemptSeconds) || c.AttemptAt.IsZero())) ||
		c.ElapsedSeconds < 0 || c.LastSuccessSeconds < 0 ||
		c.ElapsedSeconds > int(gatewayPoolMaxObservedIdle.Seconds()) ||
		c.LastSuccessSeconds > int(gatewayPoolMaxObservedIdle.Seconds()) ||
		(c.AttemptSeconds > 0 && c.ElapsedSeconds < c.AttemptSeconds-int(gatewayPoolCooldownGrace.Seconds())) ||
		(c.Outcome == "" && !c.AttemptAt.IsZero() &&
			c.UpdatedAt.Sub(c.AttemptAt) > gatewayPoolCooldownGrace) ||
		len(c.Successes) > 32 ||
		(c.RecommendedSeconds != 0 && (!validStep(c.RecommendedSeconds) ||
			(c.RecommendationSource != "account" && c.RecommendationSource != "pool"))) ||
		(c.Outcome != "" && c.Outcome != openAIGatewayVerdictFull && c.Outcome != openAIGatewayVerdictDegraded) {
		return false
	}
	for step, count := range c.Successes {
		if !validStep(step) || count < 1 || count > gatewayPoolCooldownLocks {
			return false
		}
	}
	return c.FixedSeconds == 0 || c.Successes[c.FixedSeconds] == gatewayPoolCooldownLocks
}

func (s *openAICodexCookieStore) hydrateCooldown(identity, gateway string, c *gatewayPoolCooldown, base int) {
	if !validGatewayPoolCooldown(c, time.Now().UTC(), base) {
		return
	}
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	key := gatewayPoolLedgerKey(identity, gateway)
	if prev, ok := s.poolCooldown[key]; ok && !c.UpdatedAt.After(prev.UpdatedAt) {
		return
	}
	if s.poolCooldown == nil {
		s.poolCooldown = map[string]gatewayPoolCooldown{}
	}
	restored := c.clone()
	// 未完成的尝试属于写入它的请求/进程。恢复冷却，不恢复可供下一次响应计分的在途样本。
	if restored.Outcome == "" {
		restored.AttemptAt = time.Time{}
		restored.AttemptSeconds, restored.ElapsedSeconds = 0, 0
	}
	s.poolCooldown[key] = restored
}

const openAIGatewayLedgerTagExtraKey = "openai_gwpool_ledger_tag"

func gatewayPoolLedgerTag(identity string) string {
	sum := sha256.Sum256([]byte("gwpool-ledger\x00" + gatewayPoolLedgerIdentity(identity)))
	return hex.EncodeToString(sum[:])
}

// 每次取新票前合并所有账号行里带相同绑定标签的新鲜历史。不能永久缓存“已加载”，
// 否则另一个实例写入其他克隆行后，本实例永远看不到。缓存中的活票不经过这里。
func (s *openAICodexCookieStore) hydrateGatewayPoolSharedHistory(ctx context.Context, account *Account, identity string) error {
	fresh, err := s.freshGatewayPoolAccount(ctx, account)
	if err != nil || fresh == nil {
		return fmt.Errorf("unable to read current gateway cooldown")
	}
	s.gatewayPoolHydrateUsed(fresh, identity)
	if s.historyByTag == nil {
		return nil
	}
	tag := gatewayPoolLedgerTag(identity)
	_, err, _ = s.poolHistoryLoad.Do(tag, func() (any, error) {
		accounts, err := s.historyByTag(ctx, tag)
		if err != nil {
			return nil, err
		}
		for i := range accounts {
			s.gatewayPoolHydrateUsed(&accounts[i], identity)
		}
		return nil, nil
	})
	return err
}

func (s *openAICodexCookieStore) beginGatewayPoolAttempt(identity, gateway string, window time.Duration) bool {
	if identity == "" || gateway == "" {
		return true
	}
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	key := gatewayPoolLedgerKey(identity, gateway)
	c := s.poolCooldown[key]
	var usedAt time.Time
	if used, ok := s.poolUsed.Load(key); ok {
		usedAt, _ = used.(time.Time)
	}
	base, recommendation := s.gatewayPoolInitialCooldown(identity, gateway, window)
	if !c.begin(time.Now().UTC(), usedAt, base) {
		return false
	}
	setGatewayPoolRecommendation(&c, recommendation)
	if s.poolCooldown == nil {
		s.poolCooldown = map[string]gatewayPoolCooldown{}
	}
	s.poolCooldown[key] = c
	return true
}

func (s *openAICodexCookieStore) observeGatewayPoolCooldown(identity, gateway, verdict string, window time.Duration) *gatewayPoolCooldownSample {
	if identity == "" || gateway == "" {
		return nil
	}
	s.poolCooldownMu.Lock()
	defer s.poolCooldownMu.Unlock()
	key := gatewayPoolLedgerKey(identity, gateway)
	c := s.poolCooldown[key]
	base, recommendation := s.gatewayPoolInitialCooldown(identity, gateway, window)
	sample, changed := c.observe(time.Now().UTC(), verdict, base)
	if changed {
		setGatewayPoolRecommendation(&c, recommendation)
		if s.poolCooldown == nil {
			s.poolCooldown = map[string]gatewayPoolCooldown{}
		}
		s.poolCooldown[key] = c
	}
	if sample != nil {
		sample.Gateway = gateway
	}
	return sample
}

// 运行态 history 是调度中立键，不能从五分钟的调度快照做决策。生产构造器注入 GetByID；
// 未接数据库的离线替身仍可使用传入账号。
func (s *openAICodexCookieStore) freshGatewayPoolAccount(ctx context.Context, account *Account) (*Account, error) {
	if s.accountByID == nil {
		return account, nil
	}
	return s.accountByID(ctx, account.ID)
}

func (s *openAICodexCookieStore) gatewayPoolHistoryLock(id int64) func() {
	lock, _ := s.poolHistoryLocks.LoadOrStore(id, &sync.Mutex{})
	mu, ok := lock.(*sync.Mutex)
	if !ok {
		panic("gwpool history lock has an invalid type")
	}
	mu.Lock()
	return mu.Unlock
}

func (s *OpenAIGatewayService) noteGatewayPoolCooldownVerdict(
	ctx context.Context, account *Account, applied OpenAIGatewayPoolApplied, verdict string,
) {
	if s == nil || account == nil || applied.Gateway == "" ||
		(verdict != openAIGatewayVerdictFull && verdict != openAIGatewayVerdictDegraded) {
		return
	}
	identity, err := s.codexCookies.gatewayPoolIdentity(ctx, account)
	if err != nil {
		return
	}
	sample := s.codexCookies.observeGatewayPoolCooldown(identity, applied.Gateway, verdict, account.gatewayPoolGatewayWindow())
	s.reportGatewayPoolCooldown(account, identity, sample)
}
