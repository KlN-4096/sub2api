package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

type openAIQuotaUsageResult struct {
	usage *OpenAIQuotaUsage
	call  *openAIQuotaCall
}

type openAIQuotaCachedUsage struct {
	usage         *OpenAIQuotaUsage
	err           error
	nextAllowedAt time.Time
}

func (c *openAIQuotaCachedUsage) fresh(now time.Time) bool {
	return c != nil && now.Before(c.nextAllowedAt)
}

func (s *OpenAIQuotaService) querySharedUsage(ctx context.Context, accountID int64, force bool) (*openAIQuotaUsageResult, error) {
	call, err := s.prepareUpstreamCall(ctx, accountID, false)
	if err != nil {
		return nil, err
	}
	// /wham/usage 是按上游用户计的。codexAccountIdentityNamespace 在缺
	// chatgpt_user_id 时会退化成只带 chatgpt_account_id 的形式，而它是工作区级的
	// （见 handler/admin/account_codex_import.go 对同 account_id 多成员的告警）：
	// 同工作区的两行会互相读到对方的额度，再被 persistOpenAICodexProbeSnapshot
	// 写进各自的 extra，污染调度判断。只有定位得到具体用户才允许共享这条在途请求；
	// 退回来的是解析后的凭证行 ID，影子与母账号仍然共享。
	key := codexAccountIdentityNamespace(call.account)
	if key == "" || strings.TrimSpace(call.account.GetCredential("chatgpt_user_id")) == "" {
		key = fmt.Sprintf("account:%d", call.account.ID)
	}
	load := func() *openAIQuotaCachedUsage {
		value, _ := s.usageCache.Load(key)
		cached, _ := value.(*openAIQuotaCachedUsage)
		return cached
	}
	observed := load()
	if !force && observed.fresh(time.Now()) {
		return &openAIQuotaUsageResult{usage: observed.usage, call: call}, observed.err
	}
	resultCh := s.usageFlight.DoChan(key, func() (any, error) {
		// Close the cache-check/flight gap: a refresh completed while this caller
		// joined. Force may bypass an old deadline, not a concurrent fresh result.
		if current := load(); current != observed && current.fresh(time.Now()) {
			return &openAIQuotaUsageResult{usage: current.usage, call: call}, current.err
		}
		// One canceled dashboard request must not cancel the shared query for
		// other callers. The detached operation still has a hard time limit.
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openaiQuotaUpstreamTimeout)
		defer cancel()
		result, err := s.fetchUsage(callCtx, accountID, call)
		cached := &openAIQuotaCachedUsage{err: err, nextAllowedAt: nextOpenAIProbeAllowedAt(time.Now())}
		if result != nil {
			cached.usage = result.usage
		}
		s.usageCache.Store(key, cached)
		return result, err
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			if isOpenAIAutoResetContext(ctx) {
				return nil, infraerrors.New(infraerrors.Code(result.Err), "OPENAI_QUOTA_UPSTREAM_ERROR", "upstream usage query failed")
			}
			return nil, result.Err
		}
		// 带 ok 的断言：DoChan 的 Val 是 any，err==nil 时当前实现必然给出非 nil 的
		// *openAIQuotaUsageResult，但这条不变量只由 fetchUsage 的返回路径维持。
		// 将来任何一条 (nil, nil) 都会在这里变成生产进程里的空指针解引用。
		shared, ok := result.Val.(*openAIQuotaUsageResult)
		if !ok || shared == nil {
			return nil, infraerrors.New(http.StatusBadGateway, "OPENAI_QUOTA_UPSTREAM_ERROR", "upstream usage query returned no result")
		}
		if call.account.IsOpenAIAgentIdentity() && call.account.GetCredential("task_id") != shared.call.account.GetCredential("task_id") {
			// Recovery replaced the leader's task. Reload this caller's entire
			// snapshot, retaining its own row identity rather than borrowing the
			// leader's UA/proxy for a subsequent credit-details request.
			call, err = s.prepareUpstreamCall(ctx, accountID, false)
			if err != nil {
				return nil, err
			}
		}
		return &openAIQuotaUsageResult{usage: shared.usage, call: call}, nil
	}
}
