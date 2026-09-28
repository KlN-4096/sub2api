package handler

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// noAccountErrorClassification describes the HTTP response to emit when
// account selection failed with ErrNoAvailableAccounts. Handlers obtain it
// via classifyNoAccountError and choose between:
//
//   - 404 model_not_found — the group has accounts, but none of them are
//     configured to serve the requested model (config / typo / unsupported
//     model). Returning 503 here misleads operators and trips reverse-proxy
//     health checks; 404 lets the client surface the real problem.
//
//   - 503 api_error — accounts that could serve the model exist but are
//     temporarily exhausted (rate limit, quota auto-pause, runtime block) OR
//     the group has no accounts at all. Both stay on 503 because retrying
//     after a backoff can plausibly succeed (or, in the empty-pool case, the
//     operator may be in the middle of adding accounts).
type noAccountErrorClassification struct {
	Status        int
	ErrType       string
	Message       string
	ModelNotFound bool // true when this is a 404 model_not_found classification
}

var selectionModelRateLimitedPattern = regexp.MustCompile(`(?:model_rate_limited|rate_limited)=(\d+)`)

// 已废弃（2026-09-23）：降智暂停随 292 猎手一起废弃，后续版本移除时这段分类一并删。
//
// selectionTurnStateHoldPattern 认出降智暂停造成的空池。它借 model_rate_limits 存放，但不是限流：
// 回 429「所有账号都在限流」既说不清原因，又让 Codex 按限流硬重试（客户端只显示 "exceeded retry
// limit, last status: 429"，正文被吞）。单独分类后与注入点的换号错误同口径回 503 + 说明。
//
// 计数由 OpenAI 侧的 openAISelectionFilterStats.summary 产出（形如 "pool=1, filtered:
// turn_state_hold=1"）——Codex 走的是那条调度路径，不是 GatewayService 的 summarize。
// 正则由 service 侧的常量拼出来：手抄一份的话，改名时两边的测试都绿而生产静默掉回 429。
var selectionTurnStateHoldPattern = regexp.MustCompile(regexp.QuoteMeta(service.OpenAITurnStateHoldSelectionReason) + `=(\d+)`)

// selectionPoolPattern 取 OpenAI 侧 summary 的池子大小。summary 的构造保证
// sum(reasons) + len(passed) == pool，所以「暂停计数 >= pool」精确等价于「池里每个账号
// 都是因降智暂停被挡的」。GatewayService 那份 summary 没有 pool= 字段，取不到就是 0，
// 暂停分支自然不会在那条路上触发（降智暂停也本来不写在那些账号上）。
var selectionPoolPattern = regexp.MustCompile(`\bpool=(\d+)`)

// selectionTurnStateHoldMessage 与 service.openAITurnStateHoldError 的 ClientMessage 同口径。
// 不写「几分钟」：暂停时长是一个空闲窗口（默认 60 分钟），猎到票会提前放回，但上界是一小时。
const selectionTurnStateHoldMessage = "All accounts serving this model are paused: no healthy x-codex-turn-state is available and the hunter is fetching one. This clears as soon as the hunter finds a ticket."

// selectionFailureCount 取 pattern 的计数，未匹配或非正数返回 0。
func selectionFailureCount(pattern *regexp.Regexp, lowered string) int {
	match := pattern.FindStringSubmatch(lowered)
	if len(match) != 2 {
		return 0
	}
	count, err := strconv.Atoi(match[1])
	if err != nil || count <= 0 {
		return 0
	}
	return count
}

// classifySelectionFailureError preserves the scheduler's compact reason when
// every model-capable account is temporarily rate limited.
func classifySelectionFailureError(err error, fallback noAccountErrorClassification) noAccountErrorClassification {
	if err == nil {
		return fallback
	}
	// A 404 model_not_found fallback is authoritative and must not be downgraded
	// to a rate-limit verdict. classifyNoAccountError only reaches it through
	// DiagnoseModelAvailabilityForPlatform, a dedicated database query over
	// persistent eligibility (active + schedulable + model_mapping) that already
	// established no account in the group can serve this model at all. A transient
	// per-model cooldown on one of the remaining candidates does not make "all
	// available accounts are rate-limited" true.
	//
	// Reporting 429 here is actively harmful: retrying can never succeed, and
	// clients that treat 429 as a rate limit retry hard and swallow the body
	// (Codex surfaces only "exceeded retry limit, last status: 429"), losing the
	// one message that names the real problem. It also flips the ops attribution
	// from a local model-configuration issue to routing capacity, because call
	// sites gate markOpsRoutingCapacityLimitedIfNoAvailable on ModelNotFound.
	if fallback.ModelNotFound {
		return fallback
	}
	lowered := strings.ToLower(err.Error())
	// 只有整个池子都被降智暂停挡住时才说「都被暂停了」。拿暂停去比限流是不够的：池子还可能
	// 被 quota_auto_pause_7d / runtime_blocked / capability_mismatch 等十几个 reason 占满，
	// 那时 1 个暂停也会 held >= rateLimited，于是把「9 个号周额度打满」说成「猎手在补票」——
	// 正是这次改动要消灭的那个毛病换个方向复发。
	if held := selectionFailureCount(selectionTurnStateHoldPattern, lowered); held > 0 {
		if pool := selectionFailureCount(selectionPoolPattern, lowered); pool > 0 && held >= pool {
			return noAccountErrorClassification{
				Status:  http.StatusServiceUnavailable,
				ErrType: "api_error",
				Message: selectionTurnStateHoldMessage,
			}
		}
	}
	if selectionFailureCount(selectionModelRateLimitedPattern, lowered) <= 0 {
		return fallback
	}
	return noAccountErrorClassification{
		Status:  http.StatusTooManyRequests,
		ErrType: "rate_limit_error",
		Message: "All available accounts are currently rate-limited. Please retry later.",
	}
}

// classifyNoAccountError decides between 404 model_not_found and 503
// api_error for "no available accounts" failures.
//
// The classifier intentionally does not consume the original error: the
// selection layer never tells us *why* the pool came up empty (rate-limited
// vs. unsupported model are both wrapped as ErrNoAvailableAccounts). Instead
// we re-check pool composition through DiagnoseModelAvailabilityForPlatform.
// Its dedicated database query considers only persistent eligibility
// (active status + schedulable setting) and model_mapping, bypassing scheduler
// snapshots and transient filters. That guarantees a 404 is only returned
// when persistent account/group/model configuration must change before the
// request can succeed.
//
// routingModel is the model name that account selection actually compared
// against (i.e. after group-level dispatch mapping). displayModel is the
// raw model the caller asked for; it is used only in the user-facing error
// message so that internal mapping details don't leak. Most callers pass
// the same value for both.
//
// platform is the platform the request was routed to (use
// service.PlatformOpenAI / PlatformAnthropic / PlatformGemini). It is
// required because Anthropic/Gemini routes additionally surface
// mixed-scheduled Antigravity accounts; passing the wrong platform would
// flip a legitimate 503 to a misleading 404 (or vice versa).
func classifyNoAccountError(
	ctx context.Context,
	diag service.ModelAvailabilityDiagnoser,
	apiKey *service.APIKey,
	routingModel string,
	displayModel string,
	platform string,
) noAccountErrorClassification {
	fallback := noAccountErrorClassification{
		Status:  http.StatusServiceUnavailable,
		ErrType: "api_error",
		Message: "Service temporarily unavailable",
	}

	routingModel = strings.TrimSpace(routingModel)
	displayModel = strings.TrimSpace(displayModel)
	if displayModel == "" {
		displayModel = routingModel
	}
	if diag == nil || apiKey == nil || apiKey.GroupID == nil || routingModel == "" {
		return fallback
	}

	result := diag.DiagnoseModelAvailabilityForPlatform(ctx, apiKey.GroupID, routingModel, platform)
	if result.HasAccountsInPool && !result.HasModelSupport {
		return noAccountErrorClassification{
			Status:        http.StatusNotFound,
			ErrType:       "model_not_found",
			Message:       fmt.Sprintf("Model %q is not supported by any configured account in this group", displayModel),
			ModelNotFound: true,
		}
	}
	return fallback
}

// classifyNoAccountErrorFromGin is a thin wrapper that forwards the gin
// context's underlying request context. Most call sites already have a
// *gin.Context handy, so this keeps the call sites uncluttered.
func classifyNoAccountErrorFromGin(
	c *gin.Context,
	diag service.ModelAvailabilityDiagnoser,
	apiKey *service.APIKey,
	routingModel string,
	displayModel string,
	platform string,
) noAccountErrorClassification {
	ctx := context.Background()
	if c != nil && c.Request != nil {
		ctx = c.Request.Context()
	}
	classification := classifyNoAccountError(ctx, diag, apiKey, routingModel, displayModel, platform)
	if classification.ModelNotFound {
		service.MarkOpsClientBusinessLimited(c, service.OpsClientBusinessLimitedReasonLocalModelConfiguration)
	}
	return classification
}

func classifyOpenAICompatibleNoAccountErrorFromGin(
	c *gin.Context,
	diag service.ModelAvailabilityDiagnoser,
	apiKey *service.APIKey,
	routingModel string,
	displayModel string,
) noAccountErrorClassification {
	ctx := context.Background()
	if c != nil && c.Request != nil {
		ctx = c.Request.Context()
	}
	return classifyNoAccountErrorFromGin(
		c,
		diag,
		apiKey,
		routingModel,
		displayModel,
		openAICompatibleRequestPlatform(ctx, apiKey),
	)
}

func openAICompatibleSelectionErrorForLog(err error, platform string) error {
	if err == nil || platform != service.PlatformGrok {
		return err
	}
	message := strings.ReplaceAll(err.Error(), "OpenAI accounts", "Grok accounts")
	if message == err.Error() {
		return err
	}
	return fmt.Errorf("%s", message)
}
