package service

import (
	"net/http"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const (
	openAIQualityGuardEnabledKey                       = "openai_quality_guard_enabled"
	openAIQualityGuardModeKey                          = "openai_quality_guard_mode"
	openAIQualityGuardAttemptsKey                      = "openai_quality_guard_max_attempts"
	openAIQualityGuardIntervalKey                      = "openai_quality_guard_interval_s"
	qualityGuardModeBlock                              = "block"
	qualityGuardModeWait                               = "wait"
	qualityGuardDefaultAttempts                        = 20
	qualityGuardMaxAttempts                            = 100
	qualityGuardDefaultInterval                        = 5
	qualityGuardMaxInterval                            = 300
	qualityGuardProbeTimeout                           = 15 * time.Second
	OpenAIQualityGuardReason      GatewayFailureReason = "openai_quality_guard"
)

type openAIQualityGuardConfig struct {
	mode     string
	attempts int
	interval time.Duration
}

func (a *Account) qualityGuardEnabled() bool {
	return a.IsOpenAIOAuthLike() && !a.UsesGatewayPool() && a.getExtraBool(openAIQualityGuardEnabledKey)
}

func (a *Account) qualityGuardConfig() openAIQualityGuardConfig {
	mode, _ := a.Extra[openAIQualityGuardModeKey].(string)
	attempts, ok := gatewayPoolInteger(a.Extra[openAIQualityGuardAttemptsKey], qualityGuardMaxAttempts)
	if !ok {
		attempts = qualityGuardDefaultAttempts
	}
	interval, ok := gatewayPoolInteger(a.Extra[openAIQualityGuardIntervalKey], qualityGuardMaxInterval)
	if !ok {
		interval = qualityGuardDefaultInterval
	}
	return openAIQualityGuardConfig{mode, attempts, time.Duration(interval) * time.Second}
}

func touchesOpenAIQualityGuardConfig(extra map[string]any) bool {
	for _, key := range []string{openAIQualityGuardEnabledKey, openAIQualityGuardModeKey, openAIQualityGuardAttemptsKey, openAIQualityGuardIntervalKey} {
		if _, ok := extra[key]; ok {
			return true
		}
	}
	return false
}

func validateOpenAIQualityGuardExtra(account *Account, extra map[string]any) error {
	invalid := func(message string) error {
		return infraerrors.New(http.StatusBadRequest, "QUALITY_GUARD_SETTING_INVALID", message)
	}
	if raw := extra[openAIQualityGuardEnabledKey]; raw != nil {
		if _, ok := raw.(bool); !ok {
			return invalid("Quality guard enabled must be boolean")
		}
	}
	mode, _ := extra[openAIQualityGuardModeKey].(string)
	if raw := extra[openAIQualityGuardModeKey]; raw != nil && mode != qualityGuardModeBlock && mode != qualityGuardModeWait {
		return invalid("Quality guard mode must be block or wait")
	}
	for key, limit := range map[string]int{openAIQualityGuardAttemptsKey: qualityGuardMaxAttempts, openAIQualityGuardIntervalKey: qualityGuardMaxInterval} {
		if raw := extra[key]; raw != nil {
			if _, ok := gatewayPoolInteger(raw, limit); !ok {
				return invalid("Quality guard attempts must be 1–100 and interval must be 1–300 seconds (integers)")
			}
		}
	}
	if enabled, _ := extra[openAIQualityGuardEnabledKey].(bool); enabled {
		if !account.IsOpenAIOAuthLike() {
			return invalid("Quality guard requires a local OpenAI OAuth or setup-token account")
		}
		if mode != qualityGuardModeBlock && mode != qualityGuardModeWait {
			return invalid("Choose block or wait before enabling quality guard")
		}
	}
	return nil
}

func qualityGuardFailure(message string) *UpstreamFailoverError {
	return &UpstreamFailoverError{
		StatusCode: http.StatusServiceUnavailable, ClientStatusCode: http.StatusServiceUnavailable,
		ClientMessage: message, Reason: OpenAIQualityGuardReason, NextAccountAction: NextAccountStop,
	}
}

const (
	qualityGuardBlockedMessage   = "降智防护：检测到 turn-state 被覆盖，已丢弃原响应 / Quality guard blocked a replaced turn-state response"
	qualityGuardExhaustedMessage = "降智防护：Luna 确认次数已耗尽，未交付原响应 / Quality guard exhausted Luna confirmations; the discarded response was not delivered"
	qualityGuardChangedMessage   = "降智防护：账号、凭据或配置已变更，已停止等待 / Quality guard stopped because account availability, credentials or settings changed"
	qualityGuardUnknownMessage   = "降智防护：无法安全确认或重发，已丢弃原响应 / Quality guard could not safely confirm or replay the discarded response"
	qualityGuardTimeoutMessage   = "降智防护：已达到请求的首输出时限，停止等待 / Quality guard stopped at the request's first-output deadline"
)
