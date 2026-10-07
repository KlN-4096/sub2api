package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/upstreamrecord"
)

const openAIUpstreamRecordingExtraKey = "openai_upstream_recording_enabled"
const openAIRecordingPolicyTimeout = time.Second

type openAIRecordingKindKey struct{}

func openAIUpstreamRecordingEnabled(account *Account) bool {
	if account == nil || account.Platform != PlatformOpenAI {
		return false
	}
	enabled, _ := account.Extra[openAIUpstreamRecordingExtraKey].(bool)
	return enabled
}

func validateOpenAIUpstreamRecordingExtra(platform string, extra map[string]any) error {
	raw, exists := extra[openAIUpstreamRecordingExtraKey]
	if !exists {
		return nil
	}
	if platform != PlatformOpenAI {
		return infraerrors.BadRequest("OPENAI_RECORDING_PLATFORM_INVALID", "upstream recording is only supported for OpenAI accounts")
	}
	if _, valid := raw.(bool); !valid {
		return infraerrors.BadRequest("OPENAI_RECORDING_INVALID", "openai_upstream_recording_enabled must be a boolean")
	}
	return nil
}

func withOpenAIRecordingKind(ctx context.Context, kind string) context.Context {
	return context.WithValue(ctx, openAIRecordingKindKey{}, kind)
}

func (s *OpenAIGatewayService) getOpenAIRecorder() *upstreamrecord.Recorder {
	s.openaiRecordingOnce.Do(func() {
		if s.openaiRecording != nil {
			return
		}
		dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
		if dataDir == "" {
			dataDir = "data"
		}
		opts := upstreamrecord.Options{
			Directory: filepath.Join(dataDir, "openai-recordings"),
			KeyFile:   filepath.Join(dataDir, "openai-recording.key"),
			MaxBytes:  upstreamrecord.DefaultMaxBytes,
		}
		if s.cfg != nil {
			configured := s.cfg.Gateway.OpenAIRecording
			if strings.TrimSpace(configured.Directory) != "" {
				opts.Directory = configured.Directory
			}
			if strings.TrimSpace(configured.KeyFile) != "" {
				opts.KeyFile = configured.KeyFile
			}
			if configured.MaxBytes > 0 {
				opts.MaxBytes = configured.MaxBytes
			}
		}
		s.openaiRecording = upstreamrecord.New(opts)
	})
	return s.openaiRecording
}

// Reads the selected account, never its credential parent. Rechecking opt-ins
// before recording prevents a stale scheduler snapshot from continuing after
// the administrator disables collection. A failed policy read skips recording,
// not forwarding. The default-off HTTP path does no extra DB or filesystem IO.
func (s *OpenAIGatewayService) openAIRecordingAccount(ctx context.Context, account *Account, checkDisabled bool) *Account {
	if s == nil || account == nil || account.Platform != PlatformOpenAI ||
		(!checkDisabled && !openAIUpstreamRecordingEnabled(account)) {
		return nil
	}
	if s.accountRepo == nil {
		if openAIUpstreamRecordingEnabled(account) {
			return account
		}
		return nil
	}
	policyCtx, cancel := context.WithTimeout(ctx, openAIRecordingPolicyTimeout)
	defer cancel()
	fresh, err := s.accountRepo.GetByID(policyCtx, account.ID)
	if err != nil || fresh == nil || fresh.ID != account.ID {
		slog.Warn("openai_recording_skipped", "account_id", account.ID, "reason", "account_policy_unavailable")
		return nil
	}
	if !openAIUpstreamRecordingEnabled(fresh) {
		return nil
	}
	return fresh
}

func openAIRecordingMetadata(ctx context.Context, account *Account, kind string) map[string]any {
	snapshot, _ := json.Marshal(account.Extra)
	digest := sha256.Sum256(snapshot)
	meta := map[string]any{
		"kind": kind, "account_type": account.Type, "platform": account.Platform,
		"account_extra_sha256": hex.EncodeToString(digest[:]),
		"correlation":          openAIRecordingCorrelation(ctx, kind),
	}
	if account.ProxyID != nil {
		meta["proxy_id"] = *account.ProxyID // Never archive proxy credentials.
	}
	if build, ok := debug.ReadBuildInfo(); ok {
		meta["go_version"] = build.GoVersion
		for _, setting := range build.Settings {
			if strings.HasPrefix(setting.Key, "vcs.") {
				meta[setting.Key] = setting.Value
			}
		}
	}
	return meta
}

func openAIRecordingCorrelation(ctx context.Context, kind string) map[string]any {
	meta := make(map[string]any)
	availability := make(map[string]string)
	for name, key := range map[string]ctxkey.Key{
		"request_id": ctxkey.RequestID, "client_request_id": ctxkey.ClientRequestID,
		"model": ctxkey.Model, "requested_model": ctxkey.RequestedPublicModel,
	} {
		raw := ctx.Value(key)
		value, ok := raw.(string)
		switch {
		case raw == nil || (ok && value == ""):
			availability[name] = "context_absent"
			if kind == "models" && strings.Contains(name, "request_id") {
				availability[name] = "no_user_request_context"
			}
		case !ok:
			availability[name] = "invalid_context_type"
		case len(value) > 512:
			availability[name] = "context_value_too_long"
		default:
			meta[name] = value
			availability[name] = "present"
		}
	}
	for name, key := range map[string]ctxkey.Key{"retry_count": ctxkey.RetryCount, "account_switch_count": ctxkey.AccountSwitchCount} {
		if value, ok := ctx.Value(key).(int); ok {
			meta[name] = value
			availability[name] = "present"
		} else {
			availability[name] = "context_absent_or_invalid"
		}
	}
	meta["availability"] = availability
	return meta
}

func (s *OpenAIGatewayService) recordOpenAIHTTP(account *Account, request *http.Request, kind string, send func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	if request == nil {
		return send(request)
	}
	capture := s.beginOpenAIRecording(request.Context(), account, kind)
	return upstreamrecord.HTTP(capture, request, send)
}

func (s *OpenAIGatewayService) beginOpenAIRecording(ctx context.Context, account *Account, kind string) *upstreamrecord.Capture {
	fresh := s.openAIRecordingAccount(ctx, account, false)
	if fresh == nil {
		return nil
	}
	if source, ok := ctx.Value(openAIRecordingKindKey{}).(string); ok {
		kind = source
	}
	return s.getOpenAIRecorder().Begin(account.ID, openAIRecordingMetadata(ctx, fresh, kind))
}

func (s *AccountTestService) recordOpenAIHTTP(account *Account, request *http.Request, send func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	if s.openaiGatewayService == nil {
		return send(request)
	}
	return s.openaiGatewayService.recordOpenAIHTTP(account, request, "account_test", send)
}

func (s *OpenAIGatewayService) StopOpenAIRecording(ctx context.Context) error {
	if s == nil {
		return nil
	}
	// Synchronize lazy initialization with shutdown, without creating storage.
	s.openaiRecordingOnce.Do(func() {})
	return s.openaiRecording.Close(ctx)
}
