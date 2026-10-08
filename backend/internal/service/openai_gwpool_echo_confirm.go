package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

type gatewayPoolConfirmBodyKey struct{}

// Capture only final wire identities, never user input, instructions, tools or
// response IDs. Sending these values through the identity builder again would
// namespace them twice. The original body is not retained in this context value.
func gatewayPoolConfirmBody(body []byte) []byte {
	if !gjson.ValidBytes(body) || gjson.GetBytes(body, "model").String() == "" {
		return nil
	}
	payload := openAITurnStateProbeBody(gatewayPoolProbeModelLuna, gatewayPoolWarmProbeEffort, openAITurnStateProbeIdentity{}, gatewayPoolWarmProbeText)
	delete(payload, "prompt_cache_key")
	delete(payload, "client_metadata")
	for _, key := range []string{"prompt_cache_key", "reasoning", "service_tier"} {
		if value := gjson.GetBytes(body, key); value.Exists() {
			payload[key] = json.RawMessage(value.Raw)
		}
	}
	metadata := map[string]json.RawMessage{}
	for _, key := range []string{"session_id", "thread_id", "turn_id", "root_turn_id",
		"x-codex-installation-id", "x-codex-window-id", "x-codex-turn-metadata"} {
		if value := gjson.GetBytes(body, "client_metadata."+key); value.Exists() {
			metadata[key] = json.RawMessage(value.Raw)
			if key == openAIWSTurnMetadataHeader && value.Type == gjson.String {
				encoded, _ := json.Marshal(alignCodexTurnMetadataJSON(value.String(), map[string]string{"model": gatewayPoolProbeModelLuna}))
				metadata[key] = encoded
			}
		}
	}
	if len(metadata) > 0 {
		payload["client_metadata"] = metadata
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

func gatewayPoolConfirmTemplate(request *http.Request) ([]byte, error) {
	if body, ok := request.Context().Value(gatewayPoolConfirmBodyKey{}).([]byte); ok {
		if len(body) == 0 {
			return nil, errOpenAIGatewayPoolWarmUnverified
		}
		return body, nil
	}
	// Direct callers/tests may not go through buildUpstreamRequest. Never read
	// or replay the original stream, and never guess the encoding.
	if request.GetBody == nil || request.Header.Get("Content-Encoding") != "" {
		return nil, errOpenAIGatewayPoolWarmUnverified
	}
	body, err := request.GetBody()
	if err != nil {
		return nil, errOpenAIGatewayPoolWarmUnverified
	}
	defer func() { _ = body.Close() }()
	const maxFallbackBody = 1 << 20
	raw, err := io.ReadAll(io.LimitReader(body, maxFallbackBody+1))
	if err != nil || len(raw) > maxFallbackBody {
		return nil, errOpenAIGatewayPoolWarmUnverified
	}
	template := gatewayPoolConfirmBody(raw)
	if len(template) == 0 {
		return nil, errOpenAIGatewayPoolWarmUnverified
	}
	return template, nil
}

func gatewayPoolConfirmationRequest(ctx context.Context, original *http.Request, body []byte, state string) (*http.Request, error) {
	var err error
	switch original.Header.Get("Content-Encoding") {
	case "":
	case codexRequestZstdContentEncoding:
		body, err = encodeCodexZstdRequestBody(body)
	default:
		return nil, errOpenAIGatewayPoolWarmUnverified
	}
	if err != nil {
		return nil, err
	}
	request := original.Clone(WithHTTPUpstreamProfile(ctx, HTTPUpstreamProfileOpenAIConfirmation))
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	request.ContentLength = int64(len(body))
	request.TransferEncoding = nil
	request.Header.Del("Content-Length")
	request.Header.Set(openAICodexTurnStateHeader, state)
	alignCodexTurnMetadataFields(request.Header, map[string]string{"model": gatewayPoolProbeModelLuna})
	return request, nil
}

// The whole round shares one deadline, bounded by the caller and this exact
// probe budget. Reference TTL cannot cancel a confirmation or business response.
func (s *OpenAIGatewayService) gatewayPoolConfirmResponse(request *http.Request, account *Account,
	proxyURL, identity, state string, applied OpenAIGatewayPoolApplied, businessSentAt time.Time,
) (bool, bool, error) {
	pair, live := s.codexCookies.cachedPoolPair(identity)
	if live != openAIGatewayPoolPairLive || pair.version != applied.Version || pair.firstSent.IsZero() {
		return false, false, retryGatewayPoolAttempt(errOpenAIGatewayPoolWarmUnverified)
	}
	template, err := gatewayPoolConfirmTemplate(request)
	if err != nil {
		return false, false, err
	}
	// Freeze age at this business send, not at response arrival or after
	// contact persistence. Unknown/inconsistent sending time uses the old tier.
	age := gatewayPoolEchoYoungAge
	if !businessSentAt.IsZero() && !businessSentAt.Before(pair.firstSent) {
		age = businessSentAt.Sub(pair.firstSent)
	}
	attempts := gatewayPoolEchoStrikes(age)
	shotTimeout := gatewayPoolProbeTimeout(request.Context(), account)
	ctx, cancel := context.WithTimeout(request.Context(), time.Duration(attempts)*shotTimeout)
	defer cancel()
	trace := &gatewayPoolProbeTrace{}
	// This confirms an already-dispatched business response. The old 429
	// admission window must still gate new business, not this bounded probe.
	ctx = context.WithValue(ctx, gatewayPoolProbeTraceKey{}, trace)
	started := time.Now()
	var steps []gatewayPoolProbeStep
	full, conclusive, err := gatewayPoolConfirmState(ctx, request.Header.Get("Cookie"), state,
		attempts, func(ctx context.Context, cookie, state string) (int, string, error) {
			current, status := s.codexCookies.cachedPoolPair(identity)
			if status != openAIGatewayPoolPairLive || current.version != applied.Version {
				return 0, "", retryGatewayPoolAttempt(errOpenAIGatewayPoolWarmUnverified)
			}
			shotCtx, stop := context.WithTimeout(ctx, shotTimeout)
			defer stop()
			probe, err := gatewayPoolConfirmationRequest(shotCtx, request, template, state)
			if err != nil {
				return 0, "", err
			}
			probe.Header.Set("Cookie", cookie)
			probe = probe.WithContext(withOpenAIRecordingKind(probe.Context(), "echo_confirmation"))
			shotAt := time.Now()
			resp, sentAt, err := s.gatewayPoolObservedRoundTrip(probe, proxyURL, account, true)
			if !sentAt.IsZero() {
				trace.markSent(sentAt)
				s.codexCookies.gatewayPoolMarkSent(identity, applied.Version, sentAt)
			}
			step := gatewayPoolProbeStep{Shot: "confirm", Sent: !sentAt.IsZero(), DurationMS: time.Since(shotAt).Milliseconds()}
			var fresh string
			if resp != nil {
				step.Status = resp.StatusCode
				fresh = extractOpenAICodexTurnState(resp.Header)
				step.GotState = fresh != ""
				step.EchoAccepted = err == nil && resp.StatusCode == http.StatusOK && (fresh == "" || fresh == state)
				for _, c := range resp.Cookies() {
					if c.Name == "__oailb" {
						step.ActualGateway = openAICodexRouteGateway("__oailb=" + c.Value)
					}
				}
				if resp.Body != nil {
					if resp.StatusCode >= http.StatusBadRequest {
						const errorBodyLimit = 16 << 10
						body, _ := io.ReadAll(io.LimitReader(resp.Body, errorBodyLimit))
						s.handleOpenAIAccountUpstreamError(ctx, account, resp.StatusCode, resp.Header, body,
							gjson.GetBytes(template, "model").String())
					}
					_ = resp.Body.Close()
				}
				if step.ActualGateway != "" && step.ActualGateway != applied.Gateway {
					err = retryGatewayPoolAttempt(errOpenAIGatewayPoolWarmUnverified)
				}
			}
			steps = append(steps, step)
			return step.Status, fresh, err
		})
	current, status := s.codexCookies.cachedPoolPair(identity)
	if status == openAIGatewayPoolPairNone || current.invalidated || current.version != applied.Version || ctx.Err() != nil {
		full, conclusive, err = false, false, retryGatewayPoolAttempt(errOpenAIGatewayPoolWarmUnverified)
	}
	// Same round as the original business: extra traffic updates last contact
	// but must not become another initial full-strength sample.
	s.noteGatewayPoolProbeAndContact(request.Context(), account, gatewayPoolProbeObservation{
		Source: "business", Identity: identity, Applied: applied, Model: gjson.GetBytes(template, "model").String(),
		Shots: trace.shots, FirstSent: trace.firstSent, LastSent: trace.lastSent, Steps: steps,
		// Confirmation traffic is not an independent quality observation.
		// Preserve raw header steps; only the final business decision trains
		// cooldowns, so a late cancellation/CAS failure leaves no false sample.
		DurationMS: time.Since(started).Milliseconds(),
	})
	current, status = s.codexCookies.cachedPoolPair(identity)
	if status == openAIGatewayPoolPairNone || current.invalidated || current.version != applied.Version || ctx.Err() != nil {
		full, conclusive, err = false, false, retryGatewayPoolAttempt(errOpenAIGatewayPoolWarmUnverified)
	}
	return full, conclusive, err
}

// The business response has already supplied the first fresh state. Each
// confirmation echoes the most recently supplied value, never the expired one.
// The budget is fixed for this round; crossing a ticket-age boundary mid-round
// does not silently remove a confirmation.
func gatewayPoolConfirmState(ctx context.Context, cookie, state string, attempts int,
	shoot gatewayPoolWarmShooter,
) (full, conclusive bool, err error) {
	if strings.TrimSpace(state) == "" || attempts < 1 || attempts > gatewayPoolEchoStrikes(0) {
		return false, false, errOpenAIGatewayPoolWarmUnverified
	}
	for range attempts {
		if err := ctx.Err(); err != nil {
			return false, false, err
		}
		status, fresh, err := shoot(ctx, cookie, state)
		if err != nil || ctx.Err() != nil || status != http.StatusOK {
			if err == nil {
				err = ctx.Err()
				if err == nil && status != http.StatusOK {
					err = &gatewayPoolProbeHTTPError{status: status}
				}
			}
			return false, false, err
		}
		if fresh == "" || fresh == state {
			return true, true, nil
		}
		state = fresh
	}
	return false, true, nil
}
