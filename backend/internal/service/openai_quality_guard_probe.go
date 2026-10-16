package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/chatgptcookies"
	"github.com/tidwall/gjson"
)

const qualityGuardProbeBodyLimit = 4 << 20

type qualityGuardProbeResult struct {
	state    string
	accepted bool
	stop     bool
	retryAt  time.Time
}

func (s *OpenAIGatewayService) qualityGuardProbe(original *http.Request, account *Account, proxyURL string, template []byte, state string, jar *chatgptcookies.Jar) qualityGuardProbeResult {
	ctx, cancel := context.WithTimeout(withoutOpenAITurnStateRelay(original.Context()), qualityGuardProbeTimeout)
	defer cancel()
	probe, err := gatewayPoolConfirmationRequest(ctx, original, template, state)
	if err != nil {
		return qualityGuardProbeResult{stop: true}
	}
	qualityGuardAttachCookies(probe, jar)
	qualityGuardRefreshAttemptID(probe.Header)
	route := qualityGuardRouteCookies(probe.Cookies())
	probe = probe.WithContext(withOpenAIRecordingKind(probe.Context(), "quality_guard_confirmation"))
	resp, err := s.doOpenAIUpstreamRoundTrip(probe, proxyURL, account)
	defer qualityGuardClose(resp)
	if err != nil || resp == nil {
		return qualityGuardProbeResult{}
	}
	result := qualityGuardProbeResult{state: extractOpenAICodexTurnState(resp.Header)}
	jar.SetCookies(probe.URL, resp.Cookies())
	if at := parseRetryAfterResetTime(resp.Header, time.Now()); at != nil {
		result.retryAt = *at
	}
	if resp.StatusCode != http.StatusOK {
		result.stop = resp.StatusCode >= 400 && resp.StatusCode < 500 &&
			resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusRequestTimeout
		return result
	}
	if resp.Body == nil {
		return result
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, qualityGuardProbeBodyLimit+1))
	if err != nil || len(body) > qualityGuardProbeBodyLimit || ctx.Err() != nil {
		return result
	}
	complete, eventState := qualityGuardProbeCompleted(body, isEventStreamResponse(resp.Header))
	if eventState != "" {
		result.state = eventState
	}
	// A route-cookie update is not proof for the next route. Confirm it on a
	// subsequent probe, using this request's private jar, never the shared jar.
	result.accepted = complete && (result.state == "" || result.state == state) &&
		route == qualityGuardRouteCookies(jar.Cookies(probe.URL))
	return result
}

func qualityGuardRouteCookies(cookies []*http.Cookie) string {
	var cflb, oailb string
	for _, cookie := range cookies {
		switch cookie.Name {
		case "__cflb":
			cflb = cookie.Value
		case "__oailb":
			oailb = cookie.Value
		}
	}
	return cflb + "\x00" + oailb
}

// A 200, [DONE], EOF, or terminal failed/incomplete frame alone is not success.
// Consume a bounded probe body (not business traffic) and retain any error,
// even if a later frame claims completion.
func qualityGuardProbeCompleted(body []byte, stream bool) (bool, string) {
	if !stream {
		return gjson.ValidBytes(body) && gjson.GetBytes(body, "status").String() == "completed" &&
			!qualityGuardJSONError(body), ""
	}
	completed, failed, state := false, false, ""
	forEachOpenAISSEFrame(string(body), func(kind string, data []byte) {
		if !gjson.ValidBytes(data) || qualityGuardJSONError(data) {
			failed = true
			return
		}
		switch kind {
		case "error", "response.failed", "response.incomplete", "response.cancelled":
			failed = true
		case "response.completed", "response.done":
			status := gjson.GetBytes(data, "response.status").String()
			if status == "completed" {
				completed = true
			} else {
				failed = true
			}
		case "response.metadata":
			gjson.GetBytes(data, "headers").ForEach(func(key, value gjson.Result) bool {
				if strings.EqualFold(key.String(), openAICodexTurnStateHeader) && strings.TrimSpace(value.String()) != "" {
					state = strings.TrimSpace(value.String())
				}
				return true
			})
		}
	})
	return completed && !failed, state
}

func qualityGuardJSONError(body []byte) bool {
	for _, path := range []string{"error", "response.error", "incomplete_details", "response.incomplete_details"} {
		value := gjson.GetBytes(body, path)
		if value.Exists() && value.Type != gjson.Null {
			return true
		}
	}
	return false
}
