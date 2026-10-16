package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/chatgptcookies"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type qualityGuardRequestKey struct{}

const qualityGuardInterceptedAccountKey = "openai_quality_guard_intercepted_account"

// Request-private only: never a cross-request state cache. Keep the business
// plaintext only when it has a second state carrier that must track the header.
type qualityGuardRequest struct {
	stateBody []byte
	noteSent  func(string)
	intercept func()
}

func withQualityGuardRequest(ctx context.Context, c *gin.Context, account *Account, body []byte) context.Context {
	data := &qualityGuardRequest{noteSent: func(state string) { markOpenAITurnStateSent(c, account, state) }}
	if c != nil {
		c.Set(qualityGuardInterceptedAccountKey, int64(0))
		data.intercept = func() { c.Set(qualityGuardInterceptedAccountKey, account.ID) }
	}
	if gjson.GetBytes(body, "client_metadata."+openAICodexTurnStateHeader).Exists() {
		data.stateBody = body
	}
	return context.WithValue(ctx, qualityGuardRequestKey{}, data)
}

// This is the actual HTTP boundary (not the incoming protocol). It owns no
// downstream writer and does not enable/disable WebSocket or invoke pool logic.
func (s *OpenAIGatewayService) doQualityGuardUpstream(original *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	cfg := account.qualityGuardConfig()
	if cfg.mode != qualityGuardModeBlock && cfg.mode != qualityGuardModeWait {
		return nil, qualityGuardFailure(qualityGuardUnknownMessage)
	}
	if err := requireOpenAIProxyBinding(account, proxyURL); err != nil {
		return nil, err
	}
	request := original.Clone(original.Context())
	s.codexCookies.Attach(account, request.URL.String(), request.Header)
	jar := chatgptcookies.NewJar()
	jar.SetCookies(request.URL, request.Cookies())
	confirmations := 0
	var nextProbeAt time.Time
	var recoveryCtx context.Context
	for {
		if err := request.Context().Err(); err != nil {
			return nil, err
		}
		resp, err := s.doOpenAIUpstreamRoundTrip(request, proxyURL, account)
		if err != nil || resp == nil {
			qualityGuardClose(resp)
			if request.Context().Err() != nil {
				return nil, request.Context().Err()
			}
			if recoveryCtx != nil {
				return nil, qualityGuardFailure(qualityGuardUnknownMessage)
			}
			if err == nil {
				err = qualityGuardFailure(qualityGuardUnknownMessage)
			}
			return nil, err
		}
		if err := request.Context().Err(); err != nil {
			qualityGuardClose(resp)
			return nil, err
		}
		sent := extractOpenAICodexTurnState(request.Header)
		fresh := extractOpenAICodexTurnState(resp.Header)
		// HTTP failures retain the normal upstream error path, even if they
		// carry state. A first minted state is not evidence of replacement.
		if resp.StatusCode != http.StatusOK || sent == "" || fresh == "" || sent == fresh {
			s.codexCookies.Store(account, request.URL.String(), resp.Header)
			return resp, nil
		}
		qualityGuardClose(resp) // BEFORE any wait, template work or probe.
		if data, _ := request.Context().Value(qualityGuardRequestKey{}).(*qualityGuardRequest); data != nil && data.intercept != nil {
			data.intercept()
		}
		if cfg.mode == qualityGuardModeBlock {
			return nil, qualityGuardFailure(qualityGuardBlockedMessage)
		}
		if recoveryCtx == nil {
			// Retry-After may delay a shot, but cannot turn twenty attempts
			// into an unlimited wait. Do not put this deadline on business
			// response bodies: a recovered answer may stream normally.
			budget := time.Duration(cfg.attempts)*qualityGuardProbeTimeout + time.Duration(cfg.attempts-1)*cfg.interval
			var cancel context.CancelFunc
			recoveryCtx, cancel = context.WithTimeout(request.Context(), budget)
			defer cancel()
		}
		jar.SetCookies(request.URL, resp.Cookies())
		template, err := gatewayPoolConfirmTemplate(request)
		if err != nil || original.GetBody == nil {
			return nil, qualityGuardFailure(qualityGuardUnknownMessage)
		}
		state := fresh
		recovered := false
		for confirmations < cfg.attempts {
			if err := qualityGuardWait(recoveryCtx, nextProbeAt); err != nil {
				if request.Context().Err() != nil {
					return nil, request.Context().Err()
				}
				return nil, qualityGuardFailure(qualityGuardExhaustedMessage)
			}
			if !s.qualityGuardAccountUnchanged(request.Context(), account, cfg) {
				return nil, qualityGuardFailure(qualityGuardChangedMessage)
			}
			confirmations++
			result := s.qualityGuardProbe(request.WithContext(recoveryCtx), account, proxyURL, template, state, jar)
			if err := request.Context().Err(); err != nil {
				return nil, err
			}
			if result.state != "" {
				state = result.state
			}
			if result.stop {
				return nil, qualityGuardFailure(qualityGuardUnknownMessage)
			}
			nextProbeAt = time.Now().Add(cfg.interval)
			if result.retryAt.After(nextProbeAt) {
				nextProbeAt = result.retryAt
			}
			if result.accepted {
				recovered = true
				break
			}
		}
		if !recovered {
			return nil, qualityGuardFailure(qualityGuardExhaustedMessage)
		}
		if !s.qualityGuardAccountUnchanged(request.Context(), account, cfg) {
			return nil, qualityGuardFailure(qualityGuardChangedMessage)
		}
		request, err = qualityGuardReplay(original, state, jar)
		if err != nil {
			return nil, qualityGuardFailure(qualityGuardUnknownMessage)
		}
		// If this replay is covered again, it shares the same finite probe
		// budget and next-probe gap; it cannot reset to twenty attempts.
	}
}

func qualityGuardReplay(original *http.Request, state string, jar *chatgptcookies.Jar) (*http.Request, error) {
	request := original.Clone(original.Context())
	body, err := original.GetBody()
	if err != nil {
		return nil, err
	}
	request.Body = body
	if data, _ := original.Context().Value(qualityGuardRequestKey{}).(*qualityGuardRequest); data != nil {
		if len(data.stateBody) > 0 {
			_ = body.Close()
			wire, err := sjson.SetBytes(data.stateBody, "client_metadata."+openAICodexTurnStateHeader, state)
			if err != nil {
				return nil, err
			}
			if original.Header.Get("Content-Encoding") == codexRequestZstdContentEncoding {
				wire, err = encodeCodexZstdRequestBody(wire)
				if err != nil {
					return nil, err
				}
			}
			request.Body = io.NopCloser(bytes.NewReader(wire))
			request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(wire)), nil }
			request.ContentLength = int64(len(wire))
			request.Header.Del("Content-Length")
		}
		if data.noteSent != nil {
			data.noteSent(state)
		}
	}
	request.Header.Set(openAICodexTurnStateHeader, state)
	qualityGuardRefreshAttemptID(request.Header)
	qualityGuardAttachCookies(request, jar)
	return request, nil
}

func qualityGuardRefreshAttemptID(headers http.Header) {
	if headers.Get("x-codex-inference-call-id") != "" {
		headers.Set("x-codex-inference-call-id", uuid.NewString())
	}
}

func qualityGuardAttachCookies(request *http.Request, jar *chatgptcookies.Jar) {
	request.Header.Del("Cookie")
	for _, cookie := range jar.Cookies(request.URL) {
		request.AddCookie(cookie)
	}
}

func qualityGuardClose(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func qualityGuardWait(ctx context.Context, until time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay := time.Until(until); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func (s *OpenAIGatewayService) qualityGuardAccountUnchanged(ctx context.Context, account *Account, cfg openAIQualityGuardConfig) bool {
	if s.accountRepo == nil {
		return true // isolated service tests; production always has a repository
	}
	fresh, err := s.accountRepo.GetByID(ctx, account.ID)
	return err == nil && fresh != nil && fresh.IsSchedulable() && fresh.qualityGuardEnabled() &&
		fresh.qualityGuardConfig() == cfg && reflect.DeepEqual(fresh.ProxyID, account.ProxyID) &&
		reflect.DeepEqual(fresh.Proxy, account.Proxy) &&
		reflect.DeepEqual(fresh.Credentials, account.Credentials)
}
