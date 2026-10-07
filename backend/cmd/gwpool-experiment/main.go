//go:build gwpoolexperiment

// Explicitly bounded, one-off pro1 experiment. Input secrets remain in memory.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

const stateHeader = "x-codex-turn-state"
const question = "不要思考也不要联网,只回答一个名字,现在日本首相是谁"

type candidate struct {
	Gateway string `json:"gateway"`
	Cookie  string `json:"cookie"`
	Hint    string `json:"hint"`
}
type input struct {
	Account           service.Account `json:"account"`
	Model             string          `json:"model"`
	ProxyURL          string          `json:"proxy_url"`
	HTTP2             bool            `json:"http2"`
	TransportVerified bool            `json:"transport_verified"`
	ExpectedProtocol  string          `json:"expected_protocol"`
	Candidates        []candidate     `json:"candidates"`
}
type runner struct {
	input    input
	mode     string
	count    int
	limit    int
	upstream service.HTTPUpstream
}
type shot struct {
	Status  int
	State   string
	Outcome string
}

func emit(value any) { _ = json.NewEncoder(os.Stdout).Encode(value) }

func (r *runner) request(ctx context.Context, model, text string, fixed *service.GatewayPoolExperimentSession) (*http.Request, error) {
	return service.BuildGatewayPoolExperimentRequest(ctx, &r.input.Account, model, text, fixed)
}

func (r *runner) send(c candidate, model, label, state, text string, fixed *service.GatewayPoolExperimentSession) (shot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	ctx = service.WithHTTPUpstreamRedirectsDisabled(ctx)
	req, err := r.request(ctx, model, text, fixed)
	if err != nil {
		return shot{}, fmt.Errorf("request_build_failed")
	}
	if req.URL.Scheme != "https" || req.URL.Host != "chatgpt.com" || req.URL.Path != "/backend-api/codex/responses" {
		return shot{}, fmt.Errorf("unexpected_target")
	}
	req.Header.Set("Cookie", c.Cookie)
	if state != "" {
		req.Header.Set(stateHeader, state)
	}
	if r.count >= r.limit {
		return shot{}, fmt.Errorf("budget_exhausted")
	}
	// O_EXCL reserves each send across restarts, including uncertain failures.
	r.count++
	path := fmt.Sprintf("/opt/sub2api/gwpool-%s-pro1-20261003.%d.sent", r.mode, r.count)
	marker, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return shot{}, fmt.Errorf("budget_marker_already_reserved_or_unwritable")
	}
	_, _ = marker.WriteString("one authorized model request reserved\n")
	_ = marker.Close()
	start := time.Now()
	resp, err := service.GatewayPoolExperimentRoundTrip(r.upstream, req, r.input.ProxyURL, &r.input.Account)
	result := map[string]any{"mode": r.mode, "request": r.count, "gateway": c.Gateway,
		"candidate_hint": c.Hint, "label": label, "model": model, "elapsed_ms": time.Since(start).Milliseconds()}
	if err != nil {
		result["transport_error"] = true
		emit(result) // Never print the raw error, URL, cookie, token or state.
		return shot{}, fmt.Errorf("transport_failed")
	}
	defer resp.Body.Close()
	out := shot{Status: resp.StatusCode, State: resp.Header.Get(stateHeader), Outcome: "unknown"}
	result["http_status"], result["protocol"], result["got_state"] = resp.StatusCode, resp.Proto, out.State != ""
	for _, updated := range resp.Cookies() {
		if updated.Name == "__oailb" {
			actual := service.GatewayPoolExperimentGateway("__oailb=" + updated.Value)
			if actual != c.Gateway {
				result["error"] = "gateway_changed_or_unreadable"
				emit(result)
				return shot{}, fmt.Errorf("gateway_changed_or_unreadable")
			}
		}
	}
	if resp.Proto != r.input.ExpectedProtocol {
		result["error"] = "protocol_mismatch"
		emit(result)
		return shot{}, fmt.Errorf("protocol_mismatch")
	}
	if state != "" && resp.StatusCode == http.StatusOK {
		out.Outcome = "full"
		if out.State != "" && out.State != state {
			out.Outcome = "refreshed"
		}
	}
	result["echo"] = out.Outcome
	if resp.StatusCode != http.StatusOK {
		var failure struct {
			Error struct {
				Code  string `json:"code"`
				Type  string `json:"type"`
				Param string `json:"param"`
			} `json:"error"`
		}
		if json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&failure) == nil {
			allowed := map[string]bool{
				"invalid_request_error": true, "unsupported_value": true, "unsupported_parameter": true,
				"model_not_found": true, "invalid_api_key": true, "invalid_token": true,
				"insufficient_scope": true, "permission_denied": true,
			}
			if allowed[failure.Error.Code] {
				result["error_code"] = failure.Error.Code
			}
			if allowed[failure.Error.Type] {
				result["error_type"] = failure.Error.Type
			}
			for _, param := range []string{"model", "reasoning.effort", "instructions", "input", "stream", "tools", "store"} {
				if failure.Error.Param == param {
					result["error_param"] = param
				}
			}
		}
	}
	// Quality probes stop on headers, exactly like the production warm shot.
	// Only the two explicitly authorized question shots read generated output.
	if text == question && resp.StatusCode == http.StatusOK {
		answer, complete := readQuestionAnswer(resp.Body)
		result["answer_bytes"], result["read_complete"] = len(answer), complete
		if complete {
			result["answer_matches_user_criterion"] = strings.TrimSpace(answer) == "高市早苗"
		} else {
			result["error"] = "question_stream_incomplete"
			emit(result)
			return out, fmt.Errorf("question_stream_incomplete")
		}
	}
	emit(result)
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("non_200_no_retry")
	}
	return out, nil
}

func readQuestionAnswer(body io.Reader) (string, bool) {
	const maxBytes = 1 << 20
	const maxAnswerBytes = 1024
	limited := &io.LimitedReader{R: body, N: maxBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), maxBytes)
	answer := ""
	for scanner.Scan() {
		line := strings.TrimPrefix(scanner.Text(), "data: ")
		var event struct {
			Type  string `json:"type"`
			Delta string `json:"delta"`
		}
		if json.Unmarshal([]byte(line), &event) != nil {
			continue
		}
		if event.Type == "response.output_text.delta" {
			if len(answer)+len(event.Delta) > maxAnswerBytes {
				return answer, false
			}
			answer += event.Delta
		}
		if event.Type == "response.completed" {
			return answer, limited.N > 0 && scanner.Err() == nil
		}
		if event.Type == "response.failed" || event.Type == "error" {
			return answer, false
		}
	}
	return answer, false
}

func requireState(value shot, err error) error {
	if err != nil {
		return err
	}
	if value.State == "" {
		return fmt.Errorf("missing_state")
	}
	return nil
}

func (r *runner) luna(c candidate) error {
	a, err := r.send(c, r.input.Model, "original-A", "", "hi", nil)
	if err = requireState(a, err); err != nil {
		return err
	}
	b, err := r.send(c, r.input.Model, "original-B", a.State, "hi", nil)
	if err != nil {
		return err
	}
	la, err := r.send(c, "gpt-6-luna", "luna-A", "", "hi", nil)
	if err = requireState(la, err); err != nil {
		return err
	}
	lb, err := r.send(c, "gpt-6-luna", "luna-B", la.State, "hi", nil)
	if err != nil {
		return err
	}
	recheck, err := r.send(c, r.input.Model, "original-recheck", a.State, "hi", nil)
	emit(map[string]any{"group": c.Hint, "original": b.Outcome, "luna": lb.Outcome,
		"original_recheck": recheck.Outcome, "complete": err == nil,
		"agreement": err == nil && b.Outcome == lb.Outcome && b.Outcome == recheck.Outcome})
	return err
}

func (r *runner) sessions(c candidate) error {
	oldA, err := r.send(c, r.input.Model, "fresh-session-A", "", "hi", nil)
	if err = requireState(oldA, err); err != nil {
		return err
	}
	if _, err = r.send(c, r.input.Model, "fresh-session-B", oldA.State, "hi", nil); err != nil {
		return err
	}
	fixed := service.NewGatewayPoolExperimentSession(&r.input.Account)
	newA, err := r.send(c, r.input.Model, "same-session-A", "", "hi", fixed)
	if err = requireState(newA, err); err != nil {
		return err
	}
	if _, err = r.send(c, r.input.Model, "same-session-B", newA.State, "hi", fixed); err != nil {
		return err
	}
	if _, err = r.send(c, r.input.Model, "question-fresh-session", oldA.State, question, nil); err != nil {
		return err
	}
	_, err = r.send(c, r.input.Model, "question-same-session", newA.State, question, fixed)
	return err
}

func main() {
	mode := flag.String("mode", "", "luna or sessions")
	flag.Parse()
	gin.SetMode(gin.ReleaseMode)
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if *mode != "luna" && *mode != "sessions" {
		emit(map[string]any{"error": "unknown_experiment"})
		os.Exit(1)
	}
	previous, err := filepath.Glob("/opt/sub2api/gwpool-" + *mode + "-pro1-20261003.*.sent")
	if err != nil || len(previous) != 0 {
		emit(map[string]any{"error": "this_authorization_was_already_used_no_retry"})
		os.Exit(1)
	}
	r := runner{mode: *mode}
	if json.NewDecoder(io.LimitReader(os.Stdin, 4<<20)).Decode(&r.input) != nil ||
		r.input.Account.ID != 1 || r.input.Account.Name != "pro1" || r.input.Account.IsShadow() ||
		!r.input.Account.IsOpenAIOAuth() || !r.input.TransportVerified ||
		r.input.Account.Concurrency <= 0 || r.input.ProxyURL == "" ||
		!regexp.MustCompile(`^gpt-[a-zA-Z0-9.-]{1,48}$`).MatchString(r.input.Model) || r.input.Model == "gpt-6-luna" ||
		(r.input.ExpectedProtocol != "HTTP/2.0" && r.input.ExpectedProtocol != "HTTP/1.1") ||
		len(r.input.Candidates) == 0 || len(r.input.Candidates) > 2 {
		emit(map[string]any{"error": "invalid_pro1_experiment_input"})
		os.Exit(1)
	}
	for _, c := range r.input.Candidates {
		if !regexp.MustCompile(`^unified-[0-9]{1,3}$`).MatchString(c.Gateway) ||
			(c.Hint != "historical-full" && c.Hint != "historical-refreshed") ||
			c.Cookie == "" || strings.ContainsAny(c.Cookie, "\r\n") ||
			service.GatewayPoolExperimentGateway(c.Cookie) != c.Gateway {
			emit(map[string]any{"error": "invalid_candidate"})
			os.Exit(1)
		}
	}
	cfg := &config.Config{}
	cfg.Gateway.OpenAIHTTP2.Enabled = r.input.HTTP2
	cfg.Gateway.OpenAIHTTP2.AllowProxyFallbackToHTTP1 = false
	r.upstream = repository.NewHTTPUpstream(cfg)
	completed := 0
	var runErr error
	switch *mode {
	case "luna":
		r.limit = 10
		for i, c := range r.input.Candidates {
			if i >= 2 {
				break
			}
			if runErr = r.luna(c); runErr != nil {
				break
			}
			completed++
		}
	case "sessions":
		r.limit = 6
		if len(r.input.Candidates) > 0 {
			runErr = r.sessions(r.input.Candidates[0])
			if runErr == nil {
				completed++
			}
		}
	default:
		emit(map[string]any{"error": "unknown_experiment"})
		os.Exit(1)
	}
	result := map[string]any{"experiment_complete": runErr == nil, "completed_groups": completed,
		"mode": *mode, "reserved_requests": r.count, "limit": r.limit,
		"production_protocol_runtime_state": "not_observable_in_isolated_process"}
	if runErr != nil {
		result["error"] = runErr.Error() // Only safe enum errors created above.
	}
	emit(result)
	if runErr != nil {
		os.Exit(1)
	}
}
