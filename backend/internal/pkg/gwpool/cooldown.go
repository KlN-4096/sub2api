package gwpool

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const cooldownAccountHeader = "X-Gwpool-Account-Tag"
const CooldownMaxSeconds = 24 * 60 * 60
const cooldownMaxHeader = "X-Gwpool-Cooldown-Max-Seconds"
const cooldownObservedHeader = "X-Gwpool-Observed-At"
const CooldownReportInterval = 6 * time.Hour
const CooldownReportPolicyTTL = 24 * time.Hour
const CooldownBatchLimit = 32

var cooldownSteps = [...]int{3600, 7200, 14400, 21600, 28800, 36000, 43200, 57600, 72000, 86400}

func IsCooldownStep(seconds int) bool {
	for _, step := range cooldownSteps {
		if seconds == step {
			return true
		}
	}
	return false
}

func NextCooldownSeconds(seconds int) int {
	for _, step := range cooldownSteps {
		if seconds < step {
			return step
		}
	}
	return CooldownMaxSeconds
}

type CooldownReport struct {
	ID             string    `json:"id"`
	AccountTag     string    `json:"account_tag"`
	Gateway        string    `json:"gateway"`
	WindowSeconds  int       `json:"window_seconds"`
	ElapsedSeconds int       `json:"elapsed_seconds"`
	Result         string    `json:"result"`
	ObservedAt     time.Time `json:"observed_at,omitempty"`
}

type CooldownRecommendation struct {
	Seconds               int       `json:"seconds"`
	Samples               int       `json:"samples"`
	Source                string    `json:"source"`
	ReportIntervalSeconds int       `json:"report_interval_seconds,omitempty"`
	ReportPolicyExpiresAt time.Time `json:"report_policy_expires_at,omitempty"`
}

func validCooldownTag(tag string) bool {
	if len(tag) != 64 {
		return false
	}
	_, err := hex.DecodeString(tag)
	return err == nil
}

// Recommendations affect scheduling, so unknown sources, weak evidence and unbounded values are rejected.
func (r CooldownRecommendation) Valid() bool {
	if !IsCooldownStep(r.Seconds) || r.Samples < 2 {
		return false
	}
	return r.Source == "account" || (r.Source == "pool" && r.Samples >= 6)
}

func (r CooldownRecommendation) StableReporting(now time.Time) bool {
	return r.Valid() && r.Source == "account" && r.Samples >= 6 &&
		r.ReportIntervalSeconds == int(CooldownReportInterval.Seconds()) &&
		r.ReportPolicyExpiresAt.After(now) &&
		!r.ReportPolicyExpiresAt.After(now.Add(CooldownReportPolicyTTL+time.Minute))
}

func CooldownSuccessBucket(report CooldownReport) int {
	observed := report.ElapsedSeconds
	const grace = 60
	if observed <= report.WindowSeconds+grace {
		observed = report.WindowSeconds
	}
	for _, step := range cooldownSteps {
		if observed <= step+grace {
			return step
		}
	}
	return 0
}

func validCooldownReport(report CooldownReport) bool {
	now := time.Now()
	return validCooldownTag(report.ID) && validCooldownTag(report.AccountTag) &&
		sanitizeOpaque(report.Gateway, maxGatewayLen) != "" &&
		report.WindowSeconds >= 3600 && report.WindowSeconds <= CooldownMaxSeconds &&
		report.ElapsedSeconds >= report.WindowSeconds-60 && report.ElapsedSeconds <= 7*24*3600 &&
		(report.Result == "full" || report.Result == "degraded") &&
		(report.ObservedAt.IsZero() || (!report.ObservedAt.Before(now.Add(-7*24*time.Hour)) && !report.ObservedAt.After(now.Add(time.Minute))))
}

func (c *Client) ReportCooldown(ctx context.Context, report CooldownReport) (*CooldownRecommendation, error) {
	if c == nil || !validCooldownReport(report) {
		return nil, fmt.Errorf("%w: invalid cooldown report", ErrPool)
	}
	// Keep the original JSON shape for old servers with strict decoding; the
	// event timestamp uses an optional header which they can safely ignore.
	wire := struct {
		ID             string `json:"id"`
		AccountTag     string `json:"account_tag"`
		Gateway        string `json:"gateway"`
		WindowSeconds  int    `json:"window_seconds"`
		ElapsedSeconds int    `json:"elapsed_seconds"`
		Result         string `json:"result"`
	}{report.ID, report.AccountTag, report.Gateway, report.WindowSeconds, report.ElapsedSeconds, report.Result}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%w: encode cooldown report", ErrPool)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("cooldown/report"), bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: build cooldown report request", ErrPool)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(cooldownMaxHeader, strconv.Itoa(CooldownMaxSeconds))
	if !report.ObservedAt.IsZero() {
		req.Header.Set(cooldownObservedHeader, report.ObservedAt.UTC().Format(time.RFC3339Nano))
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &PoolError{Status: resp.StatusCode}
	}
	var out struct {
		OK             bool                    `json:"ok"`
		Recommendation *CooldownRecommendation `json:"recommendation"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil || !out.OK {
		return nil, fmt.Errorf("%w: invalid cooldown report acknowledgement", ErrPool)
	}
	if out.Recommendation == nil || !out.Recommendation.Valid() {
		return nil, nil
	}
	return out.Recommendation, nil
}
