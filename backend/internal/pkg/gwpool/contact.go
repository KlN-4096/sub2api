package gwpool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const ContactCriterion = "state-echo-v1"

type ContactReport struct {
	ID             string    `json:"id"`
	AccountTag     string    `json:"account_tag"`
	Gateway        string    `json:"gateway"`
	Model          string    `json:"model"`
	Criterion      string    `json:"criterion"`
	Source         string    `json:"source"`
	First          string    `json:"first"`
	At             time.Time `json:"at"`
	GapKnown       bool      `json:"gap_known"`
	ElapsedSeconds int64     `json:"elapsed_seconds"`
	Outcome        string    `json:"outcome"`
	FullWindowMS   int64     `json:"full_window_ms"`
	WindowFinal    bool      `json:"window_final"`
}

type ContactStats struct {
	Gateway       string `json:"gateway"`
	Model         string `json:"model"`
	Criterion     string `json:"criterion"`
	Source        string `json:"source"`
	First         string `json:"first"`
	Interval      string `json:"interval"`
	Full          int    `json:"full"`
	Refreshed     int    `json:"refreshed"`
	Unknown       int    `json:"unknown"`
	WindowSamples int    `json:"window_samples"`
	WindowMeanMS  int64  `json:"window_mean_ms"`
}

func ContactInterval(known bool, seconds int64) string {
	if !known {
		return "unknown"
	}
	switch {
	case seconds < 3600:
		return "<1h"
	case seconds < 7200:
		return "1-2h"
	case seconds < 14400:
		return "2-4h"
	case seconds < 21600:
		return "4-6h"
	case seconds < 28800:
		return "6-8h"
	case seconds < 36000:
		return "8-10h"
	default:
		return ">=10h"
	}
}

func (s ContactStats) Valid() bool {
	const maxSamples = 8192
	return s.Model != "" && s.Criterion != "" &&
		s.Full >= 0 && s.Full <= maxSamples && s.Refreshed >= 0 && s.Refreshed <= maxSamples && s.Unknown >= 0 && s.Unknown <= maxSamples && s.Full+s.Refreshed+s.Unknown <= maxSamples &&
		s.WindowSamples >= 0 && s.WindowSamples <= s.Full && s.WindowMeanMS >= 0 && s.WindowMeanMS <= 10*60*60*1000
}

func (c *Client) ReportContact(ctx context.Context, report ContactReport) error {
	if c == nil || !validCooldownTag(report.ID) || !validCooldownTag(report.AccountTag) ||
		sanitizeOpaque(report.Gateway, maxGatewayLen) == "" || report.Model == "" || report.Criterion == "" {
		return fmt.Errorf("%w: invalid contact report", ErrPool)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("%w: encode contact report", ErrPool)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("contact/report"), bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("%w: build contact report", ErrPool)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return &PoolError{Status: resp.StatusCode}
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil || !out.OK {
		return fmt.Errorf("%w: invalid contact report acknowledgement", ErrPool)
	}
	return nil
}
