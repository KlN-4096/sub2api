package gwpool

import (
	"context"
	"time"
)

const (
	ContactCriterion  = "state-echo-v1"
	ContactBatchLimit = 32
)

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

// Valid reports whether the pool can accept this report; the client never sends
// one that fails it.
func (report ContactReport) Valid() bool {
	return validCooldownTag(report.ID) && validCooldownTag(report.AccountTag) &&
		sanitizeOpaque(report.Gateway, maxGatewayLen) != "" && report.Model != "" && report.Criterion != ""
}

type ContactReportResult struct {
	ID     string `json:"id"`
	Status int    `json:"status"`
}

func (r ContactReportResult) acknowledged() (string, int) { return r.ID, r.Status }

// ReportContactBatch sends contact reports of any members of this key at once.
func (c *Client) ReportContactBatch(ctx context.Context, reports []ContactReport) ([]ContactReportResult, error) {
	return postReportBatch[ContactReport, ContactReportResult](ctx, c, "contact", reports, ContactBatchLimit, nil)
}

func (report ContactReport) reportID() string { return report.ID }
