package gwpool

import (
	"encoding/hex"
	"slices"
	"time"
)

const cooldownAccountHeader = "X-Gwpool-Account-Tag"
const CooldownMaxSeconds = 24 * 60 * 60
const cooldownMaxHeader = "X-Gwpool-Cooldown-Max-Seconds"
const CooldownReportInterval = 6 * time.Hour
const CooldownReportPolicyTTL = 24 * time.Hour
const CooldownBatchLimit = 32

var cooldownSteps = [...]int{3600, 7200, 14400, 21600, 28800, 36000, 43200, 57600, 72000, 86400}

func IsCooldownStep(seconds int) bool {
	return slices.Contains(cooldownSteps[:], seconds)
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

// Valid reports whether the pool can accept this report; the client never sends
// one that fails it.
func (report CooldownReport) Valid() bool {
	now := time.Now()
	return validCooldownTag(report.ID) && validCooldownTag(report.AccountTag) &&
		sanitizeOpaque(report.Gateway, maxGatewayLen) != "" &&
		report.WindowSeconds >= 3600 && report.WindowSeconds <= CooldownMaxSeconds &&
		report.ElapsedSeconds >= report.WindowSeconds-60 && report.ElapsedSeconds <= 7*24*3600 &&
		(report.Result == "full" || report.Result == "degraded") &&
		(report.ObservedAt.IsZero() || (!report.ObservedAt.Before(now.Add(-7*24*time.Hour)) && !report.ObservedAt.After(now.Add(time.Minute))))
}

func (report CooldownReport) reportID() string { return report.ID }
