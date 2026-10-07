package gwpool

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const cooldownAccountHeader = "X-Gwpool-Account-Tag"

type CooldownReport struct {
	ID             string `json:"id"`
	AccountTag     string `json:"account_tag"`
	Gateway        string `json:"gateway"`
	WindowSeconds  int    `json:"window_seconds"`
	ElapsedSeconds int    `json:"elapsed_seconds"`
	Result         string `json:"result"`
}

type CooldownRecommendation struct {
	Seconds int    `json:"seconds"`
	Samples int    `json:"samples"`
	Source  string `json:"source"`
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
	if r.Seconds < 3600 || r.Seconds > 36000 || r.Samples < 2 {
		return false
	}
	switch r.Seconds {
	case 3600, 7200, 14400, 21600, 28800, 36000:
	default:
		return false
	}
	return r.Source == "account" || (r.Source == "pool" && r.Samples >= 6)
}

func (c *Client) ReportCooldown(ctx context.Context, report CooldownReport) (*CooldownRecommendation, error) {
	if c == nil || !validCooldownTag(report.ID) || !validCooldownTag(report.AccountTag) ||
		sanitizeOpaque(report.Gateway, maxGatewayLen) == "" ||
		report.WindowSeconds < 3600 || report.WindowSeconds > 36000 ||
		report.ElapsedSeconds < report.WindowSeconds-60 || report.ElapsedSeconds > 7*24*3600 ||
		(report.Result != "full" && report.Result != "degraded") {
		return nil, fmt.Errorf("%w: invalid cooldown report", ErrPool)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("%w: encode cooldown report", ErrPool)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("cooldown/report"), bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: build cooldown report request", ErrPool)
	}
	req.Header.Set("Content-Type", "application/json")
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
