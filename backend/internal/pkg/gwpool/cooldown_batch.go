package gwpool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

type CooldownReportResult struct {
	ID             string                  `json:"id"`
	Status         int                     `json:"status"`
	Recommendation *CooldownRecommendation `json:"recommendation,omitempty"`
}

// ReportCooldownBatch retains per-event acknowledgements. An unsupported batch
// endpoint is a protocol error, not permission to send a second legacy request.
func (c *Client) ReportCooldownBatch(ctx context.Context, reports []CooldownReport) ([]CooldownReportResult, error) {
	if c == nil || len(reports) == 0 || len(reports) > CooldownBatchLimit {
		return nil, fmt.Errorf("%w: invalid cooldown batch size", ErrPool)
	}
	ids := map[string]struct{}{}
	for _, report := range reports {
		if _, duplicate := ids[report.ID]; duplicate || !validCooldownReport(report) {
			return nil, fmt.Errorf("%w: invalid cooldown batch item", ErrPool)
		}
		ids[report.ID] = struct{}{}
	}
	raw, err := json.Marshal(struct {
		Reports []CooldownReport `json:"reports"`
	}{reports})
	if err != nil {
		return nil, fmt.Errorf("%w: encode cooldown batch", ErrPool)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("cooldown/reports"), bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: build cooldown batch", ErrPool)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(cooldownMaxHeader, strconv.Itoa(CooldownMaxSeconds))
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &PoolError{Status: resp.StatusCode}
	}
	var out struct {
		OK      bool                   `json:"ok"`
		Results []CooldownReportResult `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil || !out.OK || len(out.Results) != len(reports) {
		return nil, fmt.Errorf("%w: invalid cooldown batch acknowledgement", ErrPool)
	}
	for i := range out.Results {
		result := &out.Results[i]
		if _, exists := ids[result.ID]; !exists || (result.Status != http.StatusOK && (result.Status < 400 || result.Status > 599)) {
			return nil, fmt.Errorf("%w: invalid cooldown item acknowledgement", ErrPool)
		}
		delete(ids, result.ID)
		if result.Recommendation != nil && !result.Recommendation.Valid() {
			result.Recommendation = nil
		}
	}
	return out.Results, nil
}
